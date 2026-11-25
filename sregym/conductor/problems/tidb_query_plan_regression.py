"""A FleetCast dashboard query loses its TiDB index range scan."""

import json
import logging
import subprocess
from statistics import median

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.service.apps.fleet_cast import FleetCast
from sregym.utils.decorators import mark_fault_injected

logger = logging.getLogger(__name__)

ACTIVE_CONTACTS_QUERY = """
SELECT COUNT(DISTINCT satellite_id)
FROM contact_windows
WHERE assigned = TRUE AND end_time > UTC_TIMESTAMP()
""".strip()

HISTORY_BATCH_ROWS = 32768
HISTORY_ROWS = 262144
ACTIVE_ROWS = 32

SEED_HISTORY = """
    INSERT INTO contact_windows (id, satellite_id, ground_station_id, end_time, assigned)
    WITH RECURSIVE digits AS (
        SELECT 0 AS n UNION ALL SELECT n + 1 FROM digits WHERE n < 255
    )
    SELECT -({offset} + a.n * 128 + b.n + 1), CONCAT('HISTORY-', a.n), 'GS-1', '2020-01-01', TRUE
    FROM digits a CROSS JOIN digits b
    WHERE b.n < 128
    ON DUPLICATE KEY UPDATE id = contact_windows.id
"""

SEED_ACTIVE_CONTACTS = f"""
    INSERT INTO contact_windows (id, satellite_id, ground_station_id, end_time, assigned)
    WITH RECURSIVE contacts AS (
        SELECT 1 AS n UNION ALL SELECT n + 1 FROM contacts WHERE n < {ACTIVE_ROWS}
    )
    SELECT -({HISTORY_ROWS} + n), CONCAT('PLAN-SAT-', n), 'GS-1', '2099-01-01', TRUE
    FROM contacts
    ON DUPLICATE KEY UPDATE id = contact_windows.id
"""

# Runs in the existing FleetCast backend container, which already has PyMySQL
# and the TiDB connection settings. A single connection keeps pod exec and
# connection setup outside the query latency measurements.
SQL_CLIENT = """
import json
import os
import sys
from time import perf_counter

import pymysql

ca = os.environ.get("TIDB_CA_PEM")
connection = pymysql.connect(
    host=os.environ["TIDB_HOST"],
    port=4000,
    user=os.environ["TIDB_USER"],
    password=os.environ["TIDB_PASSWORD"],
    database=os.environ["TIDB_DATABASE"],
    ssl={"ca": ca, "check_hostname": True} if ca else None,
    connect_timeout=5,
    read_timeout=90,
    write_timeout=10,
    autocommit=True,
)
with connection, connection.cursor() as cursor:
    results = []
    for sql in json.load(sys.stdin):
        start = perf_counter()
        cursor.execute(sql)
        rows = cursor.fetchall()
        results.append({"rows": rows, "ms": (perf_counter() - start) * 1000})
    print(json.dumps(results))
"""


class TiDBQueryPlanMitigationOracle(Oracle):
    """Grade the dashboard SQL's access path and median warm latency."""

    def evaluate(self) -> dict:
        """Probe the real dashboard query against the live FleetCast database."""
        try:
            probe = self.problem.probe_query()
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            return self.fail_from_exception(exc)

        target_ms = self.problem.latency_target_ms
        if target_ms is None:
            return self.fail("baseline_missing")
        if not probe["indexed"] or probe["median_ms"] >= target_ms or min(probe["counts"]) < ACTIVE_ROWS:
            return self.fail("dashboard_query_unhealthy", target_ms=target_ms, **probe)
        return {"success": True, "target_ms": target_ms, **probe}


class TiDBQueryPlanRegression(Problem):
    """Hide FleetCast's active-contact index after populating contact history."""

    # FleetCast's backend already schedules the dashboard query every 15 seconds.
    run_default_workload = False

    def __init__(self):
        super().__init__(app=FleetCast())
        self.propagation_duration_s = 20
        self.problem_id = "tidb_query_plan_regression"
        self.faulty_service = "basic-tidb"
        self.latency_target_ms: float | None = None
        self.root_cause = self.build_structured_root_cause(
            component="service/basic-tidb",
            namespace="tidb-cluster",
            description=(
                "FleetCast's /api/dashboard activeContacts query counts distinct satellite_id from "
                "satellite_sim.contact_windows where assigned = TRUE and end_time > UTC_TIMESTAMP(). "
                "The idx_active (assigned, end_time) index was made INVISIBLE in TiDB. The optimizer "
                "lost its index range scan and scans the historical contact rows, increasing latency "
                "without SQL errors."
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(
            problem=self,
            expected=self.root_cause,
        )
        self.mitigation_oracle = TiDBQueryPlanMitigationOracle(problem=self)

    def run_sql(self, *statements: str, timeout: int = 60) -> list[dict]:
        """Execute SQL from the FleetCast backend and return rows and timings."""
        release = self.app.helm_configs["release_name"]
        command = [
            "kubectl",
            "-n",
            self.app.namespace,
            "exec",
            "-i",
            f"deployment/{release}-satellite-app-backend",
            "-c",
            "backend",
            "--",
            "python",
            "-c",
            SQL_CLIENT,
        ]
        result = subprocess.run(command, input=json.dumps(statements), text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            raise subprocess.CalledProcessError(result.returncode, command, stderr=result.stderr)
        return json.loads(result.stdout)

    def probe_query(self) -> dict:
        """Collect EXPLAIN, one warm-up, and seven timed executions."""
        # UTC_TIMESTAMP() has one-second precision. Cross a clock tick between
        # samples so TiKV cannot reuse the cached result for the same cutoff.
        explain, _warmup, *timed = self.run_sql(
            f"EXPLAIN {ACTIVE_CONTACTS_QUERY}",
            ACTIVE_CONTACTS_QUERY,
            *(("DO SLEEP(1.05)", ACTIVE_CONTACTS_QUERY) * 7),
        )
        samples = timed[1::2]
        plan = explain["rows"]
        samples_ms = [sample["ms"] for sample in samples]
        return {
            "indexed": any("IndexRangeScan" in row[0] and "index:idx_active(" in row[3] for row in plan),
            "full_scan": any("TableFullScan" in row[0] and "table:contact_windows" in row[3] for row in plan),
            "plan": plan,
            "counts": [sample["rows"][0][0] for sample in samples],
            "samples_ms": samples_ms,
            "median_ms": median(samples_ms),
        }

    @mark_fault_injected(strict=True)
    def inject_fault(self):
        """Seed historical contacts, verify the baseline, then hide the index."""
        if self.fault_injected:
            return
        self.run_sql(
            *(SEED_HISTORY.format(offset=offset) for offset in range(0, HISTORY_ROWS, HISTORY_BATCH_ROWS)),
            SEED_ACTIVE_CONTACTS,
            "ANALYZE TABLE contact_windows",
            timeout=300,
        )
        baseline = self.probe_query()
        logger.info("FleetCast dashboard query baseline: %s", baseline)
        if not baseline["indexed"] or min(baseline["counts"]) < ACTIVE_ROWS:
            raise RuntimeError("FleetCast active-contacts query did not establish an indexed baseline")
        self.latency_target_ms = max(10.0, 4 * baseline["median_ms"])

        try:
            self.run_sql("ALTER TABLE contact_windows ALTER INDEX idx_active INVISIBLE")
            regressed = self.probe_query()
            logger.info(
                "FleetCast dashboard query after index change (target %.2f ms): %s", self.latency_target_ms, regressed
            )
            if (
                regressed["indexed"]
                or not regressed["full_scan"]
                or regressed["median_ms"] < self.latency_target_ms
                or min(regressed["counts"]) < ACTIVE_ROWS
            ):
                raise RuntimeError("Hiding idx_active did not produce a slow, successful dashboard query")
        except Exception:
            self.recover_fault()
            raise

    @mark_fault_injected(strict=True)
    def recover_fault(self):
        """Restore index visibility, retaining the same data for verification."""
        self.run_sql("ALTER TABLE contact_windows ALTER INDEX idx_active VISIBLE")
