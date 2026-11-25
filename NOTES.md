# FleetCast TiDB query-plan regression

## Fault and recovery

Problem ID `tidb_query_plan_regression` uses the existing FleetCast app and its TiDB v8.1.0 cluster. The backend runs the dashboard query every 15 seconds. The affected SQL is the `active_contacts_now` query in `/api/dashboard`.

```sql
SELECT COUNT(DISTINCT satellite_id)
FROM contact_windows
WHERE assigned = TRUE AND end_time > UTC_TIMESTAMP();
```

Before injection, the problem inserts 262,144 expired contact rows and 32 active contacts into the existing `satellite_sim.contact_windows` table and analyzes it. The synthetic rows use explicit negative primary keys so they can coexist with FleetCast's auto-increment IDs and be seeded again without duplicating rows.

Injection makes the existing `idx_active (assigned, end_time)` index invisible. Recovery makes it visible again. Neither statement rebuilds the index or restarts the service.

```sql
ALTER TABLE satellite_sim.contact_windows ALTER INDEX idx_active INVISIBLE;
ALTER TABLE satellite_sim.contact_windows ALTER INDEX idx_active VISIBLE;
```

## Measurement and oracle

The probe runs from FleetCast's backend container using its existing PyMySQL client. It captures `EXPLAIN`, runs one warm-up, and measures seven executions on the same connection. Each measured execution follows a 1.05-second pause so the one-second `UTC_TIMESTAMP()` cutoff changes between samples. Timings include query execution and fetching the result. They exclude connection setup, the pauses, and Kubernetes exec overhead.

The mitigation oracle requires an `IndexRangeScan` on `idx_active`, at least 32 active contacts in every result, and median latency below the greater of 10 ms or four times the measured baseline median. The diagnosis oracle expects the affected query and the invisible index as its cause.

## Database validation

On November 25, 2026, the SQL phase ran against native TiDB, PD, and TiKV v8.1.0 processes on one host. The run used the table DDL from `sregym/service/apps/tidb_cluster_operator.py`, the same fixture and SQL client as the problem, and the actual problem and mitigation oracle methods. The harness directed the client to local TiDB over TCP in place of Kubernetes exec. All measured queries returned 32 in all four stages.

| Stage | Query latency samples (ms) | Median (ms) | Outcome |
| --- | --- | ---: | --- |
| Baseline | 3.94, 4.73, 6.80, 3.59, 4.50, 8.15, 7.94 | 4.73 | Indexed |
| After injection | 215.40, 188.23, 105.65, 138.34, 122.87, 120.77, 90.70 | 122.87 | Full scan |
| Oracle with fault live | 98.02, 94.75, 106.06, 101.73, 108.81, 101.21, 86.12 | 101.21 | Fail |
| Oracle after recovery | 3.31, 2.75, 3.30, 3.37, 3.86, 3.05, 5.03 | 3.31 | Pass |

The measured recovery target was 18.93 ms. Below are the `EXPLAIN` operators, estimated row counts, execution tasks, and access objects from the run.

```text
Baseline
StreamAgg_7                    1.00        root
└─IndexLookUp_19               24.00       root
  ├─IndexRangeScan_17(Build)   24.00       cop[tikv]  table:contact_windows, index:idx_active(assigned, end_time)
  └─TableRowIDScan_18(Probe)   24.00       cop[tikv]  table:contact_windows

Fault live
StreamAgg_7                    1.00        root
└─TableReader_13               24.00       root
  └─Selection_12               24.00       cop[tikv]
    └─TableFullScan_11         262176.00   cop[tikv]  table:contact_windows

Recovered
StreamAgg_7                    1.00        root
└─IndexLookUp_19               24.00       root
  ├─IndexRangeScan_17(Build)   24.00       cop[tikv]  table:contact_windows, index:idx_active(assigned, end_time)
  └─TableRowIDScan_18(Probe)   24.00       cop[tikv]  table:contact_windows
```

## Repository validation

From the repository root with submodules initialized and the kind cluster configured:

```bash
uv sync
uv run prek install
uv run prek run --all-files
uv run python tests/integration/validate_problem.py --problem tidb_query_plan_regression
```

The changed-file hooks passed. The all-files run exited 1 and reported the same changes on a clean checkout of the upstream base. The hooks reformatted or fixed whitespace in `docker/blueprint-hotel/compatibility.patch`, `sregym/conductor/problems/kafka_producer_leak.py`, `sregym/generators/fault/inject_kafka.py`, and `sregym/service/apps/workspace/application.py`. Those changes were reverted.

The integration command on the development runner stopped at `Missing kubeconfig. Please set up a cluster.` The database measurements above establish the SQL behavior. The kind workflow still needs to verify FleetCast deployment, the in-pod SQL connection, and the lifecycle on CI.

PR validation trigger:

```text
/validate-problem tidb_query_plan_regression
```
