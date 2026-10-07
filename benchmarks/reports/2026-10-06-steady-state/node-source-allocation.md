# Tenant node-source resync allocation

The DB-backed node source reloads the entire node table independently for every tenant controller source, then filters by cluster in memory. This creates recurring fleet-size × cluster-count work even when topology is unchanged.

Measured at production controller revision `a4b4f2a21cf3344ca19138ed4f5bbe099f430cb4` with the production `NodeEventSource`, PostgreSQL 17.4, macOS arm64 / Go 1.27.1, a 32-connection pool, 100 clusters and 5,000 nodes, two sources per cluster. Five repetitions each, startup plus three forced resyncs per repetition. Fixture insertion is excluded.

| Source concurrency | Queries / resync | Modeled rows fetched / resync | Median first resync | Allocated / resync | Pool waits / resync |
|---:|---:|---:|---:|---:|---:|
| 2 | 200 | 1,000,000 | 7.58 s | 7.20 GB | 0 |
| 10 | 200 | 1,000,000 | 5.23 s | 7.20 GB | 0 |
| 100 | 200 | 1,000,000 | 4.63 s | 7.20 GB | 168 |

The row count is a topology estimate checked against the source implementation, not a PostgreSQL rows-returned counter. Query, allocation, elapsed-time and pool-wait counters are measured. Allocation is cumulative allocation, not peak or retained heap. Timing is preliminary local evidence: tool compilation occurred during this batch, so these numbers are not an idle-host capacity claim. The allocation and query amplification are consistent across repetitions and concurrency settings.

Both tenant node health and node remediation register a cluster-filtered source. `NodeEventSource.enqueueAll` calls `ListNodes(ctx, db.NodeFilters{})`; `Enqueue` applies `s.filters.Matches` afterward. Each source resyncs at 15 minutes plus jitter. The harness drains queues without downstream reconciles, so this isolates source overhead and understates complete reconciliation work.

Reproduce using the existing component lane, a dedicated PostgreSQL database, and the checked-in 100-cluster / 5,000-node scenario:

```bash
python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --output /tmp/nke-perf-inventory.json
make -C controller perf-build
controller/bin/cwe-perf node-source \
  -inventory /tmp/nke-perf-inventory.json \
  -db-dsn "$BENCHMARK_DB_DSN" \
  -copies-per-cluster 2 -source-concurrency 2 -resyncs 3 \
  -output /tmp/nke-source-result.json
```

`perf-build` is added by the local harness update; the existing README also documents building the driver. Repeat with source concurrency 10 and 100.

Track this as a steady-state efficiency concern. Acceptance for a future fix: preserve initial/event/resync convergence while restricting DB reads to each source’s cluster or sharing the fleet read; rerun the identical source matrix and mixed worker soak. No production remediation is included in the current experiment work.
