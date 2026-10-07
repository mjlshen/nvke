# nke-controller performance investigation log

This document records measurements and bottleneck hypotheses produced while
executing the [performance analysis plan](performance-analysis-plan.md). Keep
the plan focused on intent and use this log for evidence that changes as the
investigation progresses.

## 2026-09-22: database-backed Prometheus collection

### Status and method

These are preliminary local synthetic measurements, not production capacity
claims. A new harness lane materialized complete cluster, node-pool, retained
node-set, node, and condition inventory in PostgreSQL, registered the production
`dbmetrics.Collector` in an isolated Prometheus registry, and executed complete
Prometheus text scrapes through `promhttp`.

The lane reports query count, serialized sample count, response bytes,
allocation, database-pool waits, and per-scrape latency. One unmeasured warm-up
preceded three measured scrapes. The environment was a macOS arm64 workstation,
Go 1.27.1, PostgreSQL 18.6, `GOMAXPROCS=12`, and a 32-connection database pool.
All data was hot-cache after the warm-up.

Except for the smoke case, every cluster had ten node pools and every pool had
ten retained node sets:

| Clusters | Nodes | Pools | Node sets | Queries/scrape | Series/scrape | Bytes/scrape | Median scrape | Allocated/scrape |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 100 | 30 | 60 | 45 | 1,920 | 305 KB | 14.7 ms | 4.5 MB |
| 10 | 1,000 | 100 | 1,000 | 115 | 18,510 | 2.91 MB | 120 ms | 41.7 MB |
| 100 | 1,000 | 1,000 | 10,000 | 1,105 | 32,100 | 8.53 MB | 833 ms | 115.6 MB |
| 10 | 10,000 | 100 | 1,000 | 115 | 171,510 | 23.44 MB | 728 ms | 353.0 MB |

Every measured query and series count exactly matched the model, all scrapes
were complete, and no database pool waits or measured database errors occurred.

### Findings

The collector has two independent scaling axes:

1. **Topology drives query amplification.** Holding nodes at 1,000 while moving
   from 10 clusters/100 pools to 100 clusters/1,000 pools increased queries from
   115 to 1,105 and median latency from 120 ms to 833 ms. The production path
   performs five base queries, one node-pool query per cluster, and one node-set
   query per pool.
2. **Nodes drive cardinality, serialization, and allocation.** Holding topology
   at 10 clusters/100 pools/1,000 sets while increasing nodes from 1,000 to
   10,000 left query count fixed at 115 but increased the response from 2.91 MB
   to 23.44 MB, series from 18,510 to 171,510, and allocation from 41.7 MB to
   353.0 MB per scrape. Sixteen condition series plus one node-info series are
   emitted for every node.

Three concurrent 10,000-node scrapes completed in 962 ms wall time. Individual
scrape latency rose to 954-962 ms from a 728 ms sequential median, and retained
heap growth at the end of the phase rose from 203.5 MB to 442.8 MB. Total
allocation and database query count were essentially unchanged, showing that
overlapping scrapes primarily amplify simultaneous CPU and live-memory demand.

The current cases remain comfortably below the collector's ten-second timeout,
but a single 10,000-node scrape already constructs a 23 MB response and allocates
roughly 353 MB. The next experiment should repeat these shapes under fixed CPU
and memory limits and concurrent API/reconciliation work before treating the
local latency as operational headroom.

### Stepped curves through 5,000 nodes

A follow-up run used one warm-up followed by five measured scrapes at every
step. The harness also installed a query hook after fixture creation and split
database time by production collector query shape. These measurements used the
same workstation and PostgreSQL instance shape as the initial spot checks.

#### Node-count curve

This curve held topology fixed at 10 clusters, 100 pools, and 1,000 retained
node sets while increasing nodes:

| Nodes | Queries | Series | Response | Median scrape | DB query time | Allocated/scrape |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 115 | 3,210 | 0.85 MB | 37.4 ms | 30.0 ms | 11.5 MB |
| 250 | 115 | 5,760 | 1.20 MB | 40.4 ms | 30.0 ms | 16.4 MB |
| 500 | 115 | 10,010 | 1.77 MB | 52.2 ms | 34.6 ms | 24.5 MB |
| 1,000 | 115 | 18,510 | 2.91 MB | 82.0 ms | 44.5 ms | 41.7 MB |
| 2,500 | 115 | 44,010 | 6.33 MB | 164.9 ms | 68.9 ms | 93.6 MB |
| 5,000 | 115 | 86,510 | 12.03 MB | 361.5 ms | 119.6 ms | 180.8 MB |

Above 1,000 nodes, increasing nodes five times increased median scrape latency
4.4 times, series 4.7 times, payload 4.1 times, and allocation 4.3 times. The
corresponding endpoint exponents are approximately 0.88-0.96. The node axis is
therefore approximately linear at the upper end of the tested range; there is
no evidence of super-linear node-count scaling through 5,000 nodes. Fixed
topology/query overhead makes the full 100-to-5,000 curve look sub-linear.

#### Cluster and associated-topology curve

This curve held the fleet at 5,000 nodes while increasing clusters. Every
cluster had ten pools and every pool had ten retained node sets, so pools and
node sets increased with clusters as they would in a balanced fleet:

| Clusters | Pools | Node sets | Queries | Series | Median scrape | Total DB time | Node-set query time |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 100 | 1,000 | 115 | 86,510 | 361.5 ms | 119.6 ms | 27.5 ms |
| 25 | 250 | 2,500 | 280 | 88,775 | 393.0 ms | 167.4 ms | 70.8 ms |
| 50 | 500 | 5,000 | 555 | 92,550 | 565.7 ms | 315.1 ms | 205.7 ms |
| 75 | 750 | 7,500 | 830 | 96,325 | 766.5 ms | 508.6 ms | 389.8 ms |
| 100 | 1,000 | 10,000 | 1,105 | 100,100 | 1,087.0 ms | 800.6 ms | 674.0 ms |

This curve exposes the confirmed super-linear component. Increasing clusters,
pools, and node sets ten times increased cumulative node-set-list query time
24.5 times, corresponding to an endpoint exponent of approximately 1.39. Over
the less overhead-dominated 25-to-100-cluster range, a four-times topology
increase raised node-set query time 9.5 times, an exponent of approximately
1.62.

The mechanism is the nested collector traversal: it issues one node-set query
per pool, and without an index on `node_sets(node_pool_id)` each query inspects
the growing node-set table. With pools `P` and total node sets `S`, that portion
is consistent with `O(P*S)`. When both grow with clusters, the underlying scan
work approaches quadratic even though fixed query and cache overhead reduce the
observed exponent at these sizes. End-to-end scrape time increased only three
times because the fixed cost of serializing the same 5,000 nodes remains large;
the query profile prevents that cost from hiding the super-linear SQL path.

#### Retained-node-set curve

This curve held 10 clusters, 100 pools, and 5,000 nodes fixed while increasing
retained node sets per pool:

| Sets/pool | Total sets | Series | Median scrape | Node-set query time |
|---:|---:|---:|---:|---:|
| 1 | 100 | 85,610 | 342.9 ms | 29.6 ms |
| 2 | 200 | 85,710 | 333.0 ms | 31.0 ms |
| 5 | 500 | 86,010 | 347.2 ms | 33.4 ms |
| 10 | 1,000 | 86,510 | 361.5 ms | 27.5 ms |
| 20 | 2,000 | 87,510 | 358.1 ms | 62.2 ms |
| 50 | 5,000 | 90,510 | 379.9 ms | 61.6 ms |

With pool count fixed, increasing retained sets adds series linearly and the
scan component remains small enough that per-query/socket overhead and cache
effects dominate locally. This curve does not show super-linear end-to-end
growth through 5,000 retained sets. It is consistent with the `O(P*S)` model:
holding `P` fixed makes that term linear in `S`; the super-linear behavior
appears when both pool count and the total node-set table grow together.

Across all stepped runs, actual query and series counts matched the model, all
scrapes were complete, and there were no measured database errors or connection
pool waits. The measurements isolate hot-cache collection and serialization;
they do not cover Prometheus ingestion/storage, cold PostgreSQL cache, CPU or
memory limits, or interference with API and reconciliation traffic.

#### Overlapping scrapes at the largest tested topology

At 100 clusters, 1,000 pools, 10,000 retained node sets, and 5,000 nodes, the
same scrape was run sequentially and with two or four concurrent collectors:

| Concurrency | Scrapes | Median per scrape | End-of-phase heap delta | Allocated/scrape | Pool waits |
|---:|---:|---:|---:|---:|---:|
| 1 | 5 | 1,087 ms | 90.6 MB | 253.7 MB | 0 |
| 2 | 4 | 1,353 ms | 252.0 MB | 253.7 MB | 0 |
| 4 | 4 | 1,238 ms | 495.2 MB | 253.6 MB | 0 |

Per-scrape allocation remained constant, but live heap retained at the end of
the phase rose strongly with overlap. Individual latency was 14-25% above the
sequential median; the non-monotonic two-versus-four ordering is within the
noise expected from one local run and hot shared caches. The important capacity
signal is that the collector has no single-flight or cached snapshot: every
overlapping scrape repeats the queries, object construction, and approximately
254 MB of allocation independently. Four-way overlap completed without pool
waits only because the local 32-connection pool had ample capacity.

## 2026-09-18: tenant managers and node-agent readiness

### Status

These are local synthetic measurements for choosing the next experiments.
They are not product limits or capacity claims.

The tests exercised production tenant controller-manager, controller-runtime,
database repository, node-agent readiness reconciler, and gRPC client code
against deterministic virtual tenant APIs and node agents. They did not run the
complete worker, Temporal, a production site-controller Kubernetes cluster, or
real tenant clusters.

### Environment

| Input | Value |
|---|---|
| Source revision | `dfc509585444092ec1d86769d04ba2309f9ae891` plus the uncommitted performance harness |
| Host | macOS arm64 workstation |
| Go | 1.27.1 |
| `GOMAXPROCS` | 12 |
| PostgreSQL | 18.6, isolated temporary instance |
| Tenant API and node agent | deterministic in-process simulators on localhost |
| Tenant-manager startup concurrency | 2 |
| Repetitions | 1 warm-up followed by 5 measured runs unless noted |

The PostgreSQL version and workstation shape are not production-equivalent.
Comparisons within the same experiment are useful; absolute durations must be
re-established on controlled Linux infrastructure.

### Tenant manager and initial reconciliation matrix

| Clusters | Nodes | Manager cold start median | Initial reconciliation median | Steady goroutines | Active watches |
|---:|---:|---:|---:|---:|---:|
| 10 | 100 | 22 ms | 328 ms | 544 | 20 |
| 10 | 1,000 | 19 ms | 462 ms | 544 | 20 |
| 10 | 5,000 | 41 ms | 1,864 ms | 544 | 20 |
| 50 | 100 | 86 ms | 281 ms | 2,704 | 100 |
| 100 | 100 | 160 ms | 282 ms | 5,404 | 200 |
| 100 | 5,000 | 1,246 ms | 1,788 ms | 5,404 | 200 |

The manager and reconciliation durations are medians of five runs. Goroutine
and watch counts are stable values observed at quiescence.

#### Finding: per-cluster runtime cost is the leading cluster-count bottleneck

Each tenant cluster receives its own controller-runtime manager, caches,
controllers, and watches. The experiment observed approximately 54 goroutines
and two long-lived watches per cluster:

- 10 clusters: 544 goroutines and 20 watches;
- 50 clusters: 2,704 goroutines and 100 watches; and
- 100 clusters: 5,404 goroutines and 200 watches.

The relationship is nearly linear and exists even when total node count is
held at 100. Manager startup also increases with cluster count. At 5,000 total
nodes, spreading nodes over more clusters increases manager startup cost while
allowing more tenant caches/controllers to perform initial reconciliation in
parallel.

This is evidence of a scaling mechanism, not yet proof that goroutines are the
first exhausted resource. The next cluster-axis experiment must capture file
descriptors, connections, per-manager heap, CPU profiles, and tenant API-server
load on Linux.

### Node-agent readiness

The production controller currently uses controller-runtime's default of one
reconcile worker. Each node is requeued two minutes after its probe completes.
The changed-condition path performs a synchronous agent RPC plus a transaction
that locks and updates the node. The unchanged path performs the RPC and
initial node read but avoids the update transaction.

With a zero-delay agent and one worker, five repetitions produced:

| Nodes | Health sweep median |
|---:|---:|
| 100 | 99 ms |
| 1,000 | 1,090 ms |
| 5,000 | 4,792 ms |

With a deterministic 10 ms delay in every agent RPC:

| Nodes | Workers | State | Duration | Database queries |
|---:|---:|---|---:|---:|
| 100 | 1 | transition | 1.52 s | 600 |
| 1,000 | 1 | transition | 14.55 s | 6,000 |
| 1,000 | 1 | unchanged steady state | 13.57 s | 1,000 |
| 5,000 | 1 | transition | 75.36 s | 30,000 |
| 5,000 | 10 | transition | 8.52 s | 30,000 |
| 5,000 | 10 | unchanged steady state | 7.80 s | 5,000 |

The 100-node and 5,000-node single-worker transition values are single runs.
The 1,000-node transition/steady comparison and the final 10-worker
transition/steady comparison are also single validation runs; an earlier
three-run 5,000-node/10-worker transition experiment had a median of 8.31 s.

#### Finding: one readiness worker makes agent latency a fleet-wide serial path

At 5,000 nodes, a single worker took about 75 seconds for a condition-changing
sweep with only 10 ms of configured agent delay. Bounded concurrency of 10
reduced the same transition to roughly 8 seconds without database pool waits in
this synthetic environment.

The result makes worker concurrency a strong tuning candidate, but does not yet
establish the correct production value. Increasing it also increases concurrent
agent connections, database work during health transitions, and load following
a restart or resync. Test concurrency against realistic agent latency,
timeouts, failures, database resources, and other reconcilers before changing
the default.

The phase split also matters operationally. A health transition used six
database operations per node in this harness, while an unchanged probe used
one. Capacity testing must include both steady state and correlated condition
changes; treating the transition sweep as normal background database load would
overstate steady-state cost.

### Harness corrections made during this investigation

- Updated benchmark fixtures for the current nested CAPI and node provider
  database model.
- Marked and removed all benchmark-owned clusters before each scenario so a
  large-to-small matrix order cannot leave stale inventory. A descending
  100-cluster/100-node to 10-cluster/100-node validation ended with exactly 10
  clusters and 100 nodes.
- Split node-agent measurements into the condition-changing sweep and one or
  more unchanged steady-state sweeps, including per-phase database query counts.
- Verified 11 Python simulator tests and the Go benchmark packages.

## 2026-09-18: node-set foreign-key lookup scaling

### Status and method

This targeted database experiment confirms a super-linear interaction in two
production query paths. It is not an end-to-end controller or capacity test.

The [SQL fixture](../../benchmarks/sql/node-set-scaling.sql) uses the production
schema and the query shapes from `ListNodeSets` and `ListNodes`. It independently
varies node pools (`P`), retained node sets (`S`), and nodes (`N`), performs one
unreported warm-up, and records five hot-cache repetitions. The comparison adds
only temporary candidate indexes on `node_sets(node_pool_id)` and
`nodes(node_set_id)`; no production migration was changed.

### Node-pool reconciliation traversal

The modeled traversal lists nodes once for every retained node set. Median
duration in milliseconds at 100 node pools was:

| Retained node sets | Nodes | Current schema | With candidate indexes |
|---:|---:|---:|---:|
| 100 | 100 | 0.637 | 0.906 |
| 100 | 1,000 | 4.720 | 0.996 |
| 100 | 5,000 | 20.699 | 1.547 |
| 1,000 | 100 | 6.135 | 6.401 |
| 1,000 | 1,000 | 42.204 | 2.550 |
| 1,000 | 5,000 | 204.652 | 4.283 |
| 5,000 | 100 | 30.028 | 34.860 |
| 5,000 | 1,000 | 206.199 | 10.294 |
| 5,000 | 5,000 | 983.650 | 13.091 |

On the current schema, increasing `S` and `N` together by 50 times increased
duration from 0.637 ms to 983.650 ms, or 1,544 times. Holding one dimension
fixed isolates the interaction: at 5,000 nodes, increasing retained node sets
50 times increased duration 47.5 times; at 5,000 retained node sets, increasing
nodes 50 times increased duration 32.8 times. The work is therefore consistent
with repeated `O(S*N)` row inspection rather than a single linear fleet scan.

The sparse cases also show that indexes are not free: when 5,000 node sets
contain only 100 nodes, function/query-call overhead dominates and the indexed
version is slightly slower. The production reconciler deletes empty non-current
sets, so the dense diagonal where retained sets have nodes is the more relevant
failure mode.

### Metrics node-set traversal

With ten retained node sets per pool, median duration was:

| Node pools | Total node sets | Current schema | With `node_pool_id` index |
|---:|---:|---:|---:|
| 10 | 100 | 0.081 ms | 0.134 ms |
| 50 | 500 | 1.497 ms | 0.608 ms |
| 100 | 1,000 | 3.497 ms | 1.637 ms |
| 250 | 2,500 | 17.509 ms | 2.263 ms |

Increasing both pool count and total node-set count by 25 times increased the
current-schema traversal 216 times. The candidate index reduced that to 16.9
times. This confirms the predicted `O(P*S)` interaction in the nested metrics
collector; with a fixed number of retained sets per pool, that becomes
quadratic in pool count.

### Query-plan evidence and conclusion

At 5,000 node sets and 5,000 nodes, `EXPLAIN (ANALYZE, BUFFERS)` showed:

- the node-set lookup sequentially scanned all 5,000 node sets, discarded
  4,950, and returned the 50 belonging to one pool;
- the node lookup sequentially scanned all 5,000 nodes, discarded 4,999, and
  returned the one belonging to a selected node set;
- after adding the candidate indexes, PostgreSQL selected a bitmap index scan
  for `node_sets.node_pool_id` and an index scan for `nodes.node_set_id`.

The conclusion is satisfactory for the narrow question: the current schema has
two confirmed multiplicative scan paths, and the absent foreign-key indexes are
the cause in this controlled fixture. It does not yet prove the end-to-end
impact under concurrent reconciliation and Prometheus scraping. Before merging
an index migration, repeat the dense cases through the real repositories and
complete worker, measure index size and write amplification, and use production-
like cardinalities and PostgreSQL settings.

## 2026-09-18: ten tenant clusters and 100,000 nodes

### Status and method

These measurements target the large-node, low-cluster shape: ten balanced
tenant clusters with up to 100,000 total nodes. They used the same workstation
and temporary PostgreSQL environment described above.

The new `node-source` harness lane starts the production `NodeEventSource`,
drains its real controller-runtime rate-limiting queue, and invokes the same
resync method used by its periodic 15-minute timer. It creates one
cluster-filtered source per tenant cluster, matching the tenant node-status
controller, and limits concurrent source scans to two. Fixture creation is
timed separately and excluded.

The full tenant lane then starts ten real tenant managers against the virtual
tenant APIs and waits for initial Node reconciliation and controller
quiescence. The 100,000-node endpoint was repeated three times.

### DB node-source startup and resync

| Nodes | Rows loaded per pass | Source startup | Forced resync | Startup allocation | Resync allocation |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 100,000 | 0.564 s | 0.560 s | 0.809 GB | 0.807 GB |
| 25,000 | 250,000 | 1.400 s | 1.351 s | 2.169 GB | 2.164 GB |
| 50,000 | 500,000 | 2.768 s | 2.601 s | 4.415 GB | 4.405 GB |
| 100,000 | 1,000,000 | 5.373 s | 5.517 s | 8.857 GB | 8.836 GB |

The 10,000- and 100,000-node values are medians of three runs; the intermediate
points are single runs. Allocation is cumulative allocation during the phase,
not retained heap.

Duration remains approximately linear at fixed cluster count: increasing node
count ten times increased startup 9.5 times and resync 9.8 times. The important
finding is amplification. Every cluster-filtered source executes
`ListNodes` without database filters, receives the complete fleet, and only
then applies its cluster filter in memory. Ten sources therefore load one
million full node rows to emit 100,000 events. The 100,000-node resync allocated
8.84 GB transiently, ran 49-53 garbage collections, and retained roughly
416-543 MB more heap at phase completion across the three runs. There were no
database-pool waits with source concurrency two.

This experiment models only one tenant-style node source per cluster. The full
worker has additional global sources, and tenant node remediation adds another
per-cluster source when registered. The measured row and allocation volume is
therefore a lower bound for a complete-worker resync.

### Full tenant-manager startup

At 100,000 nodes, three full startup repetitions produced:

| Measure | Result |
|---|---:|
| Tenant-manager/cache-ready median | 156 ms |
| Initial reconciliation median | 35.156 s |
| Reconciles per run | 200,000 |
| Database queries per run | 1,000,233-1,000,237 |
| Post-convergence heap median | 203 MB |
| Cumulative process allocation median | 34.06 GB |
| Steady goroutines | 544 |
| Active tenant watches | 20 |

The cache-ready phase is not the operational recovery time: DB-source seeding
and Node convergence continue afterward. The relevant restart figure in this
fixture is approximately 35 seconds. The stable 200,000 reconcile count shows
that each node entered the controller queues twice during initial convergence,
through the Kubernetes informer and DB-backed source paths. The roughly ten
database queries per node make query volume, allocation rate, and garbage
collection more important at this fleet shape than manager or watch count.

### Conclusion and next change to test

At ten clusters, startup and periodic resync are near-linear in node count, but
the current per-source full-fleet load creates a large `clusters * nodes`
amplification and substantial transient allocation. The first optimization to
benchmark should push each source's supported cluster and condition filters
into `ListNodes`, so a tenant source receives approximately 10,000 rows instead
of 100,000. Compare that with a shared fleet-wide source that reads once and
fans out in memory. The same 10k-100k matrix must be rerun after either change,
including a complete-worker restart and a resync while API and lifecycle work
continue.

### Unmeasured high-priority hypotheses

The following code paths remain hypotheses until a targeted benchmark measures
them:

1. **Database-backed Prometheus collection.** A scrape performs five base
   database queries plus one node-pool query per cluster and one node-set query
   per node pool. At 100 clusters and 1,000 pools, the info-metric traversal
   alone implies 1,105 queries per scrape before considering emitted series and
   serialization. Collection is synchronous and has a 10-second timeout.
2. **Unpaginated list APIs.** Cluster, node-pool, node-set, and node list paths
   load and serialize complete result sets. Node listing also computes rollout
   information for the returned set.
3. **Outbox transaction duration.** A batch holds a database transaction and row
   locks while it starts workflows serially in Temporal. Slow Temporal calls can
   therefore consume a database connection and extend lock duration.
4. **Complete-process interference.** REST, Temporal work, reconciliation,
   tenant managers, metric collection, and outbox delivery share the worker
   process. Component results do not show whether one plane starves another.

### Next experiments

1. Push DB node-source filters into `ListNodes` and rerun the 10k, 25k, 50k,
   and 100k startup/resync matrix; compare with a single-read shared fan-out
   design.
2. Validate indexes on `node_sets(node_pool_id)` and `nodes(node_set_id)` through
   the real repositories and complete worker under concurrent reconciliation,
   scraping, and mutations; capture query counts, plans, index size, and write
   cost.
3. Add an inventory-complete metrics-scrape lane with clusters, pools, node
   sets, and nodes; record query count, series count, bytes, duration, heap, and
   impact on concurrent API reads.
4. Repeat the cluster-axis tenant-manager test on controlled Linux resources
   with CPU/heap/goroutine profiles, file-descriptor counts, and realistic
   tenant API latency.
5. Run readiness sweeps with representative latency and failure distributions
   at worker counts 1, 2, 5, 10, and 20; monitor database and connection limits
   and mixed-controller latency.
6. Add API read workloads for list endpoints at 10,000, 25,000, 50,000, and
   100,000 nodes.
7. Exercise the complete worker with Temporal and a management Kubernetes
   cluster before moving to CAPD lifecycle tests.
