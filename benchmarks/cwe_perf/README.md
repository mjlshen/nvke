# CWE performance harness

This directory contains the low-compute, deterministic dependencies and
scenario driver for the CWE performance harness. The production-path Go driver
lives in `controller/benchmarks/`, inside the controller Go module so it can
exercise internal reconcilers without exporting benchmark-only APIs. None of
the harness is linked into the released worker binary.

The harness models tenant-cluster count and total node count independently. A
single virtual Kubernetes API process multiplexes all tenant clusters by bearer
token, so a 100-cluster/5,000-node test does not require 100 API servers or any
etcd processes.

The checked-in balanced scenarios cover the 3×3 matrix of 10/50/100 clusters
and 100/1,000/5,000 total nodes, plus a skewed 100-cluster/5,000-node case. Use
rows or columns of that matrix to change one scaling dimension at a time.

## Current commands

Materialize a deterministic inventory:

```bash
python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/smoke.json \
  --output /tmp/cwe-perf-inventory.json
```

Run the virtual tenant API:

```bash
python3 -m benchmarks.cwe_perf serve-tenant-api \
  --scenario benchmarks/scenarios/smoke.json \
  --listen 127.0.0.1:18080
```

Use `--latency-ms` for a fixed dependency delay and `--failure-every N` to
return a deterministic `503` on every Nth request per tenant. The node-agent
lane provides the corresponding `-agent-delay` and `-agent-failure-every`
controls.

Each generated kubeconfig selects its tenant through the URL prefix
`/clusters/<zero-based index>`. A bearer token is also generated, but the URL
prefix is authoritative because client-go does not transmit credentials over
plain HTTP. The server exposes Kubernetes discovery, Node list/watch/get/patch,
and CSR list/watch/get/approval calls used by CWE's tenant managers.
CSR approval accepts JSON and the Kubernetes protobuf status subset emitted by
controller-runtime; original fixture metadata/spec are preserved for protobuf
updates. Harness-only
endpoints are under `/__admin/`:

- `GET /__admin/stats` returns bounded request/watch counters.
- `POST /__admin/reset` restores fixture objects and counters and drops watches.
  The complete-worker lane calls this before every run.
- `POST /__admin/node-event` changes one virtual Node and broadcasts a
  `MODIFIED` watch event. The JSON body is
    `{"cluster":0,"node":"cluster-0000-node-00000","ready":false}`.
- `POST /__admin/csr-event` adds a deterministic CSR and broadcasts an `ADDED`
  event. This is useful for focused CSR approval tests.

Build the production-path driver with the repository-pinned Go toolchain:

```bash
make -C controller perf-build
cp controller/bin/cwe-perf /tmp/cwe-perf
```

The driver has five lanes:

- `tenant` starts the real `tenant.ManagerSet`, CSR controller, Node health
  controller, controller-runtime caches, queues, and PostgreSQL repositories.
- `agent-health` runs the real node-agent readiness reconciler and gRPC client
  against one shared deterministic agent endpoint.
- `node-source` measures startup seeding and forced resync of the real DB-backed
  node source. It uses the same per-cluster in-memory filter as tenant node
  controllers and drains real controller-runtime queues.
- `prometheus` materializes clusters, node pools, retained node sets, and nodes,
  then measures complete Prometheus text scrapes from the production DB-backed
  collector. It reports query count, series count, response bytes, allocation,
  database-pool waits, per-scrape latency, and database time split by collector
  query shape.
- `black-box` starts the complete CWE worker binary against a supplied
  PostgreSQL, Temporal frontend, and one real management Kubernetes cluster.

The component lanes need one PostgreSQL process. A disposable local instance
is sufficient; use a dedicated database because each run replaces rows with
the deterministic fixture IDs:

```bash
docker run --rm --name cwe-perf-postgres \
  -p 5432:5432 -e POSTGRES_PASSWORD=example-password postgres:17.4
```

In another terminal, start the simulator and run a smoke test:

```bash
python3 -m benchmarks.cwe_perf serve-tenant-api \
  --scenario benchmarks/scenarios/smoke.json --listen 127.0.0.1:18080

python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/smoke.json \
  --output /tmp/cwe-perf-inventory.json

/tmp/cwe-perf tenant -inventory /tmp/cwe-perf-inventory.json \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  -profiles-dir controller/test/fixtures/profiles \
  -events 10 -output /tmp/cwe-perf-tenant.json

/tmp/cwe-perf agent-health -inventory /tmp/cwe-perf-inventory.json \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  -profiles-dir controller/test/fixtures/profiles \
  -concurrency 1 -output /tmp/cwe-perf-agent.json
```

The agent-health lane reports the initial condition-changing sweep separately
from unchanged steady-state sweeps. Use `-steady-sweeps N` to control how many
steady sweeps follow the transition sweep; the default is one. This distinction
matters because a transition takes the database update path while an unchanged
probe returns after its initial read.

`-slow-node-every N -slow-node-delay 2s` delays a persistent cohort of nodes;
`-failed-node-every N` fails a persistent cohort. The component driver sends
harness-only gRPC metadata so cohorts stay attached to node identity across
sweeps. `-timeout` remains the per-probe deadline. Results include throughput,
whether a sweep fits within two minutes, distinct probed nodes, maximum observed
probe gap, and configured workload controls. These are direct sweeps, not the
production work queue or two-minute requeue schedule, and use plaintext gRPC.

The node-source lane triggers the same path as the periodic 15-minute resync
without waiting for the timer. This example runs ten tenant-style sources over
a 100,000-node fleet with at most two simultaneous full-table reads:

```bash
python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/balanced-c010-n100000.json \
  --output /tmp/cwe-perf-inventory-100k.json

/tmp/cwe-perf node-source \
  -inventory /tmp/cwe-perf-inventory-100k.json \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  -profiles-dir controller/test/fixtures/profiles \
  -source-concurrency 2 -copies-per-cluster 1 -resyncs 1 \
  -output /tmp/cwe-perf-node-source-100k.json
```

`estimatedRowsLoaded` is intentionally much larger than the emitted event
count: each source currently loads every node row and applies its cluster filter
in memory. Increase `-copies-per-cluster` to model multiple tenant controllers
that each own an independent source.

The Prometheus lane adds topology that the base inventory does not contain.
This example creates 100 clusters, 10 pools per cluster, 10 retained node sets
per pool, and uses the scenario's 5,000 nodes. One warm-up scrape is excluded
from the measured database, allocation, and latency counters:

```bash
python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --output /tmp/cwe-perf-inventory.json

/tmp/cwe-perf prometheus \
  -inventory /tmp/cwe-perf-inventory.json \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  -profiles-dir controller/test/fixtures/profiles \
  -node-pools-per-cluster 10 -node-sets-per-pool 10 \
  -warmup-scrapes 1 -scrapes 5 -concurrency 1 \
  -output /tmp/cwe-perf-prometheus.json
```

Use `-concurrency 2` or higher to model overlapping scrapes. The result records
an `incompleteScrapes` count by comparing the serialized sample count with the
complete fixture's expected series count; this exposes partial results caused
by the collector's internal timeout without turning an intentionally stressful
run into a harness failure. `queryProfile` separates the nested cluster,
node-pool, and node-set list queries from the fleet-wide node-condition,
node-info, pool-aggregate, and lifecycle queries so serialization cost cannot
hide a super-linear database path.

`-scrape-interval 15s` adds idle time between scrapes **per scrape worker**.
Zero retains the original back-to-back behavior. Concurrency models independent
scrapers; the configured interval is completion-to-next-start, not a wall-clock
Prometheus cadence. Phase `-timeout` must cover all measured scrapes and waits.

## Steady-state experiment suite

The suite runs readiness latency/concurrency/cohort cases, two node sources per
cluster, dense repeated scrapes, and tenant Node events. Dependencies remain
explicit. Use a dedicated database and a **new** results directory; prior
evidence is never overwritten. The suite owns port 18080 and snapshots the
driver binary before executing, so rebuilding it cannot change a running batch.
Inventory/kubeconfig fixtures are temporary and are not saved with results.

```bash
python3 -m benchmarks.cwe_perf.steady_state \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --harness controller/bin/cwe-perf \
  --db-dsn "$BENCHMARK_DB_DSN" \
  --results-dir /tmp/nke-steady-state-run --repetitions 5
```

Use repeatable `--case` arguments to select cases, for example
`--case agent-delay-25ms-c1 --case agent-slow-cohort`, or
`--case source-c2 --case source-c10 --case source-c100`. `agent-default-timeout` uses the production two-minute deadline with one slow
node per 5,000; the broader `agent-timeout-cohort` uses a shortened two-second
deadline to keep a 1% timeout-cohort run practical. The full suite includes
100-ms serial probes of 5,000 nodes; several hours are expected at five
repetitions. `--tenant-latency-ms` and `--tenant-failure-every` control virtual
tenant dependencies. Each case retains raw JSON and the suite updates
`summary.json` after every completed case. Failed cases preserve partial results
and stop the batch; failed guardrails are evidence, not successful capacity runs.

For five comparable repetitions, let the matrix driver own the tenant API and
write raw results plus a median/p95 summary. It performs one unmeasured warm-up
per scenario by default; set `--warmups 0` to disable it:

```bash
python3 -m benchmarks.cwe_perf run-matrix \
  --scenario benchmarks/scenarios/smoke.json \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --harness /tmp/cwe-perf --mode tenant --repetitions 5 \
  --db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  --results-dir /tmp/cwe-perf-results --harness-arg=-events=10
```

## Complete-worker lane

The black-box lane intentionally does not hide dependency setup: pin the
PostgreSQL, Temporal, and Kubernetes versions as experiment inputs. Start a
Temporal development server on `127.0.0.1:7233`, create a small Kind cluster,
and build the normal worker with `make build`. Then run:

```bash
/tmp/cwe-perf black-box -inventory /tmp/cwe-perf-inventory.json \
  -binary controller/bin/controller \
  -management-kubeconfig "$KUBECONFIG" \
  -temporal-host-port 127.0.0.1:7233 \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable' \
  -duration 30s -output /tmp/cwe-perf-black-box.json
```

The worker fixture includes a current worker pool/set per cluster and joined,
provisioned Nodes. With certificate simulation enabled, one Node per cluster
moves into a separate legacy control-plane pool/set. Pool/set desired counts
match the seeded inventory.

This lane creates or updates deterministic tenant kubeconfig Secrets in
`nke-system`; use a disposable management cluster. It launches the released
worker unchanged, waits for `/readyz`, samples its protected Prometheus
endpoint, and stops it gracefully. The shared tenant API and node-agent
simulators still replace 100 API servers and 5,000 agent processes.

The measurement now waits for initial agent and tenant-node convergence, then
writes and flushes periodic observations to `-samples` (default:
`<output>.samples.jsonl`). An existing sample file is refused. Each observation
contains worker CPU/RSS/heap/GC, **labeled** controller queues when exposed, total scrape
samples, inventory metric sample counts, observed DB readiness counts, list-API
status/latency, and simulator counters. The result's `runtime` section belongs
to the driver; worker runtime belongs to the sample stream. A final observation
and explicit recovery/completeness checks are saved after restoring injected
Node health changes. Inspect those checks and sample errors even when the
process exits successfully.

Supported complete-worker controls:

| Flag | Purpose |
|---|---|
| `-duration 2h -sample-interval 15s` | Capture repeated real requeue/resync cycles and resource trends |
| `-agent-delay 25ms -agent-failure-every 100` | Inject delay / every-Nth-request agent failure |
| `-agent-outage-after 5m -agent-outage-duration 2m` | Timed agent outage and recovery |
| `-tenant-outage-after 10m -tenant-outage-duration 1m` | Drop tenant watches, return 503s, then permit recovery |
| `-node-event-interval 30s` | Alternate health changes/restoration on fixture Nodes |
| `-serving-csrs` | Submit one valid kubelet-serving CSR per cluster and check approval |
| `-restart-after 20m` | Restart once and wait for a new full-fleet probe sweep |
| `-cert-renewal-interval 5m` | Seed one CP node per cluster and simulate periodic unchanged-certificate workflows |

The virtual API exposes runtime faults through `POST /__admin/faults`, with
`{"outageClusters":[0],"dropWatches":true}`; clear with
`{"outageClusters":[]}`. Stats expose CSR approval counts and active outages.
Restart/outage flags must leave time to observe recovery. Tenant outage and
Node-event injection run at observation opportunities; agent outage uses timers.

This lane uses a real management Kubernetes API/leader lease and Temporal, but
agent identity is **aggregate only**: the unchanged worker does not send the
component driver's metadata. It cannot prove per-node probe freshness from RPC
counts. Certificate simulation validates scheduling and workflow delivery,
returns `OUTCOME_UNCHANGED`, and executes no certificate or static-pod commands.
It disables NICo/SPIFFE/authentication and direct control-plane provisioning
for these disposable legacy-pool fixtures and uses
`nke-system` for the lease. Real SPIFFE cost, actual certificate rotation,
multi-replica failover, and production dependency latency still need real-stack
validation. A deliberate graceful restart does not reproduce lost-lease failure.

Summarize resource and queue trends while keeping restarted process generations
separate:

```bash
python3 -m benchmarks.cwe_perf.observations \
  --samples /tmp/cwe-perf-black-box.json.samples.jsonl \
  --output /tmp/cwe-perf-worker-trends.json
```

The summary records signal availability, labeled queue peaks/final depths, resource ranges and
quarter medians, linear memory trends, API latency, and per-generation counter
deltas. Missing queue/reconcile metrics are unavailable evidence, not zero
backlog. At the tested revision the full-worker endpoint does not expose the
controller-runtime registry; see the local finding. A positive memory trend
alone does not establish a leak. On macOS,
where the worker's Prometheus process collector omits RSS, the driver samples
`ps` for the spawned worker PID and marks that fallback in the stream.

Findings and completed raw runs are recorded locally under
[`reports/2026-10-06-steady-state/`](../reports/2026-10-06-steady-state/README.md).
Do not create GitHub issues for these experiments.

## Result contract

Every Go lane writes schema-versioned JSON containing scenario and build
metadata, separately timed setup/workload phases, errors, Go process metrics,
database query/pool counters, and simulator request/failure/watch counters.
Fixture seeding is deliberately separate from measured cold start. Failed runs
also write their partial result before returning a non-zero status.

The tenant lane does not inject measured events until every fixture Node has
been reconciled and the controller-runtime work queues have remained idle for
three consecutive samples. It also waits for queues to drain after the event
phase, preventing startup work or process shutdown from contaminating the
result.

## Reproducibility

Scenarios contain an explicit seed, but object identity and balanced placement
are deterministic even when no randomness is requested. Comparable runs must
pin the scenario file, CWE revision, dependency versions, CPU/memory limits,
and `GOMAXPROCS`. Reset persistence between runs and distinguish cold-start
from warm-cache measurements.

For tight comparisons, run on an otherwise idle host, fix `GOMAXPROCS`, pin the
same container/runtime versions, keep agent delay/error flags identical, and
alternate baseline and candidate runs. Five repetitions are the default; do
not turn a single noisy timing into a CI threshold.

The virtual API is a protocol/load simulator, not a Kubernetes conformance
implementation. A small real CAPI/Kind lane remains necessary to validate that
its request surface matches a supported Kubernetes API server.
