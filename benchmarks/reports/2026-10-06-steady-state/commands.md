# Reproduction commands

Run from the repository root. Use disposable dependencies and dedicated databases:
the harness replaces deterministic fixture rows and resets the virtual tenant API.
No external credentials are required. `example-password` below is a disposable
fixture value. The worker runs as a host process without a pod resource limit.

## Dependencies and binaries

```bash
make -C controller perf-build
make -C controller build
cp controller/bin/cwe-perf /tmp/nke-steady-perf
cp controller/bin/controller /tmp/nke-steady-worker

docker run --rm --name nke-steady-postgres \
  -p 127.0.0.1:15432:5432 \
  -e POSTGRES_PASSWORD=example-password postgres:17.4
```

In another terminal:

```bash
docker exec nke-steady-postgres psql -U postgres \
  -c 'CREATE DATABASE steady_worker'
kind create cluster --name nke-steady --image kindest/node:v1.37.0 \
  --kubeconfig /tmp/nke-steady-kubeconfig
kubectl --kubeconfig /tmp/nke-steady-kubeconfig apply -f \
  "$(go env GOMODCACHE)/sigs.k8s.io/cluster-api@v1.13.6/config/crd/bases/cluster.x-k8s.io_clusters.yaml"
temporal server start-dev --ip 127.0.0.1 --port 17233 --ui-port 18233 \
  --db-filename /tmp/nke-steady-temporal.db
```

The Cluster CRD supplies the tenant-access informer. The Machine CRD and SPIRE
controller are omitted. Versions and frozen binary SHA-256 digests for the
recorded run are in `environment.json`. Use a new Temporal database for independent
runs so pending workflows from a prior run cannot attach to reused fixture IDs.

## Component experiments

The component suite owns port 18080. The completed source, scrape and tenant-event
matrix used five repetitions; the agent matrix uses one repetition with two
unchanged sweeps per case. Every elapsed-time comparison is qualified because
runs and compilation overlap on this workstation.

```bash
python3 -m benchmarks.cwe_perf.steady_state \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --harness /tmp/nke-steady-perf \
  --db-dsn 'postgres://postgres:example-password@127.0.0.1:15432/postgres?sslmode=disable' \
  --results-dir /tmp/nke-components-new --repetitions 5 \
  --case source-c2 --case source-c10 --case source-c100 \
  --case scrapes-c1 --case scrapes-c2 --case scrapes-c4 --case tenant-events
```

Run the agent cases with `--repetitions 1` and select these names:
`agent-delay-0ms-c1`, `agent-delay-10ms-c1`, `agent-delay-25ms-c1`,
`agent-delay-100ms-c1`, `agent-delay-100ms-c2`, `agent-delay-100ms-c4`,
`agent-delay-100ms-c8`, `agent-slow-cohort`, `agent-failed-cohort`,
`agent-timeout-cohort`. The separate `agent-default-timeout` case uses one slow
node per 5,000 and the production two-minute deadline. The larger timeout cohort
uses a shortened two-second deadline.

## Mixed full-worker soak

Start this virtual API in a separate terminal. The complete-worker lane resets
its objects and counters before each run. Use the same scenario for the server
and inventory.

```bash
python3 -m benchmarks.cwe_perf materialize \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --server http://127.0.0.1:18081 --output /tmp/nke-worker-inventory.json
python3 -m benchmarks.cwe_perf serve-tenant-api \
  --scenario benchmarks/scenarios/clusters-100-nodes-5000.json \
  --listen 127.0.0.1:18081
```

The scenario intentionally includes 50 Kubernetes-not-ready Nodes. The fixture
uses one worker pool/set per cluster and, for certificate simulation, one
control-plane Node in a separate legacy pool/set. Node provisioned/joined status
is seeded; tenant kubeconfigs are supplied directly through Secrets. Cluster
bootstrap, physical provisioning, and real node commands are not exercised.

```bash
SERVER_LISTEN_ADDRESS=127.0.0.1:18800 \
METRICS_LISTEN_ADDRESS=127.0.0.1:18900 \
/tmp/nke-steady-perf black-box \
  -inventory /tmp/nke-worker-inventory.json -binary /tmp/nke-steady-worker \
  -management-kubeconfig /tmp/nke-steady-kubeconfig \
  -db-dsn 'postgres://postgres:example-password@127.0.0.1:15432/steady_worker?sslmode=disable' \
  -temporal-host-port 127.0.0.1:17233 \
  -ready-url http://127.0.0.1:18800/readyz \
  -metrics-url http://127.0.0.1:18900/metrics \
  -duration 2h -sample-interval 15s -agent-delay 10ms \
  -node-event-interval 30s -serving-csrs -cert-renewal-interval 5m \
  -output /tmp/nke-worker-soak-new.json
```

A separate recovery run uses the same command with `-duration 20m`,
`-agent-outage-after 3m -agent-outage-duration 2m`,
`-tenant-outage-after 7m -tenant-outage-duration 1m`, and `-restart-after 10m`.
It follows the soak; do not run both workers against the same database,
management lease, tenant simulator, or Temporal task queue simultaneously.
The shorter 10-cluster/100-node smoke used 3 minutes with agent outage at 20s
for 30s, tenant outage at 50s for 20s, restart at 90s, events every 10s and
certificate checks every 30s.

```bash
python3 -m benchmarks.cwe_perf.observations \
  --samples /tmp/nke-worker-soak-new.json.samples.jsonl \
  --output /tmp/nke-worker-soak-new-summary.json
```

Inspect raw errors, checks, signal coverage, per-generation trends and scenario
readiness counts. Missing controller-runtime/queue and worker DB-counter metrics
are explicitly unavailable evidence. A graceful restart is not a lost-lease or
multi-replica failover test. Findings remain local; do not create GitHub issues.
