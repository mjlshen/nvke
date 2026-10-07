# Steady-state experiment records — 2026-10-06

The unchanged worker completed a two-hour mixed workload at 100 clusters and 5,000 nodes, passing all nine final checks with 468 error-free observations. This supports successful steady-state operation in the tested local fixture. Production freshness, bounded queues, and maximum capacity remain unproven. Findings are recorded locally; no production remediation is included.

## Findings

- [Serial agent-probe cadence](agent-probe-cadence.md): at the default single worker, 25-ms probes required 147–152 seconds for unchanged fleet sweeps. One node using the actual two-minute timeout also pushed unchanged sweeps beyond two minutes.
- [Tenant node-source allocation](node-source-allocation.md): two independently scanning sources per cluster modeled one million fetched rows per resync at this fleet size and allocated approximately 7.2 GB per pass.
- [Dense metrics scrape allocation](metrics-scrape-allocation.md): approximately 250.6 MB allocated per dense scrape. Allocation is not retained memory; this denser inventory differs from the worker fixture.
- [Unavailable full-worker controller metrics](controller-metrics-unavailable.md): queue, reconciliation, and worker database-pool signals were absent from the unchanged worker endpoint. Missing metrics cannot support an idle-queue conclusion.
- [Completed large-fleet outage/restart result](worker-recovery.md)
- [Completed two-hour worker result](worker-steady-state.md): all inventory/recovery checks passed, goroutines stayed between 6,934 and 6,953, median RSS was about 399 MiB, and average CPU use was 0.225 cores. Small positive fitted memory trends do not establish a leak or long-term boundedness.

## Evidence and reproduction

- [Experiment coverage and limits](coverage.md)
- [Environment and frozen binary digests](environment.json)
- [Reproduction commands and isolation requirements](commands.md)
- [Component suite summary](components/summary.json): five repetitions each of three source-concurrency settings, three scrape-concurrency settings and tenant Node events; 35 runs total. Tenant-event cases produced 99 actual updates for 100 requested.
- [Verified agent summaries](agents/summary-verified.json): ten completed cases, one repetition each and two unchanged sweeps after transition; all raw results retained under `agents/`.
- [Production probe-timeout case](agent-default-timeout.json)
- [Two-hour worker raw result](worker-soak-5000.json), [observation stream](worker-soak-5000.json.samples.jsonl), and [summary](worker-soak-5000-summary.json)

The 10-cluster/100-node recovery smoke (`worker-smoke-02.json`) passed all nine checks and delivered 60 simulated certificate operations. The final joined/provisioned fixture smoke passed all nine checks with RSS sampling and 40 operations. The final source smoke produced 20 SQL queries and 200 consumed events on startup and each of three resync passes. These short runs establish harness functionality.

[Validation logs](validation/README.md) confirm controller formatting/license/lint, full module tests, benchmark race tests, and 23 Python simulator/summary/shutdown tests passed. Harness changes are confined to benchmark code, documentation and the benchmark build target.

## Large-fleet recovery run

The separate 20-minute recovery experiment completed after the soak and passed all nine checks with 74 error-free observations. It recorded a two-minute agent outage, a one-minute outage across all tenant APIs, and graceful worker restart. Agent readiness recovered and restart convergence took 94.6 seconds. See [the recovery result and its limits](worker-recovery.md), [raw result](worker-recovery-5000.json), [observations](worker-recovery-5000.json.samples.jsonl), and [summary](worker-recovery-5000-summary.json).

## Limits and local-record policy

The worker used a host process without a pod CPU/memory limit, local PostgreSQL and Temporal development services, one real Kind management cluster, and shared virtual tenant/agent servers. The fixture disables NICo, SPIFFE, API authentication and direct CP provisioning; it uses legacy CP pools and omits the Machine CRD/SPIRE controller. Real node commands, certificate rotation, multi-replica failover and production network/storage latency remain untested. Full-worker RPC identity is aggregate only, so counts cannot prove individual-node freshness. The component and worker experiments overlapped on this workstation, qualifying elapsed-time/resource capacity claims.

Historical `worker-checkpoint.json` and `agent-summary-checkpoint.json` files are explicitly incomplete checkpoints, superseded by completed results. Harness-development runs in `/tmp/nke-steady-results/` are excluded from successful capacity evidence: the original readiness-only smoke never acquired leadership; `worker-smoke-01` lacked protobuf approval support and effective CP classification; an early fixture run reused approval counters before reset was added.

GitHub issues #1287 and #1288 were mistakenly opened and are closed. Do not open GitHub issues for these experiments; retain findings here.
