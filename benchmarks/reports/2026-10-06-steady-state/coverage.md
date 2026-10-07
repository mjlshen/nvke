# Steady-state experiment coverage

This assessment distinguishes completed component measurements, full-worker evidence, and operations the fixtures cannot validate. Findings remain local; production remediation is outside this change.

| Operation or concern | Harness and evidence | Status / limits |
|---|---|---|
| Periodic agent readiness probes | Ten latency/concurrency/persistent-cohort cases under `agents/`; separate `agent-default-timeout.json` | Complete. Direct production-reconciler sweeps; no production queue/timer or SPIFFE cost. One repetition per agent setting. |
| Tenant Node-health and remediation source resync | `components/source-c2`, `source-c10`, `source-c100` | Five repetitions per setting, two sources per cluster, three resyncs. Source events drained without downstream reconciliation. Modeled fetched rows distinguish estimates from measured SQL queries. |
| Repeated database-backed metrics collection | `components/scrapes-c1`, `scrapes-c2`, `scrapes-c4` | Five repetitions; twelve measured scrapes per run; paced workers. Dense fixture differs from full-worker fixture. Allocation differs from retained memory. |
| Tenant Node watch updates | `components/tenant-events` | Five repetitions. 99 actual updates for 100 requested because one selected Node was already not ready. |
| Full-worker repeated requeues, resyncs, API reads and metrics | Two-hour `worker-soak-5000` run | Complete: 7,200.46-second measurement, 468 error-free observations, all nine final checks passed. |
| Serving CSR approvals | Full-worker smoke and soak | Smoke, completed soak and recovery passed. Valid ephemeral CSRs and real approval controller; simulator receives Kubernetes protobuf approvals. Real certificate issuance/installation omitted. |
| Periodic control-plane certificate workflows | Full-worker smoke and soak | Smoke, completed soak and recovery passed. Temporal and scheduling exercised, simulated operations return unchanged; real rotation/static-pod commands omitted. |
| Agent and tenant dependency outages | `worker-smoke-02`, then 20-minute `worker-recovery-5000` | Small smoke and completed large case passed all final checks; injected faults appear in raw observations. |
| Graceful worker restart | Same recovery runs | Small smoke and completed large case passed; large restart converged in 94.625 seconds. Aggregate probes establish activity, not per-node freshness or lost-lease/multi-replica failover. |
| CPU, RSS, heap, GC and goroutine trends | Per-generation `*.samples.jsonl` and observation summaries | Available. Host-process fixtures and shared workstation qualify capacity claims. RSS uses a marked macOS ps fallback. |
| Controller queue/reconcile and worker database pool trends | Labeled metric parser; `controller-metrics-unavailable.md` | Unavailable in unchanged worker endpoint. Missing signals cannot establish idle queues or absence of pool contention. |
| Outbox provisioning delivery / real lifecycle mutations | Existing broader harness and future real-stack tests | Not validated by this steady-state fixture. Certificate scheduling uses Temporal but does not prove provisioning outbox behavior. |
| Production maximum capacity | Controlled Linux resource-limited and longer real-stack runs | Not established by these local experiments. |

Harness checks passed: controller formatting, license check, lint, full module tests, benchmark race tests, and 23 Python tests. Functional full-worker smokes and final node-source smoke are retained. See `commands.md` and `environment.json` for reproduction and frozen binaries.
