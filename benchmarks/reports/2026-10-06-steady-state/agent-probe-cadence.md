# Serial agent-probe cadence at 5,000 nodes

The default readiness controller has one worker and requeues each node after two minutes. At 25 ms of RPC delay, 5,000 serial probes require at least 125 seconds of RPC time alone. A complete fleet sweep therefore cannot fit inside a two-minute window before accounting for connection setup, scheduling, or database reads. This is a steady-state cadence concern, not evidence of process failure or an unbounded queue.

One completed repetition at 100 clusters / 5,000 nodes, with two unchanged sweeps after the initial condition-changing sweep:

| Simulated RPC delay | Transition sweep | Steady sweep 1 | Steady sweep 2 |
|---|---:|---:|---:|
| 0 ms | 22.11 s | 4.91 s | 4.28 s |
| 10 ms | 79.31 s | 69.87 s | 69.66 s |
| 25 ms | 160.43 s | 151.77 s | 147.08 s |

All 5,000 node identities were observed; each run completed 15,000 RPCs without simulated failures. Unchanged sweeps performed 5,000 DB queries; the transition sweep performed 30,000.

The component lane calls the production reconciler directly with plaintext gRPC. It does not exercise the production work queue or two-minute requeue timer. Maximum probe gaps include the transition phase, so they are not an isolated steady-state percentile. Compilation overlapped parts of these runs; elapsed measurements are preliminary single-run evidence. The 125-second RPC-only lower bound is independent of that interference. The concurrency and persistent cohort comparisons below extend this measurement. Real SPIFFE source creation and network delay are omitted.

Raw JSON is under `agents/`. Reproduce with the suite's `agent-delay-0ms-c1`, `agent-delay-10ms-c1`, and `agent-delay-25ms-c1` cases. Record findings locally; do not open GitHub issues. No production remediation has been made.

## Production timeout case

A separate completed run used one persistent slow node per 5,000, a simulated
3-minute RPC delay for that node, and the actual 2-minute probe deadline. It
completed the initial sweep and two unchanged sweeps. Raw evidence is in
`agent-default-timeout.json`.

- node-agent-health-sweep: 131.96 seconds; fits a two-minute fleet sweep: False.
- node-agent-health-steady-sweep-01: 124.47 seconds; fits a two-minute fleet sweep: False.
- node-agent-health-steady-sweep-02: 125.93 seconds; fits a two-minute fleet sweep: False.

The run observed 5,000 distinct nodes, 15,000 RPCs, and 3 simulated RPC timeouts. Only one selected node per sweep consumed the full deadline. Unchanged sweeps require a normal DB read per node, with extra transaction/update work on timeout transitions. This directly illustrates that one default-duration timeout consumes essentially the entire two-minute serial-probe budget. The run uses direct sweeps rather than the production queue/timer, and ran concurrently with other experiments; it does not establish a production freshness SLO.

## Concurrency comparison at 100 ms RPC delay

These are direct harness sweeps, not changes to production controller settings.
One repetition per setting; two unchanged sweeps follow the transition sweep.

| Workers | Transition sweep | Steady sweep 1 | Steady sweep 2 |
|---:|---:|---:|---:|
| 1 | 547.31 s | 521.94 s | 523.26 s |
| 2 | 272.65 s | 262.42 s | 263.55 s |
| 4 | 138.62 s | 132.75 s | 133.41 s |
| 8 | 69.59 s | 67.89 s | 66.92 s |

Four workers still exceed the two-minute fleet-sweep window; eight workers fit inside it in this synthetic workload. All cases ran concurrently with the full-worker soak, so elapsed times characterize this local test rather than isolated capacity. No production concurrency setting was changed.

## Persistent slow and failed cohorts

Both cases selected the same 50 nodes on each of three sweeps (one per 100 nodes), using node identity metadata rather than request ordering. Each case observed all 5,000 identities and completed 15,000 RPCs.

| Cohort | Transition | Steady 1 | Steady 2 | Simulated outcome |
|---|---:|---:|---:|---|
| 50 nodes delayed 2 seconds | 117.98 s | 109.36 s | 109.49 s | 150 slow RPCs; no timeouts |
| 50 nodes immediately unavailable | 11.04 s | 4.01 s | 4.23 s | 150 failed RPCs |

Both fit inside the two-minute direct-sweep window. Immediate failures are inexpensive in this fixture; they do not establish the cost of network timeouts or downstream remediation. The same failures persisted across unchanged sweeps, which performed 5,000 database queries each without database errors. Single repetitions ran alongside the full-worker soak. Raw files are `agents/agent-slow-cohort/run-01.json` and `agents/agent-failed-cohort/run-01.json`.

## Accelerated timeout cohort

Fifty persistent slow nodes used a 3-second simulated delay and a 2-second probe deadline. This shortened deadline exercises repeated timeouts without waiting 100 minutes per sweep. It does not use the production 2-minute deadline. The run completed three sweeps and observed 150 timeouts across 15,000 RPCs and all 5,000 identities.

- node-agent-health-sweep: 117.93 seconds; within two minutes: True.
- node-agent-health-steady-sweep-01: 108.63 seconds; within two minutes: True.
- node-agent-health-steady-sweep-02: 109.05 seconds; within two minutes: True.

Raw evidence: `agents/agent-timeout-cohort/run-01.json`. The separate production-deadline case above establishes the impact of the actual 2-minute timeout. All ten matrix cases are complete; `agents/summary-verified.json` recomputes their numeric summaries and boolean pass/fail counts from retained raw results.
