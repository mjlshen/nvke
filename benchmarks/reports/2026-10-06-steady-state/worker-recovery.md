# Large-fleet outage and restart result

The 100-cluster / 5,000-node recovery experiment completed its 20-minute measurement and passed all nine final checks with no run or observation errors. Faults were actually observed in the raw stream, rather than inferred from configuration alone.

- A two-minute agent outage began at minute 3. Eight observations recorded an active outage; total simulated failed RPCs reached 6,047. The lowest observed agent-ready count was 1,745. Failed RPCs stopped increasing after restoration.
- The first sampled full-ready fleet after restoration was at minute 7.205, about 132 seconds after the scheduled outage end. This is a sampling bound, not an exact per-node freshness/recovery percentile.
- A one-minute outage at minute 7 affected all 100 virtual tenant APIs and dropped their watches. Four observations recorded all 100 tenant APIs in outage. The simulator recorded 2,220 dependency failures.
- Graceful restart at minute 10 produced a second worker generation. The driver recorded 94.625 seconds for readiness plus a new aggregate 5,000-probe sweep. It does not prove that each individual node was re-probed.
- The driver pauses observations while waiting for restart convergence; the largest observation gap was 110.07 seconds. No worker metrics are captured during that interval.
- Final state: 5,000 nodes, 5,000 agent-ready nodes, all 5,000 Kubernetes Nodes observed, and 4,950 Kubernetes-ready nodes, matching the intentional baseline. There were 40 injected Node health changes, 100 approved serving CSRs and 500 simulated unchanged-certificate operations.

The stream contains 74 observations: 39 before restart and 35 after it. Post-restart goroutines ranged from 6,934 to 6,944. Post-restart list-cluster API latency had a median of 3.029 ms and p95 of 4.223 ms. Process generations remain separate in the resource summary; fitting short startup/outage windows is not evidence of a leak.

Final aggregate inventory checks establish restored recorded state. They do not establish per-node freshness, watch recovery for every cluster, or real certificate rotation. Tenant active-watch counts are available in the stream but final recorded Node conditions can remain unchanged during an outage. The full-worker endpoint exposes no controller queue/reconcile or DB-pool metrics; absent observations do not prove bounded backlog. Real SPIFFE, multi-replica failover and lost-lease behavior remain outside this fixture.

Raw evidence: [result](worker-recovery-5000.json), [observations](worker-recovery-5000.json.samples.jsonl), [per-generation summary](worker-recovery-5000-summary.json). See [commands](commands.md) and [coverage](coverage.md). Findings remain local; no production remediation has been made.
