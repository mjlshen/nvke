# Two-hour complete-worker steady-state result

The local 100-cluster / 5,000-node mixed workload completed its full two-hour measurement and passed all nine final completeness/recovery checks. This is evidence that the unchanged worker can sustain this particular fixture. It does not establish a production capacity maximum or per-node freshness.

- Measurement duration: 7200.462 seconds, after 104.66 seconds of initial fleet convergence.
- Observations: 468, with no sample or run errors.
- Final inventory: 5,000 nodes, 5,000 agent-ready nodes, all 5,000 Kubernetes Nodes observed, and 4,950 Kubernetes-ready nodes. The other 50 are intentional fixture state. Injected health changes were restored.
- Agent RPCs: 312,997; no simulated failures or timeouts. RPCs are aggregate and cannot establish individual-node probe freshness.
- Certificate work: 100 serving CSRs approved; 2,500 simulated unchanged-certificate operations delivered through Temporal. No real rotation or node commands.
- List-cluster API: median 2.643 ms, p95 3.587 ms, maximum 15.674 ms. This is the cluster list endpoint, not the unpaginated node-list workload.

| Worker signal | Observed result |
|---|---|
| CPU | 0.225 cores averaged over observed interval |
| RSS | 340.8–516.2 MiB; median 399.1 MiB; final 387.2 MiB |
| Heap allocation in use | 86.1–263.5 MiB; median 159.2 MiB |
| Goroutines | 6934–6953; final 6935 |
| Cumulative allocation | 146.90 GB during observation interval; this is allocation churn, not retained memory |
| GC cycles | 1566 |

RSS first/last-quarter medians were 394.9 and 406.5 MiB; heap first/last-quarter medians were 165.3 and 158.0 MiB. Linear fitted trends were 2.82 MiB/hour for RSS and 1.66 MiB/hour for heap. These small positive fits over two hours do not establish a memory leak; neither do they prove long-term boundedness. No worker restart occurred during this soak.

Queue depths, reconciliation counters and worker DB pool metrics were absent in all 468 samples. They are unavailable evidence, not zero backlog or zero contention. The separate component lanes still expose their own DB query/pool counters.

The host process had no pod resource limit; dependencies were local and shared virtual tenant/agent services replaced independent endpoints. The component suite and early builds/tests overlapped the soak. SPIFFE, actual certificate commands, NICo and provisioning were omitted. See [coverage](coverage.md) and [environment](environment.json).

Raw evidence: [result](worker-soak-5000.json), [observation stream](worker-soak-5000.json.samples.jsonl), [derived summary](worker-soak-5000-summary.json). The separate [20-minute large-fleet recovery experiment](worker-recovery.md) also completed and passed all nine final checks.
