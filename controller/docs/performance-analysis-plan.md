Jira Epic: NKX-13661
Michael Shen (SW-GPU) US
Purpose
The purpose of this document is to outline how we plan to analyze the performance of nke-controller (previously CWE) to support DSX.
Goals
What fleet size and lifecycle concurrency can nke-controller manage while remaining correct and responsive?
What bottlenecks arise as things scale, primarily under two dimensions: the number of nodes and the number of clusters?
Which changes can be made to measurably improve performance (throughput, latency, efficiency) without sacrificing correctness?
Non-goals
Proving the independent maximum performance of PostgreSQL, Temporal, Kubernetes, SPIRE, Teleport, a provider, or physical hardware.
Optimizing real bare-metal boot duration when the delay is outside nke-controller’s control.
Trading away correctness, durability, or safe retry behavior for a better latency number.
Setting product commitments - we will publish our numbers, but it is up to product owners and nke-controller operators to decide on their SLOs.

Performance Tests
Dimensions

Dimension
Initial test points
Why it matters
tenant clusters
10, 50, 100
Drives caches, watches, API clients, and lifecycle state
node pools
50, 250, 1,000
Drives reconciliation and retained configuration state
total nodes
100, 1,000, 5,000
Drives agent checks, tenant watches, metrics, and database rows
concurrent lifecycle operations
1, 10, 25, 50
Drives workflow, activity, database, and dependency concurrency
API read traffic
1, 10, 50, 100 requests/s
Exercises query, mapping, serialization, and response size
mutation bursts
1, 5, 20 requests/s for 60 seconds
Exercises transactions, locks, and outbox delivery
nke-controller replicas
1, 2, 3
Tests failover, coordination, and scaling efficiency

Test Plan
1. Hermetic nke-controller tests
Run the real nke-controller against PostgreSQL, Temporal, and deterministic substitutes for providers, tenant Kubernetes APIs, SPIRE, Teleport, and node agents. The substitutes will support repeatable and configurable latency, jitter, throttling, failure, and recovery behavior.

This layer will exercise API reads and mutations, outbox delivery, workflow fan-out, reconciliation, node-agent readiness, metrics scraping, and dependency degradation without requiring thousands of machines.
2. CAPD representative lifecycle tests
Use CAPD for repeatable cluster lifecycle testing, then validate a smaller set of critical paths. This is the typical local development environment.

single-cluster creation and deletion;
parallel creation of several clusters;
node-pool scale-out and scale-in bursts;
upgrade, patch, reboot, remediation, and teardown;
mixed lifecycle work while API reads, reconciliation, and metric scrapes continue; and
interruption and recovery of controller and dependency replicas.
3. Real nke-controller deployment with full-fledged NKE clusters
Deploy the complete management stack with a scale we can validate at, with resource requests and limits, security, metrics, replica topology, etc. learnings from #1 and #2. Drive the same logical workloads used by the hermetic tests and observe contention across the Kubernetes control plane, nke-controller, Temporal, PostgreSQL, CAPI providers, and observability services.
