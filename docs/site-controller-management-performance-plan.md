# Site-controller management performance plan

## Purpose

This initiative will test and optimize the performance of the NKE
site-controller management stack and `nke-controller` under realistic load.
It combines profiling, repeatable benchmarking, capacity testing, and targeted
tuning to answer four practical questions:

1. What fleet size and lifecycle concurrency can a supported site-controller
   deployment manage while remaining correct and responsive?
2. Which component becomes the first bottleneck as the number of sites,
   clusters, node pools, nodes, and concurrent operations increases?
3. How much time and resource consumption is attributable to `nke-controller`,
   and how much belongs to its platform and external dependencies?
4. Which changes measurably improve throughput, latency, or resource efficiency
   without weakening correctness, recovery, or operability?

The outcome is not a single benchmark score. It is a documented capacity
envelope, a reproducible body of evidence, and a prioritized set of verified
improvements.

This document describes the program-level plan. The detailed CWE measurement
methodology is in the
[controller performance analysis plan](../controller/docs/performance-analysis-plan.md),
and commands for the existing test tooling are in the
[CWE performance harness](../benchmarks/README.md).

## System under test

The initiative uses two related measurement boundaries.

One CWE deployment is scoped to one site and runs on that site's controller
cluster. The number of sites is therefore fixed at one for every experiment; it
is not a capacity dimension of a CWE deployment. Scaling to multiple sites
means deploying and measuring multiple independent site-controller stacks.
Provider `site_id` fields attached to tenant-cluster configuration do not change
that management boundary.

### Site-controller management stack

The stack-level boundary represents the production management plane on a site
controller cluster. It includes:

- the site-controller Kubernetes control plane and its API server;
- `nke-controller`, also referred to as the Cloud Workflow Engine (CWE);
- Temporal and its persistence;
- PostgreSQL used by `nke-controller`;
- Cluster API and infrastructure/bootstrap/control-plane providers;
- SPIRE, Teleport, and other management-plane services used on lifecycle paths;
- monitoring, logging, and metrics collection; and
- network calls to infrastructure providers, tenant clusters, and NKE node
  agents.

This boundary answers whether the deployed system meets its capacity and
recovery objectives. It captures contention between workloads, dependency
limits, Kubernetes scheduling effects, and the cost of observability.

### `nke-controller`

The controller-level boundary isolates the application paths that NKE owns and
can tune directly:

- REST request validation, database transactions, and response serialization;
- workflow admission through the transactional outbox;
- Temporal workflows and activities;
- site and Kubernetes reconcilers;
- PostgreSQL queries and connection-pool use;
- calls to Cluster API, tenant Kubernetes APIs, SPIRE, Teleport, providers, and
  node agents; and
- metrics collection and other periodic background work.

Measuring both boundaries prevents a fast component benchmark from being
mistaken for a scalable system, and prevents dependency delay from being
incorrectly assigned to `nke-controller`.

## Goals

- Define representative expected, maximum, and overload fleet shapes.
- Establish baseline throughput, latency, convergence time, and resource use.
- Determine the safe operating envelope for supported deployment sizes.
- Find the saturation point and first constrained resource for each important
  workload.
- Separate queueing and active controller work from external wait time.
- Validate graceful behavior during bursts, dependency degradation, replica
  restart, and recovery.
- Optimize bottlenecks in evidence-ranked order and verify the improvement with
  an equivalent before-and-after experiment.
- Add low-cost regression coverage suitable for pull requests and more complete
  scheduled tests on controlled infrastructure.
- Publish the resulting maximums, recommended resource sizing, concurrency
  settings, and known limits.

## Non-goals

- Proving the independent maximum performance of PostgreSQL, Temporal,
  Kubernetes, SPIRE, Teleport, a provider, or physical hardware.
- Using a synthetic requests-per-second result as the only definition of
  capacity.
- Optimizing real bare-metal boot duration when the delay is outside NKE's
  control. Provider time will still be measured so it can be separated from
  NKE-controlled time.
- Setting product commitments before product, SRE, and infrastructure owners
  confirm the fleet envelope and service objectives.
- Trading away correctness, durability, or safe retry behavior for a better
  latency number.

## Capacity model

Capacity is multidimensional. Each result must identify the tested point across
the following axes rather than reporting only aggregate request rate:

| Dimension | Initial test points | Why it matters |
|---|---:|---|
| tenant clusters | 10, 50, 100 | Drives caches, watches, API clients, and lifecycle state |
| node pools | 50, 250, 1,000 | Drives reconciliation and retained configuration state |
| total nodes | 100, 1,000, 5,000 | Drives agent checks, tenant watches, metrics, and database rows |
| concurrent lifecycle operations | 1, 10, 25, 50 | Drives workflow, activity, database, and dependency concurrency |
| API read traffic | 1, 10, 50, 100 requests/s | Exercises query, mapping, serialization, and response size |
| mutation bursts | 1, 5, 20 requests/s for 60 seconds | Exercises transactions, locks, and outbox delivery |
| metrics scrape interval | 60, 30, 15 seconds | Measures observability cost and database interference |
| `nke-controller` replicas | 1, 2, 3 | Tests failover, coordination, and scaling efficiency |

These are exploration points, not declared product maximums. Owners must
replace or augment them with expected production distributions. Test data must
also include realistic skew: a few large clusters, many small clusters, old
node sets and workflow history, failed or deleting resources, and a mixture of
lifecycle states.

Three values will be published for each supported deployment profile:

- **expected operating point:** the normal production envelope;
- **safe maximum:** the highest demonstrated point that meets all guardrails
  with documented headroom; and
- **observed saturation point:** the first point at which a guardrail fails,
  including the constraining resource and failure mode.

The safe maximum is deliberately below the observed saturation point. It must
be supported by sustained tests and recovery evidence, not extrapolated from a
short linear ramp.

## What will be measured

### User- and lifecycle-facing outcomes

- API throughput, in-flight requests, error rate, and p50/p90/p95/p99 latency;
- time from a committed API mutation to Temporal workflow start;
- schedule-to-start and execution latency for workflows and activities;
- cluster create, scale, upgrade, reboot, remediation, and delete duration;
- cluster, node-pool, node-set, and node reconciliation lag and convergence
  time;
- node-event-to-database and desired-state-to-observed-state convergence;
- backlog size, oldest work age, retry count, and recovery time; and
- correctness of final state, including missing, duplicated, or stuck work.

An API success usually means that durable work was admitted, not that the
lifecycle operation completed. Admission latency and lifecycle completion time
will therefore be reported separately.

### Resource and dependency signals

- CPU use, throttling, resident memory, heap, allocations, garbage collection,
  goroutines, file descriptors, and network traffic;
- Kubernetes pod scheduling, restart, readiness, and resource-pressure signals;
- PostgreSQL statement latency, query plans, locks, transactions, connection
  pool waits, buffer hit rate, temporary files, WAL, and deadlocks;
- Temporal task-queue depth, schedule-to-start latency, pollers, worker slots,
  retries, and persistence health;
- controller-runtime queue depth, reconcile duration, retry rate, and lag;
- outbox pending count, oldest-entry age, batch duration, and delivery outcome;
- provider, Kubernetes, SPIRE, Teleport, and node-agent call latency, errors,
  retries, and throttling; and
- Prometheus scrape duration, response size, active series, query cost, and
  impact on foreground work.

### Attribution

Lifecycle measurements will divide elapsed time into:

1. active `nke-controller` execution;
2. waiting in API, outbox, controller, or Temporal queues;
3. retry and backoff time;
4. polling or reconciliation interval delay; and
5. time waiting on external services or machines.

The benchmark run ID and stable resource identifiers will connect load-driver
output, metrics, logs, Temporal history, profiles, and database evidence.
Sensitive payloads, credentials, kubeconfigs, and join tokens must never be
captured in test artifacts.

## Workload plan

The program uses multiple test layers because no single environment is both
fully realistic and sufficiently deterministic for diagnosis.

### 1. Component benchmarks

Table-driven Go benchmarks will measure CPU and allocation-heavy operations in
isolation, including request validation, large list mapping and serialization,
workflow-history transformation, profile rendering, provider conversion, and
reconciliation planning. Results will include `ns/op`, `B/op`, and `allocs/op`
and will be compared with `benchstat`.

These tests are useful for localizing code-level regressions but do not define
system capacity.

### 2. Hermetic `nke-controller` tests

Run the production controller paths against PostgreSQL, Temporal, and
deterministic substitutes for providers, tenant Kubernetes APIs, SPIRE,
Teleport, and node agents. The substitutes must support repeatable latency,
jitter, throttling, failure, and recovery behavior.

This layer will exercise API reads and mutations, outbox delivery, workflow
fan-out, reconciliation, node-agent readiness, metrics scraping, and dependency
degradation without requiring thousands of machines. The existing performance
harness already models tenant-cluster and node counts independently and is the
starting point for this layer.

### 3. Deployed site-controller stack tests

Deploy the complete management stack with production-equivalent configuration,
resource requests and limits, security, metrics, and replica topology. Drive
the same logical workloads used by the hermetic tests and observe contention
across the Kubernetes control plane, `nke-controller`, Temporal, PostgreSQL,
CAPI providers, and observability services.

This layer validates stack sizing, pod placement, throttling, failover,
dependency coupling, and the cost of running all control loops together.

### 4. Representative lifecycle tests

Use CAPD or `nico-sim` for repeatable cluster lifecycle testing, then validate a
smaller set of critical paths on approved representative infrastructure. Cover:

- single-cluster creation and deletion;
- parallel creation of several clusters;
- node-pool scale-out and scale-in bursts;
- upgrade, patch, reboot, remediation, and teardown;
- mixed lifecycle work while API reads, reconciliation, and metric scrapes
  continue; and
- interruption and recovery of controller and dependency replicas.

Real infrastructure validates assumptions about the critical path. It is not
the primary saturation environment because machine and provider variation can
hide controller bottlenecks.

## Core scenario matrix

| Scenario | Load shape | Main question |
|---|---|---|
| idle inventory | small, expected, and maximum inventories with normal controllers and scrapes | What is the fixed and per-object cost? |
| API reads | filtered and unfiltered list/get/history traffic at each inventory size | Where do query, serialization, payload, or heap costs stop scaling? |
| admission burst | safe concurrent create/update/delete operations followed by recovery | Can transactions and the outbox absorb bursts without lost work? |
| workflow fan-out | increasing concurrent lifecycle workflows | Which of worker slots, CPU, database, or dependencies saturates first? |
| reconciliation wave | enqueue many cluster, node-pool, node-set, and node changes together | What is the maximum stable reconcile rate and convergence time? |
| scrape pressure | vary inventory and scrape interval | Does observability interfere with API or lifecycle work? |
| slow or failing dependency | inject latency, throttling, errors, and outages into one dependency at a time | Are retries bounded, and does unrelated work remain healthy? |
| replica restart | restart one controller, Temporal, or database-facing workload during load | Is accepted work durable and is recovery timely? |
| replica scaling | repeat equivalent workloads with one, two, and three controller replicas | Does added capacity improve throughput safely and efficiently? |
| soak | expected mixed traffic for 2 hours initially and 24 hours before declaring a maximum | Are memory, goroutines, connections, history, or series unbounded? |
| overload and recovery | ramp beyond the expected point, remove excess load, and observe recovery | Does the stack degrade predictably and clear its backlog? |

Mutating workloads must use independent resources unless conflict behavior is
the subject of the test. Expected conflict responses must not be counted as
capacity failures.

## Execution phases

### Phase 0: agree on the contract

- Confirm expected and maximum production fleet shapes with product, SRE, and
  infrastructure owners.
- Define provisional service-level indicators and correctness guardrails.
- Select representative site-controller deployment profiles and dependency
  versions.
- Define the result schema, ownership, and where evidence will be retained.

Exit criterion: stakeholders agree that the workloads and measurements
represent the system whose limits will be published.

### Phase 1: make results attributable

- Inventory existing metrics, logs, Temporal history, and profiles.
- Add missing low-cardinality HTTP, database pool, outbox, controller,
  dependency, and queue metrics.
- Enable protected profiling in test environments and create a common
  dashboard.
- Quantify instrumentation overhead and keep it below an agreed budget.

Exit criterion: a slow or failed run can be attributed to a component, queue,
dependency, or resource rather than described only by end-to-end latency.

### Phase 2: establish a reproducible baseline

- Pin source revisions, images, tool and dependency versions, configuration,
  machine shapes, resource limits, random seeds, and fixtures.
- Run correctness checks before performance tests.
- Establish cold- and warm-cache controller baselines.
- Run the hermetic inventory matrix and publish the first baseline report.

Exit criterion: another engineer can repeat a run and obtain statistically
comparable results.

### Phase 3: characterize the complete stack

- Run the core matrix on a deployed site-controller stack.
- Capture profiles, query plans, queue behavior, and dependency utilization at
  expected load, near saturation, and during recovery.
- Repeat lifecycle-critical scenarios with CAPD or `nico-sim` and selected
  representative infrastructure.

Exit criterion: the first bottleneck and capacity knee are demonstrated for
each primary workload rather than inferred from resource averages.

### Phase 4: optimize and verify

- Rank bottlenecks by customer impact, frequency, confidence, expected gain,
  engineering risk, and cost.
- Change one limiting factor at a time.
- Repeat the identical baseline/candidate experiment and then the mixed
  workload to detect a shifted bottleneck.
- Record unsuccessful experiments as well as accepted improvements.

Likely hypotheses include database-backed metric collection, unbounded list
responses, outbox transaction duration, serialized reconciliation, Temporal
worker and database-pool coupling, cross-plane contention in the single worker
process, and inefficient queries or indexes. They are hypotheses until a test
demonstrates them.

Exit criterion: each accepted tuning change has reproducible evidence of
improvement and passes correctness, overload, and recovery checks.

### Phase 5: publish limits and prevent regression

- Run sustained tests at the proposed safe maximum and overload point.
- Document supported maximums, required resources, dependency assumptions,
  concurrency settings, headroom, and known failure modes.
- Add fast stable checks to pull requests and schedule the larger saturation,
  lifecycle, and soak suites on controlled infrastructure.
- Define how and when capacity results expire or must be rerun.

Exit criterion: operators can size a deployment from the published evidence,
and future changes are checked against the same workload contract.

## Experiment and comparison rules

- Every run records its source revision, image digest, dependency versions,
  configuration, infrastructure, resource limits, fixture, seed, start time,
  duration, and unique run ID.
- Reset to an equivalent fixture or database snapshot before comparable runs.
- Separate cold-cache, warm-cache, and steady-state results.
- Isolate the load generator from the system under test and verify that it is
  not saturated.
- Use at least five repetitions for short tests and at least three for costly
  lifecycle tests. Alternate baseline and candidate order where practical.
- Report distributions and confidence intervals, not only averages.
- Compare equivalent achieved throughput and inventory. A candidate is not
  faster if it silently does less work or accumulates a larger backlog.
- Preserve profiles, dashboards, query plans, logs, and correctness summaries
  alongside the aggregate result.

As an initial regression policy, investigate a statistically supported change
of more than 10% in throughput, median latency, CPU, or memory, or more than 15%
in p95 latency. These values are triage thresholds, not final product SLOs. Any
data loss, missed or duplicate unsafe operation, deadlock, crash loop, or
unbounded resource/backlog growth is a failure regardless of average latency.

## Declaring a maximum

A fleet or concurrency point can be documented as a safe maximum only when:

1. all agreed latency, error, convergence, and correctness guardrails pass;
2. a sustained soak shows no unbounded resource or backlog growth;
3. the stack recovers within its objective after a defined burst or dependency
   interruption;
4. the load generator and test dependencies are not the limiting resource;
5. the first bottleneck above that point is understood and reproducible;
6. enough CPU, memory, database, queue, and dependency headroom remains for
   normal variation; and
7. the deployment profile, configuration, and external assumptions are stated
   with the result.

Maximums are conditional. For example, a node limit measured with a particular
scrape interval, workflow concurrency, database size, and provider latency must
not be presented as universal.

## Deliverables

1. A confirmed production workload and fleet envelope.
2. A versioned performance harness, deterministic fixtures, and execution
   runbook.
3. A dashboard and profiling procedure covering the full management stack.
4. A baseline report for expected, maximum, overload, and recovery conditions.
5. A capacity report that records the safe maximum, saturation point, first
   bottleneck, resource sizing, and configuration for each deployment profile.
6. An evidence-ranked optimization backlog and before/after reports for the
   changes implemented.
7. An operator-facing sizing and limits document.
8. Pull-request regression checks and scheduled saturation, lifecycle, and soak
   suites.

## Completion criteria

The initiative is complete when the team can reproduce and explain the
performance of the expected production configuration, publish defensible
maximums with headroom and assumptions, identify the next limiting resource,
and detect meaningful regressions. It is not complete merely because a load
test ran or because one bottleneck was optimized.

## Decisions still required

- Expected and maximum fleet distributions for each supported site profile.
- Product-facing latency, convergence, recovery, and availability objectives.
- Required safety margin between the demonstrated saturation point and the
  published maximum.
- Canonical controlled environment and ownership of scheduled tests.
- Artifact retention period and location for potentially large profiles,
  metrics, and logs.
- Which benchmark gates are advisory and which will block a release.
