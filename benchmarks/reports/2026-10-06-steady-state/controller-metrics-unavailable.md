# Full-worker queue and reconcile metrics are not exposed

The complete-worker experiment at controller revision
`a4b4f2a21cf3344ca19138ed4f5bbe099f430cb4` exposes Go runtime, process and fleet
inventory metrics on the configured HTTP endpoint. Repeated complete scrapes
contain no `workqueue_*` or `controller_runtime_*` samples, despite active
management and tenant reconcilers and confirmed fleet convergence. There are
also no worker DB query/pool-counter samples under `nke_controller_db_*`.

Source evidence:

- `controller/internal/server/metrics.go`: `promhttp.Handler()` serves the
  default Prometheus registry.
- `controller/internal/kube/client.go`: management-manager metrics
  `BindAddress` is `"0"`.
- `controller/internal/controllers/tenant/manager.go`: tenant-manager metrics
  `BindAddress` is `"0"`.
- Controller-runtime uses its own registry; the production worker does not
  gather or register it with the endpoint's default registry.
- The in-process tenant lane gathers `controllermetrics.Registry` directly,
  so its queue observations cannot establish what the full worker exposes.

Impact: the full-worker soak can measure resource trends, API responses, Node
state, RPC activity, CSR approval and metric inventory, but cannot establish
per-controller queue saturation, reconciliation errors or worker DB pool waits
through this endpoint. Empty queue output is missing evidence, not proof of
idle queues. This also limits operator visibility into steady-state lag.

The observation summarizer now records signal availability explicitly.
This finding requires future production observability work; no production
remediation has been made. Record findings locally, without GitHub issues.
