# CWE production-path performance driver

This package exists inside the controller module solely so the performance
harness can exercise CWE's real internal controller paths. It is not imported
by `cmd/worker` and is not included in the production binary.

`cmd/cwe-perf` provides five modes:

- tenant manager cold start and Node watch-to-database convergence;
- full-fleet node-agent readiness sweeps with configurable concurrency,
  deterministic latency, and deterministic failures;
- DB node-source startup and resync;
- complete Prometheus scrapes from the production database-backed collector;
- complete-worker black-box startup and steady-state sampling.

See [`../../benchmarks/cwe_perf/README.md`](../../benchmarks/cwe_perf/README.md) for setup,
commands, scenario files, result format, and reproducibility controls.
