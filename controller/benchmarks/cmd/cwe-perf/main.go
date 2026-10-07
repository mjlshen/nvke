// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/dsx-ai-factory/nke/controller/benchmarks/internal/perf"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctrlruntimelog.SetLogger(logr.FromSlogHandler(logger.Handler()))

	var result perf.Result
	var output string
	var err error
	switch os.Args[1] {
	case "tenant":
		result, output, err = tenantCommand(ctx, os.Args[2:], logger)
	case "agent-health":
		result, output, err = agentCommand(ctx, os.Args[2:], logger)
	case "node-source":
		result, output, err = nodeSourceCommand(ctx, os.Args[2:], logger)
	case "prometheus":
		result, output, err = prometheusCommand(ctx, os.Args[2:], logger)
	case "black-box":
		result, output, err = blackBoxCommand(ctx, os.Args[2:], logger)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		if output != "" {
			result.Errors = append(result.Errors, err.Error())
			result.AddRuntimeMetrics()
			if writeErr := perf.WriteResult(output, result); writeErr != nil {
				fmt.Fprintf(os.Stderr, "cwe-perf: also failed to write result: %v\n", writeErr)
			}
		}
		fmt.Fprintf(os.Stderr, "cwe-perf: %v\n", err)
		os.Exit(1)
	}
	if err := perf.WriteResult(output, result); err != nil {
		fmt.Fprintf(os.Stderr, "cwe-perf: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "wrote %s\n", output)
}

func prometheusCommand(ctx context.Context, args []string, logger *slog.Logger) (perf.Result, string, error) {
	flags := flag.NewFlagSet("prometheus", flag.ContinueOnError)
	inventoryPath := flags.String("inventory", "", "materialized inventory JSON")
	dsn := flags.String("db-dsn", "postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable", "dedicated PostgreSQL DSN")
	profilesDir := flags.String("profiles-dir", defaultProfilesDir(), "profiles fixture used by database migrations")
	output := flags.String("output", "cwe-perf-result.json", "result JSON path")
	poolsPerCluster := flags.Int("node-pools-per-cluster", 10, "node pools materialized for each cluster")
	setsPerPool := flags.Int("node-sets-per-pool", 1, "retained node sets materialized for each node pool")
	warmups := flags.Int("warmup-scrapes", 1, "unmeasured scrapes before the measurement")
	scrapes := flags.Int("scrapes", 5, "measured Prometheus scrapes")
	interval := flags.Duration("scrape-interval", 0, "idle time between scrapes per worker; zero runs back-to-back")
	concurrency := flags.Int("concurrency", 1, "concurrent Prometheus scrapes")
	timeout := flags.Duration("timeout", 2*time.Minute, "whole warmup or measured scrape phase timeout")
	if err := flags.Parse(args); err != nil {
		return perf.Result{}, "", err
	}
	if *poolsPerCluster <= 0 {
		return perf.Result{}, "", fmt.Errorf("-node-pools-per-cluster must be greater than zero")
	}
	if *setsPerPool <= 0 {
		return perf.Result{}, "", fmt.Errorf("-node-sets-per-pool must be greater than zero")
	}
	if *warmups < 0 {
		return perf.Result{}, "", fmt.Errorf("-warmup-scrapes must not be negative")
	}
	if *scrapes <= 0 {
		return perf.Result{}, "", fmt.Errorf("-scrapes must be greater than zero")
	}
	if *interval < 0 {
		return perf.Result{}, "", fmt.Errorf("-scrape-interval must not be negative")
	}
	if err := configureProfilesDir(*profilesDir); err != nil {
		return perf.Result{}, "", err
	}
	inventory, err := loadAndValidate(*inventoryPath, *concurrency, *timeout)
	if err != nil {
		return perf.Result{}, "", err
	}
	result, err := perf.RunPrometheus(ctx, inventory, perf.PrometheusOptions{
		DSN: *dsn, NodePoolsPerCluster: *poolsPerCluster, NodeSetsPerPool: *setsPerPool,
		WarmupScrapes: *warmups, Scrapes: *scrapes, Concurrency: *concurrency, Timeout: *timeout, Interval: *interval,
	}, logger)
	return result, *output, err
}

func tenantCommand(ctx context.Context, args []string, logger *slog.Logger) (perf.Result, string, error) {
	flags := flag.NewFlagSet("tenant", flag.ContinueOnError)
	inventoryPath := flags.String("inventory", "", "materialized inventory JSON")
	dsn := flags.String("db-dsn", "postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable", "dedicated PostgreSQL DSN")
	profilesDir := flags.String("profiles-dir", defaultProfilesDir(), "profiles fixture used by database migrations")
	output := flags.String("output", "cwe-perf-result.json", "result JSON path")
	maxStarts := flags.Int("max-concurrent-starts", 2, "tenant manager startup concurrency")
	events := flags.Int("events", 1, "number of clusters receiving one Node event")
	timeout := flags.Duration("timeout", 10*time.Minute, "phase timeout")
	if err := flags.Parse(args); err != nil {
		return perf.Result{}, "", err
	}
	if *events < 0 {
		return perf.Result{}, "", fmt.Errorf("-events must not be negative")
	}
	if err := configureProfilesDir(*profilesDir); err != nil {
		return perf.Result{}, "", err
	}
	inventory, err := loadAndValidate(*inventoryPath, *maxStarts, *timeout)
	if err != nil {
		return perf.Result{}, "", err
	}
	result, err := perf.RunTenant(ctx, inventory, perf.TenantOptions{DSN: *dsn, MaxConcurrentStarts: *maxStarts, EventCount: *events, Timeout: *timeout}, logger)
	return result, *output, err
}

func agentCommand(ctx context.Context, args []string, logger *slog.Logger) (perf.Result, string, error) {
	flags := flag.NewFlagSet("agent-health", flag.ContinueOnError)
	inventoryPath := flags.String("inventory", "", "materialized inventory JSON")
	dsn := flags.String("db-dsn", "postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable", "dedicated PostgreSQL DSN")
	profilesDir := flags.String("profiles-dir", defaultProfilesDir(), "profiles fixture used by database migrations")
	output := flags.String("output", "cwe-perf-result.json", "result JSON path")
	concurrency := flags.Int("concurrency", 1, "readiness reconcile workers")
	steadySweeps := flags.Int("steady-sweeps", 1, "unchanged readiness sweeps after the initial transition sweep")
	delay := flags.Duration("agent-delay", 0, "fixed simulated RPC delay")
	failureEvery := flags.Uint64("agent-failure-every", 0, "fail every Nth RPC; zero disables failures")
	slowEvery := flags.Uint64("slow-node-every", 0, "add slow-node delay to every Nth identified node")
	slowDelay := flags.Duration("slow-node-delay", 0, "additional delay for selected slow nodes")
	failedNodeEvery := flags.Uint64("failed-node-every", 0, "persistently fail every Nth identified node")
	timeout := flags.Duration("timeout", 2*time.Minute, "per-probe timeout")
	if err := flags.Parse(args); err != nil {
		return perf.Result{}, "", err
	}
	if *steadySweeps < 0 {
		return perf.Result{}, "", fmt.Errorf("-steady-sweeps must not be negative")
	}
	if *delay < 0 || *slowDelay < 0 {
		return perf.Result{}, "", fmt.Errorf("agent delays must not be negative")
	}
	if err := configureProfilesDir(*profilesDir); err != nil {
		return perf.Result{}, "", err
	}
	inventory, err := loadAndValidate(*inventoryPath, *concurrency, *timeout)
	if err != nil {
		return perf.Result{}, "", err
	}
	result, err := perf.RunAgentHealth(ctx, inventory, perf.AgentOptions{
		DSN: *dsn, Concurrency: *concurrency, SteadySweeps: *steadySweeps,
		Delay: *delay, FailureEvery: *failureEvery, Timeout: *timeout,
		SlowEvery: *slowEvery, SlowDelay: *slowDelay, FailedNodeEvery: *failedNodeEvery,
	}, logger)
	return result, *output, err
}

func nodeSourceCommand(ctx context.Context, args []string, logger *slog.Logger) (perf.Result, string, error) {
	flags := flag.NewFlagSet("node-source", flag.ContinueOnError)
	inventoryPath := flags.String("inventory", "", "materialized inventory JSON")
	dsn := flags.String("db-dsn", "postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable", "dedicated PostgreSQL DSN")
	profilesDir := flags.String("profiles-dir", defaultProfilesDir(), "profiles fixture used by database migrations")
	output := flags.String("output", "cwe-perf-result.json", "result JSON path")
	concurrency := flags.Int("source-concurrency", 2, "concurrent DB node sources")
	copiesPerCluster := flags.Int("copies-per-cluster", 1, "tenant-style node sources per cluster")
	resyncs := flags.Int("resyncs", 1, "forced resync passes after source startup")
	timeout := flags.Duration("timeout", 10*time.Minute, "phase timeout")
	if err := flags.Parse(args); err != nil {
		return perf.Result{}, "", err
	}
	if *copiesPerCluster <= 0 {
		return perf.Result{}, "", fmt.Errorf("-copies-per-cluster must be greater than zero")
	}
	if *resyncs < 0 {
		return perf.Result{}, "", fmt.Errorf("-resyncs must not be negative")
	}
	if err := configureProfilesDir(*profilesDir); err != nil {
		return perf.Result{}, "", err
	}
	inventory, err := loadAndValidate(*inventoryPath, *concurrency, *timeout)
	if err != nil {
		return perf.Result{}, "", err
	}
	result, err := perf.RunNodeSource(ctx, inventory, perf.NodeSourceOptions{
		DSN: *dsn, Concurrency: *concurrency, CopiesPerCluster: *copiesPerCluster,
		Resyncs: *resyncs, Timeout: *timeout,
	}, logger)
	return result, *output, err
}

func blackBoxCommand(ctx context.Context, args []string, logger *slog.Logger) (perf.Result, string, error) {
	flags := flag.NewFlagSet("black-box", flag.ContinueOnError)
	inventoryPath := flags.String("inventory", "", "materialized inventory JSON")
	binary := flags.String("binary", "./bin/controller", "complete CWE worker binary")
	kubeconfig := flags.String("management-kubeconfig", "", "kubeconfig for one real management cluster")
	dsn := flags.String("db-dsn", "postgres://postgres:example-password@127.0.0.1:5432/postgres?sslmode=disable", "dedicated PostgreSQL DSN")
	profilesDir := flags.String("profiles-dir", defaultProfilesDir(), "profiles fixture used by database migrations")
	temporal := flags.String("temporal-host-port", "127.0.0.1:7233", "Temporal frontend address")
	readyURL := flags.String("ready-url", "http://127.0.0.1:8080/readyz", "worker readiness URL")
	metricsURL := flags.String("metrics-url", "http://127.0.0.1:9090/metrics", "worker Prometheus URL")
	duration := flags.Duration("duration", 30*time.Second, "steady-state sample duration")
	sampleInterval := flags.Duration("sample-interval", 15*time.Second, "idle time between worker observations")
	samplesPath := flags.String("samples", "", "streaming JSONL observations; defaults to output + .samples.jsonl")
	agentDelay := flags.Duration("agent-delay", 0, "fixed simulated RPC delay")
	agentFailureEvery := flags.Uint64("agent-failure-every", 0, "fail every Nth RPC")
	outageAfter := flags.Duration("agent-outage-after", 0, "elapsed time before agent outage")
	outageDuration := flags.Duration("agent-outage-duration", 0, "agent outage duration; zero disables outage")
	restartAfter := flags.Duration("restart-after", 0, "restart worker once after elapsed time; zero disables restart")
	nodeEventInterval := flags.Duration("node-event-interval", 0, "alternate one Node health event per interval; zero disables")
	servingCSRs := flags.Bool("serving-csrs", false, "submit one valid kubelet-serving CSR per cluster")
	tenantOutageAfter := flags.Duration("tenant-outage-after", 0, "elapsed time before virtual tenant API outage")
	tenantOutageDuration := flags.Duration("tenant-outage-duration", 0, "tenant API outage duration; zero disables")
	certificateInterval := flags.Duration("cert-renewal-interval", 0, "simulate certificate-check workflows on one CP node per cluster at this interval; zero keeps base fixture")
	timeout := flags.Duration("timeout", 10*time.Minute, "startup timeout")
	output := flags.String("output", "cwe-perf-result.json", "result JSON path")
	if err := flags.Parse(args); err != nil {
		return perf.Result{}, "", err
	}
	if *kubeconfig == "" {
		return perf.Result{}, "", fmt.Errorf("-management-kubeconfig is required")
	}
	if *duration <= 0 || *sampleInterval <= 0 || *agentDelay < 0 || *outageAfter < 0 || *outageDuration < 0 || *restartAfter < 0 {
		return perf.Result{}, "", fmt.Errorf("durations must be positive for measurement/sampling and nonnegative for fault controls")
	}
	if (*restartAfter > 0 && *restartAfter >= *duration) || (*outageDuration > 0 && *outageAfter+*outageDuration >= *duration) {
		return perf.Result{}, "", fmt.Errorf("restart and outage must leave a recovery window within -duration")
	}
	if *certificateInterval < 0 || *nodeEventInterval < 0 || *tenantOutageAfter < 0 || *tenantOutageDuration < 0 || (*tenantOutageDuration > 0 && *tenantOutageAfter+*tenantOutageDuration >= *duration) {
		return perf.Result{}, "", fmt.Errorf("tenant workload durations must be nonnegative and leave a recovery window")
	}
	if *samplesPath == "" {
		*samplesPath = *output + ".samples.jsonl"
	}
	if err := configureProfilesDir(*profilesDir); err != nil {
		return perf.Result{}, "", err
	}
	inventory, err := loadAndValidate(*inventoryPath, 1, *timeout)
	if err != nil {
		return perf.Result{}, "", err
	}
	result, err := perf.RunBlackBox(ctx, inventory, perf.BlackBoxOptions{
		Binary: *binary, ManagementKubeconfig: *kubeconfig, DSN: *dsn,
		TemporalHostPort: *temporal, ReadyURL: *readyURL, MetricsURL: *metricsURL,
		SteadyStateDuration: *duration, Timeout: *timeout,
		SampleInterval: *sampleInterval, SamplesPath: *samplesPath,
		AgentDelay: *agentDelay, AgentFailureEvery: *agentFailureEvery,
		AgentOutageAfter: *outageAfter, AgentOutageDuration: *outageDuration, RestartAfter: *restartAfter,
		NodeEventInterval: *nodeEventInterval, ServingCSRs: *servingCSRs,
		TenantOutageAfter: *tenantOutageAfter, TenantOutageDuration: *tenantOutageDuration,
		CertificateInterval: *certificateInterval,
	}, logger)
	return result, *output, err
}

func loadAndValidate(path string, positive int, timeout time.Duration) (perf.Inventory, error) {
	if path == "" {
		return perf.Inventory{}, fmt.Errorf("-inventory is required")
	}
	if positive <= 0 {
		return perf.Inventory{}, fmt.Errorf("concurrency must be greater than zero")
	}
	if timeout <= 0 {
		return perf.Inventory{}, fmt.Errorf("timeout must be greater than zero")
	}
	return perf.LoadInventory(path)
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: cwe-perf {tenant|agent-health|node-source|prometheus|black-box} [flags]\nGOMAXPROCS=%d\n", runtime.GOMAXPROCS(0))
}

func defaultProfilesDir() string {
	if configured := os.Getenv("PROFILES_DIR"); configured != "" {
		return configured
	}
	for _, candidate := range []string{"test/fixtures/profiles", "controller/test/fixtures/profiles"} {
		if _, err := os.Stat(filepath.Join(candidate, "osProfiles", "metadata.yaml")); err == nil {
			return candidate
		}
	}
	return ""
}

func configureProfilesDir(path string) error {
	if path == "" {
		return fmt.Errorf("-profiles-dir is required; use the controller/test/fixtures/profiles directory from this checkout")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolving profiles directory: %w", err)
	}
	metadata := filepath.Join(absolute, "osProfiles", "metadata.yaml")
	if _, err := os.Stat(metadata); err != nil {
		return fmt.Errorf("validating profiles directory %s: %w", absolute, err)
	}
	if err := os.Setenv("PROFILES_DIR", absolute); err != nil {
		return fmt.Errorf("configuring profiles directory: %w", err)
	}
	return nil
}
