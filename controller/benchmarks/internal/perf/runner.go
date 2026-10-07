// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/types"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/dsx-ai-factory/nke/controller/internal/agent"
	"github.com/dsx-ai-factory/nke/controller/internal/config"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/nodeagentreadiness"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant/csr"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenant/nodestatus"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

const (
	metricNodes                   = "nodes"
	phaseFixtureSeed              = "fixture-seed"
	simulatedNodeConditionMessage = "deterministic virtual node"
)

// TenantOptions configures the in-process tenant-manager lane.
type TenantOptions struct {
	DSN                 string
	MaxConcurrentStarts int
	EventCount          int
	Timeout             time.Duration
}

// AgentOptions configures the node-agent readiness lane.
type AgentOptions struct {
	DSN             string
	Concurrency     int
	SteadySweeps    int
	Delay           time.Duration
	FailureEvery    uint64
	Timeout         time.Duration
	SlowEvery       uint64
	SlowDelay       time.Duration
	FailedNodeEvery uint64
}

type countingFactory struct {
	delegate tenant.RuntimeFactory
	started  atomic.Int64
	startedC chan struct{}
}

func (f *countingFactory) Start(ctx context.Context, target tenant.Target) (tenant.Runtime, error) {
	runtime, err := f.delegate.Start(ctx, target)
	if err == nil {
		f.started.Add(1)
		select {
		case f.startedC <- struct{}{}:
		default:
		}
	}
	return runtime, err
}

// RunTenant starts the real per-tenant managers, then measures cold start and
// propagation of deterministic Node watch events into PostgreSQL.
func RunTenant(ctx context.Context, inventory Inventory, options TenantOptions, logger *slog.Logger) (Result, error) {
	result := NewResult("tenant", inventory)
	store, err := OpenStore(ctx, options.DSN, logger)
	if err != nil {
		return result, err
	}
	defer store.Cleanup() //nolint:errcheck // the run result is already complete

	setupStart := time.Now()
	if err := store.ResetInventory(ctx, inventory); err != nil {
		return result, err
	}
	if err := store.SeedInventory(ctx, inventory, "127.0.0.1"); err != nil {
		return result, err
	}
	result.Phases = append(result.Phases, Phase{Name: phaseFixtureSeed, DurationMS: elapsedMS(setupStart)})

	factory := &countingFactory{
		delegate: tenant.NewManagerFactory(
			logger.With("component", "tenant-runtime"),
			csr.NewRegistrar(store.DB, logger.With("component", "tenant-csr")),
			nodestatus.NewRegistrar(store.DB, logger.With("component", "tenant-nodestatus")),
		),
		startedC: make(chan struct{}, len(inventory.Clusters)),
	}
	managerSet, err := tenant.NewManagerSet(ctx, logger, factory, tenant.ManagerSetOptions{
		MaxConcurrentStarts: options.MaxConcurrentStarts,
		StartupJitter:       0,
		InitialRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff:     time.Second,
	})
	if err != nil {
		return result, err
	}
	defer managerSet.Shutdown()

	coldStart := time.Now()
	for _, cluster := range inventory.Clusters {
		if err := managerSet.Upsert(ctx, tenant.Target{
			ClusterID:           cluster.ID,
			ClusterName:         cluster.Name,
			KubeconfigSecretRef: types.NamespacedName{Namespace: "nke-system", Name: cluster.Name + "-kubeconfig"},
			KubeconfigSecretUID: types.UID("cwe-perf-" + cluster.ID.String()),
			Kubeconfig:          cluster.Kubeconfig(),
		}); err != nil {
			return result, fmt.Errorf("starting tenant manager for %s: %w", cluster.Name, err)
		}
	}
	if err := waitForManagerStarts(ctx, factory, len(inventory.Clusters), options.Timeout); err != nil {
		return result, fmt.Errorf("waiting for tenant manager cold start: %w", err)
	}
	result.Phases = append(result.Phases, Phase{
		Name:       "tenant-manager-cold-start",
		DurationMS: elapsedMS(coldStart),
		Metrics:    map[string]any{"managers": factory.started.Load()},
	})

	initialReconcileStart := time.Now()
	if err := waitForInitialNodeReconciliation(ctx, store.DB, inventory, options.Timeout); err != nil {
		return result, fmt.Errorf("waiting for initial Node reconciliation: %w", err)
	}
	if err := waitForControllerQuiescence(ctx, options.Timeout); err != nil {
		return result, fmt.Errorf("waiting for tenant controllers to become idle: %w", err)
	}
	result.Phases = append(result.Phases, Phase{
		Name:       "tenant-initial-reconciliation",
		DurationMS: elapsedMS(initialReconcileStart),
		Metrics:    map[string]any{metricNodes: inventory.NodeCount()},
	})

	eventCount := min(options.EventCount, len(inventory.Clusters))
	if eventCount > 0 {
		targets := make([]Node, 0, eventCount)
		targetClusters := make([]Cluster, 0, eventCount)
		for _, cluster := range inventory.Clusters {
			for _, node := range cluster.Nodes {
				if node.Ready {
					targets = append(targets, node)
					targetClusters = append(targetClusters, cluster)
					break
				}
			}
			if len(targets) == eventCount {
				break
			}
		}
		if len(targets) == 0 {
			return result, fmt.Errorf("node event phase requires at least one initially ready node")
		}
		eventStart := time.Now()
		for index, target := range targets {
			cluster := targetClusters[index]
			if err := postNodeEvent(ctx, cluster.Server, cluster.Index, target.Name, false); err != nil {
				return result, err
			}
		}
		if err := waitForKubernetesReady(ctx, store.DB, targets, db.ConditionStatusFalse, options.Timeout); err != nil {
			return result, fmt.Errorf("waiting for Node event convergence: %w", err)
		}
		result.Phases = append(result.Phases, Phase{
			Name:       "node-event-convergence",
			DurationMS: elapsedMS(eventStart),
			Metrics:    map[string]any{"events": len(targets), "requestedEvents": options.EventCount},
		})
		if err := waitForControllerQuiescence(ctx, options.Timeout); err != nil {
			return result, fmt.Errorf("waiting for Node event processing to become idle: %w", err)
		}
	}

	result.Metrics["database"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	if queueMetrics, metricsErr := gatherControllerMetrics(); metricsErr == nil {
		result.Metrics["controllerRuntime"] = queueMetrics
	}
	if stats, statsErr := tenantAPIStats(ctx, inventory.Clusters[0].Server); statsErr == nil {
		result.Metrics["tenantAPI"] = stats
	}
	result.AddRuntimeMetrics()
	return result, nil
}

func waitForManagerStarts(ctx context.Context, factory *countingFactory, expected int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for factory.started.Load() < int64(expected) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out after %s", timeout)
		case <-factory.startedC:
		}
	}
	return nil
}

func waitForInitialNodeReconciliation(ctx context.Context, database db.DB, inventory Inventory, timeout time.Duration) error {
	return waitUntil(ctx, timeout, func() (bool, error) {
		nodes, err := database.ListNodes(ctx, db.NodeFilters{})
		if err != nil {
			return false, err
		}
		return initialNodesReconciled(inventory, nodes), nil
	})
}

func initialNodesReconciled(inventory Inventory, nodes []db.Node) bool {
	expected := make(map[uuid.UUID]bool, inventory.NodeCount())
	for _, cluster := range inventory.Clusters {
		for _, node := range cluster.Nodes {
			expected[node.ID] = node.Ready
		}
	}
	for _, node := range nodes {
		ready, exists := expected[node.ID]
		if !exists {
			continue
		}
		if node.Conditions.KubernetesReady.Status != db.ConditionStatusFromBool(ready) ||
			node.Conditions.KubernetesReady.Message != simulatedNodeConditionMessage {
			return false
		}
		delete(expected, node.ID)
	}
	return len(expected) == 0
}

func waitForControllerQuiescence(ctx context.Context, timeout time.Duration) error {
	stableSamples := 0
	return waitUntil(ctx, timeout, func() (bool, error) {
		metrics, err := gatherControllerMetrics()
		if err != nil {
			return false, err
		}
		if !controllerIsQuiescent(metrics) {
			stableSamples = 0
			return false, nil
		}
		stableSamples++
		return stableSamples >= 3, nil
	})
}

func controllerIsQuiescent(metrics map[string]float64) bool {
	depth, hasDepth := metrics["workqueue_depth"]
	active, hasActive := metrics["controller_runtime_active_workers"]
	return hasDepth && hasActive && depth == 0 && active == 0
}

// RunAgentHealth exercises the production readiness reconciler and gRPC client
// against one shared node-agent simulator with bounded worker concurrency.
func RunAgentHealth(ctx context.Context, inventory Inventory, options AgentOptions, logger *slog.Logger) (Result, error) {
	result := NewResult("agent-health", inventory)
	result.Metrics["configuration"] = map[string]any{
		"concurrency": options.Concurrency, "agentDelay": options.Delay.String(),
		"failureEvery": options.FailureEvery, "probeTimeout": options.Timeout.String(),
		"steadySweeps": options.SteadySweeps, "transport": "plaintext", "scheduler": "direct-sweeps",
		"slowNodeEvery": options.SlowEvery, "slowNodeDelay": options.SlowDelay.String(), "failedNodeEvery": options.FailedNodeEvery,
	}
	simulator, err := StartAgentSimulator(ctx, "127.0.0.1:0", options.Delay, options.FailureEvery)
	if err != nil {
		return result, err
	}
	defer simulator.Stop()
	simulator.ConfigureCohort(options.SlowEvery, options.SlowDelay, options.FailedNodeEvery)
	_, port, err := net.SplitHostPort(simulator.Address())
	if err != nil {
		return result, fmt.Errorf("splitting simulated agent address: %w", err)
	}
	store, err := OpenStore(ctx, options.DSN, logger)
	if err != nil {
		return result, err
	}
	defer store.Cleanup() //nolint:errcheck // the run result is already complete

	setupStart := time.Now()
	if err := store.ResetInventory(ctx, inventory); err != nil {
		return result, err
	}
	if err := store.SeedInventory(ctx, inventory, "127.0.0.1"); err != nil {
		return result, err
	}
	result.Phases = append(result.Phases, Phase{Name: phaseFixtureSeed, DurationMS: elapsedMS(setupStart)})

	cfg := config.Config{NodeAgent: config.NodeAgentConfig{Port: port}, NodeHealthCheckTimeout: options.Timeout}
	nodeAgentClient, err := agent.NewClient(ctx, cfg, "", logger)
	if err != nil {
		return result, fmt.Errorf("creating node-agent client: %w", err)
	}
	reconciler := nodeagentreadiness.NewAgentReadinessController(
		cfg,
		logger,
		store.DB,
		nodeAgentClient,
	)
	transitionPhase, err := runAgentHealthSweep(ctx, inventory, options.Concurrency, reconciler, store)
	if err != nil {
		return result, fmt.Errorf("reconciling node-agent readiness: %w", err)
	}
	transitionPhase.Name = "node-agent-health-sweep"
	transitionPhase.Metrics["state"] = "transition"
	result.Phases = append(result.Phases, transitionPhase)

	for sweep := range options.SteadySweeps {
		steadyPhase, sweepErr := runAgentHealthSweep(ctx, inventory, options.Concurrency, reconciler, store)
		if sweepErr != nil {
			return result, fmt.Errorf("reconciling steady node-agent readiness sweep %d: %w", sweep+1, sweepErr)
		}
		steadyPhase.Name = fmt.Sprintf("node-agent-health-steady-sweep-%02d", sweep+1)
		steadyPhase.Metrics["state"] = "steady"
		result.Phases = append(result.Phases, steadyPhase)
	}
	result.Metrics["agentSimulator"] = simulator.Stats()
	result.Metrics["database"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	result.AddRuntimeMetrics()
	return result, nil
}

func runAgentHealthSweep(
	ctx context.Context,
	inventory Inventory,
	concurrency int,
	reconciler *nodeagentreadiness.AgentReadinessController,
	store *Store,
) (Phase, error) {
	databaseBefore := store.Bun.DBStats()
	keys := make(chan dbreconcile.Key)
	ordinals := make(map[uuid.UUID]int, inventory.NodeCount())
	for _, cluster := range inventory.Clusters {
		for _, node := range cluster.Nodes {
			ordinals[node.ID] = len(ordinals) + 1
		}
	}
	errorsChannel := make(chan error, inventory.NodeCount())
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for key := range keys {
				probeCtx := metadata.AppendToOutgoingContext(ctx, "x-perf-node-id", key.ID.String(), "x-perf-node-ordinal", strconv.Itoa(ordinals[key.ID]))
				if _, reconcileErr := reconciler.Reconcile(probeCtx, key); reconcileErr != nil {
					errorsChannel <- reconcileErr
				}
			}
		}()
	}

	started := time.Now()
	for _, cluster := range inventory.Clusters {
		for _, node := range cluster.Nodes {
			keys <- dbreconcile.NodeKey(node.ID)
		}
	}
	close(keys)
	workers.Wait()
	close(errorsChannel)
	if reconcileErr := <-errorsChannel; reconcileErr != nil {
		return Phase{}, reconcileErr
	}
	databaseAfter := store.Bun.DBStats()
	duration := time.Since(started)
	return Phase{
		DurationMS: elapsedMS(started),
		Metrics: map[string]any{
			metricNodes:            inventory.NodeCount(),
			"concurrency":          concurrency,
			"databaseQueries":      databaseAfter.Queries - databaseBefore.Queries,
			"databaseErrors":       databaseAfter.Errors - databaseBefore.Errors,
			"probesPerSecond":      float64(inventory.NodeCount()) / duration.Seconds(),
			"withinTwoMinuteSweep": duration <= 2*time.Minute,
		},
	}, nil
}

func waitForKubernetesReady(ctx context.Context, database db.DB, nodes []Node, status db.ConditionStatus, timeout time.Duration) error {
	return waitUntil(ctx, timeout, func() (bool, error) {
		for _, target := range nodes {
			node, err := database.GetNode(ctx, target.ID)
			if err != nil {
				return false, err
			}
			if node.Conditions.KubernetesReady.Status != status {
				return false, nil
			}
		}
		return true, nil
	})
}

func waitUntil(ctx context.Context, timeout time.Duration, condition func() (bool, error)) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out after %s", timeout)
		case <-ticker.C:
		}
	}
}

func postNodeEvent(ctx context.Context, server string, clusterIndex int, nodeName string, ready bool) error {
	payload, err := json.Marshal(map[string]any{"cluster": clusterIndex, "node": nodeName, "ready": ready})
	if err != nil {
		return fmt.Errorf("encoding Node event: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server+"/__admin/node-event", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating Node event request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("posting Node event: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("posting Node event returned %s: %s", response.Status, body)
	}
	return nil
}

func tenantAPIStats(ctx context.Context, server string) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server+"/__admin/stats", http.NoBody)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var stats map[string]any
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func databaseStats(stats bun.DBStats, pool sql.DBStats) map[string]any {
	return map[string]any{
		"queries":            stats.Queries,
		"errors":             stats.Errors,
		"maxOpenConnections": pool.MaxOpenConnections,
		"openConnections":    pool.OpenConnections,
		"inUseConnections":   pool.InUse,
		"idleConnections":    pool.Idle,
		"waitCount":          pool.WaitCount,
		"waitDurationNanos":  pool.WaitDuration.Nanoseconds(),
		"maxIdleClosed":      pool.MaxIdleClosed,
		"maxIdleTimeClosed":  pool.MaxIdleTimeClosed,
		"maxLifetimeClosed":  pool.MaxLifetimeClosed,
	}
}

func elapsedMS(start time.Time) float64 {
	value, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", float64(time.Since(start).Microseconds())/1000), 64)
	return value
}

func gatherControllerMetrics() (map[string]float64, error) {
	families, err := controllermetrics.Registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("gathering controller-runtime metrics: %w", err)
	}
	values := make(map[string]float64)
	for _, family := range families {
		name := family.GetName()
		if !strings.HasPrefix(name, "workqueue_") && !strings.HasPrefix(name, "controller_runtime_") {
			continue
		}
		for _, metric := range family.GetMetric() {
			if gauge := metric.GetGauge(); gauge != nil {
				values[name] += gauge.GetValue()
			} else if counter := metric.GetCounter(); counter != nil {
				values[name] += counter.GetValue()
			}
		}
	}
	return values, nil
}
