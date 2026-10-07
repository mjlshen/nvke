// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"

	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	dbfilters "github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/filters"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

const (
	sourceCoalesceSettleTime = 1100 * time.Millisecond
)

// NodeSourceOptions configures the DB-backed node-source seed and resync lane.
type NodeSourceOptions struct {
	DSN              string
	Concurrency      int
	CopiesPerCluster int
	Resyncs          int
	Timeout          time.Duration
}

type benchmarkNodeSource struct {
	source *dbwrapper.NodeEventSource[dbreconcile.Key]
	queue  workqueue.TypedRateLimitingInterface[dbreconcile.Key]
}

type memorySnapshot struct {
	heapAlloc   uint64
	heapObjects uint64
	totalAlloc  uint64
	mallocs     uint64
	gcCycles    uint32
	gcPause     uint64
}

// RunNodeSource exercises the production DB node source with the same
// per-cluster filters used by tenant controllers. Each source intentionally
// executes the current unfiltered ListNodes query before applying its filter.
func RunNodeSource(
	ctx context.Context,
	inventory Inventory,
	options NodeSourceOptions,
	logger *slog.Logger,
) (Result, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := NewResult("node-source", inventory)
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

	var consumed atomic.Int64
	progress := make(chan struct{}, 1)
	sources := make([]benchmarkNodeSource, 0, len(inventory.Clusters)*options.CopiesPerCluster)
	for range options.CopiesPerCluster {
		for clusterIndex := range inventory.Clusters {
			clusterID := inventory.Clusters[clusterIndex].ID
			queue := workqueue.NewTypedRateLimitingQueue(
				workqueue.DefaultTypedControllerRateLimiter[dbreconcile.Key](),
			)
			source := dbwrapper.NewNodeSource(store.DB, dbfilters.Node{ClusterID: &clusterID})
			sources = append(sources, benchmarkNodeSource{source: source, queue: queue})
			go drainNodeSource(queue, &consumed, progress)
		}
	}
	defer func() {
		for index := range sources {
			sources[index].queue.ShutDown()
		}
	}()

	eventsPerPass := int64(inventory.NodeCount() * options.CopiesPerCluster)
	rowsLoadedPerPass := int64(inventory.NodeCount() * len(sources))
	startupPhase, err := measureNodeSourcePhase(
		ctx,
		store,
		"db-node-source-startup",
		options.Timeout,
		options.Concurrency,
		sources,
		&consumed,
		progress,
		eventsPerPass,
		rowsLoadedPerPass,
		func(_ context.Context, source benchmarkNodeSource) error {
			return source.source.Start(ctx, source.queue)
		},
	)
	result.Phases = append(result.Phases, startupPhase)
	if err != nil {
		return result, err
	}

	for repetition := 1; repetition <= options.Resyncs; repetition++ {
		if err := waitForSourceCoalescers(ctx); err != nil {
			return result, err
		}
		target := eventsPerPass * int64(repetition+1)
		phase, resyncErr := measureNodeSourcePhase(
			ctx,
			store,
			fmt.Sprintf("db-node-source-resync-%02d", repetition),
			options.Timeout,
			options.Concurrency,
			sources,
			&consumed,
			progress,
			target,
			rowsLoadedPerPass,
			func(ctx context.Context, source benchmarkNodeSource) error {
				nodes, err := store.DB.ListNodes(ctx, db.NodeFilters{})
				if err != nil {
					return err
				}
				return source.source.Enqueue(ctx, nodes...)
			},
		)
		result.Phases = append(result.Phases, phase)
		if resyncErr != nil {
			return result, resyncErr
		}
	}

	result.Metrics["database"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	result.AddRuntimeMetrics()
	return result, nil
}

func drainNodeSource(
	queue workqueue.TypedRateLimitingInterface[dbreconcile.Key],
	consumed *atomic.Int64,
	progress chan<- struct{},
) {
	for {
		item, shutdown := queue.Get()
		if shutdown {
			return
		}
		queue.Forget(item)
		queue.Done(item)
		consumed.Add(1)
		select {
		case progress <- struct{}{}:
		default:
		}
	}
}

func measureNodeSourcePhase(
	ctx context.Context,
	store *Store,
	name string,
	timeout time.Duration,
	concurrency int,
	sources []benchmarkNodeSource,
	consumed *atomic.Int64,
	progress <-chan struct{},
	target int64,
	rowsLoaded int64,
	action func(context.Context, benchmarkNodeSource) error,
) (Phase, error) {
	runtime.GC()
	memoryBefore := readMemorySnapshot()
	databaseBefore := store.Bun.DBStats()
	poolBefore := store.SQL.Stats()
	started := time.Now()

	phaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	actionErr := runNodeSources(phaseCtx, sources, concurrency, action)
	var waitErr error
	if actionErr == nil {
		waitErr = waitForConsumed(phaseCtx, consumed, progress, target)
	}

	databaseAfter := store.Bun.DBStats()
	poolAfter := store.SQL.Stats()
	memoryAfter := readMemorySnapshot()
	phase := Phase{
		Name:       name,
		DurationMS: elapsedMS(started),
		Metrics: map[string]any{
			"sources":                  len(sources),
			"sourceConcurrency":        concurrency,
			"eventsConsumedCumulative": consumed.Load(),
			"targetEventsCumulative":   target,
			"estimatedRowsLoaded":      rowsLoaded,
			"databaseQueries":          databaseAfter.Queries - databaseBefore.Queries,
			"databaseErrors":           databaseAfter.Errors - databaseBefore.Errors,
			"databaseWaitCount":        poolAfter.WaitCount - poolBefore.WaitCount,
			"databaseWaitNanos":        (poolAfter.WaitDuration - poolBefore.WaitDuration).Nanoseconds(),
			"heapAllocDeltaBytes":      signedDelta(memoryAfter.heapAlloc, memoryBefore.heapAlloc),
			"heapObjectsDelta":         signedDelta(memoryAfter.heapObjects, memoryBefore.heapObjects),
			"totalAllocDeltaBytes":     memoryAfter.totalAlloc - memoryBefore.totalAlloc,
			"mallocsDelta":             memoryAfter.mallocs - memoryBefore.mallocs,
			"gcCycles":                 memoryAfter.gcCycles - memoryBefore.gcCycles,
			"gcPauseNanos":             memoryAfter.gcPause - memoryBefore.gcPause,
		},
	}
	return phase, errors.Join(actionErr, waitErr)
}

func runNodeSources(
	ctx context.Context,
	sources []benchmarkNodeSource,
	concurrency int,
	action func(context.Context, benchmarkNodeSource) error,
) error {
	jobs := make(chan benchmarkNodeSource)
	errorsChannel := make(chan error, len(sources))
	var workers sync.WaitGroup
	for range min(concurrency, len(sources)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for source := range jobs {
				if err := action(ctx, source); err != nil {
					errorsChannel <- err
				}
			}
		}()
	}
	for _, source := range sources {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			close(errorsChannel)
			return errors.Join(ctx.Err(), drainErrors(errorsChannel))
		case jobs <- source:
		}
	}
	close(jobs)
	workers.Wait()
	close(errorsChannel)
	return drainErrors(errorsChannel)
}

func drainErrors(errorsChannel <-chan error) error {
	var found []error
	for err := range errorsChannel {
		found = append(found, err)
	}
	return errors.Join(found...)
}

func waitForConsumed(
	ctx context.Context,
	consumed *atomic.Int64,
	progress <-chan struct{},
	target int64,
) error {
	for consumed.Load() < target {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %d node-source events after consuming %d: %w", target, consumed.Load(), ctx.Err())
		case <-progress:
		}
	}
	return nil
}

func waitForSourceCoalescers(ctx context.Context) error {
	timer := time.NewTimer(sourceCoalesceSettleTime)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readMemorySnapshot() memorySnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return memorySnapshot{
		heapAlloc: stats.HeapAlloc, heapObjects: stats.HeapObjects,
		totalAlloc: stats.TotalAlloc, mallocs: stats.Mallocs,
		gcCycles: stats.NumGC, gcPause: stats.PauseTotalNs,
	}
}

func signedDelta(after, before uint64) float64 {
	if after >= before {
		return float64(after - before)
	}
	return -float64(before - after)
}
