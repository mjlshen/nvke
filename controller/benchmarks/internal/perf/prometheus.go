// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uptrace/bun"

	"github.com/dsx-ai-factory/nke/controller/internal/dbmetrics"
)

// PrometheusOptions configures the production DB metrics collector lane.
type PrometheusOptions struct {
	DSN                 string
	NodePoolsPerCluster int
	NodeSetsPerPool     int
	WarmupScrapes       int
	Scrapes             int
	Concurrency         int
	Timeout             time.Duration
}

type scrapeObservation struct {
	duration time.Duration
	bytes    int64
	series   int64
	status   int
}

type queryProfile struct {
	mu      sync.Mutex
	metrics map[string]queryProfileMetric
}

type queryProfileMetric struct {
	count   uint64
	total   time.Duration
	maximum time.Duration
}

// RunPrometheus measures complete Prometheus text scrapes from the production
// DB-backed collector over a materialized PostgreSQL inventory.
func RunPrometheus(ctx context.Context, inventory Inventory, options PrometheusOptions, logger *slog.Logger) (Result, error) {
	result := NewResult("prometheus", inventory)
	store, err := OpenStore(ctx, options.DSN, logger)
	if err != nil {
		return result, err
	}
	defer store.Cleanup() //nolint:errcheck // the run result is already complete

	setupStart := time.Now()
	if err := store.ResetInventory(ctx, inventory); err != nil {
		return result, err
	}
	if err := store.SeedMetricsInventory(ctx, inventory, options.NodePoolsPerCluster, options.NodeSetsPerPool); err != nil {
		return result, err
	}
	topology := prometheusTopology(inventory, options)
	result.Phases = append(result.Phases, Phase{
		Name:       phaseFixtureSeed,
		DurationMS: elapsedMS(setupStart),
		Metrics:    topology,
	})

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(dbmetrics.NewCollector(logger.With("component", "dbmetrics"), store.DB)); err != nil {
		return result, fmt.Errorf("registering DB metrics collector: %w", err)
	}
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	queries := &queryProfile{metrics: make(map[string]queryProfileMetric)}
	store.Bun.AddQueryHook(queries)

	if options.WarmupScrapes > 0 {
		warmupCtx, cancel := context.WithTimeout(ctx, options.Timeout)
		warmups, warmupErr := runPrometheusScrapes(warmupCtx, handler, options.WarmupScrapes, min(options.Concurrency, options.WarmupScrapes))
		cancel()
		result.Metrics["warmup"] = summarizeScrapes(warmups, topology["expectedSeriesPerScrape"].(int64))
		if warmupErr != nil {
			return result, fmt.Errorf("warming Prometheus collector: %w", warmupErr)
		}
	}
	queries.reset()

	runtime.GC()
	memoryBefore := readMemorySnapshot()
	databaseBefore := store.Bun.DBStats()
	poolBefore := store.SQL.Stats()
	started := time.Now()
	measurementCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	observations, scrapeErr := runPrometheusScrapes(measurementCtx, handler, options.Scrapes, options.Concurrency)
	cancel()
	databaseAfter := store.Bun.DBStats()
	poolAfter := store.SQL.Stats()
	memoryAfter := readMemorySnapshot()

	expectedSeries := topology["expectedSeriesPerScrape"].(int64)
	metrics := summarizeScrapes(observations, expectedSeries)
	metrics["scrapesRequested"] = options.Scrapes
	metrics["scrapeConcurrency"] = options.Concurrency
	metrics["databaseQueries"] = databaseAfter.Queries - databaseBefore.Queries
	metrics["expectedDatabaseQueries"] = int64(options.Scrapes) * topology["expectedQueriesPerScrape"].(int64)
	metrics["databaseErrors"] = databaseAfter.Errors - databaseBefore.Errors
	metrics["databaseWaitCount"] = poolAfter.WaitCount - poolBefore.WaitCount
	metrics["databaseWaitNanos"] = (poolAfter.WaitDuration - poolBefore.WaitDuration).Nanoseconds()
	metrics["heapAllocDeltaBytes"] = signedDelta(memoryAfter.heapAlloc, memoryBefore.heapAlloc)
	metrics["heapObjectsDelta"] = signedDelta(memoryAfter.heapObjects, memoryBefore.heapObjects)
	metrics["totalAllocDeltaBytes"] = memoryAfter.totalAlloc - memoryBefore.totalAlloc
	metrics["mallocsDelta"] = memoryAfter.mallocs - memoryBefore.mallocs
	metrics["gcCycles"] = memoryAfter.gcCycles - memoryBefore.gcCycles
	metrics["gcPauseNanos"] = memoryAfter.gcPause - memoryBefore.gcPause
	metrics["queryProfile"] = queries.snapshot(len(observations))
	if len(observations) > 0 {
		completed := uint64(len(observations))
		metrics["databaseQueriesPerScrape"] = (databaseAfter.Queries - databaseBefore.Queries) / uint32(completed)
		metrics["totalAllocBytesPerScrape"] = (memoryAfter.totalAlloc - memoryBefore.totalAlloc) / completed
		metrics["mallocsPerScrape"] = (memoryAfter.mallocs - memoryBefore.mallocs) / completed
	}
	result.Phases = append(result.Phases, Phase{Name: "prometheus-scrapes", DurationMS: elapsedMS(started), Metrics: metrics})
	result.Metrics["topology"] = topology
	result.Metrics["database"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	result.AddRuntimeMetrics()
	return result, scrapeErr
}

func (p *queryProfile) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (p *queryProfile) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	duration := time.Since(event.StartTime)
	category := queryCategory(event.Query)
	p.mu.Lock()
	metric := p.metrics[category]
	metric.count++
	metric.total += duration
	metric.maximum = max(metric.maximum, duration)
	p.metrics[category] = metric
	p.mu.Unlock()
}

func (p *queryProfile) reset() {
	p.mu.Lock()
	p.metrics = make(map[string]queryProfileMetric)
	p.mu.Unlock()
}

func (p *queryProfile) snapshot(scrapes int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[string]any, len(p.metrics))
	for category, metric := range p.metrics {
		entry := map[string]any{
			"count":           metric.count,
			"totalDurationMs": durationMS(metric.total),
			"maxDurationMs":   durationMS(metric.maximum),
		}
		if metric.count > 0 {
			entry["averageDurationMs"] = durationMS(metric.total / time.Duration(metric.count))
		}
		if scrapes > 0 {
			entry["countPerScrape"] = float64(metric.count) / float64(scrapes)
			entry["durationMsPerScrape"] = durationMS(metric.total) / float64(scrapes)
		}
		result[category] = entry
	}
	return result
}

func queryCategory(query string) string {
	switch {
	case strings.Contains(query, `FROM "node_sets" AS "node_set"`):
		return "node-set-list"
	case strings.Contains(query, `FROM "node_pools" AS "node_pool"`):
		return "node-pool-list"
	case strings.Contains(query, `FROM "clusters" AS "cluster"`):
		return "cluster-list"
	case strings.Contains(query, "jsonb_each(n.conditions)"):
		return "node-conditions"
	case strings.Contains(query, "COUNT(*) FILTER"):
		return "node-pool-aggregates"
	case strings.Contains(query, "ORDER BY c.name, np.name, n.name"):
		return "node-info"
	case strings.Contains(query, "UNION ALL") && strings.Contains(query, "markedForRecycling"):
		return "node-lifecycle"
	default:
		return "other"
	}
}

func prometheusTopology(inventory Inventory, options PrometheusOptions) map[string]any {
	clusters := len(inventory.Clusters)
	pools := clusters * options.NodePoolsPerCluster
	nodeSets := pools * options.NodeSetsPerPool
	nodes := inventory.NodeCount()
	conditionsPerNode := len(benchmarkNodeConditions().All())
	return map[string]any{
		"clusters":          clusters,
		"nodePools":         pools,
		"nodeSets":          nodeSets,
		"nodes":             nodes,
		"conditionsPerNode": conditionsPerNode,
		// Conditions, pool aggregates, node info, topology (clusters/pools/sets), and lifecycle.
		"expectedQueriesPerScrape": int64(7),
		// Conditions + node info + four pool aggregates + cluster/pool/set info.
		// The fixture has no active lifecycle condition, so it emits no lifecycle series.
		"expectedSeriesPerScrape": int64(nodes*conditionsPerNode + nodes + pools*4 + clusters + pools + nodeSets),
	}
}

func runPrometheusScrapes(ctx context.Context, handler http.Handler, count, concurrency int) ([]scrapeObservation, error) {
	var next atomic.Int64
	var workers sync.WaitGroup
	results := make(chan scrapeObservation, count)
	workerCount := min(count, concurrency)
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				if ctx.Err() != nil {
					return
				}
				results <- scrapePrometheus(ctx, handler)
			}
		}()
	}
	workers.Wait()
	close(results)
	observations := make([]scrapeObservation, 0, count)
	for observation := range results {
		observations = append(observations, observation)
	}
	if len(observations) < count {
		return observations, fmt.Errorf("completed %d of %d scrapes: %w", len(observations), count, ctx.Err())
	}
	var statusErrors []error
	for index, observation := range observations {
		if observation.status != http.StatusOK {
			statusErrors = append(statusErrors, fmt.Errorf("scrape %d returned HTTP %d", index+1, observation.status))
		}
	}
	return observations, errors.Join(statusErrors...)
}

func scrapePrometheus(ctx context.Context, handler http.Handler) scrapeObservation {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://cwe-perf.invalid/metrics", http.NoBody)
	if err != nil {
		return scrapeObservation{status: http.StatusInternalServerError}
	}
	writer := newCountingResponseWriter()
	started := time.Now()
	handler.ServeHTTP(writer, request)
	writer.finish()
	return scrapeObservation{
		duration: time.Since(started),
		bytes:    writer.bytes,
		series:   writer.series,
		status:   writer.status,
	}
}

func summarizeScrapes(observations []scrapeObservation, expectedSeries int64) map[string]any {
	durations := make([]time.Duration, 0, len(observations))
	var totalBytes int64
	var totalSeries int64
	incomplete := 0
	statusFailures := 0
	for _, observation := range observations {
		durations = append(durations, observation.duration)
		totalBytes += observation.bytes
		totalSeries += observation.series
		if observation.series != expectedSeries {
			incomplete++
		}
		if observation.status != http.StatusOK {
			statusFailures++
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	metrics := map[string]any{
		"scrapesCompleted":   len(observations),
		"totalResponseBytes": totalBytes,
		"totalSeries":        totalSeries,
		"incompleteScrapes":  incomplete,
		"statusFailures":     statusFailures,
	}
	if len(observations) == 0 {
		return metrics
	}
	metrics["responseBytesPerScrape"] = totalBytes / int64(len(observations))
	metrics["seriesPerScrape"] = totalSeries / int64(len(observations))
	metrics["minDurationMs"] = durationMS(durations[0])
	metrics["medianDurationMs"] = durationMS(durations[(len(durations)-1)/2])
	metrics["p95DurationMs"] = durationMS(durations[percentileIndex(len(durations), 95)])
	metrics["maxDurationMs"] = durationMS(durations[len(durations)-1])
	return metrics
}

func percentileIndex(length, percentile int) int {
	index := (length*percentile + 99) / 100
	return max(0, min(length-1, index-1))
}

func durationMS(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}

type countingResponseWriter struct {
	header  http.Header
	status  int
	bytes   int64
	series  int64
	partial []byte
}

func newCountingResponseWriter() *countingResponseWriter {
	return &countingResponseWriter{header: make(http.Header), status: http.StatusOK}
}

func (w *countingResponseWriter) Header() http.Header { return w.header }

func (w *countingResponseWriter) WriteHeader(status int) { w.status = status }

func (w *countingResponseWriter) Write(contents []byte) (int, error) {
	w.bytes += int64(len(contents))
	w.partial = append(w.partial, contents...)
	for {
		newline := bytes.IndexByte(w.partial, '\n')
		if newline < 0 {
			break
		}
		w.countLine(w.partial[:newline])
		w.partial = w.partial[newline+1:]
	}
	return len(contents), nil
}

func (w *countingResponseWriter) finish() {
	if len(w.partial) > 0 {
		w.countLine(w.partial)
		w.partial = nil
	}
}

func (w *countingResponseWriter) countLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) > 0 && line[0] != '#' {
		w.series++
	}
}
