// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// WorkerSample records one bounded observation of the worker, not the driver.
type WorkerSample struct {
	At         time.Time          `json:"at"`
	ElapsedMS  float64            `json:"elapsedMs"`
	Generation int                `json:"generation"`
	DurationMS float64            `json:"durationMs"`
	Metrics    map[string]float64 `json:"metrics"`
	Series     int                `json:"series"`
	Inventory  map[string]int     `json:"inventory"`
	Agent      map[string]any     `json:"agent"`
	TenantAPI  map[string]any     `json:"tenantAPI,omitempty"`
	Errors     []string           `json:"errors"`
	API        map[string]any     `json:"api"`
}

func waitInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// readMetricSamples retains labels so one busy controller cannot hide in totals.
// The worker serves Prometheus text without timestamps. Refuse truncated bodies
// and malformed samples rather than interpreting partial scrapes as recovery.
func readMetricSamples(reader io.Reader) (map[string]float64, int, error) {
	metrics := make(map[string]float64)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	series := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		separator := strings.LastIndexByte(line, ' ')
		if separator < 1 {
			return nil, series, fmt.Errorf("invalid metric sample")
		}
		value, err := strconv.ParseFloat(line[separator+1:], 64)
		if err != nil {
			return nil, series, fmt.Errorf("parsing metric value: %w", err)
		}
		series++
		key := line[:separator]
		family := strings.SplitN(key, "{", 2)[0]
		if family == "nke_controller_node_info" || family == "nke_controller_cluster_info" || family == "nke_controller_node_condition" {
			metrics["harness_samples_"+family]++
		}
		if hasMetricPrefix(key, []string{"go_", "process_", "workqueue_", "controller_runtime_", "nke_controller_db_"}) && !math.IsNaN(value) && !math.IsInf(value, 0) {
			metrics[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, series, fmt.Errorf("reading worker metrics: %w", err)
	}
	return metrics, series, nil
}

func observeWorker(ctx context.Context, url string) (map[string]float64, int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, 0, fmt.Errorf("creating metrics request: %w", err)
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, 0, fmt.Errorf("scraping worker: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("scraping worker returned %s", response.Status)
	}
	// Stream the entire response; large fixtures exceed the old 16 MiB cutoff.
	return readMetricSamples(response.Body)
}

func observeInventory(ctx context.Context, store *Store, inventory Inventory) (map[string]int, error) {
	ids := clusterIDs(inventory)
	var counts struct {
		Nodes              int
		AgentReady         int
		KubernetesReady    int
		Probed             int
		KubernetesObserved int
	}
	if err := store.Bun.NewSelect().TableExpr(metricNodes).
		ColumnExpr("COUNT(*) AS nodes").
		ColumnExpr("COUNT(*) FILTER (WHERE conditions->'nodeAgentReady'->>'status' = 'True') AS agent_ready").
		ColumnExpr("COUNT(*) FILTER (WHERE conditions->'kubernetesReady'->>'status' = 'True') AS kubernetes_ready").
		ColumnExpr("COUNT(*) FILTER (WHERE conditions->'nodeAgentReady'->>'message' <> 'CWE performance fixture') AS probed").
		ColumnExpr("COUNT(*) FILTER (WHERE conditions->'kubernetesReady'->>'status' IN ('True', 'False')) AS kubernetes_observed").
		Where("cluster_id IN (?)", bun.List(ids)).Scan(ctx, &counts); err != nil {
		return nil, fmt.Errorf("observing fixture nodes: %w", err)
	}
	return map[string]int{
		metricNodes: counts.Nodes, "agentReady": counts.AgentReady, "kubernetesReady": counts.KubernetesReady,
		"probed": counts.Probed, "kubernetesObserved": counts.KubernetesObserved,
	}, nil
}

func sampleWorker(ctx context.Context, options BlackBoxOptions, store *Store, inventory Inventory, simulator *AgentSimulator, start time.Time, generation, workerPID int) WorkerSample {
	started := time.Now()
	sample := WorkerSample{At: started.UTC(), ElapsedMS: elapsedMS(start), Generation: generation, Agent: simulator.Stats(), Errors: []string{}}
	var err error
	sample.Metrics, sample.Series, err = observeWorker(ctx, options.MetricsURL)
	if err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	}
	// On platforms where the Prometheus process collector omits RSS, sample the
	// actual worker PID. ps reports KiB on Linux and macOS.
	if sample.Metrics != nil {
		if _, exists := sample.Metrics["process_resident_memory_bytes"]; !exists {
			rss, rssErr := observeWorkerRSS(ctx, workerPID)
			if rssErr != nil {
				sample.Errors = append(sample.Errors, rssErr.Error())
			} else {
				sample.Metrics["process_resident_memory_bytes"] = rss
				sample.Metrics["harness_worker_rss_from_ps"] = 1
			}
		}
	}
	sample.Inventory, err = observeInventory(ctx, store, inventory)
	if err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	}
	sample.TenantAPI, err = tenantAPIStats(ctx, inventory.Clusters[0].Server)
	if err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	}
	sample.API, err = observeListAPI(ctx, options.ReadyURL)
	if err != nil {
		sample.Errors = append(sample.Errors, err.Error())
	}
	sample.DurationMS = elapsedMS(started)
	return sample
}

func observeWorkerRSS(ctx context.Context, pid int) (float64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid worker PID %d", pid)
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output() //nolint:gosec // read-only ps arguments contain only the spawned worker PID
	if err != nil {
		return 0, fmt.Errorf("sampling worker RSS: %w", err)
	}
	kib, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
	if err != nil {
		return 0, fmt.Errorf("parsing worker RSS: %w", err)
	}
	return kib * 1024, nil
}

func observeListAPI(ctx context.Context, readyURL string) (map[string]any, error) {
	target, err := url.Parse(readyURL)
	if err != nil {
		return nil, fmt.Errorf("parsing worker URL: %w", err)
	}
	target.Path = "/api/clusters"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating list request: %w", err)
	}
	started := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("listing clusters: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing clusters returned %s", response.Status)
	}
	var payload struct {
		Clusters []json.RawMessage `json:"clusters"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decoding cluster list: %w", err)
	}
	return map[string]any{"status": response.StatusCode, "clusters": len(payload.Clusters), "durationMs": elapsedMS(started)}, nil
}

func openSampleLog(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("creating sample log: %w", err)
	}
	return file, nil
}

func appendSample(file *os.File, sample WorkerSample) error {
	if err := json.NewEncoder(file).Encode(sample); err != nil {
		return fmt.Errorf("writing worker sample: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing worker sample: %w", err)
	}
	return nil
}
