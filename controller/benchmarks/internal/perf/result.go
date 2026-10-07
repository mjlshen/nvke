// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"
)

// Result is the stable machine-readable output from a harness run.
type Result struct {
	SchemaVersion int             `json:"schemaVersion"`
	Mode          string          `json:"mode"`
	Scenario      json.RawMessage `json:"scenario"`
	StartedAt     time.Time       `json:"startedAt"`
	Environment   map[string]any  `json:"environment"`
	Phases        []Phase         `json:"phases"`
	Metrics       map[string]any  `json:"metrics"`
	Errors        []string        `json:"errors"`
}

// Phase captures one timed operation in the benchmark.
type Phase struct {
	Name       string         `json:"name"`
	DurationMS float64        `json:"durationMs"`
	Metrics    map[string]any `json:"metrics,omitempty"`
}

// NewResult initializes a result with reproducibility metadata.
func NewResult(mode string, inventory Inventory) Result {
	revision := "unknown"
	modified := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
			if setting.Key == "vcs.modified" {
				modified = setting.Value
			}
		}
	}
	return Result{
		SchemaVersion: 1,
		Mode:          mode,
		Scenario:      inventory.Scenario,
		StartedAt:     time.Now().UTC(),
		Environment: map[string]any{
			"goVersion":    runtime.Version(),
			"goMaxProcs":   runtime.GOMAXPROCS(0),
			"os":           runtime.GOOS,
			"architecture": runtime.GOARCH,
			"revision":     revision,
			"vcsModified":  modified,
		},
		Phases:  []Phase{},
		Metrics: map[string]any{},
		Errors:  []string{},
	}
}

// AddRuntimeMetrics records bounded process-level Go metrics.
func (r *Result) AddRuntimeMetrics() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	r.Metrics["runtime"] = map[string]any{
		"goroutines":        runtime.NumGoroutine(),
		"heapAllocBytes":    stats.HeapAlloc,
		"heapObjects":       stats.HeapObjects,
		"totalAllocBytes":   stats.TotalAlloc,
		"gcCycles":          stats.NumGC,
		"gcPauseTotalNanos": stats.PauseTotalNs,
	}
}

// WriteResult writes JSON atomically enough for a single-process benchmark.
func WriteResult(path string, result Result) error {
	contents, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding result: %w", err)
	}
	contents = append(contents, '\n')
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		return fmt.Errorf("writing result: %w", err)
	}
	return nil
}
