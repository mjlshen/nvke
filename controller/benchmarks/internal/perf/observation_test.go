// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricObservationsPreserveControllerLabelsAndCountAllSeries(t *testing.T) {
	t.Parallel()
	metrics, series, err := readMetricSamples(strings.NewReader(`# HELP workqueue_depth depth
workqueue_depth{name="healthy"} 0
workqueue_depth{name="busy controller"} 5000
go_goroutines 42
nke_controller_node_info{node="fixture"} 1
go_gc_duration_seconds{quantile="0"} NaN
`))
	require.NoError(t, err)
	require.Equal(t, 5, series)
	require.InDelta(t, 5000.0, metrics[`workqueue_depth{name="busy controller"}`], 0.001)
	require.InDelta(t, 0.0, metrics[`workqueue_depth{name="healthy"}`], 0.001)
	require.NotContains(t, metrics, `go_gc_duration_seconds{quantile="0"}`)
}

func TestMetricObservationRejectsMalformedSample(t *testing.T) {
	t.Parallel()
	_, _, err := readMetricSamples(strings.NewReader("go_goroutines invalid\n"))
	require.Error(t, err)
}

func TestPacedScrapesHonorCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = writer.Write([]byte("fixture 1\n"))
		cancel()
	})
	started := time.Now()
	observations, err := runPacedPrometheusScrapes(ctx, handler, 10, 1, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, observations, 1)
	require.EqualValues(t, 1, calls.Load())
	require.Less(t, time.Since(started), time.Second)
}

func TestOnePacedScrapeDoesNotWaitAfterCompletion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte("fixture 1\n")) })
	observations, err := runPacedPrometheusScrapes(ctx, handler, 1, 1, time.Hour)
	require.NoError(t, err)
	require.Len(t, observations, 1)
}

func TestWorkerRSSObservationSamplesSelectedProcess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rss, err := observeWorkerRSS(ctx, os.Getpid())
	require.NoError(t, err)
	require.Positive(t, rss)
	_, err = observeWorkerRSS(ctx, -1)
	require.Error(t, err)
}
