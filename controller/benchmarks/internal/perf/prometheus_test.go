// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCountingResponseWriterCountsSamplesAcrossWrites(t *testing.T) {
	t.Parallel()
	writer := newCountingResponseWriter()
	writer.WriteHeader(http.StatusAccepted)
	_, err := writer.Write([]byte("# HELP metric fixture\nmetric{node=\"a\"}"))
	require.NoError(t, err)
	_, err = writer.Write([]byte(" 1\nmetric{node=\"b\"} 2\n"))
	require.NoError(t, err)
	writer.finish()

	require.Equal(t, http.StatusAccepted, writer.status)
	require.EqualValues(t, 2, writer.series)
	require.EqualValues(t, 60, writer.bytes)
}

func TestPrometheusTopology(t *testing.T) {
	t.Parallel()
	inventory := Inventory{Clusters: []Cluster{
		{Nodes: make([]Node, 10)},
		{Nodes: make([]Node, 20)},
	}}
	topology := prometheusTopology(inventory, PrometheusOptions{NodePoolsPerCluster: 3, NodeSetsPerPool: 2})

	require.Equal(t, 2, topology["clusters"])
	require.Equal(t, 6, topology["nodePools"])
	require.Equal(t, 12, topology["nodeSets"])
	require.EqualValues(t, 7, topology["expectedQueriesPerScrape"])
	require.EqualValues(t, 554, topology["expectedSeriesPerScrape"])
}

func TestSummarizeScrapes(t *testing.T) {
	t.Parallel()
	metrics := summarizeScrapes([]scrapeObservation{
		{duration: 10 * time.Millisecond, bytes: 100, series: 5, status: http.StatusOK},
		{duration: 30 * time.Millisecond, bytes: 120, series: 4, status: http.StatusOK},
		{duration: 20 * time.Millisecond, bytes: 110, series: 5, status: http.StatusInternalServerError},
	}, 5)

	require.Equal(t, 3, metrics["scrapesCompleted"])
	require.Equal(t, 1, metrics["incompleteScrapes"])
	require.Equal(t, 1, metrics["statusFailures"])
	require.EqualValues(t, 110, metrics["responseBytesPerScrape"])
	require.InDelta(t, 20.0, metrics["medianDurationMs"], 0.001)
	require.InDelta(t, 30.0, metrics["p95DurationMs"], 0.001)
}

func TestQueryCategory(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		`SELECT "node_set".* FROM "node_sets" AS "node_set" JOIN node_pools np`: "node-set-list",
		`SELECT "node_pool".* FROM "node_pools" AS "node_pool" JOIN clusters c`: "node-pool-list",
		`SELECT "cluster".* FROM "clusters" AS "cluster"`:                       "cluster-list",
		`SELECT key FROM nodes n CROSS JOIN jsonb_each(n.conditions)`:           "node-conditions",
		`SELECT COUNT(*) FILTER (WHERE true) FROM node_pools`:                   "node-pool-aggregates",
		`SELECT * FROM nodes ORDER BY c.name, np.name, n.name`:                  "node-info",
		`SELECT state FROM nodes UNION ALL SELECT 'markedForRecycling'`:         "node-lifecycle",
	}
	for query, expected := range tests {
		require.Equal(t, expected, queryCategory(query))
	}
}
