// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

func TestInitialNodesReconciled(t *testing.T) {
	t.Parallel()
	readyID := uuid.MustParse("94b65f94-d10b-590b-baca-a8278ea504a0")
	notReadyID := uuid.MustParse("e3be22a8-a1a8-5356-b241-2d45a9f82976")
	inventory := Inventory{Clusters: []Cluster{{Nodes: []Node{
		{ID: readyID, Ready: true},
		{ID: notReadyID, Ready: false},
	}}}}
	nodes := []db.Node{
		{ID: readyID, Conditions: db.NodeConditions{KubernetesReady: db.Condition{Status: db.ConditionStatusTrue, Message: simulatedNodeConditionMessage}}},
		{ID: notReadyID, Conditions: db.NodeConditions{KubernetesReady: db.Condition{Status: db.ConditionStatusFalse, Message: simulatedNodeConditionMessage}}},
	}

	require.True(t, initialNodesReconciled(inventory, nodes))
	require.False(t, initialNodesReconciled(inventory, nodes[:1]))
	nodes[1].Conditions.KubernetesReady.Message = "CWE performance fixture"
	require.False(t, initialNodesReconciled(inventory, nodes))
}

func TestControllerIsQuiescent(t *testing.T) {
	t.Parallel()
	require.False(t, controllerIsQuiescent(map[string]float64{}))
	require.True(t, controllerIsQuiescent(map[string]float64{
		"workqueue_depth":                   0,
		"controller_runtime_active_workers": 0,
	}))
	require.False(t, controllerIsQuiescent(map[string]float64{"workqueue_depth": 1}))
	require.False(t, controllerIsQuiescent(map[string]float64{"controller_runtime_active_workers": 1}))
}
