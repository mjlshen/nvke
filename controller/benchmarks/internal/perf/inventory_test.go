// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nkeproto "github.com/dsx-ai-factory/nke/agent/pkg/agent/proto"
)

func TestLoadInventory(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "inventory.json")
	contents := `{
  "schemaVersion": 1,
  "scenario": {"name":"test"},
  "clusters": [{
    "index": 0,
    "id": "a0d60d92-ee8d-52a3-8252-7755395594da",
    "name": "cluster-0000",
    "token": "cwe-perf-0",
    "server": "http://127.0.0.1:18080",
    "nodes": [{"id":"94b65f94-d10b-590b-baca-a8278ea504a0","name":"node-0","ready":true,"unschedulable":false}]
  }]
}`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	inventory, err := LoadInventory(path)
	require.NoError(t, err)
	require.Equal(t, 1, inventory.NodeCount())
	require.Contains(t, string(inventory.Clusters[0].Kubeconfig()), "token: cwe-perf-0")
}

func TestLoadInventoryRejectsDuplicateNodes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "inventory.json")
	contents := `{"schemaVersion":1,"scenario":{},"clusters":[
{"index":0,"id":"a0d60d92-ee8d-52a3-8252-7755395594da","name":"a","token":"a","server":"http://a","nodes":[{"id":"94b65f94-d10b-590b-baca-a8278ea504a0","name":"a"}]},
{"index":1,"id":"5fb43d3c-945b-57cd-9fc3-3f89b9335e1f","name":"b","token":"b","server":"http://b","nodes":[{"id":"94b65f94-d10b-590b-baca-a8278ea504a0","name":"b"}]}
]}`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	_, err := LoadInventory(path)
	require.ErrorContains(t, err, "duplicate node ID")
}

func TestAgentSimulator(t *testing.T) {
	t.Parallel()
	simulator, err := StartAgentSimulator(context.Background(), "127.0.0.1:0", 0, 2)
	require.NoError(t, err)
	t.Cleanup(simulator.Stop)

	connection, err := grpc.NewClient(simulator.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := nkeproto.NewNodeAgentClient(connection)
	response, err := client.GetKubeadmStatus(context.Background(), &nkeproto.KubeadmStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, "cwe-perf-agent/v1", response.GetAgentVersion())
	_, err = client.GetKubeadmStatus(context.Background(), &nkeproto.KubeadmStatusRequest{})
	require.Error(t, err)
	require.ErrorContains(t, err, "deterministic simulated failure")
	require.EqualValues(t, 2, simulator.Stats()["requests"])
}
