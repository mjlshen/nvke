// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/google/uuid"
)

// Inventory is the deterministic set emitted by benchmarks/cwe_perf.
type Inventory struct {
	SchemaVersion int             `json:"schemaVersion"`
	Scenario      json.RawMessage `json:"scenario"`
	Clusters      []Cluster       `json:"clusters"`
}

// Cluster is one virtual tenant Kubernetes cluster.
type Cluster struct {
	Index  int       `json:"index"`
	ID     uuid.UUID `json:"id"`
	Name   string    `json:"name"`
	Token  string    `json:"token"`
	Server string    `json:"server"`
	Nodes  []Node    `json:"nodes"`
}

// Node is one virtual host and Kubernetes Node.
type Node struct {
	ID            uuid.UUID         `json:"id"`
	Name          string            `json:"name"`
	Ready         bool              `json:"ready"`
	Unschedulable bool              `json:"unschedulable"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// LoadInventory reads and validates a materialized inventory.
func LoadInventory(path string) (Inventory, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Inventory{}, fmt.Errorf("reading inventory: %w", err)
	}
	var inventory Inventory
	if err := json.Unmarshal(contents, &inventory); err != nil {
		return Inventory{}, fmt.Errorf("decoding inventory: %w", err)
	}
	if inventory.SchemaVersion != 1 {
		return Inventory{}, fmt.Errorf("unsupported inventory schema version %d", inventory.SchemaVersion)
	}
	if len(inventory.Clusters) == 0 {
		return Inventory{}, fmt.Errorf("inventory contains no clusters")
	}
	seenClusters := make(map[uuid.UUID]struct{}, len(inventory.Clusters))
	seenNodes := make(map[uuid.UUID]struct{})
	for _, cluster := range inventory.Clusters {
		if cluster.ID == uuid.Nil || cluster.Name == "" || cluster.Token == "" || cluster.Server == "" {
			return Inventory{}, fmt.Errorf("cluster %d is missing identity or access data", cluster.Index)
		}
		if _, exists := seenClusters[cluster.ID]; exists {
			return Inventory{}, fmt.Errorf("duplicate cluster ID %s", cluster.ID)
		}
		seenClusters[cluster.ID] = struct{}{}
		for _, node := range cluster.Nodes {
			if node.ID == uuid.Nil || node.Name == "" {
				return Inventory{}, fmt.Errorf("cluster %s contains a node without identity", cluster.Name)
			}
			if _, exists := seenNodes[node.ID]; exists {
				return Inventory{}, fmt.Errorf("duplicate node ID %s", node.ID)
			}
			seenNodes[node.ID] = struct{}{}
		}
	}
	return inventory, nil
}

// NodeCount returns the total number of nodes in the inventory.
func (i Inventory) NodeCount() int {
	total := 0
	for _, cluster := range i.Clusters {
		total += len(cluster.Nodes)
	}
	return total
}

// Kubeconfig returns deterministic access material for a virtual cluster.
func (c Cluster) Kubeconfig() []byte {
	return fmt.Appendf(nil, `apiVersion: v1
kind: Config
clusters:
- name: tenant
  cluster:
    server: %s
    insecure-skip-tls-verify: true
users:
- name: cwe-perf
  user:
    token: %s
contexts:
- name: tenant
  context:
    cluster: tenant
    user: cwe-perf
current-context: tenant
`, c.Server, c.Token)
}
