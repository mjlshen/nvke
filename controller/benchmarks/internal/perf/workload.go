// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"

	"github.com/dsx-ai-factory/nke/controller/internal/db"
)

func seedCertificateNodes(ctx context.Context, store *Store, inventory Inventory) error {
	// Use a separate legacy CP pool: ListNodes classifies pooled nodes using
	// their pool's control_plane flag, not the node flag.
	for _, cluster := range inventory.Clusters {
		var pool db.NodePool
		if err := store.Bun.NewSelect().Model(&pool).Where("id = ?", benchmarkChildID(cluster.ID, "pool", 0)).Scan(ctx); err != nil {
			return fmt.Errorf("loading fixture pool: %w", err)
		}
		var set db.NodeSet
		if err := store.Bun.NewSelect().Model(&set).Where("id = ?", *pool.CurrentNodeSetID).Scan(ctx); err != nil {
			return fmt.Errorf("loading fixture set: %w", err)
		}
		oldPoolID, oldSetID := pool.ID, set.ID
		pool.ID = benchmarkChildID(cluster.ID, "certificate-pool", 0)
		pool.Name += "-cp"
		pool.NodeNamePrefix += "-cp"
		pool.ControlPlane, pool.DesiredCount, pool.CurrentNodeSetID = true, 1, nil
		set.ID, set.NodePoolID, set.DesiredCount = benchmarkChildID(pool.ID, "set", 0), pool.ID, 1
		if _, err := store.Bun.NewInsert().Model(&pool).Exec(ctx); err != nil {
			return fmt.Errorf("inserting fixture CP pool: %w", err)
		}
		if _, err := store.Bun.NewInsert().Model(&set).Exec(ctx); err != nil {
			return fmt.Errorf("inserting fixture CP set: %w", err)
		}
		if _, err := store.Bun.NewUpdate().Model((*db.NodePool)(nil)).Set("current_node_set_id = ?", set.ID).Where("id = ?", pool.ID).Exec(ctx); err != nil {
			return fmt.Errorf("linking fixture CP set: %w", err)
		}
		if _, err := store.Bun.NewUpdate().TableExpr(metricNodes).Set("control_plane = true").Set("node_pool_id = ?", pool.ID).Set("node_set_id = ?", set.ID).Where("id = ?", cluster.Nodes[0].ID).Exec(ctx); err != nil {
			return fmt.Errorf("assigning fixture CP node: %w", err)
		}
		if _, err := store.Bun.NewUpdate().Model((*db.NodePool)(nil)).Set("desired_count = desired_count - 1").Where("id = ?", oldPoolID).Exec(ctx); err != nil {
			return fmt.Errorf("adjusting fixture worker pool: %w", err)
		}
		if _, err := store.Bun.NewUpdate().Model((*db.NodeSet)(nil)).Set("desired_count = desired_count - 1").Where("id = ?", oldSetID).Exec(ctx); err != nil {
			return fmt.Errorf("adjusting fixture worker set: %w", err)
		}
	}
	return nil
}

func postAdminJSON(ctx context.Context, server, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encoding simulator workload: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server+"/__admin/"+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating simulator workload request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("sending simulator workload: %w", err)
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		return fmt.Errorf("reading simulator response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("simulator workload returned %s", response.Status)
	}
	return nil
}

func setTenantOutage(ctx context.Context, inventory Inventory, outage bool) error {
	clusters := []int{}
	if outage {
		for _, cluster := range inventory.Clusters {
			clusters = append(clusters, cluster.Index)
		}
	}
	return postAdminJSON(ctx, inventory.Clusters[0].Server, "faults", map[string]any{"outageClusters": clusters, "dropWatches": outage})
}

// submitServingCSRs creates runtime-only public CSRs; private keys are discarded.
func submitServingCSRs(ctx context.Context, inventory Inventory) error {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("creating fixture CSR key: %w", err)
	}
	for _, cluster := range inventory.Clusters {
		node := cluster.Nodes[0]
		der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
			Subject: pkix.Name{CommonName: "system:node:" + node.Name, Organization: []string{"system:nodes"}}, DNSNames: []string{node.Name},
		}, key)
		if err != nil {
			return fmt.Errorf("creating fixture CSR: %w", err)
		}
		object := map[string]any{"spec": map[string]any{
			"request":    base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})),
			"signerName": "kubernetes.io/kubelet-serving", "username": "system:node:" + node.Name,
			"groups": []string{"system:nodes", "system:authenticated"}, "usages": []string{"digital signature", "key encipherment", "server auth"},
		}}
		if err := postAdminJSON(ctx, cluster.Server, "csr-event", map[string]any{"cluster": cluster.Index, "name": "cwe-perf-serving-" + node.Name, "object": object}); err != nil {
			return err
		}
	}
	return nil
}
