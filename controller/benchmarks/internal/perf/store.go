// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/dsx-ai-factory/nke/controller/internal/controllers/dbreconcile/dbwrapper"
	"github.com/dsx-ai-factory/nke/controller/internal/db"
	"github.com/dsx-ai-factory/nke/controller/internal/db/migrations"
)

const benchmarkFixtureKind = "CWEPerformanceFixture"

// Store owns the PostgreSQL handles used by a benchmark run.
type Store struct {
	Bun     *bun.DB
	SQL     *sql.DB
	DB      *dbwrapper.DB
	Cleanup func() error
}

// OpenStore connects to PostgreSQL and applies the production migrations.
func OpenStore(ctx context.Context, dsn string, logger *slog.Logger) (*Store, error) {
	sqlDB := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	sqlDB.SetMaxOpenConns(32)
	sqlDB.SetMaxIdleConns(32)
	bunDB := bun.NewDB(sqlDB, pgdialect.New())
	if err := bunDB.PingContext(ctx); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("pinging benchmark database: %w", err)
	}
	if err := migrations.Migrate(ctx, bunDB); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("migrating benchmark database: %w", err)
	}
	rawDB := db.NewDB(bunDB) //nolint:forbidigo // this harness store is its composition root
	return &Store{Bun: bunDB, SQL: sqlDB, DB: dbwrapper.New(rawDB, logger), Cleanup: bunDB.Close}, nil
}

// ResetInventory removes every row owned by the benchmark, including clusters
// left by a larger preceding scenario. The exact-ID condition also cleans up
// fixtures produced by an older harness that did not persist the marker.
func (s *Store) ResetInventory(ctx context.Context, inventory Inventory) error {
	clusterIDs := make([]any, 0, len(inventory.Clusters))
	for _, cluster := range inventory.Clusters {
		clusterIDs = append(clusterIDs, cluster.ID)
	}
	if len(clusterIDs) == 0 {
		return nil
	}
	if _, err := s.Bun.NewDelete().Model((*db.Node)(nil)).
		Where(
			"cluster_id IN (SELECT id FROM clusters WHERE capi->'infra_cluster'->>'kind' = ? OR id IN (?))",
			benchmarkFixtureKind,
			bun.In(clusterIDs),
		).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting benchmark nodes: %w", err)
	}
	if _, err := s.Bun.NewUpdate().Model((*db.NodePool)(nil)).
		Set("current_node_set_id = NULL").
		Where(
			"cluster_id IN (SELECT id FROM clusters WHERE capi->'infra_cluster'->>'kind' = ? OR id IN (?))",
			benchmarkFixtureKind,
			bun.In(clusterIDs),
		).
		Exec(ctx); err != nil {
		return fmt.Errorf("clearing benchmark current node sets: %w", err)
	}
	if _, err := s.Bun.NewDelete().Model((*db.NodeSet)(nil)).
		Where(
			"node_pool_id IN (SELECT id FROM node_pools WHERE cluster_id IN (SELECT id FROM clusters WHERE capi->'infra_cluster'->>'kind' = ? OR id IN (?)))",
			benchmarkFixtureKind,
			bun.In(clusterIDs),
		).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting benchmark node sets: %w", err)
	}
	if _, err := s.Bun.NewDelete().Model((*db.NodePool)(nil)).
		Where(
			"cluster_id IN (SELECT id FROM clusters WHERE capi->'infra_cluster'->>'kind' = ? OR id IN (?))",
			benchmarkFixtureKind,
			bun.In(clusterIDs),
		).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting benchmark node pools: %w", err)
	}
	if _, err := s.Bun.NewDelete().Model((*db.Cluster)(nil)).
		Where("capi->'infra_cluster'->>'kind' = ? OR id IN (?)", benchmarkFixtureKind, bun.In(clusterIDs)).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting benchmark clusters: %w", err)
	}
	return nil
}

// SeedInventory inserts clusters and nodes in bounded batches. Setup time is
// reported separately and must not be included in cold-start measurements.
func (s *Store) SeedInventory(ctx context.Context, inventory Inventory, agentAddress string) error {
	now := time.Now().UTC()
	clusters := make([]db.Cluster, 0, len(inventory.Clusters))
	nodes := make([]db.Node, 0, inventory.NodeCount())
	for _, sourceCluster := range inventory.Clusters {
		clusters = append(clusters, db.Cluster{
			ID:                sourceCluster.ID,
			Name:              sourceCluster.Name,
			KubernetesVersion: "v1.35.0",
			ProviderType:      db.ProviderTypeCAPI,
			Capi: &db.CAPIClusterProvider{InfraCluster: json.RawMessage(
				`{"apiVersion":"performance.nke.nvidia.com/v1alpha1","kind":"CWEPerformanceFixture"}`,
			)},
			Conditions: db.ClusterConditions{},
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		for _, sourceNode := range sourceCluster.Nodes {
			nodes = append(nodes, db.Node{
				ID:         sourceNode.ID,
				ClusterID:  sourceCluster.ID,
				Name:       sourceNode.Name,
				IP:         agentAddress,
				Conditions: benchmarkNodeConditions(),
				CreatedAt:  now,
				UpdatedAt:  now,
			})
		}
	}
	if _, err := s.Bun.NewInsert().Model(&clusters).Exec(ctx); err != nil {
		return fmt.Errorf("inserting benchmark clusters: %w", err)
	}
	for start := 0; start < len(nodes); start += 500 {
		end := min(start+500, len(nodes))
		batch := nodes[start:end]
		if _, err := s.Bun.NewInsert().Model(&batch).Exec(ctx); err != nil {
			return fmt.Errorf("inserting benchmark node batch at %d: %w", start, err)
		}
	}
	return nil
}

// SeedMetricsInventory inserts a complete cluster, node-pool, node-set, and
// node topology for exercising the production database-backed metrics
// collector. Nodes are distributed evenly across pools and retained node sets.
func (s *Store) SeedMetricsInventory(ctx context.Context, inventory Inventory, poolsPerCluster, nodeSetsPerPool int) error {
	now := time.Now().UTC()
	clusters := make([]db.Cluster, 0, len(inventory.Clusters))
	nodePools := make([]db.NodePool, 0, len(inventory.Clusters)*poolsPerCluster)
	nodeSets := make([]db.NodeSet, 0, len(inventory.Clusters)*poolsPerCluster*nodeSetsPerPool)
	nodes := make([]db.Node, 0, inventory.NodeCount())

	for _, sourceCluster := range inventory.Clusters {
		clusters = append(clusters, benchmarkCluster(sourceCluster, now))
		poolNodeCounts := make([]int, poolsPerCluster)
		setNodeCounts := make([][]int, poolsPerCluster)
		poolIDs := make([]uuid.UUID, poolsPerCluster)
		setIDs := make([][]uuid.UUID, poolsPerCluster)
		for poolIndex := range poolsPerCluster {
			poolIDs[poolIndex] = benchmarkChildID(sourceCluster.ID, "pool", poolIndex)
			setNodeCounts[poolIndex] = make([]int, nodeSetsPerPool)
			setIDs[poolIndex] = make([]uuid.UUID, nodeSetsPerPool)
			for setIndex := range nodeSetsPerPool {
				setIDs[poolIndex][setIndex] = benchmarkChildID(poolIDs[poolIndex], "set", setIndex)
			}
		}

		for nodeIndex, sourceNode := range sourceCluster.Nodes {
			poolIndex := nodeIndex % poolsPerCluster
			setIndex := (nodeIndex / poolsPerCluster) % nodeSetsPerPool
			poolNodeCounts[poolIndex]++
			setNodeCounts[poolIndex][setIndex]++
			nodes = append(nodes, db.Node{
				ID:           sourceNode.ID,
				NodePoolID:   poolIDs[poolIndex],
				NodeSetID:    setIDs[poolIndex][setIndex],
				ClusterID:    sourceCluster.ID,
				Name:         sourceNode.Name,
				IP:           "192.0.2.1",
				AgentVersion: "cwe-perf-agent/v1",
				Conditions:   benchmarkNodeConditions(),
				CreatedAt:    now,
				UpdatedAt:    now,
			})
		}

		for poolIndex, poolID := range poolIDs {
			nodePools = append(nodePools, db.NodePool{
				ID:                   poolID,
				ClusterID:            sourceCluster.ID,
				Name:                 fmt.Sprintf("%s-pool-%04d", sourceCluster.Name, poolIndex),
				NodeNamePrefix:       fmt.Sprintf("%s-p%04d", sourceCluster.Name, poolIndex),
				SystemReservedMemory: "16Gi",
				KubeReservedMemory:   "1Gi",
				NodeLabels:           []string{},
				Conditions:           db.NodePoolConditions{},
				KubernetesVersion:    "v1.35.0",
				DesiredCount:         poolNodeCounts[poolIndex],
				CreatedAt:            now,
				UpdatedAt:            now,
				Capi: &db.CAPINodePoolProvider{InfraMachineTemplate: json.RawMessage(
					`{"apiVersion":"performance.nke.nvidia.com/v1alpha1","kind":"MachineTemplate"}`,
				)},
			})
			for setIndex, setID := range setIDs[poolIndex] {
				nodeSets = append(nodeSets, db.NodeSet{
					ID:                   setID,
					NodePoolID:           poolID,
					DesiredCount:         setNodeCounts[poolIndex][setIndex],
					KubernetesVersion:    "v1.35.0",
					SystemReservedMemory: "16Gi",
					KubeReservedMemory:   "1Gi",
					CreatedAt:            now.Add(time.Duration(setIndex) * time.Second),
					UpdatedAt:            now.Add(time.Duration(setIndex) * time.Second),
					Capi: &db.CAPINodePoolProvider{InfraMachineTemplate: json.RawMessage(
						`{"apiVersion":"performance.nke.nvidia.com/v1alpha1","kind":"MachineTemplate"}`,
					)},
				})
			}
		}
	}

	if _, err := s.Bun.NewInsert().Model(&clusters).Exec(ctx); err != nil {
		return fmt.Errorf("inserting benchmark clusters: %w", err)
	}
	// current_node_set_id cannot reference rows that have not been inserted yet.
	// Insert pools without the pointer, insert the sets, then update each pool to
	// its deterministic first set.
	if err := insertBatches(ctx, s.Bun, nodePools, 500, "node pool"); err != nil {
		return err
	}
	if err := insertBatches(ctx, s.Bun, nodeSets, 500, "node set"); err != nil {
		return err
	}
	if _, err := s.Bun.NewUpdate().Model((*db.NodePool)(nil)).
		Set("current_node_set_id = (SELECT ns.id FROM node_sets ns WHERE ns.node_pool_id = node_pool.id ORDER BY ns.created_at, ns.id::text LIMIT 1)").
		Where("cluster_id IN (?)", bun.In(clusterIDs(inventory))).
		Exec(ctx); err != nil {
		return fmt.Errorf("setting benchmark current node sets: %w", err)
	}
	if err := insertBatches(ctx, s.Bun, nodes, 500, "node"); err != nil {
		return err
	}
	return nil
}

func benchmarkCluster(source Cluster, now time.Time) db.Cluster {
	return db.Cluster{
		ID:                source.ID,
		Name:              source.Name,
		KubernetesVersion: "v1.35.0",
		ProviderType:      db.ProviderTypeCAPI,
		Capi: &db.CAPIClusterProvider{InfraCluster: json.RawMessage(
			`{"apiVersion":"performance.nke.nvidia.com/v1alpha1","kind":"CWEPerformanceFixture"}`,
		)},
		Conditions: db.ClusterConditions{},
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func benchmarkChildID(parent uuid.UUID, kind string, index int) uuid.UUID {
	return uuid.NewSHA1(parent, fmt.Appendf(nil, "%s-%d", kind, index))
}

func clusterIDs(inventory Inventory) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(inventory.Clusters))
	for _, cluster := range inventory.Clusters {
		ids = append(ids, cluster.ID)
	}
	return ids
}

func insertBatches[T any](ctx context.Context, database *bun.DB, rows []T, size int, kind string) error {
	for start := 0; start < len(rows); start += size {
		end := min(start+size, len(rows))
		batch := rows[start:end]
		if _, err := database.NewInsert().Model(&batch).Exec(ctx); err != nil {
			return fmt.Errorf("inserting benchmark %s batch at %d: %w", kind, start, err)
		}
	}
	return nil
}

func benchmarkNodeConditions() db.NodeConditions {
	unknown := db.NewCondition(false, "CWE performance fixture")
	conditions := db.NodeConditions{
		Pending:                 unknown,
		Provisioning:            unknown,
		ReadyToJoin:             unknown,
		Joined:                  unknown,
		Provisioned:             unknown,
		Deleting:                unknown,
		KubernetesReady:         unknown,
		KubernetesUnschedulable: unknown,
		NodeAgentReady:          unknown,
		Rebooting:               unknown,
		MarkedForRecycling:      unknown,
		MarkedForRepair:         unknown,
		TeleportRegistered:      unknown,
	}
	conditions.KubernetesReady.Status = db.ConditionStatusUnknown
	conditions.KubernetesUnschedulable.Status = db.ConditionStatusUnknown
	return conditions
}
