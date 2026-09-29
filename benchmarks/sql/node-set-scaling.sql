-- SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Required psql variables:
--   pools          number of node pools
--   sets_per_pool  retained node sets per pool
--   nodes          total nodes, assigned round-robin across the node sets
--   indexed        true to add the candidate foreign-key indexes
--   plans          true to emit representative EXPLAIN plans
--   repetitions    number of measured traversal repetitions

\set ON_ERROR_STOP on

DROP FUNCTION IF EXISTS cwe_perf_metric_traversal();
DROP FUNCTION IF EXISTS cwe_perf_reconcile_traversal();
DROP FUNCTION IF EXISTS cwe_perf_measure_metric(integer);
DROP FUNCTION IF EXISTS cwe_perf_measure_reconcile(integer);

TRUNCATE TABLE nodes, node_sets, node_pools, clusters CASCADE;

INSERT INTO clusters (
    id,
    name,
    kubernetes_version,
    provider_type,
    capi,
    conditions
) VALUES (
    '00000000-0000-0000-0000-000000000001',
    'cwe-perf-query-scaling',
    'v1.35.0',
    'capi',
    '{"infra_cluster":{"apiVersion":"performance.nke.nvidia.com/v1alpha1","kind":"CWEPerformanceFixture"}}',
    '{}'
);

INSERT INTO node_pools (
    id,
    cluster_id,
    name,
    node_name_prefix,
    control_plane,
    node_labels,
    conditions,
    capi
)
SELECT
    (
        substr(md5('pool/' || pool_number), 1, 8) || '-' ||
        substr(md5('pool/' || pool_number), 9, 4) || '-' ||
        substr(md5('pool/' || pool_number), 13, 4) || '-' ||
        substr(md5('pool/' || pool_number), 17, 4) || '-' ||
        substr(md5('pool/' || pool_number), 21, 12)
    )::uuid,
    '00000000-0000-0000-0000-000000000001',
    'pool-' || lpad(pool_number::text, 6, '0'),
    'node-' || lpad(pool_number::text, 6, '0'),
    false,
    ARRAY[]::text[],
    '{}',
    '{"infra_machine_template":{}}'
FROM generate_series(1, :pools) AS pool_number;

INSERT INTO node_sets (
    id,
    node_pool_id,
    capi
)
SELECT
    (
        substr(md5('set/' || pool_number || '/' || set_number), 1, 8) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 9, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 13, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 17, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 21, 12)
    )::uuid,
    (
        substr(md5('pool/' || pool_number), 1, 8) || '-' ||
        substr(md5('pool/' || pool_number), 9, 4) || '-' ||
        substr(md5('pool/' || pool_number), 13, 4) || '-' ||
        substr(md5('pool/' || pool_number), 17, 4) || '-' ||
        substr(md5('pool/' || pool_number), 21, 12)
    )::uuid,
    '{"infra_machine_template":{}}'
FROM generate_series(1, :pools) AS pool_number
CROSS JOIN generate_series(1, :sets_per_pool) AS set_number;

-- Keep node count independent of retained node-set count so that the two
-- cardinalities can be varied separately.
INSERT INTO nodes (
    id,
    node_pool_id,
    node_set_id,
    cluster_id,
    control_plane,
    name,
    ip,
    conditions
)
SELECT
    (
        substr(md5('node/' || node_number), 1, 8) || '-' ||
        substr(md5('node/' || node_number), 9, 4) || '-' ||
        substr(md5('node/' || node_number), 13, 4) || '-' ||
        substr(md5('node/' || node_number), 17, 4) || '-' ||
        substr(md5('node/' || node_number), 21, 12)
    )::uuid,
    (
        substr(md5('pool/' || pool_number), 1, 8) || '-' ||
        substr(md5('pool/' || pool_number), 9, 4) || '-' ||
        substr(md5('pool/' || pool_number), 13, 4) || '-' ||
        substr(md5('pool/' || pool_number), 17, 4) || '-' ||
        substr(md5('pool/' || pool_number), 21, 12)
    )::uuid,
    (
        substr(md5('set/' || pool_number || '/' || set_number), 1, 8) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 9, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 13, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 17, 4) || '-' ||
        substr(md5('set/' || pool_number || '/' || set_number), 21, 12)
    )::uuid,
    '00000000-0000-0000-0000-000000000001',
    false,
    'node-' || node_number,
    '127.0.0.1',
    '{}'
FROM generate_series(1, :nodes) AS node_number
CROSS JOIN LATERAL (
    SELECT
        ((node_number - 1) % :pools) + 1 AS pool_number,
        (((node_number - 1) / :pools) % :sets_per_pool) + 1 AS set_number
) AS placement;

DROP INDEX IF EXISTS node_sets_node_pool_id_cwe_perf_idx;
DROP INDEX IF EXISTS nodes_node_set_id_cwe_perf_idx;

\if :indexed
CREATE INDEX node_sets_node_pool_id_cwe_perf_idx ON node_sets (node_pool_id);
CREATE INDEX nodes_node_set_id_cwe_perf_idx ON nodes (node_set_id);
\endif

ANALYZE clusters;
ANALYZE node_pools;
ANALYZE node_sets;
ANALYZE nodes;

-- Models the nested part of the DB-backed metrics collector: one node-set list
-- for every node pool.
CREATE FUNCTION cwe_perf_metric_traversal() RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    pool_id uuid;
    matched bigint;
    total bigint := 0;
BEGIN
    FOR pool_id IN SELECT id FROM node_pools LOOP
        SELECT count(*) INTO matched
        FROM node_sets AS node_set
        JOIN node_pools AS np ON np.id = node_set.node_pool_id
        JOIN clusters AS c ON c.id = np.cluster_id
        WHERE node_set.node_pool_id = pool_id;
        total := total + matched;
    END LOOP;
    RETURN total;
END;
$$;

-- Models the retained-history part of node-pool reconciliation: one node list
-- for every node set.
CREATE FUNCTION cwe_perf_reconcile_traversal() RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    set_id uuid;
    matched bigint;
    total bigint := 0;
BEGIN
    FOR set_id IN SELECT id FROM node_sets LOOP
        SELECT count(*) INTO matched
        FROM nodes AS node
        LEFT JOIN node_sets AS nps ON nps.id = node.node_set_id
        LEFT JOIN node_pools AS np ON np.id = COALESCE(nps.node_pool_id, node.node_pool_id)
        LEFT JOIN clusters AS c ON c.id = COALESCE(np.cluster_id, node.cluster_id)
        WHERE node.node_set_id = set_id;
        total := total + matched;
    END LOOP;
    RETURN total;
END;
$$;

CREATE FUNCTION cwe_perf_measure_metric(repetition_count integer)
RETURNS TABLE (run integer, duration_ms double precision, matched bigint)
LANGUAGE plpgsql AS $$
DECLARE
    sample_number integer;
    started timestamptz;
BEGIN
    FOR sample_number IN 1..repetition_count LOOP
        run := sample_number;
        started := clock_timestamp();
        matched := cwe_perf_metric_traversal();
        duration_ms := extract(epoch FROM clock_timestamp() - started) * 1000;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE FUNCTION cwe_perf_measure_reconcile(repetition_count integer)
RETURNS TABLE (run integer, duration_ms double precision, matched bigint)
LANGUAGE plpgsql AS $$
DECLARE
    sample_number integer;
    started timestamptz;
BEGIN
    FOR sample_number IN 1..repetition_count LOOP
        run := sample_number;
        started := clock_timestamp();
        matched := cwe_perf_reconcile_traversal();
        duration_ms := extract(epoch FROM clock_timestamp() - started) * 1000;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- One unreported warm-up for each traversal.
SELECT cwe_perf_metric_traversal() \g /dev/null
SELECT cwe_perf_reconcile_traversal() \g /dev/null

SELECT
    'RESULT',
    'metric',
    :pools,
    :sets_per_pool,
    (:pools * :sets_per_pool),
    :nodes,
    :'indexed',
    run,
    round(duration_ms::numeric, 3),
    matched
FROM cwe_perf_measure_metric(:repetitions);

SELECT
    'RESULT',
    'reconcile',
    :pools,
    :sets_per_pool,
    (:pools * :sets_per_pool),
    :nodes,
    :'indexed',
    run,
    round(duration_ms::numeric, 3),
    matched
FROM cwe_perf_measure_reconcile(:repetitions);

\if :plans
EXPLAIN (ANALYZE, BUFFERS)
SELECT node_set.*
FROM node_sets AS node_set
JOIN node_pools AS np ON np.id = node_set.node_pool_id
JOIN clusters AS c ON c.id = np.cluster_id
WHERE node_set.node_pool_id = (SELECT id FROM node_pools ORDER BY id LIMIT 1);

EXPLAIN (ANALYZE, BUFFERS)
SELECT node.*
FROM nodes AS node
LEFT JOIN node_sets AS nps ON nps.id = node.node_set_id
LEFT JOIN node_pools AS np ON np.id = COALESCE(nps.node_pool_id, node.node_pool_id)
LEFT JOIN clusters AS c ON c.id = COALESCE(np.cluster_id, node.cluster_id)
WHERE node.node_set_id = (SELECT id FROM node_sets ORDER BY id LIMIT 1);
\endif
