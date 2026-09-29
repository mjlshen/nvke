# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import dataclasses
import json
import pathlib
import uuid
from typing import Any


_OBJECT_NAMESPACE = uuid.UUID("5a78818d-0d35-4b3a-9ee8-e4c67441f69d")


@dataclasses.dataclass(frozen=True)
class Scenario:
    name: str
    seed: int
    clusters: int
    nodes: int
    distribution: str = "balanced"
    ready_fraction: float = 1.0

    @classmethod
    def load(cls, path: str | pathlib.Path) -> "Scenario":
        with pathlib.Path(path).open(encoding="utf-8") as stream:
            raw = json.load(stream)
        scenario = cls(
            name=str(raw["name"]),
            seed=int(raw["seed"]),
            clusters=int(raw["inventory"]["clusters"]),
            nodes=int(raw["inventory"]["nodes"]),
            distribution=str(raw["inventory"].get("distribution", "balanced")),
            ready_fraction=float(raw["inventory"].get("readyFraction", 1.0)),
        )
        scenario.validate()
        return scenario

    def validate(self) -> None:
        if not self.name:
            raise ValueError("scenario name must not be empty")
        if self.clusters <= 0:
            raise ValueError("cluster count must be greater than zero")
        if self.nodes < self.clusters:
            raise ValueError("node count must be at least the cluster count")
        if self.distribution not in {"balanced", "skewed"}:
            raise ValueError("distribution must be balanced or skewed")
        if not 0.0 <= self.ready_fraction <= 1.0:
            raise ValueError("readyFraction must be between zero and one")


def _node_counts(scenario: Scenario) -> list[int]:
    if scenario.distribution == "balanced" or scenario.clusters == 1:
        quotient, remainder = divmod(scenario.nodes, scenario.clusters)
        return [quotient + (1 if index < remainder else 0) for index in range(scenario.clusters)]

    # Put sixty percent of the fleet in at most five hot clusters, then
    # distribute the rest. Keep at least one node in every cold cluster.
    hot_clusters = min(5, scenario.clusters)
    cold_clusters = scenario.clusters - hot_clusters
    hot_nodes = min((scenario.nodes * 3) // 5, scenario.nodes - cold_clusters)
    counts = [0] * scenario.clusters
    quotient, remainder = divmod(hot_nodes, hot_clusters)
    for index in range(hot_clusters):
        counts[index] = quotient + (1 if index < remainder else 0)

    remaining = scenario.nodes - sum(counts)
    if cold_clusters == 0:
        counts[-1] += remaining
        return counts
    quotient, remainder = divmod(remaining, cold_clusters)
    for offset in range(cold_clusters):
        counts[hot_clusters + offset] = quotient + (1 if offset < remainder else 0)
    return counts


def materialize(scenario: Scenario, server: str = "http://127.0.0.1:18080") -> dict[str, Any]:
    clusters: list[dict[str, Any]] = []
    global_node_index = 0
    ready_cutoff = int(scenario.nodes * scenario.ready_fraction)
    for cluster_index, count in enumerate(_node_counts(scenario)):
        cluster_name = f"cluster-{cluster_index:04d}"
        cluster_id = str(uuid.uuid5(_OBJECT_NAMESPACE, f"{scenario.seed}/cluster/{cluster_index}"))
        nodes = []
        for local_index in range(count):
            node_name = f"{cluster_name}-node-{local_index:05d}"
            nodes.append(
                {
                    "id": str(uuid.uuid5(_OBJECT_NAMESPACE, f"{scenario.seed}/node/{global_node_index}")),
                    "name": node_name,
                    "ready": global_node_index < ready_cutoff,
                    "unschedulable": False,
                }
            )
            global_node_index += 1
        clusters.append(
            {
                "index": cluster_index,
                "id": cluster_id,
                "name": cluster_name,
                "token": f"cwe-perf-{cluster_index}",
                "server": f"{server.rstrip('/')}/clusters/{cluster_index}",
                "nodes": nodes,
            }
        )
    return {
        "schemaVersion": 1,
        "scenario": dataclasses.asdict(scenario),
        "clusters": clusters,
    }
