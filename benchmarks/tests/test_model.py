# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import pathlib
import tempfile
import unittest

from benchmarks.cwe_perf.model import Scenario, materialize


class ScenarioTest(unittest.TestCase):
    def test_balanced_inventory_is_deterministic(self) -> None:
        scenario = Scenario("test", 17, 3, 10, ready_fraction=0.8)
        first = materialize(scenario)
        second = materialize(scenario)
        self.assertEqual(first, second)
        self.assertEqual([4, 3, 3], [len(cluster["nodes"]) for cluster in first["clusters"]])
        self.assertEqual(8, sum(node["ready"] for cluster in first["clusters"] for node in cluster["nodes"]))

    def test_skewed_inventory_preserves_totals(self) -> None:
        inventory = materialize(Scenario("skew", 17, 10, 100, distribution="skewed"))
        counts = [len(cluster["nodes"]) for cluster in inventory["clusters"]]
        self.assertEqual(100, sum(counts))
        self.assertGreater(sum(counts[:5]), sum(counts[5:]))

    def test_load_rejects_more_clusters_than_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "scenario.json"
            path.write_text(json.dumps({"name": "bad", "seed": 1, "inventory": {"clusters": 2, "nodes": 1}}))
            with self.assertRaisesRegex(ValueError, "at least"):
                Scenario.load(path)


if __name__ == "__main__":
    unittest.main()
