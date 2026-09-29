# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import pathlib
import tempfile
import unittest

from benchmarks.cwe_perf.matrix import _remove_prior_outputs, _write_summary


class MatrixSummaryTest(unittest.TestCase):
    def test_removes_only_prior_generated_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            generated = ("run-01.json", "warmup-01.json", "summary.json")
            for name in (*generated, "inventory.json", "notes.txt"):
                (root / name).write_text("{}", encoding="utf-8")

            _remove_prior_outputs(root)

            for name in generated:
                self.assertFalse((root / name).exists())
            self.assertTrue((root / "inventory.json").exists())
            self.assertTrue((root / "notes.txt").exists())

    def test_summarizes_measured_phases_and_excludes_seed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for index, duration in enumerate((10.0, 20.0, 30.0), 1):
                result = {
                    "phases": [
                        {"name": "fixture-seed", "durationMs": 999.0},
                        {"name": "tenant-manager-cold-start", "durationMs": duration},
                    ]
                }
                (root / f"run-{index:02d}.json").write_text(json.dumps(result), encoding="utf-8")

            _write_summary(root, "test", "tenant")
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(0, summary["warmups"])
            self.assertNotIn("fixture-seed", summary["phases"])
            self.assertEqual(20.0, summary["phases"]["tenant-manager-cold-start"]["medianMs"])
            self.assertEqual(30.0, summary["phases"]["tenant-manager-cold-start"]["p95Ms"])


if __name__ == "__main__":
    unittest.main()
