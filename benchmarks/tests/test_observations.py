# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest

from benchmarks.cwe_perf.observations import summarize


class ObservationSummaryTest(unittest.TestCase):
    def test_restart_separates_counters_and_missing_metrics(self) -> None:
        samples = [
            {"generation": 1, "elapsedMs": 0, "metrics": {"process_cpu_seconds_total": 10}},
            {"generation": 1, "elapsedMs": 1000, "metrics": {"process_cpu_seconds_total": 12}, "errors": ["failed scrape"]},
            {"generation": 2, "elapsedMs": 2000, "metrics": {"process_cpu_seconds_total": 1}},
            {"generation": 2, "elapsedMs": 3000, "metrics": {"process_cpu_seconds_total": 4}},
        ]
        report = summarize(samples)
        self.assertEqual(1, report["errors"])
        self.assertEqual(2, report["generations"]["1"]["counters"]["process_cpu_seconds_total"]["delta"])
        self.assertEqual(3, report["generations"]["2"]["counters"]["process_cpu_seconds_total"]["delta"])
        self.assertEqual(3, report["generations"]["2"]["counters"]["process_cpu_seconds_total"]["averageCPUCores"])
        self.assertNotIn("process_resident_memory_bytes", report["generations"]["1"]["resources"])

    def test_resource_trend_and_busy_queue_remain_visible(self) -> None:
        mib = 1024**2
        queue = 'workqueue_depth{name="busy"}'
        report = summarize([
            {"generation": 1, "elapsedMs": 0, "metrics": {"process_resident_memory_bytes": 100*mib, queue: 5000}},
            {"generation": 1, "elapsedMs": 3600000, "metrics": {"process_resident_memory_bytes": 104*mib, queue: 3}},
        ])
        metrics = report["generations"]["1"]
        self.assertAlmostEqual(4, metrics["resources"]["process_resident_memory_bytes"]["linearTrendMiBPerHour"])
        self.assertEqual({"peak": 5000, "last": 3}, metrics["queues"][queue])
        self.assertEqual({"availableSamples": 2, "missingSamples": 0}, metrics["signalCoverage"]["queueDepth"])

    def test_failed_scrape_with_null_fields_is_preserved(self) -> None:
        report = summarize([
            {"generation": 1, "elapsedMs": 0, "metrics": {"process_cpu_seconds_total": 10}},
            {"generation": 1, "elapsedMs": 1000, "metrics": None, "api": None,
             "inventory": None, "errors": ["metrics unavailable", "API unavailable"]},
        ])
        self.assertEqual(2, report["errors"])
        self.assertEqual(2, report["samples"])
        self.assertNotIn("process_cpu_seconds_total", report["generations"]["1"]["counters"])

    def test_absent_queue_metrics_do_not_mean_idle_queues(self) -> None:
        report = summarize([{"generation": 1, "elapsedMs": 0, "metrics": {"go_goroutines": 42}}])
        generation = report["generations"]["1"]
        self.assertEqual({}, generation["queues"])
        self.assertEqual({"availableSamples": 0, "missingSamples": 1}, generation["signalCoverage"]["queueDepth"])
