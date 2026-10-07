# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Summarize worker observation streams without mixing process generations."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

RESOURCE_METRICS = ["process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_goroutines"]
COUNTER_METRICS = ["process_cpu_seconds_total", "go_memstats_alloc_bytes_total", "go_gc_duration_seconds_count"]


def describe(values: list[float]) -> dict[str, float]:
    return {"first": values[0], "last": values[-1], "min": min(values), "max": max(values), "median": statistics.median(values)}


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    generations: dict[int, list[dict[str, Any]]] = {}
    for sample in samples:
        generations.setdefault(sample["generation"], []).append(sample)
    report: dict[str, Any] = {"samples": len(samples), "errors": sum(len(s.get("errors", [])) for s in samples), "generations": {}}
    for generation, observations in sorted(generations.items()):
        observations.sort(key=lambda s: s["elapsedMs"])
        duration = (observations[-1]["elapsedMs"] - observations[0]["elapsedMs"]) / 1000
        result: dict[str, Any] = {"samples": len(observations), "observedDurationSeconds": duration, "resources": {}, "counters": {}}
        for metric in RESOURCE_METRICS:
            points = [(s["elapsedMs"] / 1000, s["metrics"][metric]) for s in observations if metric in (s.get("metrics") or {})]
            if not points:
                continue
            values = [p[1] for p in points]
            summary = describe(values)
            if metric.endswith("bytes") and len(points) >= 2:
                times = [p[0] for p in points]
                mean_x, mean_y = statistics.mean(times), statistics.mean(values)
                denominator = sum((x - mean_x) ** 2 for x in times)
                if denominator:
                    summary["linearTrendMiBPerHour"] = sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator * 3600 / (1024 ** 2)
                width = max(1, len(values) // 4)
                summary["firstQuarterMedian"] = statistics.median(values[:width])
                summary["lastQuarterMedian"] = statistics.median(values[-width:])
            result["resources"][metric] = summary
        for metric in COUNTER_METRICS:
            points = [(s["elapsedMs"] / 1000, s["metrics"][metric]) for s in observations if metric in (s.get("metrics") or {})]
            if len(points) >= 2:
                values = [p[1] for p in points]
                seconds = points[-1][0] - points[0][0]
                reset = any(b < a for a, b in zip(values, values[1:]))
                counter = {"delta": values[-1] - values[0], "resetObserved": reset, "observedDurationSeconds": seconds}
                if metric == "process_cpu_seconds_total" and seconds > 0 and not reset:
                    counter["averageCPUCores"] = (values[-1] - values[0]) / seconds
                result["counters"][metric] = counter
        signals = {"workerCPU": "process_cpu_seconds_total", "workerRSS": "process_resident_memory_bytes",
                   "workerHeap": "go_memstats_heap_alloc_bytes", "queueDepth": "workqueue_depth{",
                   "controllerReconciliations": "controller_runtime_reconcile_total{", "workerDatabase": "nke_controller_db_"}
        result["signalCoverage"] = {}
        for name, prefix in signals.items():
            present = sum(any(key.startswith(prefix) for key in (s.get("metrics") or {})) for s in observations)
            result["signalCoverage"][name] = {"availableSamples": present, "missingSamples": len(observations)-present}
        queue_keys = {key for s in observations for key in (s.get("metrics") or {}) if key.startswith("workqueue_depth{")}
        result["queues"] = {key: {"peak": max((s.get("metrics") or {}).get(key, 0) for s in observations),
                                    "last": (observations[-1].get("metrics") or {}).get(key)} for key in sorted(queue_keys)}
        api_latencies = [s["api"]["durationMs"] for s in observations if (s.get("api") or {}).get("status") == 200]
        if api_latencies:
            ordered = sorted(api_latencies)
            result["listAPILatencyMs"] = {**describe(api_latencies), "p95": ordered[min(len(ordered)-1, (len(ordered)*95+99)//100-1)]}
        result["finalInventory"] = observations[-1].get("inventory", {})
        result["finalAgent"] = observations[-1].get("agent", {})
        result["finalTenantAPI"] = observations[-1].get("tenantAPI", {})
        report["generations"][str(generation)] = result
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    samples = [json.loads(line) for line in pathlib.Path(args.samples).read_text(encoding="utf-8").splitlines() if line.strip()]
    if not samples:
        parser.error("the observation stream is empty")
    with pathlib.Path(args.output).open("x", encoding="utf-8") as stream:
        json.dump(summarize(samples), stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
