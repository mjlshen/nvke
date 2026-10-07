# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reproducible steady-state experiments; dependency setup remains explicit."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import pathlib
import platform
import shutil
import statistics
import tempfile
from typing import Any

from .model import Scenario, materialize
from .tenant_api import VirtualTenantAPI


def cases() -> list[tuple[str, str, list[str]]]:
    experiments = [
        (f"agent-delay-{delay}ms-c{workers}", "agent-health",
         ["-agent-delay", f"{delay}ms", "-concurrency", str(workers), "-steady-sweeps", "2"])
        for delay, workers in [(0, 1), (10, 1), (25, 1), (100, 1), (100, 2), (100, 4), (100, 8)]
    ]
    experiments += [
        ("agent-slow-cohort", "agent-health", ["-slow-node-every", "100", "-slow-node-delay", "2s", "-steady-sweeps", "2"]),
        ("agent-failed-cohort", "agent-health", ["-failed-node-every", "100", "-steady-sweeps", "2"]),
        ("agent-default-timeout", "agent-health", ["-slow-node-every", "5000", "-slow-node-delay", "3m", "-timeout", "2m", "-steady-sweeps", "2"]),
        ("agent-timeout-cohort", "agent-health", ["-slow-node-every", "100", "-slow-node-delay", "3s", "-timeout", "2s", "-steady-sweeps", "2"]),
    ]
    experiments += [
        (f"source-c{workers}", "node-source", ["-copies-per-cluster", "2", "-source-concurrency", str(workers), "-resyncs", "3"])
        for workers in [2, 10, 100]
    ]
    experiments += [
        (f"scrapes-c{workers}", "prometheus", ["-node-pools-per-cluster", "10", "-node-sets-per-pool", "10",
                                              "-scrapes", "12", "-concurrency", str(workers), "-scrape-interval", "1s"])
        for workers in [1, 2, 4]
    ]
    experiments += [("tenant-events", "tenant", ["-events", "100"])]
    return experiments


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    phases: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        for phase in result.get("phases", []):
            if phase["name"] != "fixture-seed":
                phases.setdefault(phase["name"], []).append(phase)
    summary: dict[str, Any] = {"runs": len(results), "errors": sum(len(r.get("errors", [])) for r in results), "phases": {}}
    for name, observations in phases.items():
        durations = [p["durationMs"] for p in observations]
        phase_summary: dict[str, Any] = {"medianMs": statistics.median(durations), "minMs": min(durations), "maxMs": max(durations)}
        metric_names = {key for p in observations for key, value in p.get("metrics", {}).items()
                        if isinstance(value, (float, int)) and not isinstance(value, bool)}
        phase_summary["metrics"] = {key: statistics.median([p["metrics"][key] for p in observations if key in p.get("metrics", {})])
                                    for key in sorted(metric_names)}
        checks = {key for p in observations for key, value in p.get("metrics", {}).items() if isinstance(value, bool)}
        phase_summary["booleanChecks"] = {
            key: {"passed": sum(p["metrics"].get(key) is True for p in observations),
                  "failed": sum(p["metrics"].get(key) is False for p in observations)}
            for key in sorted(checks)
        }
        summary["phases"][name] = phase_summary
    return summary


async def run(args: argparse.Namespace) -> None:
    known = {case[0] for case in cases()}
    unknown = set(args.case or []) - known
    if unknown:
        raise ValueError(f"unknown component experiments: {sorted(unknown)}")
    scenario = Scenario.load(args.scenario)
    root = pathlib.Path(args.results_dir)
    # Refuse accidental overwrite of prior evidence.
    root.mkdir(parents=True, exist_ok=False)
    inventory = materialize(scenario, "http://127.0.0.1:18080")
    api = VirtualTenantAPI(inventory, args.tenant_latency_ms, args.tenant_failure_every)
    server = await api.serve("127.0.0.1", 18080)
    report: dict[str, Any] = {"schemaVersion": 1, "scenario": inventory["scenario"], "repetitions": args.repetitions,
                              "platform": platform.platform(),
                              "tenantLatencyMs": args.tenant_latency_ms, "tenantFailureEvery": args.tenant_failure_every,
                              "cases": {}}
    try:
        with tempfile.TemporaryDirectory(prefix="nke-steady-inventory-") as scratch:
            harness_path = pathlib.Path(scratch) / "cwe-perf"
            shutil.copy2(args.harness, harness_path)
            report["harnessSHA256"] = hashlib.sha256(harness_path.read_bytes()).hexdigest()
            inventory_path = pathlib.Path(scratch) / "inventory.json"
            inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
            experiments = [case for case in cases() if not args.case or case[0] in args.case]
            if args.case and not experiments:
                raise ValueError("no matching component experiment")
            for name, lane, flags in experiments:
                case_root = root / name
                case_root.mkdir()
                results = []
                for repetition in range(args.repetitions):
                    await api.reset()
                    output = case_root / f"run-{repetition+1:02d}.json"
                    command = [str(harness_path), lane, "-inventory", str(inventory_path), "-db-dsn", args.db_dsn,
                               "-output", str(output), *flags]
                    print(f"running {name} repetition {repetition+1}/{args.repetitions}", flush=True)
                    process = await asyncio.create_subprocess_exec(*command)
                    try:
                        return_code = await process.wait()
                    except asyncio.CancelledError:
                        if process.returncode is None:
                            process.terminate()
                        await process.wait()
                        raise
                    if output.exists():
                        results.append(json.loads(output.read_text(encoding="utf-8")))
                    if return_code:
                        report["cases"][name] = {"mode": lane, "flags": flags, "exitCode": return_code, **summarize(results)}
                        write_report(root, report)
                        raise RuntimeError(f"{name} exited with status {return_code}; partial evidence retained")
                report["cases"][name] = {"mode": lane, "flags": flags, **summarize(results)}
                write_report(root, report)
    finally:
        await api.reset()
        server.close()
        await server.wait_closed()


def write_report(root: pathlib.Path, report: dict[str, Any]) -> None:
    (root / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--harness", required=True)
    parser.add_argument("--db-dsn", required=True)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--case", action="append", help="run only named cases; repeat to select several")
    parser.add_argument("--tenant-latency-ms", type=int, default=0)
    parser.add_argument("--tenant-failure-every", type=int, default=0)
    args = parser.parse_args()
    if args.repetitions <= 0 or args.tenant_latency_ms < 0 or args.tenant_failure_every < 0:
        parser.error("repetitions must be positive and dependency controls nonnegative")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
