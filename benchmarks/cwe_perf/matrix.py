# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import asyncio
import json
import math
import pathlib
import statistics
from typing import Any

from .model import Scenario, materialize
from .tenant_api import VirtualTenantAPI


async def run_matrix(args: argparse.Namespace) -> None:
    results_root = pathlib.Path(args.results_dir)
    results_root.mkdir(parents=True, exist_ok=True)
    for scenario_path in args.scenarios:
        scenario = Scenario.load(scenario_path)
        inventory = materialize(scenario, f"http://{args.listen[0]}:{args.listen[1]}")
        scenario_root = results_root / scenario.name
        scenario_root.mkdir(parents=True, exist_ok=True)
        _remove_prior_outputs(scenario_root)
        inventory_path = scenario_root / "inventory.json"
        inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        api = None
        server = None
        if args.mode not in {"node-source", "prometheus"}:
            api = VirtualTenantAPI(inventory, args.latency_ms, args.failure_every)
            server = await api.serve(*args.listen)
        try:
            total_runs = args.warmups + args.repetitions
            for iteration in range(1, total_runs + 1):
                if api is not None:
                    await api.reset()
                if iteration <= args.warmups:
                    output_path = scenario_root / f"warmup-{iteration:02d}.json"
                else:
                    repetition = iteration - args.warmups
                    output_path = scenario_root / f"run-{repetition:02d}.json"
                command = [
                    args.harness,
                    args.mode,
                    "-inventory",
                    str(inventory_path),
                    "-db-dsn",
                    args.db_dsn,
                    "-output",
                    str(output_path),
                    *args.harness_arg,
                ]
                process = await asyncio.create_subprocess_exec(*command)
                return_code = await process.wait()
                if return_code != 0:
                    raise RuntimeError(f"harness exited with status {return_code}: {' '.join(command)}")
        finally:
            if api is not None and server is not None:
                await api.reset()
                server.close()
                await server.wait_closed()
        _write_summary(scenario_root, scenario.name, args.mode)


def _remove_prior_outputs(root: pathlib.Path) -> None:
    for pattern in ("run-*.json", "warmup-*.json", "summary.json"):
        for path in root.glob(pattern):
            path.unlink()


def _write_summary(root: pathlib.Path, scenario: str, mode: str) -> None:
    results = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(root.glob("run-*.json"))]
    phase_values: dict[str, list[float]] = {}
    for result in results:
        for phase in result["phases"]:
            if phase["name"] == "fixture-seed":
                continue
            phase_values.setdefault(phase["name"], []).append(float(phase["durationMs"]))
    summary: dict[str, Any] = {
        "schemaVersion": 1,
        "scenario": scenario,
        "mode": mode,
        "warmups": len(list(root.glob("warmup-*.json"))),
        "runs": len(results),
        "phases": {},
    }
    for name, values in phase_values.items():
        ordered = sorted(values)
        summary["phases"][name] = {
            "medianMs": statistics.median(ordered),
            "p95Ms": ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)],
            "minMs": ordered[0],
            "maxMs": ordered[-1],
        }
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
