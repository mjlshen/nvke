# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib

from .model import Scenario, materialize
from .matrix import run_matrix
from .result import new_result, write_result
from .tenant_api import VirtualTenantAPI


def _parse_listen(value: str) -> tuple[str, int]:
    host, separator, port = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("listen address must be HOST:PORT")
    return host, int(port)


def _write_json(path: str, value: object) -> None:
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


async def _serve(args: argparse.Namespace) -> None:
    scenario = Scenario.load(args.scenario)
    host, port = args.listen
    inventory = materialize(scenario, f"http://{host}:{port}")
    api = VirtualTenantAPI(inventory, args.latency_ms, args.failure_every)
    server = await api.serve(host, port)
    sockets = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"virtual tenant API listening on {sockets}", flush=True)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic CWE performance harness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize_parser = subparsers.add_parser("materialize", help="write deterministic inventory JSON")
    materialize_parser.add_argument("--scenario", required=True)
    materialize_parser.add_argument("--server", default="http://127.0.0.1:18080")
    materialize_parser.add_argument("--output", required=True)

    serve_parser = subparsers.add_parser("serve-tenant-api", help="serve multiplexed virtual tenant APIs")
    serve_parser.add_argument("--scenario", required=True)
    serve_parser.add_argument("--listen", type=_parse_listen, default=("127.0.0.1", 18080))
    serve_parser.add_argument("--latency-ms", type=int, default=0)
    serve_parser.add_argument("--failure-every", type=int, default=0)

    result_parser = subparsers.add_parser("new-result", help="write an empty, versioned result envelope")
    result_parser.add_argument("--scenario", required=True)
    result_parser.add_argument(
        "--mode", required=True, choices=("tenant", "agent-health", "node-source", "prometheus", "black-box")
    )
    result_parser.add_argument("--output", required=True)

    matrix_parser = subparsers.add_parser("run-matrix", help="run repeatable tenant or agent-health repetitions")
    matrix_parser.add_argument("--scenario", dest="scenarios", action="append", required=True)
    matrix_parser.add_argument("--harness", required=True)
    matrix_parser.add_argument("--mode", required=True, choices=("tenant", "agent-health", "node-source", "prometheus"))
    matrix_parser.add_argument("--db-dsn", required=True)
    matrix_parser.add_argument("--results-dir", required=True)
    matrix_parser.add_argument("--repetitions", type=int, default=5)
    matrix_parser.add_argument("--warmups", type=int, default=1)
    matrix_parser.add_argument("--listen", type=_parse_listen, default=("127.0.0.1", 18080))
    matrix_parser.add_argument("--harness-arg", action="append", default=[])
    matrix_parser.add_argument("--latency-ms", type=int, default=0)
    matrix_parser.add_argument("--failure-every", type=int, default=0)

    args = parser.parse_args()
    if hasattr(args, "latency_ms") and args.latency_ms < 0:
        parser.error("--latency-ms must not be negative")
    if hasattr(args, "failure_every") and args.failure_every < 0:
        parser.error("--failure-every must not be negative")
    if args.command == "materialize":
        _write_json(args.output, materialize(Scenario.load(args.scenario), args.server))
        return
    if args.command == "new-result":
        scenario = Scenario.load(args.scenario)
        write_result(args.output, new_result(materialize(scenario)["scenario"], args.mode))
        return
    if args.command == "run-matrix":
        if args.repetitions <= 0:
            parser.error("--repetitions must be greater than zero")
        if args.warmups < 0:
            parser.error("--warmups must not be negative")
        asyncio.run(run_matrix(args))
        return
    asyncio.run(_serve(args))


if __name__ == "__main__":
    main()
