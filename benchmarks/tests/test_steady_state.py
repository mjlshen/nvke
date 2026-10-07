# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import pathlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from benchmarks.cwe_perf.steady_state import cases, run, summarize
from benchmarks.cwe_perf.tenant_api import VirtualTenantAPI


class SteadyStateTest(unittest.TestCase):
    def test_summary_preserves_failed_runs_and_does_not_average_booleans(self):
        summary = summarize([
            {"errors": [], "phases": [{"name": "steady", "durationMs": 100, "metrics": {"queries": 5, "withinTwoMinuteSweep": True}}]},
            {"errors": ["incomplete"], "phases": [{"name": "steady", "durationMs": 300, "metrics": {"queries": 7, "withinTwoMinuteSweep": False}}]},
        ])
        self.assertEqual(1, summary["errors"])
        self.assertEqual(200, summary["phases"]["steady"]["medianMs"])
        self.assertEqual({"queries": 6}, summary["phases"]["steady"]["metrics"])
        self.assertEqual({"passed": 1, "failed": 1}, summary["phases"]["steady"]["booleanChecks"]["withinTwoMinuteSweep"])

    def test_case_names_are_unique(self):
        names = [case[0] for case in cases()]
        self.assertEqual(len(names), len(set(names)))


class SteadyStateShutdownTest(unittest.IsolatedAsyncioTestCase):
    async def test_suite_exits_with_an_idle_tenant_watch(self):
        port = 0
        client = None
        original_serve = VirtualTenantAPI.serve

        async def serve(api, host, requested_port):
            nonlocal port
            server = await original_serve(api, host, 0)
            port = server.sockets[0].getsockname()[1]
            return server

        async def simulated_harness(*command):
            nonlocal client
            reader, client = await asyncio.open_connection("127.0.0.1", port)
            client.write(b"GET /clusters/0/api/v1/nodes?watch=true HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await client.drain()
            await reader.readuntil(b"\r\n\r\n")
            output = pathlib.Path(command[command.index("-output") + 1])
            output.write_text(json.dumps({"phases": [], "errors": []}))
            return SimpleNamespace(wait=AsyncMock(return_value=0))

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            harness = root / "harness"
            harness.write_text("unused subprocess fixture")
            args = SimpleNamespace(case=None, scenario=str(pathlib.Path(__file__).resolve().parents[1] / "scenarios/smoke.json"),
                                   results_dir=str(root / "results"), repetitions=1, harness=str(harness),
                                   db_dsn="example", tenant_latency_ms=0, tenant_failure_every=0)
            try:
                with patch("benchmarks.cwe_perf.steady_state.cases", return_value=[("cleanup", "tenant", [])]), \
                     patch.object(VirtualTenantAPI, "serve", serve), \
                     patch("benchmarks.cwe_perf.steady_state.asyncio.create_subprocess_exec", simulated_harness):
                    await asyncio.wait_for(run(args), timeout=2)
                summary = json.loads((root / "results/summary.json").read_text())
                self.assertEqual(1, summary["cases"]["cleanup"]["runs"])
            finally:
                if client is not None:
                    client.close()
                    await client.wait_closed()
