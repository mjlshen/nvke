# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import unittest

from benchmarks.cwe_perf.model import Scenario, materialize
from benchmarks.cwe_perf.tenant_api import VirtualTenantAPI


async def request(port: int, method: str, path: str, token: str | None = None, body: dict | None = None) -> tuple[int, dict]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    payload = json.dumps(body).encode() if body is not None else b""
    headers = [f"{method} {path} HTTP/1.1", "Host: localhost", "Connection: close"]
    if token:
        headers.append(f"Authorization: Bearer {token}")
    if payload:
        headers.extend(["Content-Type: application/json", f"Content-Length: {len(payload)}"])
    writer.write(("\r\n".join(headers) + "\r\n\r\n").encode() + payload)
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, raw_body = response.split(b"\r\n\r\n", 1)
    return int(head.split(b" ", 2)[1]), json.loads(raw_body)


class VirtualTenantAPITest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.api = VirtualTenantAPI(materialize(Scenario("test", 1, 2, 4)))
        self.server = await self.api.serve("127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def test_lists_only_token_cluster_nodes(self) -> None:
        status, response = await request(self.port, "GET", "/api/v1/nodes", "cwe-perf-1")
        self.assertEqual(200, status)
        self.assertEqual(["cluster-0001-node-00000", "cluster-0001-node-00001"], [item["metadata"]["name"] for item in response["items"]])

    async def test_path_prefix_selects_cluster_without_http_credentials(self) -> None:
        status, response = await request(self.port, "GET", "/clusters/1/api/v1/nodes")
        self.assertEqual(200, status)
        self.assertEqual(["cluster-0001-node-00000", "cluster-0001-node-00001"], [item["metadata"]["name"] for item in response["items"]])

    async def test_rejects_missing_token(self) -> None:
        status, response = await request(self.port, "GET", "/api/v1/nodes")
        self.assertEqual(401, status)
        self.assertEqual("Unauthorized", response["reason"])

    async def test_deterministic_dependency_failure(self) -> None:
        api = VirtualTenantAPI(materialize(Scenario("failure", 2, 1, 1)), failure_every=2)
        server = await api.serve("127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            status, _ = await request(port, "GET", "/version", "cwe-perf-0")
            self.assertEqual(200, status)
            status, response = await request(port, "GET", "/version", "cwe-perf-0")
            self.assertEqual(503, status)
            self.assertEqual("ServiceUnavailable", response["reason"])
        finally:
            server.close()
            await server.wait_closed()

    async def test_admin_event_updates_node(self) -> None:
        event = {"cluster": 0, "node": "cluster-0000-node-00000", "ready": False}
        status, _ = await request(self.port, "POST", "/__admin/node-event", body=event)
        self.assertEqual(200, status)
        status, response = await request(self.port, "GET", "/api/v1/nodes/cluster-0000-node-00000", "cwe-perf-0")
        self.assertEqual(200, status)
        self.assertEqual("False", response["status"]["conditions"][0]["status"])

        await self.api.reset()
        status, response = await request(self.port, "GET", "/clusters/0/api/v1/nodes/cluster-0000-node-00000")
        self.assertEqual(200, status)
        self.assertEqual("True", response["status"]["conditions"][0]["status"])

    async def test_csr_can_be_added_and_approved(self) -> None:
        csr = {"spec": {"request": "AA=="}, "status": {}}
        status, _ = await request(
            self.port,
            "POST",
            "/__admin/csr-event",
            body={"cluster": 0, "name": "csr-1", "object": csr},
        )
        self.assertEqual(200, status)
        path = "/apis/certificates.k8s.io/v1/certificatesigningrequests/csr-1/approval"
        status, response = await request(self.port, "PUT", path, "cwe-perf-0", body={"metadata": {"name": "csr-1"}, "status": {"conditions": [{"type": "Approved"}]}})
        self.assertEqual(200, status)
        self.assertEqual("Approved", response["status"]["conditions"][0]["type"])


if __name__ == "__main__":
    unittest.main()
