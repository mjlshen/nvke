# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import collections
import contextlib
import copy
import json
from typing import Any
from urllib.parse import parse_qs, urlsplit


class VirtualTenantAPI:
    def __init__(self, inventory: dict[str, Any], latency_ms: int = 0, failure_every: int = 0) -> None:
        self._inventory = copy.deepcopy(inventory)
        self._clusters = {cluster["token"]: cluster for cluster in inventory["clusters"]}
        self._clusters_by_index = {cluster["index"]: cluster for cluster in inventory["clusters"]}
        self._watchers: dict[tuple[int, str], set[asyncio.Queue[dict[str, Any]]]] = collections.defaultdict(set)
        self._watch_tasks: set[asyncio.Task[None]] = set()
        self._requests: collections.Counter[str] = collections.Counter()
        self._cluster_requests: collections.Counter[int] = collections.Counter()
        self._latency = latency_ms / 1000
        self._failure_every = failure_every
        self._reset_state()

    async def reset(self) -> None:
        """Restore object state and counters between comparable repetitions."""
        tasks = tuple(self._watch_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._watch_tasks.clear()
        self._watchers.clear()
        self._reset_state()

    def _reset_state(self) -> None:
        self._nodes = {
            cluster["index"]: {node["name"]: copy.deepcopy(node) for node in cluster["nodes"]}
            for cluster in self._inventory["clusters"]
        }
        self._csrs = {cluster["index"]: {} for cluster in self._inventory["clusters"]}
        self._requests.clear()
        self._cluster_requests.clear()
        self._dependency_failures = 0
        self._resource_version = 1

    async def serve(self, host: str, port: int) -> asyncio.AbstractServer:
        return await asyncio.start_server(self._handle_connection, host, port)

    def _cluster_for(self, headers: dict[str, str], path: str) -> tuple[dict[str, Any] | None, str]:
        components = path.split("/")
        if len(components) >= 4 and components[1] == "clusters":
            try:
                cluster = self._clusters_by_index.get(int(components[2]))
            except ValueError:
                cluster = None
            return cluster, "/" + "/".join(components[3:])
        authorization = headers.get("authorization", "")
        prefix = "Bearer "
        if not authorization.startswith(prefix):
            return None, path
        return self._clusters.get(authorization[len(prefix) :]), path

    @staticmethod
    def _node_object(node: dict[str, Any], resource_version: int) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": node["name"],
                "resourceVersion": str(resource_version),
                "labels": node.get("labels", {}),
                "annotations": node.get("annotations", {}),
            },
            "spec": {"unschedulable": bool(node.get("unschedulable", False))},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if node.get("ready", True) else "False",
                        "reason": "CWEPerfSimulator",
                        "message": "deterministic virtual node",
                        "lastHeartbeatTime": "2026-01-01T00:00:00Z",
                        "lastTransitionTime": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        }

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            header_bytes = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
            lines = header_bytes.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.strip().lower()] = value.strip()
            content_length = int(headers.get("content-length", "0"))
            body = await reader.readexactly(content_length) if content_length else b""
            await self._route(writer, method, target, headers, body)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, ValueError):
            await self._send_json(writer, 400, {"kind": "Status", "status": "Failure", "reason": "BadRequest"})
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def _route(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        target: str,
        headers: dict[str, str],
        body: bytes,
    ) -> None:
        parsed = urlsplit(target)
        path = parsed.path
        query = parse_qs(parsed.query)
        self._requests[f"{method} {path}"] += 1

        cluster, path = self._cluster_for(headers, path)

        if path == "/__admin/stats" and method == "GET":
            await self._send_json(
                writer,
                200,
                {
                    "requests": dict(sorted(self._requests.items())),
                    "activeWatches": sum(len(queues) for queues in self._watchers.values()),
                    "clusters": len(self._clusters),
                    "nodes": sum(len(nodes) for nodes in self._nodes.values()),
                    "dependencyFailures": self._dependency_failures,
                    "configuredLatencyMs": self._latency * 1000,
                },
            )
            return
        if path == "/__admin/node-event" and method == "POST":
            await self._admin_node_event(writer, body)
            return
        if path == "/__admin/csr-event" and method == "POST":
            await self._admin_csr_event(writer, body)
            return

        if cluster is None:
            await self._send_json(writer, 401, {"kind": "Status", "status": "Failure", "reason": "Unauthorized"})
            return
        cluster_index = int(cluster["index"])
        self._cluster_requests[cluster_index] += 1
        if self._latency:
            await asyncio.sleep(self._latency)
        if self._failure_every and self._cluster_requests[cluster_index] % self._failure_every == 0:
            self._dependency_failures += 1
            await self._send_json(writer, 503, {"kind": "Status", "status": "Failure", "reason": "ServiceUnavailable"})
            return

        discovery = self._discovery(path)
        if discovery is not None and method == "GET":
            await self._send_json(writer, 200, discovery)
            return

        if path == "/api/v1/nodes" and method == "GET":
            if query.get("watch", ["false"])[0].lower() == "true":
                initial = None
                if query.get("sendInitialEvents", ["false"])[0].lower() == "true":
                    initial = [
                        self._node_object(node, self._resource_version)
                        for node in self._nodes[cluster_index].values()
                    ]
                await self._watch(writer, cluster_index, "nodes", initial)
            else:
                items = [self._node_object(node, self._resource_version) for node in self._nodes[cluster_index].values()]
                await self._send_json(
                    writer,
                    200,
                    {"apiVersion": "v1", "kind": "NodeList", "metadata": {"resourceVersion": str(self._resource_version)}, "items": items},
                )
            return
        if path.startswith("/api/v1/nodes/"):
            await self._node_request(writer, method, cluster_index, path.removeprefix("/api/v1/nodes/"), body)
            return

        csr_collection = "/apis/certificates.k8s.io/v1/certificatesigningrequests"
        if path == csr_collection and method == "GET":
            if query.get("watch", ["false"])[0].lower() == "true":
                initial = None
                if query.get("sendInitialEvents", ["false"])[0].lower() == "true":
                    initial = list(self._csrs[cluster_index].values())
                await self._watch(writer, cluster_index, "csrs", initial)
            else:
                await self._send_json(
                    writer,
                    200,
                    {"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequestList", "metadata": {"resourceVersion": str(self._resource_version)}, "items": list(self._csrs[cluster_index].values())},
                )
            return
        if path.startswith(csr_collection + "/"):
            await self._csr_request(writer, method, cluster_index, path.removeprefix(csr_collection + "/"), body)
            return

        await self._send_json(writer, 404, {"kind": "Status", "status": "Failure", "reason": "NotFound"})

    @staticmethod
    def _discovery(path: str) -> dict[str, Any] | None:
        if path == "/version":
            return {"major": "1", "minor": "35", "gitVersion": "v1.35.0-cwe-perf"}
        if path == "/api":
            return {"kind": "APIVersions", "versions": ["v1"], "serverAddressByClientCIDRs": []}
        if path == "/apis":
            return {
                "kind": "APIGroupList",
                "apiVersion": "v1",
                "groups": [
                    {
                        "name": "certificates.k8s.io",
                        "versions": [{"groupVersion": "certificates.k8s.io/v1", "version": "v1"}],
                        "preferredVersion": {"groupVersion": "certificates.k8s.io/v1", "version": "v1"},
                    }
                ],
            }
        if path == "/api/v1":
            return {
                "kind": "APIResourceList",
                "apiVersion": "v1",
                "groupVersion": "v1",
                "resources": [{"name": "nodes", "singularName": "node", "namespaced": False, "kind": "Node", "verbs": ["get", "list", "watch", "patch", "update"]}],
            }
        if path == "/apis/certificates.k8s.io/v1":
            return {
                "kind": "APIResourceList",
                "apiVersion": "v1",
                "groupVersion": "certificates.k8s.io/v1",
                "resources": [
                    {"name": "certificatesigningrequests", "singularName": "certificatesigningrequest", "namespaced": False, "kind": "CertificateSigningRequest", "verbs": ["get", "list", "watch", "update"]},
                    {"name": "certificatesigningrequests/approval", "singularName": "", "namespaced": False, "kind": "CertificateSigningRequest", "verbs": ["update"]},
                ],
            }
        return None

    async def _node_request(self, writer: asyncio.StreamWriter, method: str, cluster_index: int, name: str, body: bytes) -> None:
        # Status is a Node subresource; normalize it to the owning Node for this simulator.
        name = name.split("/", 1)[0]
        node = self._nodes[cluster_index].get(name)
        if node is None:
            await self._send_json(writer, 404, {"kind": "Status", "status": "Failure", "reason": "NotFound"})
            return
        if method in {"PATCH", "PUT"} and body:
            patch = json.loads(body)
            metadata = patch.get("metadata", {})
            node.setdefault("labels", {}).update(metadata.get("labels", {}))
            node.setdefault("annotations", {}).update(metadata.get("annotations", {}))
            if "unschedulable" in patch.get("spec", {}):
                node["unschedulable"] = bool(patch["spec"]["unschedulable"])
            self._resource_version += 1
            await self._broadcast(cluster_index, "nodes", self._node_object(node, self._resource_version))
        if method not in {"GET", "PATCH", "PUT"}:
            await self._send_json(writer, 405, {"kind": "Status", "status": "Failure", "reason": "MethodNotAllowed"})
            return
        await self._send_json(writer, 200, self._node_object(node, self._resource_version))

    async def _admin_node_event(self, writer: asyncio.StreamWriter, body: bytes) -> None:
        try:
            event = json.loads(body)
            cluster_index = int(event["cluster"])
            node = self._nodes[cluster_index][str(event["node"])]
            for field in ("ready", "unschedulable"):
                if field in event:
                    node[field] = bool(event[field])
            self._resource_version += 1
            obj = self._node_object(node, self._resource_version)
            await self._broadcast(cluster_index, "nodes", obj)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            await self._send_json(writer, 400, {"error": "invalid cluster, node, or JSON body"})
            return
        await self._send_json(writer, 200, obj)

    async def _admin_csr_event(self, writer: asyncio.StreamWriter, body: bytes) -> None:
        try:
            event = json.loads(body)
            cluster_index = int(event["cluster"])
            name = str(event["name"])
            obj = dict(event["object"])
            obj.setdefault("apiVersion", "certificates.k8s.io/v1")
            obj.setdefault("kind", "CertificateSigningRequest")
            obj.setdefault("metadata", {})["name"] = name
            self._resource_version += 1
            obj["metadata"]["resourceVersion"] = str(self._resource_version)
            self._csrs[cluster_index][name] = obj
            await self._broadcast(cluster_index, "csrs", obj, "ADDED")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            await self._send_json(writer, 400, {"error": "invalid cluster, CSR, or JSON body"})
            return
        await self._send_json(writer, 200, obj)

    async def _csr_request(self, writer: asyncio.StreamWriter, method: str, cluster_index: int, name: str, body: bytes) -> None:
        name = name.split("/", 1)[0]
        obj = self._csrs[cluster_index].get(name)
        if obj is None:
            await self._send_json(writer, 404, {"kind": "Status", "status": "Failure", "reason": "NotFound"})
            return
        if method == "PUT" and body:
            obj = json.loads(body)
            self._resource_version += 1
            obj.setdefault("metadata", {})["resourceVersion"] = str(self._resource_version)
            self._csrs[cluster_index][name] = obj
            await self._broadcast(cluster_index, "csrs", obj)
        if method not in {"GET", "PUT"}:
            await self._send_json(writer, 405, {"kind": "Status", "status": "Failure", "reason": "MethodNotAllowed"})
            return
        await self._send_json(writer, 200, obj)

    async def _broadcast(self, cluster_index: int, kind: str, obj: dict[str, Any], event_type: str = "MODIFIED") -> None:
        for queue in tuple(self._watchers[(cluster_index, kind)]):
            await queue.put({"type": event_type, "object": obj})

    async def _watch(
        self,
        writer: asyncio.StreamWriter,
        cluster_index: int,
        kind: str,
        initial: list[dict[str, Any]] | None,
    ) -> None:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        watchers = self._watchers[(cluster_index, kind)]
        watchers.add(queue)
        task = asyncio.current_task()
        if task is not None:
            self._watch_tasks.add(task)
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        )
        await writer.drain()
        try:
            if initial is not None:
                for obj in initial:
                    await self._write_watch_event(writer, {"type": "ADDED", "object": obj})
                api_version = "v1" if kind == "nodes" else "certificates.k8s.io/v1"
                object_kind = "Node" if kind == "nodes" else "CertificateSigningRequest"
                await self._write_watch_event(
                    writer,
                    {
                        "type": "BOOKMARK",
                        "object": {
                            "apiVersion": api_version,
                            "kind": object_kind,
                            "metadata": {
                                "resourceVersion": str(self._resource_version),
                                "annotations": {"k8s.io/initial-events-end": "true"},
                            },
                        },
                    },
                )
            while True:
                event = await queue.get()
                await self._write_watch_event(writer, event)
        except (ConnectionError, asyncio.CancelledError):
            return
        finally:
            watchers.discard(queue)
            if task is not None:
                self._watch_tasks.discard(task)

    @staticmethod
    async def _write_watch_event(writer: asyncio.StreamWriter, event: dict[str, Any]) -> None:
        payload = json.dumps(event, separators=(",", ":")).encode() + b"\n"
        writer.write(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
        await writer.drain()

    @staticmethod
    async def _send_json(writer: asyncio.StreamWriter, status: int, value: dict[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable"}.get(status, "Error")
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
            + payload
        )
        await writer.drain()
