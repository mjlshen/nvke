# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Decode the narrow Kubernetes protobuf CSR approval surface used by CWE.

The simulator retains the original CSR spec/metadata and accepts only status
conditions from an approval update. This is not a general Kubernetes codec.
Field numbers follow k8s.io/api/certificates/v1 and runtime.Unknown.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ValueError("truncated protobuf varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("oversized protobuf varint")


def _fields(data: bytes) -> dict[int, list[bytes | int]]:
    result: dict[int, list[bytes | int]] = {}
    offset = 0
    while offset < len(data):
        tag, offset = _varint(data, offset)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise ValueError("invalid protobuf field")
        if wire_type == 0:
            value, offset = _varint(data, offset)
        elif wire_type in (1, 2, 5):
            if wire_type == 2:
                size, offset = _varint(data, offset)
            else:
                size = 8 if wire_type == 1 else 4
            end = offset + size
            if end > len(data):
                raise ValueError("truncated protobuf field")
            value, offset = data[offset:end], end
        else:
            raise ValueError("unsupported protobuf wire type")
        result.setdefault(number, []).append(value)
    return result


def _bytes(fields: dict[int, list[bytes | int]], number: int) -> bytes:
    values = fields.get(number, [b""])
    if len(values) != 1 or not isinstance(values[0], bytes):
        raise ValueError("invalid protobuf bytes field")
    return values[0]


def decode_csr_approval(data: bytes, expected_name: str) -> dict[str, Any]:
    if not data.startswith(b"k8s\x00"):
        raise ValueError("missing Kubernetes protobuf prefix")
    envelope = _fields(data[4:])
    type_meta = _fields(_bytes(envelope, 1))
    if _bytes(type_meta, 1) != b"certificates.k8s.io/v1" or _bytes(type_meta, 2) != b"CertificateSigningRequest":
        raise ValueError("unexpected Kubernetes protobuf object")
    csr = _fields(_bytes(envelope, 2))
    metadata = _fields(_bytes(csr, 1))
    if _bytes(metadata, 1).decode() != expected_name:
        raise ValueError("CSR name differs from approval target")
    status = _fields(_bytes(csr, 3))
    conditions = []
    for encoded in status.get(1, []):
        if not isinstance(encoded, bytes):
            raise ValueError("invalid CSR condition")
        fields = _fields(encoded)
        condition = {name: _bytes(fields, number).decode() for number, name in
                     [(1, "type"), (2, "reason"), (3, "message"), (6, "status")]}
        for number, name in [(4, "lastUpdateTime"), (5, "lastTransitionTime")]:
            encoded_time = _bytes(fields, number)
            if encoded_time:
                seconds = _fields(encoded_time).get(1, [0])[0]
                if not isinstance(seconds, int):
                    raise ValueError("invalid condition time")
                condition[name] = datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace("+00:00", "Z")
        conditions.append(condition)
    return {"conditions": conditions}
