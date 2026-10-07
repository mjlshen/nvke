# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import base64
import unittest

from benchmarks.cwe_perf.protobuf import decode_csr_approval

# Generated with k8s.io/apimachinery v0.37.0 protobuf.NewSerializer and a
# certificates/v1 CertificateSigningRequest. Contains no certificate or key.
APPROVAL = base64.b64decode("azhzAAozChZjZXJ0aWZpY2F0ZXMuazhzLmlvL3YxEhlDZXJ0aWZpY2F0ZVNpZ25pbmdSZXF1ZXN0EmUKGwoLZml4dHVyZS1jc3ISABoAIgAqADIAOABCABIGEgAaADoAGj4KPAoIQXBwcm92ZWQSD0ZpeHR1cmVBcHByb3ZhbBoVbWF0Y2hlcyByZWNvcmRlZCBub2RlIgAqADIEVHJ1ZRoAIgA=")


class ProtobufApprovalTest(unittest.TestCase):
    def test_kubernetes_generated_approval(self) -> None:
        status = decode_csr_approval(APPROVAL, "fixture-csr")
        self.assertEqual([{"type": "Approved", "status": "True", "reason": "FixtureApproval",
                           "message": "matches recorded node"}], status["conditions"])

    def test_rejects_wrong_target_and_truncated_wire(self) -> None:
        for data, name in [(APPROVAL, "other-csr"), (APPROVAL[:-10], "fixture-csr"),
                           (b"k8s\x00\xff", "fixture-csr"), (b"invalid", "fixture-csr")]:
            with self.subTest(data=data, name=name), self.assertRaises(ValueError):
                decode_csr_approval(data, name)
