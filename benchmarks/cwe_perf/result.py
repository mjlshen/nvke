# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import datetime
import json
import pathlib
import platform
import subprocess
from typing import Any


def new_result(scenario: dict[str, Any], mode: str) -> dict[str, Any]:
    """Return the stable envelope used by both Python and Go harness layers."""
    return {
        "schemaVersion": 1,
        "mode": mode,
        "scenario": scenario,
        "startedAt": datetime.datetime.now(datetime.UTC).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "revision": _revision(),
        },
        "phases": [],
        "metrics": {},
        "errors": [],
    }


def _revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def write_result(path: str | pathlib.Path, result: dict[str, Any]) -> None:
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
