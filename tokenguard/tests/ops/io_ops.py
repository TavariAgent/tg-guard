import json
import os
from typing import Any

from ...token_system import task_token_guard


@task_token_guard(
    operation_type='write_json_fast',
    tags={'weight': 'light',
          'storage_speed': 'MODERATE'}
)
def write_json_fast(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return {
        "path": path,
        "bytes": os.path.getsize(path),
        "keys": len(payload),
    }


@task_token_guard(
    operation_type='append_log_slow',
    tags={'weight': 'heavy',
          'storage_speed': 'MODERATE'}
)
def append_log_slow(path: str, message: str) -> dict[str, Any]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    return {
        "path": path,
        "chars": len(message),
    }


@task_token_guard(
    operation_type='write_blob_moderate',
    tags={'weight': 'medium',
          'storage_speed': 'MODERATE'}
)
def write_blob_moderate(path: str, size_kb: int) -> dict[str, Any]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    blob = b"x" * (size_kb * 1024)
    with open(path, "wb") as f:
        f.write(blob)
    return {
        "path": path,
        "bytes": len(blob),
    }