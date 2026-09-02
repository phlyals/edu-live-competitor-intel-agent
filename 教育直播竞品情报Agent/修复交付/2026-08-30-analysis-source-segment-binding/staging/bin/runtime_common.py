#!/usr/bin/env python3
"""Shared, local-only helpers for the competitor-intel Runtime."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config.json"
DB_PATH = ROOT / "runtime_v3.db"
PROFILE_ID = "edu_live_competitor_intel"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_config() -> dict:
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if data.get("profile_id") != PROFILE_ID:
        raise ValueError("Runtime config profile_id does not match the isolated Profile")
    storage = data.get("storage") or {}
    volume = Path(str(storage.get("volume", ""))).expanduser().resolve()
    root = Path(str(storage.get("root", ""))).expanduser().resolve()
    if not str(volume).startswith("/Volumes/") or root == volume or volume not in root.parents:
        raise ValueError("Storage root must be a dedicated directory below the configured /Volumes mount")
    for name, raw_path in (storage.get("directories") or {}).items():
        path = Path(str(raw_path)).expanduser().resolve()
        if root != path and root not in path.parents:
            raise ValueError(f"Configured directory escapes storage root: {name}")
    return data


def storage_status(config: dict | None = None) -> dict:
    try:
        config = config or load_config()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "ERROR", "reason": f"Invalid Runtime storage config: {exc}"}
    storage = config["storage"]
    volume = Path(storage["volume"])
    root = Path(storage["root"])
    paths = {name: Path(path) for name, path in storage.get("directories", {}).items()}
    result = {
        "path": str(root),
        "volume": str(volume),
        "required_free_bytes": int(storage.get("minimum_free_bytes", 0)),
        "checked_at": utc_now(),
    }
    if not volume.is_dir() or not os.path.ismount(volume):
        return {**result, "status": "WAITING_HUMAN", "reason": "ExternalStorage is not mounted"}
    if not root.is_dir():
        return {**result, "status": "WAITING_TOOL", "reason": "Configured storage root has not been initialized"}
    if not os.access(root, os.R_OK | os.W_OK | os.X_OK):
        return {**result, "status": "WAITING_HUMAN", "reason": "Configured storage root is not writable by the current user"}
    missing = [name for name, path in paths.items() if not path.is_dir()]
    if missing:
        return {**result, "status": "WAITING_TOOL", "reason": "Storage directory layout is incomplete", "missing_directories": missing}
    usage = shutil.disk_usage(root)
    result.update({
        "status": "READY",
        "free_bytes": usage.free,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "root_mode": oct(root.stat().st_mode & 0o777),
        "directories": {name: str(path) for name, path in paths.items()},
    })
    if usage.free < result["required_free_bytes"]:
        result["status"] = "WAITING_HUMAN"
        result["reason"] = "External storage is below the configured free-space safety threshold"
    return result


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def write_heartbeat(service_name: str, pid: int, status: str, started_at: str, details: dict) -> None:
    timestamp = utc_now()
    last_success = timestamp if status == "READY" else None
    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO service_heartbeats(
                service_name, pid, status, started_at, last_heartbeat_at,
                last_success_at, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(service_name) DO UPDATE SET
                pid=excluded.pid,
                status=excluded.status,
                started_at=excluded.started_at,
                last_heartbeat_at=excluded.last_heartbeat_at,
                last_success_at=COALESCE(excluded.last_success_at, service_heartbeats.last_success_at),
                details_json=excluded.details_json
            """,
            (service_name, pid, status, started_at, timestamp, last_success, json.dumps(details, ensure_ascii=False)),
        )
        conn.commit()


def safety_gate(config: dict, key: str, label: str) -> dict:
    enabled = bool((config.get("safety") or {}).get(key, False))
    if enabled:
        return {"status": "READY", "enabled": True, "capability": label}
    return {
        "status": "DISABLED",
        "enabled": False,
        "capability": label,
        "reason": "Safety gate is closed; explicit human approval has not been recorded",
    }
