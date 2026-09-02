#!/usr/bin/env python3
"""Controlled local administration for the isolated business Runtime."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from runtime_common import load_config, storage_status, utc_now, write_heartbeat


ROOT = Path(__file__).resolve().parents[1]


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"READY", "DISABLED"} or payload.get("ok") else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    preflight = sub.add_parser("preflight")
    preflight.add_argument("task_kind", choices=("identity", "product_scan_dry_run", "product_scan", "live_monitor", "transcribe", "feishu_delivery", "knowledge_update", "full"))
    sub.add_parser("storage-init")
    sub.add_parser("safety")
    heartbeat = sub.add_parser("heartbeat")
    heartbeat.add_argument("--service", default="manual-health-check")
    args = parser.parse_args()

    if args.command == "status":
        return subprocess.call([str(ROOT / "bin" / "runtime_status.py"), "--json"])
    if args.command == "preflight":
        return subprocess.call([str(ROOT / "bin" / "preflight.py"), args.task_kind])

    config = load_config()
    if args.command == "storage-init":
        storage = config["storage"]
        root = Path(storage["root"])
        root.mkdir(parents=True, exist_ok=True)
        root.chmod(0o700)
        for path in storage.get("directories", {}).values():
            directory = Path(path)
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(0o700)
        state = storage_status(config)
        return emit({"ok": state["status"] == "READY", **state, "initialized_at": utc_now(), "business_actions_started": False})
    if args.command == "safety":
        return emit({"ok": True, "status": "READY", "safety": config.get("safety", {}), "note": "False means the real business action is fail-closed"})
    if args.command == "heartbeat":
        state = storage_status(config)
        started = utc_now()
        write_heartbeat(args.service, os.getpid(), state["status"], started, {"storage": state, "mode": "one-shot-read-only"})
        return emit({"ok": True, "status": state["status"], "service": args.service, "heartbeat_at": utc_now(), "business_actions_started": False})
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
