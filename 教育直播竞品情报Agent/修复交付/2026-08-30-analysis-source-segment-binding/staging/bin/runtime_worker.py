#!/usr/bin/env python3
"""Heartbeat-only supervisor; it performs no scan, recording, or external write."""

from __future__ import annotations

import argparse
import json
import os
import signal
import time

from runtime_common import load_config, storage_status, utc_now, write_heartbeat


RUNNING = True


def stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    interval = max(15, min(args.interval, 300))
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started_at = utc_now()
    pid = os.getpid()
    print(json.dumps({"event": "runtime_supervisor_started", "pid": pid, "started_at": started_at, "mode": "heartbeat_only", "business_actions_enabled": False}), flush=True)
    while RUNNING:
        config = load_config()
        state = storage_status(config)
        write_heartbeat("runtime-supervisor", pid, state["status"], started_at, {
            "mode": "heartbeat_only",
            "business_actions_enabled": False,
            "storage": state,
        })
        deadline = time.monotonic() + interval
        while RUNNING and time.monotonic() < deadline:
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    write_heartbeat("runtime-supervisor", pid, "STOPPED", started_at, {"mode": "heartbeat_only", "stopped_at": utc_now()})
    print(json.dumps({"event": "runtime_supervisor_stopped", "pid": pid, "stopped_at": utc_now()}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
