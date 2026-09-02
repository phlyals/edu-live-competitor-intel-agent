#!/usr/bin/env python3
"""One-pass, tri-state live monitor for explicitly mapped Douyin room URLs."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from runtime_common import load_config, utc_now


ROOT = Path(__file__).resolve().parents[1]
RECORDER = ROOT / "bin" / "recorder.py"
PYTHON = ROOT / ".venv" / "bin" / "python"
V3_CONFIG = ROOT / "v3" / "v3_config.json"


def v3_activation_active() -> bool:
    try:
        payload = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
        return payload.get("profile_id") == "edu_live_competitor_intel" and (payload.get("atomic_activation") or {}).get("activation_state") == "ACTIVE"
    except (OSError, json.JSONDecodeError):
        return False


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"READY", "COMPLETE"} else 1


def parse_json(text: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"status": "UNKNOWN", "reason": "Recorder returned invalid JSON"}


def self_test() -> int:
    result = subprocess.run(
        [str(PYTHON), str(RECORDER), "--self-test"],
        capture_output=True, text=True, check=False, timeout=30,
    )
    recorder = parse_json(result.stdout)
    ready = RECORDER.is_file() and PYTHON.is_file() and recorder.get("status") == "READY"
    return emit({
        "status": "READY" if ready else "WAITING_TOOL",
        "checked_at": utc_now(),
        "implementation": str(Path(__file__).resolve()),
        "recorder_self_test": recorder,
        "monitoring_started": False,
        "recording_started": False,
    })


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--targets", type=Path)
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.targets:
        return emit({"status": "ERROR", "reason": "--targets is required"})

    config = load_config()
    if not (config.get("safety") or {}).get("live_monitor", False) and not v3_activation_active():
        return emit({"status": "DISABLED", "reason": "The live_monitor safety gate is closed", "monitoring_started": False})
    target_path = args.targets.expanduser().resolve()
    storage_root = Path(config["storage"]["root"]).resolve()
    if target_path != storage_root and storage_root not in target_path.parents:
        return emit({"status": "ERROR", "reason": "Targets file must stay inside the configured external storage root"})
    targets = json.loads(target_path.read_text(encoding="utf-8"))
    if not isinstance(targets, list):
        return emit({"status": "ERROR", "reason": "Targets file must contain a JSON list"})

    results = []
    for target in targets:
        competitor_id = str((target or {}).get("competitor_id") or "")
        live_url = str((target or {}).get("douyin_live_url") or "")
        if not competitor_id or not live_url:
            results.append({"competitor_id": competitor_id or None, "live_status": "UNKNOWN", "reason": "No current Douyin live URL is mapped"})
            continue
        process = subprocess.run(
            [str(PYTHON), str(RECORDER), "--url", live_url, "--check-only"],
            capture_output=True, text=True, check=False, timeout=60,
        )
        payload = parse_json(process.stdout)
        state = payload.get("status") if payload.get("status") in {"LIVE", "OFFLINE_CONFIRMED"} else "UNKNOWN"
        results.append({
            "competitor_id": competitor_id,
            "live_status": state,
            "checked_at": payload.get("checked_at") or utc_now(),
            "anchor_name": payload.get("anchor_name"),
            "title": payload.get("title"),
            "reason": payload.get("reason") if state == "UNKNOWN" else None,
        })
    return emit({
        "status": "COMPLETE",
        "checked_at": utc_now(),
        "target_count": len(results),
        "results": results,
        "recording_started": False,
    })


if __name__ == "__main__":
    raise SystemExit(main())
