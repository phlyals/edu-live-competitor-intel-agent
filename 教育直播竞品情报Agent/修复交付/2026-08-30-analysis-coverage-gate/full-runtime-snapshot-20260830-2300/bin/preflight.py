#!/usr/bin/env python3
"""Run a non-destructive, fail-closed Capability Preflight."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATUS = ROOT / "bin" / "runtime_status.py"

REQUIREMENTS = {
    "identity": ("runtime_config", "profile_gateway", "runtime", "runtime_worker"),
    "product_scan_dry_run": ("runtime_config", "runtime", "storage", "computer_use", "browser_giant_buyin", "scanner_validation_implementation"),
    "product_scan": ("runtime_config", "runtime", "storage", "computer_use", "browser_giant_buyin", "scanner_validation_implementation", "identity_resolution_implementation", "buyin_identity_resolution", "browser_scanner_implementation", "real_product_scan_gate"),
    "live_monitor": ("runtime_config", "runtime", "runtime_worker", "storage", "computer_use", "browser_giant_buyin", "identity_resolution_implementation", "buyin_identity_resolution", "qr_profile_resolver_implementation", "douyin_profile_mapping", "homepage_monitor_bridge", "live_source_mapping", "stream_tools", "monitor_implementation", "recorder_implementation", "live_monitor_gate", "recording_gate"),
    "transcribe": ("runtime_config", "runtime", "storage", "local_asr", "asr_chinese_validation", "transcription_implementation"),
    "feishu_delivery": ("runtime_config", "runtime", "feishu_business_workspace", "outbox_implementation", "feishu_business_write_gate"),
    "knowledge_update": ("runtime_config", "runtime", "knowledge_root", "knowledge_writer_implementation", "formal_knowledge_write_gate"),
}
REQUIREMENTS["full"] = tuple(dict.fromkeys(item for values in REQUIREMENTS.values() for item in values))
PRIORITY = {"ERROR": 60, "WAITING_HUMAN": 50, "WAITING_TOOL": 40, "DISABLED": 30, "DEGRADED": 20, "READY": 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_kind", choices=tuple(REQUIREMENTS))
    args = parser.parse_args()
    status = json.loads(subprocess.check_output([str(STATUS), "--json"], text=True))
    checks = []
    blockers = []
    for capability in REQUIREMENTS[args.task_kind]:
        detail = status.get(capability, {"status": "ERROR", "reason": "Capability is absent from Runtime status"})
        entry = {"capability_id": capability, **detail}
        checks.append(entry)
        if detail.get("status") != "READY":
            blockers.append(entry)
    overall = "READY"
    if blockers:
        overall = max((item.get("status", "ERROR") for item in blockers), key=lambda value: PRIORITY.get(value, 100))
    print(json.dumps({
        "task_kind": args.task_kind,
        "overall": overall,
        "ready": overall == "READY",
        "required_capabilities": list(REQUIREMENTS[args.task_kind]),
        "checks": checks,
        "blockers": blockers,
        "truthfulness": "This preflight command itself performed no browser action, scan, recording, Feishu write, or knowledge write; see capability evidence for separately authorized read-only checks",
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
