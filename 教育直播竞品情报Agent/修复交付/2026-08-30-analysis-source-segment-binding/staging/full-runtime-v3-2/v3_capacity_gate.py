#!/usr/bin/env python3
"""Record a truthful single-device capacity gate result.

The harness never labels a repeated-source or unmeasured test as final
capacity proof.  It records the exact configured target and missing evidence
so the release gate cannot be satisfied by an invented number.
"""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import connect, init_db, load_config, utc_now  # noqa: E402


def main() -> int:
    init_db()
    config = load_config()
    activation = config.get("atomic_activation") or {}
    target = int(config.get("capacity_test_concurrency") or activation.get("capacity_test_concurrency") or activation.get("max_concurrent_recordings") or 65)
    normal = int(config.get("normal_recording_concurrency") or activation.get("normal_recording_concurrency") or 30)
    expected = int(config.get("expected_max_recording_concurrency") or activation.get("expected_max_recording_concurrency") or 50)
    with connect() as conn:
        distinct_sources = conn.execute("SELECT count(DISTINCT live_url) FROM monitor_targets WHERE live_status='LIVE'").fetchone()[0]
        node_count = conn.execute("SELECT count(*) FROM worker_nodes").fetchone()[0]
    report = {
        "status": "BLOCKED_EXTERNAL",
        "target_concurrency": target,
        "normal_concurrency": normal,
        "expected_max_concurrency": expected,
        "duration_seconds": 0,
        "distinct_live_sources_observed": distinct_sources,
        "worker_node_count": node_count,
        "single_device_mode": bool(config.get("single_node_mode")),
        "production_equivalent": False,
        "network_verified": False,
        "hostname": socket.gethostname(),
        "reason": (
            f"尚未完成单设备 {target} 路生产等价录制压力证明；"
            f"当前配置为平时{normal}路、极限{expected}路并预留30%余量，"
            f"本轮只观测到{distinct_sources}个不同直播源，不能把重复源或空测标记为PASS。"
        ),
        "checked_at": utc_now(),
    }
    path = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/capacity-gate.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with connect() as conn:
        conn.execute("INSERT INTO capacity_test_runs(test_id,target_concurrency,duration_seconds,status,evidence_path,evidence_hash,metrics_json,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(test_id) DO UPDATE SET target_concurrency=excluded.target_concurrency,status=excluded.status,evidence_path=excluded.evidence_path,evidence_hash=excluded.evidence_hash,metrics_json=excluded.metrics_json,ended_at=excluded.ended_at", (f"capacity:{target}-single-device-gate", target, 0, report["status"], str(path), __import__("hashlib").sha256(path.read_bytes()).hexdigest(), json.dumps(report, ensure_ascii=False), report["checked_at"], report["checked_at"]))
        conn.commit()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
