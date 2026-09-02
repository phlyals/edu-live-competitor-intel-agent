#!/usr/bin/env python3
"""Measure the single-device LD recording path without touching real streams.

This is deliberately a local fixture test.  It proves the CPU/disk/process
path at the configured concurrency, but it never claims that 65 independent
remote live sources or the network have been verified.  The strict release
gate therefore keeps requiring a separate production-equivalent proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import connect, init_db, load_config, utc_now  # noqa: E402


FFMPEG = Path(shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg")
FFPROBE = Path(shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe")
EVIDENCE_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/capacity-smoke")
SOURCE_CANDIDATES = (
    Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/segment-self-test/00_直播录像_00000.ts"),
    Path("/Volumes/ExternalStorage/同行直播录制/media/completed/acct_565c3ca37ff7c78d/sess_fa45b280aa74e86625d5e7f3/00_直播录像.ts"),
)


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def probe(path: Path) -> dict:
    if not FFPROBE.is_file() or not path.is_file():
        return {}
    proc = subprocess.run([str(FFPROBE), "-v", "error", "-show_entries", "format=duration,size", "-show_entries", "stream=width,height,codec_name", "-of", "json", str(path)], capture_output=True, text=True, timeout=60, check=False)
    try:
        return json.loads(proc.stdout) if proc.returncode == 0 else {}
    except json.JSONDecodeError:
        return {}


def fixture(source: Path, root: Path) -> Path | None:
    target = root / "ld-fixture.ts"
    if not FFMPEG.is_file() or not source.is_file():
        return None
    command = [str(FFMPEG), "-hide_banner", "-loglevel", "error", "-y", "-i", str(source), "-t", "10", "-vf", "scale=720:1280", "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "450k", "-c:a", "aac", "-b:a", "64k", "-f", "mpegts", str(target)]
    proc = subprocess.run(command, capture_output=True, text=True, timeout=300, check=False)
    return target if proc.returncode == 0 and target.is_file() and target.stat().st_size > 0 else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--duration", type=int, default=20)
    args = parser.parse_args()
    config = load_config()
    activation = config.get("atomic_activation") or {}
    concurrency = int(args.concurrency or config.get("capacity_test_concurrency") or activation.get("capacity_test_concurrency") or 65)
    duration = max(5, int(args.duration))
    started = utc_now()
    root = EVIDENCE_ROOT / run_id()
    root.mkdir(parents=True, exist_ok=True)
    source = next((path for path in SOURCE_CANDIDATES if path.is_file()), None)
    fixture_path = fixture(source, root) if source else None
    report = {"status": "FAIL", "target_concurrency": concurrency, "duration_seconds": duration, "source_mode": "local_ld_fixture_stream_copy", "production_equivalent": False, "network_verified": False, "single_device_mode": bool(config.get("single_node_mode")), "started_at": started, "source_path": str(source) if source else None, "fixture_path": str(fixture_path) if fixture_path else None}
    if not fixture_path:
        report["reason"] = "无法生成本地LD测试样本"
    else:
        before_free = shutil.disk_usage(root).free
        processes = []
        for index in range(concurrency):
            output = root / f"recording-{index:03d}.ts"
            log = root / f"recording-{index:03d}.log"
            handle = log.open("wb")
            command = [str(FFMPEG), "-hide_banner", "-loglevel", "error", "-y", "-re", "-stream_loop", "-1", "-i", str(fixture_path), "-t", str(duration), "-map", "0:v:0?", "-map", "0:a:0?", "-c", "copy", "-f", "mpegts", str(output)]
            processes.append((subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT), handle, output))
        max_running = 0
        while True:
            running = sum(proc.poll() is None for proc, _, _ in processes)
            max_running = max(max_running, running)
            if running == 0:
                break
            time.sleep(0.5)
        for proc, handle, _ in processes:
            handle.close()
        failures = [proc.returncode for proc, _, _ in processes if proc.returncode != 0]
        outputs = [path for _, _, path in processes if path.is_file() and path.stat().st_size > 0]
        valid_probe = probe(outputs[0]) if outputs else {}
        after_free = shutil.disk_usage(root).free
        report.update({"status": "PASS" if len(outputs) == concurrency and not failures else "FAIL", "max_parallel_processes": max_running, "completed_outputs": len(outputs), "failed_processes": len(failures), "nonzero_return_codes": failures[:10], "disk_bytes_written": max(0, before_free - after_free), "sample_probe": valid_probe, "ended_at": utc_now(), "reason": None if len(outputs) == concurrency and not failures else "一个或多个本地录制进程失败"})
    report.setdefault("ended_at", utc_now())
    report["evidence_hash"] = hashlib.sha256(json.dumps(report, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    evidence_path = root / "capacity-smoke.json"
    evidence_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with connect() as conn:
        conn.execute("INSERT INTO capacity_test_runs(test_id,target_concurrency,duration_seconds,status,evidence_path,evidence_hash,metrics_json,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(test_id) DO UPDATE SET target_concurrency=excluded.target_concurrency,duration_seconds=excluded.duration_seconds,status=excluded.status,evidence_path=excluded.evidence_path,evidence_hash=excluded.evidence_hash,metrics_json=excluded.metrics_json,ended_at=excluded.ended_at", ("capacity-smoke:" + root.name, concurrency, duration, report["status"], str(evidence_path), report["evidence_hash"], json.dumps(report, ensure_ascii=False), started, report["ended_at"]))
        conn.commit()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
