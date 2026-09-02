import asyncio
import importlib.util
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def live(token: str) -> dict:
    return {
        "is_live": True,
        "stream_url": f"https://cdn.example/live.flv?token={token}",
        "stream_urls": [],
        "stream_protocol": "FLV",
        "anchor_name": "test",
        "room_id": "room",
        "quality": "LD",
    }


def measure(module, rounds: int = 5) -> list[float]:
    gaps = []
    real_sleep = time.sleep
    for run_index in range(rounds):
        calls = 0
        starts, ends = [], []

        async def resolve(_url, _quality):
            nonlocal calls
            calls += 1
            if calls == 1:
                return live(f"one-{run_index}")
            if calls == 2:
                # A compressed, deterministic stand-in for resolver latency.
                real_sleep(0.08)
                return live(f"two-{run_index}")
            return {"is_live": False, "stream_url": None}

        def record(_url, path, *_args):
            starts.append(time.monotonic())
            path.write_bytes(b"media")
            real_sleep(0.20 if len(starts) == 1 else 0.01)
            ends.append(time.monotonic())
            return (0, "") if len(starts) == 1 else (1, "final short read")

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(module, "resolve_stream", side_effect=resolve), \
                patch.object(module, "record_stream", side_effect=record), \
                patch.object(module, "probe_duration", side_effect=[0.2, None]), \
                patch.object(module.time, "sleep"), \
                patch.object(module, "REFRESH_PRERESOLVE_LEAD_SECONDS", 0.1, create=True):
            code, _ = module.record_live_with_refresh(
                "https://live.douyin.com/123", "LD",
                Path(directory) / "whole.ts.partial", "ffmpeg", 0.2,
            )
        if code != 0 or len(starts) < 2:
            raise AssertionError(f"measurement run failed: code={code}, starts={len(starts)}")
        gaps.append(starts[1] - ends[0])
    return gaps


if __name__ == "__main__":
    baseline = load(Path(sys.argv[1]), "handoff_baseline")
    candidate = load(Path(sys.argv[2]), "handoff_candidate")
    before = measure(baseline)
    after = measure(candidate)
    payload = {
        "clock": "compressed deterministic timing; no external network or real 900-second wait",
        "resolver_delay_seconds": 0.08,
        "baseline_gaps_seconds": before,
        "candidate_gaps_seconds": after,
        "baseline_median_seconds": statistics.median(before),
        "candidate_median_seconds": statistics.median(after),
        "reduction_rate": 1 - statistics.median(after) / statistics.median(before),
    }
    payload["status"] = "PASS" if payload["candidate_median_seconds"] < payload["baseline_median_seconds"] * 0.5 else "FAIL"
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    raise SystemExit(0 if payload["status"] == "PASS" else 1)
