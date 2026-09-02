#!/usr/bin/env python3
"""Profile-isolated wrapper around the proven shared Douyin recorder."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path

from runtime_common import load_config, storage_status, utc_now


SHARED_PYTHON = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/avtranscribe/.venv/bin/python")
SHARED_RECORDER = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/avtranscribe/record_douyin_live.py")
UPSTREAM_ROOT = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/DouyinLiveRecorder")
V3_CONFIG = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3/v3_config.json")


def v3_activation_active() -> bool:
    try:
        payload = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
        return payload.get("profile_id") == "edu_live_competitor_intel" and (payload.get("atomic_activation") or {}).get("activation_state") == "ACTIVE"
    except (OSError, json.JSONDecodeError):
        return False


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"READY", "LIVE", "OFFLINE_CONFIRMED", "RECORDED"} else 1


def parse_last_json(text: str) -> dict | None:
    decoder = json.JSONDecoder()
    values = []
    for offset, char in enumerate(text or ""):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("status"):
            values.append(value)
    return values[-1] if values else None


def safe_id(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", value or ""):
        raise ValueError(f"{label} contains unsupported characters")
    return value


def self_test() -> int:
    checks = {
        "shared_python": SHARED_PYTHON.is_file(),
        "shared_recorder": SHARED_RECORDER.is_file(),
        "upstream_recorder": (UPSTREAM_ROOT / "src" / "spider.py").is_file() and (UPSTREAM_ROOT / "src" / "stream.py").is_file(),
        "ffmpeg": bool(shutil.which("ffmpeg") or Path("/opt/homebrew/bin/ffmpeg").is_file()),
        "ffprobe": bool(shutil.which("ffprobe") or Path("/opt/homebrew/bin/ffprobe").is_file()),
    }
    interface_ok = False
    interface_error = None
    if checks["shared_python"] and checks["shared_recorder"]:
        result = subprocess.run(
            [str(SHARED_PYTHON), str(SHARED_RECORDER), "--help"],
            capture_output=True, text=True, check=False, timeout=20,
        )
        interface_ok = result.returncode == 0 and "--check-only" in result.stdout
        interface_error = None if interface_ok else "shared recorder CLI validation failed"
    checks["shared_recorder_interface"] = interface_ok
    status = "READY" if all(checks.values()) else "WAITING_TOOL"
    return emit({
        "status": status,
        "checked_at": utc_now(),
        "implementation": str(Path(__file__).resolve()),
        "shared_recorder": str(SHARED_RECORDER),
        "upstream_root": str(UPSTREAM_ROOT),
        "checks": checks,
        "reason": interface_error if status != "READY" else None,
        "network_request_performed": False,
        "recording_started": False,
    })


def run_shared(command: list[str]) -> tuple[int, dict]:
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=None)
    payload = parse_last_json(result.stdout) or {
        "status": "error",
        "message": "Shared recorder returned no structured result",
        "return_code": result.returncode,
    }
    return result.returncode, payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--check-only", action="store_true")
    parser.add_argument("--url")
    parser.add_argument("--replay-file", help="Local replay verification; does not access the production DB")
    parser.add_argument("--replay-output-dir", help="Required isolated output directory for --replay-file")
    parser.add_argument("--duration", type=float, default=None, help="0 means record until the stream ends")
    parser.add_argument("--account-id")
    parser.add_argument("--session-id")
    parser.add_argument("--approved", action="store_true", help="Required in addition to the recording safety gate")
    parser.add_argument("--resume", action="store_true", help="Resume the same business session into an internal continuation file")
    parser.add_argument("--quality", default=None, help="Upstream quality (default: V3 recording_quality)")
    parser.add_argument("--approved-read-only-probe", action="store_true", help="One-off user-approved status probe without opening the persistent monitor gate")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if args.replay_file:
        if args.url or args.check_only or not args.approved or not args.replay_output_dir:
            return emit({"status": "ERROR", "reason": "Replay requires --approved and --replay-output-dir, without --url/--check-only"})
        source = Path(args.replay_file).expanduser().resolve()
        if not source.is_file():
            return emit({"status": "ERROR", "reason": "Replay source must be an existing local file"})
        output = Path(args.replay_output_dir).expanduser().resolve()
        command = [str(SHARED_PYTHON), str(SHARED_RECORDER), "--input-file", str(source),
                   "--duration", "0", "--output-dir", str(output), "--filename", "整场直播.ts",
                   "--segment-seconds", "0"]
        code, payload = run_shared(command)
        return emit({**payload, "status": "RECORDED" if code == 0 and payload.get("status") == "recorded" else "ERROR",
                     "return_code": code, "source_file": str(source), "verification_mode": "LOCAL_REPLAY",
                     "network_request_performed": False})
    if not args.url:
        return emit({"status": "ERROR", "reason": "--url is required"})

    config = load_config()
    try:
        v3_config = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return emit({"status": "ERROR", "reason": f"V3 config unavailable: {exc.__class__.__name__}"})
    requested_duration = float(args.duration if args.duration is not None else (v3_config.get("recording_duration_seconds") or 0))
    requested_quality = str(args.quality or v3_config.get("recording_quality") or "LD").upper()
    segment_seconds = float(v3_config.get("recording_segment_seconds") or 0)
    refresh_seconds = float(v3_config.get("recording_stream_refresh_seconds") or 0)
    safety = config.get("safety") or {}
    if args.check_only:
        if not safety.get("live_monitor", False) and not v3_activation_active() and not args.approved_read_only_probe:
            return emit({
                "status": "DISABLED",
                "reason": "The live_monitor safety gate is closed",
                "recording_started": False,
            })
        command = [
            str(SHARED_PYTHON), str(SHARED_RECORDER), "--url", args.url,
            "--check-only",
        ]
        return_code, payload = run_shared(command)
        mapped = "LIVE" if payload.get("status") == "live" else "OFFLINE_CONFIRMED" if payload.get("status") == "not_live" else "UNKNOWN"
        return emit({
            "status": mapped,
            "checked_at": utc_now(),
            "shared_status": payload.get("status"),
            "anchor_name": payload.get("anchor_name"),
            "title": payload.get("title"),
            "room_id": payload.get("room_id"),
            "return_code": return_code,
            "reason": payload.get("message") if mapped == "UNKNOWN" else None,
            "authorization_mode": "persistent_gate" if safety.get("live_monitor", False) else "one_off_read_only_probe",
            "recording_started": False,
        })

    if (not safety.get("recording", False) and not v3_activation_active()) or not args.approved:
        return emit({
            "status": "DISABLED",
            "reason": "Recording requires both an enabled recording gate and --approved",
            "recording_started": False,
        })
    if not args.account_id or not args.session_id:
        return emit({"status": "ERROR", "reason": "--account-id and --session-id are required for recording"})
    if requested_duration < 0 or requested_duration > 86400:
        return emit({"status": "ERROR", "reason": "--duration must be between 0 and 86400 seconds"})

    state = storage_status(config)
    if state.get("status") != "READY":
        return emit({"status": state.get("status", "ERROR"), "reason": state.get("reason"), "recording_started": False})
    account_id = safe_id(args.account_id, "account-id")
    session_id = safe_id(args.session_id, "session-id")
    partial_root = Path(config["storage"]["directories"]["media_partial"]).resolve()
    completed_root = Path(config["storage"]["directories"]["media_completed"]).resolve()
    partial_dir = partial_root / account_id / session_id
    completed_dir = completed_root / account_id / session_id
    if completed_dir.exists():
        return emit({"status": "ERROR", "reason": "Completed session output already exists; refusing to overwrite", "recording_started": False})
    if partial_dir.is_symlink():
        return emit({"status": "ERROR", "reason": "Session output directory must not be a symlink", "recording_started": False})
    partial_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    existing_parts = sorted(p for p in partial_dir.iterdir() if p.is_file() and (p.name.endswith('.ts.partial') or p.suffix=='.ts') and p.stat().st_size>0)
    if existing_parts and not args.resume:
        return emit({"status": "ERROR", "reason": "Session output already exists; use --resume to continue the same session", "recording_started": False})
    part_index = len(existing_parts)
    filename = "整场直播.ts.partial" if part_index == 0 else f"整场直播.part{part_index:02d}.ts.partial"
    while (partial_dir / filename).exists() or (partial_dir / (filename + ".recording-state.json")).exists():
        part_index += 1
        filename = f"整场直播.part{part_index:02d}.ts.partial"

    command = [
        str(SHARED_PYTHON), str(SHARED_RECORDER), "--url", args.url,
        "--duration", str(requested_duration), "--output-dir", str(partial_dir),
        "--filename", filename,
        "--quality", requested_quality,
        "--segment-seconds", str(segment_seconds),
        "--refresh-seconds", str(refresh_seconds),
    ]
    return_code, payload = run_shared(command)
    if payload.get("status") != "recorded" or return_code != 0:
        return emit({
            "status": "ERROR",
            "reason": payload.get("message") or "Shared recorder failed",
            "partial_dir": str(partial_dir),
            "return_code": return_code,
            "ffmpeg_return_code": payload.get("ffmpeg_return_code"),
            "ffmpeg_tail": payload.get("ffmpeg_tail") or payload.get("ffmpeg_error_tail"),
            "recording_state_path": payload.get("recording_state_path"),
            "stream_attempts": payload.get("stream_attempts"),
            "recording_started": (partial_dir / filename).is_file() and (partial_dir / filename).stat().st_size>0,
        })
    # Keep the whole-session file in partial storage until Runtime V3 confirms
    # the five-minute down window.  Continuation files exist only after a
    # recorder restart and are merged by the finalizer.
    part_path = partial_dir / filename
    if not part_path.exists():
        return emit({"status": "ERROR", "reason": "Recorder returned no partial media file", "partial_dir": str(partial_dir), "recording_started": True})
    return emit({
        "status": "RECORDED",
        "recorded_at": utc_now(),
        "account_id": account_id,
        "session_id": session_id,
        "output_path": str(part_path),
        "output_paths": [str(path) for path in sorted(partial_dir.glob("*.ts.partial"))],
        "completed_dir": str(completed_dir),
        "output_size_bytes": sum(path.stat().st_size for path in partial_dir.glob("*.ts.partial")),
        "segment_count": len(list(partial_dir.glob("*.ts.partial"))),
        "quality": requested_quality,
        "segment_seconds": segment_seconds,
        "refresh_seconds": refresh_seconds,
        "recorded_duration_seconds": payload.get("recorded_duration_seconds"),
        "part_index": part_index,
        "return_code": return_code,
        "ffmpeg_return_code": payload.get("ffmpeg_return_code"),
        "ffmpeg_tail": payload.get("ffmpeg_tail"),
        "recording_state_path": payload.get("recording_state_path"),
        "stream_attempts": payload.get("stream_attempts"),
    })


if __name__ == "__main__":
    raise SystemExit(main())
