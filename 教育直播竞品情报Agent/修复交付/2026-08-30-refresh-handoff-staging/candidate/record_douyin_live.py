#!/usr/bin/env python3
"""Check and record one Douyin live room.

The Douyin URL parsing and stream discovery are delegated to the vendored
ihmily/DouyinLiveRecorder project.  This wrapper only adds a small, stable
interface for Hermes: URL, duration, and output directory.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlparse


RECORDER_ROOT = Path(
    os.environ.get(
        "DOUYIN_LIVE_RECORDER_ROOT",
        "/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/DouyinLiveRecorder",
    )
).expanduser()

# Resolve the next signed URL while the current FFmpeg process is still
# recording.  This removes resolver latency from the sequential handoff while
# keeping exactly one FFmpeg process active per room.
REFRESH_PRERESOLVE_LEAD_SECONDS = 30.0

if str(RECORDER_ROOT) not in sys.path:
    sys.path.insert(0, str(RECORDER_ROOT))


def emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def safe_name(value: str | None, fallback: str) -> str:
    value = (value or "").strip()
    value = re.sub(r"[\\/:*?\"<>|\s]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("._")
    return value[:80] or fallback


def redact_stream_urls(value: str) -> str:
    return re.sub(r"https?://\S+", "<stream-url>", value or "")


def ffmpeg_binary() -> str | None:
    configured = os.environ.get("FFMPEG_BIN")
    if configured and Path(configured).exists():
        return configured
    return shutil.which("ffmpeg") or (
        "/opt/homebrew/bin/ffmpeg" if Path("/opt/homebrew/bin/ffmpeg").exists() else None
    )


def ffprobe_binary() -> str | None:
    configured = os.environ.get("FFPROBE_BIN")
    if configured and Path(configured).exists():
        return configured
    return shutil.which("ffprobe") or (
        "/opt/homebrew/bin/ffprobe" if Path("/opt/homebrew/bin/ffprobe").exists() else None
    )


def validate_douyin_url(url: str) -> None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not host.endswith("douyin.com"):
        raise ValueError("只支持抖音直播间链接（live.douyin.com 或 v.douyin.com）。")


async def resolve_stream(url: str, quality: str) -> dict:
    try:
        from src import spider, stream  # type: ignore
    except Exception as exc:  # pragma: no cover - environment failure
        raise RuntimeError(f"DouyinLiveRecorder 导入失败：{exc}") from exc

    # The upstream project has separate handling for long web room URLs and
    # share/app URLs.  Reuse that distinction instead of duplicating parsing.
    if "v.douyin.com" in url or "/user/" in url:
        room_data = await spider.get_douyin_app_stream_data(url, proxy_addr=None, cookies=None)
    else:
        room_data = await spider.get_douyin_web_stream_data(url, proxy_addr=None, cookies=None)

    if not isinstance(room_data, dict):
        raise RuntimeError("上游录制器没有返回直播间信息。")

    stream_data = await stream.get_douyin_stream_url(room_data, quality, None)
    if not isinstance(stream_data, dict):
        raise RuntimeError("上游录制器没有返回直播流信息。")

    candidates = ordered_stream_urls(stream_data)
    return {
        "is_live": bool(stream_data.get("is_live")),
        "anchor_name": stream_data.get("anchor_name") or room_data.get("anchor_name"),
        "title": stream_data.get("title") or room_data.get("title"),
        "room_id": room_data.get("id_str") or room_data.get("id") or room_data.get("room_id"),
        # FLV is a continuous HTTP stream. Prefer it over HLS, whose short-
        # lived segment URLs caused playlist expiry and 404 gaps in production.
        "stream_url": candidates[0] if candidates else stream_data.get("record_url"),
        "stream_urls": list(dict.fromkeys(candidates)),
        "stream_protocol": "FLV" if candidates and candidates[0] == stream_data.get("flv_url") else "HLS",
        "quality": stream_data.get("quality") or quality,
    }


def ordered_stream_urls(stream_data: dict) -> list[str]:
    """Continuous FLV first; HLS remains an explicit fallback."""
    return list(dict.fromkeys(value for value in (stream_data.get("flv_url"), stream_data.get("m3u8_url")) if value))


def probe_duration(path: Path) -> float | None:
    binary = ffprobe_binary()
    if not binary:
        return None
    result = subprocess.run(
        [
            binary,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    try:
        return float(result.stdout.strip())
    except (TypeError, ValueError):
        return None


def recording_state_path(output_path: Path) -> Path:
    return output_path.with_name(output_path.name + ".recording-state.json")


def stream_fingerprint(stream_url: str) -> dict:
    """Comparable stream identity without persisting authentication values."""
    parsed = urlparse(stream_url)
    query_keys = sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)})
    return {
        "scheme": parsed.scheme.lower(),
        "hostname": (parsed.hostname or "").lower(),
        "path_sha256": hashlib.sha256(parsed.path.encode()).hexdigest(),
        "query_keys": query_keys,
        "query_sha256": hashlib.sha256(parsed.query.encode()).hexdigest() if parsed.query else None,
        "url_sha256": hashlib.sha256(stream_url.encode()).hexdigest(),
    }


def refresh_output_path(output_path: Path, index: int) -> Path:
    if index == 0:
        return output_path
    name = output_path.name
    marker = ".ts.partial"
    if name.endswith(marker):
        return output_path.with_name(f"{name[:-len(marker)]}.refresh{index:04d}{marker}")
    return output_path.with_name(f"{output_path.stem}.refresh{index:04d}{output_path.suffix}")


def schedule_stream_preresolution(url: str, quality: str, delay_seconds: float) -> dict:
    """Resolve one future stream URL in a cancellable daemon thread.

    The returned state intentionally contains the URL only in memory.  Callers
    persist the existing safe fingerprint, never the signed URL itself.
    """
    state = {
        "cancel": threading.Event(),
        "ready": threading.Event(),
        "info": None,
        "resolved_at": None,
        "error_kind": None,
    }

    def resolve_later() -> None:
        if state["cancel"].wait(max(0.0, delay_seconds)):
            state["ready"].set()
            return
        try:
            state["info"] = asyncio.run(resolve_stream(url, quality))
            state["resolved_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        except Exception as exc:  # The normal on-demand path remains fallback.
            state["error_kind"] = type(exc).__name__
        finally:
            state["ready"].set()

    thread = threading.Thread(target=resolve_later, daemon=True, name="stream-url-preresolve")
    state["thread"] = thread
    thread.start()
    return state


def record_stream(stream_url: str, output_path: Path, duration: float, ffmpeg: str,
                  segment_seconds: float = 0, stream_metadata: dict | None = None) -> tuple[int, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    network_input = urlparse(stream_url).scheme in {"http", "https"}
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin", "-nostats", "-n",
    ]
    if network_input:
        command.extend([
            "-rw_timeout", "15000000", "-reconnect", "1",
            "-reconnect_streamed", "1", "-reconnect_on_network_error", "1",
            "-reconnect_on_http_error", "429,5xx", "-reconnect_delay_max", "10",
            "-reconnect_max_retries", "5", "-reconnect_delay_total_max", "30",
        ])
    command.extend([
        "-i",
        stream_url,
        "-map",
        "0:v:0?",
        "-map",
        "0:a:0?",
        "-c",
        "copy",
        "-f",
        "segment" if segment_seconds > 0 else "mpegts",
    ])
    if duration > 0:
        # Insert the finite-duration flag immediately before stream mapping;
        # duration=0 intentionally means record until the live stream ends.
        map_index = command.index("-map")
        command[map_index:map_index] = ["-t", str(duration)]
    if segment_seconds > 0:
        command.extend(["-segment_time", str(segment_seconds), "-segment_format", "mpegts", "-reset_timestamps", "1"])
    command.append(str(output_path))
    # Drain stderr continuously with a strict memory bound. Long warning-heavy
    # streams must not accumulate hours of subprocess.run(capture_output) data.
    tail = deque(maxlen=50)
    lock = threading.Lock()
    now = lambda: datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    state = {"started_at": now(), "last_file_write_at": None, "ended_at": None,
             "return_code": None, "exit_kind": None, "status": "STARTING",
             "output_path": str(output_path), "input_kind": "LIVE_HTTP" if network_input else "LOCAL_REPLAY"}
    if stream_metadata:
        state["stream"] = stream_metadata
    state_path = recording_state_path(output_path)
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                               text=True, errors="replace")
    state.update(pid=process.pid, status="RUNNING")

    def drain():
        for line in iter(lambda: process.stderr.readline(8192), ""):
            with lock:
                tail.append(redact_stream_urls(line.rstrip()))
        process.stderr.close()

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    previous_handlers = {}

    def request_stop(signum, _frame):
        state["termination_signal"] = signum
        if process.poll() is None:
            try:
                process.send_signal(signum)
            except ProcessLookupError:
                pass

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.signal(signum, request_stop)

    def snapshot():
        try:
            paths = [output_path]
            if segment_seconds > 0:
                pattern = re.sub(r"%0?\d*d", "*", output_path.name)
                paths = list(output_path.parent.glob(pattern))
            stats = [p.stat() for p in paths if p.is_file()]
            state["output_size_bytes"] = sum(s.st_size for s in stats)
            if stats:
                state["last_file_write_at"] = datetime.fromtimestamp(max(s.st_mtime for s in stats), timezone.utc).isoformat(timespec="milliseconds")
            state["free_disk_bytes"] = shutil.disk_usage(output_path.parent).free
            state["checked_at"] = now()
            with lock:
                state["ffmpeg_tail"] = list(tail)
            temporary = state_path.with_name(state_path.name + f".{os.getpid()}.tmp")
            temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            temporary.replace(state_path)
        except OSError as exc:
            # Do not terminate a recording just because diagnostic writes fail.
            state["telemetry_write_error"] = type(exc).__name__

    try:
        snapshot()
        last_snapshot = time.monotonic()
        while process.poll() is None:
            time.sleep(0.2)
            if time.monotonic() - last_snapshot >= 5:
                snapshot()
                last_snapshot = time.monotonic()
        reader.join(timeout=2)
        code = process.wait()
        state.update(ended_at=now(), return_code=code, status="EXITED",
                     exit_kind="SIGNAL" if code < 0 or state.get("termination_signal") else "NORMAL" if code == 0 else "ERROR")
        snapshot()
        with lock:
            return code, "\n".join(tail)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def record_live_with_refresh(url: str, quality: str, output_path: Path, ffmpeg: str,
                             refresh_seconds: float) -> tuple[int, dict]:
    """Record bounded chunks, pre-resolving the next signed URL."""
    outputs: list[Path] = []
    attempts: list[dict] = []
    chunk_index = 0
    consecutive_failures = 0
    initial_resolution_misses = 0
    last_info: dict = {}
    pending_resolution: tuple[dict, str] | None = None
    while True:
        if pending_resolution is None:
            info = asyncio.run(resolve_stream(url, quality))
            resolved_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            resolution_source = "ON_DEMAND"
        else:
            info, resolved_at = pending_resolution
            pending_resolution = None
            resolution_source = "PRE_RESOLVED"
        last_info = info
        if not info.get("is_live") or not info.get("stream_url"):
            if outputs:
                # One false-negative resolver result previously ended an
                # otherwise healthy recording. Require three observations.
                for delay in (5, 15):
                    time.sleep(delay)
                    check = asyncio.run(resolve_stream(url, quality))
                    last_info = check
                    if check.get("is_live") and check.get("stream_url"):
                        info = check
                        break
                else:
                    return 0, {"status": "recorded", "info": last_info, "outputs": outputs, "attempts": attempts}
                # A later observation proved live; continue below using it.
                initial_resolution_misses = 0
            else:
                initial_resolution_misses += 1
                if initial_resolution_misses < 3:
                    time.sleep((5, 15)[initial_resolution_misses - 1])
                    continue
                return 2, {"status": "not_live", "info": info, "outputs": outputs, "attempts": attempts}
        initial_resolution_misses = 0
        chunk = refresh_output_path(output_path, chunk_index)
        while chunk.exists() or recording_state_path(chunk).exists():
            chunk_index += 1
            chunk = refresh_output_path(output_path, chunk_index)
        fingerprint = stream_fingerprint(info["stream_url"])
        lead_seconds = min(REFRESH_PRERESOLVE_LEAD_SECONDS, refresh_seconds / 2)
        preresolution = schedule_stream_preresolution(
            url, quality, max(0.0, refresh_seconds - lead_seconds)
        )
        try:
            code, tail = record_stream(info["stream_url"], chunk, refresh_seconds, ffmpeg, 0,
                                       {**fingerprint, "resolved_at": resolved_at,
                                        "resolution_source": resolution_source,
                                        "protocol": info.get("stream_protocol") or "UNKNOWN"})
        finally:
            preresolution["cancel"].set()
            preresolution["thread"].join(timeout=0.05)
        duration = probe_duration(chunk) if chunk.is_file() and chunk.stat().st_size > 0 else None
        if duration is not None:
            outputs.append(chunk)
        attempts.append({"chunk_index": chunk_index, "output_path": str(chunk), "return_code": code,
                         "duration_seconds": duration, "stream": fingerprint,
                         "stderr_tail": tail.splitlines()[-50:]})
        complete_chunk = code == 0 and duration is not None and duration >= refresh_seconds - 5
        if complete_chunk:
            consecutive_failures = 0
            prefetched = preresolution.get("info") if preresolution["ready"].is_set() else None
            if isinstance(prefetched, dict) and prefetched.get("is_live") and prefetched.get("stream_url"):
                pending_resolution = (prefetched, preresolution["resolved_at"])
        else:
            consecutive_failures += 1
            # A short final chunk can be a natural end. Require three fresh
            # offline resolutions before declaring it, because this resolver
            # was intermittently false-negative in the failed production run.
            offline = 0
            for delay in (0, 5, 15):
                if delay:
                    time.sleep(delay)
                check = asyncio.run(resolve_stream(url, quality))
                last_info = check
                if check.get("is_live") and check.get("stream_url"):
                    offline = 0
                    break
                offline += 1
            if offline == 3 and outputs:
                return 0, {"status": "recorded", "info": last_info, "outputs": outputs, "attempts": attempts}
            if consecutive_failures >= 3:
                return 4, {"status": "recording_failed", "info": last_info, "outputs": outputs, "attempts": attempts}
            time.sleep((5, 15)[consecutive_failures - 1])
        chunk_index += 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Check and record one Douyin live room")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--url", help="Douyin live room URL")
    source.add_argument("--input-file", help="Local replay file; no room probe or network request")
    parser.add_argument("--duration", type=float, default=1200, help="recording duration in seconds; 0 means until stream end")
    parser.add_argument("--output-dir", default=".", help="directory for the recording")
    parser.add_argument("--filename", default="00_直播录像.ts", help="recording filename")
    parser.add_argument("--segment-seconds", type=float, default=0, help="close a media segment at this interval; 0 keeps one file")
    parser.add_argument("--refresh-seconds", type=float, default=0, help="live URL refresh interval; 0 disables periodic refresh")
    parser.add_argument("--quality", default="OD", help="upstream quality code, default OD/original")
    parser.add_argument("--check-only", action="store_true", help="only check live status; do not record")
    args = parser.parse_args()

    try:
        if args.input_file:
            input_file = Path(args.input_file).expanduser().resolve()
            if not input_file.is_file() or args.check_only:
                raise ValueError("回放输入必须是存在的本地文件，且不能用于 --check-only。")
            info = {"is_live": True, "stream_url": str(input_file), "quality": "SOURCE"}
        else:
            validate_douyin_url(args.url)
            if not RECORDER_ROOT.exists():
                raise RuntimeError(f"DouyinLiveRecorder 不存在：{RECORDER_ROOT}")
            info = asyncio.run(resolve_stream(args.url, args.quality))
        if args.duration < 0 or args.refresh_seconds < 0:
            raise ValueError("--duration 和 --refresh-seconds 不能小于 0。")
        refreshed_live_mode = bool(args.url and not args.check_only and args.duration == 0 and args.refresh_seconds > 0)
        if not info["is_live"] and not refreshed_live_mode:
            emit(
                {
                    "status": "not_live",
                    "message": "当前未开播",
                    "url": args.url,
                    "anchor_name": info.get("anchor_name"),
                    "title": info.get("title"),
                    "room_id": info.get("room_id"),
                    "checked_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            return 2
        if not info.get("stream_url"):
            raise RuntimeError("直播状态为 LIVE，但没有可录制的流 URL；不能判定下播。")

        result_base = {
            "status": "live",
            "message": "当前正在直播",
            "url": args.url,
            "anchor_name": info.get("anchor_name"),
            "title": info.get("title"),
            "room_id": info.get("room_id"),
            "quality": info.get("quality"),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "duration_requested_seconds": args.duration,
        }
        if args.check_only:
            emit(result_base)
            return 0

        ffmpeg = ffmpeg_binary()
        if not ffmpeg:
            emit({**result_base, "status": "error", "message": "找不到 FFmpeg。"})
            return 3

        output_path = (Path(args.output_dir).expanduser() / args.filename).resolve()
        if output_path.exists():
            raise ValueError("目标录制文件已存在，拒绝覆盖；请使用新的续录文件名。")
        if args.url and args.duration == 0 and args.refresh_seconds > 0:
            return_code, live = record_live_with_refresh(args.url, args.quality, output_path, ffmpeg, args.refresh_seconds)
            outputs = live["outputs"]
            attempts = live["attempts"]
            total_size = sum(path.stat().st_size for path in outputs if path.is_file())
            total_duration = sum(float(item["duration_seconds"] or 0) for item in attempts)
            payload = {
                **result_base,
                "status": "recorded" if return_code == 0 and outputs else live["status"],
                "message": "直播录制完成" if return_code == 0 and outputs else "直播录制失败，已保留可能生成的文件。",
                "return_code": return_code,
                "output_path": str(outputs[-1]) if outputs else str(output_path),
                "output_paths": [str(path) for path in outputs],
                "output_size_bytes": total_size,
                "recorded_duration_seconds": total_duration if outputs else None,
                "segment_count": len(outputs),
                "refresh_seconds": args.refresh_seconds,
                "stream_attempts": attempts,
            }
            final_info = live.get("info") or {}
            for key in ("anchor_name", "title", "room_id", "quality"):
                if final_info.get(key) is not None:
                    payload[key] = final_info[key]
            emit(payload)
            return 0 if payload["status"] == "recorded" else (2 if live["status"] == "not_live" else 4)
        return_code, stderr_tail = record_stream(info["stream_url"], output_path, args.duration, ffmpeg, args.segment_seconds)
        result_base.update(recording_state_path=str(recording_state_path(output_path)),
                           ffmpeg_return_code=return_code, ffmpeg_tail=stderr_tail.splitlines()[-50:])
        patterns = [output_path]
        if args.segment_seconds > 0:
            wildcard = output_path.name.replace("%05d", "*").replace("%04d", "*").replace("%03d", "*").replace("%02d", "*")
            patterns = sorted(output_path.parent.glob(wildcard))
        existing = [path for path in patterns if path.exists() and path.stat().st_size > 0]
        total_size = sum(path.stat().st_size for path in existing)
        if return_code != 0 or not existing:
            emit(
                {
                    **result_base,
                    "status": "recording_failed",
                    "message": "直播录制失败，已保留可能生成的文件。",
                    "return_code": return_code,
                    "output_path": str(output_path),
                    "output_paths": [str(path) for path in existing],
                    "output_size_bytes": total_size,
                    "ffmpeg_error_tail": stderr_tail,
                }
            )
            return 4

        duration_probe = probe_duration(existing[-1]) if len(existing) == 1 else None
        emit(
            {
                **result_base,
                "status": "recorded",
                "message": "直播录制完成",
                "output_path": str(existing[-1]),
                "output_paths": [str(path) for path in existing],
                "output_size_bytes": total_size,
                "recorded_duration_seconds": duration_probe,
                "segment_count": len(existing),
                "segment_seconds": args.segment_seconds,
            }
        )
        return 0
    except Exception as exc:
        emit(
            {
                "status": "error",
                "message": str(exc),
                "url": args.url,
                "checked_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
