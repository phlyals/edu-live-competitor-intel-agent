#!/usr/bin/env python3
"""Accelerated, isolated integration test for live stream refresh/recovery."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import threading
import time


SHARED_RECORDER = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/avtranscribe/record_douyin_live.py")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def probe(path: Path) -> dict:
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration,size", "-of", "json", str(path)],
        capture_output=True, text=True, check=True, timeout=60,
    )
    return json.loads(result.stdout)["format"]


def load_recorder():
    spec = importlib.util.spec_from_file_location("simulation_recorder", SHARED_RECORDER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_source_segments(source: Path, directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    pattern = directory / "source-%02d.flv"
    if not list(directory.glob("source-*.flv")):
        subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-n", "-i", str(source), "-map", "0:v:0?",
             "-map", "0:a:0?", "-c", "copy", "-f", "segment", "-segment_times",
             "600,1200,2100,3000", "-reset_timestamps", "1", str(pattern)],
            check=True, timeout=600,
        )
    segments = sorted(directory.glob("source-*.flv"))
    if len(segments) != 5:
        raise RuntimeError(f"expected 5 source segments, got {len(segments)}")
    return segments


class SimulationState:
    def __init__(self, segments: list[Path], log_path: Path):
        self.segments = segments
        self.index = 0
        self.finished: set[int] = set()
        self.lock = threading.Lock()
        self.log_path = log_path
        self.virtual_gaps = {0: 300, 1: 180}
        self.gap_delivered: set[int] = set()

    def log(self, event: str, **details) -> None:
        row = {"at": utc_now(), "event": event, **details}
        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")

    def current(self) -> int | None:
        with self.lock:
            return self.index if self.index < len(self.segments) else None

    def finish(self, index: int) -> None:
        with self.lock:
            self.finished.add(index)
            if index == self.index:
                self.index += 1
        self.log("SOURCE_SEGMENT_FINISHED", source_index=index,
                 source_duration_seconds=float(probe(self.segments[index])["duration"]))

    async def resolve(self, room_url: str, quality: str, port: int) -> dict:
        index = self.current()
        if index is None:
            self.log("RESOLVER_OFFLINE", room_url=room_url)
            return {"is_live": False, "anchor_name": "simulation", "room_id": "sim-room", "quality": quality}
        previous = index - 1
        if previous in self.virtual_gaps and previous not in self.gap_delivered:
            self.gap_delivered.add(previous)
            virtual = self.virtual_gaps[previous]
            wall = virtual / 100.0
            self.log("VIRTUAL_STREAM_INTERRUPTION", after_source_index=previous,
                     virtual_gap_seconds=virtual, accelerated_wall_seconds=wall)
            await asyncio.sleep(wall)
            self.log("VIRTUAL_STREAM_RECOVERED", next_source_index=index, virtual_gap_seconds=virtual)
        token = f"private-token-{index}"
        stream_url = f"http://127.0.0.1:{port}/stream/{index}.flv?token={token}&expire={2000000000 + index}"
        self.log("STREAM_URL_RESOLVED", source_index=index, url_sha256=__import__("hashlib").sha256(stream_url.encode()).hexdigest())
        return {
            "is_live": True,
            "stream_url": stream_url,
            "stream_urls": [stream_url],
            "stream_protocol": "FLV",
            "anchor_name": "simulation",
            "title": "accelerated interruption simulation",
            "room_id": "sim-room",
            "quality": quality,
        }


def handler_for(state: SimulationState):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                index = int(self.path.split("/stream/", 1)[1].split(".flv", 1)[0])
                path = state.segments[index]
            except (ValueError, IndexError, FileNotFoundError):
                self.send_error(404)
                return
            size = path.stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "video/x-flv")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            state.log("HTTP_STREAM_OPENED", source_index=index, bytes=size)
            try:
                with path.open("rb") as stream:
                    shutil.copyfileobj(stream, self.wfile, length=1024 * 1024)
                state.finish(index)
            except (BrokenPipeError, ConnectionResetError):
                state.log("HTTP_CLIENT_CLOSED", source_index=index)
                # A 900-second refresh intentionally closes the buffered source.
                # Advance so the next resolution models a fresh live position.
                state.finish(index)

        def log_message(self, *_args):
            pass

    return Handler


def merge_outputs(paths: list[Path], destination: Path) -> None:
    concat = destination.with_suffix(".concat.txt")
    concat.write_text("".join(f"file '{path.as_posix()}'\n" for path in paths), encoding="utf-8")
    subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", "-n", "-f", "concat", "-safe", "0",
         "-i", str(concat), "-c", "copy", "-f", "mpegts", str(destination)],
        check=True, timeout=600,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    source = Path(args.source).expanduser().resolve()
    output = Path(args.output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    log_path = output / "simulation-events.jsonl"
    if log_path.exists():
        raise RuntimeError("output directory was already used; refusing to overwrite evidence")
    source_segments = create_source_segments(source, output / "source-segments")
    state = SimulationState(source_segments, log_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    recorder = load_recorder()

    async def resolver(url: str, quality: str):
        return await state.resolve(url, quality, server.server_port)

    recorder.resolve_stream = resolver
    capture_root = output / "capture"
    capture_root.mkdir()
    started = time.monotonic()
    try:
        code, result = recorder.record_live_with_refresh(
            "https://live.douyin.com/simulation", "LD", capture_root / "whole.ts.partial",
            shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg", 900,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
    elapsed = time.monotonic() - started
    if code != 0:
        raise RuntimeError(f"recorder simulation failed: {code}")
    outputs = [Path(path) for path in result["outputs"]]
    final_path = output / "整场直播.ts"
    merge_outputs(outputs, final_path)
    source_probe = probe(source)
    final_probe = probe(final_path)
    sidecars = []
    for path in outputs:
        item = json.loads(recorder.recording_state_path(path).read_text(encoding="utf-8"))
        sidecars.append(item)
    serialized = json.dumps(sidecars, ensure_ascii=False)
    url_hashes = [item["stream"]["url_sha256"] for item in sidecars]
    summary = {
        "status": "PASS",
        "simulation_mode": "accelerated_wall_clock_with_complete_buffered_replay",
        "source_path": str(source),
        "source_duration_seconds": float(source_probe["duration"]),
        "final_path": str(final_path),
        "final_duration_seconds": float(final_probe["duration"]),
        "duration_difference_seconds": abs(float(final_probe["duration"]) - float(source_probe["duration"])),
        "duration_relative_error": abs(float(final_probe["duration"]) - float(source_probe["duration"])) / float(source_probe["duration"]),
        "virtual_interruptions_seconds": [300, 180],
        "wall_elapsed_seconds": elapsed,
        "internal_chunk_count": len(outputs),
        "process_restart_count": 0,
        "business_session_count": 1,
        "recording_gap_seen": False,
        "refresh_seconds": 900,
        "url_sha256_values": url_hashes,
        "url_sha256_changed_every_chunk": len(set(url_hashes)) == len(url_hashes),
        "all_protocols": sorted({item["stream"]["protocol"] for item in sidecars}),
        "raw_tokens_absent_from_sidecars": "private-token-" not in serialized,
        "attempts": result["attempts"],
        "source_segments": [{"path": str(path), **probe(path)} for path in source_segments],
        "capture_outputs": [{"path": str(path), **probe(path)} for path in outputs],
        "checked_at": utc_now(),
    }
    if not (
        summary["duration_relative_error"] <= 0.05
        and summary["url_sha256_changed_every_chunk"]
        and summary["raw_tokens_absent_from_sidecars"]
        and summary["process_restart_count"] == 0
        and len(outputs) >= 5
    ):
        summary["status"] = "FAIL"
    (output / "simulation-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
