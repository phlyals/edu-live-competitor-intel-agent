#!/usr/bin/env python3
"""Durable post-recording pipeline for Runtime V3.

It is intentionally fail-closed: recording completion is never inferred from
process existence, and transcription/analysis are never marked complete until
their source files and output artifacts exist.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import socket
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from v3_runtime import connect, init_db, upsert_heartbeat, utc_now
from v3_analysis_contract import (
    ANALYSIS_SCOPE_FORMAL,
    ANALYSIS_SPEC_VERSION,
    MODEL_VERSION as ANALYSIS_MODEL_VERSION,
    PROMPT_VERSION as ANALYSIS_PROMPT_VERSION,
    QUALIFIED,
    SAMPLE_NONQUALIFYING,
    TRANSCRIPT_SCOPE_FULL,
    TRANSCRIPT_SCOPE_SAMPLE,
)
from v3_long_jobs import (
    TRANSCRIPT,
    LeaseLostError,
    claim_next,
    fail_or_retry,
    finish,
    parse_checkpoint,
    reconcile_exhausted,
    renew,
    save_checkpoint,
    versioned_output_path,
)


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "bin" / "python"
TRANSCRIBE = ROOT / "bin" / "transcribe.py"
FFMPEG = Path(shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg")
RUNNING = True
DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD = 0.90
DEFAULT_FULL_SESSION_COVERAGE_TARGET = 0.95
TRANSCRIPT_LEASE_SECONDS = 600


def stop(*_args):
    global RUNNING
    RUNNING = False


def transcript_worker_id() -> str:
    return f"pipeline-v3:{socket.gethostname()}:{os.getpid()}"


class ProcessTimeoutError(RuntimeError):
    pass


def run_process_with_lease(
    command: list[str],
    *,
    timeout: int,
    renew_callback=None,
    poll_seconds: float = 15.0,
) -> subprocess.CompletedProcess:
    """Run a child while periodically renewing the PostgreSQL lease."""
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        while process.poll() is None:
            if time.monotonic() - started >= timeout:
                process.kill()
                process.communicate()
                raise ProcessTimeoutError(f"child process exceeded {timeout}s")
            if renew_callback:
                renew_callback()
            try:
                process.wait(timeout=max(0.01, min(poll_seconds, timeout)))
            except subprocess.TimeoutExpired:
                pass
        stdout, stderr = process.communicate()
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    except Exception:
        if process.poll() is None:
            process.kill()
            process.communicate()
        raise


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_pipeline_config() -> dict:
    path = ROOT / "v3" / "v3_config.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _finite_number(value) -> float | None:
    """Return a finite float, rejecting booleans and JSON NaN/Infinity."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def timestamp_coverage(
    result: dict,
    audio_duration_seconds: float,
    threshold: float,
    target: float = DEFAULT_FULL_SESSION_COVERAGE_TARGET,
) -> dict:
    """Validate every ASR timestamp and calculate the interval union and gaps.

    A FULL_SESSION transcript is only qualified when every timestamp is valid
    and the timestamp union covers at least ``threshold`` of the actual audio.
    Invalid rows are never silently dropped to make a result pass.
    """
    duration = _finite_number(audio_duration_seconds)
    minimum = _finite_number(threshold)
    minimum = minimum if minimum is not None and 0 < minimum <= 1 else DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD
    desired = _finite_number(target)
    desired = desired if desired is not None and minimum <= desired <= 1 else max(minimum, DEFAULT_FULL_SESSION_COVERAGE_TARGET)
    segments = result.get("segments")
    errors: list[dict] = []
    intervals: list[tuple[float, float]] = []
    invalid_count = 0

    if duration is None or duration <= 0:
        errors.append({"code": "INVALID_AUDIO_DURATION"})
    if not isinstance(segments, list):
        errors.append({"code": "SEGMENTS_NOT_LIST"})
        segments = []
    elif not segments:
        errors.append({"code": "SEGMENTS_EMPTY"})

    for index, segment in enumerate(segments):
        if not isinstance(segment, dict):
            invalid_count += 1
            if len(errors) < 100:
                errors.append({"code": "SEGMENT_NOT_OBJECT", "index": index})
            continue
        start = _finite_number(segment.get("start"))
        end = _finite_number(segment.get("end"))
        if start is None or end is None:
            invalid_count += 1
            if len(errors) < 100:
                errors.append({"code": "NON_FINITE_TIMESTAMP", "index": index})
            continue
        if duration is None or duration <= 0 or start < 0 or end <= start or end > duration:
            invalid_count += 1
            if len(errors) < 100:
                errors.append({"code": "TIMESTAMP_OUT_OF_RANGE", "index": index, "start": start, "end": end})
            continue
        intervals.append((start, end))

    merged: list[list[float]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])

    raw_covered = sum(end - start for start, end in intervals)
    covered = sum(end - start for start, end in merged)
    gaps: list[dict] = []
    if duration is not None and duration > 0:
        cursor = 0.0
        for start, end in merged:
            if start > cursor:
                gaps.append({"start_time": cursor, "end_time": start})
            cursor = max(cursor, end)
        if cursor < duration:
            gaps.append({"start_time": cursor, "end_time": duration})
    rate = covered / duration if duration is not None and duration > 0 else 0.0
    raw_rate = raw_covered / duration if duration is not None and duration > 0 else 0.0
    valid = not errors and invalid_count == 0
    qualified = valid and rate >= minimum
    return {
        "schema_version": 1,
        "audio_duration_seconds": duration,
        "segment_count": len(segments),
        "valid_segment_count": len(intervals),
        "invalid_segment_count": invalid_count,
        "raw_covered_duration_seconds": raw_covered,
        "raw_coverage_rate": raw_rate,
        "covered_duration_seconds": covered,
        "coverage_rate": rate,
        "covered_segments": [{"start_time": start, "end_time": end} for start, end in merged],
        "gaps": gaps,
        "minimum_coverage_rate": minimum,
        "target_coverage_rate": desired,
        "timestamps_valid": valid,
        "is_qualified": qualified,
        "meets_target": valid and rate >= desired,
        "validation_errors": errors,
    }


def _metadata(value) -> dict:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _qualified_full_session(row, threshold: float = DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD) -> bool:
    metadata = _metadata(row["metadata_json"])
    quality = metadata.get("timestamp_coverage") or {}
    rate = _finite_number(quality.get("coverage_rate"))
    minimum = _finite_number(threshold)
    minimum = minimum if minimum is not None and 0 < minimum <= 1 else DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD
    return (
        row["status"] == "COMPLETE"
        and metadata.get("coverage_scope") == "FULL_SESSION"
        and quality.get("is_qualified") is True
        and quality.get("timestamps_valid") is True
        and rate is not None
        and rate >= minimum
    )


def _current_quality_gate(row, threshold: float, target: float) -> bool:
    """Return whether this row already carries the current gate calculation."""
    metadata = _metadata(row["metadata_json"])
    quality = metadata.get("timestamp_coverage") or {}
    minimum = _finite_number(threshold)
    minimum = minimum if minimum is not None and 0 < minimum <= 1 else DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD
    desired = _finite_number(target)
    desired = desired if desired is not None and minimum <= desired <= 1 else max(minimum, DEFAULT_FULL_SESSION_COVERAGE_TARGET)
    stored_minimum = _finite_number(quality.get("minimum_coverage_rate"))
    stored_target = _finite_number(quality.get("target_coverage_rate"))
    same_gate = (
        metadata.get("coverage_scope") == "FULL_SESSION"
        and quality.get("schema_version") == 1
        and stored_minimum is not None
        and stored_target is not None
        and abs(stored_minimum - minimum) <= 1e-12
        and abs(stored_target - desired) <= 1e-12
    )
    if not same_gate:
        return False
    if row["status"] == "COMPLETE":
        return _qualified_full_session(row,minimum)
    return row["status"] == "QUALITY_BLOCKED" and quality.get("is_qualified") is False


def register_segments() -> int:
    changed = 0
    with connect() as conn:
        jobs = conn.execute("SELECT j.*,s.status AS session_status,s.started_at,s.ended_at FROM recording_jobs j JOIN live_sessions s ON s.session_id=j.session_id WHERE j.status IN ('COMPLETE','WAITING_STREAM','RUNNING','STARTING') OR s.status='MEDIA_COMPLETE'").fetchall()
        for job in jobs:
            completed_root = Path(job["completed_dir"])
            root = Path(job["completed_dir"] if completed_root.exists() else job["partial_dir"])
            media_complete = completed_root.exists() or job["session_status"] in {"MEDIA_COMPLETE", "ENDED", "DUPLICATE_SUPERSEDED"}
            for path in sorted(root.glob("*.ts")) if root.exists() else []:
                seg_id = "segment_" + hashlib.sha256(f"{job['session_id']}:{path.name}".encode()).hexdigest()[:24]
                try:size = path.stat().st_size
                except FileNotFoundError:continue  # finalizer moved this directory
                prior = conn.execute("SELECT segment_id,checksum,bytes FROM recording_segments WHERE session_id=? AND path=?", (job["session_id"], str(path))).fetchone()
                try:checksum = str(prior["checksum"]) if prior and prior["checksum"] and int(prior["bytes"] or 0) == size else digest(path)
                except FileNotFoundError:continue
                if prior:seg_id=prior['segment_id']
                old=conn.execute('SELECT session_id,path,checksum FROM recording_segments WHERE segment_id=?',(seg_id,)).fetchone()
                if old and (old['session_id']!=job['session_id'] or (old['path']!=str(path) and Path(old['path']).is_file() and old['checksum']!=checksum)):
                    conn.execute("UPDATE recording_jobs SET last_error='media relocation conflict; original files retained' WHERE session_id=?",(job['session_id'],))
                    continue
                # Discovery never promotes a segment to canonical.  Only the
                # finalizer may do that after manifest and media validation.
                conn.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,captured_to,status,bytes,lifecycle_status,lifecycle_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(segment_id) DO UPDATE SET path=excluded.path,checksum=excluded.checksum,bytes=excluded.bytes,status=excluded.status,captured_to=excluded.captured_to,lifecycle_updated_at=CASE WHEN recording_segments.lifecycle_status='UNCLASSIFIED' THEN recording_segments.lifecycle_updated_at ELSE excluded.lifecycle_updated_at END", (seg_id, job["session_id"], str(path), checksum, job["started_at"] or utc_now(), job["ended_at"], "COMPLETE" if media_complete else "PARTIAL", size, "SOURCE_RETAINED", utc_now()))
                changed += 1
        conn.commit()
    return changed


def media_duration(path: Path) -> float | None:
    if not path.is_file():return None
    proc=subprocess.run(['/opt/homebrew/bin/ffprobe','-v','error','-show_entries','format=duration','-of','json',str(path)],capture_output=True,text=True,timeout=60,check=False)
    try:
        value=float(json.loads(proc.stdout)['format']['duration'])
        return value if proc.returncode==0 and value>0 else None
    except (ValueError,KeyError,TypeError):return None


def extract_audio(
    media: Path,
    audio: Path,
    *,
    max_seconds: int = 0,
    bitrate_kbps: int = 48,
    attempt_tag: str | None = None,
    renew_callback=None,
) -> bool:
    """Extract speech audio; max_seconds=0 means the complete final session.

    Opus is used for the retained speech track to avoid keeping 16-kHz PCM for
    seven days.  The source media and timestamps remain unchanged, and
    faster-whisper decodes the compressed speech track directly through
    FFmpeg.
    """
    if not FFMPEG.is_file() or not media.is_file():
        return False
    duration=media_duration(media)
    if duration is None:return False
    expected=min(duration,max_seconds) if max_seconds>0 else duration
    existing=media_duration(audio)
    if existing is not None and abs(existing-expected)<=2:return True
    audio.parent.mkdir(parents=True, exist_ok=True)
    safe_tag = re.sub(r"[^A-Za-z0-9_.-]", "_", attempt_tag or str(os.getpid()))
    temporary=audio.with_name(audio.stem+f'.extracting.{safe_tag}'+audio.suffix)
    command = [str(FFMPEG), "-hide_banner", "-loglevel", "error", "-y", "-i", str(media)]
    if max_seconds > 0:
        command.extend(["-t", str(max_seconds)])
    command.extend(["-vn", "-ac", "1", "-ar", "16000"])
    if audio.suffix.lower() == ".opus":
        command.extend(["-c:a", "libopus", "-b:a", f"{max(16, int(bitrate_kbps))}k", "-vbr", "on", "-application", "voip"])
    elif audio.suffix.lower() in {".m4a", ".aac"}:
        command.extend(["-c:a", "aac", "-b:a", f"{max(32, int(bitrate_kbps))}k"])
    else:
        command.extend(["-c:a", "pcm_s16le"])
    command.append(str(temporary))
    try:
        proc = run_process_with_lease(command, timeout=1800, renew_callback=renew_callback)
    except ProcessTimeoutError:
        temporary.unlink(missing_ok=True)
        return False
    except LeaseLostError:
        temporary.unlink(missing_ok=True)
        raise
    actual=media_duration(temporary) if proc.returncode==0 else None
    if actual is None or abs(actual-expected)>2:
        temporary.unlink(missing_ok=True)
        return False
    # The final audio name is content-addressed by the canonical media digest.
    # Competing stale attempts may create their own temporary files, but only a
    # validated file is atomically published at this deterministic path.
    if renew_callback:
        renew_callback()
    # ``existing`` was already rejected above when its duration did not match.
    # Replacing that invalid intermediate is safe and prevents a permanent
    # retry loop; a validated content-addressed audio file is reused unchanged.
    temporary.replace(audio)
    return True


class TranscriptJobError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool):
        super().__init__(message)
        self.code = code
        self.retryable = bool(retryable)


def write_json_atomic(path: Path, payload: dict) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    json.loads(serialized)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        json.loads(temporary.read_text(encoding="utf-8"))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def discover_transcript_jobs() -> int:
    """Create durable rows before any audio extraction or ASR work starts."""
    config = load_pipeline_config()
    quality_config = config.get("transcript_quality") or {}
    coverage_threshold = quality_config.get(
        "full_session_min_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD
    )
    coverage_target = quality_config.get(
        "full_session_target_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_TARGET
    )
    discovered = 0
    with connect() as conn:
        sessions = [dict(r) for r in conn.execute(
            "SELECT DISTINCT s.session_id,s.status,s.ended_at,s.completeness,"
            "s.metadata_json AS session_metadata,r.path,r.segment_id,r.checksum "
            "FROM live_sessions s JOIN recording_segments r ON r.session_id=s.session_id "
            "WHERE r.status='COMPLETE' AND r.lifecycle_status='CANONICAL_ACTIVE' "
            "AND s.status='MEDIA_COMPLETE' ORDER BY s.ended_at DESC,r.path"
        )]
    for session in sessions:
        media = Path(session["path"])
        full_session = media.name == "整场直播.ts"
        media_coverage = _metadata(session["session_metadata"]).get("media_coverage") or {}
        if full_session and (
            session["completeness"] != "COMPLETE" or media_coverage.get("continuous_capture") is not True
        ):
            continue
        if not media.is_file():
            continue
        media_hash = str(session["checksum"] or digest(media))
        source_digest = hashlib.sha256(("FULL_SESSION:" + media_hash).encode()).hexdigest() if full_session else media_hash
        transcript_id = "transcript_" + hashlib.sha256(
            f"{session['session_id']}:{source_digest}".encode()
        ).hexdigest()[:24]
        now = utc_now()
        checkpoint = {
            "schema_version": 1,
            "phase": "DISCOVERED",
            "session_id": session["session_id"],
            "segment_id": session["segment_id"],
            "media_path": str(media),
            "media_sha256": media_hash,
            "source_digest": source_digest,
            "full_session": full_session,
            "recording_completeness": session["completeness"],
            "ended_at": session["ended_at"],
        }
        metadata = {
            "coverage_scope": "FULL_SESSION" if full_session else "SAMPLE",
            "source_segment_id": session["segment_id"],
            "segment_id": session["segment_id"],
            "sample_only": not full_session,
            "sample_seconds": None if full_session else 300,
            "model_name": str(config.get("asr_model") or "faster-whisper-small"),
        }
        transcript_scope = TRANSCRIPT_SCOPE_FULL if full_session else TRANSCRIPT_SCOPE_SAMPLE
        waiting_qualification = "PENDING_QUALIFICATION" if full_session else SAMPLE_NONQUALIFYING
        with connect() as conn:
            previous = conn.execute(
                "SELECT * FROM transcripts WHERE session_id=? AND source_digest=?",
                (session["session_id"], source_digest),
            ).fetchone()
            if previous:
                if (
                    full_session and previous["status"] == "COMPLETE"
                    and not _current_quality_gate(previous, coverage_threshold, coverage_target)
                    and previous["output_path"] and Path(previous["output_path"]).is_file()
                ):
                    checkpoint.update({
                        "phase": "LEGACY_ARTIFACT_REVALIDATION",
                        "audio_path": previous["source_path"],
                        "asr_output_path": previous["output_path"],
                        "asr_output_sha256": digest(Path(previous["output_path"])),
                    })
                    conn.execute(
                        "UPDATE transcripts SET status='PENDING',next_attempt_at=?,updated_at=?,checkpoint_json=? "
                        "WHERE transcript_id=? AND status='COMPLETE' AND lease_owner IS NULL",
                        (now, now, json.dumps(checkpoint, ensure_ascii=False, sort_keys=True), previous["transcript_id"]),
                    )
                    discovered += 1
                    conn.commit()
                continue
            cursor = conn.execute(
                "INSERT OR IGNORE INTO transcripts("
                "transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,updated_at,"
                "next_attempt_at,checkpoint_json,scope,qualification_status,metadata_json) "
                "VALUES(?,?,?,?,?,'PENDING',?,?,?,?,?,?,?,?)",
                (transcript_id, session["session_id"], source_digest, "faster-whisper",
                 str(config.get("asr_model") or "faster-whisper-small"), str(media), now, now, now,
                 json.dumps(checkpoint, ensure_ascii=False, sort_keys=True),
                 transcript_scope, waiting_qualification,
                 json.dumps(metadata, ensure_ascii=False, sort_keys=True)),
            )
            discovered += max(0, int(cursor.rowcount or 0))
            conn.commit()
    return discovered


def _claim_source(job: dict) -> tuple[dict, Path, str, bool]:
    checkpoint = parse_checkpoint(job.get("checkpoint_json"))
    with connect() as conn:
        session = conn.execute(
            "SELECT s.session_id,s.ended_at,s.completeness,s.metadata_json AS session_metadata,"
            "r.segment_id,r.path,r.checksum FROM live_sessions s JOIN recording_segments r "
            "ON r.session_id=s.session_id WHERE s.session_id=? AND s.status='MEDIA_COMPLETE' "
            "AND r.status='COMPLETE' AND r.lifecycle_status='CANONICAL_ACTIVE' ORDER BY r.path",
            (job["session_id"],),
        ).fetchall()
    expected_digest = str(job["source_digest"])
    for candidate in map(dict, session):
        media = Path(candidate["path"])
        if not media.is_file():
            continue
        full_session = media.name == "整场直播.ts"
        media_hash = str(candidate["checksum"] or digest(media))
        derived = hashlib.sha256(("FULL_SESSION:" + media_hash).encode()).hexdigest() if full_session else media_hash
        if derived == expected_digest:
            if checkpoint.get("media_sha256") not in {None, "", media_hash}:
                raise TranscriptJobError("SOURCE_DIGEST_CHANGED", "canonical media changed after discovery", retryable=False)
            return candidate, media, media_hash, full_session
    raise TranscriptJobError("CANONICAL_MEDIA_MISSING", "current canonical media for transcript is missing", retryable=False)


def process_transcript_claim(job: dict) -> str:
    config = load_pipeline_config()
    quality_config = config.get("transcript_quality") or {}
    coverage_threshold = quality_config.get(
        "full_session_min_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD
    )
    coverage_target = quality_config.get(
        "full_session_target_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_TARGET
    )
    # Keep the newest in-memory checkpoint across every failure path.  Each
    # successful phase also updates ``job['checkpoint_json']`` through
    # ``save_checkpoint``; retaining the local object prevents a later ASR or
    # publication error from replacing that durable progress with the stale
    # checkpoint that was present when the claim was first acquired.
    checkpoint = parse_checkpoint(job.get("checkpoint_json"))
    try:
        session, media, media_hash, full_session = _claim_source(job)

        def renew_claim() -> None:
            with connect() as conn:
                renew(conn, TRANSCRIPT, job, lease_seconds=TRANSCRIPT_LEASE_SECONDS)
            upsert_heartbeat(
                "pipeline-v3",
                "READY",
                {
                    "phase": "TRANSCRIPT_RUNNING",
                    "transcript_id": job["transcript_id"],
                    "session_id": job["session_id"],
                    "lease_epoch": int(job["lease_epoch"]),
                    "attempt": int(job["attempts"]),
                    "lease_until": job.get("lease_until"),
                    "checked_at": utc_now(),
                },
                success=True,
            )

        audio_format = str(config.get("asr_audio_format") or "opus").lower().lstrip(".")
        audio_ext = "." + audio_format
        preferred_audio = (
            media.with_name(media.stem + "." + media_hash[:12] + audio_ext)
            if full_session else media.with_suffix(".sample300s" + audio_ext)
        )
        source_duration = media_duration(media)
        if source_duration is None:
            raise TranscriptJobError("SOURCE_DURATION_INVALID", "canonical media duration is invalid", retryable=False)
        expected_audio_duration = source_duration if full_session else min(source_duration, 300)
        checkpoint_audio = Path(str(checkpoint.get("audio_path") or ""))
        checkpoint_audio_duration = media_duration(checkpoint_audio) if checkpoint_audio.is_file() else None
        checkpoint_audio_valid = (
            checkpoint_audio_duration is not None
            and abs(checkpoint_audio_duration - expected_audio_duration) <= 2
        )
        audio = checkpoint_audio if checkpoint_audio_valid else preferred_audio
        attempt_tag = f"lease-{int(job['lease_epoch']):08d}.attempt-{int(job['attempts']):04d}"
        existing_audio_duration = media_duration(audio) if audio.is_file() else None
        if (existing_audio_duration is None or abs(existing_audio_duration - expected_audio_duration) > 2) and not extract_audio(
            media, audio, max_seconds=0 if full_session else 300,
            bitrate_kbps=int(config.get("asr_audio_bitrate_kbps") or 48),
            attempt_tag=attempt_tag, renew_callback=renew_claim,
        ):
            raise TranscriptJobError(
                "AUDIO_EXTRACTION_FAILED", "audio extraction or duration validation failed", retryable=True
            )
        renew_claim()
        audio_duration = media_duration(audio)
        if audio_duration is None:
            raise TranscriptJobError("AUDIO_DURATION_INVALID", "extracted audio duration is invalid", retryable=True)
        audio_hash = digest(audio)
        checkpoint.update({
            "schema_version": 1, "phase": "AUDIO_READY", "media_path": str(media),
            "media_sha256": media_hash, "audio_path": str(audio), "audio_sha256": audio_hash,
            "audio_duration_seconds": audio_duration, "full_session": full_session,
        })
        with connect() as conn:
            save_checkpoint(conn, TRANSCRIPT, job, checkpoint)

        base_output = audio.with_suffix(".transcript.json")
        asr_source_output = None
        result = None
        prior_output = Path(str(checkpoint.get("asr_output_path") or ""))
        if prior_output.is_file() and checkpoint.get("asr_output_sha256") == digest(prior_output):
            try:
                prior_result = json.loads(prior_output.read_text(encoding="utf-8"))
                prior_duration = _finite_number(prior_result.get("duration")) if isinstance(prior_result, dict) else None
                if (
                    isinstance(prior_result, dict) and prior_result.get("status") == "READY"
                    and prior_duration is not None and abs(prior_duration - audio_duration) <= 2
                ):
                    asr_source_output = prior_output
                    result = prior_result
            except (OSError, json.JSONDecodeError):
                pass
        output = versioned_output_path(base_output, job)
        if asr_source_output is None:
            model_dir = Path(str(config.get("asr_model_dir") or ROOT / "models"))
            command = [
                str(PYTHON), str(TRANSCRIBE), "--input", str(audio), "--output", str(output),
                "--model-dir", str(model_dir), "--language", "zh", "--beam-size",
                str(int(config.get("asr_beam_size") or 5)), "--vad-silence-ms",
                str(int(config.get("asr_vad_min_silence_ms") or 500)), "--speech-pad-ms",
                str(int(config.get("asr_speech_pad_ms") or 200)),
            ]
            hotwords = str(config.get("asr_hotwords") or "").strip()
            if hotwords:
                command.extend(["--hotwords", hotwords])
            initial_prompt = str(config.get("asr_initial_prompt") or "").strip()
            if initial_prompt:
                command.extend(["--initial-prompt", initial_prompt])
            if not bool(config.get("asr_condition_on_previous_text", False)):
                command.append("--no-condition-on-previous-text")
            proc = run_process_with_lease(command, timeout=7200, renew_callback=renew_claim)
            if proc.returncode != 0:
                raise TranscriptJobError(
                    "ASR_PROCESS_FAILED", f"ASR child exited with code {proc.returncode}", retryable=True
                )
            asr_source_output = output
        if result is None:
            try:
                result = json.loads(asr_source_output.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise TranscriptJobError("ASR_OUTPUT_INVALID", "ASR output is missing or invalid JSON", retryable=True) from exc
        if not isinstance(result, dict) or result.get("status") != "READY":
            raise TranscriptJobError("ASR_NOT_READY", "ASR output did not report READY", retryable=True)
        reported_duration = _finite_number(result.get("duration"))
        if reported_duration is None or abs(reported_duration - audio_duration) > 2:
            raise TranscriptJobError(
                "ASR_DURATION_MISMATCH", "ASR result does not cover the extracted audio duration", retryable=True
            )
        # A legacy or previous-attempt ASR artifact is never overwritten.
        # Copy its validated JSON into this lease epoch's immutable result.
        write_json_atomic(output, result)
        checkpoint.update({
            "phase": "ASR_READY", "asr_output_path": str(output),
            "asr_output_sha256": digest(output), "asr_reported_duration_seconds": reported_duration,
        })
        with connect() as conn:
            save_checkpoint(conn, TRANSCRIPT, job, checkpoint)

        status = "COMPLETE"
        if full_session:
            quality = timestamp_coverage(result, audio_duration, coverage_threshold, coverage_target)
            status = "COMPLETE" if quality["is_qualified"] else "QUALITY_BLOCKED"
            result.update(
                coverage_scope="FULL_SESSION", source_media_sha256=media_hash,
                source_audio_sha256=audio_hash, source_audio_duration_seconds=audio_duration,
                covered_audio_seconds=quality["covered_duration_seconds"], timestamp_coverage=quality,
                quality_gate_status="FULL_SESSION_QUALIFIED" if quality["is_qualified"] else "QUALITY_BLOCKED",
                recording_completeness=session["completeness"],
            )
            if status == "QUALITY_BLOCKED":
                result["reason"] = "FULL_SESSION timestamp coverage did not pass the quality gate"
            write_json_atomic(output, result)
        model_name = str(config.get("asr_model") or "faster-whisper-small")
        metadata = {
            **result, "coverage_scope": "FULL_SESSION" if full_session else "SAMPLE",
            "source_segment_id": session["segment_id"], "sample_only": not full_session,
            "sample_seconds": None if full_session else 300, "model_name": model_name,
            "audio_format": audio.suffix.lower().lstrip("."),
            "audio_bitrate_kbps": int(config.get("asr_audio_bitrate_kbps") or 48),
        }
        transcript_scope = TRANSCRIPT_SCOPE_FULL if full_session else TRANSCRIPT_SCOPE_SAMPLE
        qualification_status = (
            QUALIFIED if full_session and status == "COMPLETE"
            else "QUALITY_BLOCKED" if full_session and status == "QUALITY_BLOCKED"
            else SAMPLE_NONQUALIFYING
        )
        checkpoint.update({"phase": status, "final_output_path": str(output), "final_output_sha256": digest(output)})
        with connect() as conn:
            finish(
                conn, TRANSCRIPT, job, status,
                {"language": result.get("language"), "source_path": str(audio), "output_path": str(output),
                 "low_confidence_count": int(result.get("low_confidence_count") or 0),
                 "scope": transcript_scope, "qualification_status": qualification_status,
                 "metadata_json": json.dumps(metadata, ensure_ascii=False),
                 "checkpoint_json": json.dumps(checkpoint, ensure_ascii=False, sort_keys=True)},
                commit_transaction=False,
            )
            if status == "COMPLETE" and full_session:
                try:
                    end_time = datetime.fromisoformat(str(session.get("ended_at") or utc_now()).replace("Z", "+00:00"))
                    audio_due = (end_time + timedelta(hours=int((config.get("retention") or {}).get("audio_hours") or 168))).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                except ValueError:
                    audio_due = utc_now()
                conn.execute(
                    "INSERT INTO retention_jobs(retention_job_id,object_type,object_id,policy_name,status,not_before,created_at,updated_at,payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,policy_name) DO UPDATE SET "
                    "not_before=excluded.not_before,payload_json=excluded.payload_json",
                    ("retention:audio:" + session["session_id"], "AUDIO", session["session_id"], "AUDIO_168H",
                     "PENDING", audio_due, utc_now(), utc_now(),
                     json.dumps({"session_id": session["session_id"], "path": str(audio), "transcript_id": job["transcript_id"]}, ensure_ascii=False)),
                )
            conn.commit()
        return status
    except LeaseLostError:
        return "LEASE_LOST"
    except TranscriptJobError as exc:
        with connect() as conn:
            return fail_or_retry(
                conn, TRANSCRIPT, job, error_type=exc.code, error_message=str(exc),
                retryable=exc.retryable, checkpoint=checkpoint,
            )
    except ProcessTimeoutError as exc:
        with connect() as conn:
            return fail_or_retry(
                conn, TRANSCRIPT, job, error_type="ASR_TIMEOUT", error_message=str(exc), retryable=True,
                checkpoint=checkpoint,
            )
    except Exception as exc:
        with connect() as conn:
            return fail_or_retry(
                conn, TRANSCRIPT, job, error_type="TRANSCRIPT_INTERNAL_ERROR",
                error_message=f"{exc.__class__.__name__}: {str(exc)}", retryable=True,
                checkpoint=checkpoint,
            )


def transcribe_pending(*, max_jobs: int = 1) -> int:
    discover_transcript_jobs()
    with connect() as conn:
        reconcile_exhausted(conn, TRANSCRIPT)
    completed = 0
    for _ in range(max(1, int(max_jobs))):
        with connect() as conn:
            job = claim_next(conn, TRANSCRIPT, transcript_worker_id(), lease_seconds=TRANSCRIPT_LEASE_SECONDS)
        if not job:
            break
        completed += int(process_transcript_claim(job) == "COMPLETE")
    return completed

def create_analysis_tasks() -> int:
    """Create formal analyses only from the current qualified full transcript.

    SAMPLE transcripts are useful ASR diagnostics, but they cannot represent a
    whole live session.  Keep them in ``transcripts`` and fail closed here so a
    300-second sample can never become a formal ``single_session`` analysis.
    """
    created = 0
    config = load_pipeline_config()
    quality_config = config.get("transcript_quality") or {}
    coverage_threshold = quality_config.get(
        "full_session_min_timestamp_coverage_rate",
        DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD,
    )
    with connect() as conn:
        rows = conn.execute(
            "SELECT t.transcript_id,t.session_id,t.source_digest,t.status,t.output_path,t.scope,t.qualification_status,t.metadata_json,"
            "s.status AS session_status,s.completeness,s.metadata_json AS session_metadata "
            "FROM transcripts t JOIN live_sessions s ON s.session_id=t.session_id "
            "WHERE t.status='COMPLETE' AND t.output_path LIKE '%.json'"
        ).fetchall()
        canonical_by_session: dict[str, dict[str, str]] = {}
        for segment in conn.execute(
            "SELECT segment_id,session_id,checksum,path FROM recording_segments "
            "WHERE status='COMPLETE' AND lifecycle_status='CANONICAL_ACTIVE'"
        ):
            checksum = str(segment["checksum"] or "")
            path = Path(str(segment["path"] or ""))
            if not checksum and path.is_file():
                checksum = digest(path)
            if checksum:
                expected_source_digest = hashlib.sha256(("FULL_SESSION:" + checksum).encode()).hexdigest()
                canonical_by_session.setdefault(str(segment["session_id"]), {})[str(segment["segment_id"])] = expected_source_digest
        for row in rows:
            metadata = _metadata(row["metadata_json"])
            quality = metadata.get("timestamp_coverage") or {}
            session_metadata = _metadata(row["session_metadata"])
            current_segments = canonical_by_session.get(str(row["session_id"]), {})
            source_segment_id = str(metadata.get("source_segment_id") or "")
            eligible = (
                _qualified_full_session(row, coverage_threshold)
                and row["scope"] == TRANSCRIPT_SCOPE_FULL
                and row["qualification_status"] == QUALIFIED
                and metadata.get("sample_only") is False
                and metadata.get("quality_gate_status") == "FULL_SESSION_QUALIFIED"
                and source_segment_id in current_segments
                and str(row["source_digest"] or "") == current_segments.get(source_segment_id)
                and row["session_status"] == "MEDIA_COMPLETE"
                and row["completeness"] == "COMPLETE"
                and (session_metadata.get("media_coverage") or {}).get("continuous_capture") is True
                and row["output_path"]
                and Path(str(row["output_path"])).is_file()
            )
            if not eligible:
                continue
            transcript_path = Path(str(row["output_path"]))
            transcript_content_digest = digest(transcript_path)
            existing = conn.execute(
                "SELECT analysis_id FROM analyses WHERE transcript_id=? AND analysis_type='single_session' "
                "AND transcript_content_digest=? AND analysis_spec_version=? AND model_version=? AND prompt_version=? LIMIT 1",
                (row["transcript_id"], transcript_content_digest, ANALYSIS_SPEC_VERSION,
                 ANALYSIS_MODEL_VERSION, ANALYSIS_PROMPT_VERSION),
            ).fetchone()
            if existing:
                continue
            aid = "analysis_" + hashlib.sha256(
                f"{row['transcript_id']}:{transcript_content_digest}:{ANALYSIS_SPEC_VERSION}:"
                f"{ANALYSIS_MODEL_VERSION}:{ANALYSIS_PROMPT_VERSION}".encode()
            ).hexdigest()[:24]
            upstream_version = transcript_content_digest
            analysis_metadata = {
                "requires_model_analysis": True,
                "qualification_state": "FULL_SESSION_QUALIFIED",
                "formal_analysis_eligible": True,
                "source_transcript_id": row["transcript_id"],
                "source_transcript_digest": row["source_digest"],
                "source_coverage_scope": "FULL_SESSION",
                "source_timestamp_coverage_rate": quality.get("coverage_rate"),
                "source_segment_id": metadata.get("source_segment_id"),
                "transcript_content_digest": transcript_content_digest,
                "analysis_spec_version": ANALYSIS_SPEC_VERSION,
                "model_version": ANALYSIS_MODEL_VERSION,
                "prompt_version": ANALYSIS_PROMPT_VERSION,
            }
            due_at = utc_now()
            inserted = conn.execute(
                "INSERT OR IGNORE INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,output_path,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json,updated_at,next_attempt_at,checkpoint_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (aid, row["session_id"], row["transcript_id"], "single_session", transcript_content_digest,
                 "PENDING", None, "CURRENT", ANALYSIS_SCOPE_FORMAL, QUALIFIED,
                 transcript_content_digest, ANALYSIS_SPEC_VERSION, ANALYSIS_MODEL_VERSION,
                 ANALYSIS_PROMPT_VERSION, json.dumps(analysis_metadata, ensure_ascii=False, sort_keys=True),
                 due_at, due_at, "{}"),
            )
            conn.execute(
                "INSERT OR IGNORE INTO lineage_edges(edge_id,downstream_type,downstream_id,upstream_type,upstream_id,upstream_version,state,created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                ("lineage_" + hashlib.sha256(f"{aid}:{row['transcript_id']}:{upstream_version}".encode()).hexdigest()[:24],
                 "analysis", aid, "transcript", row["transcript_id"], upstream_version, "CURRENT", utc_now()),
            )
            if inserted.rowcount == 1:
                created += 1
        conn.commit()
    return created


def pipeline_health_snapshot() -> tuple[str, dict]:
    """Summarize durable transcript quality and recoverability for heartbeat."""
    config = load_pipeline_config()
    quality_config = config.get("transcript_quality") or {}
    coverage_threshold = quality_config.get("full_session_min_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD)
    coverage_target = quality_config.get("full_session_target_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_TARGET)
    with connect() as conn:
        transcript_rows = [dict(row) for row in conn.execute(
            "SELECT transcript_id,session_id,source_digest,status,source_path,output_path,metadata_json FROM transcripts"
        )]
        canonical_rows = [dict(row) for row in conn.execute(
            "SELECT s.session_id,s.completeness,s.metadata_json AS session_metadata,r.path,r.checksum "
            "FROM live_sessions s JOIN recording_segments r ON r.session_id=s.session_id "
            "WHERE s.status='MEDIA_COMPLETE' AND r.status='COMPLETE' AND r.lifecycle_status='CANONICAL_ACTIVE'"
        )]

    canonical_available: dict[str, bool] = {}
    expected_digests: dict[str, set[str]] = {}
    eligible_sessions: set[str] = set()
    for row in canonical_rows:
        media = Path(row["path"])
        available = media.is_file()
        canonical_available[row["session_id"]] = canonical_available.get(row["session_id"], False) or available
        if media.name == "整场直播.ts" and available:
            try:
                media_hash = str(row["checksum"] or digest(media))
            except OSError:
                media_hash = ""
            if media_hash:
                expected = hashlib.sha256(("FULL_SESSION:" + media_hash).encode()).hexdigest()
                expected_digests.setdefault(row["session_id"], set()).add(expected)
        media_coverage = _metadata(row["session_metadata"]).get("media_coverage") or {}
        if (
            media.name == "整场直播.ts"
            and row["completeness"] == "COMPLETE"
            and media_coverage.get("continuous_capture") is True
            and available
        ):
            eligible_sessions.add(row["session_id"])

    qualified_sessions: set[str] = set()
    target_sessions: set[str] = set()
    quality_blocked_sessions: set[str] = set()
    missing_source = 0
    missing_output = 0
    unrecoverable = 0
    for row in transcript_rows:
        if row["status"] == "CANCELLED_SUPERSEDED_SOURCE":
            # Lifecycle cancellation is a resolved tombstone, not pipeline debt.
            continue
        is_current = row["source_digest"] in expected_digests.get(row["session_id"], set())
        if row["status"] in {"COMPLETE", "QUALITY_BLOCKED"} and not is_current:
            # Immutable history remains queryable, but only the transcript for
            # the current canonical media may determine current pipeline health.
            continue
        source_exists = bool(row["source_path"] and Path(row["source_path"]).is_file())
        output_exists = bool(row["output_path"] and Path(row["output_path"]).is_file())
        if not source_exists:
            missing_source += 1
        if not output_exists:
            missing_output += 1
        if not source_exists and not output_exists and not canonical_available.get(row["session_id"], False):
            unrecoverable += 1
        if is_current and row["status"] == "QUALITY_BLOCKED":
            quality_blocked_sessions.add(row["session_id"])
        if is_current and output_exists and _qualified_full_session(row,coverage_threshold):
            qualified_sessions.add(row["session_id"])
            quality = _metadata(row["metadata_json"]).get("timestamp_coverage") or {}
            target = _finite_number(coverage_target)
            target = target if target is not None and 0 < target <= 1 else DEFAULT_FULL_SESSION_COVERAGE_TARGET
            rate = _finite_number(quality.get("coverage_rate"))
            if quality.get("meets_target") is True and rate is not None and rate >= target:
                target_sessions.add(row["session_id"])

    pending_sessions = eligible_sessions - qualified_sessions
    reasons = []
    if quality_blocked_sessions:
        reasons.append("FULL_SESSION_QUALITY_BLOCKED")
    if unrecoverable:
        reasons.append("UNRECOVERABLE_TRANSCRIPT_BACKLOG")
    if pending_sessions:
        reasons.append("ELIGIBLE_CANONICAL_PENDING")
    status = "DEGRADED" if reasons else "READY"
    details = {
        "full_session_qualified": len(qualified_sessions),
        "full_session_meets_target": len(target_sessions),
        "quality_blocked": len(quality_blocked_sessions),
        "eligible_canonical_total": len(eligible_sessions),
        "eligible_canonical_pending": len(pending_sessions),
        "stale_backlog": {
            "missing_source": missing_source,
            "missing_output": missing_output,
            "unrecoverable": unrecoverable,
        },
        "health_reasons": reasons,
    }
    return status, details


def once() -> dict:
    segments = register_segments()
    transcripts = transcribe_pending()
    analysis_tasks = create_analysis_tasks()
    heartbeat_status, health = pipeline_health_snapshot()
    result = {"segments": segments, "transcripts": transcripts, "analysis_tasks": analysis_tasks, **health, "checked_at": utc_now()}
    upsert_heartbeat("pipeline-v3", heartbeat_status, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("once", "daemon"))
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    init_db()
    if args.command == "once":
        print(json.dumps(once(), ensure_ascii=False, indent=2))
        return 0
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    while RUNNING:
        once(); time.sleep(max(10, args.interval))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
