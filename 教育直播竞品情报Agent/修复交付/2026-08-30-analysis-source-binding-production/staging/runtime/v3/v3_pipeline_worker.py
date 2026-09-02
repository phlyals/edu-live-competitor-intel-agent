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
import signal
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


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "bin" / "python"
TRANSCRIBE = ROOT / "bin" / "transcribe.py"
FFMPEG = Path(shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg")
RUNNING = True
DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD = 0.90
DEFAULT_FULL_SESSION_COVERAGE_TARGET = 0.95


def stop(*_args):
    global RUNNING
    RUNNING = False


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


def extract_audio(media: Path, audio: Path, *, max_seconds: int = 0, bitrate_kbps: int = 48) -> bool:
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
    temporary=audio.with_name(audio.stem+'.extracting'+audio.suffix)
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
    proc = subprocess.run(command, capture_output=True, text=True, timeout=1800, check=False)
    actual=media_duration(temporary) if proc.returncode==0 else None
    if actual is None or abs(actual-expected)>2:return False
    temporary.replace(audio)
    return True


def transcribe_pending() -> int:
    created = 0
    config = load_pipeline_config()
    quality_config = config.get("transcript_quality") or {}
    coverage_threshold = quality_config.get("full_session_min_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_THRESHOLD)
    coverage_target = quality_config.get("full_session_target_timestamp_coverage_rate", DEFAULT_FULL_SESSION_COVERAGE_TARGET)
    with connect() as conn:
        sessions = [dict(r) for r in conn.execute("SELECT DISTINCT s.session_id,s.status,s.ended_at,s.completeness,s.metadata_json AS session_metadata,r.path,r.segment_id,r.checksum FROM live_sessions s JOIN recording_segments r ON r.session_id=s.session_id WHERE r.status='COMPLETE' AND r.lifecycle_status='CANONICAL_ACTIVE' AND s.status='MEDIA_COMPLETE' ORDER BY s.ended_at DESC,r.path")]
    for session in sessions:
        media = Path(session["path"])
        full_session = media.name == "整场直播.ts"
        coverage=_metadata(session['session_metadata']).get('media_coverage') or {}
        if full_session and (session['completeness']!='COMPLETE' or coverage.get('continuous_capture') is not True):continue
        if not media.is_file():continue
        media_hash=str(session['checksum'] or digest(media))
        source_digest=hashlib.sha256(('FULL_SESSION:'+media_hash).encode()).hexdigest() if full_session else media_hash
        with connect() as conn:
            previous=conn.execute("SELECT * FROM transcripts WHERE session_id=? AND source_digest=?",(session['session_id'],source_digest)).fetchone()
        if previous:
            if not full_session and previous['status']=='COMPLETE' and previous['output_path'] and Path(previous['output_path']).is_file():continue
            if full_session and _current_quality_gate(previous,coverage_threshold,coverage_target) and previous['output_path'] and Path(previous['output_path']).is_file():continue
        audio_format = str(config.get("asr_audio_format") or "opus").lower().lstrip(".")
        audio_ext = "." + audio_format
        preferred_audio = media.with_name(media.stem+'.'+media_hash[:12]+audio_ext) if full_session else media.with_suffix(".sample300s" + audio_ext)
        legacy_audio = media.with_suffix(".wav") if full_session else media.with_suffix(".sample300s.wav")
        previous_audio = Path(previous['source_path']) if previous and previous['source_path'] else None
        previous_output = Path(previous['output_path']) if previous and previous['output_path'] else None
        if full_session and previous_audio and previous_audio.is_file() and previous_output and previous_output.is_file():
            # Revalidate the immutable artifact already attached to this exact
            # source_digest; do not spend another ASR run merely because old DB
            # metadata predates the timestamp coverage gate.
            audio = previous_audio
        else:
            audio = preferred_audio if full_session or preferred_audio.is_file() or not legacy_audio.is_file() else legacy_audio
        if not extract_audio(media, audio, max_seconds=0 if full_session else 300, bitrate_kbps=int(config.get("asr_audio_bitrate_kbps") or 48)):
            with connect() as conn:
                waiting_scope = TRANSCRIPT_SCOPE_FULL if full_session else TRANSCRIPT_SCOPE_SAMPLE
                waiting_qualification = "PENDING_QUALIFICATION" if full_session else SAMPLE_NONQUALIFYING
                conn.execute("INSERT OR IGNORE INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,scope,qualification_status,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)", ("transcript_" + hashlib.sha256(f"{session['session_id']}:{source_digest}".encode()).hexdigest()[:24], session["session_id"], source_digest, "faster-whisper", str(config.get('asr_model') or 'faster-whisper-small'), "WAITING_TOOL", str(media), utc_now(), waiting_scope, waiting_qualification, json.dumps({"reason": "audio extraction/duration validation failed", "segment_id": session["segment_id"], "coverage_scope": waiting_scope, "sample_only": not full_session}, ensure_ascii=False)))
                conn.commit()
            continue
        out = previous_output if full_session and previous_output and previous_output.is_file() else audio.with_suffix(".transcript.json")
        result = None
        if out.is_file():
            try:
                existing = json.loads(out.read_text(encoding="utf-8"))
                previous_artifact = bool(previous and previous['output_path'] and Path(previous['output_path']).resolve()==out.resolve())
                source_matches = existing.get('source_media_sha256')==media_hash or previous_artifact
                if existing.get("status") == "READY" and (not full_session or source_matches):
                    result = existing
            except (OSError, json.JSONDecodeError):
                result = None
        if result is None:
            model_dir = Path(str(config.get("asr_model_dir") or ROOT / "models"))
            command = [str(PYTHON), str(TRANSCRIBE), "--input", str(audio), "--output", str(out), "--model-dir", str(model_dir), "--language", "zh", "--beam-size", str(int(config.get("asr_beam_size") or 5)), "--vad-silence-ms", str(int(config.get("asr_vad_min_silence_ms") or 500)), "--speech-pad-ms", str(int(config.get("asr_speech_pad_ms") or 200))]
            hotwords = str(config.get("asr_hotwords") or "").strip()
            if hotwords:
                command.extend(["--hotwords", hotwords])
            initial_prompt = str(config.get("asr_initial_prompt") or "").strip()
            if initial_prompt:
                command.extend(["--initial-prompt", initial_prompt])
            if not bool(config.get("asr_condition_on_previous_text", False)):
                command.append("--no-condition-on-previous-text")
            proc = subprocess.run(command, capture_output=True, text=True, timeout=7200, check=False)
            try:
                result = json.loads(proc.stdout)
            except json.JSONDecodeError:
                result = {"status": "PAUSED", "reason": (proc.stderr or proc.stdout)[-1000:]}
        if not isinstance(result, dict):
            result = {"status": "PAUSED", "reason": "ASR output root must be a JSON object"}
        status = "COMPLETE" if isinstance(result, dict) and result.get("status") == "READY" and out.is_file() else "PAUSED"
        if status=='COMPLETE':
            try:
                result=json.loads(out.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                result={"status":"PAUSED","reason":"ASR output is not valid JSON"};status='PAUSED'
            duration=media_duration(audio)
            reported_duration=_finite_number(result.get('duration'))
            if status=='COMPLETE' and (duration is None or reported_duration is None or abs(reported_duration-duration)>2):
                status='PAUSED';result['reason']='ASR input duration does not cover the complete audio'
            elif status=='COMPLETE' and full_session:
                quality=timestamp_coverage(result,duration,coverage_threshold,coverage_target)
                status='COMPLETE' if quality['is_qualified'] else 'QUALITY_BLOCKED'
                result.update(
                    coverage_scope='FULL_SESSION',
                    source_media_sha256=media_hash,
                    source_audio_sha256=digest(audio),
                    source_audio_duration_seconds=duration,
                    covered_audio_seconds=quality['covered_duration_seconds'],
                    timestamp_coverage=quality,
                    quality_gate_status='FULL_SESSION_QUALIFIED' if quality['is_qualified'] else 'QUALITY_BLOCKED',
                    recording_completeness=session['completeness'],
                )
                if status=='QUALITY_BLOCKED':
                    result['reason']='FULL_SESSION timestamp coverage did not pass the quality gate'
                temporary=out.with_suffix('.verified.tmp')
                temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False));temporary.replace(out)
        with connect() as conn:
            transcript_id = "transcript_" + hashlib.sha256(f"{session['session_id']}:{source_digest}".encode()).hexdigest()[:24]
            model_name = str(config.get("asr_model") or "faster-whisper-small")
            metadata = {**result, "coverage_scope": 'FULL_SESSION' if full_session else 'SAMPLE', "source_segment_id": session["segment_id"], "sample_only": not full_session, "sample_seconds": None if full_session else 300, "model_name": model_name, "audio_format": audio.suffix.lower().lstrip("."), "audio_bitrate_kbps": int(config.get("asr_audio_bitrate_kbps") or 48)}
            transcript_scope = TRANSCRIPT_SCOPE_FULL if full_session else TRANSCRIPT_SCOPE_SAMPLE
            qualification_status = (
                QUALIFIED if full_session and status == "COMPLETE"
                else "QUALITY_BLOCKED" if full_session and status == "QUALITY_BLOCKED"
                else SAMPLE_NONQUALIFYING
            )
            conn.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,language,source_path,output_path,low_confidence_count,created_at,scope,qualification_status,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,source_digest) DO UPDATE SET status=excluded.status,language=excluded.language,output_path=excluded.output_path,low_confidence_count=excluded.low_confidence_count,scope=excluded.scope,qualification_status=excluded.qualification_status,metadata_json=excluded.metadata_json", (transcript_id, session["session_id"], source_digest, "faster-whisper", model_name, status, result.get("language"), str(audio), str(out) if out.is_file() else None, int(result.get("low_confidence_count") or 0), utc_now(), transcript_scope, qualification_status, json.dumps(metadata, ensure_ascii=False)))
            if status == "COMPLETE" and full_session:
                try:
                    end_time = datetime.fromisoformat(str(session.get("ended_at") or utc_now()).replace("Z", "+00:00"))
                    audio_hours = int((config.get("retention") or {}).get("audio_hours") or 168)
                    audio_due = (end_time + timedelta(hours=audio_hours)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                except ValueError:
                    audio_due = utc_now()
                conn.execute("INSERT INTO retention_jobs(retention_job_id,object_type,object_id,policy_name,status,not_before,created_at,updated_at,payload_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,policy_name) DO UPDATE SET not_before=excluded.not_before,payload_json=excluded.payload_json", ("retention:audio:" + session["session_id"], "AUDIO", session["session_id"], "AUDIO_168H", "PENDING", audio_due, utc_now(), utc_now(), json.dumps({"session_id": session["session_id"], "path": str(audio), "transcript_id": transcript_id}, ensure_ascii=False)))
            conn.commit()
        if status == "COMPLETE":
            created += 1
    return created


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
            inserted = conn.execute(
                "INSERT OR IGNORE INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,output_path,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (aid, row["session_id"], row["transcript_id"], "single_session", transcript_content_digest,
                 "PENDING", None, "CURRENT", ANALYSIS_SCOPE_FORMAL, QUALIFIED,
                 transcript_content_digest, ANALYSIS_SPEC_VERSION, ANALYSIS_MODEL_VERSION,
                 ANALYSIS_PROMPT_VERSION, json.dumps(analysis_metadata, ensure_ascii=False, sort_keys=True)),
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
