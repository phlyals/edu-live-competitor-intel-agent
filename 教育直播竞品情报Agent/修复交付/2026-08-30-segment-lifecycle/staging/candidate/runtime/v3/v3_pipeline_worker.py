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
import os
import signal
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from v3_runtime import connect, init_db, upsert_heartbeat, utc_now


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "bin" / "python"
TRANSCRIBE = ROOT / "bin" / "transcribe.py"
FFMPEG = Path(shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg")
RUNNING = True


def stop(*_args):
    global RUNNING
    RUNNING = False


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


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
    with connect() as conn:
        sessions = [dict(r) for r in conn.execute("SELECT DISTINCT s.session_id,s.status,s.ended_at,s.completeness,s.metadata_json AS session_metadata,r.path,r.segment_id,r.checksum FROM live_sessions s JOIN recording_segments r ON r.session_id=s.session_id WHERE r.status='COMPLETE' AND r.lifecycle_status='CANONICAL_ACTIVE' AND s.status='MEDIA_COMPLETE' ORDER BY s.ended_at DESC,r.path")]
    for session in sessions:
        media = Path(session["path"])
        full_session = media.name == "整场直播.ts"
        coverage=json.loads(session['session_metadata'] or '{}').get('media_coverage') or {}
        if full_session and (session['completeness']!='COMPLETE' or coverage.get('continuous_capture') is not True):continue
        media_hash=str(session['checksum'] or digest(media))
        source_digest=hashlib.sha256(('FULL_SESSION:'+media_hash).encode()).hexdigest() if full_session else media_hash
        with connect() as conn:
            previous=conn.execute("SELECT * FROM transcripts WHERE session_id=? AND source_digest=? AND status='COMPLETE'",(session['session_id'],source_digest)).fetchone()
        if previous:
            metadata=json.loads(previous['metadata_json'] or '{}')
            if (not full_session or metadata.get('coverage_scope')=='FULL_SESSION') and previous['output_path'] and Path(previous['output_path']).is_file():continue
        config = json.loads((ROOT / "v3" / "v3_config.json").read_text(encoding="utf-8")) if (ROOT / "v3" / "v3_config.json").is_file() else {}
        audio_format = str(config.get("asr_audio_format") or "opus").lower().lstrip(".")
        audio_ext = "." + audio_format
        preferred_audio = media.with_name(media.stem+'.'+media_hash[:12]+audio_ext) if full_session else media.with_suffix(".sample300s" + audio_ext)
        legacy_audio = media.with_suffix(".wav") if full_session else media.with_suffix(".sample300s.wav")
        audio = preferred_audio if full_session or preferred_audio.is_file() or not legacy_audio.is_file() else legacy_audio
        if not extract_audio(media, audio, max_seconds=0 if full_session else 300, bitrate_kbps=int(config.get("asr_audio_bitrate_kbps") or 48)):
            with connect() as conn:
                conn.execute("INSERT OR IGNORE INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?)", ("transcript_" + hashlib.sha256(f"{session['session_id']}:{source_digest}".encode()).hexdigest()[:24], session["session_id"], source_digest, "faster-whisper", str(config.get('asr_model') or 'faster-whisper-small'), "WAITING_TOOL", str(media), utc_now(), json.dumps({"reason": "audio extraction/duration validation failed", "segment_id": session["segment_id"]}, ensure_ascii=False)))
                conn.commit()
            continue
        out = audio.with_suffix(".transcript.json")
        result = None
        if out.is_file():
            try:
                existing = json.loads(out.read_text(encoding="utf-8"))
                if existing.get("status") == "READY" and (not full_session or existing.get('source_media_sha256')==media_hash):
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
        status = "COMPLETE" if result.get("status") == "READY" and out.is_file() else "PAUSED"
        if status=='COMPLETE':
            result=json.loads(out.read_text())
            duration=media_duration(audio)
            if duration is None or result.get('duration') is None or abs(float(result['duration'])-duration)>2:
                status='PAUSED';result['reason']='ASR input duration does not cover the complete audio'
            elif full_session:
                result.update(coverage_scope='FULL_SESSION',source_media_sha256=media_hash,source_audio_sha256=digest(audio),covered_audio_seconds=duration,recording_completeness=session['completeness'])
                temporary=out.with_suffix('.verified.tmp')
                temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2));temporary.replace(out)
        with connect() as conn:
            transcript_id = "transcript_" + hashlib.sha256(f"{session['session_id']}:{source_digest}".encode()).hexdigest()[:24]
            config = json.loads((ROOT / "v3" / "v3_config.json").read_text(encoding="utf-8")) if (ROOT / "v3" / "v3_config.json").is_file() else {}
            model_name = str(config.get("asr_model") or "faster-whisper-small")
            metadata = {**result, "coverage_scope": 'FULL_SESSION' if full_session else 'SAMPLE', "source_segment_id": session["segment_id"], "sample_only": not full_session, "sample_seconds": None if full_session else 300, "model_name": model_name, "audio_format": audio.suffix.lower().lstrip("."), "audio_bitrate_kbps": int(config.get("asr_audio_bitrate_kbps") or 48)}
            conn.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,language,source_path,output_path,low_confidence_count,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,source_digest) DO UPDATE SET status=excluded.status,language=excluded.language,output_path=excluded.output_path,low_confidence_count=excluded.low_confidence_count,metadata_json=excluded.metadata_json", (transcript_id, session["session_id"], source_digest, "faster-whisper", model_name, status, result.get("language"), str(audio), str(out) if out.is_file() else None, int(result.get("low_confidence_count") or 0), utc_now(), json.dumps(metadata, ensure_ascii=False)))
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
    created = 0
    with connect() as conn:
        rows = conn.execute("SELECT t.transcript_id,t.session_id,t.output_path FROM transcripts t WHERE t.status='COMPLETE' AND t.output_path LIKE '%.json' AND NOT EXISTS (SELECT 1 FROM analyses a WHERE a.session_id=t.session_id AND a.status NOT IN ('SKIPPED_HISTORICAL'))").fetchall()
        for row in rows:
            aid = "analysis_" + hashlib.sha256(row["transcript_id"].encode()).hexdigest()[:24]
            conn.execute("INSERT OR IGNORE INTO analyses(analysis_id,session_id,analysis_type,source_digest,status,output_path,lineage_state,metadata_json) VALUES(?,?,?,?,?,?,?,?)", (aid, row["session_id"], "single_session", hashlib.sha256(str(row["output_path"]).encode()).hexdigest(), "PENDING", None, "CURRENT", json.dumps({"requires_model_analysis": True}, ensure_ascii=False)))
            conn.execute("INSERT OR IGNORE INTO lineage_edges(edge_id,downstream_type,downstream_id,upstream_type,upstream_id,upstream_version,created_at) VALUES(?,?,?,?,?,?,?)", ("lineage_" + hashlib.sha256(aid.encode()).hexdigest()[:24], "analysis", aid, "transcript", row["transcript_id"], hashlib.sha256(str(row["output_path"]).encode()).hexdigest(), utc_now()))
            created += 1
        conn.commit()
    return created


def once() -> dict:
    segments = register_segments()
    transcripts = transcribe_pending()
    analysis_tasks = create_analysis_tasks()
    result = {"segments": segments, "transcripts": transcripts, "analysis_tasks": analysis_tasks, "checked_at": utc_now()}
    upsert_heartbeat("pipeline-v3", "READY", result)
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
