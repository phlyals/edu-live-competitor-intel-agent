#!/usr/bin/env python3
"""Runtime V3 worker and full-fleet monitor supervisor.

The worker is safe by construction: activation is atomic and refuses to start
until every known competitor has a verified canonical live target.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import subprocess
import shutil
import threading
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from v3_runtime import (
    DB_PATH,
    PROFILE_ID,
    RUNTIME_ROOT,
    activation_readiness,
    connect,
    identity_assertion,
    init_db,
    load_config,
    new_id,
    record_event,
    status_snapshot,
    utc_now,
    claim_outbox,
    claim_task,
    checkpoint,
    complete_outbox,
    enqueue_outbox,
    import_result,
    release_task,
    update_task,
)


PYTHON = Path(__file__).resolve().parents[1] / ".venv" / "bin" / "python"
RECORDER = Path(__file__).resolve().parents[1] / "bin" / "recorder.py"
SCANNER = Path(__file__).resolve().parents[1] / "bin" / "tabbit_scanner.py"
STREAMGET = Path(__file__).resolve().parents[1] / "bin" / "streamget_probe.py"
V3_CONFIG = Path(__file__).resolve().parent / "v3_config.json"
SERVICE = "runtime-v3"
STOP_EVENT = threading.Event()
RECORDER_PROCESSES: dict[int, subprocess.Popen] = {}
RECORDING_LOG_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/exports/runtime-v3-recordings")
OFFLINE_WINDOW_SECONDS = 300
OFFLINE_MIN_OBSERVATIONS = 3
OFFLINE_MAX_OBSERVATION_GAP = 150


def emit(payload: dict, code: int = 0) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return code


def active() -> bool:
    config = load_config()
    return ((config.get("atomic_activation") or {}).get("activation_state") == "ACTIVE")


def write_activation(state: str) -> None:
    config = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
    config["atomic_activation"]["activation_state"] = state
    temporary = V3_CONFIG.with_name(f".{V3_CONFIG.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(V3_CONFIG)


def heartbeat(status: str, details: dict) -> None:
    now = utc_now()
    with connect() as conn:
        conn.execute("INSERT INTO heartbeats(service_name,pid,status,started_at,last_heartbeat_at,last_success_at,details_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(service_name) DO UPDATE SET pid=excluded.pid,status=excluded.status,started_at=excluded.started_at,last_heartbeat_at=excluded.last_heartbeat_at,last_success_at=CASE WHEN excluded.status='READY' THEN excluded.last_heartbeat_at ELSE heartbeats.last_success_at END,details_json=excluded.details_json", (SERVICE, os.getpid(), status, now, now, now if status == "READY" else None, json.dumps(details, ensure_ascii=False, sort_keys=True)))
        conn.execute("INSERT INTO worker_nodes(node_id,node_role,hostname,status,last_heartbeat_at,metadata_json) VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_at=excluded.last_heartbeat_at,metadata_json=excluded.metadata_json", (os.environ.get("V3_NODE_ID") or socket.gethostname(), os.environ.get("V3_NODE_ROLE") or "primary", socket.gethostname(), status, now, json.dumps({"service_name": SERVICE, "details": details}, ensure_ascii=False, sort_keys=True)))


def activate() -> int:
    identity_assertion(verify_cli=True)
    readiness = activation_readiness()
    if not readiness["ready"]:
        write_activation("PENDING_PRECHECK")
        return emit({"ok": False, "status": "WAITING_CONFIGURATION", "activation": readiness}, 1)
    write_activation("ACTIVE")
    with connect() as conn:
        record_event(conn, "ATOMIC_FULL_FLEET_ACTIVATED", object_type="runtime", object_id=PROFILE_ID, payload=readiness)
        conn.commit()
    return emit({"ok": True, "status": "ACTIVE", "activation": readiness, "full_fleet": True})


def create_session_if_live(conn, target: dict, result: dict) -> dict | None:
    if result.get("live_status") != "LIVE":
        return None
    competitor_id = target["competitor_id"]
    real_room_id = str(result.get("room_id") or "").strip()
    room_id = real_room_id or f"target:{competitor_id}:day:{datetime.now(timezone.utc).date().isoformat()}"
    platform_session_id = f"douyin:{room_id}"
    now = utc_now()
    raw_active_rows = conn.execute("SELECT * FROM live_sessions WHERE monitor_target_id=? AND status IN ('RECORDING','DETECTED','WAITING_STREAM','WAITING_CAPACITY') ORDER BY started_at,session_id", (target["monitor_target_id"],)).fetchall()
    active_rows = []
    for candidate in raw_active_rows:
        old_job = conn.execute("SELECT * FROM recording_jobs WHERE session_id=?", (candidate["session_id"],)).fetchone()
        completed_root = Path(str(old_job["completed_dir"])) if old_job else None
        has_completed_media = bool(completed_root and completed_root.is_dir() and any(completed_root.glob("*.ts")))
        if has_completed_media and old_job and (old_job["status"] in {"WAITING_STREAM", "COMPLETE", "DUPLICATE_SUPERSEDED"} or not pid_alive(old_job["pid"])):
            conn.execute("UPDATE live_sessions SET status='MEDIA_COMPLETE',ended_at=COALESCE(ended_at,?),completeness='COMPLETE' WHERE session_id=?", (now, candidate["session_id"]))
            conn.execute("UPDATE recording_jobs SET status='COMPLETE',pid=NULL,updated_at=? WHERE session_id=?", (now, candidate["session_id"]))
            conn.execute("UPDATE recording_leases SET status='RELEASED',lease_until=? WHERE session_id=? AND status='ACTIVE'", (now, candidate["session_id"]))
            continue
        active_rows.append(candidate)
    if active_rows:
        canonical = active_rows[0]
        for duplicate in active_rows[1:]:
            duplicate_platform = f"duplicate:{duplicate['platform_session_id']}:{hashlib.sha256(duplicate['session_id'].encode('utf-8')).hexdigest()[:12]}"
            try:
                metadata = json.loads(duplicate["metadata_json"] or "{}")
            except json.JSONDecodeError:
                metadata = {}
            metadata.update({"superseded_by": canonical["session_id"], "duplicate_detected_at": now})
            conn.execute("UPDATE live_sessions SET platform_session_id=?,status='DUPLICATE_SUPERSEDED',ended_at=?,metadata_json=? WHERE session_id=?", (duplicate_platform, now, json.dumps(metadata, ensure_ascii=False, sort_keys=True), duplicate["session_id"]))
        if real_room_id and canonical["platform_session_id"] != platform_session_id:
            conn.execute("UPDATE live_sessions SET platform_session_id=?,metadata_json=? WHERE session_id=?", (platform_session_id, json.dumps(result, ensure_ascii=False, sort_keys=True), canonical["session_id"]))
        updated = conn.execute("SELECT * FROM live_sessions WHERE session_id=?", (canonical["session_id"],)).fetchone()
        return {**dict(updated), "_created": False}
    existing = conn.execute("SELECT * FROM live_sessions WHERE monitor_target_id=? AND platform_session_id=?", (target["monitor_target_id"], platform_session_id)).fetchone()
    if existing and existing["status"] not in {"ENDED", "FINALIZED", "DUPLICATE_SUPERSEDED"}:
        return {**dict(existing), "_created": False}
    session_id = "session_" + hashlib.sha256(f"{target['monitor_target_id']}:{platform_session_id}:{now}".encode("utf-8")).hexdigest()[:32]
    conn.execute("INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,source_url,metadata_json) VALUES(?,?,?,'WAITING_STREAM',?,?,?)", (session_id, target["monitor_target_id"], platform_session_id, now, target["live_url"], json.dumps(result, ensure_ascii=False, sort_keys=True)))
    return {**dict(conn.execute("SELECT * FROM live_sessions WHERE session_id=?", (session_id,)).fetchone()), "_created": True}


def repair_duplicate_sessions() -> dict:
    repaired = 0
    now = utc_now()
    with connect() as conn:
        targets = conn.execute("SELECT DISTINCT monitor_target_id FROM live_sessions WHERE status IN ('RECORDING','DETECTED')").fetchall()
        for target_row in targets:
            rows = conn.execute("SELECT * FROM live_sessions WHERE monitor_target_id=? AND status IN ('RECORDING','DETECTED') ORDER BY started_at,session_id", (target_row[0],)).fetchall()
            if len(rows) <= 1:
                continue
            canonical = rows[0]
            real_platform = next((row["platform_session_id"] for row in rows if not row["platform_session_id"].startswith("douyin:target:")), canonical["platform_session_id"])
            for duplicate in rows[1:]:
                try:
                    metadata = json.loads(duplicate["metadata_json"] or "{}")
                except json.JSONDecodeError:
                    metadata = {}
                _, recording_key, partial_dir, _ = recording_keys(dict(duplicate), {"competitor_id": conn.execute("SELECT competitor_id FROM monitor_targets WHERE monitor_target_id=?", (duplicate["monitor_target_id"],)).fetchone()[0]})
                metadata.update({"superseded_by": canonical["session_id"], "duplicate_detected_at": now, "duplicate_media_path": str(partial_dir), "media_retained_for_audit": True})
                duplicate_platform = f"duplicate:{duplicate['platform_session_id']}:{hashlib.sha256(duplicate['session_id'].encode()).hexdigest()[:12]}"
                conn.execute("UPDATE live_sessions SET platform_session_id=?,status='DUPLICATE_SUPERSEDED',ended_at=?,metadata_json=? WHERE session_id=?", (duplicate_platform, now, json.dumps(metadata, ensure_ascii=False, sort_keys=True), duplicate["session_id"]))
                conn.execute("UPDATE recording_jobs SET status='DUPLICATE_SUPERSEDED',pid=NULL,updated_at=?,last_error='duplicate session stopped; media retained' WHERE session_id=?", (now, duplicate["session_id"]))
                repaired += 1
            if canonical["platform_session_id"] != real_platform:
                conn.execute("UPDATE live_sessions SET platform_session_id=? WHERE session_id=?", (real_platform, canonical["session_id"]))
        conn.commit()
    return {"repaired": repaired}


def update_session_liveness(conn, target: dict, state: str, now: str) -> None:
    rows = conn.execute("SELECT * FROM live_sessions WHERE monitor_target_id=? AND status IN ('RECORDING','DETECTED','WAITING_STREAM','WAITING_CAPACITY') ORDER BY started_at DESC", (target["monitor_target_id"],)).fetchall()
    for row in rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        if state != "OFFLINE_CONFIRMED":
            # Unknown observations break continuity; they do not count as
            # evidence of being offline for the entire elapsed wall time.
            for key in ("down_since", "offline_observations", "last_offline_at"):
                metadata.pop(key, None)
            metadata["last_seen_live_at" if state == "LIVE" else "last_unknown_at"] = now
            # LIVE proves the broadcast, not that our recorder is writing bytes.
            conn.execute("UPDATE live_sessions SET metadata_json=? WHERE session_id=?", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["session_id"]))
            continue
        job = conn.execute("SELECT * FROM recording_jobs WHERE session_id=?", (row["session_id"],)).fetchone()
        health = metadata.setdefault("recording_health", {})
        try:
            previous_bytes = int(health["bytes"]) if health.get("bytes") is not None else None
        except (TypeError, ValueError):
            previous_bytes = None
        observed_bytes = recording_bytes(Path(job["partial_dir"])) if job else None
        grew = previous_bytes is not None and observed_bytes is not None and observed_bytes > previous_bytes
        recorder_active = bool(job and job["status"] in {"RUNNING", "STARTING"} and pid_alive(job["pid"]))
        if observed_bytes is not None:
            # Consume every byte observation exactly once. A recorder may flush
            # a small final tail after the last RUNNING sample; persisting this
            # baseline prevents the same fixed tail from vetoing offline forever.
            health["bytes"] = observed_bytes
            health["last_observed_at"] = now
        if grew:
            health["last_growth_at"] = now
            health["growth_observations"] = int(health.get("growth_observations") or 0) + 1
        if grew and recorder_active:
            for key in ("down_since", "offline_observations", "last_offline_at"):
                metadata.pop(key, None)
            metadata["offline_rejected_media_growth_at"] = now
            record_event(conn, "OFFLINE_REJECTED_MEDIA_GROWTH", object_type="session", object_id=row["session_id"],
                         payload={"observed_at": now, "previous_bytes": previous_bytes,
                                  "observed_bytes": observed_bytes, "pid": job["pid"],
                                  "job_status": job["status"]}, severity="WARNING")
            conn.execute("UPDATE live_sessions SET metadata_json=? WHERE session_id=?", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["session_id"]))
            continue
        if grew:
            record_event(conn, "OFFLINE_MEDIA_TAIL_CONSUMED", object_type="session", object_id=row["session_id"],
                         payload={"observed_at": now, "previous_bytes": previous_bytes,
                                  "observed_bytes": observed_bytes, "pid": job["pid"] if job else None,
                                  "job_status": job["status"] if job else None}, severity="INFO")
        down_since = metadata.get("down_since")
        last_offline = metadata.get("last_offline_at")
        try:
            gap = (datetime.fromisoformat(now.replace("Z", "+00:00")) - datetime.fromisoformat(str(last_offline).replace("Z", "+00:00"))).total_seconds()
        except (ValueError, TypeError):
            gap = OFFLINE_MAX_OBSERVATION_GAP + 1
        if gap < 0 or gap > OFFLINE_MAX_OBSERVATION_GAP:
            down_since = None
        metadata["last_offline_at"] = now
        metadata["offline_observations"] = int(metadata.get("offline_observations") or 0) + 1 if down_since else 1
        if not down_since:
            metadata["down_since"] = now
            conn.execute("UPDATE live_sessions SET metadata_json=? WHERE session_id=?", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["session_id"]))
            continue
        try:
            elapsed = (datetime.fromisoformat(now.replace("Z", "+00:00")) - datetime.fromisoformat(str(down_since).replace("Z", "+00:00"))).total_seconds()
        except ValueError:
            elapsed = 0
        if elapsed >= OFFLINE_WINDOW_SECONDS and metadata["offline_observations"] >= OFFLINE_MIN_OBSERVATIONS:
            conn.execute("UPDATE live_sessions SET status='ENDED',ended_at=?,metadata_json=? WHERE session_id=?", (down_since, json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["session_id"]))
            record_event(conn, "SESSION_OFFLINE_CONFIRMED", object_type="session", object_id=row["session_id"],
                         payload={"down_since": down_since, "confirmed_at": now, "observations": metadata["offline_observations"]})
        else:
            conn.execute("UPDATE live_sessions SET metadata_json=? WHERE session_id=?", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["session_id"]))


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    owned = RECORDER_PROCESSES.get(pid)
    if owned is not None:
        if owned.poll() is None:
            return True
        RECORDER_PROCESSES.pop(pid, None)
        return False
    try:
        os.kill(int(pid), 0)
        state = subprocess.run(["/bin/ps", "-o", "stat=", "-p", str(int(pid))], capture_output=True,
                               text=True, timeout=5, check=False)
        return state.returncode == 0 and bool(state.stdout.strip()) and not state.stdout.strip().startswith("Z")
    except subprocess.TimeoutExpired:
        # kill(pid, 0) succeeded; a slow ps is not evidence that it is dead.
        return True
    except (OSError, ValueError):
        return False


def recording_keys(session: dict, target: dict) -> tuple[str, str, Path, Path]:
    account_key = "acct_" + hashlib.sha256(target["competitor_id"].encode("utf-8")).hexdigest()[:16]
    recording_key = "sess_" + hashlib.sha256(session["session_id"].encode("utf-8")).hexdigest()[:24]
    partial = Path("/Volumes/ExternalStorage/同行直播录制/media/partial") / account_key / recording_key
    completed = Path("/Volumes/ExternalStorage/同行直播录制/media/completed") / account_key / recording_key
    return account_key, recording_key, partial, completed


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def discover_recorder_pid(recording_key: str) -> int | None:
    proc = subprocess.run(["/bin/ps", "-axo", "pid=,command="], capture_output=True, text=True, timeout=10, check=False)
    for line in proc.stdout.splitlines():
        if "/runtime/bin/recorder.py" not in line or f"--session-id {recording_key}" not in line:
            continue
        try:
            return int(line.strip().split(None, 1)[0])
        except (ValueError, IndexError):
            continue
    return None


def recorder_uses_fixed_segments(recording_key: str) -> bool:
    proc = subprocess.run(["/bin/ps", "-axo", "pid=,command="], capture_output=True, text=True, timeout=10, check=False)
    # Inspect the complete process tree, not only the profile wrapper.  Older
    # recorders put the segment flag on the shared child command, while the
    # wrapper itself only carried the session path.  Any positive value is a
    # legacy fixed-segment recorder and must be migrated before it can remain
    # attached to a live session.
    import re
    pattern = re.compile(r"--segment-seconds\s+([0-9]+(?:\.[0-9]+)?)")
    for line in proc.stdout.splitlines():
        if recording_key not in line:
            continue
        match = pattern.search(line)
        if match and float(match.group(1)) > 0:
            return True
    return False


def stop_recorder_group(pid: int | None) -> None:
    if not pid:
        return
    try:
        os.killpg(int(pid), signal.SIGTERM)
        time.sleep(1)
        if pid_alive(pid):
            os.killpg(int(pid), signal.SIGKILL)
    except (OSError, ValueError):
        return


def upsert_recording_lease(conn, session_id: str, *, now: str | None = None, lease_seconds: int = 90) -> None:
    """Create or renew the deterministic lease for one recording session."""
    now = now or utc_now()
    lease_id = "rlease_" + hashlib.sha256(session_id.encode()).hexdigest()[:24]
    acquired_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    lease_until = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    conn.execute("INSERT INTO recording_leases(lease_id,session_id,node_id,fencing_token,acquired_at,lease_until,status) VALUES(?,?,?,?,?,?,?) ON CONFLICT(lease_id) DO UPDATE SET node_id=excluded.node_id,fencing_token=excluded.fencing_token,acquired_at=excluded.acquired_at,lease_until=excluded.lease_until,status='ACTIVE'", (lease_id, session_id, os.environ.get("V3_NODE_ID") or socket.gethostname(), acquired_ms, now, lease_until, "ACTIVE"))


def recording_bytes(directory: Path) -> int:
    return sum(p.stat().st_size for p in directory.iterdir() if p.is_file() and (p.name.endswith('.ts.partial') or p.suffix=='.ts')) if directory.is_dir() else 0


def recording_failure(conn, session: dict, job: dict, reason: str, now: str) -> None:
    row=conn.execute('SELECT metadata_json FROM live_sessions WHERE session_id=?',(session['session_id'],)).fetchone()
    meta=json.loads(row['metadata_json'] or '{}');health=meta.setdefault('recording_health',{})
    failures=int(health.get('consecutive_failures',min(int(job.get('restart_count') or 0),3)))+1
    delay=600 if failures>3 else 30
    retry=(datetime.fromisoformat(now.replace('Z','+00:00'))+timedelta(seconds=delay)).isoformat(timespec='milliseconds').replace('+00:00','Z')
    health.update(consecutive_failures=failures,next_retry_at=retry,last_failure=reason,last_failure_at=now)
    meta['recording_gap_seen']=True
    conn.execute("UPDATE live_sessions SET status='WAITING_STREAM',metadata_json=? WHERE session_id=?",(json.dumps(meta,ensure_ascii=False),session['session_id']))
    conn.execute("UPDATE recording_jobs SET status='WAITING_STREAM',pid=NULL,last_error=?,updated_at=? WHERE session_id=?",(f'{reason}; retry at {retry}',now,session['session_id']))
    conn.execute("UPDATE recording_leases SET status='RELEASED',lease_until=? WHERE session_id=?",(now,session['session_id']))
    record_event(conn,'RECORDING_BACKOFF' if delay==600 else 'RECORDING_RETRY_SCHEDULED',object_type='session',object_id=session['session_id'],payload={'reason':reason,'consecutive_failures':failures,'retry_at':retry,'wait_seconds':delay},severity='WARNING')


def recording_progress(conn, session: dict, job: dict, now: str) -> str:
    meta=json.loads(session['metadata_json'] or '{}');health=meta.setdefault('recording_health',{})
    size=recording_bytes(Path(job['partial_dir']))
    if health.get('pid')!=job['pid']:
        health.update(pid=job['pid'],bytes=size,checked_at=now,last_growth_at=None,growth_observations=0)
    elif size>int(health.get('bytes') or 0):
        health.update(last_growth_at=now,growth_observations=int(health.get('growth_observations') or 0)+1,consecutive_failures=0,next_retry_at=None)
        meta.setdefault('first_media_growth_at',now)
    reference=health.get('last_growth_at') or health.get('checked_at') or now
    age=(datetime.fromisoformat(now.replace('Z','+00:00'))-datetime.fromisoformat(reference.replace('Z','+00:00'))).total_seconds()
    state='RUNNING' if size>0 and health.get('growth_observations',0)>0 and age<120 else 'STALLED' if age>=120 else 'STARTING'
    health['bytes']=size
    # checked_at is the initial sample time, so a motionless file cannot reset the stall timer.
    health['last_observed_at']=now
    conn.execute('UPDATE live_sessions SET metadata_json=?,status=? WHERE session_id=?',(json.dumps(meta,ensure_ascii=False),'RECORDING' if state=='RUNNING' else 'WAITING_STREAM',session['session_id']))
    conn.execute('UPDATE recording_jobs SET status=?,updated_at=? WHERE session_id=?',(state if state!='STALLED' else 'WAITING_STREAM',now,session['session_id']))
    return state


def ensure_recording_job(session: dict, target: dict) -> dict:
    account_key, recording_key, partial_dir, completed_dir = recording_keys(session, target)
    now = utc_now()
    with connect() as conn:
        current = conn.execute("SELECT status FROM live_sessions WHERE session_id=?", (session["session_id"],)).fetchone()
        if not current or current["status"] not in {"RECORDING", "DETECTED", "WAITING_STREAM", "WAITING_CAPACITY"} or target.get("status", "ACTIVE") != "ACTIVE":
            return {"started": False, "reason": "session or target is no longer active"}
        job = conn.execute("SELECT * FROM recording_jobs WHERE session_id=?", (session["session_id"],)).fetchone()
        job=dict(job) if job else None
        if job and pid_alive(job["pid"]):
            if recorder_uses_fixed_segments(recording_key):
                stop_recorder_group(job["pid"])
                conn.execute("UPDATE recording_jobs SET status='STARTING',pid=NULL,updated_at=?,last_error='migrating from fixed segments to full-session file' WHERE session_id=?", (utc_now(), session["session_id"]))
                conn.commit()
                time.sleep(1)
            else:
                upsert_recording_lease(conn, session["session_id"], now=now)
                conn.commit()
                return {"started": False, "pid": job["pid"], "recording_key": recording_key}
        discovered = discover_recorder_pid(recording_key)
        if discovered:
            conn.execute("INSERT INTO recording_jobs(job_id,session_id,status,pid,account_key,recording_key,partial_dir,completed_dir,started_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET status='STARTING',pid=excluded.pid,account_key=excluded.account_key,recording_key=excluded.recording_key,partial_dir=excluded.partial_dir,completed_dir=excluded.completed_dir,updated_at=excluded.updated_at,last_error=NULL", ("recjob_" + hashlib.sha256(session["session_id"].encode()).hexdigest()[:24], session["session_id"], "STARTING", discovered, account_key, recording_key, str(partial_dir), str(completed_dir), session["started_at"] or now, now))
            upsert_recording_lease(conn, session["session_id"], now=now)
            conn.commit()
            return {"started": False, "pid": discovered, "recording_key": recording_key, "discovered": True}
        if job and job.get('pid'):
            recording_failure(conn,session,job,'recorder exited before session end was confirmed',now)
        row=conn.execute('SELECT metadata_json FROM live_sessions WHERE session_id=?',(session['session_id'],)).fetchone()
        meta=json.loads(row['metadata_json'] or '{}');health=meta.setdefault('recording_health',{})
        if health.get('next_retry_at') and health['next_retry_at']>now:
            conn.commit()
            return {'started':False,'recording_key':recording_key,'reason':'recording backoff','next_retry_at':health['next_retry_at']}
        if health.get('next_retry_at') and int(health.get('consecutive_failures') or 0)>3:
            health['consecutive_failures']=0
        health['next_retry_at']=None
        capacity = int((load_config().get("atomic_activation") or {}).get("max_concurrent_recordings", 65))
        running_count = conn.execute("SELECT count(*) FROM recording_jobs WHERE status IN ('RUNNING','STARTING') AND session_id<>?", (session["session_id"],)).fetchone()[0]
        if running_count >= capacity:
            conn.execute("INSERT INTO recording_jobs(job_id,session_id,status,pid,account_key,recording_key,partial_dir,completed_dir,started_at,updated_at,restart_count,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET status='WAITING_CAPACITY',pid=NULL,account_key=excluded.account_key,recording_key=excluded.recording_key,partial_dir=excluded.partial_dir,completed_dir=excluded.completed_dir,updated_at=excluded.updated_at,last_error=excluded.last_error", ("recjob_" + hashlib.sha256(session["session_id"].encode()).hexdigest()[:24], session["session_id"], "WAITING_CAPACITY", None, account_key, recording_key, str(partial_dir), str(completed_dir), session["started_at"] or now, now, 0, f"recording capacity {capacity} reached ({running_count} running sessions)"))
            conn.execute("UPDATE live_sessions SET status='WAITING_CAPACITY',completeness='UNKNOWN' WHERE session_id=? AND status IN ('RECORDING','DETECTED','WAITING_STREAM','WAITING_CAPACITY')", (session["session_id"],))
            conn.commit()
            return {"started": False, "pid": None, "recording_key": recording_key, "reason": "recording capacity reached", "capacity": capacity, "running_count": running_count}
        conn.execute("INSERT INTO recording_jobs(job_id,session_id,status,pid,account_key,recording_key,partial_dir,completed_dir,started_at,updated_at,restart_count,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(session_id) DO UPDATE SET status='STARTING',pid=NULL,account_key=excluded.account_key,recording_key=excluded.recording_key,partial_dir=excluded.partial_dir,completed_dir=excluded.completed_dir,updated_at=excluded.updated_at,restart_count=recording_jobs.restart_count+1,last_error=NULL", ("recjob_" + hashlib.sha256(session["session_id"].encode()).hexdigest()[:24], session["session_id"], "STARTING", None, account_key, recording_key, str(partial_dir), str(completed_dir), session["started_at"] or now, now, 0))
        conn.execute("UPDATE live_sessions SET status='WAITING_STREAM',metadata_json=? WHERE session_id=?",(json.dumps(meta,ensure_ascii=False),session['session_id']))
        conn.commit()
    log_dir = RECORDING_LOG_ROOT / recording_key
    log_dir.mkdir(parents=True, exist_ok=True)
    handle = (log_dir / "recorder.log").open("ab")
    v3_config = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
    argv = [str(PYTHON), str(RECORDER), "--url", target["live_url"], "--account-id", account_key, "--session-id", recording_key, "--duration", str(v3_config.get("recording_duration_seconds", 0)), "--quality", str(v3_config.get("recording_quality") or "LD"), "--approved"]
    # A legacy fixed-segment run may have left ordinary .ts files in the
    # partial directory.  Treat either suffix as the same business-session
    # continuation so that the supervisor never starts a second session.
    if any(partial_dir.glob("*.ts.partial")) or any(partial_dir.glob("*.ts")):
        argv.append("--resume")
    process = subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    RECORDER_PROCESSES[process.pid] = process
    handle.close()
    with connect() as conn:
        if process.poll() is not None:
            RECORDER_PROCESSES.pop(process.pid, None)
            recording_failure(conn,session,job or {},'recorder exited before establishing stream',utc_now())
            conn.commit()
            return {"started": False, "pid": None, "recording_key": recording_key, "reason": "recorder exited before stream"}
        conn.execute("UPDATE recording_jobs SET status='STARTING',pid=?,updated_at=? WHERE session_id=?", (process.pid, utc_now(), session["session_id"]))
        record_event(conn, "RECORDING_PROCESS_STARTED", object_type="session", object_id=session["session_id"],
                     payload={"pid": process.pid, "recording_key": recording_key, "resume": "--resume" in argv,
                              "log_path": str(log_dir / "recorder.log")})
        row=conn.execute('SELECT metadata_json FROM live_sessions WHERE session_id=?',(session['session_id'],)).fetchone()
        meta=json.loads(row['metadata_json'] or '{}');health=meta.setdefault('recording_health',{})
        health.update(pid=process.pid,bytes=recording_bytes(partial_dir),checked_at=utc_now(),last_growth_at=None,growth_observations=0)
        conn.execute('UPDATE live_sessions SET metadata_json=? WHERE session_id=?',(json.dumps(meta,ensure_ascii=False),session['session_id']))
        upsert_recording_lease(conn, session["session_id"])
        conn.commit()
    return {"started": True, "pid": process.pid, "recording_key": recording_key}


FINALIZER_SOURCE_MANIFEST = "source-segments.preconcat.json"
FINALIZER_CONCAT_LIST = ".concat-list.txt"
FINALIZER_MERGE_TEMP = ".整场直播.merge.tmp"


def _atomic_write_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _parse_source_time(value: object) -> tuple[float, str] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.timestamp(), parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _source_generation(name: str) -> tuple[int, int, int]:
    part = re.search(r"\.part(\d+)(?:\.|$)", name, flags=re.IGNORECASE)
    refresh = re.search(r"\.refresh(\d+)(?:\.|$)", name, flags=re.IGNORECASE)
    legacy = re.search(r"_(\d+)\.ts$", name, flags=re.IGNORECASE)
    if name.startswith("整场直播.") or name.startswith("整场直播.ts"):
        return (int(part.group(1)) if part else 0,
                int(refresh.group(1)) if refresh else 0, 0)
    return (1_000_000_000, 0, int(legacy.group(1)) if legacy else 1_000_000_000)


def _source_sidecar(path: Path) -> tuple[Path, dict, str]:
    sidecar = Path(str(path) + ".recording-state.json")
    if not sidecar.is_file():
        return sidecar, {}, "MISSING"
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return sidecar, {}, "MALFORMED_JSON"
    if not isinstance(payload, dict):
        return sidecar, {}, "MALFORMED_JSON"
    classification = "PRESENT_VALID" if _parse_source_time(payload.get("started_at")) else "PRESENT_NO_STARTED_AT"
    return sidecar, payload, classification


def _probe_media_details(path: Path) -> dict:
    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    proc = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries",
         "format=duration,size:stream=index,codec_type,codec_name,channels,width,height",
         "-of", "json", str(path)],
        capture_output=True, text=True, timeout=60, check=False,
    )
    try:
        payload = json.loads(proc.stdout or "{}")
        duration = float((payload.get("format") or {}).get("duration"))
    except (ValueError, TypeError, json.JSONDecodeError):
        payload = {}
        duration = 0.0
    streams = []
    for stream in payload.get("streams") or []:
        stream_type = stream.get("codec_type")
        codec = stream.get("codec_name")
        if stream_type not in {"audio", "video"} or not codec:
            continue
        streams.append({
            "index": stream.get("index"),
            "codec_type": stream_type,
            "codec_name": codec,
            "channels": stream.get("channels"),
            "width": stream.get("width"),
            "height": stream.get("height"),
        })
    stream_types = sorted({stream["codec_type"] for stream in streams})
    codecs = sorted({(stream["codec_type"], stream["codec_name"]) for stream in streams})
    return {
        "ok": proc.returncode == 0 and duration > 0 and stream_types == ["audio", "video"],
        "return_code": proc.returncode,
        "duration_seconds": duration,
        "stream_types": stream_types,
        "codecs": [{"codec_type": stream_type, "codec_name": codec} for stream_type, codec in codecs],
        "streams": streams,
        "stderr_tail": (proc.stderr or "")[-1000:],
    }


def _source_manifest_entries(parts: list[Path]) -> tuple[list[dict], str]:
    entries = []
    for part in parts:
        sidecar, sidecar_payload, classification = _source_sidecar(part)
        parsed_started_at = _parse_source_time(sidecar_payload.get("started_at"))
        entries.append({
            "path": str(part),
            "name": part.name,
            "sha256": file_digest(part),
            "bytes": part.stat().st_size,
            "generation": _source_generation(part.name),
            "sidecar": {
                "path": str(sidecar),
                "classification": classification,
                "sha256": file_digest(sidecar) if sidecar.is_file() else None,
                "started_at": parsed_started_at[1] if parsed_started_at else None,
                "ended_at": sidecar_payload.get("ended_at"),
                "status": sidecar_payload.get("status"),
                "return_code": sidecar_payload.get("return_code"),
                "exit_kind": sidecar_payload.get("exit_kind"),
                "ffmpeg_tail_class": sidecar_payload.get("ffmpeg_tail_class"),
                "ffmpeg_error_codes": sidecar_payload.get("ffmpeg_error_codes") or [],
            },
            "_started_at_epoch": parsed_started_at[0] if parsed_started_at else None,
        })
    if entries and all(entry["_started_at_epoch"] is not None for entry in entries):
        strategy = "SIDECAR_STARTED_AT"
        entries.sort(key=lambda entry: (entry["_started_at_epoch"], entry["generation"], entry["name"]))
    else:
        strategy = "EXPLICIT_GENERATION_THEN_FILENAME"
        entries.sort(key=lambda entry: (entry["generation"], entry["name"]))
    for entry in entries:
        entry.pop("_started_at_epoch", None)
        entry["probe"] = _probe_media_details(Path(entry["path"]))
    return entries, strategy


def _record_finalizer_error(conn, session_id: str, reason: str) -> None:
    conn.execute("UPDATE recording_jobs SET last_error=? WHERE session_id=?", (reason[:2000], session_id))


def _classify_published_source_segments(conn, session_id: str, canonical_segment_id: str,
                                         source_entries: list[dict]) -> None:
    """Classify only rows for which finalization produced direct evidence.

    A source moved from partial to completed storage keeps its segment identity:
    update the path and mark it retained.  If that destination is already owned
    by an equivalent row, keep the owner and supersede the stale duplicate.  A
    hash conflict fails closed instead of violating UNIQUE(session_id, path).
    """
    known_sources: dict[str, dict] = {}
    for entry in source_entries:
        current_path = str(entry.get("path") or "")
        original_path = str(entry.get("original_path") or "")
        info = {"checksum": str(entry.get("sha256") or "") or None, "current_path": current_path}
        if current_path:
            known_sources[current_path] = info
        if original_path:
            known_sources[original_path] = info
    now = utc_now()
    rows = conn.execute(
        "SELECT segment_id,path,checksum FROM recording_segments WHERE session_id=? AND segment_id<>?",
        (session_id, canonical_segment_id),
    ).fetchall()
    for row in rows:
        recorded_path = str(row["path"])
        info = known_sources.get(recorded_path)
        replacement_path = str((info or {}).get("current_path") or "")
        if Path(recorded_path).is_file():
            lifecycle, replacement, new_path = "SOURCE_RETAINED", None, recorded_path
        elif info and replacement_path and Path(replacement_path).is_file():
            owner = conn.execute(
                "SELECT segment_id,checksum FROM recording_segments WHERE session_id=? AND path=?",
                (session_id, replacement_path),
            ).fetchone()
            compatible = (not row["checksum"] or not info.get("checksum") or str(row["checksum"]) == info["checksum"])
            if not owner or owner["segment_id"] == row["segment_id"]:
                lifecycle, replacement, new_path = ("SOURCE_RETAINED", None, replacement_path) if compatible else ("LOST_REVIEW", None, recorded_path)
            elif compatible and (not owner["checksum"] or not info.get("checksum") or str(owner["checksum"]) == info["checksum"]):
                lifecycle, replacement, new_path = "SOURCE_SUPERSEDED", canonical_segment_id, recorded_path
            else:
                lifecycle, replacement, new_path = "LOST_REVIEW", None, recorded_path
        elif info and (not row["checksum"] or not info.get("checksum") or str(row["checksum"]) == info["checksum"]):
            lifecycle, replacement, new_path = "SOURCE_SUPERSEDED", canonical_segment_id, recorded_path
        else:
            lifecycle, replacement, new_path = "LOST_REVIEW", None, recorded_path
        conn.execute(
            "UPDATE recording_segments SET path=?,lifecycle_status=?,superseded_by_segment_id=?,lifecycle_updated_at=? WHERE segment_id=?",
            (new_path, lifecycle, replacement, now, row["segment_id"]),
        )


def finalize_media_for_session(conn, session: dict, job: dict) -> None:
    partial_dir = Path(job["partial_dir"])
    completed_dir = Path(job["completed_dir"])
    if session['status']=='MEDIA_COMPLETE' and not json.loads(session['metadata_json'] or '{}').get('media_coverage'):
        # Revalidate legacy "complete" labels without altering their media.
        session={**session,'status':'ENDED'}
    if session['status']!='ENDED' or pid_alive(job.get('pid')):
        return
    # V3 Final keeps one user-visible full-session file.  Continuation parts
    # are merged here only when a recorder restart was required.
    if not partial_dir.exists() and not completed_dir.exists():
        return
    media_root = completed_dir if completed_dir.exists() else partial_dir
    source_entries: list[dict] = []
    ordering_strategy = "NO_SOURCE_MANIFEST"
    source_manifest_path = media_root / FINALIZER_SOURCE_MANIFEST
    if source_manifest_path.is_file():
        try:
            saved_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
            saved_sources = saved_manifest.get("sources")
            ordering_strategy = str(saved_manifest.get("ordering_strategy") or "RECOVERED_SOURCE_MANIFEST")
        except (OSError, json.JSONDecodeError, AttributeError):
            saved_sources = None
        if not isinstance(saved_sources, list):
            _record_finalizer_error(conn, session["session_id"], "source manifest is malformed; retained evidence not promoted")
            return
        source_entries = saved_sources
    if media_root == partial_dir:
        parts = [path for path in media_root.iterdir() if path.is_file() and (path.name.endswith(".ts.partial") or path.name.endswith(".ts")) and path.name != "整场直播.ts"]
        final_path = media_root / "整场直播.ts"
        if parts and not final_path.exists():
            source_entries, ordering_strategy = _source_manifest_entries(parts)
            source_manifest = {
                "session_id": session["session_id"],
                "phase": "PRE_CONCAT",
                "created_at": utc_now(),
                "ordering_strategy": ordering_strategy,
                "sources": source_entries,
            }
            _atomic_write_json(source_manifest_path, source_manifest)
            invalid_sources = [entry["name"] for entry in source_entries if not entry["probe"]["ok"]]
            codec_signatures = {
                tuple((codec["codec_type"], codec["codec_name"]) for codec in entry["probe"]["codecs"])
                for entry in source_entries if entry["probe"]["ok"]
            }
            if invalid_sources:
                _record_finalizer_error(conn, session["session_id"], "source media validation failed; source parts retained: " + ", ".join(invalid_sources))
                return
            if len(codec_signatures) != 1:
                _record_finalizer_error(conn, session["session_id"], "source codec/stream layout changed; source parts retained")
                return
            ordered_parts = [Path(entry["path"]) for entry in source_entries]
            concat_list = media_root / FINALIZER_CONCAT_LIST
            concat_list.write_text("".join("file '" + path.as_posix().replace("'", "'\\''") + "'\n" for path in ordered_parts), encoding="utf-8")
            merging = media_root / FINALIZER_MERGE_TEMP
            if merging.exists():
                merging.unlink()
            if len(ordered_parts) == 1:
                shutil.copyfile(ordered_parts[0], merging)
                proc = None
            else:
                ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
                proc = subprocess.run([ffmpeg, "-hide_banner", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_list), "-c", "copy", "-f", "mpegts", str(merging)], capture_output=True, text=True, timeout=3600, check=False)
            if (proc is not None and proc.returncode != 0) or not merging.exists() or merging.stat().st_size == 0:
                detail = (proc.stderr or "")[-1000:] if proc is not None else "empty copied output"
                _record_finalizer_error(conn, session["session_id"], "media concat failed; source parts retained: " + detail)
                return
            merged_probe = _probe_media_details(merging)
            expected_concat_duration = sum(entry["probe"]["duration_seconds"] for entry in source_entries)
            duration_tolerance = max(2.0, expected_concat_duration * 0.02)
            source_codecs = next(iter(codec_signatures))
            merged_codecs = tuple((codec["codec_type"], codec["codec_name"]) for codec in merged_probe["codecs"])
            if (not merged_probe["ok"] or
                    abs(merged_probe["duration_seconds"] - expected_concat_duration) > duration_tolerance or
                    merged_codecs != source_codecs):
                _record_finalizer_error(
                    conn, session["session_id"],
                    "merged media validation failed; source parts retained "
                    f"expected_duration={expected_concat_duration:.3f} actual_duration={merged_probe['duration_seconds']:.3f} "
                    f"expected_codecs={source_codecs!r} actual_codecs={merged_codecs!r}",
                )
                return
            merging.replace(final_path)
        if final_path.exists() and not completed_dir.exists():
            completed_dir.parent.mkdir(parents=True, exist_ok=True)
            partial_dir.replace(completed_dir)
            media_root = completed_dir
            if source_entries:
                for entry in source_entries:
                    entry["original_path"] = entry["path"]
                    entry["path"] = str(media_root / entry["name"])
                    entry["sidecar"]["original_path"] = entry["sidecar"]["path"]
                    entry["sidecar"]["path"] = str(Path(str(media_root / entry["name"]) + ".recording-state.json"))
                source_manifest_path = media_root / FINALIZER_SOURCE_MANIFEST
                source_manifest = {
                    "session_id": session["session_id"],
                    "phase": "PUBLISHED_SOURCES_RETAINED",
                    "created_at": utc_now(),
                    "ordering_strategy": ordering_strategy,
                    "sources": source_entries,
                }
                _atomic_write_json(source_manifest_path, source_manifest)
    for path in sorted(media_root.glob("整场直播.ts")) if media_root.exists() else []:
        segment_id = "segment_" + hashlib.sha256(f"{session['session_id']}:{path.name}".encode("utf-8")).hexdigest()[:24]
        checksum = file_digest(path)
        canonical=conn.execute('SELECT segment_id FROM recording_segments WHERE session_id=? AND path=?',(session['session_id'],str(path))).fetchone()
        if canonical:segment_id=canonical['segment_id']
        prior=conn.execute('SELECT session_id,path,checksum FROM recording_segments WHERE segment_id=?',(segment_id,)).fetchone()
        if prior and (prior['session_id']!=session['session_id'] or (prior['path']!=str(path) and Path(prior['path']).is_file() and prior['checksum']!=checksum)):
            conn.execute("UPDATE recording_jobs SET last_error='media relocation conflict; original files retained' WHERE session_id=?",(session['session_id'],))
            return
        captured_from = session["started_at"] or utc_now()
        captured_to = session["ended_at"] if session["status"] == "ENDED" else None
        conn.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,captured_to,status,bytes,lifecycle_status,lifecycle_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(segment_id) DO UPDATE SET path=excluded.path,checksum=excluded.checksum,status=excluded.status,bytes=excluded.bytes,captured_to=excluded.captured_to", (segment_id, session["session_id"], str(path), checksum, captured_from, captured_to, "COMPLETE" if session["status"] == "ENDED" else "PARTIAL", path.stat().st_size, "UNCLASSIFIED", utc_now()))
    final_files = sorted(media_root.glob("整场直播.ts")) if media_root.exists() else []
    if session["status"] != "ENDED" or not final_files:
        return
    path = final_files[0]
    final_probe = _probe_media_details(path)
    if not final_probe["ok"]:
        _record_finalizer_error(conn, session["session_id"], "published media failed final A/V validation; retained evidence not promoted")
        return
    duration = final_probe["duration_seconds"]
    expected=(datetime.fromisoformat(session['ended_at'].replace('Z','+00:00'))-datetime.fromisoformat(session['started_at'].replace('Z','+00:00'))).total_seconds()
    metadata=json.loads(session['metadata_json'] or '{}')
    complete=not metadata.get('recording_gap_seen') and int(job.get('restart_count') or 0)==0 and abs(duration-expected)<=max(60,expected*0.02)
    metadata['media_coverage']={'duration_seconds':duration,'observed_session_seconds':expected,'continuous_capture':complete,'checked_at':utc_now()}
    checksum = file_digest(path)
    captured_from = session["started_at"] or utc_now()
    captured_to = session["ended_at"] or utc_now()
    segment_id = conn.execute('SELECT segment_id FROM recording_segments WHERE session_id=? AND path=?',(session['session_id'],str(path))).fetchone()['segment_id']
    conn.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,captured_to,status,bytes,lifecycle_status,superseded_by_segment_id,lifecycle_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,path) DO UPDATE SET checksum=excluded.checksum,status='COMPLETE',bytes=excluded.bytes,captured_to=excluded.captured_to,lifecycle_status='CANONICAL_ACTIVE',superseded_by_segment_id=NULL,lifecycle_updated_at=excluded.lifecycle_updated_at", (segment_id, session["session_id"], str(path), checksum, captured_from, captured_to, "COMPLETE", path.stat().st_size, "CANONICAL_ACTIVE", None, utc_now()))
    manifest_path = media_root / "media-manifest.json"
    retained_sources = []
    if source_entries:
        for entry in source_entries:
            retained_path = media_root / entry["name"]
            retained_sidecar = Path(str(retained_path) + ".recording-state.json")
            retained_sources.append({
                **{key: value for key, value in entry.items() if key not in {"path", "sidecar"}},
                "path": str(retained_path),
                "sidecar": {**entry["sidecar"], "path": str(retained_sidecar)},
            })
    source_manifest_path = media_root / FINALIZER_SOURCE_MANIFEST
    source_manifest_hash = file_digest(source_manifest_path) if source_manifest_path.is_file() else None
    manifest = {
        "session_id": session["session_id"], "final_path": str(path), "sha256": checksum,
        "bytes": path.stat().st_size, "captured_from": captured_from, "captured_to": captured_to,
        "created_at": utc_now(), "coverage": metadata["media_coverage"],
        "completeness": "COMPLETE" if complete else "PARTIAL",
        "final_probe": final_probe,
        "source_manifest_path": str(source_manifest_path) if source_manifest_hash else None,
        "source_manifest_sha256": source_manifest_hash,
        "retained_sources": retained_sources,
    }
    _atomic_write_json(manifest_path, manifest)
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    conn.execute("INSERT INTO media_manifests(manifest_id,session_id,status,manifest_path,manifest_hash,segment_count,total_bytes,complete_from,complete_to,verified_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET status='VERIFIED',manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,segment_count=excluded.segment_count,total_bytes=excluded.total_bytes,complete_from=excluded.complete_from,complete_to=excluded.complete_to,verified_at=excluded.verified_at,metadata_json=excluded.metadata_json", ("manifest:" + session["session_id"], session["session_id"], "VERIFIED", str(manifest_path), manifest_hash, max(1, len(retained_sources)), path.stat().st_size, captured_from, captured_to, utc_now(), json.dumps({"final_file": str(path), "source_manifest": str(source_manifest_path) if source_manifest_hash else None, "retained_source_count": len(retained_sources)}, ensure_ascii=False)))
    _classify_published_source_segments(conn, session["session_id"], segment_id, retained_sources)
    try:
        end_time = datetime.fromisoformat(captured_to.replace("Z", "+00:00"))
        retention = json.loads(V3_CONFIG.read_text(encoding="utf-8")).get("retention") or {}
        video_hours = int(retention.get("video_hours") or 72)
        video_due = (end_time + timedelta(hours=video_hours)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except ValueError:
        video_due = utc_now()
    conn.execute("INSERT INTO retention_jobs(retention_job_id,object_type,object_id,policy_name,status,not_before,created_at,updated_at,payload_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,policy_name) DO UPDATE SET not_before=excluded.not_before,payload_json=excluded.payload_json", ("retention:video:" + session["session_id"], "VIDEO", session["session_id"], "VIDEO_72H", "PENDING", video_due, utc_now(), utc_now(), json.dumps({"session_id": session["session_id"], "path": str(path), "manifest_path": str(manifest_path), "source_manifest_path": str(source_manifest_path) if source_manifest_hash else None, "retained_sources": [entry["path"] for entry in retained_sources]}, ensure_ascii=False)))
    conn.execute("UPDATE live_sessions SET status='MEDIA_COMPLETE',completeness=?,metadata_json=? WHERE session_id=?", ('COMPLETE' if complete else 'PARTIAL',json.dumps(metadata,ensure_ascii=False),session["session_id"]))


def reconcile_recording_jobs() -> dict:
    restarted = finalized = running = 0
    with connect() as conn:
        jobs = [dict(row) for row in conn.execute("SELECT j.*,s.status AS session_status,s.started_at AS session_started_at,s.ended_at AS session_ended_at,s.monitor_target_id,s.source_url FROM recording_jobs j JOIN live_sessions s ON s.session_id=j.session_id")]
    for job in jobs:
        alive = pid_alive(job["pid"])
        with connect() as conn:
            session = conn.execute("SELECT * FROM live_sessions WHERE session_id=?", (job["session_id"],)).fetchone()
            target = conn.execute("SELECT * FROM monitor_targets WHERE monitor_target_id=?", (session["monitor_target_id"],)).fetchone() if session else None
            if not session or not target:
                continue
            if alive and session['status']=='ENDED':
                record_event(conn, "RECORDING_STOP_REQUESTED", object_type="session", object_id=session["session_id"],
                             payload={"pid": job["pid"], "reason": "confirmed offline window", "ended_at": session["ended_at"]})
                stop_recorder_group(job['pid'])
                alive=pid_alive(job['pid'])
                if alive:continue
            if alive:
                # Migrate any pre-V3 fixed-segment child in place.  This is a
                # one-time compatibility action: the old process group is
                # stopped, the same business session is retained, and the
                # supervisor starts the configured whole-session recorder.
                if recorder_uses_fixed_segments(job["recording_key"]):
                    stop_recorder_group(job["pid"])
                    conn.execute("UPDATE recording_jobs SET status='STARTING',pid=NULL,updated_at=?,last_error=? WHERE session_id=?", (utc_now(), "migrating legacy fixed segments to one full-session file", session["session_id"]))
                    conn.commit()
                    ensure_recording_job(dict(session), dict(target))
                    restarted += 1
                else:
                    state=recording_progress(conn,dict(session),job,utc_now())
                    if state=='STALLED':
                        stop_recorder_group(job['pid'])
                        recording_failure(conn,dict(session),job,'media stopped growing for 120 seconds',utc_now())
                    else:
                        upsert_recording_lease(conn, session["session_id"])
                        running += int(state=='RUNNING')
            elif session["status"] in {"ENDED", "MEDIA_COMPLETE", "DUPLICATE_SUPERSEDED"}:
                finalize_media_for_session(conn, dict(session), job)
                final_state=conn.execute('SELECT status FROM live_sessions WHERE session_id=?',(session['session_id'],)).fetchone()[0]
                conn.execute("UPDATE recording_jobs SET status=?,pid=NULL,updated_at=? WHERE session_id=?", ('COMPLETE' if final_state in {'MEDIA_COMPLETE','DUPLICATE_SUPERSEDED'} else 'WAITING_STREAM',utc_now(), session["session_id"]))
                conn.execute("UPDATE recording_leases SET status='RELEASED',lease_until=? WHERE session_id=? AND status='ACTIVE'", (utc_now(), session["session_id"]))
                finalized += 1
            elif target["status"] == "ACTIVE" and target["live_status"] in {"LIVE", "UNKNOWN"}:
                # An already-authorised session must recover even if the room
                # probes temporarily fail. UNKNOWN neither creates a new
                # session nor proves the existing broadcast ended. The same
                # bounded retry/backoff and fresh URL resolution still apply.
                conn.commit()
                ensure_recording_job(dict(session), dict(target))
                restarted += 1
                continue
            else:
                conn.execute("UPDATE recording_jobs SET status='WAITING_STREAM',pid=NULL,updated_at=? WHERE session_id=?", (utc_now(), session["session_id"]))
                conn.execute("UPDATE live_sessions SET status='WAITING_STREAM' WHERE session_id=? AND status='RECORDING'", (session["session_id"],))
                conn.execute("UPDATE recording_leases SET status='RELEASED',lease_until=? WHERE session_id=? AND status='ACTIVE'", (utc_now(), session["session_id"]))
            conn.commit()
    return {"running": running, "restarted": restarted, "finalized": finalized}


def monitor_once(*, start_recordings: bool = False) -> dict:
    if not active():
        return {"status": "DISABLED", "reason": "V3 atomic activation is not ACTIVE", "monitoring_started": False}
    readiness = activation_readiness()
    # The migration bridge must continue protecting the full fleet while the
    # final release gate is blocked.  Production readiness controls the atomic
    # cutover command; it must never silently turn off already-authorized live
    # monitoring and recording.
    now_due = utc_now()
    with connect() as conn:
        total_targets = conn.execute("SELECT count(*) FROM monitor_targets WHERE status='ACTIVE'").fetchone()[0]
        targets = [dict(row) for row in conn.execute("SELECT m.*,c.account_name FROM monitor_targets m JOIN competitors c ON c.competitor_id=m.competitor_id WHERE m.status='ACTIVE' AND (m.next_check_at IS NULL OR m.next_check_at<=?) ORDER BY m.next_check_at,m.monitor_target_id", (now_due,))]

    def secondary_probe(target: dict) -> dict:
        # Use the independent streamget resolver instead of yt-dlp: current
        # yt-dlp releases reject live.douyin.com room URLs as unsupported, so
        # treating that error as a second probe created a permanent UNKNOWN
        # state without adding independent evidence.
        command = [str(PYTHON), str(STREAMGET), "--url", target["live_url"]]
        try:
            proc = subprocess.run(command, capture_output=True, text=True, timeout=15, check=False)
        except subprocess.TimeoutExpired:
            return {"status": "UNKNOWN", "reason": "streamget timeout"}
        if proc.returncode == 0:
            try:
                data = json.loads(proc.stdout)
            except json.JSONDecodeError:
                return {"status": "UNKNOWN", "reason": "streamget invalid JSON"}
            if data.get("status") in {"LIVE", "OFFLINE_CONFIRMED"}:
                return {"status": data.get("status"), "room_id": data.get("room_id"), "anchor_name": data.get("anchor_name"), "title": data.get("title"), "upstream_version": data.get("upstream_version")}
            return {"status": "UNKNOWN", "reason": data.get("reason") or "streamget returned UNKNOWN"}
        return {"status": "UNKNOWN", "reason": (proc.stderr or proc.stdout)[-500:]}

    def probe(target: dict) -> tuple[dict, dict, str]:
        proc = subprocess.run([str(PYTHON), str(RECORDER), "--url", target["live_url"], "--check-only", "--approved-read-only-probe"], capture_output=True, text=True, timeout=20, check=False)
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = {"status": "UNKNOWN", "reason": "recorder returned invalid JSON"}
        primary_state = payload.get("status") if payload.get("status") in {"LIVE", "OFFLINE_CONFIRMED"} else "UNKNOWN"
        secondary = secondary_probe(target)
        secondary_state = secondary.get("status") if secondary.get("status") in {"LIVE", "OFFLINE_CONFIRMED"} else "UNKNOWN"
        if "LIVE" in {primary_state, secondary_state}:
            state = "LIVE"
        elif primary_state == secondary_state == "OFFLINE_CONFIRMED":
            state = "OFFLINE_CONFIRMED"
        else:
            state = "UNKNOWN"
        combined = {**payload, "room_id": payload.get("room_id") or secondary.get("room_id"), "primary_probe": primary_state, "secondary_probe": secondary_state, "secondary_detail": secondary}
        return target, combined, state

    results = []
    probed: list[tuple[dict, dict, str]] = []
    capacity = int((load_config().get("atomic_activation") or {}).get("max_concurrent_recordings", 65))
    with ThreadPoolExecutor(max_workers=min(capacity, max(1, len(targets)))) as executor:
        futures = [executor.submit(probe, target) for target in targets]
        for future in as_completed(futures):
            try:
                probed.append(future.result())
            except Exception as exc:  # noqa: BLE001
                probed.append(({}, {"status": "UNKNOWN", "reason": exc.__class__.__name__}, "UNKNOWN"))
    for target, payload, state in sorted(probed, key=lambda item: item[0].get("monitor_target_id", "")):
        if not target:
            results.append({"competitor_id": None, "account_name": None, "live_status": "UNKNOWN", "checked_at": utc_now(), "recording_started": False, "reason": payload.get("reason")})
            continue
        now = utc_now()
        with connect() as conn:
            previous = conn.execute("SELECT live_status,consecutive_unknown FROM monitor_targets WHERE monitor_target_id=?", (target["monitor_target_id"],)).fetchone()
            previous_unknown = int(previous["consecutive_unknown"] or 0) if previous else 0
            if state == "UNKNOWN":
                delay = min(180, 60 * (2 ** min(previous_unknown, 2)))
            else:
                delay = 30 + int(hashlib.sha256(target["monitor_target_id"].encode()).hexdigest()[:2], 16) % 6
            next_check = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            conn.execute("UPDATE monitor_targets SET live_status=?,last_checked_at=?,next_check_at=?,consecutive_unknown=CASE WHEN ?='UNKNOWN' THEN consecutive_unknown+1 ELSE 0 END WHERE monitor_target_id=?", (state, now, next_check, state, target["monitor_target_id"]))
            session = create_session_if_live(conn, target, {**payload, "live_status": state})
            update_session_liveness(conn, target, state, now)
            conn.commit()
        if session:
            enqueue_outbox(object_type="session_projection", object_id=session["session_id"], destination="feishu_base", payload={"session_id": session["session_id"], "status": session["status"], "profile_id": PROFILE_ID})
        if not previous or previous["live_status"] != state:
            enqueue_outbox(object_type="monitor_status", object_id=target["monitor_target_id"], destination="feishu_base", payload={"monitor_id": target["monitor_target_id"], "state": state, "profile_id": PROFILE_ID})
        recording_started = False
        if session and start_recordings:
            recording = ensure_recording_job(session, target)
            recording_started = bool(recording.get("started"))
        results.append({"competitor_id": target["competitor_id"], "account_name": target["account_name"], "live_status": state, "checked_at": now, "recording_started": recording_started, "reason": payload.get("reason") if state == "UNKNOWN" else None})
    recording_health = reconcile_recording_jobs()
    all_unknown = bool(results) and all(item["live_status"] == "UNKNOWN" for item in results)
    health = "DEGRADED" if all_unknown else "READY"
    v3_config = json.loads(V3_CONFIG.read_text(encoding="utf-8"))
    heartbeat(health, {"target_count": total_targets, "due_target_count": len(targets), "result_count": len(results), "unknown_count": sum(1 for item in results if item["live_status"] == "UNKNOWN"), "live_count": sum(1 for item in results if item["live_status"] == "LIVE"), "recording_health": recording_health, "full_fleet": True, "production_gate": readiness.get("production_gate"), "final_ready": readiness.get("final_ready"), "normal_recording_concurrency": v3_config.get("normal_recording_concurrency", 30), "expected_max_recording_concurrency": v3_config.get("expected_max_recording_concurrency", 50), "capacity_test_concurrency": v3_config.get("capacity_test_concurrency", 65), "recording_quality": v3_config.get("recording_quality", "LD"), "recording_mode": v3_config.get("recording_mode", "LOWEST_VIDEO_WITH_AUDIO"), "recording_segment_seconds": v3_config.get("recording_segment_seconds", 0), "recording_duration_seconds": v3_config.get("recording_duration_seconds", 0), "speech_audio_format": v3_config.get("asr_audio_format", "opus"), "speech_audio_bitrate_kbps": v3_config.get("asr_audio_bitrate_kbps", 48), "video_retention_hours": (v3_config.get("retention") or {}).get("video_hours", 72), "audio_retention_hours": (v3_config.get("retention") or {}).get("audio_hours", 168)})
    return {"status": "COMPLETE", "health": health, "checked_at": utc_now(), "target_count": total_targets, "due_target_count": len(targets), "results": results, "recording_health": recording_health, "full_fleet": True, "activation": readiness}


PRODUCT_URL_RE = re.compile(r"https://[^\s<>\[\]()\"']+", re.IGNORECASE)
PRODUCT_ID_RE = re.compile(r"(?<!\d)\d{10,22}(?!\d)")
PRODUCT_QUERY_KEYS = ("promotion_id", "product_id", "id")


def _allowed_product_host(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return parsed.scheme.lower() == "https" and (
        host == "douyin.com" or host.endswith(".douyin.com")
        or host == "jinritemai.com" or host.endswith(".jinritemai.com")
    )


def extract_product_candidate(content: str) -> str | None:
    for match in PRODUCT_URL_RE.finditer(content or ""):
        url = match.group(0).rstrip("。,.，；;！!？?")
        if _allowed_product_host(url):
            return url
    numeric = PRODUCT_ID_RE.search(content or "")
    return numeric.group(0) if numeric else None


def _product_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in PRODUCT_QUERY_KEYS:
        for value in query.get(key, []):
            match = PRODUCT_ID_RE.search(str(value))
            if match:
                return match.group(0)
    decoded = unquote(unquote(url))
    for key in PRODUCT_QUERY_KEYS:
        match = re.search(rf"(?:^|[?&]){key}=(\d{{10,22}})(?:[&#]|$)", decoded)
        if match:
            return match.group(1)
    return None


def resolve_product_input(content: str, *, max_redirects: int = 5, timeout_seconds: float = 15.0) -> tuple[str, dict]:
    candidate = extract_product_candidate(content)
    if not candidate:
        raise ValueError("商品任务缺少受支持的商品链接或商品ID")
    if PRODUCT_ID_RE.fullmatch(candidate):
        return candidate, {"input": candidate, "resolved_input": candidate, "redirect_chain": [], "resolution": "numeric_id"}
    if not _allowed_product_host(candidate):
        raise ValueError("商品链接域名不在白名单")
    original_host = (urlparse(candidate).hostname or "").lower()
    direct_id = _product_id_from_url(candidate)
    if direct_id:
        return direct_id, {"input": candidate, "resolved_input": direct_id, "redirect_chain": [candidate], "resolution": "query_product_id"}
    if original_host == "alliance.jinritemai.com":
        return candidate, {"input": candidate, "resolved_input": candidate, "redirect_chain": [candidate], "resolution": "alliance_url"}

    import httpx

    chain = [candidate]
    current = candidate
    with httpx.Client(follow_redirects=False, timeout=timeout_seconds, trust_env=False, headers={"User-Agent": "Mozilla/5.0 RuntimeV3ProductResolver/1.0"}) as client:
        for _ in range(max_redirects + 1):
            if not _allowed_product_host(current):
                raise ValueError("短链跳转到非白名单域名")
            response = client.head(current)
            if response.status_code in {405, 501}:
                response = client.get(current, headers={"Range": "bytes=0-0"})
            if response.status_code not in {301, 302, 303, 307, 308}:
                break
            location = response.headers.get("location")
            if not location:
                raise RuntimeError("短链响应缺少Location")
            current = urljoin(current, location)
            chain.append(current)
        else:
            raise RuntimeError("商品短链跳转次数超过限制")
    if not _allowed_product_host(current):
        raise ValueError("商品最终链接域名不在白名单")
    product_id = _product_id_from_url(current)
    if not product_id:
        raise ValueError("最终链接中没有可验证的商品ID")
    return product_id, {"input": candidate, "resolved_url": current, "resolved_input": product_id, "redirect_chain": chain, "resolution": "short_url_redirect"}


def queue_scan_delivery(task_id: str, imported: dict, result_path: Path, input_data: dict) -> dict:
    from v3_runtime import enqueue_outbox_conn
    with connect() as conn:
        outbox_id=enqueue_outbox_conn(conn,object_type='scan_result',object_id=imported['scan_id'],destination='feishu_base',payload={'result_path':str(result_path),'product_id':imported['product_id'],'task_id':task_id,'scan_id':imported['scan_id'],'source_message_id':input_data.get('message_id'),'profile_id':PROFILE_ID})
        conn.execute("UPDATE tasks SET product_id=?,status='DELIVERY_PENDING',business_state='LOCAL_COMMITTED',runtime_state='IDLE',delivery_state='PENDING',current_step='DELIVERY',resume_from='OUTBOX',lease_owner=NULL,lease_until=NULL,updated_at=? WHERE task_id=?",(imported['product_id'],utc_now(),task_id))
        conn.execute("UPDATE task_leases SET status='RELEASED',released_at=? WHERE task_id=? AND status='ACTIVE'",(utc_now(),task_id))
        record_event(conn,'LOCAL_SCAN_COMMITTED',task_id=task_id,object_type='product',object_id=imported['product_id'],payload={'result_path':str(result_path),'outbox_id':outbox_id,'scan_id':imported['scan_id']})
        conn.commit()
    return {'task_id':task_id,'status':'DELIVERY_PENDING','product_id':imported['product_id'],'outbox_id':outbox_id}


def process_task_once() -> dict | None:
    task = claim_task(f"{SERVICE}:{os.getpid()}")
    if not task:
        return None
    task_id = task["task_id"]
    input_data = json.loads(task["input_json"] or "{}")
    with connect() as conn:
        saved=conn.execute("SELECT payload_json FROM checkpoints WHERE task_id=? AND checkpoint_type='SCAN_AND_IDENTITIES_COMMITTED'",(task_id,)).fetchone()
    if saved:
        committed=json.loads(saved['payload_json'])
        result_path=Path(committed['result_path'])
        with connect() as conn:
            scan=conn.execute('SELECT result_digest FROM scan_runs WHERE scan_id=? AND product_id=?',(committed['scan_id'],committed['product_id'])).fetchone()
        if not scan or not result_path.is_file() or hashlib.sha256(result_path.read_bytes()).hexdigest()!=scan['result_digest']:
            release_task(task_id,status='WAITING_HUMAN',runtime_state='WAITING_HUMAN',error_type='COMMITTED_EVIDENCE_MISSING',error_message='已提交扫描的真本缺失或校验不一致',resume_from='LOCAL_COMMIT')
            return {'task_id':task_id,'status':'WAITING_HUMAN'}
        return queue_scan_delivery(task_id,committed,result_path,input_data)
    message_id = str(input_data.get("message_id") or "")
    if not message_id:
        release_task(task_id, status="FAILED_FINAL", runtime_state="FAILED", error_type="INVALID_COMMAND", error_message="商品任务缺少商品链接或精确message_id", resume_from="INGRESS")
        return {"task_id": task_id, "status": "FAILED_FINAL", "error_type": "INVALID_COMMAND"}
    try:
        product_input, resolution = resolve_product_input(str(input_data.get("content") or ""))
    except Exception as exc:  # noqa: BLE001
        release_task(task_id, status="RETRY_WAIT", runtime_state="WAITING_TOOL", error_type="PRODUCT_LINK_RESOLUTION_FAILED", error_message=str(exc), resume_from="INGRESS")
        return {"task_id": task_id, "status": "RETRY_WAIT", "error_type": "PRODUCT_LINK_RESOLUTION_FAILED"}
    input_data["product_resolution"] = resolution
    with connect() as conn:
        conn.execute("UPDATE tasks SET input_json=?,current_step='PRODUCT_RESOLVED',updated_at=? WHERE task_id=?", (json.dumps(input_data, ensure_ascii=False, sort_keys=True), utc_now(), task_id))
        record_event(conn, "PRODUCT_INPUT_RESOLVED", task_id=task_id, object_type="task", object_id=task_id, payload=resolution)
        checkpoint(conn, task_id, "PRODUCT_INPUT_RESOLVED", "CURRENT", resolution)
        conn.commit()
    attempt_dir = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3") / task_id / f"attempt-{int(task['attempts']):04d}"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    # Runtime V3 owns the scan transaction.  The browser scanner is a
    # read-only acquisition component; the retired MVP runner is never invoked
    # by production code and cannot bypass the V3 database/outbox.
    proc = subprocess.run([str(PYTHON), str(SCANNER), product_input, "--task-id", task_id, "--output-dir", str(attempt_dir)], capture_output=True, text=True, timeout=1800, check=False)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {"status": "FAILED", "error_type": "INVALID_SCANNER_OUTPUT", "error_message": (proc.stderr or proc.stdout)[-1000:]}
    if payload.get("status") not in {"COMPLETE"}:
        error_type = str(payload.get("error_type") or "SCAN_FAILED")
        human_required = error_type in {"BUYIN_LOGIN_REQUIRED", "TABBIT_TAB_AMBIGUOUS", "TABBIT_WINDOW_AMBIGUOUS", "TABBIT_ACQUISITION_BLOCKED", "STORAGE_UNAVAILABLE", "STORAGE_LOW_SPACE", "QR_IDENTITY_UNRESOLVED"}
        next_status = "WAITING_HUMAN" if human_required else ("RETRY_WAIT" if payload.get("status") in {"FAILED", "INCOMPLETE", "DELIVERY_FAILED"} else "FAILED_FINAL")
        release_task(task_id, status=next_status, runtime_state="WAITING_HUMAN" if human_required else "WAITING_TOOL", error_type=error_type, error_message=str(payload.get("error_message") or "扫描未完成"), resume_from="SCANNING")
        with connect() as conn:
            actual=dict(conn.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())
        source_message=input_data.get('notification_message_id') or message_id
        if actual['status'] in {'WAITING_HUMAN','FAILED_FINAL'} and input_data.get("chat_id") and source_message.startswith('om_'):
            from v3_task_control import task_summary
            enqueue_outbox(object_type="task_notification", object_id=task_id, destination="feishu_chat", payload={"task_id": task_id, "chat_id": input_data["chat_id"], "source_message_id": source_message, "text": task_summary(actual)+"\n原任务及已有证据保留，无需重发商品。", "idempotency_key": hashlib.sha256(f"{task_id}:{error_type}:{actual['attempts']}".encode()).hexdigest()[:32], "profile_id": PROFILE_ID})
        return {"task_id": task_id, "status": actual['status'], "error_type": error_type}
    result_path = Path(str(payload.get("output_dir") or "")) / "result.json"
    if not result_path.is_file():
        release_task(task_id, status="FAILED_FINAL", runtime_state="FAILED", error_type="RESULT_MISSING", error_message="扫描完成但result.json不存在", resume_from="LOCAL_COMMIT")
        return {"task_id": task_id, "status": "FAILED_FINAL", "error_type": "RESULT_MISSING"}
    try:
        imported = import_result(result_path, task_id=task_id)
    except ValueError as exc:
        release_task(task_id,status="WAITING_IDENTITY",runtime_state="WAITING_TOOL",error_type="IDENTITY_OR_EVIDENCE_NOT_VERIFIED",error_message=str(exc),resume_from="IDENTITY_CLOSURE")
        return {"task_id":task_id,"status":"WAITING_IDENTITY","error_type":str(exc)}
    return queue_scan_delivery(task_id,imported,result_path,input_data)


def process_outbox_once() -> dict | None:
    item = claim_outbox(f"{SERVICE}:{os.getpid()}")
    if not item:
        return None
    body = json.loads(item["payload_json"] or "{}")
    if item["object_type"] in {"ingress_ack", "task_notification"}:
        command = [
            str(load_config()["lark_cli"]), "im", "+messages-reply",
            "--message-id", str(body.get("source_message_id") or ""),
            "--text", str(body.get("text") or ""),
            "--idempotency-key", str(body.get("idempotency_key") or item["outbox_id"]),
            "--as", "bot", "--format", "json",
        ]
        delivery_env = dict(os.environ)
        delivery_env["PATH"] = "/opt/homebrew/bin:/Users/mac/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        delivery_env["NO_PROXY"] = "open.feishu.cn,.feishu.cn,localhost,127.0.0.1"
        delivery_env["no_proxy"] = delivery_env["NO_PROXY"]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False, env=delivery_env)
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = {"ok": False, "error": (proc.stderr or proc.stdout)[-1000:]}
        if proc.returncode == 0 and payload.get("ok") is not False:
            complete_outbox(item["outbox_id"], {"status": "VERIFIED", "profile_id": PROFILE_ID, "delivery": payload})
            return {"outbox_id": item["outbox_id"], "status": "SENT"}
        from v3_runtime import retry_outbox
        error = payload.get("error") or {}
        retry_outbox(item["outbox_id"], error_type="FEISHU_ACK_FAILED", error_message=str(error.get("message") if isinstance(error, dict) else error), retry_after_seconds=60)
        return {"outbox_id": item["outbox_id"], "status": "RETRY", "error_type": "FEISHU_ACK_FAILED"}
    if item["object_type"] == "monitor_status":
        projector = Path(__file__).resolve().parent / "v3_project_feishu.py"
        proc = subprocess.run([str(PYTHON), str(projector), "--monitor-id", str(body.get("monitor_id") or "")], capture_output=True, text=True, timeout=180, check=False)
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = {"status": "DELIVERY_FAILED", "error_type": "INVALID_MONITOR_PROJECTION", "error_message": (proc.stderr or proc.stdout)[-1000:]}
        if payload.get("status") == "VERIFIED":
            complete_outbox(item["outbox_id"], {"status": "VERIFIED", "profile_id": PROFILE_ID, "projection": payload})
            return {"outbox_id": item["outbox_id"], "status": "SENT"}
        from v3_runtime import retry_outbox
        retry_outbox(item["outbox_id"], error_type=str(payload.get("error_type") or "MONITOR_PROJECTION_FAILED"), error_message=str(payload.get("error_message") or "monitor status projection failed"), retry_after_seconds=60)
        return {"outbox_id": item["outbox_id"], "status": "RETRY"}
    if item["object_type"] in {"semantic_projection", "evidence_projection", "session_projection"}:
        projector = Path(__file__).resolve().parent / "v3_project_feishu.py"
        projector_args = [str(PYTHON), str(projector)]
        if item["object_type"] == "session_projection":
            projector_args.extend(["--session-id", str(body.get("session_id") or item["object_id"])])
        else:
            projector_args.extend(["--analysis-id", str(body.get("analysis_id") or item["object_id"])])
        proc = subprocess.run(projector_args, capture_output=True, text=True, timeout=900, check=False)
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload = {"status": "DELIVERY_FAILED", "error_type": "INVALID_SEMANTIC_PROJECTION", "error_message": (proc.stderr or proc.stdout)[-1000:]}
        if payload.get("status") == "VERIFIED":
            complete_outbox(item["outbox_id"], {"status": "VERIFIED", "profile_id": PROFILE_ID, "delivery": payload})
            return {"outbox_id": item["outbox_id"], "status": "SENT"}
        from v3_runtime import retry_outbox
        retry_outbox(item["outbox_id"], error_type=str(payload.get("error_type") or "SEMANTIC_PROJECTION_FAILED"), error_message=str(payload.get("error_message") or "semantic projection failed"), retry_after_seconds=120)
        return {"outbox_id": item["outbox_id"], "status": "RETRY"}
    result_path = Path(str(body.get("result_path") or ""))
    if not result_path.is_file():
        from v3_runtime import retry_outbox
        retry_outbox(item["outbox_id"], error_type="RESULT_MISSING", error_message="outbox结果文件不存在", retry_after_seconds=300)
        return {"outbox_id": item["outbox_id"], "status": "RETRY"}
    projector = Path(__file__).resolve().parent / "v3_project_feishu.py"
    projector_command = [str(PYTHON), str(projector)]
    if body.get('product_id'): projector_command.extend(['--product-id',str(body['product_id'])])
    proc = subprocess.run(projector_command, capture_output=True, text=True, timeout=900, check=False)
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        payload = {"status": "DELIVERY_FAILED", "error_type": "INVALID_DELIVERY_OUTPUT", "error_message": (proc.stderr or proc.stdout)[-1000:]}
    if payload.get("status") == "VERIFIED":
        complete_outbox(item["outbox_id"], {"status": "VERIFIED", "profile_id": PROFILE_ID, "delivery": payload.get("delivery") or {}, "table": payload.get("table") or {}})
        with connect() as conn:
            if body.get('task_id'):
                conn.execute("UPDATE tasks SET status='COMPLETE',business_state='COMPLETE',runtime_state='IDLE',delivery_state='VERIFIED',error_type=NULL,error_message=NULL,next_attempt_at=NULL,last_success_at=?,updated_at=? WHERE task_id=? AND status='DELIVERY_PENDING'", (utc_now(),utc_now(),body['task_id']))
                conn.execute("UPDATE dead_letters SET resolved_at=?,resolution_json=? WHERE source_type='task' AND source_id=? AND resolved_at IS NULL",(utc_now(),json.dumps({'resolution':'original_task_completed','scan_id':body.get('scan_id')}),body['task_id']))
                task_row=conn.execute('SELECT input_json FROM tasks WHERE task_id=?',(body['task_id'],)).fetchone()
            else:
                task_row=None
            conn.commit()
        if task_row:
            source=json.loads(task_row['input_json'])
            notification_message=source.get('notification_message_id') or source.get('message_id') or ''
            if source.get('chat_id') and notification_message.startswith('om_'):
                enqueue_outbox(object_type='task_notification',object_id=body['task_id'],destination='feishu_chat',payload={'task_id':body['task_id'],'chat_id':source['chat_id'],'source_message_id':notification_message,'text':f"任务 {body['task_id']} 已完成商品扫描、同行身份建档和飞书交付。扫描版本：{body.get('scan_id')}。监控中的开播状态以实时探针为准。",'idempotency_key':hashlib.sha256(f"{body['task_id']}:complete".encode()).hexdigest()[:32],'profile_id':PROFILE_ID})
        return {"outbox_id": item["outbox_id"], "status": "SENT"}
    from v3_runtime import retry_outbox
    retry_outbox(item["outbox_id"], error_type=str(payload.get("error_type") or "DELIVERY_FAILED"), error_message=str(payload.get("error_message") or "飞书交付未完成"), retry_after_seconds=min(3600, 30 * (int(item.get("attempts") or 0) + 1)))
    return {"outbox_id": item["outbox_id"], "status": "RETRY", "error_type": payload.get("error_type")}


def schedule_product_rescans() -> int:
    config = load_config()
    interval = int(config.get("product_rescan_interval_seconds", 21600))
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(seconds=interval)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    bucket = int(now.timestamp()) // interval
    created = 0
    with connect() as conn:
        products = conn.execute("SELECT * FROM products WHERE status='ACTIVE' AND last_seen_at<=?", (cutoff,)).fetchall()
        for product in products:
            # Keep one outstanding refresh per product, including a blocked one.
            if conn.execute("SELECT 1 FROM tasks t WHERE t.product_id=? AND t.task_type='product_rescan' AND t.status NOT IN ('COMPLETE','SUPERSEDED') AND (t.status IN ('RUNNING','RECEIVED','RETRY_WAIT','DELIVERY_PENDING') OR NOT EXISTS (SELECT 1 FROM tasks done WHERE done.product_id=t.product_id AND done.status='COMPLETE' AND done.last_success_at>=t.started_at)) LIMIT 1",(product['product_id'],)).fetchone():continue
            dedupe = f"scheduled_scan:{product['product_id']}:{bucket}"
            task_id = "task_" + hashlib.sha256(dedupe.encode()).hexdigest()[:24]
            payload = {"message_id": f"schedule:{bucket}:{hashlib.sha256(product['product_id'].encode()).hexdigest()[:12]}", "content": product["source_url"], "scheduled": True, "product_id": product["product_id"]}
            from v3_task_control import product_scope
            scope=product_scope(conn,product['product_id'])
            if scope.get('sender_id') in config.get('allowed_sender_ids',[]) and str(scope.get('message_id') or '').startswith('om_'):
                payload.update(chat_id=scope['chat_id'],sender_id=scope['sender_id'],notification_message_id=scope['message_id'])
            cursor = conn.execute("INSERT OR IGNORE INTO tasks(task_id,task_type,dedupe_key,status,business_state,runtime_state,delivery_state,product_id,input_json,current_step,started_at,updated_at) VALUES(?,?,?,'RECEIVED','RECEIVED','IDLE','NOT_STARTED',?,?, 'SCHEDULED_SCAN',?,?)", (task_id, "product_rescan", dedupe, product["product_id"], json.dumps(payload, ensure_ascii=False, sort_keys=True), utc_now(), utc_now()))
            created += cursor.rowcount
        conn.commit()
    return created


def task_loop() -> None:
    while not STOP_EVENT.is_set():
        try:
            schedule_product_rescans()
            result = process_task_once()
            if not result:
                STOP_EVENT.wait(2)
        except Exception as exc:  # noqa: BLE001
            heartbeat("DEGRADED", {"component": "task_loop", "error": f"{exc.__class__.__name__}: {exc}"})
            STOP_EVENT.wait(5)


def outbox_loop() -> None:
    while not STOP_EVENT.is_set():
        try:
            processed = process_outbox_once()
            if not processed:
                STOP_EVENT.wait(2)
        except Exception as exc:  # noqa: BLE001
            heartbeat("DEGRADED", {"component": "outbox_loop", "error": f"{exc.__class__.__name__}: {exc}"})
            STOP_EVENT.wait(5)


def daemon(interval: int, start_recordings: bool) -> int:
    identity_assertion(verify_cli=True)
    init_db()
    if not active():
        return emit({"status": "WAITING_CONFIGURATION", "reason": "V3 atomic activation is not ACTIVE", "activation": activation_readiness()}, 1)
    heartbeat("STARTING", {"full_fleet": True})
    signal.signal(signal.SIGTERM, lambda *_: STOP_EVENT.set())
    signal.signal(signal.SIGINT, lambda *_: STOP_EVENT.set())
    threads = [threading.Thread(target=task_loop, name="runtime-v3-task-loop", daemon=True), threading.Thread(target=outbox_loop, name="runtime-v3-outbox-loop", daemon=True)]
    for thread in threads:
        thread.start()
    try:
        while not STOP_EVENT.is_set():
            payload = monitor_once(start_recordings=start_recordings)
            if payload.get("status") not in {"COMPLETE"}:
                heartbeat("WAITING_CONFIGURATION", payload)
                return emit(payload, 1)
            STOP_EVENT.wait(max(2, interval))
    except KeyboardInterrupt:
        STOP_EVENT.set()
    for thread in threads:
        thread.join(timeout=5)
    heartbeat("STOPPED", {"reason": "signal"})
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "self-check", "activate", "monitor-once", "daemon"))
    parser.add_argument("--start-recordings", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()
    if args.command == "init":
        init_db()
        return emit({"ok": True, "status": "READY", "schema_version": 3})
    if args.command == "status":
        return emit(status_snapshot())
    if args.command == "self-check":
        return emit({"status": "READY", "identity": identity_assertion(verify_cli=True), "activation": activation_readiness(), "status_snapshot": status_snapshot()})
    if args.command == "activate":
        return activate()
    if args.command == "monitor-once":
        return emit(monitor_once(start_recordings=args.start_recordings), 0)
    return daemon(args.interval, args.start_recordings)


if __name__ == "__main__":
    raise SystemExit(main())
