#!/usr/bin/env python3
"""Automatic, dependency-aware media retention for Runtime V3."""

from __future__ import annotations

import argparse
import json
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from v3_runtime import connect, init_db, load_config, upsert_heartbeat, utc_now

RUNNING = True


def stop(*_args):
    global RUNNING
    RUNNING = False


def iso_after(hours: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def update_blocked(conn, row, reason: str) -> None:
    conn.execute("UPDATE retention_jobs SET status='BLOCKED_DEPENDENCY',next_attempt_at=?,attempts=attempts+1,last_error=?,updated_at=? WHERE retention_job_id=?", (iso_after(1), reason[:1000], utc_now(), row["retention_job_id"]))


def retention_delete_enabled(config: dict) -> bool:
    """Fail closed: only the literal JSON boolean true permits deletion."""
    return ((config.get("retention") or {}).get("delete_enabled") is True)


def once(*, heartbeat_service: str = "retention-v3") -> dict:
    deleted = blocked = failed = 0
    config = load_config()
    retention = config.get("retention") or {}
    video_hours = int(retention.get("video_hours") or 72)
    audio_hours = int(retention.get("audio_hours") or 168)
    delete_enabled = retention_delete_enabled(config)
    if not delete_enabled:
        result = {
            "deleted": 0,
            "blocked_dependencies": 0,
            "failed": 0,
            "checked_at": utc_now(),
            "delete_enabled": False,
            "mode": "DELETE_DISABLED",
            "policies": {"video_hours": video_hours, "audio_hours": audio_hours},
        }
        upsert_heartbeat(heartbeat_service, "READY", result, success=True)
        return result
    init_db()
    now = utc_now()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM retention_jobs WHERE status IN ('PENDING','RETRY','BLOCKED_DEPENDENCY') AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND not_before<=? ORDER BY not_before,retention_job_id", (now, now)).fetchall()
        for row in rows:
            payload = json.loads(row["payload_json"] or "{}")
            path = Path(str(payload.get("path") or ""))
            session_id = str(payload.get("session_id") or row["object_id"] or "")
            if row["object_type"] == "VIDEO":
                session = conn.execute("SELECT status FROM live_sessions WHERE session_id=?", (session_id,)).fetchone()
                transcript = conn.execute("SELECT status,source_path FROM transcripts WHERE session_id=? AND status='COMPLETE' ORDER BY created_at DESC LIMIT 1", (session_id,)).fetchone()
                if not session or session["status"] in {"RECORDING", "DETECTED", "WAITING_STREAM"}:
                    update_blocked(conn, row, "video retention waits for session completion")
                    blocked += 1
                    continue
                if not transcript or not transcript["source_path"] or not Path(str(transcript["source_path"])).is_file():
                    update_blocked(conn, row, "video retention waits for successful audio extraction and transcript")
                    blocked += 1
                    continue
            elif row["object_type"] == "AUDIO":
                transcript = conn.execute("SELECT status FROM transcripts WHERE session_id=? AND status='COMPLETE'", (session_id,)).fetchone()
                pending_review = conn.execute("SELECT count(*) FROM review_items WHERE object_id=? AND status='PENDING'", (session_id,)).fetchone()[0]
                if not transcript:
                    update_blocked(conn, row, "audio retention waits for complete transcript")
                    blocked += 1
                    continue
                if pending_review:
                    update_blocked(conn, row, "audio retained while review is pending")
                    blocked += 1
                    continue
            else:
                conn.execute("UPDATE retention_jobs SET status='FAILED',last_error='unknown retention object type',updated_at=? WHERE retention_job_id=?", (utc_now(), row["retention_job_id"]))
                failed += 1
                continue
            if not path.is_file():
                conn.execute("UPDATE retention_jobs SET status='DELETED',last_error='already absent; verified idempotently',updated_at=? WHERE retention_job_id=?", (utc_now(), row["retention_job_id"]))
                deleted += 1
                continue
            try:
                path.unlink()
                conn.execute("UPDATE retention_jobs SET status='DELETED',next_attempt_at=NULL,last_error=NULL,updated_at=?,payload_json=? WHERE retention_job_id=?", (utc_now(), json.dumps({**payload, "deleted_at": utc_now()}, ensure_ascii=False), row["retention_job_id"]))
                deleted += 1
            except OSError as exc:
                conn.execute("UPDATE retention_jobs SET status='RETRY',next_attempt_at=?,attempts=attempts+1,last_error=?,updated_at=? WHERE retention_job_id=?", (iso_after(1), str(exc)[:1000], utc_now(), row["retention_job_id"]))
                failed += 1
        conn.commit()
    result = {"deleted": deleted, "blocked_dependencies": blocked, "failed": failed, "checked_at": utc_now(), "delete_enabled": True, "mode": "DELETE_ENABLED", "policies": {"video_hours": video_hours, "audio_hours": audio_hours}}
    upsert_heartbeat(heartbeat_service, "READY" if failed == 0 else "DEGRADED", result, success=failed == 0)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once", "daemon"))
    parser.add_argument("--interval", type=int, default=3600)
    args = parser.parse_args()
    if args.command == "once":
        print(json.dumps(once(), ensure_ascii=False, indent=2))
        return 0
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    next_scan = 0.0
    while RUNNING:
        now_mono = time.monotonic()
        if now_mono >= next_scan:
            once()
            next_scan = time.monotonic() + max(60, args.interval)
        else:
            # Keep the service heartbeat fresh between hourly retention scans;
            # otherwise observability would incorrectly report a healthy
            # process as stale for most of the interval.
            delete_enabled = retention_delete_enabled(load_config())
            upsert_heartbeat("retention-v3", "READY", {"idle": True, "next_scan_in_seconds": max(0, int(next_scan - time.monotonic())), "delete_enabled": delete_enabled, "mode": "DELETE_ENABLED" if delete_enabled else "DELETE_DISABLED"}, success=True)
        time.sleep(60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
