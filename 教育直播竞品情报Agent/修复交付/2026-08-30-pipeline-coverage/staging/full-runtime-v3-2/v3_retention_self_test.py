#!/usr/bin/env python3
"""Exercise the 72h/168h retention policies against explicit temp assets."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_retention_worker import once  # noqa: E402
from v3_runtime import connect, init_db, utc_now  # noqa: E402


def main() -> int:
    init_db()
    base = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/retention-self-test")
    base.mkdir(parents=True, exist_ok=True)
    video = base / "video.ts"
    audio = base / "audio.opus"
    video.write_bytes(b"retention-video-test")
    audio.write_bytes(b"retention-audio-test")
    with connect() as conn:
        session = conn.execute("SELECT s.session_id FROM live_sessions s JOIN transcripts t ON t.session_id=s.session_id AND t.status='COMPLETE' WHERE s.status='MEDIA_COMPLETE' LIMIT 1").fetchone()
        if not session:
            raise RuntimeError("retention self-test requires one completed session and transcript")
        session_id = session[0]
        now = utc_now()
        for object_type, path, policy in (("VIDEO", video, "SELFTEST_VIDEO_72H"), ("AUDIO", audio, "SELFTEST_AUDIO_168H")):
            conn.execute("INSERT INTO retention_jobs(retention_job_id,object_type,object_id,policy_name,status,not_before,created_at,updated_at,payload_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,policy_name) DO UPDATE SET status='PENDING',not_before=excluded.not_before,payload_json=excluded.payload_json", (f"retention:selftest:{object_type.lower()}", object_type, session_id, policy, "PENDING", now, now, now, json.dumps({"session_id": session_id, "path": str(path)}, ensure_ascii=False)))
        conn.commit()
    worker_result = once(heartbeat_service="retention-self-test")
    with connect() as conn:
        statuses = {row["policy_name"]: row["status"] for row in conn.execute("SELECT policy_name,status FROM retention_jobs WHERE policy_name LIKE 'SELFTEST_%'")}
        # The test has its own transient heartbeat so it cannot overwrite the
        # production retention worker's PID.  Remove that diagnostic row after
        # the result has been captured; the JSON/fault-drill artifact remains
        # the durable audit record.
        conn.execute("DELETE FROM heartbeats WHERE service_name='retention-self-test'")
        conn.commit()
    report = {"status": "PASS" if statuses.get("SELFTEST_VIDEO_72H") == "DELETED" and statuses.get("SELFTEST_AUDIO_168H") == "DELETED" and not video.exists() and not audio.exists() else "FAIL", "statuses": statuses, "files_deleted": {"video": not video.exists(), "audio": not audio.exists()}, "worker_result": worker_result, "checked_at": utc_now()}
    out = base / "retention-self-test.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with connect() as conn:
        conn.execute("INSERT INTO fault_drill_runs(drill_id,drill_type,status,evidence_path,evidence_hash,metrics_json,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(drill_id) DO UPDATE SET status=excluded.status,evidence_path=excluded.evidence_path,evidence_hash=excluded.evidence_hash,metrics_json=excluded.metrics_json,ended_at=excluded.ended_at", ("drill:retention-policies", "video_72h_audio_168h_auto_delete", report["status"], str(out), hashlib.sha256(out.read_bytes()).hexdigest(), json.dumps(report, ensure_ascii=False), report["checked_at"], report["checked_at"]))
        conn.commit()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
