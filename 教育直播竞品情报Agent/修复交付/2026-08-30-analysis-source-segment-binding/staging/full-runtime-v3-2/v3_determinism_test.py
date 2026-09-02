#!/usr/bin/env python3
"""Durable idempotency proof for duplicate Feishu messages and outbox events."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import connect, enqueue_outbox, ingest_message, init_db, utc_now  # noqa: E402


def main() -> int:
    init_db()
    message_id = "selftest:duplicate-message-100"
    responses = [ingest_message(message_id=message_id, chat_id="selftest-chat", sender_id="ou_e2bb6eeeda749177d2b1191664831934", content="扫描商品 selftest") for _ in range(100)]
    with connect() as conn:
        inbox_count = conn.execute("SELECT count(*) FROM inbox_messages WHERE message_id=?", (message_id,)).fetchone()[0]
        task_count = conn.execute("SELECT count(*) FROM tasks WHERE dedupe_key=?", ("feishu:" + message_id,)).fetchone()[0]
        task_id = responses[0].get("task_id")
        # The live task loop may claim the self-test task while the 100
        # duplicate ingests are running.  Remove durable child rows first so
        # PostgreSQL's foreign keys make the cleanup deterministic.
        conn.execute("DELETE FROM task_leases WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM task_attempts WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM checkpoints WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM domain_events WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM inbox_messages WHERE message_id=?", (message_id,))
        conn.execute("DELETE FROM tasks WHERE task_id=?", (task_id,))
        conn.commit()
    outbox_ids = [enqueue_outbox(object_type="selftest", object_id="duplicate-outbox-100", destination="selftest", payload={"same": True}) for _ in range(100)]
    with connect() as conn:
        outbox_count = conn.execute("SELECT count(*) FROM outbox WHERE object_type='selftest' AND object_id='duplicate-outbox-100'").fetchone()[0]
        conn.execute("DELETE FROM outbox WHERE object_type='selftest' AND object_id='duplicate-outbox-100'")
        conn.commit()
    report = {"checked_at": utc_now(), "message_deliveries": 100, "inbox_rows": inbox_count, "task_rows": task_count, "same_task_id": len({item.get("task_id") for item in responses}) == 1, "outbox_deliveries": 100, "outbox_rows": outbox_count, "same_outbox_id": len(set(outbox_ids)) == 1}
    report["status"] = "PASS" if report["inbox_rows"] == report["task_rows"] == 1 and report["same_task_id"] and report["outbox_rows"] == 1 and report["same_outbox_id"] else "FAIL"
    path = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/determinism-100.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    with connect() as conn:
        conn.execute("INSERT INTO fault_drill_runs(drill_id,drill_type,status,evidence_path,evidence_hash,metrics_json,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(drill_id) DO UPDATE SET status=excluded.status,evidence_path=excluded.evidence_path,evidence_hash=excluded.evidence_hash,metrics_json=excluded.metrics_json,ended_at=excluded.ended_at", ("drill:duplicate-message-100", "duplicate_message_and_outbox_100", report["status"], str(path), digest, json.dumps(report, ensure_ascii=False), report["checked_at"], report["checked_at"]))
        conn.commit()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
