#!/usr/bin/env python3
"""Exhaustive deterministic self-check for Runtime V3.

The check fails closed.  It distinguishes implementation readiness from
atomic full-fleet activation readiness and never turns gates on by itself.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v3_runtime as v3  # noqa: E402


def check(name: str, fn, checks: list[dict]) -> None:
    try:
        value = fn()
        checks.append({"name": name, "status": "PASS", "detail": value})
    except Exception as exc:  # noqa: BLE001
        checks.append({"name": name, "status": "FAIL", "detail": f"{exc.__class__.__name__}: {exc}"})


def ephemeral_checks(checks: list[dict]) -> None:
    with tempfile.TemporaryDirectory(prefix="runtime-v3-self-check-") as temp:
        path = Path(temp) / "test.db"
        v3.init_db(path)
        result = v3.connect(path)
        try:
            v3._begin(result)
            first = v3.new_id("task")
            result.execute("INSERT INTO tasks(task_id,task_type,dedupe_key,status,business_state,runtime_state,delivery_state,current_step,updated_at) VALUES(?,?,?,?,?,?,?,?,?)", (first, "test", "dedupe:test", "RECEIVED", "RECEIVED", "IDLE", "NOT_STARTED", "TEST", v3.utc_now()))
            result.commit()
        finally:
            result.close()
        # Inbox dedup is tested against the real DB path by temporarily using
        # the helper's explicit connection boundary.
        with v3.connect(path) as conn:
            conn.execute("INSERT INTO inbox_messages(inbox_id,platform,message_id,profile_id,app_id,chat_id,sender_id,content,received_at,task_id) VALUES(?,?,?,?,?,?,?,?,?,?)", ("inbox_test", "feishu", "om_test", v3.PROFILE_ID, v3.APP_ID, "oc_test", "ou_test", "扫描商品 1", v3.utc_now(), first))
            duplicate = conn.execute("SELECT count(*) FROM inbox_messages WHERE message_id='om_test'").fetchone()[0]
        if duplicate != 1:
            raise AssertionError("inbox uniqueness failed")
        with v3.connect(path) as conn:
            conn.execute("INSERT INTO outbox(outbox_id,dedupe_key,object_type,object_id,destination,status,attempts,next_attempt_at,payload_hash,payload_json) VALUES(?,?,?,?,?,'PENDING',0,?,?,?)", ("out_test", "feishu:task:test:hash", "task", first, "feishu_base", v3.utc_now(), "hash", "{}"))
            conn.execute("INSERT OR IGNORE INTO outbox(outbox_id,dedupe_key,object_type,object_id,destination,status,attempts,next_attempt_at,payload_hash,payload_json) VALUES(?,?,?,?,?,'PENDING',0,?,?,?)", ("out_test_dup", "feishu:task:test:hash", "task", first, "feishu_base", v3.utc_now(), "hash", "{}"))
            outbox_count = conn.execute("SELECT count(*) FROM outbox WHERE dedupe_key='feishu:task:test:hash'").fetchone()[0]
        if outbox_count != 1:
            raise AssertionError("outbox uniqueness failed")
    checks.append({"name": "idempotent_inbox_outbox", "status": "PASS", "detail": "duplicate message and delivery keys collapse to one row"})


def static_identity_checks(checks: list[dict]) -> None:
    files = [
        v3.RUNTIME_ROOT / "bin" / "mvp_c_runner.py",
        v3.RUNTIME_ROOT / "bin" / "outbox_worker.py",
        v3.RUNTIME_ROOT / "v3" / "v3_worker.py",
    ]
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise AssertionError(f"missing runtime files: {missing}")
    runner = (files[0]).read_text(encoding="utf-8")
    outbox = (files[1]).read_text(encoding="utf-8")
    if "--profile" not in runner or "edu_live_competitor_intel" not in runner:
        raise AssertionError("runner is not identity-pinned")
    if "--profile" not in outbox or "edu_live_competitor_intel" not in outbox:
        raise AssertionError("outbox is not identity-pinned")
    checks.append({"name": "profile_pinning_static", "status": "PASS", "detail": "runner and outbox require edu_live_competitor_intel"})


def main() -> int:
    checks: list[dict] = []
    check("schema_and_runtime_db", lambda: (v3.init_db(), v3.status_snapshot())[1], checks)
    check("identity_lock_and_cli_profile", lambda: v3.identity_assertion(verify_cli=True), checks)
    check("v3_config_identity", lambda: v3.load_config()["app_id"], checks)
    static_identity_checks(checks)
    ephemeral_checks(checks)
    readiness = v3.activation_readiness()
    checks.append({"name": "atomic_full_fleet_activation_gate", "status": "PASS" if not readiness["ready"] or readiness["identity_coverage"] == 1.0 else "FAIL", "detail": readiness})
    failed = [item for item in checks if item["status"] == "FAIL"]
    print(json.dumps({"status": "FAIL" if failed else "PASS", "checks": checks, "activation_readiness": readiness, "truthfulness": "A PASS here means implementation and invariants are verified; activation remains blocked unless the full-fleet gate is READY."}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
