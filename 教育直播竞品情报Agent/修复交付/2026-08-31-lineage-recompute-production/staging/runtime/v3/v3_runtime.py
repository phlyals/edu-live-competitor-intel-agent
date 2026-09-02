#!/usr/bin/env python3
"""Runtime V3 durable control plane.

This module is intentionally deterministic.  It owns task state, inbox/outbox
deduplication, leases, checkpoints, evidence lineage, and the monitor target
registry.  It never asks an LLM to decide a state transition.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import socket
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from v3_db import connect as connect_postgres


PROFILE_ROOT = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel")
RUNTIME_ROOT = PROFILE_ROOT / "runtime"
V3_ROOT = RUNTIME_ROOT / "v3"
DB_PATH = RUNTIME_ROOT / "runtime_v3.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "v3_schema.sql"
CONFIG_PATH = V3_ROOT / "v3_config.json"
PROFILE_ID = "edu_live_competitor_intel"
BOT_NAME = "直播竞品情报主管"
APP_ID = "cli_a978a6e73f785cc5"
PLATFORM = "feishu"
TASK_MAX_ATTEMPTS = 5
OUTBOX_MAX_ATTEMPTS = 8


def _configured_backend() -> str:
    override = os.environ.get("V3_CONTROL_PLANE_BACKEND")
    if override:
        return override.strip().lower()
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return str(config.get("control_plane_backend") or "sqlite_wal").strip().lower()
    except (OSError, json.JSONDecodeError):
        return "sqlite_wal"


def _postgres_dsn() -> str:
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        dsn = str((config.get("postgresql") or {}).get("dsn") or "").strip()
    except (OSError, json.JSONDecodeError):
        dsn = ""
    if not dsn:
        raise RuntimeError("PostgreSQL backend is enabled but postgresql.dsn is missing")
    return dsn


class IdentityConflictError(RuntimeError):
    """Raised when a stable platform identity is already owned elsewhere."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    body = value if isinstance(value, str) else json_text(value)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def worker_process_alive(worker_id: str | None) -> bool:
    """Return whether a local PID-bearing worker lease still has an owner."""
    match = re.search(r":(\d+)$", str(worker_id or ""))
    if not match:
        return True
    try:
        os.kill(int(match.group(1)), 0)
        return True
    except (OSError, ValueError):
        return False


def connect(path: Path = DB_PATH):
    # An explicit alternate path is used only by the migration and audit tools.
    # All normal Runtime V3 callers use the configured authoritative backend.
    if _configured_backend() == "postgresql" and path == DB_PATH:
        return connect_postgres(_postgres_dsn())
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not isinstance(conn, sqlite3.Connection):
        rows = conn.execute(
            "SELECT column_name AS name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=?",
            (table,),
        ).fetchall()
        return {str(row["name"]) for row in rows}
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _table_exists(conn, table: str) -> bool:
    if isinstance(conn, sqlite3.Connection):
        return bool(conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0])
    return bool(conn.execute("SELECT count(*) AS n FROM information_schema.tables WHERE table_schema='public' AND table_name=?", (table,)).fetchone()[0])


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, declaration: str) -> None:
    if name not in _table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


def init_db(path: Path = DB_PATH) -> None:
    with connect(path) as conn:
        is_postgres = not isinstance(conn, sqlite3.Connection)
        if is_postgres:
            conn.execute("SELECT pg_advisory_lock(hashtext('edu_live_competitor_intel_v3_schema'))")
        try:
            # PostgreSQL and SQLite share the logical schema, while v3_db translates
            # the small SQLite compatibility surface when the final backend is PG.
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            # Runtime V3 existed before these durable retry columns were introduced.
            # CREATE TABLE IF NOT EXISTS cannot evolve an existing table, so keep the
            # migration explicit and idempotent instead of falsely bumping metadata.
            _ensure_column(conn, "tasks", "attempts", "INTEGER NOT NULL DEFAULT 0")
            _ensure_column(conn, "tasks", "max_attempts", f"INTEGER NOT NULL DEFAULT {TASK_MAX_ATTEMPTS}")
            _ensure_column(conn, "tasks", "next_attempt_at", "TEXT")
            _ensure_column(conn, "outbox", "max_attempts", f"INTEGER NOT NULL DEFAULT {OUTBOX_MAX_ATTEMPTS}")
            _ensure_column(conn, "transcripts", "language", "TEXT")
            # Fail closed on upgraded databases.  The explicit lifecycle
            # migration classifies legacy rows before the pipeline is resumed.
            _ensure_column(conn, "recording_segments", "lifecycle_status", "TEXT NOT NULL DEFAULT 'UNCLASSIFIED'")
            _ensure_column(conn, "recording_segments", "superseded_by_segment_id", "TEXT REFERENCES recording_segments(segment_id)")
            _ensure_column(conn, "recording_segments", "lifecycle_updated_at", "TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_recording_segments_lifecycle ON recording_segments(lifecycle_status,session_id)")
            conn.execute("UPDATE tasks SET next_attempt_at=COALESCE(next_attempt_at,updated_at) WHERE status IN ('RECEIVED','RETRY_WAIT')")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(status,next_attempt_at,lease_until)")
            conn.execute("INSERT INTO schema_meta(key,value) VALUES('control_plane_revision','3.1') ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')")
            version = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            if not version or version[0] != "3":
                raise RuntimeError("Runtime V3 schema version is not 3")
        except Exception:
            # PostgreSQL aborts a transaction after a failed DDL/DML statement.
            # Roll back before issuing pg_advisory_unlock; otherwise cleanup
            # raises InFailedSqlTransaction and obscures the real startup error.
            if is_postgres:
                conn.rollback()
            raise
        finally:
            if is_postgres:
                try:
                    conn.execute("SELECT pg_advisory_unlock(hashtext('edu_live_competitor_intel_v3_schema'))")
                    conn.commit()
                except Exception:
                    # close() in the context manager still releases a session
                    # level advisory lock if cleanup itself is interrupted.
                    conn.rollback()


def load_config() -> dict:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    required = {
        "profile_id": PROFILE_ID,
        "bot_name": BOT_NAME,
        "app_id": APP_ID,
    }
    for key, expected in required.items():
        if config.get(key) != expected:
            raise RuntimeError(f"identity assertion failed: {key}")
    return config


def identity_assertion(*, verify_cli: bool = False) -> dict:
    config = load_config()
    result = {
        "profile_id": config["profile_id"],
        "bot_name": config["bot_name"],
        "app_id": config["app_id"],
        "cli_profile_required": config["lark_cli_profile"],
        "identity_locked": True,
    }
    if verify_cli:
        cli = Path(config["lark_cli"])
        if not cli.is_file():
            raise RuntimeError("profile-scoped lark-cli is missing")
        command = [str(cli), "profile", "list"]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False, env={"HOME": "/Users/mac", "PATH": "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin", "LANG": "C"})
        if proc.returncode != 0 or PROFILE_ID not in proc.stdout:
            raise RuntimeError("profile-scoped lark-cli cannot see the locked profile")
        result["cli_profile_verified"] = True
    return result


def _begin(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")


def record_event(conn: sqlite3.Connection, event_type: str, *, task_id: str | None = None, object_type: str | None = None, object_id: str | None = None, payload: Any = None, severity: str = "INFO") -> str:
    event_id = new_id("evt")
    conn.execute("INSERT INTO domain_events(event_id,task_id,event_type,severity,created_at,object_type,object_id,payload_json) VALUES(?,?,?,?,?,?,?,?)", (event_id, task_id, event_type, severity, utc_now(), object_type, object_id, json_text(payload or {})))
    return event_id


def upsert_heartbeat(service_name: str, status: str, details: dict | None = None, *, success: bool = True) -> None:
    now = utc_now()
    node_id = os.environ.get("V3_NODE_ID") or socket.gethostname()
    with connect() as conn:
        conn.execute(
            "INSERT INTO heartbeats(service_name,pid,status,started_at,last_heartbeat_at,last_success_at,details_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(service_name) DO UPDATE SET pid=excluded.pid,status=excluded.status,last_heartbeat_at=excluded.last_heartbeat_at,last_success_at=CASE WHEN excluded.last_success_at IS NOT NULL THEN excluded.last_success_at ELSE heartbeats.last_success_at END,details_json=excluded.details_json",
            (service_name, os.getpid(), status, now, now, now if success else None, json_text(details or {})),
        )
        conn.execute(
            "INSERT INTO worker_nodes(node_id,node_role,hostname,status,last_heartbeat_at,metadata_json) VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_at=excluded.last_heartbeat_at,metadata_json=excluded.metadata_json",
            (node_id, os.environ.get("V3_NODE_ROLE") or "primary", socket.gethostname(), status, now, json_text({"service_name": service_name, "details": details or {}})),
        )
        conn.commit()


def ingest_message(*, message_id: str, chat_id: str, sender_id: str, content: str, parsed: dict | None = None) -> dict:
    identity_assertion()
    if not message_id or not chat_id or not sender_id:
        raise ValueError("message_id, chat_id and sender_id are required")
    inbox_id = new_id("inbox")
    dedupe_key = f"{PLATFORM}:{message_id}"
    task_id = new_id("task")
    now = utc_now()
    parsed = parsed or {}
    with connect() as conn:
        _begin(conn)
        prior = conn.execute("SELECT * FROM inbox_messages WHERE platform=? AND message_id=?", (PLATFORM, message_id)).fetchone()
        if prior:
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (prior["task_id"],)).fetchone() if prior["task_id"] else None
            conn.commit()
            return {"created": False, "inbox_id": prior["inbox_id"], "task_id": task["task_id"] if task else None, "status": task["status"] if task else "DUPLICATE"}
        inserted = conn.execute("INSERT INTO inbox_messages(inbox_id,platform,message_id,profile_id,app_id,chat_id,sender_id,content,received_at,parsed_json,task_id) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(platform,message_id) DO NOTHING", (inbox_id, PLATFORM, message_id, PROFILE_ID, APP_ID, chat_id, sender_id, content, now, json_text(parsed), task_id))
        if inserted.rowcount == 0:
            prior = conn.execute("SELECT inbox_id,task_id FROM inbox_messages WHERE platform=? AND message_id=?", (PLATFORM, message_id)).fetchone()
            conn.commit()
            return {"created": False, "inbox_id": prior["inbox_id"], "task_id": prior["task_id"], "status": "DUPLICATE"}
        conn.execute("INSERT INTO tasks(task_id,task_type,dedupe_key,status,business_state,runtime_state,delivery_state,product_id,input_json,current_step,started_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (task_id, "feishu_command", dedupe_key, "RECEIVED", "RECEIVED", "IDLE", "NOT_STARTED", None, json_text({"message_id": message_id, "chat_id": chat_id, "sender_id": sender_id, "content": content, "parsed": parsed}), "INGRESS", now, now))
        record_event(conn, "TASK_RECEIVED", task_id=task_id, object_type="task", object_id=task_id, payload={"message_id": message_id})
        checkpoint(conn, task_id, "TASK_RECEIVED", "CURRENT", {"message_id": message_id})
        ack_text = f"已接收扫描任务，任务ID：{task_id}。Runtime V3 正在处理，结果会回写到业务工作台。"
        ack_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"runtime-v3-ack:{message_id}"))
        ack_id = enqueue_outbox_conn(conn, object_type="ingress_ack", object_id=task_id, destination="feishu_chat", payload={"task_id": task_id, "chat_id": chat_id, "source_message_id": message_id, "text": ack_text, "idempotency_key": ack_uuid, "profile_id": PROFILE_ID})
        ack_until = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn.execute("UPDATE outbox SET status='IN_FLIGHT',lease_owner=?,lease_until=?,attempts=1,last_attempt_at=? WHERE outbox_id=?", (f"gateway:{os.getpid()}", ack_until, now, ack_id))
        conn.commit()
    return {"created": True, "inbox_id": inbox_id, "task_id": task_id, "status": "RECEIVED", "ack_outbox_id": ack_id, "ack_text": ack_text, "ack_uuid": ack_uuid}


def checkpoint(conn: sqlite3.Connection, task_id: str, checkpoint_type: str, state: str, payload: Any, source_event_id: str | None = None) -> str:
    checkpoint_id = new_id("ckpt")
    conn.execute("INSERT INTO checkpoints(checkpoint_id,task_id,checkpoint_type,state,created_at,source_event_id,payload_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(task_id,checkpoint_type) DO UPDATE SET checkpoint_id=excluded.checkpoint_id,state=excluded.state,created_at=excluded.created_at,source_event_id=excluded.source_event_id,payload_json=excluded.payload_json", (checkpoint_id, task_id, checkpoint_type, state, utc_now(), source_event_id, json_text(payload)))
    return checkpoint_id


def _dead_letter_conn(
    conn: sqlite3.Connection,
    *,
    source_type: str,
    source_id: str,
    reason_type: str,
    reason: str | None,
    payload: Any,
) -> str:
    dead_letter_id = new_id("dead")
    conn.execute(
        "INSERT INTO dead_letters(dead_letter_id,source_type,source_id,reason_type,reason,payload_json,created_at) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_type,source_id) DO UPDATE SET "
        "reason_type=excluded.reason_type,reason=excluded.reason,payload_json=excluded.payload_json,"
        "resolved_at=NULL,resolution_json='{}'",
        (dead_letter_id, source_type, source_id, reason_type, reason, json_text(payload or {}), utc_now()),
    )
    row = conn.execute(
        "SELECT dead_letter_id FROM dead_letters WHERE source_type=? AND source_id=?",
        (source_type, source_id),
    ).fetchone()
    return str(row[0])


def _dead_letter_exhausted_tasks(conn: sqlite3.Connection, now_text: str) -> None:
    rows = conn.execute(
        "SELECT * FROM tasks WHERE attempts>=max_attempts AND "
        "(status IN ('RECEIVED','RETRY_WAIT') OR (status='RUNNING' AND lease_until<?))",
        (now_text,),
    ).fetchall()
    for row in rows:
        reason_type = str(row["error_type"] or "MAX_ATTEMPTS_EXCEEDED")
        reason = str(row["error_message"] or "task exhausted its retry budget")
        conn.execute(
            "UPDATE tasks SET status='FAILED_FINAL',runtime_state='FAILED',lease_owner=NULL,lease_until=NULL,"
            "next_attempt_at=NULL,error_type=?,error_message=?,updated_at=? WHERE task_id=?",
            (reason_type, reason, now_text, row["task_id"]),
        )
        _dead_letter_conn(
            conn,
            source_type="task",
            source_id=str(row["task_id"]),
            reason_type=reason_type,
            reason=reason,
            payload=dict(row),
        )
        record_event(
            conn,
            "TASK_DEAD_LETTERED",
            task_id=str(row["task_id"]),
            object_type="task",
            object_id=str(row["task_id"]),
            payload={"attempts": row["attempts"], "max_attempts": row["max_attempts"]},
            severity="ERROR",
        )


def claim_task(worker_id: str, *, lease_seconds: int = 1800) -> dict | None:
    now = datetime.now(timezone.utc)
    until = (now + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    now_text = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        _begin(conn)
        # Reconcile lease rows before selecting work.  A worker can die after
        # its task has been persisted, leaving an ACTIVE child lease even when
        # the parent task is already terminal.  Expiring those rows first
        # keeps PostgreSQL foreign keys, dashboards and future cleanup honest.
        active_leases = conn.execute("SELECT lease_id,task_id,worker_id,lease_until FROM task_leases WHERE status='ACTIVE'").fetchall()
        for lease in active_leases:
            if (lease["lease_until"] and lease["lease_until"] < now_text) or not worker_process_alive(lease["worker_id"]):
                conn.execute("UPDATE task_leases SET status='EXPIRED',released_at=? WHERE lease_id=? AND status='ACTIVE'", (now_text, lease["lease_id"]))
                conn.execute("UPDATE tasks SET status='RETRY_WAIT',runtime_state='WAITING',lease_owner=NULL,lease_until=NULL,next_attempt_at=?,updated_at=? WHERE task_id=? AND status='RUNNING'", (now_text, now_text, lease["task_id"]))
        conn.execute("UPDATE tasks SET status='RETRY_WAIT',runtime_state='WAITING',next_attempt_at=?,updated_at=? WHERE status='RUNNING' AND (lease_owner IS NULL OR lease_until IS NULL)", (now_text, now_text))
        conn.execute("UPDATE tasks SET lease_owner=NULL,lease_until=NULL WHERE status='RUNNING' AND lease_until<?", (now_text,))
        _dead_letter_exhausted_tasks(conn, now_text)
        row = conn.execute(
            "SELECT task_id FROM tasks WHERE attempts<max_attempts AND ("
            "(status IN ('RECEIVED','RETRY_WAIT') AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
            "AND (lease_until IS NULL OR lease_until<?)) OR "
            "(status='RUNNING' AND lease_until<?)) "
            "ORDER BY COALESCE(next_attempt_at,updated_at),updated_at LIMIT 1",
            (now_text, now_text, now_text),
        ).fetchone()
        if not row:
            conn.commit()
            return None
        conn.execute(
            "UPDATE tasks SET status='RUNNING',runtime_state='RUNNING',lease_owner=?,lease_until=?,"
            "next_attempt_at=NULL,attempts=attempts+1,updated_at=? WHERE task_id=?",
            (worker_id, until, now_text, row["task_id"]),
        )
        claimed = conn.execute("SELECT * FROM tasks WHERE task_id=?", (row["task_id"],)).fetchone()
        if claimed:
            conn.execute("INSERT INTO task_leases(lease_id,task_id,worker_id,fencing_token,acquired_at,lease_until,status) VALUES(?,?,?,?,?,?,?) ON CONFLICT(task_id,fencing_token) DO UPDATE SET worker_id=excluded.worker_id,lease_until=excluded.lease_until,status='ACTIVE'", (new_id("lease"), row["task_id"], worker_id, int(claimed["attempts"]), now_text, until, "ACTIVE"))
        record_event(conn, "TASK_CLAIMED", task_id=row["task_id"], object_type="task", object_id=row["task_id"], payload={"worker_id": worker_id, "lease_until": until, "attempt": claimed["attempts"]})
        conn.commit()
        return dict(claimed) if claimed else None


def renew_task_lease(task_id: str, worker_id: str, *, lease_seconds: int = 1800) -> bool:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    until = (now + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        cursor = conn.execute(
            "UPDATE tasks SET lease_until=?,updated_at=? WHERE task_id=? AND status='RUNNING' "
            "AND lease_owner=? AND lease_until>=?",
            (until, now_text, task_id, worker_id, now_text),
        )
        conn.execute("UPDATE task_leases SET lease_until=? WHERE task_id=? AND worker_id=? AND status='ACTIVE'", (until, task_id, worker_id))
        return cursor.rowcount == 1


def update_task(task_id: str, *, status: str | None = None, business_state: str | None = None, runtime_state: str | None = None, delivery_state: str | None = None, current_step: str | None = None, resume_from: str | None = None, error_type: str | None = None, error_message: str | None = None) -> None:
    fields, values = [], []
    for key, value in (("status", status), ("business_state", business_state), ("runtime_state", runtime_state), ("delivery_state", delivery_state), ("current_step", current_step), ("resume_from", resume_from), ("error_type", error_type), ("error_message", error_message)):
        if value is not None:
            fields.append(f"{key}=?")
            values.append(value)
    if not fields:
        return
    fields.append("updated_at=?")
    values.append(utc_now())
    values.append(task_id)
    with connect() as conn:
        conn.execute(f"UPDATE tasks SET {','.join(fields)} WHERE task_id=?", values)


def release_task(
    task_id: str,
    *,
    status: str,
    runtime_state: str,
    error_type: str | None = None,
    error_message: str | None = None,
    resume_from: str | None = None,
    retry_after_seconds: int | None = None,
    worker_id: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        _begin(conn)
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            conn.rollback()
            raise KeyError(f"task not found: {task_id}")
        if worker_id is not None and row["lease_owner"] != worker_id:
            conn.rollback()
            raise RuntimeError("task lease is no longer owned by this worker")
        final_status = status
        final_runtime_state = runtime_state
        next_attempt_at = None
        if status == "RETRY_WAIT":
            if int(row["attempts"]) >= int(row["max_attempts"]):
                final_status = "FAILED_FINAL"
                final_runtime_state = "FAILED"
            else:
                delay = retry_after_seconds
                if delay is None:
                    delay = min(3600, 30 * (2 ** max(0, int(row["attempts"]) - 1)))
                next_attempt_at = (now + timedelta(seconds=max(0, delay))).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        conn.execute(
            "UPDATE tasks SET status=?,runtime_state=?,resume_from=?,error_type=?,error_message=?,"
            "lease_owner=NULL,lease_until=NULL,next_attempt_at=?,updated_at=? WHERE task_id=?",
            (final_status, final_runtime_state, resume_from, error_type, error_message, next_attempt_at, now_text, task_id),
        )
        conn.execute("UPDATE task_leases SET status='RELEASED',released_at=? WHERE task_id=? AND status='ACTIVE'", (now_text, task_id))
        record_event(
            conn,
            "TASK_RELEASED",
            task_id=task_id,
            object_type="task",
            object_id=task_id,
            payload={"status": final_status, "attempts": row["attempts"], "next_attempt_at": next_attempt_at},
            severity="ERROR" if final_status == "FAILED_FINAL" else "INFO",
        )
        if final_status == "FAILED_FINAL" and status == "RETRY_WAIT":
            _dead_letter_conn(
                conn,
                source_type="task",
                source_id=task_id,
                reason_type=error_type or "MAX_ATTEMPTS_EXCEEDED",
                reason=error_message or "task exhausted its retry budget",
                payload=dict(row),
            )
        conn.commit()


def enqueue_outbox_conn(
    conn: sqlite3.Connection,
    *,
    object_type: str,
    object_id: str,
    destination: str,
    payload: dict,
    max_attempts: int = OUTBOX_MAX_ATTEMPTS,
    scope: str = "UNCLASSIFIED",
    qualification_status: str = "UNCLASSIFIED",
) -> str:
    if max_attempts <= 0:
        raise ValueError("outbox max_attempts must be positive")
    outbox_id = new_id("out")
    payload_hash = digest(payload)
    key = f"{destination}:{object_type}:{object_id}:{payload_hash}"
    conn.execute(
        "INSERT INTO outbox(outbox_id,dedupe_key,object_type,object_id,destination,status,attempts,max_attempts,"
        "next_attempt_at,payload_hash,payload_json,scope,qualification_status) VALUES(?,?,?,?,?,'PENDING',0,?,?,?,?,?,?) "
        "ON CONFLICT(dedupe_key) DO NOTHING",
        (outbox_id, key, object_type, object_id, destination, max_attempts, utc_now(), payload_hash,
         json_text(payload), scope, qualification_status),
    )
    row = conn.execute("SELECT outbox_id FROM outbox WHERE dedupe_key=?", (key,)).fetchone()
    if not row:
        raise RuntimeError("outbox enqueue did not produce a durable row")
    return str(row[0])


def enqueue_outbox(*, object_type: str, object_id: str, destination: str, payload: dict,
                   max_attempts: int = OUTBOX_MAX_ATTEMPTS, scope: str = "UNCLASSIFIED",
                   qualification_status: str = "UNCLASSIFIED") -> str:
    with connect() as conn:
        return enqueue_outbox_conn(
            conn,
            object_type=object_type,
            object_id=object_id,
            destination=destination,
            payload=payload,
            max_attempts=max_attempts,
            scope=scope,
            qualification_status=qualification_status,
        )


def _dead_letter_exhausted_outbox(conn: sqlite3.Connection, now_text: str) -> None:
    rows = conn.execute(
        "SELECT * FROM outbox WHERE attempts>=max_attempts AND ("
        "status IN ('PENDING','RETRY') OR (status='IN_FLIGHT' AND (lease_until IS NULL OR lease_until<?)))",
        (now_text,),
    ).fetchall()
    for row in rows:
        reason_type = str(row["last_error_type"] or "MAX_ATTEMPTS_EXCEEDED")
        reason = str(row["last_error"] or "delivery exhausted its retry budget")
        conn.execute(
            "UPDATE outbox SET status='DEAD_LETTER',lease_owner=NULL,lease_until=NULL,"
            "last_error_type=?,last_error=? WHERE outbox_id=?",
            (reason_type, reason, row["outbox_id"]),
        )
        _dead_letter_conn(
            conn,
            source_type="outbox",
            source_id=str(row["outbox_id"]),
            reason_type=reason_type,
            reason=reason,
            payload=dict(row),
        )


def claim_outbox(worker_id: str, *, lease_seconds: int = 120) -> dict | None:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    until = (now + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        _begin(conn)
        _dead_letter_exhausted_outbox(conn, now_text)
        row = conn.execute(
            "SELECT outbox_id FROM outbox WHERE attempts<max_attempts AND ("
            "(status IN ('PENDING','RETRY') AND next_attempt_at<=? AND (lease_until IS NULL OR lease_until<?)) OR "
            "(status='IN_FLIGHT' AND (lease_until IS NULL OR lease_until<?))) "
            "ORDER BY next_attempt_at LIMIT 1",
            (now_text, now_text, now_text),
        ).fetchone()
        if not row:
            conn.commit()
            return None
        conn.execute(
            "UPDATE outbox SET status='IN_FLIGHT',lease_owner=?,lease_until=?,last_attempt_at=?,"
            "attempts=attempts+1 WHERE outbox_id=?",
            (worker_id, until, now_text, row["outbox_id"]),
        )
        conn.commit()
        value = conn.execute("SELECT * FROM outbox WHERE outbox_id=?", (row["outbox_id"],)).fetchone()
        return dict(value) if value else None


def renew_outbox_lease(outbox_id: str, worker_id: str, *, lease_seconds: int = 120) -> bool:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    until = (now + timedelta(seconds=lease_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        cursor = conn.execute(
            "UPDATE outbox SET lease_until=? WHERE outbox_id=? AND status='IN_FLIGHT' "
            "AND lease_owner=? AND lease_until>=?",
            (until, outbox_id, worker_id, now_text),
        )
        return cursor.rowcount == 1


def complete_outbox(outbox_id: str, receipt: dict) -> None:
    with connect() as conn:
        _begin(conn)
        row = conn.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
        if not row:
            conn.rollback()
            raise KeyError(f"outbox item not found: {outbox_id}")
        now = utc_now()
        receipt_status = str(receipt.get("status") or "SENT")
        conn.execute("UPDATE outbox SET status='SENT',lease_owner=NULL,lease_until=NULL,receipt_json=?,last_error_type=NULL,last_error=NULL WHERE outbox_id=?", (json_text(receipt), outbox_id))
        conn.execute(
            "INSERT INTO delivery_receipts(receipt_id,outbox_id,destination,object_type,object_id,status,payload_hash,sent_at,verified_at,receipt_json,scope,qualification_status) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(outbox_id) DO UPDATE SET status=excluded.status,"
            "sent_at=excluded.sent_at,verified_at=excluded.verified_at,receipt_json=excluded.receipt_json,"
            "scope=excluded.scope,qualification_status=excluded.qualification_status",
            (
                f"receipt:{outbox_id}", outbox_id, row["destination"], row["object_type"], row["object_id"],
                receipt_status, row["payload_hash"], now, now if receipt_status == "VERIFIED" else None,
                json_text(receipt), row["scope"], row["qualification_status"],
            ),
        )
        conn.commit()


def retry_outbox(outbox_id: str, *, error_type: str, error_message: str, retry_after_seconds: int) -> None:
    next_at = (datetime.now(timezone.utc) + timedelta(seconds=retry_after_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        _begin(conn)
        row = conn.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
        if not row:
            conn.rollback()
            raise KeyError(f"outbox item not found: {outbox_id}")
        if row["status"] == "SENT":
            conn.commit()
            return
        message = error_message[:2000]
        if int(row["attempts"]) >= int(row["max_attempts"]):
            conn.execute(
                "UPDATE outbox SET status='DEAD_LETTER',lease_owner=NULL,lease_until=NULL,"
                "last_error_type=?,last_error=? WHERE outbox_id=?",
                (error_type, message, outbox_id),
            )
            _dead_letter_conn(
                conn,
                source_type="outbox",
                source_id=outbox_id,
                reason_type=error_type or "MAX_ATTEMPTS_EXCEEDED",
                reason=message,
                payload=dict(row),
            )
        else:
            conn.execute(
                "UPDATE outbox SET status='RETRY',next_attempt_at=?,lease_owner=NULL,lease_until=NULL,"
                "last_error_type=?,last_error=? WHERE outbox_id=?",
                (next_at, error_type, message, outbox_id),
            )
        conn.commit()


def _upsert_competitor_conn(
    conn: sqlite3.Connection,
    *,
    platform: str,
    platform_account_id: str,
    account_name: str,
    metadata: dict | None = None,
) -> str:
    platform = platform.strip()
    platform_account_id = platform_account_id.strip()
    if not platform or not platform_account_id:
        raise ValueError("competitor platform and stable account id are required")
    competitor_id = f"{platform}:{platform_account_id}"
    now = utc_now()
    account_status = "WAITING_IDENTITY" if platform_account_id.startswith("unresolved:") else "KNOWN"
    conn.execute(
        "INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,account_status,first_seen_at,last_seen_at,metadata_json) "
        "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(platform,platform_account_id) DO UPDATE SET "
        "account_name=excluded.account_name,account_status=excluded.account_status,last_seen_at=excluded.last_seen_at,metadata_json=excluded.metadata_json",
        (competitor_id, platform, platform_account_id, account_name, account_status, now, now, json_text(metadata or {})),
    )
    row = conn.execute(
        "SELECT competitor_id FROM competitors WHERE platform=? AND platform_account_id=?",
        (platform, platform_account_id),
    ).fetchone()
    if not row:
        raise RuntimeError("competitor upsert did not produce a durable row")
    return str(row[0])


def upsert_competitor(*, platform: str, platform_account_id: str, account_name: str, metadata: dict | None = None) -> str:
    with connect() as conn:
        return _upsert_competitor_conn(
            conn,
            platform=platform,
            platform_account_id=platform_account_id,
            account_name=account_name,
            metadata=metadata,
        )


def _upsert_identity_conn(
    conn: sqlite3.Connection,
    *,
    competitor_id: str,
    platform: str,
    stable_id: str,
    canonical_url: str | None,
    verification_status: str,
    evidence: dict,
) -> str:
    platform = platform.strip()
    stable_id = stable_id.strip()
    if not platform or not stable_id:
        raise ValueError("identity platform and stable id are required")
    identity_id = f"{platform}:{stable_id}"
    existing = conn.execute(
        "SELECT identity_id,competitor_id FROM identities WHERE platform=? AND stable_id=?",
        (platform, stable_id),
    ).fetchone()
    if existing and existing["competitor_id"] != competitor_id:
        raise IdentityConflictError(
            f"stable identity {platform}:{stable_id} is already owned by {existing['competitor_id']}"
        )
    conn.execute(
        "INSERT INTO identities(identity_id,competitor_id,platform,stable_id,canonical_url,evidence_json,verification_status,verified_at) "
        "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(platform,stable_id) DO UPDATE SET "
        "canonical_url=excluded.canonical_url,evidence_json=excluded.evidence_json,"
        "verification_status=excluded.verification_status,verified_at=excluded.verified_at",
        (identity_id, competitor_id, platform, stable_id, canonical_url, json_text(evidence), verification_status, utc_now() if verification_status == "VERIFIED" else None),
    )
    row = conn.execute(
        "SELECT identity_id FROM identities WHERE platform=? AND stable_id=?",
        (platform, stable_id),
    ).fetchone()
    if not row:
        raise RuntimeError("identity upsert did not produce a durable row")
    return str(row[0])


def upsert_identity(*, competitor_id: str, platform: str, stable_id: str, canonical_url: str | None, verification_status: str, evidence: dict) -> str:
    with connect() as conn:
        return _upsert_identity_conn(
            conn,
            competitor_id=competitor_id,
            platform=platform,
            stable_id=stable_id,
            canonical_url=canonical_url,
            verification_status=verification_status,
            evidence=evidence,
        )


def _upsert_monitor_target_conn(conn: sqlite3.Connection, *, competitor_id: str, live_url: str, metadata: dict | None = None) -> str:
    from v3_scan_import import canonical_monitor_url
    if not canonical_monitor_url(live_url):
        raise ValueError("monitor URL is not canonical")
    target_id = f"monitor:{competitor_id}"
    now = utc_now()
    conn.execute("INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,live_url,next_check_at,metadata_json) VALUES(?,?, 'ACTIVE', ?, ?, ?) ON CONFLICT(competitor_id) DO UPDATE SET live_url=excluded.live_url,status='ACTIVE',metadata_json=excluded.metadata_json", (target_id, competitor_id, live_url, now, json_text(metadata or {})))
    row = conn.execute("SELECT monitor_target_id FROM monitor_targets WHERE competitor_id=?", (competitor_id,)).fetchone()
    if not row:
        raise RuntimeError("monitor target upsert did not produce a durable row")
    return str(row[0])


def upsert_monitor_target(*, competitor_id: str, live_url: str, metadata: dict | None = None) -> str:
    with connect() as conn:
        return _upsert_monitor_target_conn(conn, competitor_id=competitor_id, live_url=live_url, metadata=metadata)


def import_result(result_path: Path, *, mapping_path: Path | None = None, task_id: str | None = None) -> dict:
    from v3_scan_import import import_scan
    if mapping_path is not None:
        raise ValueError("Legacy name-based identity mapping cannot import new production scans")
    return import_scan(result_path, task_id=task_id)


def activation_readiness() -> dict:
    identity_assertion()
    with connect() as conn:
        total = conn.execute("SELECT count(*) FROM competitors WHERE platform='buyin'").fetchone()[0]
        verified = conn.execute("SELECT count(DISTINCT c.competitor_id) FROM competitors c JOIN identities ib ON ib.competitor_id=c.competitor_id AND ib.platform='buyin' AND ib.verification_status='VERIFIED' JOIN identities idy ON idy.competitor_id=c.competitor_id AND idy.platform='douyin' AND idy.verification_status='VERIFIED' JOIN monitor_targets m ON m.competitor_id=c.competitor_id AND m.live_url IS NOT NULL").fetchone()[0]
        targets = conn.execute("SELECT count(*) FROM monitor_targets WHERE status='ACTIVE'").fetchone()[0]
        outbox = conn.execute("SELECT count(*) FROM outbox WHERE status NOT IN ('SENT','DEAD_LETTER')").fetchone()[0]
        schema = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        open_conflicts = conn.execute("SELECT count(*) FROM identity_conflicts WHERE status='OPEN'").fetchone()[0] if _table_exists(conn, "identity_conflicts") else 0
    coverage = (verified / total) if total else 0.0
    config = load_config()
    config_active = ((config.get("atomic_activation") or {}).get("activation_state") == "ACTIVE")
    production_gate = str(config.get("production_gate") or "BLOCKED")
    ready = bool(total and coverage == 1.0 and targets == total and schema == "3" and outbox == 0 and config_active and open_conflicts == 0)
    reason = None if ready else "100% verified identity/live-target coverage, zero open identity conflicts and zero pending delivery are required before atomic activation"
    return {"ready": ready, "final_ready": bool(ready and production_gate == "READY"), "atomic_active": config_active, "production_gate": production_gate, "total_competitors": total, "verified_monitor_targets": verified, "active_targets": targets, "identity_coverage": coverage, "open_identity_conflicts": open_conflicts, "pending_outbox": outbox, "schema_version": schema, "control_plane_backend": _configured_backend(), "reason": reason}


def canonicalize_competitor_ids() -> dict:
    """Remove legacy unresolved internal IDs after a verified UID is known."""
    changed = 0
    with connect() as conn:
        _begin(conn)
        conn.execute("PRAGMA defer_foreign_keys=ON")
        rows = conn.execute("SELECT * FROM competitors WHERE platform='buyin' AND platform_account_id LIKE 'v2_%'").fetchall()
        for row in rows:
            old_id = row["competitor_id"]
            canonical_id = f"buyin:{row['platform_account_id']}"
            if old_id == canonical_id:
                continue
            existing = conn.execute("SELECT competitor_id FROM competitors WHERE competitor_id=?", (canonical_id,)).fetchone()
            if not existing:
                temporary_account_id = f"__migrating__:{uuid.uuid4().hex}"
                conn.execute("INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,account_status,first_seen_at,last_seen_at,metadata_json) VALUES(?,?,?,?,?,?,?,?)", (canonical_id, row["platform"], temporary_account_id, row["account_name"], row["account_status"], row["first_seen_at"], row["last_seen_at"], row["metadata_json"]))
            for table in ("product_competitors", "identities", "monitor_targets"):
                conn.execute(f"UPDATE {table} SET competitor_id=? WHERE competitor_id=?", (canonical_id, old_id))
            old_monitor_id = f"monitor:{old_id}"
            new_monitor_id = f"monitor:{canonical_id}"
            monitor_row = conn.execute("SELECT monitor_target_id FROM monitor_targets WHERE monitor_target_id=?", (old_monitor_id,)).fetchone()
            if monitor_row and old_monitor_id != new_monitor_id:
                temporary_monitor_id = f"migrating:{uuid.uuid4().hex}"
                conn.execute("UPDATE monitor_targets SET monitor_target_id=? WHERE monitor_target_id=?", (temporary_monitor_id, old_monitor_id))
                conn.execute("UPDATE live_sessions SET monitor_target_id=? WHERE monitor_target_id=?", (temporary_monitor_id, old_monitor_id))
                conn.execute("UPDATE outbox SET object_id=? WHERE object_type='monitor_status' AND object_id=?", (temporary_monitor_id, old_monitor_id))
                conn.execute("UPDATE monitor_targets SET monitor_target_id=? WHERE monitor_target_id=?", (new_monitor_id, temporary_monitor_id))
                conn.execute("UPDATE live_sessions SET monitor_target_id=? WHERE monitor_target_id=?", (new_monitor_id, temporary_monitor_id))
                conn.execute("UPDATE outbox SET object_id=? WHERE object_type='monitor_status' AND object_id=?", (new_monitor_id, temporary_monitor_id))
            relation_rows = conn.execute("SELECT relation_id FROM product_competitors WHERE competitor_id=? AND relation_id LIKE ?", (canonical_id, f"%{old_id}%")).fetchall()
            for relation in relation_rows:
                conn.execute("UPDATE product_competitors SET relation_id=? WHERE relation_id=?", (relation["relation_id"].replace(old_id, canonical_id), relation["relation_id"]))
            conn.execute("DELETE FROM competitors WHERE competitor_id=?", (old_id,))
            conn.execute("UPDATE competitors SET platform_account_id=? WHERE competitor_id=?", (row["platform_account_id"], canonical_id))
            changed += 1
        for monitor in conn.execute("SELECT monitor_target_id,competitor_id FROM monitor_targets").fetchall():
            desired = f"monitor:{monitor['competitor_id']}"
            if monitor["monitor_target_id"] == desired:
                continue
            temporary = f"migrating:{uuid.uuid4().hex}"
            old_monitor = monitor["monitor_target_id"]
            conn.execute("UPDATE monitor_targets SET monitor_target_id=? WHERE monitor_target_id=?", (temporary, old_monitor))
            conn.execute("UPDATE live_sessions SET monitor_target_id=? WHERE monitor_target_id=?", (temporary, old_monitor))
            conn.execute("UPDATE outbox SET object_id=? WHERE object_type='monitor_status' AND object_id=?", (temporary, old_monitor))
            conn.execute("UPDATE monitor_targets SET monitor_target_id=? WHERE monitor_target_id=?", (desired, temporary))
            conn.execute("UPDATE live_sessions SET monitor_target_id=? WHERE monitor_target_id=?", (desired, temporary))
            conn.execute("UPDATE outbox SET object_id=? WHERE object_type='monitor_status' AND object_id=?", (desired, temporary))
        for relation in conn.execute("SELECT relation_id,product_id,competitor_id FROM product_competitors").fetchall():
            if relation["competitor_id"] in relation["relation_id"]:
                continue
            replacement = f"relation:{relation['product_id']}:{relation['competitor_id']}"
            conn.execute("UPDATE product_competitors SET relation_id=? WHERE relation_id=?", (replacement, relation["relation_id"]))
        conn.commit()
    return {"changed": changed}


def merge_audited_unresolved(mapping_path: Path) -> dict:
    """Migrate only the audited unresolved rows using the verified mapping file.

    This is a one-time reconciliation operation. Runtime never uses account
    name as a live identity key; the mapping file's current Buyin UID is the
    explicit migration authority.
    """
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    merged = 0
    with connect() as conn:
        _begin(conn)
        conn.execute("PRAGMA defer_foreign_keys=ON")
        for item in mapping.get("targets") or []:
            name = str(item.get("name") or "").strip()
            uid = str(item.get("current_buyin_uid") or "").strip()
            if not name or not uid:
                continue
            unresolved = conn.execute("SELECT competitor_id FROM competitors WHERE platform='buyin' AND account_name=? AND platform_account_id LIKE 'unresolved:%'", (name,)).fetchall()
            canonical = conn.execute("SELECT competitor_id FROM competitors WHERE platform='buyin' AND platform_account_id=?", (uid,)).fetchone()
            if len(unresolved) != 1 or not canonical or unresolved[0][0] == canonical[0]:
                continue
            old_id, new_id = unresolved[0][0], canonical[0]
            old_relations = conn.execute("SELECT relation_id,product_id FROM product_competitors WHERE competitor_id=?", (old_id,)).fetchall()
            for relation in old_relations:
                exists = conn.execute("SELECT relation_id FROM product_competitors WHERE product_id=? AND competitor_id=?", (relation["product_id"], new_id)).fetchone()
                if exists:
                    conn.execute("DELETE FROM product_competitors WHERE relation_id=?", (relation["relation_id"],))
                else:
                    conn.execute("UPDATE product_competitors SET competitor_id=? WHERE relation_id=?", (new_id, relation["relation_id"]))
            old_monitor = conn.execute("SELECT monitor_target_id FROM monitor_targets WHERE competitor_id=?", (old_id,)).fetchone()
            new_monitor = conn.execute("SELECT monitor_target_id FROM monitor_targets WHERE competitor_id=?", (new_id,)).fetchone()
            if old_monitor and new_monitor:
                conn.execute("DELETE FROM monitor_targets WHERE monitor_target_id=?", (old_monitor["monitor_target_id"],))
            elif old_monitor:
                conn.execute("UPDATE monitor_targets SET competitor_id=? WHERE monitor_target_id=?", (new_id, old_monitor["monitor_target_id"]))
            conn.execute("DELETE FROM competitors WHERE competitor_id=?", (old_id,))
            merged += 1
        conn.commit()
    return {"merged": merged, "mapping_path": str(mapping_path)}


def record_scan_evidence(result_path: Path, mapping_path: Path) -> dict:
    result = json.loads(result_path.read_text(encoding="utf-8"))
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    by_name = {str(item.get("name") or ""): item for item in mapping.get("targets") or []}
    product = result.get("product") or {}
    product_id = f"douyin:{str(product.get('target_product_id') or product.get('product_id') or '').strip()}"
    if product_id == "douyin:":
        raise ValueError("scan result has no product ID")
    digest_value = hashlib.sha256(result_path.read_bytes()).hexdigest()
    from v3_scan_import import scan_identity
    scan_id = scan_identity(product_id, digest_value, result.get("source_task_id"))
    summary = result.get("scan_summary") or {}
    now = utc_now()
    with connect() as conn:
        conn.execute("INSERT OR IGNORE INTO products(product_id,platform,platform_product_id,title,source_url,first_seen_at,last_seen_at,metadata_json) VALUES(?,?,?,?,?,?,?,?)", (product_id, "buyin", product_id.split(":",1)[1], str(product.get("name") or ""), str(product.get("original_input") or product.get("original_product_link") or ""), now, now, json_text({"source_result": str(result_path)})))
        conn.execute("INSERT INTO scan_runs(scan_id,product_id,status,evidence_state,imported_at,filter_label,filter_verified,reported_total,observed_count,result_digest,result_path,manifest_path,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scan_id) DO UPDATE SET status=excluded.status,evidence_state=excluded.evidence_state,imported_at=excluded.imported_at,observed_count=excluded.observed_count,payload_json=excluded.payload_json", (scan_id, product_id, str(summary.get("status") or "INCOMPLETE"), "COMPLETE" if summary.get("status") == "COMPLETE" else "INCOMPLETE", now, summary.get("filter_label"), 1 if summary.get("filter_verified") else 0, summary.get("page_reported_result_count"), len(result.get("observations") or []), digest_value, str(result_path), str(result_path.parent / "scan_manifest.json"), json_text({"summary": summary, "mapping": str(mapping_path)})))
        for index, row in enumerate(result.get("observations") or [], 1):
            name = str(row.get("account_name") or "")
            mapped = by_name.get(name) or {}
            uid = str(mapped.get("current_buyin_uid") or "")
            competitor = conn.execute("SELECT competitor_id FROM competitors WHERE platform='buyin' AND platform_account_id=?", (uid,)).fetchone() if uid else None
            competitor_id = competitor[0] if competitor else None
            observation_id = f"obs:{scan_id}:{index}"
            conn.execute("INSERT INTO scan_observations(observation_id,scan_id,product_id,competitor_id,observation_index,source_page,source_batch,source_position,platform_observation_key,account_name,buyin_creator_uid,live_title,live_date,collected_at,identity_state,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(scan_id,observation_index) DO UPDATE SET competitor_id=excluded.competitor_id,identity_state=excluded.identity_state,payload_json=excluded.payload_json", (observation_id, scan_id, product_id, competitor_id, index, row.get("source_page"), row.get("source_batch"), row.get("source_position"), digest({"scan": scan_id, "index": index, "name": name, "date": row.get("live_date"), "title": row.get("live_title")}), name, uid or None, row.get("live_title"), row.get("live_date"), row.get("collected_at"), "VERIFIED_UID" if uid else "WAITING_IDENTITY", json_text(row)))
        conn.commit()
    return {"scan_id": scan_id, "product_id": product_id, "observations": len(result.get("observations") or [])}


def status_snapshot() -> dict:
    with connect() as conn:
        tables = ["inbox_messages", "tasks", "task_attempts", "checkpoints", "domain_events", "products", "competitors", "identities", "scan_runs", "scan_observations", "product_competitors", "monitor_targets", "live_sessions", "recording_jobs", "recording_segments", "transcripts", "analyses", "strategy_candidates", "knowledge_versions", "knowledge_diffs", "review_items", "retention_jobs", "approvals", "outbox", "delivery_receipts", "dead_letters", "lineage_edges", "recompute_requests", "evidence_bundles", "heartbeats", "task_leases", "identity_evidence", "identity_conflicts", "worker_nodes", "recording_leases", "recording_gaps", "media_manifests", "deployment_releases", "projection_reconciliations", "capacity_test_runs", "fault_drill_runs", "audit_log"]
        counts = {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in tables}
        schema = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
    return {"profile_id": PROFILE_ID, "bot_name": BOT_NAME, "app_id": APP_ID, "schema_version": schema, "control_plane_backend": _configured_backend(), "counts": counts, "activation": activation_readiness()}


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "identity", "activation-readiness", "canonicalize", "merge-audited", "record-scan-evidence"))
    parser.add_argument("--mapping-path", type=Path)
    parser.add_argument("--result-path", type=Path)
    args = parser.parse_args()
    if args.command == "init":
        init_db()
        print(json.dumps({"ok": True, "status": "READY", "db": str(DB_PATH), "schema_version": "3"}, ensure_ascii=False, indent=2))
    elif args.command == "identity":
        print(json.dumps(identity_assertion(verify_cli=True), ensure_ascii=False, indent=2))
    elif args.command == "canonicalize":
        print(json.dumps(canonicalize_competitor_ids(), ensure_ascii=False, indent=2))
    elif args.command == "merge-audited":
        if not args.mapping_path:
            raise SystemExit("--mapping-path required")
        print(json.dumps(merge_audited_unresolved(args.mapping_path), ensure_ascii=False, indent=2))
    elif args.command == "record-scan-evidence":
        if not args.mapping_path:
            raise SystemExit("--mapping-path required")
        result_path = getattr(args, "result_path", None)
        if not result_path:
            raise SystemExit("--result-path required")
        print(json.dumps(record_scan_evidence(result_path, args.mapping_path), ensure_ascii=False, indent=2))
    elif args.command == "activation-readiness":
        print(json.dumps(activation_readiness(), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(status_snapshot(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
