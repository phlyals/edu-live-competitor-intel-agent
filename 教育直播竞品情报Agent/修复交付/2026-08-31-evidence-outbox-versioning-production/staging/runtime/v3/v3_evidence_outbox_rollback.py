#!/usr/bin/env python3
"""Rollback migration 005 before any new versioned projection is created."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row

import v3_runtime
from v3_evidence_outbox_migration import MIGRATION_KEY, VERSION_COLUMNS


def rollback(backup_path: Path, expected_sha256: str, dsn: str | None = None) -> dict:
    if not backup_path.is_file():
        raise RuntimeError("backup file missing")
    actual = hashlib.sha256(backup_path.read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise RuntimeError("backup hash mismatch")
    backup = json.loads(backup_path.read_text(encoding="utf-8"))
    conn = psycopg.connect(
        dsn or v3_runtime._postgres_dsn(), autocommit=False,
        row_factory=dict_row,
    )
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtext('edu_live_v3_evidence_outbox_v1'))"
        )
        cur.execute(
            "SELECT outbox_id FROM outbox "
            "WHERE projection_binding_status='VERSIONED_EVIDENCE'"
        )
        activity = [row["outbox_id"] for row in cur.fetchall()]
        if activity:
            raise RuntimeError(
                "versioned projection activity exists; restore the PostgreSQL dump"
            )
        cur.execute("DROP TRIGGER IF EXISTS trg_v3_guard_projection_identity ON outbox")
        cur.execute("DROP TRIGGER IF EXISTS trg_v3_guard_receipt_projection_identity ON delivery_receipts")
        for table, id_column in (
            ("outbox", "outbox_id"), ("delivery_receipts", "receipt_id")
        ):
            old = {row[id_column]: row for row in backup["rows"][table]}
            cur.execute(f"SELECT {id_column} FROM {table}")
            current = {row[id_column] for row in cur.fetchall()}
            if current != set(old):
                raise RuntimeError(f"{table} row set changed; restore full dump")
            for object_id, row in old.items():
                values = {
                    key: row.get(key) for key in VERSION_COLUMNS
                }
                values["projection_binding_status"] = (
                    values.get("projection_binding_status") or "UNCLASSIFIED"
                )
                columns = sorted(VERSION_COLUMNS)
                cur.execute(
                    f"UPDATE {table} SET "
                    + ",".join(f"{column}=%s" for column in columns)
                    + f" WHERE {id_column}=%s",
                    (*[values[column] for column in columns], object_id),
                )
        cur.execute("DELETE FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
        for row in backup["rows"].get("schema_meta") or []:
            if row["key"] == MIGRATION_KEY:
                cur.execute(
                    "INSERT INTO schema_meta(key,value,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                    "updated_at=excluded.updated_at",
                    (row["key"], row["value"], row.get("updated_at")),
                )
        cur.execute("DROP INDEX IF EXISTS uq_outbox_projection_version")
        cur.execute("DROP INDEX IF EXISTS idx_outbox_projection_binding")
        cur.execute("DROP INDEX IF EXISTS idx_delivery_receipts_projection_version")
        cur.execute("DROP FUNCTION IF EXISTS v3_guard_projection_identity()")
        cur.execute("DROP FUNCTION IF EXISTS v3_guard_receipt_projection_identity()")
        return {
            "status": "ROLLED_BACK_ROWS", "backup_sha256": actual,
            "columns_retained": True,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--expected-backup-sha256", required=True)
    parser.add_argument("--dsn", help=argparse.SUPPRESS)
    args = parser.parse_args()
    print(json.dumps(
        rollback(args.backup, args.expected_backup_sha256, args.dsn),
        ensure_ascii=False, indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
