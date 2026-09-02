#!/usr/bin/env python3
"""Restore pre-003 row state only before any long-job lease has been used."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row

import v3_runtime
from v3_long_job_migration import MIGRATION_KEY

LONG_COLUMNS = (
    "updated_at", "attempts", "max_attempts", "next_attempt_at",
    "last_attempt_at", "lease_owner", "lease_until", "lease_epoch",
    "last_error_type", "last_error", "checkpoint_json",
)


def rollback(
    backup_path: Path,
    expected_backup_sha256: str,
    dsn: str | None = None,
) -> dict:
    if not backup_path.is_file():
        raise RuntimeError("backup file missing")
    actual = hashlib.sha256(backup_path.read_bytes()).hexdigest()
    if actual != expected_backup_sha256:
        raise RuntimeError("backup hash mismatch")
    backup = json.loads(backup_path.read_text(encoding="utf-8"))
    conn = psycopg.connect(
        dsn or v3_runtime._postgres_dsn(),
        autocommit=False, row_factory=dict_row,
    )
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtext('edu_live_v3_long_job_claim_retry_v1'))"
        )
        for table, id_column in (
            ("transcripts", "transcript_id"), ("analyses", "analysis_id")
        ):
            cur.execute(
                f"SELECT {id_column},attempts,lease_epoch,lease_owner,"
                f"last_attempt_at FROM {table} WHERE attempts<>0 OR lease_epoch<>0 "
                "OR lease_owner IS NOT NULL OR last_attempt_at IS NOT NULL"
            )
            used = [dict(row) for row in cur.fetchall()]
            if used:
                raise RuntimeError(
                    "long-job leases have already been used; restore the "
                    "PostgreSQL dump instead of row rollback"
                )
        rows = backup["rows"]
        for table, id_column in (
            ("transcripts", "transcript_id"), ("analyses", "analysis_id")
        ):
            for row in rows[table]:
                values = {
                    "updated_at": row.get("updated_at"),
                    "attempts": int(row.get("attempts") or 0),
                    "max_attempts": int(row.get("max_attempts") or 5),
                    "next_attempt_at": row.get("next_attempt_at"),
                    "last_attempt_at": row.get("last_attempt_at"),
                    "lease_owner": row.get("lease_owner"),
                    "lease_until": row.get("lease_until"),
                    "lease_epoch": int(row.get("lease_epoch") or 0),
                    "last_error_type": row.get("last_error_type"),
                    "last_error": row.get("last_error"),
                    "checkpoint_json": row.get("checkpoint_json") or "{}",
                }
                clauses = ",".join(f"{key}=%s" for key in LONG_COLUMNS)
                cur.execute(
                    f"UPDATE {table} SET {clauses} WHERE {id_column}=%s",
                    (*[values[key] for key in LONG_COLUMNS], row[id_column]),
                )
        cur.execute("DELETE FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
        for row in rows.get("schema_meta") or []:
            if row["key"] != MIGRATION_KEY:
                continue
            cur.execute(
                "INSERT INTO schema_meta(key,value,updated_at) VALUES(%s,%s,%s) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                "updated_at=excluded.updated_at",
                (row["key"], row["value"], row.get("updated_at")),
            )
        cur.execute("DROP INDEX IF EXISTS idx_transcripts_long_job_due")
        cur.execute("DROP INDEX IF EXISTS idx_analyses_long_job_due")
        return {
            "status": "ROLLED_BACK_ROWS",
            "backup_sha256": actual,
            "columns_retained": True,
            "indexes_removed": True,
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
