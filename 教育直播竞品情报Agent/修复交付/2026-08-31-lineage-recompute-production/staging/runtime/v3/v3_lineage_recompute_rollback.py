#!/usr/bin/env python3
"""Rollback migration 004 only before recompute activity has started."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row

import v3_runtime
from v3_lineage_recompute_migration import MIGRATION_KEY


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
            "hashtext('edu_live_v3_lineage_recompute_v1'))"
        )
        cur.execute("SELECT * FROM recompute_requests ORDER BY request_id")
        requests = [dict(row) for row in cur.fetchall()]
        if requests:
            raise RuntimeError(
                "recompute activity exists; restore the PostgreSQL dump "
                "instead of row rollback"
            )
        cur.execute("DROP TRIGGER IF EXISTS trg_v3_guard_lineage_identity ON lineage_edges")
        cur.execute("DROP TRIGGER IF EXISTS trg_v3_guard_recompute_identity ON recompute_requests")
        old_by_id = {
            row["edge_id"]: row
            for row in backup["rows"]["lineage_edges"]
        }
        cur.execute("SELECT edge_id FROM lineage_edges")
        current_ids = {row["edge_id"] for row in cur.fetchall()}
        if current_ids != set(old_by_id):
            raise RuntimeError("lineage edge set changed; restore full dump")
        for edge_id, row in old_by_id.items():
            cur.execute(
                "UPDATE lineage_edges SET binding_status=%s,"
                "upstream_engine_version=%s,upstream_model_version=%s,"
                "downstream_model_version=%s,downstream_prompt_version=%s,"
                "downstream_schema_version=%s,updated_at=%s,metadata_json=%s "
                "WHERE edge_id=%s",
                (
                    row.get("binding_status") or "UNCLASSIFIED",
                    row.get("upstream_engine_version"),
                    row.get("upstream_model_version"),
                    row.get("downstream_model_version"),
                    row.get("downstream_prompt_version"),
                    row.get("downstream_schema_version"),
                    row.get("updated_at"), row.get("metadata_json") or "{}",
                    edge_id,
                ),
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
        cur.execute("DROP INDEX IF EXISTS idx_lineage_upstream_version")
        cur.execute("DROP INDEX IF EXISTS idx_recompute_requests_due")
        cur.execute("DROP INDEX IF EXISTS idx_recompute_requests_candidate")
        cur.execute("DROP TABLE recompute_requests")
        cur.execute("DROP FUNCTION IF EXISTS v3_guard_lineage_identity()")
        cur.execute("DROP FUNCTION IF EXISTS v3_guard_recompute_identity()")
        return {
            "status": "ROLLED_BACK_ROWS", "backup_sha256": actual,
            "lineage_columns_retained": True,
            "recompute_table_removed": True,
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
