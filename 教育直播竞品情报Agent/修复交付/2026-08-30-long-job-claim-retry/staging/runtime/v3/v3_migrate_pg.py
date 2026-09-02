#!/usr/bin/env python3
"""One-way, auditable SQLite -> PostgreSQL Runtime V3 migration.

The source SQLite database remains untouched.  Each table is copied with an
explicit column intersection and a per-table row/hash manifest is emitted so
the cutover can be repeated safely and reconciled before services resume.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import v3_runtime as v3  # noqa: E402


TABLE_ORDER = [
    "schema_meta", "inbox_messages", "tasks", "task_attempts", "checkpoints",
    "domain_events", "products", "competitors", "scan_runs", "scan_observations",
    "identities", "product_competitors", "monitor_targets", "live_sessions",
    "recording_jobs", "recording_segments", "transcripts", "analyses", "approvals",
    "outbox", "delivery_receipts", "dead_letters", "lineage_edges", "evidence_bundles",
    "strategy_candidates", "knowledge_versions", "knowledge_diffs", "review_items",
    "retention_jobs", "heartbeats", "reconciliation_runs",
]


def canonical_row(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def table_hash(rows: list[dict], table: str = "") -> str:
    if table == "schema_meta":
        # updated_at is an operational timestamp rewritten by init_db on the
        # target.  The durable identity of schema_meta is key/value.
        rows = [{k: v for k, v in row.items() if k != "updated_at"} for row in rows]
    digest = hashlib.sha256()
    for row in sorted((canonical_row(row) for row in rows)):
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def pg_columns(conn, table: str) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=? ORDER BY ordinal_position",
        (table,),
    ).fetchall()
    return [str(row["column_name"]) for row in rows]


def copy_table(source: sqlite3.Connection, target, table: str) -> dict:
    source_cols = sqlite_columns(source, table)
    target_cols = set(pg_columns(target, table))
    cols = [col for col in source_cols if col in target_cols]
    if not cols:
        return {"table": table, "source_rows": 0, "target_rows": 0, "source_hash": "", "target_hash": "", "status": "SKIPPED"}
    source.row_factory = sqlite3.Row
    select_cols = ", ".join(f'"{col}"' for col in cols)
    rows = [dict(row) for row in source.execute(f'SELECT {select_cols} FROM "{table}"').fetchall()]
    # The expression above is intentionally generated from PRAGMA column names;
    # no user-provided identifiers enter this migration.
    placeholders = ",".join("?" for _ in cols)
    query = f'INSERT INTO "{table}" ({", ".join(cols)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
    for row in rows:
        target.execute(query, tuple(row.get(col) for col in cols))
    copied = [dict(row) for row in target.execute(f'SELECT {select_cols} FROM "{table}"').fetchall()]
    return {
        "table": table,
        "source_rows": len(rows),
        "target_rows": len(copied),
        "source_hash": table_hash(rows, table),
        "target_hash": table_hash(copied, table),
        "status": "PASS" if len(rows) == len(copied) and table_hash(rows, table) == table_hash(copied, table) else "FAIL",
    }


def main() -> int:
    source_path = v3.DB_PATH
    if not source_path.exists():
        raise SystemExit(f"SQLite source missing: {source_path}")
    source = sqlite3.connect(source_path)
    source.row_factory = sqlite3.Row
    v3.init_db()
    report = {"source": str(source_path), "backend": "postgresql", "tables": [], "status": "PASS"}
    with v3.connect() as target:
        target.execute("BEGIN")
        try:
            for table in TABLE_ORDER:
                exists = target.execute(
                    "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema='public' AND table_name=?",
                    (table,),
                ).fetchone()
                if not exists or int(exists["n"]) != 1:
                    report["tables"].append({"table": table, "status": "MISSING_TARGET"})
                    report["status"] = "FAIL"
                    continue
                result = copy_table(source, target, table)
                report["tables"].append(result)
                if result.get("status") == "FAIL":
                    report["status"] = "FAIL"
            if report["status"] != "PASS":
                target.rollback()
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 1
            target.commit()
        except Exception:
            target.rollback()
            raise
    source.close()
    output = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/pg-migration-manifest.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
