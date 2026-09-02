#!/usr/bin/env python3
"""Versioned PostgreSQL migration for fenced transcript and analysis jobs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row

import v3_runtime

ROOT = Path(__file__).resolve().parent
DDL_PATH = ROOT / "migrations" / "003_long_job_claim_retry.sql"
MIGRATION_KEY = "long_job_claim_retry_revision"
MIGRATION_VERSION = "1"
TABLES = ("transcripts", "analyses")
LONG_COLUMNS = {
    "updated_at", "attempts", "max_attempts", "next_attempt_at",
    "last_attempt_at", "lease_owner", "lease_until", "lease_epoch",
    "last_error_type", "last_error", "checkpoint_json",
}
REQUIRED_INDEXES = {
    "idx_transcripts_long_job_due", "idx_analyses_long_job_due",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def canonical_json(value) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def ddl_body() -> str:
    lines = []
    for line in DDL_PATH.read_text(encoding="utf-8").splitlines():
        if line.strip().upper() in {"BEGIN;", "COMMIT;"}:
            continue
        lines.append(line)
    return "\n".join(lines)


def schema_missing(cur) -> dict[str, list[str]]:
    missing = {}
    for table in TABLES:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s", (table,)
        )
        present = {str(row["column_name"]) for row in cur.fetchall()}
        absent = sorted(LONG_COLUMNS - present)
        if absent:
            missing[table] = absent
    return missing


def schema_signature(cur) -> str:
    cur.execute(
        "SELECT table_name,column_name,data_type,is_nullable,column_default "
        "FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name=ANY(%s) ORDER BY table_name,ordinal_position",
        (list(TABLES),),
    )
    columns = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT tablename,indexname,indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=ANY(%s) "
        "ORDER BY tablename,indexname", (list(TABLES),)
    )
    return digest({"columns": columns, "indexes": [dict(r) for r in cur.fetchall()]})


def build_plan(cur) -> dict:
    rows = {}
    for table, id_column in (
        ("transcripts", "transcript_id"), ("analyses", "analysis_id")
    ):
        cur.execute(
            f"SELECT {id_column},status FROM {table} ORDER BY {id_column}"
        )
        rows[table] = [dict(row) for row in cur.fetchall()]
    issues = []
    for table, values in rows.items():
        running = [row for row in values if row["status"] == "RUNNING"]
        if running:
            issues.append({
                "table": table,
                "reason": "RUNNING rows must be quiesced before migration",
                "ids": [next(v for k, v in row.items() if k != "status") for row in running],
            })
    core = {
        "migration_key": MIGRATION_KEY,
        "migration_version": MIGRATION_VERSION,
        "source_schema_signature": schema_signature(cur),
        "ddl_sha256": hashlib.sha256(
            DDL_PATH.read_bytes()
        ).hexdigest(),
        "row_statuses": rows,
        "issues": issues,
    }
    return {**core, "plan_sha256": digest(core)}


def migration_health(cur) -> dict:
    missing = schema_missing(cur)
    cur.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
        "AND indexname=ANY(%s)", (list(REQUIRED_INDEXES),)
    )
    indexes = {str(row["indexname"]) for row in cur.fetchall()}
    marker = None
    cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
    row = cur.fetchone()
    if row:
        marker = str(row["value"])
    errors = []
    checkpoint_invalid = []
    if not missing:
        for table, id_column in (
            ("transcripts", "transcript_id"), ("analyses", "analysis_id")
        ):
            cur.execute(
                f"SELECT {id_column},status,attempts,max_attempts,lease_epoch,"
                "lease_owner,lease_until,next_attempt_at,checkpoint_json "
                f"FROM {table}"
            )
            for item in cur.fetchall():
                if (
                    int(item["attempts"] or 0) < 0
                    or int(item["max_attempts"] or 0) < 1
                    or int(item["lease_epoch"] or 0) < 0
                ):
                    errors.append({
                        "table": table, "id": item[id_column],
                        "reason": "invalid counters",
                    })
                if item["status"] == "RUNNING" and (
                    not item["lease_owner"] or not item["lease_until"]
                    or int(item["lease_epoch"] or 0) < 1
                ):
                    errors.append({
                        "table": table, "id": item[id_column],
                        "reason": "unfenced RUNNING row",
                    })
                if item["status"] in {
                    "PENDING", "WAITING_TOOL", "PAUSED",
                    "WAITING_MODEL", "RETRY_WAIT",
                } and not item["next_attempt_at"]:
                    errors.append({
                        "table": table, "id": item[id_column],
                        "reason": "actionable row has no due time",
                    })
                try:
                    value = json.loads(item["checkpoint_json"] or "{}")
                    if not isinstance(value, dict):
                        raise ValueError
                except (json.JSONDecodeError, TypeError, ValueError):
                    checkpoint_invalid.append({
                        "table": table, "id": item[id_column],
                    })
    healthy = (
        not missing
        and indexes == REQUIRED_INDEXES
        and marker == MIGRATION_VERSION
        and not errors
        and not checkpoint_invalid
    )
    return {
        "healthy": healthy,
        "schema_missing": missing,
        "indexes": sorted(indexes),
        "marker": marker,
        "row_errors": errors,
        "checkpoint_invalid": checkpoint_invalid,
    }


def readonly_plan(dsn: str | None = None) -> dict:
    conn = psycopg.connect(
        dsn or v3_runtime._postgres_dsn(),
        autocommit=False, row_factory=dict_row,
    )
    conn.read_only = True
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        read_only = cur.fetchone()["transaction_read_only"]
        if read_only != "on":
            raise RuntimeError("migration audit is not READ ONLY")
        return {
            **build_plan(cur),
            "schema_missing_before_migration": schema_missing(cur),
            "transaction_read_only": read_only,
        }


def rows_for_backup(cur) -> dict:
    result = {}
    for table in (*TABLES, "schema_meta"):
        cur.execute(f"SELECT * FROM {table}")
        result[table] = [dict(row) for row in cur.fetchall()]
    return result


def apply(
    expected_plan_sha256: str,
    backup_dir: Path,
    dsn: str | None = None,
) -> dict:
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
        cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
        marker = cur.fetchone()
        if marker and str(marker["value"]) == MIGRATION_VERSION:
            health = migration_health(cur)
            if not health["healthy"]:
                raise RuntimeError(
                    "migration marker exists but drift was detected: "
                    + canonical_json(health)
                )
            return {
                "status": "ALREADY_APPLIED",
                "migration_version": MIGRATION_VERSION,
                "health": health,
            }
        plan = build_plan(cur)
        if plan["issues"]:
            raise RuntimeError("migration plan has unresolved issues")
        if plan["plan_sha256"] != expected_plan_sha256:
            raise RuntimeError(
                f"plan changed: expected {expected_plan_sha256}, "
                f"actual {plan['plan_sha256']}"
            )
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backup_dir, 0o700)
        backup = {"created_at": now(), "plan": plan, "rows": rows_for_backup(cur)}
        backup_path = backup_dir / (
            "pre-long-job-claim-retry-"
            + datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            + ".json"
        )
        backup_path.write_text(
            json.dumps(backup, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.chmod(backup_path, 0o600)
        backup_sha = hashlib.sha256(backup_path.read_bytes()).hexdigest()
        cur.execute(ddl_body())
        health = migration_health(cur)
        if not health["healthy"]:
            raise RuntimeError("postcondition failed: " + canonical_json(health))
        return {
            "status": "APPLIED",
            "migration_version": MIGRATION_VERSION,
            "plan_sha256": plan["plan_sha256"],
            "backup_path": str(backup_path),
            "backup_sha256": backup_sha,
            "postconditions": health,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--dsn", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.apply:
        if not args.expected_plan_sha256 or not args.backup_dir:
            parser.error(
                "--apply requires --expected-plan-sha256 and --backup-dir"
            )
        result = apply(
            args.expected_plan_sha256, args.backup_dir, args.dsn
        )
    else:
        result = readonly_plan(args.dsn)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
