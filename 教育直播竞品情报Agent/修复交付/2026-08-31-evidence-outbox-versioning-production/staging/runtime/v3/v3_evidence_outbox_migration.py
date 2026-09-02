#!/usr/bin/env python3
"""Migrate semantic projections to evidence-bound version identities."""
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
DDL_PATH = ROOT / "migrations" / "005_evidence_outbox_versioning.sql"
MIGRATION_KEY = "evidence_outbox_versioning_revision"
MIGRATION_VERSION = "1"
VERSION_COLUMNS = {
    "projection_version", "artifact_digest", "evidence_bundle_id",
    "evidence_manifest_hash", "evidence_verified_at",
    "projection_binding_status",
}
REQUIRED_INDEXES = {
    "uq_outbox_projection_version", "idx_outbox_projection_binding",
    "idx_delivery_receipts_projection_version",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def canonical_json(value) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        default=str,
    )


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def ddl_body() -> str:
    return "\n".join(
        line for line in DDL_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip().upper() not in {"BEGIN;", "COMMIT;"}
    )


def schema_state(cur) -> dict:
    result = {}
    for table in ("outbox", "delivery_receipts"):
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s", (table,)
        )
        present = {str(row["column_name"]) for row in cur.fetchall()}
        result[table] = sorted(VERSION_COLUMNS - present)
    return result


def schema_signature(cur) -> str:
    cur.execute(
        "SELECT table_name,column_name,data_type,is_nullable,column_default "
        "FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name=ANY(%s) ORDER BY table_name,ordinal_position",
        (["outbox", "delivery_receipts"],),
    )
    columns = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT tablename,indexname,indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=ANY(%s) "
        "ORDER BY tablename,indexname",
        (["outbox", "delivery_receipts"],),
    )
    return digest({"columns": columns, "indexes": [dict(r) for r in cur.fetchall()]})


def build_plan(cur) -> dict:
    cur.execute(
        "SELECT outbox_id,object_type,object_id,destination,status,attempts,"
        "payload_hash,scope,qualification_status FROM outbox ORDER BY outbox_id"
    )
    outbox = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT receipt_id,outbox_id,status,payload_hash,scope,qualification_status "
        "FROM delivery_receipts ORDER BY receipt_id"
    )
    receipts = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT bundle_id,object_id,status,manifest_hash,verified_at,scope,"
        "qualification_status FROM evidence_bundles WHERE object_type='analysis' "
        "ORDER BY bundle_id"
    )
    evidence = [dict(row) for row in cur.fetchall()]
    issues = []
    inflight = [row["outbox_id"] for row in outbox if row["status"] == "IN_FLIGHT"]
    if inflight:
        issues.append({"reason": "outbox workers must be quiesced", "outbox_ids": inflight})
    cur.execute(
        "SELECT analysis_id FROM analyses WHERE status IN "
        "('RUNNING','PENDING_RECOMPUTE','WAITING_MODEL','RETRY_WAIT')"
    )
    active_analysis = [row["analysis_id"] for row in cur.fetchall()]
    if active_analysis:
        issues.append({"reason": "analysis workers must be quiesced", "analysis_ids": active_analysis})
    core = {
        "migration_key": MIGRATION_KEY,
        "migration_version": MIGRATION_VERSION,
        "source_schema_signature": schema_signature(cur),
        "ddl_sha256": hashlib.sha256(DDL_PATH.read_bytes()).hexdigest(),
        "outbox": outbox,
        "receipts": receipts,
        "evidence": evidence,
        "issues": issues,
    }
    return {**core, "plan_sha256": digest(core)}


def legacy_projection_version(outbox_id: str) -> str:
    return "legacy_projection_" + hashlib.sha256(outbox_id.encode()).hexdigest()


def backfill_legacy(conn) -> int:
    rows = conn.execute(
        "SELECT o.outbox_id,a.artifact_digest,e.bundle_id,e.manifest_hash,e.verified_at "
        "FROM outbox o JOIN analyses a ON a.analysis_id=o.object_id "
        "JOIN evidence_bundles e ON e.object_type='analysis' AND e.object_id=o.object_id "
        "WHERE o.object_type='semantic_projection' "
        "AND o.scope='FORMAL_SINGLE_SESSION' "
        "AND o.qualification_status='FULL_SESSION_QUALIFIED' "
        "AND e.status='VERIFIED' AND e.manifest_hash=a.artifact_digest "
        "AND o.projection_version IS NULL"
    ).fetchall()
    changed = 0
    for row in rows:
        version = legacy_projection_version(row["outbox_id"])
        cursor = conn.execute(
            "UPDATE outbox SET projection_version=%s,artifact_digest=%s,"
            "evidence_bundle_id=%s,evidence_manifest_hash=%s,evidence_verified_at=%s,"
            "projection_binding_status='LEGACY_EVIDENCE_BOUND' "
            "WHERE outbox_id=%s AND projection_version IS NULL",
            (
                version, row["artifact_digest"], row["bundle_id"],
                row["manifest_hash"], row["verified_at"], row["outbox_id"],
            ),
        )
        changed += int(cursor.rowcount or 0)
        conn.execute(
            "UPDATE delivery_receipts SET projection_version=%s,artifact_digest=%s,"
            "evidence_bundle_id=%s,evidence_manifest_hash=%s,evidence_verified_at=%s,"
            "projection_binding_status='LEGACY_EVIDENCE_BOUND' "
            "WHERE outbox_id=%s AND projection_version IS NULL",
            (
                version, row["artifact_digest"], row["bundle_id"],
                row["manifest_hash"], row["verified_at"], row["outbox_id"],
            ),
        )
    return changed


def migration_health(cur) -> dict:
    missing = schema_state(cur)
    cur.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
        "AND indexname=ANY(%s)", (list(REQUIRED_INDEXES),)
    )
    indexes = {str(row["indexname"]) for row in cur.fetchall()}
    cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
    marker_row = cur.fetchone()
    marker = str(marker_row["value"]) if marker_row else None
    errors = []
    if not any(missing.values()):
        cur.execute(
            "SELECT o.*,a.artifact_digest AS actual_artifact,e.bundle_id AS actual_bundle,"
            "e.manifest_hash AS actual_manifest,e.verified_at AS actual_verified,"
            "e.status AS evidence_status,r.receipt_id,r.payload_hash AS receipt_hash,"
            "r.projection_version AS receipt_version,r.artifact_digest AS receipt_artifact "
            "FROM outbox o LEFT JOIN analyses a ON a.analysis_id=o.object_id "
            "LEFT JOIN evidence_bundles e ON e.object_type='analysis' AND e.object_id=o.object_id "
            "LEFT JOIN delivery_receipts r ON r.outbox_id=o.outbox_id "
            "WHERE o.object_type='semantic_projection' AND o.projection_version IS NOT NULL"
        )
        for row in cur.fetchall():
            if row["projection_binding_status"] not in {
                "LEGACY_EVIDENCE_BOUND", "VERSIONED_EVIDENCE"
            }:
                errors.append({"outbox_id": row["outbox_id"], "reason": "invalid binding"})
            if not (
                row["artifact_digest"] == row["actual_artifact"]
                and row["evidence_bundle_id"] == row["actual_bundle"]
                and row["evidence_manifest_hash"] == row["actual_manifest"]
                and row["evidence_verified_at"] == row["actual_verified"]
                and row["evidence_status"] == "VERIFIED"
            ):
                errors.append({"outbox_id": row["outbox_id"], "reason": "evidence identity mismatch"})
            if row["receipt_id"] and not (
                row["receipt_hash"] == row["payload_hash"]
                and row["receipt_version"] == row["projection_version"]
                and row["receipt_artifact"] == row["artifact_digest"]
            ):
                errors.append({"outbox_id": row["outbox_id"], "reason": "receipt identity mismatch"})
        cur.execute(
            "SELECT o.outbox_id FROM outbox o LEFT JOIN delivery_receipts r "
            "ON r.outbox_id=o.outbox_id WHERE o.object_type='semantic_projection' "
            "AND o.status='SENT' AND r.outbox_id IS NULL"
        )
        for row in cur.fetchall():
            errors.append({"outbox_id": row["outbox_id"], "reason": "sent without receipt"})
        cur.execute(
            "SELECT outbox_id FROM outbox WHERE object_type='semantic_projection' "
            "AND status='HELD_EVIDENCE'"
        )
        held = [row["outbox_id"] for row in cur.fetchall()]
    else:
        held = []
    healthy = (
        not any(missing.values()) and indexes == REQUIRED_INDEXES
        and marker == MIGRATION_VERSION and not errors and not held
    )
    return {
        "healthy": healthy, "missing_columns": missing,
        "indexes": sorted(indexes), "marker": marker,
        "errors": errors, "held_evidence_outbox": held,
    }


def readonly_plan(dsn: str | None = None) -> dict:
    conn = psycopg.connect(
        dsn or v3_runtime._postgres_dsn(), autocommit=False,
        row_factory=dict_row,
    )
    conn.read_only = True
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        read_only = cur.fetchone()["transaction_read_only"]
        if read_only != "on":
            raise RuntimeError("migration audit is not READ ONLY")
        return {
            **build_plan(cur), "schema_before": schema_state(cur),
            "transaction_read_only": read_only,
        }


def rows_for_backup(cur) -> dict:
    result = {}
    for table in ("outbox", "delivery_receipts", "schema_meta"):
        cur.execute(f"SELECT * FROM {table}")
        result[table] = [dict(row) for row in cur.fetchall()]
    return result


def apply(expected_plan_sha256: str, backup_dir: Path, dsn: str | None = None) -> dict:
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
        cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
        marker = cur.fetchone()
        if marker and str(marker["value"]) == MIGRATION_VERSION:
            health = migration_health(cur)
            if not health["healthy"]:
                raise RuntimeError("migration drift: " + canonical_json(health))
            return {"status": "ALREADY_APPLIED", "health": health}
        plan = build_plan(cur)
        if plan["issues"]:
            raise RuntimeError("migration plan has unresolved issues")
        if plan["plan_sha256"] != expected_plan_sha256:
            raise RuntimeError(
                f"plan changed: expected {expected_plan_sha256}, actual {plan['plan_sha256']}"
            )
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backup_dir, 0o700)
        backup = {"created_at": now(), "plan": plan, "rows": rows_for_backup(cur)}
        path = backup_dir / (
            "pre-evidence-outbox-"
            + datetime.now().strftime("%Y%m%d-%H%M%S-%f") + ".json"
        )
        path.write_text(
            json.dumps(backup, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.chmod(path, 0o600)
        backup_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        cur.execute(ddl_body())
        backfilled = backfill_legacy(conn)
        health = migration_health(cur)
        if not health["healthy"]:
            raise RuntimeError("postcondition failed: " + canonical_json(health))
        return {
            "status": "APPLIED", "plan_sha256": plan["plan_sha256"],
            "backup_path": str(path), "backup_sha256": backup_sha,
            "legacy_rows_backfilled": backfilled,
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
            parser.error("--apply requires plan hash and backup dir")
        result = apply(args.expected_plan_sha256, args.backup_dir, args.dsn)
    else:
        result = readonly_plan(args.dsn)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
