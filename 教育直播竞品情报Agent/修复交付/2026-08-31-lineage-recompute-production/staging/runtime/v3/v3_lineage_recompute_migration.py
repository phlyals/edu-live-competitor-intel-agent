#!/usr/bin/env python3
"""Versioned PostgreSQL migration for digest-bound lineage recomputation."""
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
DDL_PATH = ROOT / "migrations" / "004_lineage_recompute.sql"
MIGRATION_KEY = "lineage_recompute_revision"
MIGRATION_VERSION = "1"
LINEAGE_COLUMNS = {
    "binding_status", "upstream_engine_version", "upstream_model_version",
    "downstream_model_version", "downstream_prompt_version",
    "downstream_schema_version", "updated_at", "metadata_json",
}
REQUIRED_INDEXES = {
    "idx_lineage_upstream_version", "idx_recompute_requests_due",
    "idx_recompute_requests_candidate",
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


def table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT to_regclass(%s) AS name", ("public." + table,)
    )
    return cur.fetchone()["name"] is not None


def schema_state(cur) -> dict:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='lineage_edges'"
    )
    present = {str(row["column_name"]) for row in cur.fetchall()}
    return {
        "missing_lineage_columns": sorted(LINEAGE_COLUMNS - present),
        "recompute_requests_exists": table_exists(cur, "recompute_requests"),
    }


def schema_signature(cur) -> str:
    cur.execute(
        "SELECT table_name,column_name,data_type,is_nullable,column_default "
        "FROM information_schema.columns WHERE table_schema='public' "
        "AND table_name=ANY(%s) ORDER BY table_name,ordinal_position",
        (["lineage_edges", "recompute_requests"],),
    )
    columns = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT tablename,indexname,indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=ANY(%s) "
        "ORDER BY tablename,indexname",
        (["lineage_edges", "recompute_requests"],),
    )
    return digest({"columns": columns, "indexes": [dict(r) for r in cur.fetchall()]})


def build_plan(cur) -> dict:
    cur.execute(
        "SELECT edge_id,downstream_type,downstream_id,upstream_type,"
        "upstream_id,upstream_version,state FROM lineage_edges ORDER BY edge_id"
    )
    edges = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT analysis_id,transcript_id,transcript_content_digest,"
        "analysis_spec_version,model_version,prompt_version,status,lineage_state "
        "FROM analyses ORDER BY analysis_id"
    )
    analyses = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT transcript_id,engine,model,status,qualification_status "
        "FROM transcripts ORDER BY transcript_id"
    )
    transcripts = [dict(row) for row in cur.fetchall()]
    running = [
        row for row in analyses
        if row["status"] in {"RUNNING", "PENDING_RECOMPUTE"}
    ]
    issues = []
    if running:
        issues.append({
            "reason": "analysis work must be quiesced before migration",
            "analysis_ids": [row["analysis_id"] for row in running],
        })
    if table_exists(cur, "recompute_requests"):
        cur.execute("SELECT * FROM recompute_requests ORDER BY request_id")
        requests = [dict(row) for row in cur.fetchall()]
        if any(row["status"] == "RUNNING" for row in requests):
            issues.append({"reason": "recompute request is RUNNING"})
    else:
        requests = []
    core = {
        "migration_key": MIGRATION_KEY,
        "migration_version": MIGRATION_VERSION,
        "source_schema_signature": schema_signature(cur),
        "ddl_sha256": hashlib.sha256(DDL_PATH.read_bytes()).hexdigest(),
        "lineage_edges": edges,
        "analyses": analyses,
        "transcripts": transcripts,
        "recompute_requests": requests,
        "issues": issues,
    }
    return {**core, "plan_sha256": digest(core)}


def migration_health(cur) -> dict:
    schema = schema_state(cur)
    cur.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
        "AND indexname=ANY(%s)", (list(REQUIRED_INDEXES),)
    )
    indexes = {str(row["indexname"]) for row in cur.fetchall()}
    cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
    marker_row = cur.fetchone()
    marker = str(marker_row["value"]) if marker_row else None
    edge_errors = []
    request_errors = []
    legacy_request_errors = []
    if not schema["missing_lineage_columns"]:
        cur.execute(
            "SELECT e.*,a.transcript_id,a.transcript_content_digest,"
            "a.analysis_spec_version,a.model_version AS analysis_model_version,"
            "a.prompt_version,t.engine,t.model AS transcript_model "
            "FROM lineage_edges e LEFT JOIN analyses a "
            "ON e.downstream_type='analysis' AND a.analysis_id=e.downstream_id "
            "LEFT JOIN transcripts t ON t.transcript_id=e.upstream_id"
        )
        for row in cur.fetchall():
            if row["binding_status"] not in {
                "CONTENT_DIGEST_VERIFIED", "LEGACY_UNVERIFIED"
            }:
                edge_errors.append({"edge_id": row["edge_id"], "reason": "unclassified"})
            if row["binding_status"] == "CONTENT_DIGEST_VERIFIED" and not (
                row["upstream_type"] == "transcript"
                and row["upstream_id"] == row["transcript_id"]
                and row["upstream_version"] == row["transcript_content_digest"]
                and row["upstream_engine_version"] == row["engine"]
                and row["upstream_model_version"] == row["transcript_model"]
                and row["downstream_model_version"] == row["analysis_model_version"]
                and row["downstream_prompt_version"] == row["prompt_version"]
                and row["downstream_schema_version"] == row["analysis_spec_version"]
            ):
                edge_errors.append({"edge_id": row["edge_id"], "reason": "verified identity mismatch"})
    if schema["recompute_requests_exists"]:
        cur.execute("SELECT * FROM recompute_requests")
        for row in cur.fetchall():
            if (
                int(row["attempts"] or 0) < 0
                or int(row["max_attempts"] or 0) < 1
                or int(row["lease_epoch"] or 0) < 0
            ):
                request_errors.append({"request_id": row["request_id"], "reason": "invalid counters"})
            if row["status"] == "RUNNING" and (
                not row["lease_owner"] or not row["lease_until"]
            ):
                request_errors.append({"request_id": row["request_id"], "reason": "unfenced running"})
            try:
                checkpoint = json.loads(row["checkpoint_json"] or "{}")
                if not isinstance(checkpoint, dict):
                    raise ValueError
            except (json.JSONDecodeError, TypeError, ValueError):
                request_errors.append({"request_id": row["request_id"], "reason": "invalid checkpoint"})
        cur.execute(
            "SELECT r.request_id FROM recompute_requests r JOIN lineage_edges e "
            "ON e.downstream_type=r.downstream_type AND e.downstream_id=r.downstream_id "
            "AND e.upstream_type=r.upstream_type AND e.upstream_id=r.upstream_id "
            "AND e.upstream_version=r.old_upstream_digest "
            "WHERE e.binding_status='LEGACY_UNVERIFIED'"
        )
        legacy_request_errors = [row["request_id"] for row in cur.fetchall()]
    healthy = (
        not schema["missing_lineage_columns"]
        and schema["recompute_requests_exists"]
        and indexes == REQUIRED_INDEXES
        and marker == MIGRATION_VERSION
        and not edge_errors and not request_errors and not legacy_request_errors
    )
    return {
        "healthy": healthy,
        "schema": schema,
        "indexes": sorted(indexes),
        "marker": marker,
        "edge_errors": edge_errors,
        "request_errors": request_errors,
        "legacy_request_errors": legacy_request_errors,
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
            **build_plan(cur),
            "schema_before": schema_state(cur),
            "transaction_read_only": read_only,
        }


def rows_for_backup(cur) -> dict:
    result = {}
    for table in ("lineage_edges", "schema_meta"):
        cur.execute(f"SELECT * FROM {table}")
        result[table] = [dict(row) for row in cur.fetchall()]
    if table_exists(cur, "recompute_requests"):
        cur.execute("SELECT * FROM recompute_requests")
        result["recompute_requests"] = [dict(row) for row in cur.fetchall()]
    else:
        result["recompute_requests"] = None
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
            "hashtext('edu_live_v3_lineage_recompute_v1'))"
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
                f"plan changed: expected {expected_plan_sha256}, "
                f"actual {plan['plan_sha256']}"
            )
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backup_dir, 0o700)
        backup = {"created_at": now(), "plan": plan, "rows": rows_for_backup(cur)}
        path = backup_dir / (
            "pre-lineage-recompute-"
            + datetime.now().strftime("%Y%m%d-%H%M%S-%f") + ".json"
        )
        path.write_text(
            json.dumps(backup, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.chmod(path, 0o600)
        backup_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        cur.execute(ddl_body())
        health = migration_health(cur)
        if not health["healthy"]:
            raise RuntimeError("postcondition failed: " + canonical_json(health))
        return {
            "status": "APPLIED", "plan_sha256": plan["plan_sha256"],
            "backup_path": str(path), "backup_sha256": backup_sha,
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
