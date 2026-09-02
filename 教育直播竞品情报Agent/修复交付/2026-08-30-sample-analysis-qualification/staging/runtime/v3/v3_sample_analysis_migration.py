#!/usr/bin/env python3
"""Atomic qualification migration for transcript -> analysis -> delivery lineage.

Default mode uses an explicit READ ONLY PostgreSQL transaction.  ``--apply``
requires the exact dry-run plan hash and writes a mode-0600 row backup before
running DDL and backfill in one advisory-locked transaction.  It never deletes
historical artifacts, analyses, outbox rows, or delivery receipts.
"""
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
from v3_analysis_contract import (
    ANALYSIS_SCOPE_FORMAL,
    ANALYSIS_SCOPE_SAMPLE,
    ANALYSIS_SPEC_VERSION,
    MODEL_VERSION,
    PROMPT_VERSION,
    QUALIFIED,
    SAMPLE_NONQUALIFYING,
    TRANSCRIPT_SCOPE_FULL,
    TRANSCRIPT_SCOPE_SAMPLE,
    file_sha256,
    json_object,
)

MIGRATION_KEY = "sample_analysis_qualification_revision"
MIGRATION_VERSION = "2"
PROFILE_ID = "edu_live_competitor_intel"
ROOT = Path(__file__).resolve().parent
DDL_PATH = ROOT / "migrations" / "002_analysis_qualification.sql"
EXPECTED_COLUMNS = {
    "transcripts": {"scope", "qualification_status"},
    "analyses": {"transcript_id", "scope", "qualification_status", "transcript_content_digest", "analysis_spec_version", "model_version", "prompt_version", "artifact_digest"},
    "evidence_bundles": {"scope", "qualification_status"},
    "outbox": {"scope", "qualification_status"},
    "delivery_receipts": {"scope", "qualification_status"},
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def transcript_kind(row: dict, current_segments) -> str:
    metadata = json_object(row.get("metadata_json"))
    path = str(row.get("output_path") or row.get("source_path") or "").lower()
    if metadata.get("sample_only") is True or metadata.get("coverage_scope") == "SAMPLE" or "sample300s" in path:
        return "SAMPLE"
    quality = metadata.get("timestamp_coverage") or {}
    try:
        rate = float(quality.get("coverage_rate"))
    except (TypeError, ValueError):
        rate = -1
    source_segment_id = str(metadata.get("source_segment_id") or "")
    expected_source_digest = current_segments.get(source_segment_id) if isinstance(current_segments, dict) else None
    current_match = (
        str(row.get("source_digest") or "") == expected_source_digest
        if isinstance(current_segments, dict)
        else source_segment_id in current_segments
    )
    qualified = (
        row.get("status") == "COMPLETE"
        and metadata.get("coverage_scope") == "FULL_SESSION"
        and metadata.get("sample_only") is False
        and metadata.get("quality_gate_status") == QUALIFIED
        and quality.get("is_qualified") is True
        and quality.get("timestamps_valid") is True
        and rate >= 0.90
        and current_match
        and row.get("session_status") == "MEDIA_COMPLETE"
        and row.get("completeness") == "COMPLETE"
        and (json_object(row.get("session_metadata")).get("media_coverage") or {}).get("continuous_capture") is True
    )
    return QUALIFIED if qualified else "OTHER"


def artifact_identity(path_text: str | None) -> tuple[dict | None, str | None]:
    path = Path(str(path_text or ""))
    if not path.is_file():
        return None, "analysis artifact missing"
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"analysis artifact invalid JSON: {exc.__class__.__name__}"
    if not isinstance(artifact, dict) or not artifact.get("transcript_id"):
        return None, "analysis artifact lacks transcript_id"
    return {
        "analysis_id": str(artifact.get("analysis_id") or ""),
        "session_id": str(artifact.get("session_id") or ""),
        "transcript_id": str(artifact["transcript_id"]),
        "artifact_digest": file_sha256(path),
    }, None


def artifact_matches(identity: dict | None, analysis: dict, candidate_ids: list[str]) -> bool:
    return bool(
        identity
        and identity.get("analysis_id") == str(analysis.get("analysis_id") or "")
        and identity.get("session_id") == str(analysis.get("session_id") or "")
        and identity.get("transcript_id") in candidate_ids
    )


def schema_missing(cur) -> dict[str, list[str]]:
    missing = {}
    for table, expected in EXPECTED_COLUMNS.items():
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table,))
        present = {str(row["column_name"]) for row in cur.fetchall()}
        absent = sorted(expected - present)
        if absent:
            missing[table] = absent
    return missing


def schema_signature(cur) -> str:
    tables = sorted(EXPECTED_COLUMNS)
    cur.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY(%s) ORDER BY table_name,ordinal_position", (tables,))
    columns = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=ANY(%s) ORDER BY tablename,indexname", (tables,))
    indexes = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT c.relname AS table_name,t.tgname AS trigger_name,pg_get_triggerdef(t.oid) AS trigger_def FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal AND c.relname=ANY(%s) ORDER BY c.relname,t.tgname", (tables,))
    triggers = [dict(row) for row in cur.fetchall()]
    return digest({"columns": columns, "indexes": indexes, "triggers": triggers})


def migration_health(cur) -> dict:
    missing = schema_missing(cur)
    cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='public' AND indexname IN ('uq_analyses_transcript_spec','idx_analyses_transcript_id','idx_analyses_qualification','idx_outbox_qualification')")
    indexes = {str(row["indexname"]) for row in cur.fetchall()}
    cur.execute("SELECT count(*) AS n FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relname='analyses' AND t.tgname='trg_v3_guard_analysis_immutable_identity' AND NOT t.tgisinternal")
    trigger = cur.fetchone()["n"] == 1
    unclassified = {}
    if not missing:
        for table in ("transcripts", "analyses", "evidence_bundles", "outbox", "delivery_receipts"):
            cur.execute(f"SELECT count(*) AS n FROM {table} WHERE scope='UNCLASSIFIED' OR qualification_status='UNCLASSIFIED'")
            unclassified[table] = cur.fetchone()["n"]
        cur.execute("SELECT analysis_id,transcript_id,transcript_content_digest FROM analyses WHERE scope=%s AND qualification_status=%s", (ANALYSIS_SCOPE_FORMAL, QUALIFIED))
        lineage_errors = []
        for analysis in cur.fetchall():
            cur.execute("SELECT upstream_id,upstream_version FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=%s AND upstream_type='transcript' AND state='CURRENT'", (analysis["analysis_id"],))
            edges = [dict(row) for row in cur.fetchall()]
            if edges != [{"upstream_id": analysis["transcript_id"], "upstream_version": analysis["transcript_content_digest"]}]:
                lineage_errors.append({"analysis_id": analysis["analysis_id"], "edges": edges})
        cur.execute("SELECT count(*) AS n FROM analyses WHERE analysis_type='single_session' AND status='COMPLETE' AND (transcript_id IS NULL OR scope<>%s OR qualification_status<>%s OR transcript_content_digest IS NULL OR artifact_digest IS NULL)", (ANALYSIS_SCOPE_FORMAL, QUALIFIED))
        bad_formal = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM analyses WHERE qualification_status=%s AND (status<>'SAMPLE_NONQUALIFYING' OR lineage_state<>'INVALIDATED')", (SAMPLE_NONQUALIFYING,))
        bad_sample = cur.fetchone()["n"]
        cur.execute("SELECT analysis_id FROM analyses WHERE qualification_status=%s", (SAMPLE_NONQUALIFYING,))
        sample_ids = [str(row["analysis_id"]) for row in cur.fetchall()]
        sample_lineage_errors, correction_errors = [], []
        for analysis_id in sample_ids:
            cur.execute("SELECT edge_id FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=%s AND upstream_type='transcript' AND state='CURRENT'", (analysis_id,))
            current_sample_edges = [row["edge_id"] for row in cur.fetchall()]
            if current_sample_edges:
                sample_lineage_errors.append({"analysis_id": analysis_id, "current_edge_ids": current_sample_edges})
            cur.execute("SELECT outbox_id,status,scope,qualification_status FROM outbox WHERE object_type='semantic_projection' AND object_id=%s AND payload_json::jsonb->>'correction_version'='1'", (analysis_id,))
            corrections = [dict(row) for row in cur.fetchall()]
            if len(corrections) != 1 or corrections[0]["scope"] != ANALYSIS_SCOPE_SAMPLE or corrections[0]["qualification_status"] != SAMPLE_NONQUALIFYING or corrections[0]["status"] not in {"PENDING", "RETRY", "SENT"}:
                correction_errors.append({"analysis_id": analysis_id, "corrections": corrections})
                continue
            if corrections[0]["status"] == "SENT":
                cur.execute("SELECT status,scope,qualification_status FROM delivery_receipts WHERE outbox_id=%s", (corrections[0]["outbox_id"],))
                receipt = cur.fetchone()
                if not receipt or receipt["status"] != "VERIFIED" or receipt["scope"] != ANALYSIS_SCOPE_SAMPLE or receipt["qualification_status"] != SAMPLE_NONQUALIFYING:
                    correction_errors.append({"analysis_id": analysis_id, "sent_correction_receipt": dict(receipt) if receipt else None})
        cur.execute("SELECT count(*) AS n FROM evidence_bundles e JOIN analyses a ON a.analysis_id=e.object_id WHERE e.object_type='analysis' AND a.qualification_status=%s AND (e.scope<>%s OR e.qualification_status<>%s OR e.status NOT IN ('REQUIRED','VERIFIED'))", (QUALIFIED, ANALYSIS_SCOPE_FORMAL, QUALIFIED))
        bad_formal_evidence = cur.fetchone()["n"]
        cur.execute("SELECT count(*) AS n FROM evidence_bundles e JOIN analyses a ON a.analysis_id=e.object_id WHERE e.object_type='analysis' AND a.qualification_status=%s AND (e.scope<>%s OR e.qualification_status<>%s OR e.status<>'INVALIDATED_SAMPLE')", (SAMPLE_NONQUALIFYING, ANALYSIS_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING))
        bad_sample_evidence = cur.fetchone()["n"]
    else:
        lineage_errors, sample_lineage_errors, correction_errors, bad_formal, bad_sample, bad_formal_evidence, bad_sample_evidence = [], [], [], None, None, None, None
    required_indexes = {'uq_analyses_transcript_spec','idx_analyses_transcript_id','idx_analyses_qualification','idx_outbox_qualification'}
    healthy = not missing and indexes == required_indexes and trigger and not any(unclassified.values()) and not lineage_errors and not sample_lineage_errors and not correction_errors and bad_formal == 0 and bad_sample == 0 and bad_formal_evidence == 0 and bad_sample_evidence == 0
    return {"healthy": healthy, "schema_missing": missing, "indexes": sorted(indexes), "trigger": trigger, "unclassified": unclassified, "lineage_errors": lineage_errors, "sample_lineage_errors": sample_lineage_errors, "correction_errors": correction_errors, "bad_formal": bad_formal, "bad_sample": bad_sample, "bad_formal_evidence": bad_formal_evidence, "bad_sample_evidence": bad_sample_evidence}


def build_plan(cur) -> dict:
    cur.execute("SELECT segment_id,session_id,checksum,path FROM recording_segments WHERE status='COMPLETE' AND lifecycle_status='CANONICAL_ACTIVE'")
    current: dict[str, dict[str, str]] = {}
    for row in cur.fetchall():
        checksum = str(row["checksum"] or "")
        path = Path(str(row["path"] or ""))
        if not checksum and path.is_file():
            checksum = file_sha256(path)
        if checksum:
            expected_source_digest = hashlib.sha256(("FULL_SESSION:" + checksum).encode()).hexdigest()
            current.setdefault(str(row["session_id"]), {})[str(row["segment_id"])] = expected_source_digest

    cur.execute("SELECT t.*,s.status AS session_status,s.completeness,s.metadata_json AS session_metadata FROM transcripts t JOIN live_sessions s ON s.session_id=t.session_id ORDER BY t.transcript_id")
    transcripts = {str(row["transcript_id"]): dict(row) for row in cur.fetchall()}
    transcript_backfills = []
    kinds = {}
    for transcript_id, row in transcripts.items():
        kind = transcript_kind(row, current.get(str(row["session_id"]), set()))
        kinds[transcript_id] = kind
        if kind == "SAMPLE":
            scope, qualification = TRANSCRIPT_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING
        elif kind == QUALIFIED:
            scope, qualification = TRANSCRIPT_SCOPE_FULL, QUALIFIED
        elif row.get("status") == "CANCELLED_SUPERSEDED_SOURCE":
            scope, qualification = "SOURCE_SEGMENT", "CANCELLED_SUPERSEDED_SOURCE"
        elif str(row.get("output_path") or "").lower().endswith(".md"):
            scope, qualification = "HISTORICAL_TRANSCRIPT", "NONQUALIFYING_LEGACY"
        else:
            scope, qualification = "SOURCE_SEGMENT", "NOT_FORMAL_ELIGIBLE"
        transcript_backfills.append({"transcript_id": transcript_id, "scope": scope, "qualification_status": qualification})

    cur.execute("SELECT * FROM analyses ORDER BY analysis_id")
    analyses = [dict(row) for row in cur.fetchall()]
    sample_actions, formal_annotations, analysis_classifications, issues = [], [], [], []
    for analysis in analyses:
        analysis_id = str(analysis["analysis_id"])
        cur.execute("SELECT edge_id,upstream_id,upstream_version,state FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=%s AND upstream_type='transcript' ORDER BY edge_id", (analysis_id,))
        lineage_rows = [dict(row) for row in cur.fetchall()]
        transcript_ids = sorted({str(row["upstream_id"]) for row in lineage_rows})
        candidate_ids = [tid for tid in transcript_ids if kinds.get(tid) in {"SAMPLE", QUALIFIED}]
        if analysis["analysis_type"] != "single_session":
            analysis_classifications.append({"analysis_id": analysis_id, "scope": "HISTORICAL_FORMAL", "qualification_status": "HISTORICAL_IMPORTED"})
            continue
        if not candidate_ids:
            analysis_classifications.append({"analysis_id": analysis_id, "scope": "HISTORICAL_SINGLE_SESSION", "qualification_status": "NONQUALIFYING_LEGACY"})
            continue
        identity, error = artifact_identity(analysis.get("output_path"))
        if error or not artifact_matches(identity, analysis, candidate_ids):
            issues.append({"analysis_id": analysis_id, "reason": error or "artifact root analysis/session/transcript identity does not match database lineage", "artifact_identity": identity, "database_session_id": analysis["session_id"], "lineage_transcript_ids": transcript_ids})
            continue
        transcript_id = identity["transcript_id"]
        transcript_path = Path(str(transcripts[transcript_id].get("output_path") or ""))
        if not transcript_path.is_file():
            issues.append({"analysis_id": analysis_id, "reason": "transcript artifact missing", "transcript_id": transcript_id})
            continue
        immutable = {
            "transcript_content_digest": file_sha256(transcript_path),
            "artifact_digest": identity["artifact_digest"],
            "model_version": str(json_object(analysis.get("metadata_json")).get("model") or json_object(analysis.get("metadata_json")).get("semantic_engine") or MODEL_VERSION),
        }
        if kinds[transcript_id] == "SAMPLE":
            sample_edges = [row for row in lineage_rows if str(row["upstream_id"]) == transcript_id]
            item = {"analysis_id": analysis_id, "session_id": analysis["session_id"], "transcript_id": transcript_id, "analysis_status": analysis["status"], "analysis_lineage_state": analysis["lineage_state"], "analysis_spec_version": "legacy-sample-analysis-v1", "prompt_version": "legacy-unversioned", "lineage_edges": sample_edges, **immutable}
            sample_actions.append(item)
            analysis_classifications.append({"analysis_id": analysis_id, "scope": ANALYSIS_SCOPE_SAMPLE, "qualification_status": SAMPLE_NONQUALIFYING})
        else:
            current_edges = [row for row in lineage_rows if row["state"] == "CURRENT"]
            selected_current = [row for row in current_edges if str(row["upstream_id"]) == transcript_id]
            if len(current_edges) != 1 or len(selected_current) != 1:
                issues.append({"analysis_id": analysis_id, "reason": "formal analysis must have exactly one CURRENT transcript edge", "current_edges": current_edges})
                continue
            item = {"analysis_id": analysis_id, "session_id": analysis["session_id"], "transcript_id": transcript_id, "analysis_spec_version": ANALYSIS_SPEC_VERSION, "prompt_version": PROMPT_VERSION, "lineage_edge": selected_current[0], **immutable}
            formal_annotations.append(item)
            analysis_classifications.append({"analysis_id": analysis_id, "scope": ANALYSIS_SCOPE_FORMAL, "qualification_status": QUALIFIED})

    projected = {}
    action_by_id = {row["analysis_id"]: row for row in sample_actions + formal_annotations}
    for analysis in analyses:
        action = action_by_id.get(str(analysis["analysis_id"]))
        transcript_id = action.get("transcript_id") if action else analysis.get("transcript_id")
        spec = action.get("analysis_spec_version") if action else analysis.get("analysis_spec_version")
        content = action.get("transcript_content_digest") if action else analysis.get("transcript_content_digest")
        model = action.get("model_version") if action else analysis.get("model_version")
        prompt = action.get("prompt_version") if action else analysis.get("prompt_version")
        if not transcript_id or not spec or not content or not model or not prompt:
            continue
        key = (str(transcript_id), str(analysis["analysis_type"]), str(content), str(spec), str(model), str(prompt))
        if key in projected:
            issues.append({"reason": "projected unique transcript/spec conflict", "key": key, "analysis_ids": [projected[key], analysis["analysis_id"]]})
        else:
            projected[key] = analysis["analysis_id"]

    sample_ids = {row["analysis_id"] for row in sample_actions}
    formal_ids = {row["analysis_id"] for row in formal_annotations}
    cur.execute("SELECT bundle_id,object_type,object_id FROM evidence_bundles ORDER BY bundle_id")
    evidence_classifications = []
    for row in cur.fetchall():
        aid = str(row["object_id"])
        if row["object_type"] == "analysis" and aid in sample_ids:
            scope, qualification = ANALYSIS_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING
        elif row["object_type"] == "analysis" and aid in formal_ids:
            scope, qualification = ANALYSIS_SCOPE_FORMAL, QUALIFIED
        else:
            scope, qualification = "OTHER_EVIDENCE", "NOT_APPLICABLE"
        evidence_classifications.append({"bundle_id": row["bundle_id"], "scope": scope, "qualification_status": qualification})

    cur.execute("SELECT outbox_id,object_type,object_id FROM outbox ORDER BY outbox_id")
    outbox_classifications = []
    outbox_map = {}
    for row in cur.fetchall():
        aid = str(row["object_id"])
        if row["object_type"] == "semantic_projection" and aid in sample_ids:
            scope, qualification = ANALYSIS_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING
        elif row["object_type"] == "semantic_projection" and aid in formal_ids:
            scope, qualification = ANALYSIS_SCOPE_FORMAL, QUALIFIED
        else:
            scope, qualification = "CONTROL_PLANE", "NOT_APPLICABLE"
        outbox_map[str(row["outbox_id"])] = (scope, qualification)
        outbox_classifications.append({"outbox_id": row["outbox_id"], "scope": scope, "qualification_status": qualification})
    cur.execute("SELECT receipt_id,outbox_id FROM delivery_receipts ORDER BY receipt_id")
    receipt_classifications = [{"receipt_id": row["receipt_id"], "scope": outbox_map.get(str(row["outbox_id"]), ("CONTROL_PLANE", "NOT_APPLICABLE"))[0], "qualification_status": outbox_map.get(str(row["outbox_id"]), ("CONTROL_PLANE", "NOT_APPLICABLE"))[1]} for row in cur.fetchall()]

    core = {
        "migration_key": MIGRATION_KEY,
        "migration_version": MIGRATION_VERSION,
        "source_schema_signature": schema_signature(cur),
        "ddl_sha256": hashlib.sha256(DDL_PATH.read_bytes()).hexdigest(),
        "transcript_backfills": transcript_backfills,
        "analysis_classifications": analysis_classifications,
        "sample_actions": sample_actions,
        "formal_annotations": formal_annotations,
        "evidence_classifications": evidence_classifications,
        "outbox_classifications": outbox_classifications,
        "receipt_classifications": receipt_classifications,
        "issues": issues,
    }
    return {**core, "plan_sha256": digest(core)}


def readonly_plan(dsn: str | None = None) -> dict:
    conn = psycopg.connect(dsn or v3_runtime._postgres_dsn(), autocommit=False, row_factory=dict_row)
    conn.read_only = True
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute("SHOW transaction_read_only")
        read_only = cur.fetchone()["transaction_read_only"]
        if read_only != "on":
            raise RuntimeError("audit connection is not READ ONLY")
        return {**build_plan(cur), "schema_missing_before_migration": schema_missing(cur), "transaction_read_only": read_only}


def rows_for_backup(cur) -> dict:
    result = {}
    for table in ("transcripts", "analyses", "lineage_edges", "evidence_bundles", "outbox", "delivery_receipts"):
        cur.execute(f"SELECT * FROM {table}")
        result[table] = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT * FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
    result["schema_meta"] = [dict(row) for row in cur.fetchall()]
    return result


def database_identity(cur) -> dict:
    cur.execute("SELECT current_database() AS database_name,current_user AS database_user,COALESCE(inet_server_addr()::text,'local-socket') AS server_address,inet_server_port() AS server_port,current_setting('server_version_num') AS server_version_num,(SELECT system_identifier::text FROM pg_control_system()) AS system_identifier")
    return dict(cur.fetchone())


def merge_metadata(cur, table: str, id_column: str, row_id: str, additions: dict) -> None:
    cur.execute(f"SELECT metadata_json FROM {table} WHERE {id_column}=%s", (row_id,))
    row = cur.fetchone()
    metadata = {**json_object(row["metadata_json"] if row else None), **additions}
    cur.execute(f"UPDATE {table} SET metadata_json=%s WHERE {id_column}=%s", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row_id))


def apply(expected_plan_sha256: str, backup_dir: Path, dsn: str | None = None) -> dict:
    conn = psycopg.connect(dsn or v3_runtime._postgres_dsn(), autocommit=False, row_factory=dict_row)
    conn.isolation_level = IsolationLevel.SERIALIZABLE
    with conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('edu_sample_analysis_qualification'))")
        cur.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,))
        existing = cur.fetchone()
        if existing and existing["value"] == MIGRATION_VERSION:
            health = migration_health(cur)
            if not health["healthy"]:
                raise RuntimeError("migration marker exists but database drift was detected: " + canonical_json(health))
            return {"status": "ALREADY_APPLIED", "migration_version": MIGRATION_VERSION, "health": health}
        plan = build_plan(cur)
        if plan["issues"]:
            raise RuntimeError("migration plan has unresolved issues")
        if plan["plan_sha256"] != expected_plan_sha256:
            raise RuntimeError(f"plan changed: expected {expected_plan_sha256}, actual {plan['plan_sha256']}")
        backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backup_dir, 0o700)
        backup = {"created_at": now(), "database_identity": database_identity(cur), "plan": plan, "rows": rows_for_backup(cur)}
        backup_path = backup_dir / f"pre-sample-analysis-qualification-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
        backup_path.write_text(json.dumps(backup, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        os.chmod(backup_path, 0o600)
        backup_sha = hashlib.sha256(backup_path.read_bytes()).hexdigest()

        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
        changed_at = now()
        for item in plan["transcript_backfills"]:
            cur.execute("UPDATE transcripts SET scope=%s,qualification_status=%s WHERE transcript_id=%s", (item["scope"], item["qualification_status"], item["transcript_id"]))
            if item["qualification_status"] in {QUALIFIED, SAMPLE_NONQUALIFYING}:
                merge_metadata(cur, "transcripts", "transcript_id", item["transcript_id"], {"scope": item["scope"], "qualification_state": item["qualification_status"], "formal_analysis_eligible": item["qualification_status"] == QUALIFIED, "qualification_checked_at": changed_at})
        identity_backfills = {item["analysis_id"] for item in plan["sample_actions"] + plan["formal_annotations"]}
        for item in plan["analysis_classifications"]:
            if item["analysis_id"] in identity_backfills:
                continue
            cur.execute("UPDATE analyses SET scope=%s,qualification_status=%s WHERE analysis_id=%s", (item["scope"], item["qualification_status"], item["analysis_id"]))
        for action in plan["sample_actions"]:
            merge_metadata(cur, "analyses", "analysis_id", action["analysis_id"], {"qualification_state": SAMPLE_NONQUALIFYING, "formal_analysis_eligible": False, "source_transcript_id": action["transcript_id"], "invalidated_at": changed_at, "invalidation_reason": "300-second SAMPLE cannot represent a whole live session", "pre_invalidation_status": action["analysis_status"], "pre_invalidation_lineage_state": action["analysis_lineage_state"]})
            cur.execute("UPDATE analyses SET transcript_id=%s,status='SAMPLE_NONQUALIFYING',lineage_state='INVALIDATED',scope=%s,qualification_status=%s,transcript_content_digest=%s,source_digest=%s,analysis_spec_version=%s,model_version=%s,prompt_version=%s,artifact_digest=%s WHERE analysis_id=%s", (action["transcript_id"], ANALYSIS_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING, action["transcript_content_digest"], action["transcript_content_digest"], action["analysis_spec_version"], action["model_version"], action["prompt_version"], action["artifact_digest"], action["analysis_id"]))
            cur.execute("UPDATE lineage_edges SET state='INVALIDATED' WHERE downstream_type='analysis' AND downstream_id=%s AND upstream_type='transcript' AND upstream_id=%s", (action["analysis_id"], action["transcript_id"]))
        for action in plan["formal_annotations"]:
            merge_metadata(cur, "analyses", "analysis_id", action["analysis_id"], {"qualification_state": QUALIFIED, "formal_analysis_eligible": True, "source_transcript_id": action["transcript_id"], "transcript_content_digest": action["transcript_content_digest"], "artifact_digest": action["artifact_digest"], "artifact_identity_mode": "LEGACY_ROOT_IDS_PLUS_THREE_WAY_DIGEST", "qualification_checked_at": changed_at})
            cur.execute("UPDATE analyses SET transcript_id=%s,scope=%s,qualification_status=%s,transcript_content_digest=%s,source_digest=%s,analysis_spec_version=%s,model_version=%s,prompt_version=%s,artifact_digest=%s WHERE analysis_id=%s", (action["transcript_id"], ANALYSIS_SCOPE_FORMAL, QUALIFIED, action["transcript_content_digest"], action["transcript_content_digest"], action["analysis_spec_version"], action["model_version"], action["prompt_version"], action["artifact_digest"], action["analysis_id"]))
            cur.execute("UPDATE lineage_edges SET upstream_version=%s WHERE edge_id=%s AND state='CURRENT'", (action["transcript_content_digest"], action["lineage_edge"]["edge_id"]))
        for item in plan["evidence_classifications"]:
            cur.execute("UPDATE evidence_bundles SET scope=%s,qualification_status=%s WHERE bundle_id=%s", (item["scope"], item["qualification_status"], item["bundle_id"]))
            if item["qualification_status"] == SAMPLE_NONQUALIFYING:
                merge_metadata(cur, "evidence_bundles", "bundle_id", item["bundle_id"], {"qualification_state": SAMPLE_NONQUALIFYING, "formal_analysis_eligible": False, "invalidated_at": changed_at})
                cur.execute("UPDATE evidence_bundles SET status='INVALIDATED_SAMPLE' WHERE bundle_id=%s", (item["bundle_id"],))
            elif item["qualification_status"] == QUALIFIED:
                cur.execute("SELECT object_id FROM evidence_bundles WHERE bundle_id=%s", (item["bundle_id"],))
                bundle_row = cur.fetchone()
                formal = next((row for row in plan["formal_annotations"] if row["analysis_id"] == bundle_row["object_id"]), None) if bundle_row else None
                if formal:
                    merge_metadata(cur, "evidence_bundles", "bundle_id", item["bundle_id"], {"qualification_state": QUALIFIED, "formal_analysis_eligible": True, "transcript_id": formal["transcript_id"], "transcript_content_digest": formal["transcript_content_digest"], "artifact_digest": formal["artifact_digest"], "qualification_checked_at": changed_at})
                    cur.execute("UPDATE evidence_bundles SET status='REQUIRED' WHERE bundle_id=%s", (item["bundle_id"],))
        for item in plan["outbox_classifications"]:
            cur.execute("UPDATE outbox SET scope=%s,qualification_status=%s WHERE outbox_id=%s", (item["scope"], item["qualification_status"], item["outbox_id"]))
        for item in plan["receipt_classifications"]:
            cur.execute("UPDATE delivery_receipts SET scope=%s,qualification_status=%s WHERE receipt_id=%s", (item["scope"], item["qualification_status"], item["receipt_id"]))

        for action in plan["sample_actions"]:
            payload = {"analysis_id": action["analysis_id"], "profile_id": PROFILE_ID, "qualification_state": SAMPLE_NONQUALIFYING, "correction": True, "correction_version": 1}
            payload_hash = digest(payload)
            outbox_id = "out_sample_correction_" + hashlib.sha256(action["analysis_id"].encode()).hexdigest()[:20]
            dedupe_key = f"feishu_base:semantic_projection:{action['analysis_id']}:{payload_hash}"
            cur.execute("INSERT INTO outbox(outbox_id,dedupe_key,object_type,object_id,destination,status,attempts,max_attempts,next_attempt_at,payload_hash,payload_json,scope,qualification_status) VALUES(%s,%s,'semantic_projection',%s,'feishu_base','PENDING',0,8,%s,%s,%s,%s,%s) ON CONFLICT(dedupe_key) DO NOTHING", (outbox_id, dedupe_key, action["analysis_id"], changed_at, payload_hash, canonical_json(payload), ANALYSIS_SCOPE_SAMPLE, SAMPLE_NONQUALIFYING))
            event_id = "evt_sample_invalidation_" + hashlib.sha256(action["analysis_id"].encode()).hexdigest()[:20]
            cur.execute("INSERT INTO domain_events(event_id,event_type,severity,created_at,object_type,object_id,payload_json) VALUES(%s,'SAMPLE_ANALYSIS_INVALIDATED','WARNING',%s,'analysis',%s,%s) ON CONFLICT(event_id) DO NOTHING", (event_id, changed_at, action["analysis_id"], canonical_json({"transcript_id": action["transcript_id"], "correction_outbox_id": outbox_id})))
        cur.execute("INSERT INTO schema_meta(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=current_timestamp", (MIGRATION_KEY, MIGRATION_VERSION))

        health = migration_health(cur)
        if not health["healthy"]:
            raise RuntimeError("postcondition failed: " + canonical_json(health))
        return {"status": "APPLIED", "migration_version": MIGRATION_VERSION, "plan_sha256": plan["plan_sha256"], "backup_path": str(backup_path), "backup_sha256": backup_sha, "sample_actions": len(plan["sample_actions"]), "formal_annotations": len(plan["formal_annotations"]), "postconditions": health}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--dsn", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.apply:
        if not args.expected_plan_sha256 or not args.backup_dir:
            parser.error("--apply requires --expected-plan-sha256 and --backup-dir")
        result = apply(args.expected_plan_sha256, args.backup_dir, args.dsn)
    else:
        result = readonly_plan(args.dsn)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
