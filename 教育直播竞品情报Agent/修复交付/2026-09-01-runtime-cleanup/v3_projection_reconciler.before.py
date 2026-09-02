#!/usr/bin/env python3
"""Reconcile evidence, versioned outbox rows and real delivery receipts."""
from __future__ import annotations

import argparse
import hashlib
import json

from v3_projection import (
    enqueue_verified_analysis_projection_conn,
    json_object,
    projection_version_for,
)
from v3_runtime import connect, init_db, upsert_heartbeat, utc_now


def stable_id(prefix: str, *parts: str) -> str:
    return prefix + hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:24]


def record(conn, *, issue: str, object_id: str, status: str, differences: list[dict]) -> None:
    reconciliation_id = stable_id("projection_reconcile_", issue, object_id)
    conn.execute(
        "INSERT INTO projection_reconciliations("
        "reconciliation_id,table_name,table_id,run_id,direction,status,"
        "missing_count,extra_count,mismatch_count,differences_json,checked_at) "
        "VALUES(?,?,?,NULL,'LOCAL_TO_REMOTE',?,?,?,?,?,?) "
        "ON CONFLICT(reconciliation_id) DO UPDATE SET status=excluded.status,"
        "missing_count=excluded.missing_count,extra_count=excluded.extra_count,"
        "mismatch_count=excluded.mismatch_count,differences_json=excluded.differences_json,"
        "checked_at=excluded.checked_at",
        (
            reconciliation_id, issue, object_id, status,
            int(issue in {"MISSING_OUTBOX", "MISSING_RECEIPT"}), 0,
            int(issue not in {"MISSING_OUTBOX", "MISSING_RECEIPT"}),
            json.dumps(differences, ensure_ascii=False, sort_keys=True), utc_now(),
        ),
    )


def _expected_projection(analysis: dict) -> tuple[str, str] | None:
    lineage = str(analysis.get("lineage_state") or "")
    metadata = json_object(analysis.get("metadata_json"))
    request_id = str(metadata.get("recompute_request_id") or "")
    if lineage == "CURRENT":
        action = "CURRENT"
    elif lineage in {"STALE", "SUPERSEDED"} and request_id:
        action = lineage
    else:
        return None
    version = projection_version_for(
        analysis_id=analysis["analysis_id"],
        artifact_digest=analysis["artifact_digest"],
        analysis_spec_version=str(analysis.get("analysis_spec_version") or ""),
        model_version=str(analysis.get("model_version") or ""),
        prompt_version=str(analysis.get("prompt_version") or ""),
        action=action,
        transition_id=request_id,
    )
    return action, version


def reconcile_once(*, connect_fn=None) -> dict:
    recreated = blocked = missing_receipts = receipt_mismatches = 0
    tasks_completed = legacy_held_cancelled = 0
    connection_factory = connect_fn or connect
    with connection_factory() as conn:
        conn.execute("BEGIN IMMEDIATE")
        analyses = [dict(row) for row in conn.execute(
            "SELECT a.* FROM analyses a JOIN evidence_bundles e "
            "ON e.object_type='analysis' AND e.object_id=a.analysis_id "
            "WHERE a.analysis_type='single_session' AND a.status='COMPLETE' "
            "AND a.scope='FORMAL_SINGLE_SESSION' "
            "AND a.qualification_status='FULL_SESSION_QUALIFIED' "
            "AND e.status='VERIFIED' AND e.manifest_hash=a.artifact_digest"
        ).fetchall()]
        for analysis in analyses:
            expected = _expected_projection(analysis)
            if not expected:
                continue
            action, version = expected
            existing = conn.execute(
                "SELECT outbox_id FROM outbox WHERE destination='feishu_base' "
                "AND object_type='semantic_projection' AND object_id=? "
                "AND projection_version=?",
                (analysis["analysis_id"], version),
            ).fetchone()
            if not existing:
                metadata = json_object(analysis.get("metadata_json"))
                enqueue_verified_analysis_projection_conn(
                    conn,
                    analysis_id=analysis["analysis_id"], action=action,
                    transition_id=str(metadata.get("recompute_request_id") or ""),
                    extra_payload={
                        "reconciled_after_verified_evidence": True,
                    },
                )
                recreated += 1
                record(
                    conn, issue="MISSING_OUTBOX", object_id=analysis["analysis_id"],
                    status="REPAIRED", differences=[{"projection_version": version}],
                )

        held = conn.execute(
            "SELECT o.outbox_id,o.object_id,e.status AS evidence_status "
            "FROM outbox o LEFT JOIN evidence_bundles e "
            "ON e.object_type='analysis' AND e.object_id=o.object_id "
            "WHERE o.object_type='semantic_projection' AND o.status='HELD_EVIDENCE'"
        ).fetchall()
        for row in held:
            status = (
                "CANCELLED_VERSIONED_REPLACEMENT"
                if row["evidence_status"] == "VERIFIED"
                else "BLOCKED_EVIDENCE"
            )
            conn.execute(
                "UPDATE outbox SET status=?,lease_owner=NULL,lease_until=NULL,"
                "last_error_type='LEGACY_PRE_EVIDENCE_OUTBOX',"
                "last_error='reconciled without dispatch' WHERE outbox_id=? "
                "AND status='HELD_EVIDENCE'",
                (status, row["outbox_id"]),
            )
            legacy_held_cancelled += 1

        versioned = conn.execute(
            "SELECT o.*,a.artifact_digest AS analysis_artifact,e.bundle_id AS actual_bundle_id,"
            "e.status AS evidence_status,e.manifest_hash AS actual_manifest_hash,"
            "e.verified_at AS actual_verified_at,r.receipt_id,r.status AS receipt_status,"
            "r.payload_hash AS receipt_payload_hash,r.projection_version AS receipt_projection_version,"
            "r.artifact_digest AS receipt_artifact_digest FROM outbox o "
            "LEFT JOIN analyses a ON a.analysis_id=o.object_id "
            "LEFT JOIN evidence_bundles e ON e.object_type='analysis' AND e.object_id=o.object_id "
            "LEFT JOIN delivery_receipts r ON r.outbox_id=o.outbox_id "
            "WHERE o.object_type='semantic_projection' "
            "AND o.projection_binding_status='VERSIONED_EVIDENCE'"
        ).fetchall()
        for row in versioned:
            payload = json_object(row["payload_json"])
            errors = []
            for field, actual in (
                ("artifact_digest", row["analysis_artifact"]),
                ("evidence_bundle_id", row["actual_bundle_id"]),
                ("evidence_manifest_hash", row["actual_manifest_hash"]),
                ("evidence_verified_at", row["actual_verified_at"]),
                ("projection_version", row["projection_version"]),
            ):
                if str(row[field] or "") != str(actual or ""):
                    errors.append({"field": field, "stored": row[field], "actual": actual})
                if str(payload.get(field) or "") != str(row[field] or ""):
                    errors.append({"field": "payload." + field})
            if row["evidence_status"] != "VERIFIED" or row["projection_binding_status"] != "VERSIONED_EVIDENCE":
                errors.append({"field": "evidence_status", "value": row["evidence_status"]})
            if errors:
                if row["status"] in {"PENDING", "RETRY", "IN_FLIGHT"}:
                    conn.execute(
                        "UPDATE outbox SET status='BLOCKED_EVIDENCE',lease_owner=NULL,"
                        "lease_until=NULL,last_error_type='PROJECTION_EVIDENCE_MISMATCH',"
                        "last_error=? WHERE outbox_id=?",
                        (json.dumps(errors, ensure_ascii=False)[:2000], row["outbox_id"]),
                    )
                    blocked += 1
                record(
                    conn, issue="PROJECTION_EVIDENCE_MISMATCH",
                    object_id=row["outbox_id"], status="BLOCKED",
                    differences=errors,
                )
                continue
            if row["status"] == "SENT" and not row["receipt_id"]:
                missing_receipts += 1
                record(
                    conn, issue="MISSING_RECEIPT", object_id=row["outbox_id"],
                    status="REVIEW_REQUIRED",
                    differences=[{"reason": "SENT has no third-party receipt"}],
                )
                review_id = stable_id("review_projection_", row["outbox_id"])
                conn.execute(
                    "INSERT OR IGNORE INTO review_items("
                    "review_id,object_type,object_id,review_type,status,requested_at,"
                    "requested_by,metadata_json) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        review_id, "outbox", row["outbox_id"],
                        "DELIVERY_RECEIPT_MISSING", "PENDING", utc_now(),
                        "projection-reconciler",
                        json.dumps({"projection_version": row["projection_version"]}),
                    ),
                )
            elif row["receipt_id"]:
                receipt_errors = []
                if row["receipt_payload_hash"] != row["payload_hash"]:
                    receipt_errors.append({"field": "payload_hash"})
                if row["receipt_projection_version"] != row["projection_version"]:
                    receipt_errors.append({"field": "projection_version"})
                if row["receipt_artifact_digest"] != row["artifact_digest"]:
                    receipt_errors.append({"field": "artifact_digest"})
                if receipt_errors:
                    receipt_mismatches += 1
                    record(
                        conn, issue="RECEIPT_IDENTITY_MISMATCH",
                        object_id=row["outbox_id"], status="REVIEW_REQUIRED",
                        differences=receipt_errors,
                    )
                else:
                    record(
                        conn, issue="MISSING_RECEIPT", object_id=row["outbox_id"],
                        status="RESOLVED", differences=[],
                    )

        legacy_missing = conn.execute(
            "SELECT o.outbox_id,o.projection_version FROM outbox o "
            "LEFT JOIN delivery_receipts r ON r.outbox_id=o.outbox_id "
            "WHERE o.object_type='semantic_projection' AND o.status='SENT' "
            "AND o.projection_binding_status<>'VERSIONED_EVIDENCE' "
            "AND r.outbox_id IS NULL"
        ).fetchall()
        for row in legacy_missing:
            missing_receipts += 1
            record(
                conn, issue="MISSING_RECEIPT", object_id=row["outbox_id"],
                status="REVIEW_REQUIRED",
                differences=[{"reason": "legacy SENT has no receipt"}],
            )

        task_rows = conn.execute(
            "SELECT o.payload_json,t.task_id FROM outbox o "
            "JOIN delivery_receipts r ON r.outbox_id=o.outbox_id "
            "JOIN tasks t ON t.status='DELIVERY_PENDING' "
            "WHERE o.object_type='scan_result' AND o.status='SENT' "
            "AND r.status='VERIFIED' AND r.payload_hash=o.payload_hash"
        ).fetchall()
        for row in task_rows:
            payload = json_object(row["payload_json"])
            if payload.get("task_id") != row["task_id"]:
                continue
            cursor = conn.execute(
                "UPDATE tasks SET status='COMPLETE',business_state='COMPLETE',"
                "runtime_state='IDLE',delivery_state='VERIFIED',error_type=NULL,"
                "error_message=NULL,next_attempt_at=NULL,last_success_at=?,updated_at=? "
                "WHERE task_id=? AND status='DELIVERY_PENDING'",
                (utc_now(), utc_now(), row["task_id"]),
            )
            tasks_completed += int(cursor.rowcount or 0)
        conn.commit()
    return {
        "recreated_outbox": recreated,
        "blocked_outbox": blocked,
        "missing_receipts": missing_receipts,
        "receipt_mismatches": receipt_mismatches,
        "tasks_completed": tasks_completed,
        "legacy_held_cancelled": legacy_held_cancelled,
        "checked_at": utc_now(),
    }


def once() -> dict:
    init_db()
    result = reconcile_once()
    healthy = not any((
        result["blocked_outbox"], result["missing_receipts"],
        result["receipt_mismatches"],
    ))
    upsert_heartbeat(
        "projection-reconciler-v3", "READY" if healthy else "DEGRADED",
        result, success=healthy,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
