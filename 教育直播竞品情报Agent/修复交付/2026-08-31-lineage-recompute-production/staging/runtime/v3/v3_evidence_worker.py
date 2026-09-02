#!/usr/bin/env python3
"""Verify evidence bundles before any downstream retention or approval."""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
import time
from pathlib import Path

from v3_runtime import connect, init_db, upsert_heartbeat, utc_now
from v3_analysis_contract import file_sha256
from v3_analysis_worker import (
    _json_digest,
    bind_transcript_segments,
    source_reference_manifest,
)
from v3_recompute import (
    finalize_verified_recompute_conn,
    recompute_candidate_request,
)

RUNNING = True


def stop(*_args):
    global RUNNING
    RUNNING = False


def json_object(value) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def block_bundle(conn, row, reason: str, details: dict | None = None, *, sample: bool = False) -> None:
    metadata = {
        **json_object(row["metadata_json"]),
        "reason": reason,
        "qualification_state": "SAMPLE_NONQUALIFYING" if sample else "SOURCE_NONQUALIFYING",
        "checked_at": utc_now(),
        **(details or {}),
    }
    conn.execute(
        "UPDATE evidence_bundles SET status=?,verified_at=NULL,scope=?,qualification_status=?,metadata_json=? WHERE bundle_id=?",
        ("INVALIDATED_SAMPLE" if sample else "BLOCKED_EVIDENCE",
         "SAMPLE_AUXILIARY" if sample else row["scope"],
         "SAMPLE_NONQUALIFYING" if sample else "SOURCE_NONQUALIFYING",
         json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["bundle_id"]),
    )


def verify_strict_source_binding(
    artifact: dict,
    transcript_path: Path,
    analysis,
) -> tuple[bool, str, dict]:
    """Cross-check every model citation against the immutable transcript."""
    result = artifact.get("result")
    binding = artifact.get("evidence")
    result_binding = result.get("evidence_binding") if isinstance(result, dict) else None
    if not isinstance(result, dict) or not isinstance(binding, dict):
        return False, "strict_source_binding_missing", {}
    if (
        binding.get("mode") != "STRICT_SOURCE_SEGMENT_IDS"
        or binding.get("model_generated_timestamps") is not False
        or binding.get("nearest_segment_fallback") is not False
        or not isinstance(result_binding, dict)
        or result_binding.get("mode") != "STRICT_SOURCE_SEGMENT_IDS"
        or result_binding.get("model_generated_timestamps") is not False
        or result_binding.get("nearest_segment_fallback") is not False
    ):
        return False, "strict_source_binding_policy_mismatch", {}
    try:
        manifest = source_reference_manifest(result)
        transcript_payload = json.loads(transcript_path.read_text(encoding="utf-8"))
        source_rows = bind_transcript_segments(transcript_payload.get("segments"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return False, "strict_source_binding_invalid", {"error": str(exc)[:300]}
    source_by_id = {row["source_segment_id"]: row for row in source_rows}
    if manifest["reference_count"] < 1:
        return False, "strict_source_binding_empty", {}
    if (
        manifest["source_binding_digest"] != binding.get("source_binding_digest")
        or manifest["reference_count"] != binding.get("reference_count")
    ):
        return False, "strict_source_binding_manifest_mismatch", {}
    reference_errors = []
    for reference in manifest["references"]:
        source = source_by_id.get(reference["source_segment_id"])
        if (
            not source
            or float(reference["start"]) != float(source["start"])
            or float(reference["end"]) != float(source["end"])
            or reference["content_digest"] != source["content_digest"]
        ):
            reference_errors.append(reference["source_segment_id"])
    if reference_errors:
        return False, "strict_source_binding_reference_mismatch", {
            "reference_ids": reference_errors[:20],
            "reference_error_count": len(reference_errors),
        }
    source_set_digest = _json_digest([
        {
            "source_segment_id": row["source_segment_id"],
            "content_digest": row["content_digest"],
        }
        for row in source_rows
    ])
    source_text = "\n".join(row["line"] for row in source_rows)
    if (
        binding.get("transcript_id") != analysis["transcript_id"]
        or binding.get("transcript_artifact_sha256") != analysis["transcript_content_digest"]
        or binding.get("source_segment_set_digest") != source_set_digest
        or binding.get("source_content_hash") != hashlib.sha256(source_text.encode()).hexdigest()
    ):
        return False, "strict_source_binding_transcript_mismatch", {}
    return True, "strict_source_binding_verified", {
        "evidence_binding_status": "BOUND_V1",
        "source_binding_digest": manifest["source_binding_digest"],
        "referenced_source_segment_count": manifest["reference_count"],
        "source_segment_set_digest": source_set_digest,
    }


def verify_analysis_identity(conn, row, path: Path, actual_digest: str) -> tuple[bool, str, dict]:
    analysis = conn.execute(
        "SELECT analysis_id,session_id,transcript_id,analysis_type,status,output_path,lineage_state,scope,"
        "qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,"
        "artifact_digest,metadata_json FROM analyses WHERE analysis_id=?",
        (row["object_id"],),
    ).fetchone()
    if not analysis:
        return False, "analysis_missing", {}
    analysis_meta = json_object(analysis["metadata_json"])
    sample = bool(
        analysis["status"] == "SAMPLE_NONQUALIFYING"
        or analysis["qualification_status"] == "SAMPLE_NONQUALIFYING"
        or row["qualification_status"] == "SAMPLE_NONQUALIFYING"
        or analysis_meta.get("qualification_state") == "SAMPLE_NONQUALIFYING"
    )
    if sample:
        return False, "sample_analysis_is_nonqualifying", {"sample": True}
    recompute_request = (
        recompute_candidate_request(conn, analysis["analysis_id"])
        if analysis["lineage_state"] == "CANDIDATE"
        else None
    )
    is_recompute_candidate = bool(
        recompute_request
        and analysis_meta.get("recompute_request_id")
        == recompute_request["request_id"]
    )
    required = (
        analysis["status"] == "COMPLETE"
        and (
            analysis["lineage_state"] == "CURRENT"
            or is_recompute_candidate
        )
        and analysis["scope"] == "FORMAL_SINGLE_SESSION"
        and analysis["qualification_status"] == "FULL_SESSION_QUALIFIED"
        and row["scope"] == "FORMAL_SINGLE_SESSION"
        and row["qualification_status"] == "FULL_SESSION_QUALIFIED"
        and analysis_meta.get("qualification_state") == "FULL_SESSION_QUALIFIED"
        and analysis_meta.get("formal_analysis_eligible") is True
    )
    if not required:
        return False, "analysis_source_not_qualified", {"sample": False}
    expected_artifact = str(analysis["artifact_digest"] or "")
    bundle_digest = str(row["manifest_hash"] or "")
    if not expected_artifact or actual_digest != expected_artifact or actual_digest != bundle_digest:
        return False, "artifact_digest_three_way_mismatch", {"sample": False, "actual_digest": actual_digest, "analysis_artifact_digest": expected_artifact or None, "bundle_manifest_hash": bundle_digest or None}
    if str(path) != str(analysis["output_path"] or ""):
        return False, "analysis_bundle_path_mismatch", {"sample": False, "analysis_output_path": analysis["output_path"]}
    expected_edge_state = "CANDIDATE" if is_recompute_candidate else "CURRENT"
    edges = conn.execute(
        "SELECT upstream_id,upstream_version,binding_status,upstream_engine_version,"
        "upstream_model_version,downstream_model_version,downstream_prompt_version,"
        "downstream_schema_version FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=? "
        "AND upstream_type='transcript' AND state=? ORDER BY edge_id",
        (analysis["analysis_id"], expected_edge_state),
    ).fetchall()
    transcript = conn.execute(
        "SELECT output_path,scope,qualification_status,engine,model FROM transcripts WHERE transcript_id=?",
        (analysis["transcript_id"],),
    ).fetchone()
    edge_values = [(
        str(edge["upstream_id"]), str(edge["upstream_version"]),
        str(edge["binding_status"]), edge["upstream_engine_version"],
        edge["upstream_model_version"], edge["downstream_model_version"],
        edge["downstream_prompt_version"], edge["downstream_schema_version"],
    ) for edge in edges]
    expected_edge = [(
        str(analysis["transcript_id"] or ""),
        str(analysis["transcript_content_digest"] or ""),
        "CONTENT_DIGEST_VERIFIED",
        transcript["engine"] if transcript else None,
        transcript["model"] if transcript else None,
        analysis["model_version"], analysis["prompt_version"],
        analysis["analysis_spec_version"],
    )]
    if edge_values != expected_edge:
        return False, "analysis_lineage_identity_mismatch", {"sample": False, "current_edges": edge_values, "expected_edge": expected_edge}
    transcript_path = Path(str(transcript["output_path"] or "")) if transcript else Path("")
    if not transcript or transcript["scope"] != "FULL_SESSION" or transcript["qualification_status"] != "FULL_SESSION_QUALIFIED" or not transcript_path.is_file():
        return False, "qualified_transcript_missing", {"sample": False}
    transcript_digest = file_sha256(transcript_path)
    if transcript_digest != analysis["transcript_content_digest"]:
        return False, "transcript_content_digest_mismatch", {"sample": False, "actual_transcript_digest": transcript_digest, "analysis_transcript_digest": analysis["transcript_content_digest"]}
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False, "analysis_artifact_invalid_json", {"sample": False}
    root_expected = {"analysis_id": analysis["analysis_id"], "session_id": analysis["session_id"], "transcript_id": analysis["transcript_id"]}
    if any(str(artifact.get(key) or "") != str(value or "") for key, value in root_expected.items()):
        return False, "analysis_artifact_root_identity_mismatch", {"sample": False}
    version_expected = {
        "transcript_content_digest": analysis["transcript_content_digest"],
        "analysis_spec_version": analysis["analysis_spec_version"],
        "model_version": analysis["model_version"],
        "prompt_version": analysis["prompt_version"],
    }
    version_fields_present = all(artifact.get(key) is not None for key in version_expected)
    legacy_mode = analysis_meta.get("artifact_identity_mode") == "LEGACY_ROOT_IDS_PLUS_THREE_WAY_DIGEST"
    if version_fields_present:
        if any(str(artifact.get(key)) != str(value) for key, value in version_expected.items()):
            return False, "analysis_artifact_version_identity_mismatch", {"sample": False}
        identity_mode = "VERSIONED_ROOT_IDENTITY"
    elif legacy_mode:
        identity_mode = "LEGACY_ROOT_IDS_PLUS_THREE_WAY_DIGEST"
    else:
        return False, "analysis_artifact_version_identity_missing", {"sample": False}
    strict_details = {}
    if analysis["analysis_spec_version"] == "single-session-evidence-v4-source-ids":
        strict_ok, strict_reason, strict_details = verify_strict_source_binding(
            artifact, transcript_path, analysis,
        )
        if not strict_ok:
            return False, strict_reason, {"sample": False, **strict_details}
        identity_mode = "VERSIONED_ROOT_IDENTITY_AND_STRICT_SOURCE_IDS"
    return True, "verified", {"sample": False, "identity_mode": identity_mode, "analysis_id": analysis["analysis_id"], "transcript_id": analysis["transcript_id"], "transcript_content_digest": analysis["transcript_content_digest"], "artifact_digest": actual_digest, "analysis_spec_version": analysis["analysis_spec_version"], "model_version": analysis["model_version"], "prompt_version": analysis["prompt_version"], "is_recompute_candidate": is_recompute_candidate, "recompute_request_id": recompute_request["request_id"] if is_recompute_candidate else None, **strict_details}


def release_held_projections(conn, analysis_id: str) -> int:
    """Release only outbox rows explicitly gated by this verified evidence."""
    released = 0
    rows = conn.execute(
        "SELECT outbox_id,payload_json FROM outbox WHERE status='HELD_EVIDENCE'"
    ).fetchall()
    for row in rows:
        payload = json_object(row["payload_json"])
        if payload.get("release_after_evidence_analysis_id") != analysis_id:
            continue
        cursor = conn.execute(
            "UPDATE outbox SET status='PENDING',next_attempt_at=?,lease_owner=NULL,"
            "lease_until=NULL WHERE outbox_id=? AND status='HELD_EVIDENCE'",
            (utc_now(), row["outbox_id"]),
        )
        released += cursor.rowcount
    return released


def once() -> dict:
    verified = blocked = released = 0
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM evidence_bundles WHERE status IN ('REQUIRED','RETRY')").fetchall()
        for row in rows:
            path = Path(str(row["manifest_path"] or ""))
            if not path.is_file():
                block_bundle(conn, row, "manifest_missing")
                blocked += 1
                continue
            digest = file_sha256(path)
            if row["object_type"] == "analysis":
                ok, reason, details = verify_analysis_identity(conn, row, path, digest)
                if not ok:
                    block_bundle(conn, row, reason, details, sample=bool(details.get("sample")))
                    blocked += 1
                    continue
            elif row["manifest_hash"] and digest != row["manifest_hash"]:
                block_bundle(conn, row, "manifest_hash_mismatch", {"actual": digest, "expected": row["manifest_hash"]})
                blocked += 1
                continue
            original_metadata = json_object(row["metadata_json"])
            metadata = {**original_metadata, "verified": True, "checked_at": utc_now(), **(details if row["object_type"] == "analysis" else {})}
            conn.execute("UPDATE evidence_bundles SET status='VERIFIED',verified_at=?,manifest_hash=?,metadata_json=? WHERE bundle_id=?", (utc_now(), digest, json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["bundle_id"]))
            if (
                row["object_type"] == "analysis"
                and details.get("is_recompute_candidate") is True
            ):
                try:
                    finalize_verified_recompute_conn(
                        conn,
                        request_id=details["recompute_request_id"],
                        candidate_analysis_id=row["object_id"],
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    block_bundle(
                        conn, row, "recompute_promotion_failed",
                        {"error": str(exc)},
                    )
                    blocked += 1
                    continue
            if (
                row["object_type"] == "analysis"
                and original_metadata.get("release_projection_after_evidence") is True
            ):
                released += release_held_projections(conn, row["object_id"])
            verified += 1
        conn.commit()
    result = {"verified": verified, "blocked": blocked, "released_projections": released, "checked_at": utc_now()}
    upsert_heartbeat("evidence-v3", "READY" if blocked == 0 else "DEGRADED", result, success=blocked == 0)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once", "daemon"))
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    if args.command == "once":
        print(json.dumps(once(), ensure_ascii=False, indent=2))
        return 0
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while RUNNING:
        once()
        time.sleep(max(15, args.interval))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
