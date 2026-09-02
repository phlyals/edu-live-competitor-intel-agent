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
    required = (
        analysis["status"] == "COMPLETE"
        and analysis["lineage_state"] == "CURRENT"
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
    edges = conn.execute(
        "SELECT upstream_id,upstream_version FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=? "
        "AND upstream_type='transcript' AND state='CURRENT' ORDER BY edge_id",
        (analysis["analysis_id"],),
    ).fetchall()
    edge_values = [(str(edge["upstream_id"]), str(edge["upstream_version"])) for edge in edges]
    expected_edge = [(str(analysis["transcript_id"] or ""), str(analysis["transcript_content_digest"] or ""))]
    if edge_values != expected_edge:
        return False, "analysis_lineage_identity_mismatch", {"sample": False, "current_edges": edge_values, "expected_edge": expected_edge}
    transcript = conn.execute(
        "SELECT output_path,scope,qualification_status FROM transcripts WHERE transcript_id=?",
        (analysis["transcript_id"],),
    ).fetchone()
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
    return True, "verified", {"sample": False, "identity_mode": identity_mode, "analysis_id": analysis["analysis_id"], "transcript_id": analysis["transcript_id"], "transcript_content_digest": analysis["transcript_content_digest"], "artifact_digest": actual_digest, "analysis_spec_version": analysis["analysis_spec_version"], "model_version": analysis["model_version"], "prompt_version": analysis["prompt_version"]}


def once() -> dict:
    verified = blocked = 0
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
            metadata = {**json_object(row["metadata_json"]), "verified": True, "checked_at": utc_now(), **(details if row["object_type"] == "analysis" else {})}
            conn.execute("UPDATE evidence_bundles SET status='VERIFIED',verified_at=?,manifest_hash=?,metadata_json=? WHERE bundle_id=?", (utc_now(), digest, json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["bundle_id"]))
            verified += 1
        conn.commit()
    result = {"verified": verified, "blocked": blocked, "checked_at": utc_now()}
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
