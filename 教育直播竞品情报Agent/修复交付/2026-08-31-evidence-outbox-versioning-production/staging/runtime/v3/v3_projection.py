#!/usr/bin/env python3
"""Evidence-gated, versioned semantic projection identities."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from v3_runtime import enqueue_outbox_conn


FORMAL_SCOPE = "FORMAL_SINGLE_SESSION"
QUALIFIED = "FULL_SESSION_QUALIFIED"
VERSIONED_EVIDENCE = "VERSIONED_EVIDENCE"


def json_object(value: Any) -> dict:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def projection_version_for(
    *,
    analysis_id: str,
    artifact_digest: str,
    analysis_spec_version: str,
    model_version: str,
    prompt_version: str,
    action: str,
    transition_id: str = "",
) -> str:
    body = "\x1f".join((
        analysis_id, artifact_digest, analysis_spec_version,
        model_version, prompt_version, action, transition_id,
    ))
    return "projection_" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _verified_context(conn, analysis_id: str) -> tuple[dict, dict]:
    analysis = conn.execute(
        "SELECT * FROM analyses WHERE analysis_id=?", (analysis_id,)
    ).fetchone()
    bundle = conn.execute(
        "SELECT * FROM evidence_bundles WHERE object_type='analysis' "
        "AND object_id=?", (analysis_id,),
    ).fetchone()
    if not analysis or not bundle:
        raise ValueError("analysis or evidence bundle missing")
    analysis, bundle = dict(analysis), dict(bundle)
    if (
        analysis.get("status") != "COMPLETE"
        or analysis.get("scope") != FORMAL_SCOPE
        or analysis.get("qualification_status") != QUALIFIED
        or not analysis.get("artifact_digest")
        or bundle.get("status") != "VERIFIED"
        or not bundle.get("verified_at")
        or bundle.get("scope") != FORMAL_SCOPE
        or bundle.get("qualification_status") != QUALIFIED
        or bundle.get("manifest_hash") != analysis.get("artifact_digest")
    ):
        raise ValueError("analysis projection lacks verified evidence identity")
    return analysis, bundle


def enqueue_verified_analysis_projection_conn(
    conn,
    *,
    analysis_id: str,
    action: str,
    transition_id: str = "",
    extra_payload: dict | None = None,
) -> str:
    analysis, bundle = _verified_context(conn, analysis_id)
    action = str(action).upper()
    expected_lineage = {
        "CURRENT": "CURRENT",
        "STALE": "STALE",
        "SUPERSEDED": "SUPERSEDED",
    }.get(action)
    if expected_lineage and analysis.get("lineage_state") != expected_lineage:
        raise ValueError("projection action does not match analysis lineage state")
    version = projection_version_for(
        analysis_id=analysis_id,
        artifact_digest=analysis["artifact_digest"],
        analysis_spec_version=str(analysis.get("analysis_spec_version") or ""),
        model_version=str(analysis.get("model_version") or ""),
        prompt_version=str(analysis.get("prompt_version") or ""),
        action=action,
        transition_id=transition_id,
    )
    payload = {
        "analysis_id": analysis_id,
        "profile_id": "edu_live_competitor_intel",
        "qualification_state": QUALIFIED,
        "lineage_state": analysis["lineage_state"],
        "artifact_digest": analysis["artifact_digest"],
        "analysis_spec_version": analysis.get("analysis_spec_version"),
        "model_version": analysis.get("model_version"),
        "prompt_version": analysis.get("prompt_version"),
        "transcript_content_digest": analysis.get("transcript_content_digest"),
        "evidence_bundle_id": bundle["bundle_id"],
        "evidence_manifest_hash": bundle["manifest_hash"],
        "evidence_verified_at": bundle["verified_at"],
        "projection_version": version,
        "projection_action": action,
        **(extra_payload or {}),
    }
    return enqueue_outbox_conn(
        conn,
        object_type="semantic_projection",
        object_id=analysis_id,
        destination="feishu_base",
        payload=payload,
        scope=FORMAL_SCOPE,
        qualification_status=QUALIFIED,
        projection_version=version,
        artifact_digest=analysis["artifact_digest"],
        evidence_bundle_id=bundle["bundle_id"],
        evidence_manifest_hash=bundle["manifest_hash"],
        evidence_verified_at=bundle["verified_at"],
        projection_binding_status=VERSIONED_EVIDENCE,
    )

