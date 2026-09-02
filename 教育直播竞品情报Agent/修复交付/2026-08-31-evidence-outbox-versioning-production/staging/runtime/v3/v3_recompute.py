#!/usr/bin/env python3
"""Deterministic lineage and recompute state transitions.

This module contains no daemon loop.  Pipeline discovery, the recompute
worker, analysis publication and evidence verification share these helpers so
the same immutable identities and state transitions are used everywhere.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from v3_projection import enqueue_verified_analysis_projection_conn
from v3_runtime import utc_now


FORMAL_SCOPE = "FORMAL_SINGLE_SESSION"
QUALIFIED = "FULL_SESSION_QUALIFIED"
CONTENT_DIGEST_VERIFIED = "CONTENT_DIGEST_VERIFIED"
LEGACY_UNVERIFIED = "LEGACY_UNVERIFIED"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def json_object(value: Any) -> dict:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def stable_id(prefix: str, *parts: str, length: int = 24) -> str:
    body = "\x1f".join(str(part) for part in parts)
    return prefix + hashlib.sha256(body.encode("utf-8")).hexdigest()[:length]


def analysis_id_for(
    transcript_id: str,
    transcript_content_digest: str,
    analysis_spec_version: str,
    model_version: str,
    prompt_version: str,
) -> str:
    # Preserve the identity algorithm used by Pipeline V3 before this module
    # existed so an existing target version is found instead of duplicated.
    body = (
        f"{transcript_id}:{transcript_content_digest}:"
        f"{analysis_spec_version}:{model_version}:{prompt_version}"
    )
    return "analysis_" + hashlib.sha256(body.encode()).hexdigest()[:24]


def recompute_request_id_for(
    downstream_id: str,
    upstream_id: str,
    old_digest: str,
    new_digest: str,
    analysis_spec_version: str,
    model_version: str,
    prompt_version: str,
) -> str:
    return stable_id(
        "recompute_", downstream_id, upstream_id, old_digest, new_digest,
        analysis_spec_version, model_version, prompt_version,
    )


def review_id_for(request_id: str, new_digest: str) -> str:
    return stable_id("review_recompute_", request_id, new_digest)


def lineage_id_for(analysis_id: str, transcript_id: str, digest: str) -> str:
    body = f"{analysis_id}:{transcript_id}:{digest}"
    return "lineage_" + hashlib.sha256(body.encode()).hexdigest()[:24]


def transcript_engine_versions(conn, transcript_id: str) -> tuple[str | None, str | None]:
    try:
        row = conn.execute(
            "SELECT engine,model FROM transcripts WHERE transcript_id=?",
            (transcript_id,),
        ).fetchone()
    except Exception as exc:
        # A few focused SQLite unit fixtures intentionally model only the
        # qualification columns.  Production and the canonical schema always
        # carry engine/model; tolerate only SQLite's missing-column shape.
        if exc.__class__.__module__ != "sqlite3" or "no such column" not in str(exc):
            raise
        row = None
    return (
        str(row["engine"] or "") or None,
        str(row["model"] or "") or None,
    ) if row else (None, None)


def insert_verified_lineage_conn(
    conn,
    *,
    analysis_id: str,
    transcript_id: str,
    transcript_content_digest: str,
    state: str,
    analysis_spec_version: str,
    model_version: str,
    prompt_version: str,
    metadata: dict | None = None,
) -> str:
    if not SHA256_RE.fullmatch(str(transcript_content_digest or "")):
        raise ValueError("lineage upstream_version must be a sha256 content digest")
    engine, asr_model = transcript_engine_versions(conn, transcript_id)
    edge_id = lineage_id_for(
        analysis_id, transcript_id, transcript_content_digest
    )
    now = utc_now()
    conn.execute(
        "INSERT OR IGNORE INTO lineage_edges("
        "edge_id,downstream_type,downstream_id,upstream_type,upstream_id,"
        "upstream_version,binding_status,upstream_engine_version,"
        "upstream_model_version,downstream_model_version,"
        "downstream_prompt_version,downstream_schema_version,state,created_at,"
        "updated_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            edge_id, "analysis", analysis_id, "transcript", transcript_id,
            transcript_content_digest, CONTENT_DIGEST_VERIFIED, engine,
            asr_model, model_version, prompt_version, analysis_spec_version,
            state, now, now,
            json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
        ),
    )
    return edge_id


def _verified_old_edge(conn, analysis: dict) -> bool:
    row = conn.execute(
        "SELECT binding_status FROM lineage_edges WHERE downstream_type='analysis' "
        "AND downstream_id=? AND upstream_type='transcript' AND upstream_id=? "
        "AND upstream_version=? AND state IN ('CURRENT','STALE')",
        (
            analysis["analysis_id"], analysis["transcript_id"],
            analysis["transcript_content_digest"],
        ),
    ).fetchone()
    return bool(row and row["binding_status"] == CONTENT_DIGEST_VERIFIED)


def enqueue_recompute_request_conn(
    conn,
    *,
    old_analysis: dict,
    transcript: dict,
    new_transcript_content_digest: str,
    target_analysis_spec_version: str,
    target_model_version: str,
    target_prompt_version: str,
    reasons: list[str],
) -> dict:
    """Create one version-bound request and mark the old result visibly stale."""
    old_digest = str(old_analysis.get("transcript_content_digest") or "")
    transcript_id = str(transcript.get("transcript_id") or "")
    if (
        old_analysis.get("status") != "COMPLETE"
        or old_analysis.get("scope") != FORMAL_SCOPE
        or old_analysis.get("qualification_status") != QUALIFIED
        or old_analysis.get("transcript_id") != transcript_id
        or not SHA256_RE.fullmatch(old_digest)
        or not SHA256_RE.fullmatch(str(new_transcript_content_digest or ""))
        or not _verified_old_edge(conn, old_analysis)
    ):
        return {"created": False, "reason": LEGACY_UNVERIFIED}

    request_id = recompute_request_id_for(
        str(old_analysis["analysis_id"]), transcript_id, old_digest,
        new_transcript_content_digest, target_analysis_spec_version,
        target_model_version, target_prompt_version,
    )
    now = utc_now()
    metadata = {
        "schema_version": 1,
        "reasons": sorted(set(str(reason) for reason in reasons)),
        "old_analysis_id": old_analysis["analysis_id"],
        "old_upstream_digest": old_digest,
        "new_upstream_digest": new_transcript_content_digest,
        "target_analysis_spec_version": target_analysis_spec_version,
        "target_model_version": target_model_version,
        "target_prompt_version": target_prompt_version,
    }
    inserted = conn.execute(
        "INSERT OR IGNORE INTO recompute_requests("
        "request_id,downstream_type,downstream_id,upstream_type,upstream_id,"
        "old_upstream_digest,new_upstream_digest,target_analysis_spec_version,"
        "target_model_version,target_prompt_version,status,next_attempt_at,"
        "created_at,updated_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,"
        "'PENDING',?,?,?,?)",
        (
            request_id, "analysis", old_analysis["analysis_id"],
            "transcript", transcript_id, old_digest,
            new_transcript_content_digest, target_analysis_spec_version,
            target_model_version, target_prompt_version, now, now, now,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True),
        ),
    )
    request = conn.execute(
        "SELECT status FROM recompute_requests WHERE request_id=?",
        (request_id,),
    ).fetchone()
    active = bool(request and request["status"] not in {"COMPLETE", "OBSOLETE"})
    if active:
        old_metadata = {
            **json_object(old_analysis.get("metadata_json")),
            "lineage_stale": True,
            "recompute_request_id": request_id,
            "stale_reasons": metadata["reasons"],
            "stale_detected_at": now,
            "stale_old_upstream_digest": old_digest,
            "stale_new_upstream_digest": new_transcript_content_digest,
        }
        conn.execute(
            "UPDATE analyses SET lineage_state='STALE',metadata_json=?,updated_at=? "
            "WHERE analysis_id=? AND status='COMPLETE' "
            "AND lineage_state IN ('CURRENT','STALE')",
            (
                json.dumps(old_metadata, ensure_ascii=False, sort_keys=True),
                now, old_analysis["analysis_id"],
            ),
        )
        conn.execute(
            "UPDATE lineage_edges SET state='STALE',updated_at=? "
            "WHERE downstream_type='analysis' AND downstream_id=? "
            "AND upstream_type='transcript' AND upstream_id=? "
            "AND upstream_version=? AND state IN ('CURRENT','STALE')",
            (now, old_analysis["analysis_id"], transcript_id, old_digest),
        )
        review_id = review_id_for(request_id, new_transcript_content_digest)
        conn.execute(
            "INSERT OR IGNORE INTO review_items("
            "review_id,object_type,object_id,review_type,status,requested_at,"
            "requested_by,metadata_json) VALUES(?,?,?,?,?,?,?,?)",
            (
                review_id, "recompute_request", request_id,
                "STALE_RECOMPUTE", "PENDING", now, "pipeline-v3",
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            ),
        )
        enqueue_verified_analysis_projection_conn(
            conn,
            analysis_id=old_analysis["analysis_id"],
            action="STALE",
            transition_id=request_id,
            extra_payload={
                "recompute_request_id": request_id,
                "new_upstream_digest": new_transcript_content_digest,
                "correction_version": 1,
            },
        )
    return {
        "created": int(inserted.rowcount or 0) == 1,
        "request_id": request_id,
        "status": request["status"] if request else None,
        "reason": None,
    }


def recompute_candidate_request(conn, analysis_id: str):
    return conn.execute(
        "SELECT * FROM recompute_requests WHERE candidate_analysis_id=? "
        "AND status='CANDIDATE_CREATED' ORDER BY created_at DESC LIMIT 1",
        (analysis_id,),
    ).fetchone()


def finalize_verified_recompute_conn(
    conn,
    *,
    request_id: str,
    candidate_analysis_id: str,
) -> dict:
    """Promote a verified candidate and supersede its stale predecessor."""
    request = conn.execute(
        "SELECT * FROM recompute_requests WHERE request_id=?",
        (request_id,),
    ).fetchone()
    if not request:
        raise ValueError("recompute request missing")
    if request["status"] == "COMPLETE":
        return {"changed": False, "old_analysis_id": request["downstream_id"]}
    if (
        request["status"] != "CANDIDATE_CREATED"
        or request["candidate_analysis_id"] != candidate_analysis_id
    ):
        raise ValueError("recompute request does not own this candidate")
    candidate = conn.execute(
        "SELECT * FROM analyses WHERE analysis_id=?",
        (candidate_analysis_id,),
    ).fetchone()
    evidence = conn.execute(
        "SELECT status FROM evidence_bundles WHERE object_type='analysis' "
        "AND object_id=?",
        (candidate_analysis_id,),
    ).fetchone()
    if (
        not candidate
        or candidate["status"] != "COMPLETE"
        or candidate["lineage_state"] != "CANDIDATE"
        or not evidence
        or evidence["status"] != "VERIFIED"
    ):
        raise ValueError("recompute candidate is not verified and promotable")
    now = utc_now()
    candidate_metadata = {
        **json_object(candidate["metadata_json"]),
        "recompute_promoted_at": now,
        "recompute_request_id": request_id,
    }
    conn.execute(
        "UPDATE analyses SET lineage_state='CURRENT',metadata_json=?,updated_at=? "
        "WHERE analysis_id=? AND status='COMPLETE' AND lineage_state='CANDIDATE'",
        (
            json.dumps(candidate_metadata, ensure_ascii=False, sort_keys=True),
            now, candidate_analysis_id,
        ),
    )
    conn.execute(
        "UPDATE lineage_edges SET state='CURRENT',updated_at=? "
        "WHERE downstream_type='analysis' AND downstream_id=? "
        "AND upstream_type='transcript' AND upstream_id=? "
        "AND upstream_version=? AND state='CANDIDATE' "
        "AND binding_status='CONTENT_DIGEST_VERIFIED'",
        (
            now, candidate_analysis_id, request["upstream_id"],
            request["new_upstream_digest"],
        ),
    )
    old = conn.execute(
        "SELECT metadata_json FROM analyses WHERE analysis_id=?",
        (request["downstream_id"],),
    ).fetchone()
    old_metadata = {
        **json_object(old["metadata_json"] if old else "{}"),
        "lineage_stale": False,
        "superseded_by_analysis_id": candidate_analysis_id,
        "superseded_at": now,
        "recompute_request_id": request_id,
    }
    conn.execute(
        "UPDATE analyses SET lineage_state='SUPERSEDED',metadata_json=?,updated_at=? "
        "WHERE analysis_id=? AND status='COMPLETE' AND lineage_state='STALE'",
        (
            json.dumps(old_metadata, ensure_ascii=False, sort_keys=True),
            now, request["downstream_id"],
        ),
    )
    conn.execute(
        "UPDATE lineage_edges SET state='SUPERSEDED',updated_at=? "
        "WHERE downstream_type='analysis' AND downstream_id=? AND state='STALE'",
        (now, request["downstream_id"]),
    )
    conn.execute(
        "UPDATE recompute_requests SET status='COMPLETE',completed_at=?,updated_at=?,"
        "lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL,last_error_type=NULL,"
        "last_error=NULL WHERE request_id=? AND status='CANDIDATE_CREATED' "
        "AND candidate_analysis_id=?",
        (now, now, request_id, candidate_analysis_id),
    )
    conn.execute(
        "UPDATE review_items SET status='RESOLVED',decided_at=?,decided_by='evidence-v3',"
        "decision_notes=? WHERE object_type='recompute_request' AND object_id=? "
        "AND review_type='STALE_RECOMPUTE' AND status='PENDING'",
        (now, f"verified candidate {candidate_analysis_id} promoted", request_id),
    )
    enqueue_verified_analysis_projection_conn(
        conn,
        analysis_id=request["downstream_id"],
        action="SUPERSEDED",
        transition_id=request_id,
        extra_payload={
            "superseded_by_analysis_id": candidate_analysis_id,
            "recompute_request_id": request_id,
            "correction_version": 2,
        },
    )
    return {
        "changed": True,
        "old_analysis_id": request["downstream_id"],
        "candidate_analysis_id": candidate_analysis_id,
    }
