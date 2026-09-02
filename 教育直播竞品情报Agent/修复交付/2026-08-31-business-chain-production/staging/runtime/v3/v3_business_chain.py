#!/usr/bin/env python3
"""Shared deterministic helpers for comparison, version and knowledge workers."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


COMPARISON_SPEC_VERSION = "structural-comparison-v1"
VERSION_SPEC_VERSION = "three-session-confirmation-v1"
STRATEGY_SPEC_VERSION = "strategy-candidate-v1"
KNOWLEDGE_SPEC_VERSION = "approved-knowledge-v1"


def json_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical_digest(value: Any) -> str:
    body = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: str, length: int = 24) -> str:
    return prefix + hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:length]


def write_json_atomic(path: Path, payload: dict) -> str:
    body = json.dumps(
        payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False
    ) + "\n"
    json.loads(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(body, encoding="utf-8")
        json.loads(temporary.read_text(encoding="utf-8"))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return sha256_file(path)


def count_bucket(value: int) -> str:
    if value <= 0:
        return "0"
    if value <= 3:
        return "1-3"
    if value <= 10:
        return "4-10"
    return "11+"


def structure_features(artifact: dict) -> dict:
    """Extract a wording-insensitive live-room structure signature."""
    result = json_object(artifact.get("result"))
    explicit = result.get("strategy_structure")
    if isinstance(explicit, dict):
        return {str(key): explicit[key] for key in sorted(explicit)}
    modules = result.get("modules") if isinstance(result.get("modules"), list) else []
    module_features = []
    for module in modules:
        if not isinstance(module, dict) or not module.get("name"):
            continue
        references = module.get("timestamps")
        references = references if isinstance(references, list) else []
        module_features.append({
            "name": str(module["name"]),
            "evidence_density": count_bucket(len(references)),
        })
    module_features.sort(key=lambda row: row["name"])
    fields = {}
    for name in (
        "hook", "pain_points", "claims", "cta", "course_content",
        "interaction_patterns", "product_handoff", "risks",
    ):
        value = result.get(name)
        fields[name] = count_bucket(len(value) if isinstance(value, list) else 0)
    return {
        "schema_version": 1,
        "modules": module_features,
        "section_density": fields,
        "has_product_handoff": fields["product_handoff"] != "0",
        "has_cta": fields["cta"] != "0",
        "has_interaction": fields["interaction_patterns"] != "0",
    }


def structure_digest(artifact: dict) -> str:
    return canonical_digest(structure_features(artifact))


def flatten_features(value: Any, prefix: str = "") -> dict[str, str]:
    result = {}
    if isinstance(value, dict):
        for key in sorted(value):
            result.update(flatten_features(value[key], prefix + "." + str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            result.update(flatten_features(item, prefix + f"[{index}]"))
    else:
        result[prefix or "$"] = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return result


def compare_features(older: dict, newer: dict) -> tuple[float, list[dict]]:
    left, right = flatten_features(older), flatten_features(newer)
    keys = sorted(set(left) | set(right))
    changes = [
        {"path": key, "older": left.get(key), "newer": right.get(key)}
        for key in keys if left.get(key) != right.get(key)
    ]
    score = 1.0 if not keys else (len(keys) - len(changes)) / len(keys)
    return score, changes


def artifact_references(artifact: dict) -> list[dict]:
    evidence = json_object(artifact.get("evidence"))
    references = evidence.get("references")
    if not isinstance(references, list):
        return []
    result = []
    seen = set()
    for ref in references:
        if not isinstance(ref, dict):
            continue
        source_id = str(ref.get("source_segment_id") or "")
        content_digest = str(ref.get("content_digest") or "")
        try:
            start, end = float(ref["start"]), float(ref["end"])
        except (KeyError, TypeError, ValueError):
            continue
        key = (source_id, content_digest)
        if (
            not source_id or len(content_digest) != 64 or key in seen
            or start < 0 or end <= start
        ):
            continue
        seen.add(key)
        result.append({
            "source_segment_id": source_id,
            "content_digest": content_digest,
            "start": start, "end": end,
        })
    return sorted(result, key=lambda row: (row["start"], row["source_segment_id"]))


def load_bound_artifact(row: dict) -> dict:
    path = Path(str(row.get("output_path") or ""))
    if not path.is_file() or sha256_file(path) != row.get("artifact_digest"):
        raise ValueError("analysis artifact digest mismatch")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        str(payload.get("analysis_id") or "") != str(row.get("analysis_id") or "")
        or str(payload.get("session_id") or "") != str(row.get("session_id") or "")
        or str(payload.get("transcript_id") or "") != str(row.get("transcript_id") or "")
    ):
        raise ValueError("analysis artifact identity mismatch")
    if not artifact_references(payload):
        raise ValueError("analysis artifact has no bound evidence references")
    return payload


def qualified_analysis_rows(conn, competitor_id: str | None = None) -> list[dict]:
    where = " AND c.competitor_id=?" if competitor_id else ""
    params = (competitor_id,) if competitor_id else ()
    rows = [dict(row) for row in conn.execute(
        "SELECT a.*,s.ended_at,s.metadata_json AS session_metadata,"
        "m.competitor_id,t.output_path AS transcript_path "
        "FROM analyses a JOIN live_sessions s ON s.session_id=a.session_id "
        "JOIN monitor_targets m ON m.monitor_target_id=s.monitor_target_id "
        "JOIN competitors c ON c.competitor_id=m.competitor_id "
        "JOIN transcripts t ON t.transcript_id=a.transcript_id "
        "JOIN evidence_bundles e ON e.object_type='analysis' AND e.object_id=a.analysis_id "
        "WHERE a.analysis_type='single_session' AND a.status='COMPLETE' "
        "AND a.lineage_state='CURRENT' AND a.scope='FORMAL_SINGLE_SESSION' "
        "AND a.qualification_status='FULL_SESSION_QUALIFIED' "
        "AND s.status='MEDIA_COMPLETE' AND s.completeness='COMPLETE' "
        "AND t.status='COMPLETE' AND t.scope='FULL_SESSION' "
        "AND t.qualification_status='FULL_SESSION_QUALIFIED' "
        "AND e.status='VERIFIED' AND e.manifest_hash=a.artifact_digest "
        "AND s.ended_at IS NOT NULL" + where +
        " ORDER BY m.competitor_id,s.ended_at,a.analysis_id",
        params,
    ).fetchall()]
    return [
        row for row in rows
        if (json_object(row.get("session_metadata")).get("media_coverage") or {}).get(
            "continuous_capture"
        ) is True
    ]
