#!/usr/bin/env python3
"""Evidence-bound semantic analysis worker for completed transcripts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import time
from pathlib import Path

import httpx

from v3_runtime import connect, enqueue_outbox_conn, init_db, upsert_heartbeat, utc_now
from v3_analysis_contract import (
    ANALYSIS_SCOPE_FORMAL,
    ANALYSIS_SPEC_VERSION,
    MODEL_VERSION,
    PROMPT_VERSION,
    QUALIFIED,
    SAMPLE_NONQUALIFYING,
    TRANSCRIPT_SCOPE_FULL,
    file_sha256,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_ROOT = ROOT.parent
ANALYSIS_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/analysis")
RUNNING = True
ANALYSIS_CHUNK_CHAR_LIMIT = 5000
ANALYSIS_MAX_RETRIES = 3
DEFAULT_ANALYSIS_COVERAGE_MINIMUM = 0.90
DEFAULT_ANALYSIS_COVERAGE_TARGET = 0.95
CHUNK_FIELDS = (
    "instructor", "course_content", "interaction_patterns", "product_handoff",
    "hook", "pain_points", "claims", "cta", "risks", "evidence_refs",
)


def stop(*_args):
    global RUNNING
    RUNNING = False


def finite_number(value) -> float | None:
    """Return a finite float, rejecting booleans and JSON NaN/Infinity."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def load_analysis_quality_config() -> tuple[float, float]:
    path = ROOT / "v3" / "v3_config.json"
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        config = {}
    quality = config.get("analysis_quality") or {}
    minimum = finite_number(quality.get("full_session_min_timestamp_coverage_rate"))
    minimum = minimum if minimum is not None and 0 < minimum <= 1 else DEFAULT_ANALYSIS_COVERAGE_MINIMUM
    target = finite_number(quality.get("full_session_target_timestamp_coverage_rate"))
    target = target if target is not None and minimum <= target <= 1 else max(minimum, DEFAULT_ANALYSIS_COVERAGE_TARGET)
    return minimum, target


def read_env_key(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    env_path = PROFILE_ROOT / ".env"
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        pass
    return ""


def _json_digest(value: object) -> str:
    body = json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def bind_transcript_segments(rows: object) -> list[dict]:
    """Bind every original ASR row to an immutable, content-derived ID."""
    if not isinstance(rows, list) or not rows:
        raise ValueError("transcript segments must be a non-empty array")
    bound: list[dict] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise ValueError(f"transcript segment {index} is not an object")
        start = finite_number(raw.get("start"))
        end = finite_number(raw.get("end"))
        if start is None or end is None or start < 0 or end <= start:
            raise ValueError(f"transcript segment {index} has invalid start/end")
        source_text = str(raw.get("text") or "")
        analysis_text = re.sub(
            r"\s+", " ", str(raw.get("normalized_text") or source_text)
        ).strip()
        if not source_text.strip() or not analysis_text:
            raise ValueError(f"transcript segment {index} has no text")
        content_digest = hashlib.sha256(source_text.encode("utf-8")).hexdigest()
        identity_digest = _json_digest({
            "segment_index": index,
            "start": start,
            "end": end,
            "content_digest": content_digest,
        })
        source_segment_id = f"srcseg_{index:06d}_{identity_digest[:20]}"
        if source_segment_id in seen:
            raise ValueError(f"duplicate source segment id: {source_segment_id}")
        seen.add(source_segment_id)
        bound.append({
            "segment_index": index,
            "source_segment_id": source_segment_id,
            "start": start,
            "end": end,
            "text": source_text,
            "analysis_text": analysis_text,
            "content_digest": content_digest,
            # The model receives an ID and text, never a timestamp.
            "line": f"[{source_segment_id}] {analysis_text}",
        })
    return bound


def transcript_text(path: Path, limit: int | None = None) -> tuple[str, list[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("transcript artifact must be a JSON object")
    rows = bind_transcript_segments(payload.get("segments"))
    text = "\n".join(row["line"] for row in rows)
    return (text[:limit] if limit is not None else text), rows


def source_rows_from_transcript(payload: dict, transcript_id: str) -> list[dict]:
    del transcript_id  # Identity is content-bound and independent of DB naming.
    return bind_transcript_segments(payload.get("segments"))


def timed_lines(text: str) -> list[dict]:
    """Compatibility parser for tests; model output never uses this format."""
    rows = []
    pattern = re.compile(r"^\[([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)\]\s*(.*)$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        start, end = float(match.group(1)), float(match.group(2))
        if start < 0 or end <= start:
            continue
        rows.append({"start": start, "end": end, "text": match.group(3)})
    return bind_transcript_segments(rows)


def analysis_chunks(text: str, char_limit: int = ANALYSIS_CHUNK_CHAR_LIMIT,
                    source_rows: list[dict] | None = None) -> list[dict]:
    """Split only between timestamped rows; every row belongs to one chunk."""
    rows = list(source_rows) if source_rows is not None else timed_lines(text)
    if not rows:
        raise RuntimeError("analysis input has no timestamped transcript rows")
    chunks = []
    current: list[dict] = []
    current_chars = 0
    for row in rows:
        line_chars = len(row["line"]) + (1 if current else 0)
        if current and current_chars + line_chars > char_limit:
            chunks.append({"index": len(chunks), "start": current[0]["start"], "end": current[-1]["end"],
                           "text": "\n".join(item["line"] for item in current), "row_count": len(current),
                           "source_segment_ids": [item["source_segment_id"] for item in current],
                           "rows": current})
            current, current_chars = [], 0
            line_chars = len(row["line"])
        current.append(row)
        current_chars += line_chars
    if current:
        chunks.append({"index": len(chunks), "start": current[0]["start"], "end": current[-1]["end"],
                       "text": "\n".join(item["line"] for item in current), "row_count": len(current),
                       "source_segment_ids": [item["source_segment_id"] for item in current],
                       "rows": current})
    return chunks


def empty_chunk_result() -> dict:
    return {field: [] for field in CHUNK_FIELDS}


def validate_chunk_result(value: object, chunk: dict) -> dict:
    if not isinstance(value, dict):
        raise ValueError("analysis chunk response must be a JSON object")
    missing = sorted(set(CHUNK_FIELDS) - set(value))
    unknown = sorted(set(value) - set(CHUNK_FIELDS))
    if missing:
        raise ValueError(f"analysis chunk response omitted fields: {missing}")
    if unknown:
        raise ValueError(f"analysis chunk response has unknown fields: {unknown}")
    source_by_id = {row["source_segment_id"]: row for row in chunk.get("rows") or []}
    if not source_by_id:
        raise ValueError("analysis source chunk has no bound transcript segments")
    result = empty_chunk_result()
    for field in CHUNK_FIELDS:
        items = value[field]
        if not isinstance(items, list):
            raise ValueError(f"analysis field {field} must be an array")
        if len(items) > 3:
            raise ValueError(f"analysis field {field} exceeded the maximum item count")
        normalized = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError(f"analysis field {field} contains a non-object item")
            if set(item) != {"summary", "source_segment_ids"}:
                raise ValueError(
                    f"analysis field {field} item must contain only summary/source_segment_ids"
                )
            summary = str(item.get("summary") or "").strip()
            if not summary or len(summary) > 160:
                raise ValueError(f"analysis field {field} item has invalid summary length")
            source_ids = item.get("source_segment_ids")
            if (not isinstance(source_ids, list) or not 1 <= len(source_ids) <= 8
                    or not all(isinstance(source_id, str) and source_id for source_id in source_ids)):
                raise ValueError(f"analysis field {field} item has invalid source_segment_ids")
            if len(set(source_ids)) != len(source_ids):
                raise ValueError(f"analysis field {field} item has duplicate source_segment_ids")
            unavailable = [source_id for source_id in source_ids if source_id not in source_by_id]
            if unavailable:
                raise ValueError(
                    f"analysis field {field} references source_segment_id outside its source chunk"
                )
            source_segments = [{
                "source_segment_id": source_id,
                "start": source_by_id[source_id]["start"],
                "end": source_by_id[source_id]["end"],
                "source_text": source_by_id[source_id]["text"],
                "content_digest": source_by_id[source_id]["content_digest"],
            } for source_id in source_ids]
            normalized.append({
                "summary": summary,
                "source_segment_ids": list(source_ids),
                "source_segments": source_segments,
                # Compatibility fields for existing consumers.  These values
                # are computed exclusively from cited original segments.
                "start": min(ref["start"] for ref in source_segments),
                "end": max(ref["end"] for ref in source_segments),
                "source_binding_digest": _json_digest(source_segments),
            })
        result[field] = normalized
    return result


def request_chunk(chunk: dict) -> tuple[dict, dict]:
    key = read_env_key("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY is not configured")
    example = {field: [] for field in CHUNK_FIELDS}
    system = (
        "你是直播竞品情报分析器。输入每行格式为[source_segment_id]逐字稿原文。"
        "只能从所给逐字稿提取证据，不能补写事实，不能生成、猜测或输出任何时间戳。"
        "输出严格JSON对象，不要Markdown或解释。必须包含示例中的全部字段，每字段最多2项；"
        "每项格式只能是{summary,source_segment_ids}，summary不超过80个汉字，"
        "source_segment_ids必须是包含1到8个本时间片原始ID的JSON数组，不得改写或虚构ID。"
        "instructor只记录当前说话者明确自称或家长明确称呼当前主播的姓名，summary只能写“X老师”；"
        "提及课程作者、其他老师或合作老师绝不能算当前讲师；无法唯一确认就返回空数组。JSON示例："
        + json.dumps(example, ensure_ascii=False, separators=(",", ":"))
    )
    body = {"model": "deepseek-chat", "temperature": 0, "max_tokens": 4096,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": "请分析这一时间片并输出JSON：\n" + chunk["text"]}]}
    failures = []
    for attempt in range(1, ANALYSIS_MAX_RETRIES + 1):
        with httpx.Client(timeout=180) as client:
            response = client.post("https://api.deepseek.com/chat/completions",
                                   headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json=body)
            response.raise_for_status()
            payload = response.json()
        choice = (payload.get("choices") or [{}])[0]
        finish_reason = choice.get("finish_reason")
        content = ((choice.get("message") or {}).get("content") or "").strip()
        try:
            if finish_reason != "stop":
                raise ValueError(f"analysis completion did not stop normally: {finish_reason}")
            parsed = json.loads(content)
            result = validate_chunk_result(parsed, chunk)
            return result, {"response_id": payload.get("id"), "finish_reason": finish_reason,
                            "usage": payload.get("usage"), "attempt": attempt,
                            "content_hash": hashlib.sha256(content.encode()).hexdigest()}
        except (json.JSONDecodeError, ValueError) as exc:
            failures.append(f"attempt {attempt}: {exc}")
            body["messages"].append({"role": "assistant", "content": content[-1000:]})
            body["messages"].append({"role": "user", "content":
                f"上次输出无效：{exc}。重新生成更短的完整JSON；每字段最多1项，不能省略字段。"
                "source_segment_ids必须是含1到8个输入原始ID的JSON数组；"
                "只能引用输入中的source_segment_id；不要输出start/end或其他字段。"})
    raise RuntimeError(
        f"analysis chunk {chunk.get('index')} failed after retries: "
        + " | ".join(failures)
    )


def _identifier_hash(values: list[str]) -> str:
    body = json.dumps(values, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _interval_union(rows: list[dict], duration: float) -> tuple[list[list[float]], list[dict], list[dict]]:
    intervals: list[tuple[float, float]] = []
    errors: list[dict] = []
    for row in rows:
        start, end = finite_number(row.get("start")), finite_number(row.get("end"))
        if start is None or end is None or start < 0 or end <= start or end > duration:
            errors.append({"code": "SOURCE_TIMESTAMP_OUT_OF_RANGE",
                           "source_segment_id": row.get("source_segment_id"),
                           "start": start, "end": end})
            continue
        intervals.append((start, end))
    merged: list[list[float]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    gaps: list[dict] = []
    cursor = 0.0
    for start, end in merged:
        if start > cursor:
            gaps.append({"start_time": cursor, "end_time": start})
        cursor = max(cursor, end)
    if cursor < duration:
        gaps.append({"start_time": cursor, "end_time": duration})
    return merged, gaps, errors


def calculate_analysis_coverage(
    source_rows: list[dict],
    successful_source_segment_ids: list[str],
    source_duration: float | None,
    *,
    coverage_scope: str = "FULL_SESSION",
    minimum: float = DEFAULT_ANALYSIS_COVERAGE_MINIMUM,
    target: float = DEFAULT_ANALYSIS_COVERAGE_TARGET,
    all_chunks_complete: bool = True,
    transcript_quality: dict | None = None,
) -> dict:
    """Calculate coverage from source timestamp unions and successful IDs."""
    duration = finite_number(source_duration)
    threshold = finite_number(minimum)
    threshold = threshold if threshold is not None and 0 < threshold <= 1 else DEFAULT_ANALYSIS_COVERAGE_MINIMUM
    desired = finite_number(target)
    desired = desired if desired is not None and threshold <= desired <= 1 else max(threshold, DEFAULT_ANALYSIS_COVERAGE_TARGET)
    errors: list[dict] = []
    if duration is None or duration <= 0:
        errors.append({"code": "INVALID_SOURCE_DURATION"})
        duration = 0.0

    normalized_rows: list[dict] = []
    source_ids: list[str] = []
    for index, row in enumerate(source_rows):
        source_id = str(row.get("source_segment_id") or f"source:segment:{index:06d}")
        normalized = {**row, "source_segment_id": source_id}
        normalized_rows.append(normalized)
        source_ids.append(source_id)
    if len(source_ids) != len(set(source_ids)):
        errors.append({"code": "DUPLICATE_SOURCE_SEGMENT_ID"})

    successful_ids = list(dict.fromkeys(str(item) for item in successful_source_segment_ids))
    source_id_set = set(source_ids)
    unknown_ids = sorted(set(successful_ids) - source_id_set)
    if unknown_ids:
        errors.append({"code": "UNKNOWN_SUCCESSFUL_SOURCE_SEGMENT_ID", "count": len(unknown_ids)})
    successful_id_set = set(successful_ids) & source_id_set
    analyzed_rows = [row for row in normalized_rows if row["source_segment_id"] in successful_id_set]

    if duration > 0:
        source_union, _source_gaps, source_errors = _interval_union(normalized_rows, duration)
        analyzed_union, gaps, analyzed_errors = _interval_union(analyzed_rows, duration)
        errors.extend(source_errors)
        errors.extend(analyzed_errors)
    else:
        source_union, analyzed_union, gaps = [], [], []
    source_covered = sum(end - start for start, end in source_union)
    analyzed_covered = sum(end - start for start, end in analyzed_union)
    source_rate = source_covered / duration if duration > 0 else 0.0
    analysis_rate = analyzed_covered / duration if duration > 0 else 0.0
    segment_rate = len(successful_id_set) / len(source_ids) if source_ids else 0.0

    transcript_gate_ok = True
    if transcript_quality is not None:
        transcript_rate = finite_number(transcript_quality.get("coverage_rate"))
        transcript_duration = finite_number(transcript_quality.get("audio_duration_seconds"))
        transcript_gate_ok = (
            transcript_quality.get("timestamps_valid") is True
            and transcript_quality.get("is_qualified") is True
            and transcript_rate is not None
            and transcript_rate >= threshold
            and transcript_duration is not None
            and abs(transcript_duration - duration) <= 0.01
            and abs(transcript_rate - source_rate) <= 1e-9
        )
        if not transcript_gate_ok:
            errors.append({"code": "TRANSCRIPT_QUALITY_GATE_MISMATCH"})
    if coverage_scope != "FULL_SESSION":
        errors.append({"code": "SOURCE_SCOPE_NOT_FULL_SESSION", "coverage_scope": coverage_scope})
    if not all_chunks_complete:
        errors.append({"code": "ANALYSIS_CHUNKS_INCOMPLETE"})

    timestamps_valid = not any(error["code"] in {
        "INVALID_SOURCE_DURATION", "SOURCE_TIMESTAMP_OUT_OF_RANGE"
    } for error in errors)
    source_ids_valid = not any(error["code"] in {
        "DUPLICATE_SOURCE_SEGMENT_ID", "UNKNOWN_SUCCESSFUL_SOURCE_SEGMENT_ID"
    } for error in errors)
    qualified = (
        coverage_scope == "FULL_SESSION"
        and timestamps_valid
        and source_ids_valid
        and transcript_gate_ok
        and all_chunks_complete
        and len(successful_id_set) == len(source_ids)
        and source_rate >= threshold
        and analysis_rate >= threshold
    )
    meets_target = qualified and source_rate >= desired and analysis_rate >= desired
    return {
        "schema_version": 2,
        "coverage_scope": coverage_scope,
        "source_duration_seconds": duration,
        "source_timestamp_covered_seconds": source_covered,
        "source_transcript_coverage_rate": source_rate,
        "analyzed_timestamp_covered_seconds": analyzed_covered,
        "analysis_coverage_rate": analysis_rate,
        "timeline_coverage_rate": analysis_rate,
        "source_segment_count": len(source_ids),
        "analyzed_unique_segment_count": len(successful_id_set),
        "segment_coverage_rate": segment_rate,
        "source_segment_ids_sha256": _identifier_hash(source_ids),
        "successful_source_segment_ids_sha256": _identifier_hash(successful_ids),
        "successful_source_segment_ids": successful_ids,
        "covered_segments": [{"start_time": start, "end_time": end} for start, end in analyzed_union],
        "gaps": gaps,
        "minimum_coverage_rate": threshold,
        "target_coverage_rate": desired,
        "timestamps_valid": timestamps_valid,
        "source_segment_ids_valid": source_ids_valid,
        "all_chunks_complete": all_chunks_complete,
        "is_qualified": qualified,
        "meets_target": meets_target,
        "validation_errors": errors,
    }


def merge_chunk_results(results: list[dict], chunks: list[dict], source_duration: float | None,
                        *, coverage_scope: str = "FULL_SESSION",
                        minimum: float = DEFAULT_ANALYSIS_COVERAGE_MINIMUM,
                        target: float = DEFAULT_ANALYSIS_COVERAGE_TARGET,
                        transcript_quality: dict | None = None,
                        successful_source_segment_ids: list[str] | None = None) -> dict:
    merged = empty_chunk_result()
    seen = {field: set() for field in CHUNK_FIELDS}
    for result in results:
        for field in CHUNK_FIELDS:
            for item in result[field]:
                key = (
                    re.sub(r"\s+", "", item["summary"]).lower(),
                    tuple(item["source_segment_ids"]),
                )
                if key not in seen[field]:
                    seen[field].add(key)
                    merged[field].append(item)
    for field in CHUNK_FIELDS:
        merged[field].sort(key=lambda item: (item["start"], item["end"], item["summary"]))
    instructor_items = merged.pop("instructor")
    instructor_names = list(dict.fromkeys(item["summary"] for item in instructor_items))
    instructor_status = "NOT_STATED" if not instructor_names else "STATED" if len(instructor_names) == 1 else "CONFLICT"
    duration = float(source_duration or chunks[-1]["end"])
    module_sources = (("开场", "hook"), ("干货", "course_content"), ("需求", "pain_points"),
                      ("信任", "claims"), ("商品承接", "product_handoff"), ("成交", "cta"),
                      ("答疑", "interaction_patterns"))
    modules = []
    source_rows = [row for chunk in chunks for row in chunk["rows"]]
    for index, row in enumerate(source_rows):
        row.setdefault("source_segment_id", f"source:segment:{index:06d}")
    for name, field in module_sources:
        values = merged[field]
        timestamps = []
        seen_module_refs: set[str] = set()
        for item in values:
            for reference in item["source_segments"]:
                source_id = reference["source_segment_id"]
                if source_id in seen_module_refs:
                    continue
                seen_module_refs.add(source_id)
                timestamps.append(dict(reference))
        modules.append({"name": name, "summary": "；".join(item["summary"] for item in values[:5]),
                        "timestamps": timestamps})
    inferred_successful_ids = [
        str(row["source_segment_id"])
        for chunk in chunks[:len(results)] for row in chunk["rows"]
    ]
    successful_ids = successful_source_segment_ids if successful_source_segment_ids is not None else inferred_successful_ids
    coverage = calculate_analysis_coverage(
        source_rows, successful_ids, duration, coverage_scope=coverage_scope,
        minimum=minimum, target=target,
        all_chunks_complete=len(results) == len(chunks), transcript_quality=transcript_quality,
    )
    merged.update({
        "schema_version": "3.0",
        "instructor": {"status": instructor_status,
                       "names": instructor_names, "evidence_refs": instructor_items},
        "interaction": list(merged["interaction_patterns"]),
        "product_fulfillment": list(merged["product_handoff"]),
        "modules": modules,
        "analysis_coverage": {**coverage,
                              "first_timestamp_seconds": min(row["start"] for row in source_rows),
                              "last_timestamp_seconds": max(row["end"] for row in source_rows),
                              "chunk_count": len(chunks),
                              "input_row_count": sum(chunk["row_count"] for chunk in chunks)},
        "evidence_binding": {
            "schema_version": "1.0",
            "mode": "STRICT_SOURCE_SEGMENT_IDS",
            "model_generated_timestamps": False,
            "nearest_segment_fallback": False,
            "all_references_valid": True,
        },
    })
    return merged


def source_reference_manifest(result: dict) -> dict:
    """Return a compact immutable index of every source reference in a result."""
    references: dict[str, dict] = {}

    def visit(value: object) -> None:
        if isinstance(value, dict):
            source_segments = value.get("source_segments")
            if isinstance(source_segments, list):
                for ref in source_segments:
                    if not isinstance(ref, dict):
                        raise ValueError("analysis contains a malformed source segment reference")
                    required = ("source_segment_id", "start", "end", "source_text", "content_digest")
                    if any(key not in ref for key in required):
                        raise ValueError("analysis contains an incomplete source segment reference")
                    source_id = str(ref["source_segment_id"])
                    expected_digest = hashlib.sha256(str(ref["source_text"]).encode("utf-8")).hexdigest()
                    if str(ref["content_digest"]) != expected_digest:
                        raise ValueError("analysis source segment content digest mismatch")
                    compact = {
                        "source_segment_id": source_id,
                        "start": float(ref["start"]),
                        "end": float(ref["end"]),
                        "content_digest": expected_digest,
                    }
                    prior = references.get(source_id)
                    if prior is not None and prior != compact:
                        raise ValueError("analysis source segment id resolves to conflicting evidence")
                    references[source_id] = compact
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(result)
    ordered = [references[source_id] for source_id in sorted(references)]
    return {
        "schema_version": "1.0",
        "mode": "STRICT_SOURCE_SEGMENT_IDS",
        "reference_count": len(ordered),
        "references": ordered,
        "source_binding_digest": _json_digest(ordered),
    }


def artifact_binding_status(artifact: object) -> str:
    """Classify old artifacts without inventing bindings for their timestamps."""
    if not isinstance(artifact, dict):
        return "INVALID"
    result = artifact.get("result")
    evidence = artifact.get("evidence")
    if (isinstance(result, dict) and result.get("schema_version") == "3.0"
            and isinstance(evidence, dict)
            and evidence.get("mode") == "STRICT_SOURCE_SEGMENT_IDS"):
        try:
            manifest = source_reference_manifest(result)
        except (TypeError, ValueError):
            return "INVALID"
        if manifest["source_binding_digest"] == evidence.get("source_binding_digest"):
            return "BOUND_V1"
        return "INVALID"
    # Schema 2/free-timestamp artifacts remain readable, but are not upgraded
    # by nearest-line matching.  A verified migration must re-run the model.
    return "LEGACY_UNBOUND"



def write_json_atomic(path: Path, payload: dict) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    parsed = json.loads(serialized)
    if not isinstance(parsed, dict):
        raise RuntimeError("analysis artifact did not serialize to a JSON object")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        json.loads(temporary.read_text(encoding="utf-8"))
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


class AnalysisSourceQualityError(RuntimeError):
    def __init__(self, message: str, coverage: dict):
        super().__init__(message)
        self.coverage = coverage


def json_object(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def transcript_qualification(
    row: dict,
    *,
    canonical_source_digest: str | None,
    minimum: float,
) -> tuple[bool, str, dict]:
    """Recheck the exact immutable transcript at the analysis boundary."""
    metadata = json_object(row.get("transcript_metadata"))
    session_metadata = json_object(row.get("session_metadata"))
    quality = metadata.get("timestamp_coverage") or {}
    rate = finite_number(quality.get("coverage_rate"))
    is_sample = (
        row.get("transcript_scope") == "SAMPLE"
        or row.get("transcript_qualification_status") == SAMPLE_NONQUALIFYING
        or metadata.get("sample_only") is True
        or metadata.get("coverage_scope") == "SAMPLE"
    )
    reasons: list[str] = []
    if is_sample:
        reasons.append("SAMPLE_TRANSCRIPT")
    if row.get("transcript_status") != "COMPLETE":
        reasons.append("TRANSCRIPT_NOT_COMPLETE")
    if row.get("transcript_scope") != TRANSCRIPT_SCOPE_FULL:
        reasons.append("TRANSCRIPT_SCOPE_NOT_FULL_SESSION")
    if row.get("transcript_qualification_status") != QUALIFIED:
        reasons.append("TRANSCRIPT_QUALIFICATION_COLUMN_NOT_QUALIFIED")
    if metadata.get("coverage_scope") != "FULL_SESSION":
        reasons.append("NOT_FULL_SESSION")
    if metadata.get("sample_only") is not False:
        reasons.append("SAMPLE_FLAG_NOT_FALSE")
    if metadata.get("quality_gate_status") != QUALIFIED:
        reasons.append("TRANSCRIPT_QUALITY_GATE_NOT_QUALIFIED")
    if quality.get("is_qualified") is not True or quality.get("timestamps_valid") is not True:
        reasons.append("TRANSCRIPT_TIMESTAMPS_NOT_QUALIFIED")
    if rate is None or rate < minimum:
        reasons.append("TRANSCRIPT_COVERAGE_BELOW_MINIMUM")
    if row.get("session_status") != "MEDIA_COMPLETE" or row.get("completeness") != "COMPLETE":
        reasons.append("SESSION_NOT_MEDIA_COMPLETE")
    if (session_metadata.get("media_coverage") or {}).get("continuous_capture") is not True:
        reasons.append("SESSION_CAPTURE_NOT_CONTINUOUS")
    if not canonical_source_digest:
        reasons.append("CURRENT_CANONICAL_SOURCE_MISSING")
    elif str(row.get("transcript_source_digest") or "") != canonical_source_digest:
        reasons.append("TRANSCRIPT_SOURCE_DIGEST_NOT_CURRENT_CANONICAL")
    transcript_path = Path(str(row.get("transcript_path") or ""))
    if not transcript_path.is_file():
        reasons.append("TRANSCRIPT_ARTIFACT_MISSING")
    details = {
        "coverage_scope": metadata.get("coverage_scope"),
        "sample_only": metadata.get("sample_only"),
        "coverage_rate": rate,
        "source_segment_id": metadata.get("source_segment_id"),
        "canonical_source_digest": canonical_source_digest,
        "reasons": reasons,
    }
    state = SAMPLE_NONQUALIFYING if is_sample else (QUALIFIED if not reasons else "SOURCE_NONQUALIFYING")
    return not reasons, state, details


def prepare_analysis_source(path: Path, transcript_id: str, metadata_value,
                            minimum: float, target: float) -> tuple[str, list[dict], float, str, dict | None, dict]:
    """Read and qualify the exact lineage-bound FULL_SESSION transcript."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("analysis source transcript is not a JSON object")
    metadata = json_object(metadata_value)
    scope = str(metadata.get("coverage_scope") or payload.get("coverage_scope") or "")
    if metadata.get("sample_only") is True:
        scope = "SAMPLE"
    transcript_quality = metadata.get("timestamp_coverage") or payload.get("timestamp_coverage")
    if not isinstance(transcript_quality, dict):
        transcript_quality = None
    duration = (
        finite_number((transcript_quality or {}).get("audio_duration_seconds"))
        or finite_number(payload.get("duration"))
    )
    rows = source_rows_from_transcript(payload, transcript_id)
    text = "\n".join(row["line"] for row in rows)
    source_ids = [row["source_segment_id"] for row in rows]
    preflight = calculate_analysis_coverage(
        rows, source_ids, duration, coverage_scope=scope, minimum=minimum,
        target=target, all_chunks_complete=True, transcript_quality=transcript_quality,
    )
    if not preflight["is_qualified"]:
        raise AnalysisSourceQualityError("analysis source did not pass FULL_SESSION coverage gate", preflight)
    return text, rows, float(duration), scope, transcript_quality, payload


def revalidate_existing_artifact(artifact: dict, text: str, source_rows: list[dict],
                                 source_duration: float, coverage_scope: str,
                                 transcript_quality: dict | None,
                                 minimum: float, target: float) -> tuple[dict, dict]:
    """Recompute an old artifact without another model call.

    Revalidation is allowed only when its content hash and every successful
    chunk diagnostic exactly match the chunks deterministically rebuilt from
    the current source transcript.
    """
    if artifact_binding_status(artifact) != "BOUND_V1":
        raise ValueError("legacy free-timestamp analysis cannot be upgraded without a model rerun")
    engine = json_object(artifact.get("engine"))
    if engine.get("source_content_hash") != hashlib.sha256(text.encode()).hexdigest():
        raise ValueError("existing analysis source hash does not match transcript")
    chunks = analysis_chunks(text, source_rows=source_rows)
    diagnostics = engine.get("chunk_diagnostics")
    if not isinstance(diagnostics, list) or len(diagnostics) != len(chunks):
        raise ValueError("existing analysis lacks complete chunk diagnostics")
    successful_ids: list[str] = []
    augmented: list[dict] = []
    for expected, diagnostic in zip(chunks, diagnostics):
        if not isinstance(diagnostic, dict):
            raise ValueError("existing analysis has invalid chunk diagnostics")
        if (
            int(diagnostic.get("chunk_index", -1)) != expected["index"]
            or int(diagnostic.get("row_count", -1)) != expected["row_count"]
            or abs(float(diagnostic.get("start")) - expected["start"]) > 0.01
            or abs(float(diagnostic.get("end")) - expected["end"]) > 0.01
            or diagnostic.get("finish_reason") != "stop"
        ):
            raise ValueError("existing analysis chunk diagnostics do not match transcript")
        successful_ids.extend(expected["source_segment_ids"])
        augmented.append({**diagnostic,
                          "source_segment_ids": expected["source_segment_ids"],
                          "source_segment_ids_sha256": _identifier_hash(expected["source_segment_ids"])})
    coverage = calculate_analysis_coverage(
        source_rows, successful_ids, source_duration, coverage_scope=coverage_scope,
        minimum=minimum, target=target, all_chunks_complete=True,
        transcript_quality=transcript_quality,
    )
    result = json_object(artifact.get("result"))
    result["analysis_coverage"] = {
        **coverage,
        "first_timestamp_seconds": min(row["start"] for row in source_rows),
        "last_timestamp_seconds": max(row["end"] for row in source_rows),
        "chunk_count": len(chunks),
        "input_row_count": len(source_rows),
    }
    for module in result.get("modules") or []:
        for reference in module.get("timestamps") or []:
            index = reference.get("source_segment_index")
            if isinstance(index, int) and 0 <= index < len(source_rows):
                reference["source_segment_id"] = source_rows[index]["source_segment_id"]
    engine.update({
        "chunk_count": len(chunks),
        "successful_source_segment_count": len(successful_ids),
        "successful_source_segment_ids_sha256": _identifier_hash(successful_ids),
        "chunk_diagnostics": augmented,
        "merge_engine": "deterministic-python-v3-coverage-gated-revalidation",
    })
    artifact["engine"] = engine
    artifact["result"] = result
    return artifact, coverage


def request_analysis(text: str, source_duration: float | None = None, *,
                     source_rows: list[dict] | None = None,
                     coverage_scope: str = "FULL_SESSION",
                     minimum: float = DEFAULT_ANALYSIS_COVERAGE_MINIMUM,
                     target: float = DEFAULT_ANALYSIS_COVERAGE_TARGET,
                     transcript_quality: dict | None = None) -> tuple[dict, dict]:
    chunks = analysis_chunks(text, source_rows=source_rows)
    results, diagnostics, successful_ids = [], [], []
    for chunk in chunks:
        result, diagnostic = request_chunk(chunk)
        results.append(result)
        successful_ids.extend(chunk["source_segment_ids"])
        diagnostics.append({"chunk_index": chunk["index"], "start": chunk["start"], "end": chunk["end"],
                            "row_count": chunk["row_count"],
                            "source_segment_ids": chunk["source_segment_ids"],
                            "source_segment_ids_sha256": _identifier_hash(chunk["source_segment_ids"]),
                            **diagnostic})
    merged = merge_chunk_results(
        results, chunks, source_duration, coverage_scope=coverage_scope,
        minimum=minimum, target=target, transcript_quality=transcript_quality,
        successful_source_segment_ids=successful_ids,
    )
    return merged, {"provider": "deepseek", "model": "deepseek-chat", "response_ids": [d["response_id"] for d in diagnostics],
                    "source_content_hash": hashlib.sha256(text.encode()).hexdigest(), "chunk_count": len(chunks),
                    "source_segment_set_digest": _json_digest([
                        {"source_segment_id": row["source_segment_id"],
                         "content_digest": row["content_digest"]}
                        for row in (source_rows or [])
                    ]),
                    "successful_source_segment_count": len(successful_ids),
                    "successful_source_segment_ids_sha256": _identifier_hash(successful_ids),
                    "chunk_diagnostics": diagnostics,
                    "merge_engine": "deterministic-python-v4-source-ids-coverage-gated"}


def _coverage_metadata(coverage: dict) -> dict:
    """Keep the DB health summary small; exact IDs and gaps stay in artifact."""
    excluded = {"successful_source_segment_ids", "covered_segments", "gaps"}
    return {key: value for key, value in coverage.items() if key not in excluded}


def _current_analysis_gate(row, minimum: float, target: float) -> bool:
    metadata = json_object(row.get("metadata_json"))
    coverage = metadata.get("analysis_coverage") or {}
    stored_minimum = finite_number(coverage.get("minimum_coverage_rate"))
    stored_target = finite_number(coverage.get("target_coverage_rate"))
    if (
        coverage.get("schema_version") != 2
        or stored_minimum is None or abs(stored_minimum - minimum) > 1e-12
        or stored_target is None or abs(stored_target - target) > 1e-12
    ):
        return False
    if row.get("status") == "COMPLETE":
        rate = finite_number(coverage.get("analysis_coverage_rate"))
        return bool(
            coverage.get("coverage_scope") == "FULL_SESSION"
            and coverage.get("timestamps_valid") is True
            and coverage.get("source_segment_ids_valid") is True
            and coverage.get("all_chunks_complete") is True
            and coverage.get("is_qualified") is True
            and rate is not None and rate >= minimum
            and row.get("output_path") and Path(row["output_path"]).is_file()
        )
    return row.get("status") == "QUALITY_BLOCKED" and coverage.get("is_qualified") is False


def analysis_health_snapshot(minimum: float, target: float) -> tuple[str, dict]:
    """Report persistent quality debt, not only failures from this loop."""
    with connect() as conn:
        rows = [
            dict(row) for row in conn.execute(
                "SELECT a.*,e.status AS evidence_status FROM analyses a "
                "LEFT JOIN evidence_bundles e ON e.object_type='analysis' "
                "AND e.object_id=a.analysis_id WHERE a.analysis_type='single_session' "
                "AND a.analysis_spec_version=? AND a.model_version=? AND a.prompt_version=? "
                "AND a.lineage_state<>'SUPERSEDED' ORDER BY a.analysis_id",
                (ANALYSIS_SPEC_VERSION, MODEL_VERSION, PROMPT_VERSION),
            ).fetchall()
        ]
    qualified = target_met = blocked = waiting = evidence_pending = 0
    for row in rows:
        if row["status"] == "COMPLETE" and _current_analysis_gate(row, minimum, target):
            qualified += 1
            coverage = json_object(row.get("metadata_json")).get("analysis_coverage") or {}
            if coverage.get("meets_target") is True:
                target_met += 1
            if row.get("evidence_status") != "VERIFIED":
                evidence_pending += 1
        elif row["status"] in {"QUALITY_BLOCKED", "BLOCKED_SOURCE_QUALIFICATION"}:
            blocked += 1
        elif row["status"] in {"PENDING", "WAITING_MODEL"}:
            waiting += 1
    reasons = []
    if blocked:
        reasons.append("ANALYSIS_QUALITY_BLOCKED")
    if waiting:
        reasons.append("ANALYSIS_WORK_PENDING")
    if evidence_pending:
        reasons.append("ANALYSIS_EVIDENCE_PENDING")
    status = "DEGRADED" if reasons else "READY"
    return status, {
        "current_spec_total": len(rows),
        "current_spec_qualified": qualified,
        "current_spec_meets_target": target_met,
        "current_spec_blocked": blocked,
        "current_spec_waiting": waiting,
        "current_spec_evidence_pending": evidence_pending,
        "health_reasons": reasons,
    }


def _quality_block(
    analysis_row: dict,
    coverage: dict,
    *,
    reason: str,
    analysis_metadata: dict,
) -> None:
    """Persist a non-formal diagnostic without binding analyses.output_path."""
    diagnostic_dir = ANALYSIS_ROOT / "quality-blocked"
    diagnostic_path = diagnostic_dir / f"{analysis_row['analysis_id']}.coverage.json"
    diagnostic = {
        "analysis_id": analysis_row["analysis_id"],
        "session_id": analysis_row["session_id"],
        "transcript_id": analysis_row["transcript_id"],
        "transcript_content_digest": analysis_row["transcript_content_digest"],
        "analysis_spec_version": analysis_row["analysis_spec_version"],
        "model_version": analysis_row["model_version"],
        "prompt_version": analysis_row["prompt_version"],
        "status": "QUALITY_BLOCKED",
        "reason": reason,
        "analysis_coverage": coverage,
        "checked_at": utc_now(),
    }
    write_json_atomic(diagnostic_path, diagnostic)
    diagnostic_digest = file_sha256(diagnostic_path)
    metadata = {
        **analysis_metadata,
        "coverage_scope": coverage.get("coverage_scope"),
        "analysis_coverage": _coverage_metadata(coverage),
        "quality_gate_status": "QUALITY_BLOCKED",
        "is_formal_output": False,
        "quality_diagnostic_path": str(diagnostic_path),
        "quality_diagnostic_digest": diagnostic_digest,
        "error_type": "AnalysisCoverageQualityBlocked",
        "error_message": reason,
        "checked_at": utc_now(),
    }
    with connect() as conn:
        conn.execute(
            "UPDATE analyses SET status='QUALITY_BLOCKED',lineage_state='BLOCKED',"
            "qualification_status='ANALYSIS_QUALITY_BLOCKED',metadata_json=? "
            "WHERE analysis_id=? AND transcript_id=? AND transcript_content_digest=? "
            "AND analysis_spec_version=? AND model_version=? AND prompt_version=?",
            (
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                analysis_row["analysis_id"], analysis_row["transcript_id"],
                analysis_row["transcript_content_digest"], ANALYSIS_SPEC_VERSION,
                MODEL_VERSION, PROMPT_VERSION,
            ),
        )
        conn.execute(
            "INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,"
            "manifest_hash,scope,qualification_status,metadata_json) VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(object_type,object_id) DO UPDATE SET status=excluded.status,"
            "manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,"
            "scope=excluded.scope,qualification_status=excluded.qualification_status,"
            "metadata_json=excluded.metadata_json",
            (
                "bundle:" + analysis_row["analysis_id"], "analysis", analysis_row["analysis_id"],
                "QUALITY_BLOCKED", str(diagnostic_path), diagnostic_digest,
                ANALYSIS_SCOPE_FORMAL, "ANALYSIS_QUALITY_BLOCKED",
                json.dumps({"analysis_coverage": _coverage_metadata(coverage),
                            "reason": reason}, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()


def once() -> dict:
    completed = failed = quality_blocked = source_blocked = revalidated = 0
    minimum, target = load_analysis_quality_config()
    init_db()
    with connect() as conn:
        analyses = [
            dict(row) for row in conn.execute(
                "SELECT * FROM analyses WHERE analysis_type='single_session' "
                "AND analysis_spec_version=? AND model_version=? AND prompt_version=? "
                "AND status IN ('PENDING','WAITING_MODEL','QUALITY_BLOCKED','BLOCKED_SOURCE_QUALIFICATION') "
                "ORDER BY analysis_id",
                (ANALYSIS_SPEC_VERSION, MODEL_VERSION, PROMPT_VERSION),
            ).fetchall()
        ]

    for analysis_row in analyses:
        if _current_analysis_gate(analysis_row, minimum, target):
            continue
        analysis_metadata = json_object(analysis_row.get("metadata_json"))
        expected_transcript_id = str(analysis_row.get("transcript_id") or "")
        with connect() as conn:
            row = conn.execute(
                "SELECT t.transcript_id,t.session_id,t.source_digest AS transcript_source_digest,"
                "t.status AS transcript_status,t.output_path AS transcript_path,"
                "t.scope AS transcript_scope,t.qualification_status AS transcript_qualification_status,"
                "t.metadata_json AS transcript_metadata,s.status AS session_status,s.completeness,"
                "s.metadata_json AS session_metadata FROM transcripts t "
                "JOIN live_sessions s ON s.session_id=t.session_id "
                "WHERE t.transcript_id=? AND t.session_id=?",
                (expected_transcript_id, analysis_row["session_id"]),
            ).fetchone() if expected_transcript_id else None
            row_dict = dict(row or {})
            transcript_metadata = json_object(row_dict.get("transcript_metadata"))
            source_segment_id = str(transcript_metadata.get("source_segment_id") or "")
            canonical = conn.execute(
                "SELECT checksum,path FROM recording_segments WHERE segment_id=? AND session_id=? "
                "AND status='COMPLETE' AND lifecycle_status='CANONICAL_ACTIVE'",
                (source_segment_id, analysis_row["session_id"]),
            ).fetchone() if source_segment_id else None
            canonical_hash = str(canonical["checksum"] or "") if canonical else ""
            canonical_path = Path(str(canonical["path"] or "")) if canonical else None
            if canonical and not canonical_hash and canonical_path and canonical_path.is_file():
                canonical_hash = file_sha256(canonical_path)
            canonical_source_digest = (
                hashlib.sha256(("FULL_SESSION:" + canonical_hash).encode()).hexdigest()
                if canonical_hash else None
            )
            edges = [
                (str(edge["upstream_id"]), str(edge["upstream_version"]))
                for edge in conn.execute(
                    "SELECT upstream_id,upstream_version FROM lineage_edges "
                    "WHERE downstream_type='analysis' AND downstream_id=? "
                    "AND upstream_type='transcript' AND state='CURRENT' ORDER BY edge_id",
                    (analysis_row["analysis_id"],),
                ).fetchall()
            ]

        eligible, qualification_state, qualification = transcript_qualification(
            row_dict, canonical_source_digest=canonical_source_digest, minimum=minimum,
        )
        expected_digest = str(analysis_row.get("transcript_content_digest") or "")
        transcript_path = Path(str(row_dict.get("transcript_path") or ""))
        actual_digest = file_sha256(transcript_path) if transcript_path.is_file() else ""
        immutable_reasons: list[str] = []
        if analysis_row.get("scope") != ANALYSIS_SCOPE_FORMAL:
            immutable_reasons.append("ANALYSIS_SCOPE_NOT_FORMAL")
        if analysis_row.get("analysis_spec_version") != ANALYSIS_SPEC_VERSION:
            immutable_reasons.append("ANALYSIS_SPEC_VERSION_MISMATCH")
        if analysis_row.get("model_version") != MODEL_VERSION:
            immutable_reasons.append("MODEL_VERSION_MISMATCH")
        if analysis_row.get("prompt_version") != PROMPT_VERSION:
            immutable_reasons.append("PROMPT_VERSION_MISMATCH")
        if not expected_digest or actual_digest != expected_digest:
            immutable_reasons.append("TRANSCRIPT_CONTENT_DIGEST_MISMATCH")
        if analysis_row.get("source_digest") != expected_digest:
            immutable_reasons.append("ANALYSIS_SOURCE_DIGEST_MISMATCH")
        expected_edge = [(expected_transcript_id, expected_digest)]
        if edges != expected_edge:
            immutable_reasons.append("LINEAGE_NOT_EXACTLY_ONE_EXPECTED_TRANSCRIPT_DIGEST")
        if not eligible or immutable_reasons:
            qualification["reasons"] = [*qualification.get("reasons", []), *immutable_reasons]
            sample = qualification_state == SAMPLE_NONQUALIFYING
            blocked_status = "SAMPLE_NONQUALIFYING" if sample else "BLOCKED_SOURCE_QUALIFICATION"
            blocked_lineage = "INVALIDATED" if sample else "BLOCKED"
            blocked_scope = "SAMPLE_AUXILIARY" if sample else analysis_row.get("scope")
            blocked_qualification = SAMPLE_NONQUALIFYING if sample else "SOURCE_NONQUALIFYING"
            metadata = {
                **analysis_metadata,
                "qualification_state": qualification_state,
                "formal_analysis_eligible": False,
                "qualification": qualification,
                "qualification_checked_at": utc_now(),
            }
            with connect() as conn:
                conn.execute(
                    "UPDATE analyses SET status=?,lineage_state=?,scope=?,qualification_status=?,"
                    "metadata_json=? WHERE analysis_id=?",
                    (
                        blocked_status, blocked_lineage, blocked_scope,
                        blocked_qualification,
                        json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                        analysis_row["analysis_id"],
                    ),
                )
                conn.commit()
            source_blocked += 1
            continue

        try:
            text, source_rows, source_duration, scope, transcript_quality, _payload = prepare_analysis_source(
                transcript_path, expected_transcript_id,
                row_dict.get("transcript_metadata"), minimum, target,
            )
        except AnalysisSourceQualityError as exc:
            _quality_block(
                analysis_row, exc.coverage, reason=str(exc),
                analysis_metadata=analysis_metadata,
            )
            quality_blocked += 1
            continue
        except Exception as exc:
            with connect() as conn:
                conn.execute(
                    "UPDATE analyses SET status='WAITING_MODEL',metadata_json=? WHERE analysis_id=?",
                    (json.dumps({**analysis_metadata, "error_type": exc.__class__.__name__,
                                 "error_message": str(exc)[:500], "checked_at": utc_now()},
                                ensure_ascii=False, sort_keys=True),
                     analysis_row["analysis_id"]),
                )
                conn.commit()
            failed += 1
            continue

        prior = None
        with connect() as conn:
            candidates = conn.execute(
                "SELECT a.*,e.status AS evidence_status,e.manifest_hash AS evidence_manifest_hash "
                "FROM analyses a JOIN evidence_bundles e ON e.object_type='analysis' "
                "AND e.object_id=a.analysis_id WHERE a.transcript_id=? "
                "AND a.transcript_content_digest=? AND a.analysis_type='single_session' "
                "AND a.status='COMPLETE' AND a.scope='FORMAL_SINGLE_SESSION' "
                "AND a.qualification_status='FULL_SESSION_QUALIFIED' "
                "AND a.output_path IS NOT NULL AND a.analysis_id<>? "
                "ORDER BY CASE WHEN a.lineage_state='CURRENT' THEN 0 ELSE 1 END,a.analysis_id",
                (expected_transcript_id, expected_digest, analysis_row["analysis_id"]),
            ).fetchall()
        for candidate in candidates:
            candidate_path = Path(str(candidate["output_path"] or ""))
            if not candidate_path.is_file():
                continue
            candidate_digest = file_sha256(candidate_path)
            if (
                candidate["evidence_status"] != "VERIFIED"
                or not candidate["artifact_digest"]
                or candidate_digest != candidate["artifact_digest"]
                or candidate_digest != candidate["evidence_manifest_hash"]
            ):
                continue
            try:
                prior_artifact = json.loads(candidate_path.read_text(encoding="utf-8"))
                if (
                    str(prior_artifact.get("analysis_id") or "") != str(candidate["analysis_id"])
                    or str(prior_artifact.get("session_id") or "") != str(candidate["session_id"])
                    or str(prior_artifact.get("transcript_id") or "") != expected_transcript_id
                ):
                    continue
                rebuilt, coverage = revalidate_existing_artifact(
                    prior_artifact, text, source_rows, source_duration, scope,
                    transcript_quality, minimum, target,
                )
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            prior = dict(candidate)
            engine = json_object(rebuilt.get("engine"))
            result = json_object(rebuilt.get("result"))
            engine.update({
                "revalidated_from_analysis_id": prior["analysis_id"],
                "revalidated_from_artifact_digest": prior.get("artifact_digest"),
                "coverage_gate_version": 2,
            })
            revalidated += 1
            break
        else:
            try:
                result, engine = request_analysis(
                    text, source_duration=source_duration, source_rows=source_rows,
                    coverage_scope=scope, minimum=minimum, target=target,
                    transcript_quality=transcript_quality,
                )
                coverage = result["analysis_coverage"]
            except AnalysisSourceQualityError as exc:
                _quality_block(
                    analysis_row, exc.coverage, reason=str(exc),
                    analysis_metadata=analysis_metadata,
                )
                quality_blocked += 1
                continue
            except Exception as exc:
                with connect() as conn:
                    conn.execute(
                        "UPDATE analyses SET status='WAITING_MODEL',metadata_json=? WHERE analysis_id=?",
                        (json.dumps({**analysis_metadata, "qualification_state": QUALIFIED,
                                     "formal_analysis_eligible": True,
                                     "error_type": exc.__class__.__name__,
                                     "error_message": str(exc)[:500],
                                     "checked_at": utc_now()},
                                    ensure_ascii=False, sort_keys=True),
                         analysis_row["analysis_id"]),
                    )
                    conn.commit()
                failed += 1
                continue

        if not coverage.get("is_qualified"):
            _quality_block(
                analysis_row, coverage,
                reason="analysis coverage did not pass the formal 90% gate",
                analysis_metadata=analysis_metadata,
            )
            quality_blocked += 1
            continue

        try:
            reference_manifest = source_reference_manifest(result)
            if reference_manifest["reference_count"] < 1:
                raise ValueError("analysis returned no source-bound evidence references")
            evidence = {
                **reference_manifest,
                "transcript_id": expected_transcript_id,
                "transcript_artifact_sha256": expected_digest,
                "source_content_hash": engine["source_content_hash"],
                "source_segment_set_digest": engine["source_segment_set_digest"],
                "model_generated_timestamps": False,
                "nearest_segment_fallback": False,
            }
        except (KeyError, TypeError, ValueError) as exc:
            with connect() as conn:
                conn.execute(
                    "UPDATE analyses SET status='WAITING_MODEL',metadata_json=? "
                    "WHERE analysis_id=?",
                    (
                        json.dumps({
                            **analysis_metadata,
                            "error_type": "AnalysisEvidenceBindingError",
                            "error_message": str(exc)[:500],
                            "checked_at": utc_now(),
                        }, ensure_ascii=False, sort_keys=True),
                        analysis_row["analysis_id"],
                    ),
                )
                conn.commit()
            failed += 1
            continue

        artifact = {
            "analysis_id": analysis_row["analysis_id"],
            "session_id": analysis_row["session_id"],
            "transcript_id": expected_transcript_id,
            "transcript_content_digest": expected_digest,
            "analysis_spec_version": ANALYSIS_SPEC_VERSION,
            "model_version": MODEL_VERSION,
            "prompt_version": PROMPT_VERSION,
            "qualification_state": QUALIFIED,
            "engine": engine,
            "created_at": utc_now(),
            "evidence": evidence,
            "result": result,
        }
        if artifact_binding_status(artifact) != "BOUND_V1":
            raise RuntimeError("analysis artifact failed strict source binding self-check")
        output_path = ANALYSIS_ROOT / f"{analysis_row['analysis_id']}.json"
        write_json_atomic(output_path, artifact)
        artifact_digest = file_sha256(output_path)
        gate_status = "QUALIFIED_TARGET" if coverage.get("meets_target") else "QUALIFIED_MINIMUM"
        clean_metadata = {
            key: value for key, value in analysis_metadata.items()
            if key not in {
                "error_type", "error_message", "checked_at",
                "quality_diagnostic_path", "quality_diagnostic_digest",
            }
        }
        metadata = {
            **clean_metadata,
            **engine,
            "semantic_engine": MODEL_VERSION,
            "transcript_segment_count": len(source_rows),
            "coverage_scope": scope,
            "analysis_coverage": _coverage_metadata(coverage),
            "quality_gate_status": gate_status,
            "is_formal_output": True,
            "qualification_state": QUALIFIED,
            "formal_analysis_eligible": True,
            "artifact_digest": artifact_digest,
            "evidence_binding_status": "BOUND_V1",
            "source_binding_digest": evidence["source_binding_digest"],
            "referenced_source_segment_count": evidence["reference_count"],
            "transcript_artifact_sha256": evidence["transcript_artifact_sha256"],
            "qualification_checked_at": utc_now(),
        }
        with connect() as conn:
            updated = conn.execute(
                "UPDATE analyses SET status='COMPLETE',output_path=?,artifact_digest=?,"
                "metadata_json=?,lineage_state='CURRENT',qualification_status=? "
                "WHERE analysis_id=? AND transcript_id=? AND transcript_content_digest=? "
                "AND analysis_spec_version=? AND model_version=? AND prompt_version=?",
                (
                    str(output_path), artifact_digest,
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True), QUALIFIED,
                    analysis_row["analysis_id"], expected_transcript_id, expected_digest,
                    ANALYSIS_SPEC_VERSION, MODEL_VERSION, PROMPT_VERSION,
                ),
            )
            if updated.rowcount != 1:
                conn.rollback()
                raise RuntimeError("immutable analysis identity changed before completion")

            superseded_rows = conn.execute(
                "SELECT analysis_id,metadata_json FROM analyses WHERE transcript_id=? "
                "AND analysis_type='single_session' AND analysis_id<>? "
                "AND status='COMPLETE' AND lineage_state='CURRENT'",
                (expected_transcript_id, analysis_row["analysis_id"]),
            ).fetchall()
            for old in superseded_rows:
                old_metadata = {
                    **json_object(old["metadata_json"]),
                    "superseded_by_analysis_id": analysis_row["analysis_id"],
                    "superseded_at": utc_now(),
                }
                conn.execute(
                    "UPDATE analyses SET lineage_state='SUPERSEDED',metadata_json=? "
                    "WHERE analysis_id=? AND lineage_state='CURRENT'",
                    (json.dumps(old_metadata, ensure_ascii=False, sort_keys=True),
                     old["analysis_id"]),
                )
                conn.execute(
                    "UPDATE lineage_edges SET state='SUPERSEDED' "
                    "WHERE downstream_type='analysis' AND downstream_id=? "
                    "AND upstream_type='transcript' AND state='CURRENT'",
                    (old["analysis_id"],),
                )
                superseded_outbox_id = enqueue_outbox_conn(
                    conn, object_type="semantic_projection", object_id=old["analysis_id"],
                    destination="feishu_base",
                    payload={"analysis_id": old["analysis_id"],
                             "profile_id": "edu_live_competitor_intel",
                             "lineage_state": "SUPERSEDED",
                             "superseded_by_analysis_id": analysis_row["analysis_id"],
                             "release_after_evidence_analysis_id": analysis_row["analysis_id"],
                             "correction_version": 1},
                    scope=ANALYSIS_SCOPE_FORMAL, qualification_status=QUALIFIED,
                )
                conn.execute(
                    "UPDATE outbox SET status='HELD_EVIDENCE' WHERE outbox_id=? "
                    "AND status='PENDING'",
                    (superseded_outbox_id,),
                )

            conn.execute(
                "INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,"
                "manifest_path,manifest_hash,scope,qualification_status,metadata_json) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id) DO UPDATE SET "
                "status=excluded.status,manifest_path=excluded.manifest_path,"
                "manifest_hash=excluded.manifest_hash,scope=excluded.scope,"
                "qualification_status=excluded.qualification_status,"
                "metadata_json=excluded.metadata_json",
                (
                    "bundle:" + analysis_row["analysis_id"], "analysis",
                    analysis_row["analysis_id"], "REQUIRED", str(output_path),
                    artifact_digest, ANALYSIS_SCOPE_FORMAL, QUALIFIED,
                    json.dumps({
                        "transcript_id": expected_transcript_id,
                        "transcript_content_digest": expected_digest,
                        "artifact_digest": artifact_digest,
                        "analysis_coverage": _coverage_metadata(coverage),
                        "evidence_binding_status": "BOUND_V1",
                        "source_binding_digest": evidence["source_binding_digest"],
                        "referenced_source_segments": [
                            {
                                "source_segment_id": ref["source_segment_id"],
                                "content_digest": ref["content_digest"],
                            }
                            for ref in evidence["references"]
                        ],
                        "qualification_state": QUALIFIED,
                        "formal_analysis_eligible": True,
                        "release_projection_after_evidence": True,
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            current_outbox_id = enqueue_outbox_conn(
                conn, object_type="semantic_projection",
                object_id=analysis_row["analysis_id"], destination="feishu_base",
                payload={"analysis_id": analysis_row["analysis_id"],
                         "profile_id": "edu_live_competitor_intel",
                         "qualification_state": QUALIFIED,
                         "artifact_digest": artifact_digest,
                         "analysis_spec_version": ANALYSIS_SPEC_VERSION,
                         "release_after_evidence_analysis_id": analysis_row["analysis_id"]},
                scope=ANALYSIS_SCOPE_FORMAL, qualification_status=QUALIFIED,
            )
            conn.execute(
                "UPDATE outbox SET status='HELD_EVIDENCE' WHERE outbox_id=? "
                "AND status='PENDING'",
                (current_outbox_id,),
            )
            conn.commit()
        completed += 1

    health_status, health = analysis_health_snapshot(minimum, target)
    result = {
        "completed": completed,
        "revalidated": revalidated,
        "quality_blocked": quality_blocked,
        "source_qualification_blocked": source_blocked,
        "waiting_model": failed,
        "minimum_coverage_rate": minimum,
        "target_coverage_rate": target,
        "analysis_spec_version": ANALYSIS_SPEC_VERSION,
        **health,
        "checked_at": utc_now(),
    }
    healthy = (
        failed == 0 and quality_blocked == 0 and source_blocked == 0
        and health_status == "READY"
    )
    upsert_heartbeat(
        "analysis-v3", "READY" if healthy else "DEGRADED",
        result, success=healthy,
    )
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
