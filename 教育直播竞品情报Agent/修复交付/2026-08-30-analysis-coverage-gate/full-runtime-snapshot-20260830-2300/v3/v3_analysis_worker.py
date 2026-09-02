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


def transcript_text(path: Path, limit: int | None = None) -> tuple[str, list[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("segments") or []
    parts = []
    for row in rows:
        # Feed the losslessly normalized view to the semantic model while the
        # original `text` remains in the transcript artifact for evidence and
        # audit.  The normalizer only removes Unicode/whitespace noise; it
        # never substitutes an unverified domain term.
        text = str(row.get("normalized_text") or row.get("text") or "").strip()
        if text:
            parts.append(f"[{float(row.get('start') or 0):.2f}-{float(row.get('end') or 0):.2f}] {text}")
    text = "\n".join(parts)
    return (text[:limit] if limit is not None else text), rows


def source_rows_from_transcript(payload: dict, transcript_id: str) -> list[dict]:
    """Build stable, auditable source rows from the original transcript.

    ASR artifacts currently do not carry a segment primary key.  The transcript
    id plus the original array index is stable for that immutable artifact and
    avoids pretending a model-produced timestamp is a source identifier.
    Invalid timestamps or empty text fail closed before any model call.
    """
    segments = payload.get("segments")
    if not isinstance(segments, list) or not segments:
        raise ValueError("analysis source transcript has no segment array")
    rows: list[dict] = []
    for index, item in enumerate(segments):
        if not isinstance(item, dict):
            raise ValueError(f"analysis source segment {index} is not an object")
        start, end = finite_number(item.get("start")), finite_number(item.get("end"))
        if start is None or end is None or start < 0 or end <= start:
            raise ValueError(f"analysis source segment {index} has invalid timestamps")
        text = str(item.get("normalized_text") or item.get("text") or "").strip()
        if not text:
            raise ValueError(f"analysis source segment {index} has no analyzable text")
        segment_id = f"{transcript_id}:segment:{index:06d}"
        rows.append({
            "segment_index": index,
            "source_segment_id": segment_id,
            "start": start,
            "end": end,
            "text": text,
            "line": f"[{start:.2f}-{end:.2f}] {text}",
        })
    return rows


def timed_lines(text: str) -> list[dict]:
    rows = []
    pattern = re.compile(r"^\[([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)\]\s*(.*)$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        start, end = float(match.group(1)), float(match.group(2))
        if start < 0 or end < start:
            continue
        index = len(rows)
        rows.append({"segment_index": index, "source_segment_id": f"line:segment:{index:06d}",
                     "start": start, "end": end, "text": match.group(3), "line": line.strip()})
    return rows


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
    result = empty_chunk_result()
    for field in CHUNK_FIELDS:
        items = value.get(field, [])
        if not isinstance(items, list):
            raise ValueError(f"analysis field {field} must be an array")
        if len(items) > 3:
            raise ValueError(f"analysis field {field} exceeded the maximum item count")
        normalized = []
        for item in items:
            if not isinstance(item, dict):
                raise ValueError(f"analysis field {field} contains a non-object item")
            try:
                start, end = float(item["start"]), float(item["end"])
            except (KeyError, TypeError, ValueError):
                raise ValueError(f"analysis field {field} item lacks numeric start/end") from None
            if start < chunk["start"] - 0.01 or end > chunk["end"] + 0.01 or end < start:
                raise ValueError(f"analysis field {field} timestamp is outside its source chunk")
            summary = str(item.get("summary") or item.get("name") or "").strip()
            if not summary or len(summary) > 160:
                raise ValueError(f"analysis field {field} item has invalid summary length")
            normalized.append({"summary": summary, "start": start, "end": end})
        result[field] = normalized
    return result


def request_chunk(chunk: dict) -> tuple[dict, dict]:
    key = read_env_key("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY is not configured")
    example = {field: [] for field in CHUNK_FIELDS}
    system = (
        "你是直播竞品情报分析器。只能从所给时间片逐字稿提取证据，不能补写事实。"
        "输出严格JSON对象，不要Markdown或解释。必须包含示例中的全部字段，每字段最多2项；"
        "每项格式只能是{summary,start,end}，summary不超过80个汉字，start/end必须落在本时间片。"
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
                "上次输出无效或被截断。重新从原时间片生成更短的完整JSON；每字段最多1项，不能省略字段。"})
    raise RuntimeError("analysis chunk failed after retries: " + " | ".join(failures))


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
                key = (re.sub(r"\s+", "", item["summary"]).lower(), round(item["start"], 2), round(item["end"], 2))
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
        for item in values:
            candidates = [row for row in source_rows if row["end"] > item["start"] and row["start"] < item["end"]]
            if not candidates:
                candidates = source_rows
            row = min(candidates, key=lambda candidate: abs(candidate["start"] - item["start"]))
            timestamps.append({"start": row["start"], "end": row["end"],
                               "source_segment_index": row["segment_index"],
                               "source_segment_id": row.get("source_segment_id"),
                               "source_text": row["text"]})
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
        "schema_version": "2.0",
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
    })
    return merged


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
                    "successful_source_segment_count": len(successful_ids),
                    "successful_source_segment_ids_sha256": _identifier_hash(successful_ids),
                    "chunk_diagnostics": diagnostics, "merge_engine": "deterministic-python-v3-coverage-gated"}


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


def once() -> dict:
    completed = failed = quality_blocked = revalidated = 0
    minimum, target = load_analysis_quality_config()
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT a.*,t.output_path AS transcript_path,t.transcript_id,"
            "t.metadata_json AS transcript_metadata "
            "FROM analyses a "
            "JOIN lineage_edges le ON le.downstream_type='analysis' "
            "AND le.downstream_id=a.analysis_id AND le.upstream_type='transcript' "
            "AND le.upstream_version=a.source_digest "
            "JOIN transcripts t ON t.transcript_id=le.upstream_id "
            "WHERE a.analysis_type='single_session' "
            "AND a.status IN ('PENDING','WAITING_MODEL','COMPLETE','QUALITY_BLOCKED') "
            "AND t.status='COMPLETE' ORDER BY a.analysis_id"
        ).fetchall()
    for row in rows:
        row = dict(row)
        if _current_analysis_gate(row, minimum, target):
            continue
        try:
            text, source_rows, source_duration, scope, transcript_quality, _payload = prepare_analysis_source(
                Path(row["transcript_path"]), row["transcript_id"], row.get("transcript_metadata"),
                minimum, target,
            )
            existing_path = Path(row["output_path"]) if row.get("output_path") and str(row["output_path"]).startswith("/") else None
            output_path = existing_path if row["status"] == "COMPLETE" and existing_path and existing_path.is_file() else ANALYSIS_ROOT / f"{row['analysis_id']}.json"
            was_complete = row["status"] == "COMPLETE" and output_path.is_file()
            if was_complete:
                artifact, coverage = revalidate_existing_artifact(
                    json.loads(output_path.read_text(encoding="utf-8")), text, source_rows,
                    source_duration, scope, transcript_quality, minimum, target,
                )
                result, engine = artifact["result"], artifact["engine"]
                revalidated += 1
            else:
                result, engine = request_analysis(
                    text, source_duration=source_duration, source_rows=source_rows,
                    coverage_scope=scope, minimum=minimum, target=target,
                    transcript_quality=transcript_quality,
                )
                coverage = result["analysis_coverage"]
                artifact = {"analysis_id": row["analysis_id"], "session_id": row["session_id"],
                            "transcript_id": row["transcript_id"], "engine": engine,
                            "created_at": utc_now(), "result": result}
            write_json_atomic(output_path, artifact)
            status = "COMPLETE" if coverage["is_qualified"] else "QUALITY_BLOCKED"
            gate_status = "QUALIFIED_TARGET" if coverage["meets_target"] else (
                "QUALIFIED_MINIMUM" if coverage["is_qualified"] else "QUALITY_BLOCKED"
            )
            old_metadata = json_object(row.get("metadata_json"))
            metadata = {**old_metadata, **engine, "semantic_engine": "deepseek-chat",
                        "transcript_segment_count": len(source_rows), "coverage_scope": scope,
                        "analysis_coverage": _coverage_metadata(coverage),
                        "quality_gate_status": gate_status,
                        "is_formal_output": coverage["is_qualified"]}
            with connect() as conn:
                conn.execute("UPDATE analyses SET status=?,output_path=?,metadata_json=?,lineage_state='CURRENT' WHERE analysis_id=?", (status, str(output_path), json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["analysis_id"]))
                conn.execute("INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,metadata_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id) DO UPDATE SET status=excluded.status,manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,metadata_json=excluded.metadata_json", ("bundle:" + row["analysis_id"], "analysis", row["analysis_id"], "REQUIRED" if status == "COMPLETE" else "QUALITY_BLOCKED", str(output_path), hashlib.sha256(output_path.read_bytes()).hexdigest(), json.dumps({"transcript_id": row["transcript_id"], "analysis_coverage": _coverage_metadata(coverage)}, ensure_ascii=False)))
                if status == "COMPLETE" and not was_complete:
                    enqueue_outbox_conn(conn, object_type="semantic_projection", object_id=row["analysis_id"], destination="feishu_base", payload={"analysis_id": row["analysis_id"], "profile_id": "edu_live_competitor_intel"})
                conn.commit()
            if status == "COMPLETE":
                completed += 1
            else:
                quality_blocked += 1
        except AnalysisSourceQualityError as exc:
            coverage = exc.coverage
            metadata = {**json_object(row.get("metadata_json")),
                        "coverage_scope": coverage.get("coverage_scope"),
                        "analysis_coverage": _coverage_metadata(coverage),
                        "quality_gate_status": "QUALITY_BLOCKED",
                        "is_formal_output": False,
                        "error_type": exc.__class__.__name__, "error_message": str(exc),
                        "checked_at": utc_now()}
            with connect() as conn:
                conn.execute("UPDATE analyses SET status='QUALITY_BLOCKED',metadata_json=? WHERE analysis_id=?", (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row["analysis_id"]))
                conn.execute("UPDATE evidence_bundles SET status='QUALITY_BLOCKED',metadata_json=? WHERE object_type='analysis' AND object_id=?", (json.dumps({"transcript_id": row["transcript_id"], "analysis_coverage": _coverage_metadata(coverage)}, ensure_ascii=False), row["analysis_id"]))
                conn.commit()
            quality_blocked += 1
        except Exception as exc:  # fail closed; no fabricated analysis
            with connect() as conn:
                conn.execute("UPDATE analyses SET status='WAITING_MODEL',metadata_json=? WHERE analysis_id=?", (json.dumps({"error_type": exc.__class__.__name__, "error_message": str(exc)[:500], "checked_at": utc_now()}, ensure_ascii=False), row["analysis_id"]))
                conn.commit()
            failed += 1
    result = {"completed": completed, "revalidated": revalidated,
              "quality_blocked": quality_blocked, "waiting_model": failed,
              "minimum_coverage_rate": minimum, "target_coverage_rate": target,
              "checked_at": utc_now()}
    healthy = failed == 0 and quality_blocked == 0
    upsert_heartbeat("analysis-v3", "READY" if healthy else "DEGRADED", result, success=healthy)
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
