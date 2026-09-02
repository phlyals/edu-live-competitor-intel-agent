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
import socket
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
from v3_long_jobs import (
    ANALYSIS,
    LeaseLostError,
    claim_next,
    fail_or_retry,
    finish,
    parse_checkpoint,
    reconcile_exhausted,
    renew,
    save_checkpoint,
    versioned_output_path,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_ROOT = ROOT.parent
ANALYSIS_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/analysis")
RUNNING = True
ANALYSIS_CHUNK_CHAR_LIMIT = 5000
ANALYSIS_MAX_RETRIES = 3
ANALYSIS_LEASE_SECONDS = 600
DEFAULT_ANALYSIS_COVERAGE_MINIMUM = 0.90
DEFAULT_ANALYSIS_COVERAGE_TARGET = 0.95
CHUNK_FIELDS = (
    "instructor", "course_content", "interaction_patterns", "product_handoff",
    "hook", "pain_points", "claims", "cta", "risks", "evidence_refs",
)


def stop(*_args):
    global RUNNING
    RUNNING = False


class AnalysisRequestError(RuntimeError):
    """A redacted provider or model failure with an explicit retry policy."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        retry_after_seconds: float | None = None,
        clear_checkpoint: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.retryable = bool(retryable)
        self.retry_after_seconds = retry_after_seconds
        self.clear_checkpoint = bool(clear_checkpoint)


def worker_id() -> str:
    return f"analysis-v3:{socket.gethostname()}:{os.getpid()}"


def _retry_after(response: httpx.Response) -> float | None:
    value = getattr(response, "headers", {}).get("retry-after")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if math.isfinite(seconds) and seconds >= 0 else None


def _http_status_error(response: httpx.Response) -> AnalysisRequestError | None:
    status = int(getattr(response, "status_code", 200))
    if 200 <= status < 300:
        return None
    if status in {401, 403}:
        return AnalysisRequestError(
            f"HTTP_{status}_AUTH",
            f"analysis provider rejected credentials ({status})",
            retryable=False,
        )
    if status == 429:
        return AnalysisRequestError(
            "HTTP_429_RATE_LIMIT",
            "analysis provider rate limited the request",
            retryable=True,
            retry_after_seconds=_retry_after(response),
        )
    if status == 408:
        return AnalysisRequestError(
            "HTTP_408_TIMEOUT",
            "analysis provider reported a request timeout",
            retryable=True,
        )
    if 500 <= status <= 599:
        return AnalysisRequestError(
            f"HTTP_{status}_SERVER",
            f"analysis provider server error ({status})",
            retryable=True,
        )
    return AnalysisRequestError(
        f"HTTP_{status}_REQUEST",
        f"analysis provider rejected the request ({status})",
        retryable=False,
    )


def _request_exception(exc: Exception) -> AnalysisRequestError:
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError)):
        return AnalysisRequestError(
            "HTTP_CONNECT", "analysis provider connection failed", retryable=True
        )
    if isinstance(exc, httpx.TimeoutException):
        return AnalysisRequestError(
            "HTTP_TIMEOUT", "analysis provider request timed out", retryable=True
        )
    if isinstance(exc, httpx.RequestError):
        return AnalysisRequestError(
            "HTTP_NETWORK", "analysis provider network request failed", retryable=True
        )
    return AnalysisRequestError(
        "HTTP_CLIENT", exc.__class__.__name__, retryable=False
    )


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


def request_chunk(
    chunk: dict,
    *,
    before_attempt=None,
    sleep=time.sleep,
    client_factory=None,
) -> tuple[dict, dict]:
    client_factory = client_factory or httpx.Client
    key = read_env_key("DEEPSEEK_API_KEY")
    if not key:
        raise AnalysisRequestError(
            "CONFIG_MISSING_API_KEY",
            "DEEPSEEK_API_KEY is not configured",
            retryable=False,
        )
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
    body = {
        "model": "deepseek-chat",
        "temperature": 0,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": "请分析这一时间片并输出JSON：\n" + chunk["text"]},
        ],
    }
    failures: list[str] = []
    last_error: AnalysisRequestError | None = None
    content = ""
    for attempt in range(1, ANALYSIS_MAX_RETRIES + 1):
        if before_attempt:
            before_attempt(attempt)
        try:
            try:
                with client_factory(timeout=180) as client:
                    response = client.post(
                        "https://api.deepseek.com/chat/completions",
                        headers={
                            "Authorization": f"Bearer {key}",
                            "Content-Type": "application/json",
                        },
                        json=body,
                    )
            except Exception as exc:
                raise _request_exception(exc) from exc
            status_error = _http_status_error(response)
            if status_error:
                raise status_error
            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError) as exc:
                raise AnalysisRequestError(
                    "HTTP_RESPONSE_JSON_INVALID",
                    "analysis provider response envelope is not JSON",
                    retryable=True,
                ) from exc
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("choices"), list)
                or not payload["choices"]
            ):
                raise AnalysisRequestError(
                    "HTTP_RESPONSE_SCHEMA_INVALID",
                    "analysis provider response has no choice",
                    retryable=True,
                )
            choice = payload["choices"][0]
            finish_reason = choice.get("finish_reason")
            content = ((choice.get("message") or {}).get("content") or "").strip()
            if finish_reason != "stop":
                retryable = finish_reason not in {"content_filter", "safety"}
                raise AnalysisRequestError(
                    "MODEL_FINISH_INCOMPLETE" if retryable else "MODEL_FINISH_BLOCKED",
                    f"analysis completion finish_reason={finish_reason!r}",
                    retryable=retryable,
                )
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError as exc:
                raise AnalysisRequestError(
                    "MODEL_JSON_INVALID",
                    "analysis model output is not complete JSON",
                    retryable=True,
                ) from exc
            try:
                result = validate_chunk_result(parsed, chunk)
            except ValueError as exc:
                raise AnalysisRequestError(
                    "MODEL_SCHEMA_INVALID", str(exc), retryable=True
                ) from exc
            return result, {
                "response_id": payload.get("id"),
                "finish_reason": finish_reason,
                "usage": payload.get("usage"),
                "attempt": attempt,
                "content_hash": hashlib.sha256(content.encode()).hexdigest(),
            }
        except AnalysisRequestError as exc:
            last_error = exc
            failures.append(f"attempt {attempt}: {exc.code}")
            if not exc.retryable:
                raise
            if attempt >= ANALYSIS_MAX_RETRIES:
                break
            if exc.code.startswith("MODEL_"):
                body["messages"].append({
                    "role": "assistant",
                    "content": content[-1000:],
                })
                body["messages"].append({
                    "role": "user",
                    "content": (
                        f"上次输出无效：{exc}。重新生成更短的完整JSON；每字段最多1项，不能省略字段。"
                        "source_segment_ids必须是含1到8个输入原始ID的JSON数组；"
                        "只能引用输入中的source_segment_id；不要输出start/end或其他字段。"
                    ),
                })
            sleep(min(
                30.0,
                max(float(2 ** (attempt - 1)), float(exc.retry_after_seconds or 0)),
            ))
    assert last_error is not None
    raise AnalysisRequestError(
        last_error.code,
        f"analysis chunk {chunk.get('index')} failed after bounded retries: "
        + " | ".join(failures),
        retryable=last_error.retryable,
        retry_after_seconds=last_error.retry_after_seconds,
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


def request_analysis(
    text: str,
    source_duration: float | None = None,
    *,
    source_rows: list[dict] | None = None,
    coverage_scope: str = "FULL_SESSION",
    minimum: float = DEFAULT_ANALYSIS_COVERAGE_MINIMUM,
    target: float = DEFAULT_ANALYSIS_COVERAGE_TARGET,
    transcript_quality: dict | None = None,
    checkpoint: dict | None = None,
    checkpoint_dir: Path | None = None,
    on_checkpoint=None,
    before_request=None,
    request_fn=None,
    char_limit: int = ANALYSIS_CHUNK_CHAR_LIMIT,
) -> tuple[dict, dict]:
    request_fn = request_fn or request_chunk
    chunks = analysis_chunks(text, char_limit=char_limit, source_rows=source_rows)
    source_hash = hashlib.sha256(text.encode()).hexdigest()
    source_set_digest = _json_digest([
        {
            "source_segment_id": row["source_segment_id"],
            "content_digest": row["content_digest"],
        }
        for row in (source_rows or [])
    ])
    state = (
        checkpoint
        if isinstance(checkpoint, dict)
        and checkpoint.get("source_content_hash") == source_hash
        and checkpoint.get("source_segment_set_digest") == source_set_digest
        and checkpoint.get("analysis_spec_version") == ANALYSIS_SPEC_VERSION
        and checkpoint.get("prompt_version") == PROMPT_VERSION
        else {}
    )
    completed_chunks = (
        dict(state.get("chunks"))
        if isinstance(state.get("chunks"), dict)
        else {}
    )
    results: list[dict] = []
    diagnostics: list[dict] = []
    successful_ids: list[str] = []
    reused_chunks = 0
    for chunk in chunks:
        saved = completed_chunks.get(str(chunk["index"]))
        result = diagnostic = None
        if isinstance(saved, dict):
            try:
                result = validate_chunk_result(saved.get("result"), chunk)
                diagnostic = (
                    saved.get("diagnostic")
                    if isinstance(saved.get("diagnostic"), dict)
                    else {}
                )
                if (
                    int(diagnostic.get("chunk_index", -1)) != chunk["index"]
                    or int(diagnostic.get("row_count", -1)) != chunk["row_count"]
                    or diagnostic.get("finish_reason") != "stop"
                    or diagnostic.get("source_segment_ids_sha256")
                    != _identifier_hash(chunk["source_segment_ids"])
                ):
                    result = diagnostic = None
                checkpoint_path = Path(str(saved.get("path") or ""))
                expected_hash = str(saved.get("sha256") or "")
                if expected_hash and (
                    not checkpoint_path.is_file()
                    or file_sha256(checkpoint_path) != expected_hash
                ):
                    result = diagnostic = None
            except (OSError, TypeError, ValueError):
                result = diagnostic = None
        if result is not None:
            reused_chunks += 1
        if result is None:
            result, provider_diagnostic = request_fn(
                chunk, before_attempt=before_request
            )
            diagnostic = {
                "chunk_index": chunk["index"],
                "start": chunk["start"],
                "end": chunk["end"],
                "row_count": chunk["row_count"],
                "source_segment_ids": chunk["source_segment_ids"],
                "source_segment_ids_sha256": _identifier_hash(
                    chunk["source_segment_ids"]
                ),
                **provider_diagnostic,
            }
            record = {"result": result, "diagnostic": diagnostic}
            if checkpoint_dir:
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                checkpoint_path = checkpoint_dir / f"chunk-{chunk['index']:05d}.json"
                write_json_atomic(checkpoint_path, record)
                record.update(
                    path=str(checkpoint_path),
                    sha256=file_sha256(checkpoint_path),
                )
            completed_chunks[str(chunk["index"])] = record
            state = {
                "schema_version": 2,
                "phase": "ANALYZING_CHUNKS",
                "source_content_hash": source_hash,
                "source_segment_set_digest": source_set_digest,
                "analysis_spec_version": ANALYSIS_SPEC_VERSION,
                "model_version": MODEL_VERSION,
                "prompt_version": PROMPT_VERSION,
                "chunk_count": len(chunks),
                "completed_chunk_count": len(completed_chunks),
                "chunks": completed_chunks,
            }
            if on_checkpoint:
                on_checkpoint(state)
        results.append(result)
        diagnostics.append(diagnostic)
        successful_ids.extend(chunk["source_segment_ids"])
    merged = merge_chunk_results(
        results, chunks, source_duration,
        coverage_scope=coverage_scope,
        minimum=minimum,
        target=target,
        transcript_quality=transcript_quality,
        successful_source_segment_ids=successful_ids,
    )
    return merged, {
        "provider": "deepseek",
        "model": MODEL_VERSION,
        "response_ids": [item.get("response_id") for item in diagnostics],
        "source_content_hash": source_hash,
        "source_segment_set_digest": source_set_digest,
        "chunk_count": len(chunks),
        "successful_source_segment_count": len(successful_ids),
        "successful_source_segment_ids_sha256": _identifier_hash(successful_ids),
        "chunk_diagnostics": diagnostics,
        "checkpoint_reused_chunk_count": reused_chunks,
        "merge_engine": "deterministic-python-v4-source-ids-coverage-gated-checkpointed",
    }


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
        if (
            row["status"] == "COMPLETE"
            and row.get("lineage_state") == "CURRENT"
            and _current_analysis_gate(row, minimum, target)
        ):
            qualified += 1
            coverage = json_object(row.get("metadata_json")).get("analysis_coverage") or {}
            if coverage.get("meets_target") is True:
                target_met += 1
            if row.get("evidence_status") != "VERIFIED":
                evidence_pending += 1
        elif row.get("lineage_state") in {"STALE", "CANDIDATE"}:
            waiting += 1
        elif row["status"] in {
            "QUALITY_BLOCKED", "BLOCKED_SOURCE_QUALIFICATION", "FAILED_FINAL"
        }:
            blocked += 1
        elif row["status"] in {
            "PENDING", "WAITING_MODEL", "RETRY_WAIT", "RUNNING"
        }:
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


def _quality_block_claim(
    job: dict,
    analysis_row: dict,
    coverage: dict,
    *,
    reason: str,
    analysis_metadata: dict,
) -> str:
    """Publish a fenced non-formal diagnostic and end the claimed job."""
    diagnostic_base = ANALYSIS_ROOT / "quality-blocked" / f"{analysis_row['analysis_id']}.coverage.json"
    diagnostic_path = versioned_output_path(diagnostic_base, job)
    diagnostic = {
        "analysis_id": analysis_row["analysis_id"],
        "session_id": analysis_row["session_id"],
        "transcript_id": analysis_row["transcript_id"],
        "transcript_content_digest": analysis_row["transcript_content_digest"],
        "analysis_spec_version": analysis_row["analysis_spec_version"],
        "model_version": analysis_row["model_version"],
        "prompt_version": analysis_row["prompt_version"],
        "lease_epoch": int(job["lease_epoch"]),
        "attempt": int(job["attempts"]),
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
    checkpoint = {
        **parse_checkpoint(job.get("checkpoint_json")),
        "schema_version": 2,
        "phase": "QUALITY_BLOCKED",
        "diagnostic_path": str(diagnostic_path),
        "diagnostic_sha256": diagnostic_digest,
    }
    with connect() as conn:
        finish(
            conn, ANALYSIS, job, "QUALITY_BLOCKED",
            {
                "lineage_state": "BLOCKED",
                "qualification_status": "ANALYSIS_QUALITY_BLOCKED",
                "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                "checkpoint_json": json.dumps(checkpoint, ensure_ascii=False, sort_keys=True),
            },
            commit_transaction=False,
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
                json.dumps(
                    {"analysis_coverage": _coverage_metadata(coverage), "reason": reason},
                    ensure_ascii=False, sort_keys=True,
                ),
            ),
        )
        conn.commit()
    return "QUALITY_BLOCKED"


def _block_source_claim(
    job: dict,
    analysis_metadata: dict,
    qualification_state: str,
    qualification: dict,
) -> str:
    sample = qualification_state == SAMPLE_NONQUALIFYING
    status = "SAMPLE_NONQUALIFYING" if sample else "BLOCKED_SOURCE_QUALIFICATION"
    metadata = {
        **analysis_metadata,
        "qualification_state": qualification_state,
        "formal_analysis_eligible": False,
        "qualification": qualification,
        "qualification_checked_at": utc_now(),
    }
    with connect() as conn:
        finish(
            conn, ANALYSIS, job, status,
            {
                "lineage_state": "INVALIDATED" if sample else "BLOCKED",
                "scope": "SAMPLE_AUXILIARY" if sample else job.get("scope"),
                "qualification_status": (
                    SAMPLE_NONQUALIFYING if sample else "SOURCE_NONQUALIFYING"
                ),
                "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            },
        )
    return "SOURCE_BLOCKED"


def _release_failed_claim(
    job: dict,
    *,
    error_type: str,
    error_message: str,
    retryable: bool,
    retry_after_seconds: float | None = None,
    checkpoint: dict | None = None,
) -> str:
    try:
        with connect() as conn:
            return fail_or_retry(
                conn, ANALYSIS, job,
                error_type=error_type,
                error_message=error_message,
                retryable=retryable,
                retry_after_seconds=retry_after_seconds,
                checkpoint=checkpoint,
            )
    except LeaseLostError:
        return "LEASE_LOST"


def process_claim(job: dict) -> str:
    """Process exactly one fenced analysis lease."""
    minimum, target = load_analysis_quality_config()
    analysis_metadata = json_object(job.get("metadata_json"))
    recompute_request_id = str(
        analysis_metadata.get("recompute_request_id") or ""
    )
    is_recompute_candidate = bool(
        recompute_request_id and job.get("lineage_state") == "CANDIDATE"
    )
    try:
        expected_transcript_id = str(job.get("transcript_id") or "")
        with connect() as conn:
            row = conn.execute(
                "SELECT t.transcript_id,t.session_id,t.source_digest AS transcript_source_digest,"
                "t.status AS transcript_status,t.output_path AS transcript_path,"
                "t.engine AS transcript_engine,t.model AS transcript_model,"
                "t.scope AS transcript_scope,t.qualification_status AS transcript_qualification_status,"
                "t.metadata_json AS transcript_metadata,s.status AS session_status,s.completeness,"
                "s.metadata_json AS session_metadata FROM transcripts t "
                "JOIN live_sessions s ON s.session_id=t.session_id "
                "WHERE t.transcript_id=? AND t.session_id=?",
                (expected_transcript_id, job["session_id"]),
            ).fetchone() if expected_transcript_id else None
            row_dict = dict(row or {})
            transcript_metadata = json_object(row_dict.get("transcript_metadata"))
            source_segment_id = str(transcript_metadata.get("source_segment_id") or "")
            canonical = conn.execute(
                "SELECT checksum,path FROM recording_segments WHERE segment_id=? AND session_id=? "
                "AND status='COMPLETE' AND lifecycle_status='CANONICAL_ACTIVE'",
                (source_segment_id, job["session_id"]),
            ).fetchone() if source_segment_id else None
            canonical_hash = str(canonical["checksum"] or "") if canonical else ""
            canonical_path = Path(str(canonical["path"] or "")) if canonical else None
            if canonical and not canonical_hash and canonical_path and canonical_path.is_file():
                canonical_hash = file_sha256(canonical_path)
            canonical_source_digest = (
                hashlib.sha256(("FULL_SESSION:" + canonical_hash).encode()).hexdigest()
                if canonical_hash else None
            )
            expected_edge_state = "CANDIDATE" if is_recompute_candidate else "CURRENT"
            edges = [
                (
                    str(edge["upstream_id"]), str(edge["upstream_version"]),
                    str(edge["binding_status"]), edge["upstream_engine_version"],
                    edge["upstream_model_version"], edge["downstream_model_version"],
                    edge["downstream_prompt_version"], edge["downstream_schema_version"],
                )
                for edge in conn.execute(
                    "SELECT upstream_id,upstream_version,binding_status,"
                    "upstream_engine_version,upstream_model_version,"
                    "downstream_model_version,downstream_prompt_version,"
                    "downstream_schema_version FROM lineage_edges "
                    "WHERE downstream_type='analysis' AND downstream_id=? "
                    "AND upstream_type='transcript' AND state=? ORDER BY edge_id",
                    (job["analysis_id"], expected_edge_state),
                ).fetchall()
            ]
            recompute_request = conn.execute(
                "SELECT request_id,status,candidate_analysis_id,upstream_id,"
                "new_upstream_digest,target_analysis_spec_version,target_model_version,"
                "target_prompt_version FROM recompute_requests WHERE request_id=?",
                (recompute_request_id,),
            ).fetchone() if is_recompute_candidate else None

        eligible, qualification_state, qualification = transcript_qualification(
            row_dict, canonical_source_digest=canonical_source_digest, minimum=minimum,
        )
        expected_digest = str(job.get("transcript_content_digest") or "")
        transcript_path = Path(str(row_dict.get("transcript_path") or ""))
        actual_digest = file_sha256(transcript_path) if transcript_path.is_file() else ""
        immutable_reasons: list[str] = []
        if job.get("scope") != ANALYSIS_SCOPE_FORMAL:
            immutable_reasons.append("ANALYSIS_SCOPE_NOT_FORMAL")
        if job.get("analysis_spec_version") != ANALYSIS_SPEC_VERSION:
            immutable_reasons.append("ANALYSIS_SPEC_VERSION_MISMATCH")
        if job.get("model_version") != MODEL_VERSION:
            immutable_reasons.append("MODEL_VERSION_MISMATCH")
        if job.get("prompt_version") != PROMPT_VERSION:
            immutable_reasons.append("PROMPT_VERSION_MISMATCH")
        if not expected_digest or actual_digest != expected_digest:
            immutable_reasons.append("TRANSCRIPT_CONTENT_DIGEST_MISMATCH")
        if job.get("source_digest") != expected_digest:
            immutable_reasons.append("ANALYSIS_SOURCE_DIGEST_MISMATCH")
        expected_edge = [(
            expected_transcript_id, expected_digest, "CONTENT_DIGEST_VERIFIED",
            row_dict.get("transcript_engine"), row_dict.get("transcript_model"),
            MODEL_VERSION, PROMPT_VERSION, ANALYSIS_SPEC_VERSION,
        )]
        if edges != expected_edge:
            immutable_reasons.append("LINEAGE_NOT_EXACTLY_ONE_EXPECTED_TRANSCRIPT_DIGEST")
        if is_recompute_candidate and (
            not recompute_request
            or recompute_request["status"] != "CANDIDATE_CREATED"
            or recompute_request["candidate_analysis_id"] != job["analysis_id"]
            or recompute_request["upstream_id"] != expected_transcript_id
            or recompute_request["new_upstream_digest"] != expected_digest
            or recompute_request["target_analysis_spec_version"] != ANALYSIS_SPEC_VERSION
            or recompute_request["target_model_version"] != MODEL_VERSION
            or recompute_request["target_prompt_version"] != PROMPT_VERSION
        ):
            immutable_reasons.append("RECOMPUTE_REQUEST_IDENTITY_MISMATCH")
        if not eligible or immutable_reasons:
            qualification["reasons"] = [*qualification.get("reasons", []), *immutable_reasons]
            return _block_source_claim(
                job, analysis_metadata, qualification_state, qualification,
            )

        text, source_rows, source_duration, scope, transcript_quality, _payload = (
            prepare_analysis_source(
                transcript_path, expected_transcript_id,
                row_dict.get("transcript_metadata"), minimum, target,
            )
        )
        attempt_root = (
            ANALYSIS_ROOT / "checkpoints" / job["analysis_id"]
            / f"lease-{int(job['lease_epoch']):08d}.attempt-{int(job['attempts']):04d}"
        )

        def renew_claim(_attempt: int | None = None) -> None:
            with connect() as conn:
                renew(conn, ANALYSIS, job, lease_seconds=ANALYSIS_LEASE_SECONDS)
            upsert_heartbeat(
                "analysis-v3",
                "READY",
                {
                    "phase": "ANALYSIS_REQUEST",
                    "analysis_id": job["analysis_id"],
                    "session_id": job["session_id"],
                    "lease_epoch": int(job["lease_epoch"]),
                    "attempt": int(job["attempts"]),
                    "lease_until": job.get("lease_until"),
                    "checked_at": utc_now(),
                },
                success=True,
            )

        def persist_checkpoint(state: dict) -> None:
            with connect() as conn:
                save_checkpoint(conn, ANALYSIS, job, state)
            upsert_heartbeat(
                "analysis-v3",
                "READY",
                {
                    "phase": "ANALYZING_CHUNKS",
                    "analysis_id": job["analysis_id"],
                    "session_id": job["session_id"],
                    "lease_epoch": int(job["lease_epoch"]),
                    "attempt": int(job["attempts"]),
                    "completed_chunk_count": int(
                        state.get("completed_chunk_count") or 0
                    ),
                    "chunk_count": int(state.get("chunk_count") or 0),
                    "checked_at": utc_now(),
                },
                success=True,
            )

        result, engine = request_analysis(
            text,
            source_duration=source_duration,
            source_rows=source_rows,
            coverage_scope=scope,
            minimum=minimum,
            target=target,
            transcript_quality=transcript_quality,
            checkpoint=parse_checkpoint(job.get("checkpoint_json")),
            checkpoint_dir=attempt_root,
            on_checkpoint=persist_checkpoint,
            before_request=renew_claim,
        )
        coverage = result["analysis_coverage"]
        if not coverage.get("is_qualified"):
            return _quality_block_claim(
                job, job, coverage,
                reason="analysis coverage did not pass the formal 90% gate",
                analysis_metadata=analysis_metadata,
            )

        try:
            reference_manifest = source_reference_manifest(result)
            if reference_manifest["reference_count"] < 1:
                raise AnalysisRequestError(
                    "MODEL_EVIDENCE_EMPTY",
                    "analysis returned no source-bound evidence references",
                    retryable=True,
                    clear_checkpoint=True,
                )
            evidence = {
                **reference_manifest,
                "transcript_id": expected_transcript_id,
                "transcript_artifact_sha256": expected_digest,
                "source_content_hash": engine["source_content_hash"],
                "source_segment_set_digest": engine["source_segment_set_digest"],
                "model_generated_timestamps": False,
                "nearest_segment_fallback": False,
            }
        except AnalysisRequestError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise AnalysisRequestError(
                "MODEL_EVIDENCE_INVALID",
                str(exc),
                retryable=True,
                clear_checkpoint=True,
            ) from exc

        artifact = {
            "analysis_id": job["analysis_id"],
            "session_id": job["session_id"],
            "transcript_id": expected_transcript_id,
            "transcript_content_digest": expected_digest,
            "analysis_spec_version": ANALYSIS_SPEC_VERSION,
            "model_version": MODEL_VERSION,
            "prompt_version": PROMPT_VERSION,
            "qualification_state": QUALIFIED,
            "lease_epoch": int(job["lease_epoch"]),
            "attempt": int(job["attempts"]),
            "recompute_request_id": recompute_request_id or None,
            "engine": engine,
            "created_at": utc_now(),
            "evidence": evidence,
            "result": result,
        }
        if artifact_binding_status(artifact) != "BOUND_V1":
            raise AnalysisRequestError(
                "ARTIFACT_BINDING_SELF_CHECK_FAILED",
                "analysis artifact failed strict source binding self-check",
                retryable=False,
            )
        output_path = versioned_output_path(
            ANALYSIS_ROOT / f"{job['analysis_id']}.json", job
        )
        write_json_atomic(output_path, artifact)
        artifact_digest = file_sha256(output_path)
        gate_status = (
            "QUALIFIED_TARGET" if coverage.get("meets_target") else "QUALIFIED_MINIMUM"
        )
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
            "lease_epoch": int(job["lease_epoch"]),
            "attempt": int(job["attempts"]),
        }
        final_checkpoint = {
            **parse_checkpoint(job.get("checkpoint_json")),
            "schema_version": 2,
            "phase": "COMPLETE",
            "final_output_path": str(output_path),
            "final_output_sha256": artifact_digest,
        }
        with connect() as conn:
            finish(
                conn, ANALYSIS, job, "COMPLETE",
                {
                    "output_path": str(output_path),
                    "artifact_digest": artifact_digest,
                    "metadata_json": json.dumps(
                        metadata, ensure_ascii=False, sort_keys=True
                    ),
                    "lineage_state": (
                        "CANDIDATE" if is_recompute_candidate else "CURRENT"
                    ),
                    "qualification_status": QUALIFIED,
                    "checkpoint_json": json.dumps(
                        final_checkpoint, ensure_ascii=False, sort_keys=True
                    ),
                },
                commit_transaction=False,
            )
            superseded_rows = [] if is_recompute_candidate else conn.execute(
                "SELECT analysis_id,metadata_json FROM analyses WHERE transcript_id=? "
                "AND analysis_type='single_session' AND analysis_id<>? "
                "AND status='COMPLETE' AND lineage_state='CURRENT'",
                (expected_transcript_id, job["analysis_id"]),
            ).fetchall()
            for old in superseded_rows:
                old_metadata = {
                    **json_object(old["metadata_json"]),
                    "superseded_by_analysis_id": job["analysis_id"],
                    "superseded_at": utc_now(),
                }
                conn.execute(
                    "UPDATE analyses SET lineage_state='SUPERSEDED',metadata_json=? "
                    "WHERE analysis_id=? AND lineage_state='CURRENT'",
                    (
                        json.dumps(old_metadata, ensure_ascii=False, sort_keys=True),
                        old["analysis_id"],
                    ),
                )
                conn.execute(
                    "UPDATE lineage_edges SET state='SUPERSEDED' "
                    "WHERE downstream_type='analysis' AND downstream_id=? "
                    "AND upstream_type='transcript' AND state='CURRENT'",
                    (old["analysis_id"],),
                )
                outbox_id = enqueue_outbox_conn(
                    conn,
                    object_type="semantic_projection",
                    object_id=old["analysis_id"],
                    destination="feishu_base",
                    payload={
                        "analysis_id": old["analysis_id"],
                        "profile_id": "edu_live_competitor_intel",
                        "lineage_state": "SUPERSEDED",
                        "superseded_by_analysis_id": job["analysis_id"],
                        "release_after_evidence_analysis_id": job["analysis_id"],
                        "correction_version": 1,
                    },
                    scope=ANALYSIS_SCOPE_FORMAL,
                    qualification_status=QUALIFIED,
                )
                conn.execute(
                    "UPDATE outbox SET status='HELD_EVIDENCE' "
                    "WHERE outbox_id=? AND status='PENDING'",
                    (outbox_id,),
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
                    "bundle:" + job["analysis_id"], "analysis", job["analysis_id"],
                    "REQUIRED", str(output_path), artifact_digest,
                    ANALYSIS_SCOPE_FORMAL, QUALIFIED,
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
                        "lease_epoch": int(job["lease_epoch"]),
                        "recompute_request_id": recompute_request_id or None,
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            current_outbox = enqueue_outbox_conn(
                conn,
                object_type="semantic_projection",
                object_id=job["analysis_id"],
                destination="feishu_base",
                payload={
                    "analysis_id": job["analysis_id"],
                    "profile_id": "edu_live_competitor_intel",
                    "qualification_state": QUALIFIED,
                    "artifact_digest": artifact_digest,
                    "analysis_spec_version": ANALYSIS_SPEC_VERSION,
                    "release_after_evidence_analysis_id": job["analysis_id"],
                    "recompute_request_id": recompute_request_id or None,
                },
                scope=ANALYSIS_SCOPE_FORMAL,
                qualification_status=QUALIFIED,
            )
            conn.execute(
                "UPDATE outbox SET status='HELD_EVIDENCE' "
                "WHERE outbox_id=? AND status='PENDING'",
                (current_outbox,),
            )
            conn.commit()
        return "COMPLETE"
    except LeaseLostError:
        return "LEASE_LOST"
    except AnalysisSourceQualityError as exc:
        try:
            return _quality_block_claim(
                job, job, exc.coverage, reason=str(exc),
                analysis_metadata=analysis_metadata,
            )
        except LeaseLostError:
            return "LEASE_LOST"
    except AnalysisRequestError as exc:
        checkpoint = {} if exc.clear_checkpoint else parse_checkpoint(
            job.get("checkpoint_json")
        )
        return _release_failed_claim(
            job, error_type=exc.code, error_message=str(exc),
            retryable=exc.retryable,
            retry_after_seconds=exc.retry_after_seconds,
            checkpoint=checkpoint,
        )
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return _release_failed_claim(
            job, error_type="ANALYSIS_INPUT_INVALID",
            error_message=str(exc), retryable=False,
            checkpoint=parse_checkpoint(job.get("checkpoint_json")),
        )
    except Exception as exc:
        return _release_failed_claim(
            job, error_type="ANALYSIS_INTERNAL_ERROR",
            error_message=f"{exc.__class__.__name__}: {str(exc)}",
            retryable=True,
            checkpoint=parse_checkpoint(job.get("checkpoint_json")),
        )


def once(*, max_jobs: int = 1) -> dict:
    minimum, target = load_analysis_quality_config()
    init_db()
    with connect() as conn:
        exhausted = reconcile_exhausted(conn, ANALYSIS)
    outcomes = {
        "claimed": 0,
        "completed": 0,
        "retry_wait": 0,
        "failed_final": 0,
        "lease_lost": 0,
        "quality_blocked": 0,
        "source_qualification_blocked": 0,
        "revalidated": 0,
    }
    for _ in range(max(1, int(max_jobs))):
        with connect() as conn:
            job = claim_next(
                conn,
                ANALYSIS,
                worker_id(),
                lease_seconds=ANALYSIS_LEASE_SECONDS,
                where_sql=(
                    "analysis_type=? AND analysis_spec_version=? "
                    "AND model_version=? AND prompt_version=?"
                ),
                where_params=(
                    "single_session", ANALYSIS_SPEC_VERSION,
                    MODEL_VERSION, PROMPT_VERSION,
                ),
            )
        if not job:
            break
        outcomes["claimed"] += 1
        outcome = process_claim(job)
        outcomes["completed"] += int(outcome == "COMPLETE")
        outcomes["retry_wait"] += int(outcome == "RETRY_WAIT")
        outcomes["failed_final"] += int(outcome == "FAILED_FINAL")
        outcomes["lease_lost"] += int(outcome == "LEASE_LOST")
        outcomes["quality_blocked"] += int(outcome == "QUALITY_BLOCKED")
        outcomes["source_qualification_blocked"] += int(outcome == "SOURCE_BLOCKED")

    health_status, health = analysis_health_snapshot(minimum, target)
    result = {
        **outcomes,
        "expired_exhausted": exhausted,
        "minimum_coverage_rate": minimum,
        "target_coverage_rate": target,
        "analysis_spec_version": ANALYSIS_SPEC_VERSION,
        **health,
        "checked_at": utc_now(),
    }
    healthy = (
        outcomes["retry_wait"] == 0
        and outcomes["failed_final"] == 0
        and outcomes["lease_lost"] == 0
        and outcomes["quality_blocked"] == 0
        and outcomes["source_qualification_blocked"] == 0
        and exhausted == 0
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
