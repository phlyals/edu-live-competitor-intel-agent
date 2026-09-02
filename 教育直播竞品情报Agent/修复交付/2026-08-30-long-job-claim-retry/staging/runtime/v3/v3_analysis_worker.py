#!/usr/bin/env python3
"""Evidence-bound semantic analysis worker for completed transcripts."""

from __future__ import annotations

import argparse
import hashlib
import json
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
    complete,
    fail_or_retry,
    finite_retry_after,
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
FULL_SESSION_MIN_COVERAGE_RATE = 0.90
CHUNK_FIELDS = (
    "instructor", "course_content", "interaction_patterns", "product_handoff",
    "hook", "pain_points", "claims", "cta", "risks", "evidence_refs",
)


def stop(*_args):
    global RUNNING
    RUNNING = False


class AnalysisRequestError(RuntimeError):
    """A redacted provider failure with an explicit retry policy."""

    def __init__(self, code: str, message: str, *, retryable: bool, retry_after_seconds: float | None = None):
        super().__init__(message)
        self.code = code
        self.retryable = bool(retryable)
        self.retry_after_seconds = retry_after_seconds


def worker_id() -> str:
    return f"analysis-v3:{socket.gethostname()}:{os.getpid()}"


def _retry_after(response: httpx.Response) -> float | None:
    return finite_retry_after(getattr(response, "headers", {}).get("retry-after"))


def _http_status_error(response: httpx.Response) -> AnalysisRequestError | None:
    status = int(getattr(response, "status_code", 200))
    if 200 <= status < 300:
        return None
    if status in {401, 403}:
        return AnalysisRequestError(
            f"HTTP_{status}_AUTH", f"analysis provider rejected credentials ({status})", retryable=False
        )
    if status == 429:
        return AnalysisRequestError(
            "HTTP_429_RATE_LIMIT", "analysis provider rate limited the request",
            retryable=True, retry_after_seconds=_retry_after(response),
        )
    if status == 408:
        return AnalysisRequestError(
            "HTTP_408_TIMEOUT", "analysis provider reported a request timeout", retryable=True
        )
    if 500 <= status <= 599:
        return AnalysisRequestError(
            f"HTTP_{status}_SERVER", f"analysis provider server error ({status})", retryable=True
        )
    return AnalysisRequestError(
        f"HTTP_{status}_REQUEST", f"analysis provider rejected the request ({status})", retryable=False
    )


def _request_exception(exc: Exception) -> AnalysisRequestError:
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError)):
        return AnalysisRequestError("HTTP_CONNECT", "analysis provider connection failed", retryable=True)
    if isinstance(exc, httpx.TimeoutException):
        return AnalysisRequestError("HTTP_TIMEOUT", "analysis provider request timed out", retryable=True)
    if isinstance(exc, httpx.RequestError):
        return AnalysisRequestError("HTTP_NETWORK", "analysis provider network request failed", retryable=True)
    return AnalysisRequestError("HTTP_CLIENT", exc.__class__.__name__, retryable=False)


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
        rows.append({"segment_index": len(rows), "start": start, "end": end,
                     "text": match.group(3), "line": line.strip()})
    return rows


def analysis_chunks(text: str, char_limit: int = ANALYSIS_CHUNK_CHAR_LIMIT) -> list[dict]:
    """Split only between timestamped rows; every row belongs to one chunk."""
    rows = timed_lines(text)
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
                           "rows": current})
            current, current_chars = [], 0
            line_chars = len(row["line"])
        current.append(row)
        current_chars += line_chars
    if current:
        chunks.append({"index": len(chunks), "start": current[0]["start"], "end": current[-1]["end"],
                       "text": "\n".join(item["line"] for item in current), "row_count": len(current),
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
            "CONFIG_MISSING_API_KEY", "DEEPSEEK_API_KEY is not configured", retryable=False
        )
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
    last_error: AnalysisRequestError | None = None
    for attempt in range(1, ANALYSIS_MAX_RETRIES + 1):
        if before_attempt:
            before_attempt(attempt)
        try:
            try:
                with client_factory(timeout=180) as client:
                    response = client.post(
                        "https://api.deepseek.com/chat/completions",
                        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
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
                    "HTTP_RESPONSE_JSON_INVALID", "analysis provider response envelope is not JSON", retryable=True
                ) from exc
            if not isinstance(payload, dict) or not isinstance(payload.get("choices"), list) or not payload["choices"]:
                raise AnalysisRequestError(
                    "HTTP_RESPONSE_SCHEMA_INVALID", "analysis provider response has no choice", retryable=True
                )
            choice = payload["choices"][0]
            finish_reason = choice.get("finish_reason")
            content = ((choice.get("message") or {}).get("content") or "").strip()
            if finish_reason != "stop":
                retryable = finish_reason not in {"content_filter", "safety"}
                raise AnalysisRequestError(
                    "MODEL_FINISH_INCOMPLETE" if retryable else "MODEL_FINISH_BLOCKED",
                    f"analysis completion finish_reason={finish_reason!r}", retryable=retryable,
                )
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError as exc:
                raise AnalysisRequestError(
                    "MODEL_JSON_INVALID", "analysis model output is not complete JSON", retryable=True
                ) from exc
            try:
                result = validate_chunk_result(parsed, chunk)
            except ValueError as exc:
                raise AnalysisRequestError("MODEL_SCHEMA_INVALID", str(exc), retryable=True) from exc
            return result, {"response_id": payload.get("id"), "finish_reason": finish_reason,
                            "usage": payload.get("usage"), "attempt": attempt,
                            "content_hash": hashlib.sha256(content.encode()).hexdigest()}
        except AnalysisRequestError as exc:
            last_error = exc
            failures.append(f"attempt {attempt}: {exc.code}")
            if not exc.retryable:
                raise
            if attempt >= ANALYSIS_MAX_RETRIES:
                break
            # The correction contains no provider body or credential.  Only a
            # bounded model excerpt is included for JSON/schema repair.
            if exc.code.startswith("MODEL_"):
                body["messages"].append({"role": "assistant", "content": content[-1000:]})
                body["messages"].append({"role": "user", "content":
                    "上次输出无效或被截断。重新从原时间片生成更短的完整JSON；每字段最多1项，不能省略字段。"})
            sleep(min(30.0, max(float(2 ** (attempt - 1)), float(exc.retry_after_seconds or 0))))
    assert last_error is not None
    raise AnalysisRequestError(
        last_error.code,
        "analysis chunk failed after retries (bounded): " + " | ".join(failures),
        retryable=last_error.retryable,
        retry_after_seconds=last_error.retry_after_seconds,
    )


def merge_chunk_results(results: list[dict], chunks: list[dict], source_duration: float | None) -> dict:
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
    coverage = max(0.0, chunks[-1]["end"] - chunks[0]["start"])
    module_sources = (("开场", "hook"), ("干货", "course_content"), ("需求", "pain_points"),
                      ("信任", "claims"), ("商品承接", "product_handoff"), ("成交", "cta"),
                      ("答疑", "interaction_patterns"))
    modules = []
    source_rows = [row for chunk in chunks for row in chunk["rows"]]
    for name, field in module_sources:
        values = merged[field]
        timestamps = []
        for item in values:
            candidates = [row for row in source_rows if row["end"] > item["start"] and row["start"] < item["end"]]
            if not candidates:
                candidates = source_rows
            row = min(candidates, key=lambda candidate: abs(candidate["start"] - item["start"]))
            timestamps.append({"start": row["start"], "end": row["end"],
                               "source_segment_index": row["segment_index"], "source_text": row["text"]})
        modules.append({"name": name, "summary": "；".join(item["summary"] for item in values[:5]),
                        "timestamps": timestamps})
    merged.update({
        "schema_version": "2.0",
        "instructor": {"status": instructor_status,
                       "names": instructor_names, "evidence_refs": instructor_items},
        "interaction": list(merged["interaction_patterns"]),
        "product_fulfillment": list(merged["product_handoff"]),
        "modules": modules,
        "analysis_coverage": {"source_duration_seconds": duration,
                              "first_timestamp_seconds": chunks[0]["start"],
                              "last_timestamp_seconds": chunks[-1]["end"],
                              "timeline_coverage_rate": coverage / duration if duration > 0 else None,
                              "chunk_count": len(chunks),
                              "input_row_count": sum(chunk["row_count"] for chunk in chunks),
                              "analyzed_unique_segment_count": len({row["segment_index"] for row in source_rows}),
                              "segment_coverage_rate": 1.0,
                              "all_chunks_complete": True},
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


def json_object(value: object) -> dict:
    try:
        parsed = json.loads(value or "{}") if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def transcript_qualification(row: dict, *, canonical_source_digest: str | None) -> tuple[bool, str, dict]:
    """Return formal-analysis eligibility for one exact transcript row.

    This is deliberately repeated at the consumer boundary.  Pipeline task
    creation is not sufficient authorization: a stale/manual analysis row must
    still be unable to make a SAMPLE transcript reach the model or outbox.
    """
    metadata = json_object(row.get("transcript_metadata"))
    session_metadata = json_object(row.get("session_metadata"))
    quality = metadata.get("timestamp_coverage") or {}
    try:
        rate = float(quality.get("coverage_rate"))
    except (TypeError, ValueError):
        rate = -1.0
    is_sample = metadata.get("sample_only") is True or metadata.get("coverage_scope") == "SAMPLE"
    reasons = []
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
    if metadata.get("quality_gate_status") != "FULL_SESSION_QUALIFIED":
        reasons.append("QUALITY_GATE_NOT_QUALIFIED")
    if quality.get("is_qualified") is not True or quality.get("timestamps_valid") is not True:
        reasons.append("TIMESTAMPS_NOT_QUALIFIED")
    if rate < FULL_SESSION_MIN_COVERAGE_RATE:
        reasons.append("TIMESTAMP_COVERAGE_BELOW_MINIMUM")
    if row.get("session_status") != "MEDIA_COMPLETE" or row.get("completeness") != "COMPLETE":
        reasons.append("SESSION_NOT_MEDIA_COMPLETE")
    if (session_metadata.get("media_coverage") or {}).get("continuous_capture") is not True:
        reasons.append("SESSION_CAPTURE_NOT_CONTINUOUS")
    if not canonical_source_digest:
        reasons.append("SOURCE_SEGMENT_NOT_CURRENT_CANONICAL")
    elif str(row.get("transcript_source_digest") or "") != canonical_source_digest:
        reasons.append("TRANSCRIPT_SOURCE_DIGEST_NOT_CURRENT_CANONICAL")
    path = Path(str(row.get("transcript_path") or ""))
    if not path.is_file():
        reasons.append("TRANSCRIPT_ARTIFACT_MISSING")
    details = {
        "coverage_scope": metadata.get("coverage_scope"),
        "sample_only": metadata.get("sample_only"),
        "coverage_rate": rate if rate >= 0 else None,
        "source_segment_id": metadata.get("source_segment_id"),
        "expected_canonical_source_digest": canonical_source_digest,
        "transcript_source_digest": row.get("transcript_source_digest"),
        "reasons": reasons,
    }
    return not reasons, SAMPLE_NONQUALIFYING if is_sample else (QUALIFIED if not reasons else "SOURCE_NONQUALIFYING"), details


def request_analysis(
    text: str,
    source_duration: float | None = None,
    *,
    checkpoint: dict | None = None,
    checkpoint_dir: Path | None = None,
    on_checkpoint=None,
    before_request=None,
    request_fn=None,
    char_limit: int = ANALYSIS_CHUNK_CHAR_LIMIT,
) -> tuple[dict, dict]:
    request_fn = request_fn or request_chunk
    chunks = analysis_chunks(text, char_limit=char_limit)
    source_hash = hashlib.sha256(text.encode()).hexdigest()
    state = checkpoint if isinstance(checkpoint, dict) and checkpoint.get("source_content_hash") == source_hash else {}
    completed_chunks = state.get("chunks") if isinstance(state.get("chunks"), dict) else {}
    results, diagnostics = [], []
    for chunk in chunks:
        saved = completed_chunks.get(str(chunk["index"]))
        result = diagnostic = None
        if isinstance(saved, dict):
            try:
                result = validate_chunk_result(saved.get("result"), chunk)
                diagnostic = saved.get("diagnostic") if isinstance(saved.get("diagnostic"), dict) else {}
                if checkpoint_dir:
                    checkpoint_path = Path(str(saved.get("path") or ""))
                    expected_hash = str(saved.get("sha256") or "")
                    if not checkpoint_path.is_file() or hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() != expected_hash:
                        result = diagnostic = None
            except (OSError, ValueError):
                result = diagnostic = None
        if result is None:
            result, diagnostic = request_fn(chunk, before_attempt=before_request)
            record = {"result": result, "diagnostic": diagnostic}
            if checkpoint_dir:
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                checkpoint_path = checkpoint_dir / f"chunk-{chunk['index']:05d}.json"
                write_json_atomic(checkpoint_path, record)
                record.update(path=str(checkpoint_path), sha256=hashlib.sha256(checkpoint_path.read_bytes()).hexdigest())
            completed_chunks[str(chunk["index"])] = record
            state = {
                "schema_version": 1,
                "phase": "ANALYZING_CHUNKS",
                "source_content_hash": source_hash,
                "chunk_count": len(chunks),
                "completed_chunk_count": len(completed_chunks),
                "chunks": completed_chunks,
            }
            if on_checkpoint:
                on_checkpoint(state)
        results.append(result)
        diagnostics.append({"chunk_index": chunk["index"], "start": chunk["start"], "end": chunk["end"],
                            "row_count": chunk["row_count"], **diagnostic})
    merged = merge_chunk_results(results, chunks, source_duration)
    return merged, {"provider": "deepseek", "model": "deepseek-chat", "response_ids": [d["response_id"] for d in diagnostics],
                    "source_content_hash": source_hash, "chunk_count": len(chunks),
                    "chunk_diagnostics": diagnostics, "merge_engine": "deterministic-python-v2"}


def process_claim(job: dict) -> str:
    """Process one fenced analysis claim and return its durable outcome."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT a.*,t.output_path AS transcript_path,t.transcript_id "
                "FROM analyses a JOIN lineage_edges l ON l.downstream_type='analysis' "
                "AND l.downstream_id=a.analysis_id AND l.upstream_type='transcript' AND l.state='CURRENT' "
                "JOIN transcripts t ON t.transcript_id=l.upstream_id "
                "WHERE a.analysis_id=? AND t.status='COMPLETE'",
                (job["analysis_id"],),
            ).fetchone()
        if not row or not row["transcript_path"] or not Path(row["transcript_path"]).is_file():
            raise AnalysisRequestError(
                "TRANSCRIPT_ARTIFACT_MISSING", "claimed analysis has no current complete transcript artifact",
                retryable=False,
            )
        transcript_path = Path(row["transcript_path"])
        text, transcript_rows = transcript_text(transcript_path)
        transcript_payload = json.loads(transcript_path.read_text(encoding="utf-8"))
        source_duration = transcript_payload.get("duration") or max(
            (float(item.get("end") or 0) for item in transcript_rows), default=0
        )
        checkpoint = parse_checkpoint(job.get("checkpoint_json"))
        attempt_root = (
            ANALYSIS_ROOT / "checkpoints" / job["analysis_id"] /
            f"lease-{int(job['lease_epoch']):08d}.attempt-{int(job['attempts']):04d}"
        )

        def renew_claim(_attempt: int | None = None) -> None:
            with connect() as conn:
                renew(conn, ANALYSIS, job, lease_seconds=ANALYSIS_LEASE_SECONDS)

        def persist(state: dict) -> None:
            with connect() as conn:
                save_checkpoint(conn, ANALYSIS, job, state)

        result, engine = request_analysis(
            text,
            source_duration=float(source_duration or 0),
            checkpoint=checkpoint,
            checkpoint_dir=attempt_root,
            on_checkpoint=persist,
            before_request=renew_claim,
        )
        renew_claim()
        output_path = versioned_output_path(ANALYSIS_ROOT / f"{job['analysis_id']}.json", job)
        artifact = {
            "analysis_id": job["analysis_id"], "session_id": row["session_id"],
            "transcript_id": row["transcript_id"], "lease_epoch": int(job["lease_epoch"]),
            "attempt": int(job["attempts"]), "engine": engine, "created_at": utc_now(), "result": result,
        }
        write_json_atomic(output_path, artifact)
        artifact_hash = hashlib.sha256(output_path.read_bytes()).hexdigest()
        final_checkpoint = {
            **parse_checkpoint(job.get("checkpoint_json")),
            "schema_version": 1,
            "phase": "COMPLETE",
            "source_content_hash": engine["source_content_hash"],
            "final_output_path": str(output_path),
            "final_output_sha256": artifact_hash,
        }
        metadata = json.dumps(
            {**engine, "semantic_engine": "deepseek-chat", "transcript_segment_count": len(transcript_rows)},
            ensure_ascii=False, sort_keys=True,
        )
        with connect() as conn:
            complete(
                conn, ANALYSIS, job,
                {"output_path": str(output_path), "metadata_json": metadata, "lineage_state": "CURRENT",
                 "checkpoint_json": json.dumps(final_checkpoint, ensure_ascii=False, sort_keys=True)},
                commit_transaction=False,
            )
            conn.execute(
                "INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,metadata_json) "
                "VALUES(?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id) DO UPDATE SET "
                "status=excluded.status,manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,metadata_json=excluded.metadata_json",
                ("bundle:" + job["analysis_id"], "analysis", job["analysis_id"], "REQUIRED", str(output_path),
                 artifact_hash, json.dumps({"transcript_id": row["transcript_id"], "lease_epoch": job["lease_epoch"]}, ensure_ascii=False)),
            )
            enqueue_outbox_conn(
                conn, object_type="semantic_projection", object_id=job["analysis_id"],
                destination="feishu_base",
                payload={"analysis_id": job["analysis_id"], "profile_id": "edu_live_competitor_intel"},
            )
            conn.commit()
        return "COMPLETE"
    except LeaseLostError:
        # A newer lease owns all publication rights.  The versioned local
        # artifact, if any, is intentionally unreferenced and safe to inspect.
        return "LEASE_LOST"
    except AnalysisRequestError as exc:
        with connect() as conn:
            return fail_or_retry(
                conn, ANALYSIS, job, error_type=exc.code, error_message=str(exc),
                retryable=exc.retryable, retry_after_seconds=exc.retry_after_seconds,
                checkpoint=parse_checkpoint(job.get("checkpoint_json")),
            )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        with connect() as conn:
            return fail_or_retry(
                conn, ANALYSIS, job, error_type="ANALYSIS_INPUT_INVALID", error_message=str(exc),
                retryable=False, checkpoint=parse_checkpoint(job.get("checkpoint_json")),
            )
    except Exception as exc:  # unknown failures are bounded and fail closed
        with connect() as conn:
            return fail_or_retry(
                conn, ANALYSIS, job, error_type="ANALYSIS_INTERNAL_ERROR",
                error_message=f"{exc.__class__.__name__}: {str(exc)}", retryable=True,
                checkpoint=parse_checkpoint(job.get("checkpoint_json")),
            )


def once(*, max_jobs: int = 100) -> dict:
    completed = retry_wait = failed_final = lease_lost = claimed = 0
    init_db()
    with connect() as conn:
        exhausted = reconcile_exhausted(conn, ANALYSIS)
    for _ in range(max(1, int(max_jobs))):
        with connect() as conn:
            job = claim_next(conn, ANALYSIS, worker_id(), lease_seconds=ANALYSIS_LEASE_SECONDS)
        if not job:
            break
        claimed += 1
        outcome = process_claim(job)
        completed += int(outcome == "COMPLETE")
        retry_wait += int(outcome == "RETRY_WAIT")
        failed_final += int(outcome == "FAILED_FINAL")
        lease_lost += int(outcome == "LEASE_LOST")
    with connect() as conn:
        durable_backlog = {
            row["status"]: int(row["n"])
            for row in conn.execute(
                "SELECT status,count(*) AS n FROM analyses "
                "WHERE status IN ('PENDING','WAITING_MODEL','RETRY_WAIT','RUNNING','FAILED_FINAL') GROUP BY status"
            )
        }
    result = {
        "claimed": claimed, "completed": completed, "retry_wait": retry_wait,
        "failed_final": failed_final, "lease_lost": lease_lost, "checked_at": utc_now(),
        "expired_exhausted": exhausted,
        "durable_backlog": durable_backlog,
    }
    healthy = (
        retry_wait == 0 and failed_final == 0 and lease_lost == 0 and exhausted == 0
        and not durable_backlog
    )
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
