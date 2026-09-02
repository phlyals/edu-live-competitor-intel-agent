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
CHUNK_FIELDS = (
    "instructor", "course_content", "interaction_patterns", "product_handoff",
    "hook", "pain_points", "claims", "cta", "risks", "evidence_refs",
)


def stop(*_args):
    global RUNNING
    RUNNING = False


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
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def bind_transcript_segments(rows: object) -> list[dict]:
    """Create chunk-independent, content-bound IDs for original ASR segments.

    The ID deliberately excludes ``normalized_text``.  Normalisation is a view
    used for model comprehension; the evidence identity is tied to the original
    text, absolute row index, and original timestamps.
    """
    if not isinstance(rows, list):
        raise ValueError("transcript segments must be an array")
    bound: list[dict] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise ValueError(f"transcript segment {index} is not an object")
        try:
            start, end = float(raw["start"]), float(raw["end"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"transcript segment {index} lacks numeric start/end") from None
        if not math.isfinite(start) or not math.isfinite(end) or start < 0 or end <= start:
            raise ValueError(f"transcript segment {index} has invalid start/end")
        source_text = str(raw.get("text") or "")
        analysis_text = re.sub(r"\s+", " ", str(raw.get("normalized_text") or source_text)).strip()
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
            "line": f"[{source_segment_id}] {analysis_text}",
        })
    return bound


def transcript_text(path: Path, limit: int | None = None) -> tuple[str, list[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("transcript artifact must be a JSON object")
    rows = bind_transcript_segments(payload.get("segments"))
    # Feed the losslessly normalised view to the semantic model while original
    # text, timestamps, and digest remain server-side evidence.  Timestamps are
    # intentionally absent from model input: the model can only cite IDs.
    text = "\n".join(row["line"] for row in rows if row["analysis_text"])
    return (text[:limit] if limit is not None else text), rows


def timed_lines(text: str) -> list[dict]:
    """Parse legacy timestamped test/input text, then bind it like a transcript.

    This parser is an input compatibility shim only.  It never accepts model
    output timestamps and is not used by the production transcript path.
    """
    raw_rows = []
    pattern = re.compile(r"^\[([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)\]\s*(.*)$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        start, end = float(match.group(1)), float(match.group(2))
        if start < 0 or end < start:
            continue
        raw_rows.append({"start": start, "end": end, "text": match.group(3)})
    return bind_transcript_segments(raw_rows)


def analysis_chunks(text: str, char_limit: int = ANALYSIS_CHUNK_CHAR_LIMIT,
                    source_rows: list[dict] | None = None) -> list[dict]:
    """Split only between timestamped rows; every row belongs to one chunk."""
    rows = list(source_rows) if source_rows is not None else timed_lines(text)
    rows = [row for row in rows if str(row.get("analysis_text") or "").strip()]
    if not rows:
        raise RuntimeError("analysis input has no timestamped transcript rows")
    if char_limit < 1:
        raise ValueError("analysis chunk character limit must be positive")
    ids = [str(row.get("source_segment_id") or "") for row in rows]
    if not all(ids) or len(set(ids)) != len(ids):
        raise ValueError("analysis input has missing or duplicate source segment ids")
    chunks = []
    current: list[dict] = []
    current_chars = 0
    for row in rows:
        line_chars = len(row["line"]) + (1 if current else 0)
        if current and current_chars + line_chars > char_limit:
            chunks.append({"index": len(chunks), "start": current[0]["start"], "end": current[-1]["end"],
                           "text": "\n".join(item["line"] for item in current), "row_count": len(current),
                           "rows": current,
                           "source_segment_ids": [item["source_segment_id"] for item in current]})
            current, current_chars = [], 0
            line_chars = len(row["line"])
        current.append(row)
        current_chars += line_chars
    if current:
        chunks.append({"index": len(chunks), "start": current[0]["start"], "end": current[-1]["end"],
                       "text": "\n".join(item["line"] for item in current), "row_count": len(current),
                       "rows": current,
                       "source_segment_ids": [item["source_segment_id"] for item in current]})
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
            if (not isinstance(source_ids, list) or not 1 <= len(source_ids) <= 3
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
        "source_segment_ids必须是包含1到3个本时间片原始ID的数组，不得改写或虚构ID。"
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
                "上次输出无效或被截断。重新生成更短的完整JSON；每字段最多1项，不能省略字段。"
                "只能引用输入中的source_segment_id；不要输出start/end或其他字段。"})
    raise RuntimeError("analysis chunk failed after retries: " + " | ".join(failures))


def merge_chunk_results(results: list[dict], chunks: list[dict], source_duration: float | None) -> dict:
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
    coverage = max(0.0, chunks[-1]["end"] - chunks[0]["start"])
    module_sources = (("开场", "hook"), ("干货", "course_content"), ("需求", "pain_points"),
                      ("信任", "claims"), ("商品承接", "product_handoff"), ("成交", "cta"),
                      ("答疑", "interaction_patterns"))
    modules = []
    source_rows = [row for chunk in chunks for row in chunk["rows"]]
    for name, field in module_sources:
        values = merged[field]
        timestamps = []
        seen_module_refs: set[str] = set()
        for item in values:
            for ref in item["source_segments"]:
                source_id = ref["source_segment_id"]
                if source_id in seen_module_refs:
                    continue
                seen_module_refs.add(source_id)
                timestamps.append(dict(ref))
        modules.append({"name": name, "summary": "；".join(item["summary"] for item in values[:5]),
                        "timestamps": timestamps})
    all_source_ids = [row["source_segment_id"] for row in source_rows]
    if len(all_source_ids) != len(set(all_source_ids)):
        raise ValueError("analysis chunks contain duplicate source segment ids")
    merged.update({
        "schema_version": "3.0",
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


def request_analysis(text: str, source_duration: float | None = None,
                     source_rows: list[dict] | None = None) -> tuple[dict, dict]:
    chunks = analysis_chunks(text, source_rows=source_rows)
    results, diagnostics = [], []
    for chunk in chunks:
        result, diagnostic = request_chunk(chunk)
        results.append(result)
        diagnostics.append({"chunk_index": chunk["index"], "start": chunk["start"], "end": chunk["end"],
                            "row_count": chunk["row_count"], **diagnostic})
    merged = merge_chunk_results(results, chunks, source_duration)
    return merged, {"provider": "deepseek", "model": "deepseek-chat", "response_ids": [d["response_id"] for d in diagnostics],
                    "source_content_hash": hashlib.sha256(text.encode()).hexdigest(), "chunk_count": len(chunks),
                    "source_segment_set_digest": _json_digest([
                        {"source_segment_id": row["source_segment_id"], "content_digest": row["content_digest"]}
                        for chunk in chunks for row in chunk["rows"]
                    ]),
                    "chunk_diagnostics": diagnostics, "merge_engine": "deterministic-python-v3-source-ids"}


def once() -> dict:
    completed = failed = 0
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT a.*,t.output_path AS transcript_path,t.transcript_id FROM analyses a JOIN transcripts t ON t.session_id=a.session_id WHERE a.status IN ('PENDING','WAITING_MODEL') AND t.status='COMPLETE' ORDER BY a.analysis_id").fetchall()
    for row in rows:
        try:
            text, transcript_rows = transcript_text(Path(row["transcript_path"]))
            transcript_payload = json.loads(Path(row["transcript_path"]).read_text(encoding="utf-8"))
            source_duration = transcript_payload.get("duration") or max(
                (float(item.get("end") or 0) for item in transcript_rows), default=0
            )
            result, engine = request_analysis(
                text, source_duration=float(source_duration or 0), source_rows=transcript_rows
            )
            output_path = ANALYSIS_ROOT / f"{row['analysis_id']}.json"
            transcript_path = Path(row["transcript_path"])
            reference_manifest = source_reference_manifest(result)
            evidence = {
                **reference_manifest,
                "transcript_id": row["transcript_id"],
                "transcript_artifact_sha256": hashlib.sha256(transcript_path.read_bytes()).hexdigest(),
                "source_content_hash": engine["source_content_hash"],
                "source_segment_set_digest": engine["source_segment_set_digest"],
            }
            artifact = {"analysis_id": row["analysis_id"], "session_id": row["session_id"], "transcript_id": row["transcript_id"], "engine": engine, "created_at": utc_now(), "evidence": evidence, "result": result}
            write_json_atomic(output_path, artifact)
            with connect() as conn:
                conn.execute("UPDATE analyses SET status='COMPLETE',output_path=?,metadata_json=?,lineage_state='CURRENT' WHERE analysis_id=?", (str(output_path), json.dumps({**engine, "semantic_engine": "deepseek-chat", "transcript_segment_count": len(transcript_rows), "evidence_binding_status": "BOUND_V1", "source_binding_digest": evidence["source_binding_digest"], "referenced_source_segment_count": evidence["reference_count"], "transcript_artifact_sha256": evidence["transcript_artifact_sha256"]}, ensure_ascii=False, sort_keys=True), row["analysis_id"]))
                conn.execute("INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,metadata_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id) DO UPDATE SET status=excluded.status,manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,metadata_json=excluded.metadata_json", ("bundle:" + row["analysis_id"], "analysis", row["analysis_id"], "REQUIRED", str(output_path), hashlib.sha256(output_path.read_bytes()).hexdigest(), json.dumps({"transcript_id": row["transcript_id"], "evidence_binding_status": "BOUND_V1", "source_binding_digest": evidence["source_binding_digest"], "referenced_source_segments": [{"source_segment_id": ref["source_segment_id"], "content_digest": ref["content_digest"]} for ref in evidence["references"]]}, ensure_ascii=False, sort_keys=True)))
                enqueue_outbox_conn(conn, object_type="semantic_projection", object_id=row["analysis_id"], destination="feishu_base", payload={"analysis_id": row["analysis_id"], "profile_id": "edu_live_competitor_intel"})
                conn.commit()
            completed += 1
        except Exception as exc:  # fail closed; no fabricated analysis
            with connect() as conn:
                conn.execute("UPDATE analyses SET status='WAITING_MODEL',metadata_json=? WHERE analysis_id=?", (json.dumps({"error_type": exc.__class__.__name__, "error_message": str(exc)[:500], "checked_at": utc_now()}, ensure_ascii=False), row["analysis_id"]))
                conn.commit()
            failed += 1
    result = {"completed": completed, "waiting_model": failed, "checked_at": utc_now()}
    upsert_heartbeat("analysis-v3", "READY" if failed == 0 else "DEGRADED", result, success=failed == 0)
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
