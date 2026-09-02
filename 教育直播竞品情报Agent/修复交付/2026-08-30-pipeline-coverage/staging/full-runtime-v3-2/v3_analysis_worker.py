#!/usr/bin/env python3
"""Evidence-bound semantic analysis worker for completed transcripts."""

from __future__ import annotations

import argparse
import hashlib
import json
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


def request_analysis(text: str, source_duration: float | None = None) -> tuple[dict, dict]:
    chunks = analysis_chunks(text)
    results, diagnostics = [], []
    for chunk in chunks:
        result, diagnostic = request_chunk(chunk)
        results.append(result)
        diagnostics.append({"chunk_index": chunk["index"], "start": chunk["start"], "end": chunk["end"],
                            "row_count": chunk["row_count"], **diagnostic})
    merged = merge_chunk_results(results, chunks, source_duration)
    return merged, {"provider": "deepseek", "model": "deepseek-chat", "response_ids": [d["response_id"] for d in diagnostics],
                    "source_content_hash": hashlib.sha256(text.encode()).hexdigest(), "chunk_count": len(chunks),
                    "chunk_diagnostics": diagnostics, "merge_engine": "deterministic-python-v2"}


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
            result, engine = request_analysis(text, source_duration=float(source_duration or 0))
            output_path = ANALYSIS_ROOT / f"{row['analysis_id']}.json"
            artifact = {"analysis_id": row["analysis_id"], "session_id": row["session_id"], "transcript_id": row["transcript_id"], "engine": engine, "created_at": utc_now(), "result": result}
            write_json_atomic(output_path, artifact)
            with connect() as conn:
                conn.execute("UPDATE analyses SET status='COMPLETE',output_path=?,metadata_json=?,lineage_state='CURRENT' WHERE analysis_id=?", (str(output_path), json.dumps({**engine, "semantic_engine": "deepseek-chat", "transcript_segment_count": len(transcript_rows)}, ensure_ascii=False, sort_keys=True), row["analysis_id"]))
                conn.execute("INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,metadata_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id) DO UPDATE SET status=excluded.status,manifest_path=excluded.manifest_path,manifest_hash=excluded.manifest_hash,metadata_json=excluded.metadata_json", ("bundle:" + row["analysis_id"], "analysis", row["analysis_id"], "REQUIRED", str(output_path), hashlib.sha256(output_path.read_bytes()).hexdigest(), json.dumps({"transcript_id": row["transcript_id"]}, ensure_ascii=False)))
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
