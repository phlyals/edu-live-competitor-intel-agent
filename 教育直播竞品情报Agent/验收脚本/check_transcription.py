#!/usr/bin/env python3
"""Timestamp union / complement on one explicitly selected audio timeline."""
from acceptance_common import (CheckError, fail, interval, load_artifact, local_path,
                               metadata, probe, run_cli, segment)


def defaults():
    return dict(audio_duration_seconds=None, transcript_total_chars=0, covered_segments=[],
                gaps=[], coverage_rate=None, status="UNCOMPUTABLE", message="无法计算覆盖率")


def calculate(payload, duration):
    result = defaults()
    result["audio_duration_seconds"] = duration
    rows = payload.get("segments")
    if not isinstance(rows, list):
        result["transcript_total_chars"] = len(payload.get("text", "")) if isinstance(payload.get("text"), str) else 0
        result["message"] = "无法计算覆盖率：segments 缺失或格式异常"
        return result
    # Count Unicode code points in original segment text; never double count top-level text.
    result["transcript_total_chars"] = sum(len(r.get("text", "")) for r in rows
                                          if isinstance(r, dict) and isinstance(r.get("text", ""), str))
    if not rows and payload.get("text"):
        result.update(transcript_total_chars=len(payload["text"]) if isinstance(payload["text"], str) else 0,
                      message="无法计算覆盖率：存在正文但没有片段时间戳")
        return result
    intervals, invalid = [], []
    for index, row in enumerate(rows):
        try:
            start, end = interval(row)
            if start >= duration or end > duration + 0.001:
                raise ValueError("timestamp outside audio")
            intervals.append((start, min(end, duration)))
        except (ValueError, TypeError, OverflowError):
            invalid.append(index)
    merged = []
    for start, end in sorted(intervals):
        # Exact adjacency only: silent gaps must not be filled by a tolerance.
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    result["covered_segments"] = [segment(a, b) for a, b in merged]
    if invalid:
        result.update(invalid_segment_indices=invalid,
                      message="无法计算覆盖率：部分片段时间戳缺失、无效或超出音频范围；gaps 未计算")
        return result
    gaps, cursor = [], 0.0
    for start, end in merged:
        if start > cursor:
            gaps.append(segment(cursor, start))
        cursor = end
    if cursor < duration:
        gaps.append(segment(cursor, duration))
    covered = sum(b - a for a, b in merged)
    result.update(gaps=gaps, covered_duration_seconds=covered, coverage_rate=round(covered / duration, 2),
                  status="PASS", message="覆盖率已计算；空白/静音也计入未覆盖时段，非识别准确率")
    return result


def coverage(args, repo, transcript_id=None):
    """Shared script-two output; no file/DB writes, also called by script three."""
    result = defaults()
    try:
        session = repo.session(args.session_id)
        row = repo.transcript(args.session_id, transcript_id or args.transcript_id)
        result["transcript_id"] = row.get("transcript_id")
        meta = metadata(row)
        payload = load_artifact(row, "transcript_json", args)
        if payload.get("session_id", args.session_id) != args.session_id:
            fail("SESSION_MISMATCH", "转录文件的 session_id 与场次不一致", "UNCOMPUTABLE")
        if payload.get("transcript_id", row.get("transcript_id")) != row.get("transcript_id"):
            fail("TRANSCRIPT_MISMATCH", "转录文件与数据库记录的版本不一致", "UNCOMPUTABLE")
        if meta.get("coverage_scope") and payload.get("coverage_scope") and meta["coverage_scope"] != payload["coverage_scope"]:
            fail("COVERAGE_SCOPE_MISMATCH", "转录文件与数据库记录的覆盖范围标记不一致", "UNCOMPUTABLE")
        audio_value = row.get("audio_path") or row.get("source_path") or session.get("audio_path")
        path = local_path(audio_value, args)
        result["audio_path"] = str(path)
        duration, _ = probe(path, args, audio=True)
        result.update(calculate(payload, duration))
        scope = meta.get("coverage_scope") or payload.get("coverage_scope")
        sample = meta.get("sample_only") is True or payload.get("sample_only") is True or scope == "SAMPLE"
        result.update(coverage_scope=scope or "UNSPECIFIED", sample_only=sample)
        if sample or (repo.layout == "runtime-v3" and scope != "FULL_SESSION"):
            result.update(status="UNCOMPUTABLE", coverage_rate=None, gaps=[],
                          message="无法计算整场覆盖率：所选转录仅为样本或未证明属于 FULL_SESSION，已列出已知片段")
    except CheckError as exc:
        result.update(status=exc.status, error_code=exc.code, message=f"无法计算覆盖率：{exc}")
    return result


def check(args, repo, result):
    result.update(coverage(args, repo))


if __name__ == "__main__":
    raise SystemExit(run_cli("check_transcription", "转录覆盖率检查（只读）", defaults, check))
