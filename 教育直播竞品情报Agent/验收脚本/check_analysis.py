#!/usr/bin/env python3
"""Seven-module acceptance, without guessing mappings from legacy reports."""
from acceptance_common import (EXPECTED_MODULES, fail, interval, load_artifact,
                               run_cli, segment)
from check_transcription import coverage


def defaults():
    return dict(modules_present=[], expected_modules=list(EXPECTED_MODULES),
                missing_modules=list(EXPECTED_MODULES), modules_with_timestamps=[],
                timestamps_in_coverage=[], is_complete=False)


def modules(payload):
    # Only the documented optional V3 envelope is unwrapped.
    if "result" in payload and "modules" in payload:
        fail("REPORT_FORMAT", "格式异常：同时存在 result 和顶层 modules，无法确定报告真本", "FORMAT_ERROR")
    body = payload.get("result", payload)
    if not isinstance(body, dict) or "modules" not in body:
        fail("REPORT_FORMAT", "格式异常：报告缺少明确的 modules 结构，不映射 hook/claims 等旧字段", "FORMAT_ERROR")
    raw = body["modules"]
    if isinstance(raw, dict):
        rows = []
        for name, value in raw.items():
            if not isinstance(value, dict) or ("name" in value and value["name"] != name):
                fail("REPORT_FORMAT", "格式异常：模块必须是对象且名称一致", "FORMAT_ERROR")
            rows.append({**value, "name": name})
    elif isinstance(raw, list):
        rows = raw
    else:
        fail("REPORT_FORMAT", "格式异常：modules 必须是列表或按名称索引的对象", "FORMAT_ERROR")
    names = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("name"), str) or not row["name"].strip():
            fail("REPORT_FORMAT", "格式异常：模块缺少 name", "FORMAT_ERROR")
        if row["name"] in names:
            fail("REPORT_FORMAT", "格式异常：模块名称重复", "FORMAT_ERROR")
        names.add(row["name"])
    return rows


def references(row):
    keys = [k for k in ("timestamps", "evidence_refs") if k in row]
    if not keys:
        return []
    if len(keys) != 1 or not isinstance(row[keys[0]], list):
        fail("REPORT_FORMAT", "格式异常：时间戳引用必须是唯一的 timestamps 或 evidence_refs 数组", "FORMAT_ERROR")
    result = []
    for item in row[keys[0]]:
        try:
            result.append(interval(item, allow_point=True))
        except (ValueError, TypeError, OverflowError):
            fail("REPORT_FORMAT", "格式异常：时间戳引用须为有效的 start/end 或 start_time/end_time 秒数", "FORMAT_ERROR")
    return result


def validate(payload, coverage_result):
    result = defaults()
    rows = modules(payload)
    refs = [(row["name"], references(row)) for row in rows]
    present = [name for name, _ in refs]
    missing = [name for name in EXPECTED_MODULES if name not in present]
    ready = coverage_result.get("status") == "PASS" and coverage_result.get("coverage_rate") is not None
    covered = coverage_result.get("covered_segments", []) if ready else []
    timestamps, checks = [], []
    for name, intervals in refs:
        timestamps.append({"module": name, "has_timestamps": bool(intervals)})
        items = []
        for start, end in intervals:
            # An interval must fit wholly in one merged coverage interval.
            # Checking only its endpoints would miss a gap in the middle.
            inside = any(c["start_time"] <= start and end <= c["end_time"] for c in covered) if ready else None
            items.append({**segment(start, end), "in_coverage": inside})
        checks.append({"module": name, "in_coverage": bool(intervals) and all(i["in_coverage"] is True for i in items),
                       "references": items})
    complete = not missing and ready and all(r["has_timestamps"] for r in timestamps) and all(r["in_coverage"] for r in checks)
    result.update(modules_present=present, missing_modules=missing, modules_with_timestamps=timestamps,
                  timestamps_in_coverage=checks, is_complete=complete,
                  status="PASS" if complete else "FAIL",
                  coverage_status=coverage_result.get("status"),
                  message="七模块及时间戳引用完整" if complete else "模块缺失、缺少引用，或引用无法被转录覆盖证据支持")
    return result


def check(args, repo, result):
    repo.session(args.session_id)
    row = repo.report(args.session_id)
    result["report_id"] = row.get("analysis_id") or row.get("report_id")
    payload = load_artifact(row, "report_json", args)
    if payload.get("session_id", args.session_id) != args.session_id:
        fail("SESSION_MISMATCH", "格式异常：报告 session_id 与请求场次不一致", "FORMAT_ERROR")
    modules(payload)  # Do not attempt transcription or infer modules on an unknown schema.
    if row.get("transcript_id") and payload.get("transcript_id") and row["transcript_id"] != payload["transcript_id"]:
        fail("TRANSCRIPT_MISMATCH", "报告文件与数据库记录引用的转录版本不一致", "FAIL")
    linked_id = payload.get("transcript_id") or row.get("transcript_id")
    if linked_id and args.transcript_id and linked_id != args.transcript_id:
        fail("TRANSCRIPT_MISMATCH", "报告引用的转录版本与 --transcript-id 不一致", "FAIL")
    evidence = coverage(args, repo, linked_id or args.transcript_id)
    result.update(validate(payload, evidence))
    result["transcript_id"] = evidence.get("transcript_id")
    result["coverage_message"] = evidence.get("message")


if __name__ == "__main__":
    raise SystemExit(run_cli("check_analysis", "分析报告完整性检查（只读）", defaults, check))
