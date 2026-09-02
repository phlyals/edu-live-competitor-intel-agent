#!/usr/bin/env python3
"""Idempotently project Runtime V3 state into the formal Feishu Base.

The projector is profile-pinned, reads field schemas before writes, and uses a
stable business key to update existing records instead of creating duplicates.
Link fields are intentionally left to the Base's bidirectional relations; the
stable IDs are written first and are the reconciliation keys.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from v3_runtime import CONFIG_PATH, connect, identity_assertion, load_config


PYTHON = Path(__file__).resolve().parents[1] / ".venv" / "bin" / "python"
_FIELD_CACHE: dict[str, set[str]] = {}
_RECORD_CACHE: dict[str, dict[str, str]] = {}


def now_text() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def local_text(value: str | None) -> str:
    if not value:
        return now_text()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")


def call(args: list[str], *, timeout: int = 90) -> dict:
    last = ""
    for attempt in range(4):
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False, env={"HOME": "/Users/mac", "PATH": "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin", "LANG": "C"})
        text = (proc.stdout or proc.stderr).strip()
        last = text
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"lark-cli returned invalid JSON: {text[-500:]}") from exc
        if proc.returncode == 0 and payload.get("ok"):
            return payload
        error=payload.get('error') or {}
        # A read or an update of an exact record is safe to repeat after EOF.
        # Do not blindly repeat a create whose response may have been lost.
        retry_safe='--record-id' in args or any(cmd in args for cmd in ('+record-list','+field-list','+record-get'))
        if error.get('type')=='network' and retry_safe and attempt<3:
            time.sleep(2 ** attempt)
            continue
        if "429" in text or "rate" in text.lower():
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(json.dumps(payload.get("error") or payload, ensure_ascii=False))
    raise RuntimeError(f"Feishu call retry budget exhausted: {last[-500:]}")


def base_args(config: dict, table_id: str, command: str, *extra: str) -> list[str]:
    return [config["lark_cli"], "base", command, "--profile", config["lark_cli_profile"], "--base-token", config["business_base"], "--table-id", table_id, *extra, "--format", "json", "--as", "bot"]


def field_names(config: dict, table_id: str) -> set[str]:
    payload = call(base_args(config, table_id, "+field-list"))
    return {str(item.get("name")) for item in ((payload.get("data") or {}).get("fields") or [])}


def record_rows(config: dict, table_id: str) -> list[dict]:
    result: list[dict] = []
    offset = 0
    for _ in range(100):
        args = ["--limit", "200"]
        if offset:
            args.extend(["--offset", str(offset)])
        payload = call(base_args(config, table_id, "+record-list", *args))
        data = payload.get("data") or {}
        fields, rows, ids = data.get("fields") or [], data.get("data") or [], data.get("record_id_list") or []
        for record_id, row in zip(ids, rows):
            values = dict(zip(fields, row))
            result.append({"record_id": str(record_id), "fields": values})
        if not data.get("has_more"):
            return result
        if not rows:
            raise RuntimeError(f"table {table_id} pagination is inconsistent")
        offset += len(rows)
    raise RuntimeError(f"table {table_id} exceeds the 100-page safety bound")


def existing_records(config: dict, table_id: str) -> dict[str, str]:
    result = {}
    for record in record_rows(config, table_id):
        record_id = record["record_id"]
        values = record["fields"]
        for key in ("商品ID", "同行ID", "关系ID", "监控ID", "任务ID", "场次ID", "转录ID", "分析ID", "版本ID", "候选ID"):
            if values.get(key):
                result[f"{key}:{values[key]}"] = str(record_id)
        if values.get("商品ID") and values.get("同行ID"):
            result[f"REL:{values['商品ID']}:{values['同行ID']}"] = str(record_id)
        if values.get("同行ID") and "监控ID" in values:
            result[f"MON:{values['同行ID']}"] = str(record_id)
    return result


def upsert(config: dict, table_id: str, key_field: str, key: str, fields: dict, *, dry_run: bool) -> dict:
    available = _FIELD_CACHE.get(table_id)
    if available is None:
        available = field_names(config, table_id)
        _FIELD_CACHE[table_id] = available
    unknown = sorted(set(fields) - available)
    if unknown:
        raise RuntimeError(f"unknown fields for {table_id}: {unknown}")
    existing = _RECORD_CACHE.get(table_id)
    if existing is None:
        existing = existing_records(config, table_id)
        _RECORD_CACHE[table_id] = existing
    record_id = existing.get(f"{key_field}:{key}")
    if not record_id and key_field == "关系ID":
        record_id = existing.get(f"REL:{fields.get('商品ID')}:{fields.get('同行ID')}")
    if not record_id and key_field == "监控ID":
        record_id = existing.get(f"MON:{fields.get('同行ID')}")
    args = base_args(config, table_id, "+record-upsert", "--json", json.dumps(fields, ensure_ascii=False))
    if record_id:
        args.extend(["--record-id", record_id])
    if dry_run:
        return {"table_id": table_id, "key": key, "record_id": record_id, "fields": fields, "dry_run": True}
    response = call(args)
    return {"table_id": table_id, "key": key, "record_id": record_id, "created": not bool(record_id), "response": response}


def session_projection_states(session: dict, segment: dict | None = None) -> tuple[str, str, str]:
    """Map runtime states only to existing Feishu options without inventing completeness."""
    status = str(session["status"])
    completeness = str(session["completeness"] or "UNKNOWN").upper()
    scene_state = {
        "RECORDING": "录制中", "WAITING_CAPACITY": "检测到开播", "WAITING_STREAM": "检测到开播",
        "ENDED": "已下播", "MEDIA_COMPLETE": "处理完成", "DUPLICATE_SUPERSEDED": "失败暂停", "IMPORTED_FAILED": "失败暂停",
    }.get(status, "UNKNOWN")
    complete_text = {"COMPLETE": "完整", "PARTIAL": "部分", "INCOMPLETE": "部分"}.get(completeness, "UNKNOWN")
    if status == "RECORDING":
        recording_state = "录制中"
    elif status in {"DUPLICATE_SUPERSEDED", "IMPORTED_FAILED", "FAILED"}:
        recording_state = "失败"
    elif status in {"ENDED", "MEDIA_COMPLETE"} and completeness == "COMPLETE":
        recording_state = "成功"
    elif completeness in {"PARTIAL", "INCOMPLETE"}:
        recording_state = "部分"
    else:
        recording_state = "待开始"
    return scene_state, recording_state, complete_text


def analysis_projection_fields(analysis: dict) -> tuple[dict, str]:
    """Map qualification state without presenting a SAMPLE as formal output."""
    try:
        metadata = json.loads(analysis.get("metadata_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}
    analysis_type = str(analysis.get("analysis_type") or "single_session")
    is_single = analysis_type == "single_session"
    qualification_column = str(analysis.get("qualification_status") or "")
    scope_column = str(analysis.get("scope") or "")
    is_sample = bool(
        analysis.get("status") == "SAMPLE_NONQUALIFYING"
        or qualification_column == "SAMPLE_NONQUALIFYING"
        or scope_column == "SAMPLE_AUXILIARY"
        or metadata.get("qualification_state") == "SAMPLE_NONQUALIFYING"
    )
    qualified = bool(
        is_single
        and analysis.get("lineage_state") == "CURRENT"
        and scope_column == "FORMAL_SINGLE_SESSION"
        and qualification_column == "FULL_SESSION_QUALIFIED"
        and metadata.get("qualification_state") == "FULL_SESSION_QUALIFIED"
        and metadata.get("formal_analysis_eligible") is True
    )
    superseded_formal = bool(
        is_single
        and analysis.get("status") == "COMPLETE"
        and analysis.get("lineage_state") == "SUPERSEDED"
        and scope_column == "FORMAL_SINGLE_SESSION"
        and qualification_column == "FULL_SESSION_QUALIFIED"
    )
    stale_formal = bool(
        is_single
        and analysis.get("status") == "COMPLETE"
        and analysis.get("lineage_state") == "STALE"
        and scope_column == "FORMAL_SINGLE_SESSION"
        and qualification_column == "FULL_SESSION_QUALIFIED"
    )
    if is_sample:
        projection_type, projection_status = "部分场辅助", "证据不足"
        qualification = "SAMPLE_NONQUALIFYING"
        evidence = "SAMPLE_NONQUALIFYING：仅300秒样本，不是整场分析，不可用于正式决策或后续交付；lineage_state=" + str(analysis.get("lineage_state") or "INVALIDATED")
    elif stale_formal:
        projection_type, projection_status = "单场分析", "证据不足"
        qualification = "STALE_RECOMPUTE_REVIEW"
        evidence = (
            "STALE_RECOMPUTE_REVIEW：上游转录或分析版本已变化；旧报告文件仍保留可读，"
            "但在新版完成证据验证前不可作为当前决策版本；recompute_request="
            + str(metadata.get("recompute_request_id") or "UNKNOWN")
        )
    elif superseded_formal:
        projection_type, projection_status = "单场分析", "完成"
        qualification = "SUPERSEDED_FORMAL_VERSION"
        evidence = (
            "SUPERSEDED_FORMAL_VERSION：该报告来源完整，但已被新版分析替代，"
            "不再是当前决策版本；superseded_by="
            + str(metadata.get("superseded_by_analysis_id") or "UNKNOWN")
        )
    elif analysis.get("status") == "QUALITY_BLOCKED":
        projection_type, projection_status = "部分场辅助", "证据不足"
        qualification = "ANALYSIS_QUALITY_BLOCKED"
        evidence = "ANALYSIS_QUALITY_BLOCKED：分析覆盖率未达到90%硬门槛，不可作为正式输出"
    elif is_single and not qualified:
        projection_type, projection_status = "部分场辅助", "证据不足"
        qualification = "SOURCE_NONQUALIFYING"
        evidence = "SOURCE_NONQUALIFYING：未通过当前整场转录资格校验，不可作为正式单场分析；lineage_state=" + str(analysis.get("lineage_state") or "UNKNOWN")
    else:
        projection_type = "单场分析" if is_single else analysis_type
        projection_status = "完成" if analysis.get("status") == "COMPLETE" else "分析中"
        qualification = "FULL_SESSION_QUALIFIED" if is_single else "HISTORICAL_FORMAL"
        evidence = "Runtime V3 qualification=" + qualification + "; lineage_state=" + str(analysis.get("lineage_state") or "CURRENT")
    fields = {
        "分析ID": analysis["analysis_id"],
        "当前场次ID": analysis["session_id"],
        "分析类型": projection_type,
        "状态": projection_status,
        "分析文档": analysis.get("output_path") or "",
        "证据说明": evidence,
        "变化摘要": "",
        "整体相似度": 0,
    }
    return fields, qualification


def project(*, dry_run: bool = False, product_id: str | None = None) -> dict:
    identity_assertion(verify_cli=True)
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    tables = config["business_tables"]
    stamp = now_text()
    reports = []
    with connect() as conn:
        products = [dict(row) for row in conn.execute("SELECT * FROM products ORDER BY product_id")]
        competitors = [dict(row) for row in conn.execute("SELECT * FROM competitors ORDER BY account_name")]
        relations = [dict(row) for row in conn.execute("SELECT * FROM product_competitors ORDER BY relation_id")]
        monitors = [dict(row) for row in conn.execute("SELECT m.*,c.account_name FROM monitor_targets m JOIN competitors c ON c.competitor_id=m.competitor_id ORDER BY c.account_name")]
        sessions = [dict(row) for row in conn.execute("SELECT * FROM live_sessions WHERE status!='DUPLICATE_SUPERSEDED' ORDER BY started_at")]
        segments_by_session = {}
        for row in conn.execute("SELECT * FROM recording_segments WHERE status='COMPLETE' ORDER BY captured_to DESC"):
            segments_by_session.setdefault(row["session_id"], dict(row))
        transcripts = [dict(row) for row in conn.execute("SELECT * FROM transcripts ORDER BY created_at")]
        analyses = [dict(row) for row in conn.execute("SELECT * FROM analyses WHERE status!='SKIPPED_HISTORICAL' ORDER BY analysis_id")]
        strategies = [dict(row) for row in conn.execute("SELECT * FROM strategy_candidates ORDER BY created_at")]
        strategy_versions = [dict(row) for row in conn.execute("SELECT * FROM strategy_versions ORDER BY competitor_id,version_no")]
        versions = [dict(row) for row in conn.execute("SELECT * FROM knowledge_versions ORDER BY created_at")]
        diffs = [dict(row) for row in conn.execute("SELECT * FROM knowledge_diffs ORDER BY created_at")]
        reviews = [dict(row) for row in conn.execute("SELECT * FROM review_items ORDER BY requested_at")]
        heartbeat = conn.execute("SELECT * FROM heartbeats WHERE service_name='runtime-v3'").fetchone()
        activation = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    if product_id:
        products = [row for row in products if row['product_id']==product_id]
        if not products: raise ValueError('projection product not found: '+product_id)
        relations = [row for row in relations if row['product_id']==product_id]
        selected_ids = {row['competitor_id'] for row in relations}
        competitors = [row for row in competitors if row['competitor_id'] in selected_ids]
        monitors = [row for row in monitors if row['competitor_id'] in selected_ids]
        sessions, transcripts, analyses, strategies, strategy_versions, versions = [], [], [], [], [], []
    for product in products:
        fields = {
            "商品ID": product["product_id"], "平台商品ID": product["platform_product_id"], "商品名称": product["title"],
            "原始链接": product["source_url"], "扫描完整性": "完整", "状态": "监控中",
            "首次发现时间": local_text(product["first_seen_at"]), "最近扫描时间": local_text(product["last_seen_at"]),
        }
        reports.append(upsert(config, tables["01_商品"], "商品ID", product["product_id"], fields, dry_run=dry_run))
    for competitor in competitors:
        target = next((item for item in monitors if item["competitor_id"] == competitor["competitor_id"]), None)
        fields = {
            "同行ID": competitor["competitor_id"], "平台账号ID": competitor["platform_account_id"], "账号名称": competitor["account_name"],
            "直播间地址": target["live_url"] if target else "", "监控状态": "监控中" if target else "待加入", "账号状态": "正常",
            "首次发现时间": local_text(competitor["first_seen_at"]), "最后可见时间": local_text(competitor["last_seen_at"]),
            "备注": "Runtime V3 canonical identity; historical aliases retained in Runtime V3" if competitor["metadata_json"] else "Runtime V3 canonical identity",
        }
        reports.append(upsert(config, tables["02_同行账号"], "同行ID", competitor["competitor_id"], fields, dry_run=dry_run))
    for relation in relations:
        fields = {
            "关系ID": relation["relation_id"], "商品ID": relation["product_id"], "同行ID": relation["competitor_id"],
            "当前状态": "持续", "本轮扫描ID": relation["last_scan_id"] or "runtime-v3-import",
            "最后可见时间": local_text(relation["last_seen_at"]), "扫描完整性": "完整",
            "平台可见数据摘要": f"来源扫描：{relation['last_scan_id'] or '历史导入'}；同行身份及直播入口以对应账号监控记录为准。",
        }
        reports.append(upsert(config, tables["03_商品同行关系"], "关系ID", relation["relation_id"], fields, dry_run=dry_run))
    for monitor in monitors:
        state = {"LIVE": "直播中", "OFFLINE_CONFIRMED": "未直播", "UNKNOWN": "UNKNOWN"}.get(monitor["live_status"], "UNKNOWN")
        fields = {
            "监控ID": monitor["monitor_target_id"], "同行ID": monitor["competitor_id"], "直播状态": state,
            "下次检查时间": local_text(monitor["next_check_at"]),
            "最近检查时间": local_text(monitor["last_checked_at"]),
            "监控状态": "监控中", "连续失败次数": monitor["consecutive_unknown"], "最近错误": "" if state != "UNKNOWN" else "UNKNOWN状态保留，未判定为未直播",
        }
        reports.append(upsert(config, tables["04_账号监控"], "监控ID", monitor["monitor_target_id"], fields, dry_run=dry_run))
    for session in sessions:
        target = next((item for item in monitors if item["monitor_target_id"] == session["monitor_target_id"]), None)
        segment = segments_by_session.get(session["session_id"])
        scene_state, recording_state, complete_text = session_projection_states(session, segment)
        fields = {"场次ID": session["session_id"], "平台场次ID": session["platform_session_id"], "同行ID": target["competitor_id"] if target else "", "直播入口": session["source_url"], "场次状态": scene_state, "录制状态": recording_state, "完整性": complete_text, "开始时间": local_text(session["started_at"]), "结束时间": local_text(session["ended_at"]) if session["ended_at"] else "", "媒体证据路径": segment["path"] if segment else "", "最近错误": ""}
        reports.append(upsert(config, tables["05_直播场次"], "场次ID", session["session_id"], fields, dry_run=dry_run))
    for transcript in transcripts:
        fields = {"转录ID": transcript["transcript_id"], "场次ID": transcript["session_id"], "引擎": transcript["engine"], "模型": transcript["model"], "状态": "完成" if transcript["status"] == "COMPLETE" else "转录中", "原始媒体路径": transcript.get("source_path") or "", "带时间戳逐字稿路径": transcript.get("output_path") or "", "清洗稿路径": transcript.get("metadata_json") or "", "语言": transcript.get("language") or "zh", "低置信度片段数": transcript.get("low_confidence_count") or 0, "证据说明": "Runtime V3 provenance"}
        reports.append(upsert(config, tables["06_转录证据"], "转录ID", transcript["transcript_id"], fields, dry_run=dry_run))
    for analysis in analyses:
        fields, _qualification = analysis_projection_fields(analysis)
        reports.append(upsert(config, tables["07_场次分析"], "分析ID", analysis["analysis_id"], fields, dry_run=dry_run))
    for strategy in strategies:
        diff = next((item for item in diffs if item.get("candidate_id") == strategy["candidate_id"]), None)
        fields = {"候选ID": strategy["candidate_id"], "策略候选": strategy["strategy_type"], "验证状态": "连续三场确认" if strategy.get("version_id") else "单场观察", "正式写入状态": "已写入" if strategy["status"] == "APPROVED" else "未申请", "类别": "其他", "来源场次ID": strategy.get("session_id") or "", "来源同行ID": strategy.get("competitor_id") or "", "证据时间戳": strategy.get("evidence_json") or "", "人工结论": "已批准" if strategy["status"] == "APPROVED" else "待审核", "正式知识Diff": diff.get("diff_path") if diff else ""}
        reports.append(upsert(config, tables["09_策略候选审批"], "候选ID", strategy["candidate_id"], fields, dry_run=dry_run))
    for version in strategy_versions:
        fields = {"版本ID": version["version_id"], "版本号": str(version["version_no"]), "当前状态": "生效" if version["status"] == "ACTIVE" else "候选", "稳定打法": version["structure_digest"], "核心结构": version["content_path"], "变更原因（推断）": "连续三场确认；历史恢复同样要求连续三场", "证据文档": version["content_path"], "支持场次数": version["supporting_session_count"], "同行ID": version["competitor_id"]}
        reports.append(upsert(config, tables["08_打法版本"], "版本ID", version["version_id"], fields, dry_run=dry_run))
    for version in versions:
        fields = {"版本ID": version["version_id"], "版本号": str(version["version_no"]), "当前状态": "生效" if version["status"] == "APPROVED" else "候选", "稳定打法": version["object_key"], "核心结构": "", "变更原因（推断）": "", "证据文档": version["content_path"], "支持场次数": 0, "同行ID": ""}
        reports.append(upsert(config, tables["08_打法版本"], "版本ID", version["version_id"], fields, dry_run=dry_run))
    if heartbeat:
        fields = {
            "任务ID": "runtime-v3", "任务类型": "full-fleet-control-plane", "状态": "RUNNING" if heartbeat["status"] == "READY" else heartbeat["status"],
            "当前阶段": "全量同行监控与录制调度", "最后Checkpoint": "ATOMIC_FULL_FLEET_ACTIVATED", "Gateway Profile": "edu_live_competitor_intel",
            "最后心跳": local_text(heartbeat["last_heartbeat_at"]), "下一步": "按next_check_at错峰检测全部监控目标", "最近错误": "",
        }
        reports.append(upsert(config, tables["10_Runtime状态"], "任务ID", "runtime-v3", fields, dry_run=dry_run))
    return {"status": "DRY_RUN" if dry_run else "VERIFIED", "records": len(reports), "reports": reports, "profile_id": config["lark_cli_profile"], "schema_version": activation[0] if activation else None}


def project_monitor_status(monitor_id: str, *, dry_run: bool = False) -> dict:
    identity_assertion(verify_cli=True)
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    table_id = config["business_tables"]["04_账号监控"]
    with connect() as conn:
        row = conn.execute("SELECT m.*,c.account_name FROM monitor_targets m JOIN competitors c ON c.competitor_id=m.competitor_id WHERE m.monitor_target_id=?", (monitor_id,)).fetchone()
    if not row:
        raise RuntimeError(f"monitor target not found: {monitor_id}")
    state = {"LIVE": "直播中", "OFFLINE_CONFIRMED": "未直播", "UNKNOWN": "UNKNOWN"}.get(row["live_status"], "UNKNOWN")
    fields = {
        "监控ID": row["monitor_target_id"], "同行ID": row["competitor_id"], "直播状态": state,
        "下次检查时间": local_text(row["next_check_at"]),
        "最近检查时间": local_text(row["last_checked_at"]),
        "监控状态": "监控中", "连续失败次数": row["consecutive_unknown"], "最近错误": "" if state != "UNKNOWN" else "UNKNOWN状态保留，未判定为未直播",
    }
    report = upsert(config, table_id, "监控ID", row["monitor_target_id"], fields, dry_run=dry_run)
    return {"status": "DRY_RUN" if dry_run else "VERIFIED", "report": report, "profile_id": config["lark_cli_profile"]}


def project_analysis(analysis_id: str, *, dry_run: bool = False) -> dict:
    """Project one analysis row for an outbox event; avoid full-fleet burst writes."""
    identity_assertion(verify_cli=True)
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    table_id = config["business_tables"]["07_场次分析"]
    with connect() as conn:
        row = conn.execute("SELECT * FROM analyses WHERE analysis_id=? AND status!='SKIPPED_HISTORICAL'", (analysis_id,)).fetchone()
    if not row:
        raise RuntimeError(f"analysis not found: {analysis_id}")
    fields, qualification = analysis_projection_fields(dict(row))
    report = upsert(config, table_id, "分析ID", row["analysis_id"], fields, dry_run=dry_run)
    return {"status": "DRY_RUN" if dry_run else "VERIFIED", "qualification_state": qualification, "report": report, "profile_id": config["lark_cli_profile"]}


def project_session(session_id: str, *, dry_run: bool = False) -> dict:
    identity_assertion(verify_cli=True)
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    table_id = config["business_tables"]["05_直播场次"]
    with connect() as conn:
        session = conn.execute("SELECT * FROM live_sessions WHERE session_id=?", (session_id,)).fetchone()
        target = conn.execute("SELECT m.*,c.account_name FROM monitor_targets m JOIN competitors c ON c.competitor_id=m.competitor_id WHERE m.monitor_target_id=?", (session["monitor_target_id"],)).fetchone() if session else None
    if not session:
        raise RuntimeError(f"session not found: {session_id}")
    segment = None
    with connect() as conn:
        segment = conn.execute("SELECT path FROM recording_segments WHERE session_id=? AND status='COMPLETE' ORDER BY captured_to DESC LIMIT 1", (session["session_id"],)).fetchone()
    scene_state, recording_state, complete_text = session_projection_states(session, segment)
    fields = {"场次ID": session["session_id"], "平台场次ID": session["platform_session_id"], "同行ID": target["competitor_id"] if target else "", "直播入口": session["source_url"], "场次状态": scene_state, "录制状态": recording_state, "完整性": complete_text, "开始时间": local_text(session["started_at"]), "结束时间": local_text(session["ended_at"]) if session["ended_at"] else "", "媒体证据路径": segment["path"] if segment else "", "最近错误": ""}
    report = upsert(config, table_id, "场次ID", session["session_id"], fields, dry_run=dry_run)
    return {"status": "DRY_RUN" if dry_run else "VERIFIED", "report": report, "profile_id": config["lark_cli_profile"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--monitor-id")
    parser.add_argument("--analysis-id")
    parser.add_argument("--session-id")
    parser.add_argument("--product-id")
    args = parser.parse_args()
    if args.session_id:
        payload = project_session(args.session_id, dry_run=args.dry_run)
    elif args.analysis_id:
        payload = project_analysis(args.analysis_id, dry_run=args.dry_run)
    elif args.monitor_id:
        payload = project_monitor_status(args.monitor_id, dry_run=args.dry_run)
    else:
        payload = project(dry_run=args.dry_run, product_id=args.product_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
