#!/usr/bin/env python3
"""Import historical formal-Base rows into Runtime V3 before reconciliation."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import CONFIG_PATH, connect, init_db, utc_now  # noqa: E402


def cli_rows(config: dict, table_id: str) -> list[dict]:
    args = [config["lark_cli"], "base", "+record-list", "--profile", config["lark_cli_profile"], "--base-token", config["business_base"], "--table-id", table_id, "--limit", "200", "--format", "json", "--as", "bot"]
    proc = subprocess.run(args, capture_output=True, text=True, timeout=120, check=False, env={"HOME": "/Users/mac", "PATH": "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin", "LANG": "C"})
    payload = json.loads(proc.stdout or proc.stderr)
    if proc.returncode != 0 or not payload.get("ok"):
        raise RuntimeError(json.dumps(payload.get("error") or payload, ensure_ascii=False))
    data = payload.get("data") or {}
    fields = data.get("fields") or []
    return [{"record_id": record_id, "fields": dict(zip(fields, values))} for record_id, values in zip(data.get("record_id_list") or [], data.get("data") or [])]


def text(value) -> str:
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return ",".join(str(item.get("id") or "") for item in value)
        return ",".join(str(item) for item in value)
    return str(value or "").strip()


def url(value: str) -> str:
    match = re.search(r"https?://[^)\s]+", value or "")
    return match.group(0) if match else text(value)


def status_value(value: str, *needles: str) -> bool:
    return any(needle in value for needle in needles)


def main() -> int:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    tables = config["business_tables"]
    init_db()
    rows05 = cli_rows(config, tables["05_直播场次"])
    rows06 = cli_rows(config, tables["06_转录证据"])
    rows07 = cli_rows(config, tables["07_场次分析"])
    rows08 = cli_rows(config, tables["08_打法版本"])
    rows09 = cli_rows(config, tables["09_策略候选审批"])
    imported = {"sessions": 0, "transcripts": 0, "analyses": 0, "versions": 0, "strategies": 0}
    with connect() as conn:
        # Sessions are imported first because transcript and analysis rows
        # reference their stable场次ID.
        for item in rows05:
            f = item["fields"]
            sid = text(f.get("场次ID"))
            if not sid or conn.execute("SELECT 1 FROM live_sessions WHERE session_id=?", (sid,)).fetchone():
                continue
            competitor_key = text(f.get("同行ID"))
            target = None
            if competitor_key.startswith("douyin:"):
                identity = conn.execute("SELECT competitor_id FROM identities WHERE platform='douyin' AND stable_id=?", (competitor_key.split(":", 1)[1],)).fetchone()
                if identity:
                    target = conn.execute("SELECT monitor_target_id FROM monitor_targets WHERE competitor_id=?", (identity[0],)).fetchone()
            if not target:
                target = conn.execute("SELECT monitor_target_id FROM monitor_targets ORDER BY monitor_target_id LIMIT 1").fetchone()
            if not target:
                continue
            raw_state = text(f.get("场次状态")) + text(f.get("录制状态"))
            state = "MEDIA_COMPLETE" if status_value(raw_state, "处理完成", "成功") else ("RECORDING" if "录制中" in raw_state else "IMPORTED_FAILED")
            conn.execute("INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,ended_at,completeness,source_url,metadata_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO NOTHING", (sid, target[0], text(f.get("平台场次ID")) or sid, state, text(f.get("开始时间")) or utc_now(), text(f.get("结束时间")) or None, "COMPLETE" if "完整" in text(f.get("完整性")) else "IMPORTED", url(text(f.get("直播入口"))), json.dumps({"source": "feishu_history", "record_id": item["record_id"], "title": text(f.get("直播标题")), "raw_fields": f}, ensure_ascii=False, sort_keys=True)))
            imported["sessions"] += 1
        for item in rows06:
            f = item["fields"]
            tid = text(f.get("转录ID")); sid = text(f.get("场次ID"))
            if not tid or not sid or not conn.execute("SELECT 1 FROM live_sessions WHERE session_id=?", (sid,)).fetchone():
                continue
            digest = hashlib.sha256((tid + text(f.get("原始媒体路径")) + text(f.get("带时间戳逐字稿路径"))).encode()).hexdigest()
            state = "COMPLETE" if status_value(text(f.get("状态")), "成功", "完成") else "PAUSED"
            conn.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,language,source_path,output_path,low_confidence_count,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(transcript_id) DO NOTHING", (tid, sid, digest, text(f.get("引擎")) or "historical", text(f.get("模型")) or "historical", state, text(f.get("语言")) or "zh", text(f.get("原始媒体路径")), text(f.get("带时间戳逐字稿路径")), int(float(text(f.get("低置信度片段数")) or 0)), text(f.get("创建时间")) or utc_now(), json.dumps({"source": "feishu_history", "record_id": item["record_id"], "evidence": text(f.get("证据说明")), "cleaned_path": text(f.get("清洗稿路径"))}, ensure_ascii=False, sort_keys=True)))
            imported["transcripts"] += 1
        for item in rows07:
            f = item["fields"]
            aid = text(f.get("分析ID")); sid = text(f.get("当前场次ID")) or text(f.get("当前场次"))
            if not aid or not sid or not conn.execute("SELECT 1 FROM live_sessions WHERE session_id=?", (sid,)).fetchone():
                continue
            digest = hashlib.sha256((aid + text(f.get("分析文档")) + text(f.get("变化摘要"))).encode()).hexdigest()
            state = "COMPLETE" if status_value(text(f.get("状态")), "完成") else "PENDING"
            conn.execute("INSERT INTO analyses(analysis_id,session_id,analysis_type,source_digest,status,output_path,lineage_state,metadata_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(analysis_id) DO NOTHING", (aid, sid, text(f.get("分析类型")) or "historical_comparison", digest, state, url(text(f.get("分析文档"))), "CURRENT", json.dumps({"source": "feishu_history", "record_id": item["record_id"], "change_summary": text(f.get("变化摘要")), "evidence": text(f.get("证据说明"))}, ensure_ascii=False, sort_keys=True)))
            transcript = conn.execute("SELECT transcript_id FROM transcripts WHERE session_id=? ORDER BY created_at LIMIT 1", (sid,)).fetchone()
            if transcript:
                conn.execute("INSERT INTO lineage_edges(edge_id,downstream_type,downstream_id,upstream_type,upstream_id,upstream_version,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", ("lineage:history:" + aid, "analysis", aid, "transcript", transcript[0], digest, utc_now()))
            imported["analyses"] += 1
        for item in rows08:
            f = item["fields"]
            vid = text(f.get("版本ID"))
            if not vid or conn.execute("SELECT 1 FROM knowledge_versions WHERE version_id=?", (vid,)).fetchone():
                continue
            conn.execute("INSERT INTO knowledge_versions(version_id,object_key,version_no,status,content_path,content_hash,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(version_id) DO NOTHING", (vid, text(f.get("同行ID")) or text(f.get("稳定打法")) or "historical", 1, "DRAFT", url(text(f.get("证据文档"))), hashlib.sha256(json.dumps(f,ensure_ascii=False,sort_keys=True).encode()).hexdigest(), text(f.get("创建时间")) or utc_now(), json.dumps({"source": "feishu_history", "record_id": item["record_id"], "raw_fields": f}, ensure_ascii=False, sort_keys=True)))
            imported["versions"] += 1
        for item in rows09:
            f = item["fields"]
            cid = text(f.get("候选ID")); sid = text(f.get("来源场次ID"))
            if not cid or conn.execute("SELECT 1 FROM strategy_candidates WHERE candidate_id=?", (cid,)).fetchone():
                continue
            analysis = conn.execute("SELECT analysis_id FROM analyses WHERE session_id=? ORDER BY analysis_id DESC LIMIT 1", (sid,)).fetchone() if sid else None
            conn.execute("INSERT INTO strategy_candidates(candidate_id,session_id,analysis_id,strategy_type,status,source_digest,content_path,lineage_state,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id) DO NOTHING", (cid, sid or None, analysis[0] if analysis else None, text(f.get("类别")) or "historical", "PENDING_REVIEW", hashlib.sha256((cid + text(f.get("策略候选"))).encode()).hexdigest(), url(text(f.get("正式知识Diff"))), "CURRENT", text(f.get("创建时间")) or utc_now(), json.dumps({"source": "feishu_history", "record_id": item["record_id"], "candidate": text(f.get("策略候选")), "decision": text(f.get("人工结论"))}, ensure_ascii=False, sort_keys=True)))
            imported["strategies"] += 1
        conn.commit()
    out = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/feishu-history-import.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(imported, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(imported, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
