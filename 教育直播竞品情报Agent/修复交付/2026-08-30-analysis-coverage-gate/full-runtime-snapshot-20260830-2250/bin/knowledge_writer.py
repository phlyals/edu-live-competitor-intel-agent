#!/usr/bin/env python3
"""Publish one explicitly approved strategy candidate into the configured knowledge vault."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

from runtime_common import connect_db, load_config, utc_now


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff._-]+", "-", value).strip("-._")
    return cleaned[:80] or "未分类"


def self_test(config: dict) -> dict:
    root = Path(config["storage"]["directories"]["knowledge_vault"])
    with connect_db() as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {"strategy_candidates", "approvals"}
    missing = sorted(required - tables)
    ok = root.is_dir() and os.access(root, os.R_OK | os.W_OK | os.X_OK) and not missing
    return {"ok": ok, "status": "READY" if ok else "WAITING_TOOL", "knowledge_root": str(root), "missing_tables": missing, "writes_performed": 0}


def build_candidate(candidate_id: str, config: dict) -> tuple[dict | None, dict]:
    with connect_db() as conn:
        candidate = conn.execute(
            "SELECT category,claim,evidence_json,verification_state,approval_state,source_session_id,source_analysis_id FROM strategy_candidates WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()
        approval = conn.execute(
            "SELECT decision,decided_by,decided_at,notes FROM approvals WHERE object_type='strategy_candidate' AND object_id=?",
            (candidate_id,),
        ).fetchone()
    if not candidate:
        return None, {"ok": False, "status": "WAITING_HUMAN", "reason": "Strategy candidate does not exist"}
    if not approval or approval[0] != "APPROVED" or candidate[4] != "APPROVED":
        return None, {"ok": False, "status": "WAITING_HUMAN", "reason": "Both candidate and approval record must be APPROVED"}
    category, claim, evidence_json, verification_state, _approval_state, session_id, analysis_id = candidate
    try:
        evidence = json.loads(evidence_json or "[]")
    except json.JSONDecodeError:
        return None, {"ok": False, "status": "ERROR", "reason": "Candidate evidence_json is invalid"}
    payload = {
        "candidate_id": candidate_id,
        "category": category,
        "claim": claim,
        "verification_state": verification_state,
        "source_session_id": session_id,
        "source_analysis_id": analysis_id,
        "evidence": evidence,
        "approval": {"decision": approval[0], "decided_by": approval[1], "decided_at": approval[2], "notes": approval[3]},
        "published_at": utc_now(),
    }
    return payload, {"ok": True, "status": "READY"}


def render_markdown(payload: dict) -> str:
    evidence = json.dumps(payload["evidence"], ensure_ascii=False, indent=2)
    approval = payload["approval"]
    return f"""# {payload['category']}\n\n{payload['claim']}\n\n## 证据\n\n```json\n{evidence}\n```\n\n## 数据血缘\n\n- candidate_id: `{payload['candidate_id']}`\n- source_session_id: `{payload['source_session_id']}`\n- source_analysis_id: `{payload['source_analysis_id']}`\n- verification_state: `{payload['verification_state']}`\n- approved_by: `{approval.get('decided_by')}`\n- approved_at: `{approval.get('decided_at')}`\n- published_at: `{payload['published_at']}`\n"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--candidate-id")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = load_config()
    if args.self_test:
        print(json.dumps(self_test(config), ensure_ascii=False, indent=2))
        return 0
    if not args.candidate_id:
        parser.error("--candidate-id is required unless --self-test is used")
    if not (config.get("safety") or {}).get("formal_knowledge_write", False):
        print(json.dumps({"ok": False, "status": "DISABLED", "reason": "Formal knowledge-write safety gate is closed", "writes_performed": 0}, ensure_ascii=False, indent=2))
        return 0
    payload, state = build_candidate(args.candidate_id, config)
    if not payload:
        print(json.dumps({**state, "writes_performed": 0}, ensure_ascii=False, indent=2))
        return 1
    root = Path(config["storage"]["directories"]["knowledge_vault"])
    output = root / safe_name(payload["category"]) / f"{safe_name(args.candidate_id)}.md"
    content = render_markdown(payload)
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    if args.dry_run:
        print(json.dumps({"ok": True, "status": "READY", "dry_run": True, "output": str(output), "sha256": digest, "writes_performed": 0}, ensure_ascii=False, indent=2))
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and hashlib.sha256(output.read_bytes()).hexdigest() != digest:
        print(json.dumps({"ok": False, "status": "WAITING_HUMAN", "reason": "Knowledge file already exists with different content", "output": str(output), "writes_performed": 0}, ensure_ascii=False, indent=2))
        return 1
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    os.replace(temporary, output)
    output.chmod(0o600)
    print(json.dumps({"ok": True, "status": "READY", "output": str(output), "sha256": digest, "writes_performed": 1}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
