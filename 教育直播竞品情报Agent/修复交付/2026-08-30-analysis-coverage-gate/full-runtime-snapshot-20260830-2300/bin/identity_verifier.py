#!/usr/bin/env python3
"""Validate read-only Buyin detail-page uid identity evidence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from runtime_common import load_config, utc_now


def confined(path: Path, root: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"{label} must stay inside the configured analysis_drafts directory")
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    config = load_config()
    draft_root = Path(config["storage"]["directories"]["analysis_drafts"]).resolve()
    source = confined(args.input, draft_root, "input")
    destination = confined(args.output, draft_root, "output")
    payload = json.loads(source.read_text(encoding="utf-8"))

    if payload.get("dry_run") is not True:
        raise ValueError("Identity evidence must be explicitly marked as a dry run")
    side_effects = payload.get("side_effects") or {}
    forbidden = ("recording_started", "feishu_business_records_written", "formal_knowledge_base_written", "runtime_business_records_written")
    if any(side_effects.get(name) is not False for name in forbidden):
        raise ValueError("Every prohibited side effect must be explicitly false")

    accounts = payload.get("accounts")
    if not isinstance(accounts, list) or not accounts:
        raise ValueError("accounts must be a non-empty list")
    seen_names: set[str] = set()
    results = []
    for offset, account in enumerate(accounts, start=1):
        if not isinstance(account, dict):
            raise ValueError(f"Account {offset} is invalid")
        name = str(account.get("account_name") or "").strip()
        if not name or name in seen_names:
            raise ValueError(f"Account {offset} has a missing or duplicate account_name")
        seen_names.add(name)
        profile_url = str(account.get("profile_url") or "")
        buyin_uid = str(account.get("buyin_creator_uid") or "")
        url_uid = (parse_qs(urlparse(profile_url).query).get("uid") or [""])[0]
        detail_ok = bool(buyin_uid and url_uid == buyin_uid and account.get("detail_page_verified") is True)
        qr_ok = bool(
            account.get("qr_entry_visible") is True
            and account.get("qr_account_verified") is True
            and account.get("douyin_account_id")
            and account.get("qr_verified_at")
        )
        results.append({
            "account_name": name,
            "detail_identity_status": "READY" if detail_ok else "DEGRADED",
            "qr_identity_status": "READY" if qr_ok else "OPTIONAL_NOT_CAPTURED",
            "production_monitor_identity_ready": detail_ok,
            "buyin_creator_uid": buyin_uid or None,
            "douyin_account_id": account.get("douyin_account_id") or None,
        })

    unresolved = [item["account_name"] for item in results if not item["production_monitor_identity_ready"]]
    result = {
        "schema_version": 1,
        "profile_id": config["profile_id"],
        "validated_at": utc_now(),
        "status": "READY" if not unresolved else "DEGRADED",
        "method": "nickname_to_detail_page_then_capture_buyin_creator_uid",
        "monitor_key_policy": "buyin_creator_uid is canonical for Buyin monitoring; douyin_account_id is optional cross-platform enrichment",
        "account_count": len(results),
        "ready_account_count": len(results) - len(unresolved),
        "unresolved_account_names": unresolved,
        "accounts": results,
        "source_evidence": str(source),
        "side_effects": side_effects,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(destination)
    print(json.dumps({**result, "output": str(destination)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
