#!/usr/bin/env python3
"""Profile-local deterministic Feishu ingress for Runtime V3."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import argparse
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "v3_config.json"
URL_RE = re.compile(r"https://[^\s<>\[\]()\"']+", re.IGNORECASE)
BUSINESS_INTENTS = ("扫描商品", "查询同行", "查找同行", "新增商品", "商品同行", "竞品扫描")


def extract_urls(content: str) -> list[str]:
    urls: list[str] = []
    for match in URL_RE.finditer(content or ""):
        value = match.group(0).rstrip("。,.，；;！!？?")
        if value not in urls:
            urls.append(value)
    return urls


def supported_business_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    if parsed.scheme.lower() != "https":
        return False
    return host == "douyin.com" or host.endswith(".douyin.com") or host == "jinritemai.com" or host.endswith(".jinritemai.com")


def classify(content: str) -> dict:
    from v3_task_control import parse_control
    control = parse_control(content)
    if control:
        return {"status": "CONTROL", "control": control}
    urls = extract_urls(content)
    candidates = [url for url in urls if supported_business_url(url)]
    explicit_intent = any(intent in (content or "") for intent in BUSINESS_INTENTS)
    if not explicit_intent and not candidates:
        return {"status": "NOT_BUSINESS", "urls": urls, "candidate_urls": []}
    return {
        "status": "BUSINESS_CANDIDATE",
        "urls": urls,
        "candidate_urls": candidates,
        "explicit_intent": explicit_intent,
    }


def handle_inbound_message(
    *, message_id: str, chat_id: str, sender_id: str, content: str,
    message_type: str, chat_type: str,
) -> dict:
    if chat_type not in {"p2p", "dm"}:
        return {"status": "NOT_BUSINESS", "reason": "group_messages_not_routed"}
    classification = classify(content)
    if classification["status"] not in {"BUSINESS_CANDIDATE", "CONTROL"}:
        return classification
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if sender_id not in set(config.get("allowed_sender_ids") or []):
        return {"status": "REJECTED", "reason": "sender_not_allowed", "sender_id": sender_id}
    try:
        if classification["status"] == "CONTROL":
            from v3_task_control import ingest_control
            return ingest_control(message_id=message_id, chat_id=chat_id, sender_id=sender_id, content=content, control=classification["control"])
        from v3_runtime import ingest_message

        result = ingest_message(
            message_id=message_id,
            chat_id=chat_id,
            sender_id=sender_id,
            content=content,
            parsed={
                "message_type": message_type,
                "chat_type": chat_type,
                "candidate_urls": classification["candidate_urls"],
                "all_urls": classification["urls"],
                "explicit_intent": classification["explicit_intent"],
                "ingress_version": 2,
            },
        )
        return {
            **result,
            "status": "CAPTURED" if result.get("created") else "DUPLICATE",
            "candidate_urls": classification["candidate_urls"],
        }
    except Exception as exc:  # noqa: BLE001
        incident_id = "ingress_" + hashlib.sha256(f"{message_id}:{exc.__class__.__name__}".encode()).hexdigest()[:16]
        return {
            "created": False,
            "status": "CAPTURE_FAILED",
            "incident_id": incident_id,
            "error_type": exc.__class__.__name__,
        }


def enqueue_ack(*, task_id: str, chat_id: str, message_id: str, text: str) -> str:
    from v3_runtime import enqueue_outbox

    return enqueue_outbox(
        object_type="ingress_ack",
        object_id=task_id,
        destination="feishu_chat",
        payload={
            "task_id": task_id,
            "chat_id": chat_id,
            "source_message_id": message_id,
            "text": text,
            "profile_id": "edu_live_competitor_intel",
        },
    )


def record_gateway_health(*, connected: bool, mode: str, app_id: str) -> None:
    from v3_runtime import upsert_heartbeat

    upsert_heartbeat(
        "feishu-ingress-v3",
        "READY" if connected else "RECONNECTING",
        {"connection_mode": mode, "websocket_connected": connected, "app_id": app_id, "profile_id": "edu_live_competitor_intel"},
        success=connected,
    )


def finish_ack(*, outbox_id: str, success: bool, message_id: str | None = None, error: str | None = None) -> None:
    from v3_runtime import complete_outbox, retry_outbox
    if success:
        complete_outbox(outbox_id, {"status": "VERIFIED", "profile_id": "edu_live_competitor_intel", "app_id": "cli_a978a6e73f785cc5", "message_id": message_id})
    else:
        retry_outbox(outbox_id, error_type="FEISHU_ACK_FAILED", error_message=error or "immediate acknowledgement failed", retry_after_seconds=0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("capture", "finish_ack", "health", "classify"))
    args = parser.parse_args()
    body = json.load(sys.stdin)
    if args.action == "capture":
        result = handle_inbound_message(**body)
    elif args.action == "classify":
        result = classify(str(body.get("content") or ""))
    elif args.action == "finish_ack":
        finish_ack(**body)
        result = {"status": "OK"}
    else:
        record_gateway_health(**body)
        result = {"status": "OK"}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
