#!/usr/bin/env python3
"""Deliver explicitly approved Runtime outbox items to the configured Feishu Base."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from runtime_common import connect_db, load_config, utc_now

PROFILE_ID = "edu_live_competitor_intel"


READONLY_FIELD_TYPES = {"auto_number", "lookup", "formula", "created_at", "updated_at", "created_by", "updated_by", "attachment"}


def emit(payload: dict, code: int = 0) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return code


def run_lark(argv: list[str]) -> dict:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    text = result.stdout.strip() or result.stderr.strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = {"ok": False, "error": {"type": "invalid_cli_output", "message": text[-1000:]}}
    if result.returncode != 0 and payload.get("ok") is not False:
        payload = {"ok": False, "error": {"type": "cli_exit", "message": text[-1000:], "exit_code": result.returncode}}
    return payload


def field_schema(config: dict, table_id: str) -> tuple[dict, dict]:
    feishu = config["feishu"]
    argv = [
        feishu["lark_cli"], "base", "+field-list", "--profile", PROFILE_ID,
        "--base-token", feishu["base_token"],
        "--table-id", table_id,
        "--limit", "200", "--as", "bot", "--format", "json",
    ]
    payload = run_lark(argv)
    if not payload.get("ok"):
        return {}, payload
    fields = ((payload.get("data") or {}).get("fields") or [])
    mapping: dict[str, dict] = {}
    for field in fields:
        mapping[str(field.get("name"))] = field
        mapping[str(field.get("id"))] = field
    return mapping, payload


def validate_fields(mapping: dict[str, dict], fields: dict) -> list[str]:
    errors = []
    for key in fields:
        field = mapping.get(key)
        if not field:
            errors.append(f"Unknown field: {key}")
        elif field.get("type") in READONLY_FIELD_TYPES:
            errors.append(f"Field is read-only or requires a dedicated attachment API: {key}")
    return errors


def validate_config(config: dict) -> dict:
    tables = (config.get("feishu") or {}).get("tables") or {}
    results = {}
    ok = True
    for name, table_id in tables.items():
        mapping, payload = field_schema(config, table_id)
        table_ok = bool(mapping) and bool(payload.get("ok"))
        ok = ok and table_ok
        results[name] = {"table_id": table_id, "ok": table_ok, "field_count": len({field.get("id") for field in mapping.values() if field.get("id")})}
        if not table_ok:
            results[name]["error"] = payload.get("error")
    return {"ok": ok, "status": "READY" if ok else "WAITING_TOOL", "identity": "bot", "tables": results, "writes_performed": 0}


def process_once(config: dict, dry_run: bool) -> dict:
    if not (config.get("safety") or {}).get("feishu_business_write", False):
        return {"ok": False, "status": "DISABLED", "reason": "Feishu business-write safety gate is closed", "writes_performed": 0}
    with connect_db() as conn:
        row = conn.execute(
            "SELECT outbox_id,object_type,object_id,destination,attempts,payload_json FROM outbox WHERE status IN ('PENDING','RETRY') ORDER BY rowid LIMIT 1"
        ).fetchone()
    if not row:
        return {"ok": True, "status": "READY", "message": "No pending outbox item", "writes_performed": 0}
    outbox_id, object_type, object_id, destination, attempts, payload_json = row
    try:
        body = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        return {"ok": False, "status": "ERROR", "outbox_id": outbox_id, "reason": f"Invalid outbox payload: {exc}", "writes_performed": 0}
    table_name = body.get("table")
    fields = body.get("fields")
    allowed_tables = (config.get("feishu") or {}).get("tables") or {}
    table_id = allowed_tables.get(table_name)
    if destination != "feishu_base" or not table_id or not isinstance(fields, dict) or not fields:
        return {"ok": False, "status": "ERROR", "outbox_id": outbox_id, "reason": "Outbox destination, table, or fields are not allowlisted", "writes_performed": 0}
    mapping, schema_payload = field_schema(config, table_id)
    if not schema_payload.get("ok"):
        return {"ok": False, "status": "WAITING_TOOL", "outbox_id": outbox_id, "reason": schema_payload.get("error"), "writes_performed": 0}
    errors = validate_fields(mapping, fields)
    if errors:
        return {"ok": False, "status": "ERROR", "outbox_id": outbox_id, "reason": errors, "writes_performed": 0}
    plan = {"outbox_id": outbox_id, "object_type": object_type, "object_id": object_id, "table": table_name, "table_id": table_id, "record_id": body.get("record_id"), "fields": fields}
    if dry_run:
        return {"ok": True, "status": "READY", "dry_run": True, "plan": plan, "writes_performed": 0}
    feishu = config["feishu"]
    argv = [feishu["lark_cli"], "base", "+record-upsert", "--profile", PROFILE_ID, "--base-token", feishu["base_token"], "--table-id", table_id, "--json", json.dumps(fields, ensure_ascii=False), "--as", "bot", "--format", "json"]
    if body.get("record_id"):
        argv.extend(["--record-id", str(body["record_id"])])
    response = run_lark(argv)
    timestamp = utc_now()
    with connect_db() as conn:
        if response.get("ok"):
            conn.execute("UPDATE outbox SET status='SENT', attempts=?, last_attempt_at=?, last_error=NULL WHERE outbox_id=?", (attempts + 1, timestamp, outbox_id))
        else:
            error = json.dumps(response.get("error"), ensure_ascii=False)[:2000]
            conn.execute("UPDATE outbox SET status='RETRY', attempts=?, last_attempt_at=?, last_error=? WHERE outbox_id=?", (attempts + 1, timestamp, error, outbox_id))
        conn.commit()
    return {"ok": bool(response.get("ok")), "status": "READY" if response.get("ok") else "WAITING_TOOL", "outbox_id": outbox_id, "writes_performed": 1 if response.get("ok") else 0, "response": response}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-config", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.validate_config and not args.once:
        parser.error("choose --validate-config or --once")
    config = load_config()
    payload = validate_config(config) if args.validate_config else process_once(config, args.dry_run)
    return emit(payload, 0 if payload.get("ok") or payload.get("status") == "DISABLED" else 1)


if __name__ == "__main__":
    raise SystemExit(main())
