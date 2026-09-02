#!/usr/bin/env python3
"""Nonce-bound, asynchronous approval transitions for Runtime V3."""
from __future__ import annotations
import argparse, hashlib, json, secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from v3_runtime import connect, init_db, load_config, utc_now

def create(object_type: str, object_id: str, version: str = "") -> dict:
    nonce = secrets.token_urlsafe(24)
    aid = "approval_" + hashlib.sha256((object_type + object_id + version).encode()).hexdigest()[:24]
    expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with connect() as conn:
        conn.execute("INSERT INTO approvals(approval_id,object_type,object_id,requested_version,decision,nonce_hash,metadata_json) VALUES(?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,requested_version) DO NOTHING", (aid, object_type, object_id, version, "PENDING", hashlib.sha256(nonce.encode()).hexdigest(), json.dumps({"nonce_created_at": utc_now(), "expires_at": expires, "single_use": True}, ensure_ascii=False)))
        existing = conn.execute("SELECT approval_id,decision FROM approvals WHERE object_type=? AND object_id=? AND requested_version=?", (object_type, object_id, version)).fetchone()
        conn.commit()
    return {"approval_id": existing["approval_id"] if existing else aid, "nonce": nonce, "status": existing["decision"] if existing else "PENDING", "expires_at": expires}

def decide(approval_id: str, nonce: str, decision: str, actor: str, notes: str = "") -> dict:
    if decision not in {"APPROVED", "REJECTED"}:
        raise RuntimeError("decision must be APPROVED or REJECTED")
    allowed = set((load_config().get("allowed_approver_open_ids") or []))
    if actor not in allowed:
        raise RuntimeError("approver is not allowed for this profile")
    nonce_hash = hashlib.sha256(nonce.encode()).hexdigest()
    with connect() as conn:
        row = conn.execute("SELECT * FROM approvals WHERE approval_id=? AND nonce_hash=?", (approval_id, nonce_hash)).fetchone()
        if not row:
            raise RuntimeError("approval missing or nonce invalid")
        if row["decision"] != "PENDING":
            raise RuntimeError("approval is already decided")
        metadata = json.loads(row["metadata_json"] or "{}")
        expires_at = metadata.get("expires_at")
        if expires_at and datetime.fromisoformat(str(expires_at).replace("Z", "+00:00")) <= datetime.now(timezone.utc):
            raise RuntimeError("approval nonce has expired")
        metadata["nonce_used_at"] = utc_now()
        conn.execute("UPDATE approvals SET decision=?,decided_by=?,decided_at=?,notes=?,metadata_json=? WHERE approval_id=? AND decision='PENDING'", (decision, actor, utc_now(), notes, json.dumps(metadata, ensure_ascii=False), approval_id))
        conn.commit()
    return {"approval_id": approval_id, "decision": decision, "decided_by": actor}

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    create_parser = sub.add_parser("create"); create_parser.add_argument("object_type"); create_parser.add_argument("object_id"); create_parser.add_argument("--version", default="")
    decide_parser = sub.add_parser("decide"); decide_parser.add_argument("approval_id"); decide_parser.add_argument("nonce"); decide_parser.add_argument("decision"); decide_parser.add_argument("actor"); decide_parser.add_argument("--notes", default="")
    args = parser.parse_args(); init_db()
    result = create(args.object_type, args.object_id, args.version) if args.command == "create" else decide(args.approval_id, args.nonce, args.decision, args.actor, args.notes)
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
