#!/usr/bin/env python3
"""Verify evidence bundles before any downstream retention or approval."""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
import time
from pathlib import Path

from v3_runtime import connect, init_db, upsert_heartbeat, utc_now

RUNNING = True


def stop(*_args):
    global RUNNING
    RUNNING = False


def once() -> dict:
    verified = blocked = 0
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM evidence_bundles WHERE status IN ('REQUIRED','RETRY')").fetchall()
        for row in rows:
            path = Path(str(row["manifest_path"] or ""))
            if not path.is_file():
                conn.execute("UPDATE evidence_bundles SET status='BLOCKED_EVIDENCE',verified_at=NULL,metadata_json=? WHERE bundle_id=?", (json.dumps({"reason": "manifest_missing", "checked_at": utc_now()}, ensure_ascii=False), row["bundle_id"]))
                blocked += 1
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if row["manifest_hash"] and digest != row["manifest_hash"]:
                conn.execute("UPDATE evidence_bundles SET status='BLOCKED_EVIDENCE',verified_at=NULL,metadata_json=? WHERE bundle_id=?", (json.dumps({"reason": "manifest_hash_mismatch", "actual": digest, "expected": row["manifest_hash"]}, ensure_ascii=False), row["bundle_id"]))
                blocked += 1
                continue
            conn.execute("UPDATE evidence_bundles SET status='VERIFIED',verified_at=?,manifest_hash=?,metadata_json=? WHERE bundle_id=?", (utc_now(), digest, json.dumps({"verified": True, "checked_at": utc_now()}, ensure_ascii=False), row["bundle_id"]))
            verified += 1
        conn.commit()
    result = {"verified": verified, "blocked": blocked, "checked_at": utc_now()}
    upsert_heartbeat("evidence-v3", "READY" if blocked == 0 else "DEGRADED", result, success=blocked == 0)
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
