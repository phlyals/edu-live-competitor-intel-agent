#!/usr/bin/env python3
"""Propagate STALE lineage and create deterministic recompute work items."""
from __future__ import annotations
import argparse, json, signal, time
from v3_runtime import connect, init_db, upsert_heartbeat, utc_now

RUNNING = True

def stop(*_args):
    global RUNNING
    RUNNING = False

def once():
    with connect() as conn:
        stale = conn.execute("SELECT downstream_type,downstream_id FROM lineage_edges WHERE state='STALE'").fetchall()
        for row in stale:
            if row["downstream_type"] == "analysis":
                conn.execute("UPDATE analyses SET status='STALE',lineage_state='STALE' WHERE analysis_id=?", (row["downstream_id"],))
            conn.execute("INSERT OR IGNORE INTO review_items(review_id,object_type,object_id,review_type,status,requested_at,metadata_json) VALUES(?,?,?,?,?,?,?)", ("review_stale_" + row["downstream_type"] + row["downstream_id"], row["downstream_type"], row["downstream_id"], "STALE_RECOMPUTE", "PENDING", utc_now(), json.dumps({"reason":"upstream lineage changed"}, ensure_ascii=False)))
        conn.commit()
    result = {"stale_items": len(stale), "checked_at": utc_now()}
    upsert_heartbeat("recompute-v3", "READY", result)
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once", "daemon"))
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()
    init_db()
    if args.command == "once":
        print(json.dumps(once(), ensure_ascii=False, indent=2))
        return 0
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    while RUNNING:
        once(); time.sleep(max(15, args.interval))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
