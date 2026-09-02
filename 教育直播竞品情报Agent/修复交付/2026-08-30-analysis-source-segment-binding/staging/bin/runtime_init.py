#!/usr/bin/env python3
"""Initialize the profile-local business Runtime database.

This command only creates empty schema and directories. It never calls a
platform, browser, recorder, Feishu, or knowledge-base API.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "runtime.db"
SCHEMA = ROOT / "schema.sql"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    db = args.db.expanduser().resolve()
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA.read_text(encoding="utf-8"))
        conn.commit()
        tables = conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchone()[0]
    print(json.dumps({"ok": True, "db": str(db), "tables": tables, "initialized_at": now()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
