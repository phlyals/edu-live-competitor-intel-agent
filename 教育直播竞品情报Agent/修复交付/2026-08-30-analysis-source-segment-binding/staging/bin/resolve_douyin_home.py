#!/usr/bin/env python3
"""Run the upstream Douyin homepage-to-unique-id resolver as a small adapter."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import urlparse


UPSTREAM_ROOT = Path("/Users/mac/AI/apps/hermes/tools/DouyinLiveRecorder")
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "READY" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    parsed = urlparse(args.url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not (host == "douyin.com" or host.endswith(".douyin.com")):
        return emit({"status": "REJECTED", "reason": "Only Douyin HTTP(S) URLs are accepted"})
    try:
        from src.room import get_unique_id  # type: ignore

        unique_id = asyncio.run(get_unique_id(args.url))
        if not unique_id or not all(char.isalnum() or char in "._-" for char in unique_id):
            raise ValueError("Upstream resolver returned an invalid Douyin unique ID")
        return emit({
            "status": "READY",
            "douyin_unique_id": unique_id,
            "monitor_url": f"https://live.douyin.com/{unique_id}",
            "source": "DouyinLiveRecorder.src.room.get_unique_id",
        })
    except Exception as exc:
        return emit({"status": "UNKNOWN", "reason": str(exc)})


if __name__ == "__main__":
    raise SystemExit(main())
