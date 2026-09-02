#!/usr/bin/env python3
"""Read-only Douyin homepage/live-status probe using upstream streamget."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
from urllib.parse import urlparse

from streamget import DouyinLiveStream

from runtime_common import utc_now


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"LIVE", "OFFLINE_CONFIRMED"} else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    parsed = urlparse(args.url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not (host == "douyin.com" or host.endswith(".douyin.com")):
        return emit({"status": "REJECTED", "reason": "Only Douyin HTTP(S) URLs are accepted"})
    try:
        client = DouyinLiveStream()
        if host == "v.douyin.com" or "/user/" in parsed.path:
            data = asyncio.run(client.fetch_app_stream_data(args.url, process_data=True))
        else:
            data = asyncio.run(client.fetch_web_stream_data(args.url, process_data=True))
        if not isinstance(data, dict):
            raise ValueError("streamget returned no structured room data")
        anchor = data.get("anchor_name") or (data.get("owner") or {}).get("nickname")
        title = data.get("title")
        room_id = data.get("room_id") or data.get("id_str") or data.get("web_rid")
        raw_status = data.get("status")
        # streamget's Douyin resolver explicitly documents 2 as live and 4 as
        # offline.  Any other status (including a risk-control/intermediate
        # response) is not evidence of either state and must stay UNKNOWN.
        try:
            raw_status = int(raw_status)
        except (TypeError, ValueError):
            raw_status = None
        is_live = bool(data.get("is_live")) or raw_status == 2
        identity_confirmed = bool(anchor or room_id)
        if not identity_confirmed:
            return emit({
                "status": "UNKNOWN",
                "checked_at": utc_now(),
                "reason": "streamget did not return an anchor name or room identifier; refusing to claim offline",
                "upstream_version": importlib.metadata.version("streamget"),
                "recording_started": False,
            })
        resolved_status = "LIVE" if is_live else "OFFLINE_CONFIRMED" if raw_status == 4 else "UNKNOWN"
        return emit({
            "status": resolved_status,
            "checked_at": utc_now(),
            "anchor_name": anchor,
            "title": title,
            "room_id": str(room_id) if room_id else None,
            "platform_user_id": str((data.get("owner") or {}).get("id_str") or (data.get("owner") or {}).get("id") or "") or None,
            "sec_uid": (data.get("owner") or {}).get("sec_uid"),
            "unique_id": (data.get("owner") or {}).get("unique_id"),
            "web_rid": (data.get("owner") or {}).get("web_rid"),
            "upstream_version": importlib.metadata.version("streamget"),
            "recording_started": False,
            "raw_status": raw_status,
            "reason": None if resolved_status in {"LIVE", "OFFLINE_CONFIRMED"} else "streamget returned an unrecognized live status",
        })
    except Exception as exc:
        return emit({
            "status": "UNKNOWN",
            "checked_at": utc_now(),
            "reason": str(exc),
            "upstream_version": importlib.metadata.version("streamget"),
            "recording_started": False,
        })


if __name__ == "__main__":
    raise SystemExit(main())
