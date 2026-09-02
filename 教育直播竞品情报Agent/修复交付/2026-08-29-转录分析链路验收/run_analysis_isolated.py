#!/usr/bin/env python3
"""Invoke the existing analysis functions without production database writes."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


V3_ROOT = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3")
sys.path.insert(0, str(V3_ROOT))
import v3_analysis_worker as analysis  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transcript", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    text, all_rows = analysis.transcript_text(args.transcript)
    transcript_payload = json.loads(args.transcript.read_text(encoding="utf-8"))
    result, engine = analysis.request_analysis(text, source_duration=float(
        transcript_payload.get("duration") or max((float(row.get("end") or 0) for row in all_rows), default=0)
    ))
    timestamps = []
    for line in text.splitlines():
        if line.startswith("[") and "]" in line:
            try:
                values = line[1:line.index("]")].split("-", 1)
                timestamps.append((float(values[0]), float(values[1])))
            except (ValueError, IndexError):
                pass
    payload = {
        "analysis_function": "v3_analysis_worker.request_analysis",
        "transcript_text_function": "v3_analysis_worker.transcript_text",
        "production_database_used": False,
        "production_init_db_called": False,
        "input_character_limit": 16000,
        "analysis_input_chars": len(text),
        "analysis_input_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "transcript_total_segments": len(all_rows),
        "analysis_input_first_timestamp": timestamps[0][0] if timestamps else None,
        "analysis_input_last_timestamp": timestamps[-1][1] if timestamps else None,
        "engine": engine,
        "result": result,
    }
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": "COMPLETE", "output": str(args.output),
                      "analysis_input_chars": len(text), "analysis_input_last_timestamp": payload["analysis_input_last_timestamp"],
                      "result_keys": list(result)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
