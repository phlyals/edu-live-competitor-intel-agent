"""Immutable qualification contract shared by transcript, analysis and delivery."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

TRANSCRIPT_SCOPE_FULL = "FULL_SESSION"
TRANSCRIPT_SCOPE_SAMPLE = "SAMPLE"
ANALYSIS_SCOPE_FORMAL = "FORMAL_SINGLE_SESSION"
ANALYSIS_SCOPE_SAMPLE = "SAMPLE_AUXILIARY"
QUALIFIED = "FULL_SESSION_QUALIFIED"
SAMPLE_NONQUALIFYING = "SAMPLE_NONQUALIFYING"
SOURCE_NONQUALIFYING = "SOURCE_NONQUALIFYING"
ANALYSIS_SPEC_VERSION = "single-session-evidence-v4-source-ids"
PROMPT_VERSION = "deepseek-source-segment-ids-v1"
MODEL_VERSION = "deepseek-chat"
MIN_TIMESTAMP_COVERAGE_RATE = 0.90


def json_object(value) -> dict:
    try:
        parsed = json.loads(value or "{}") if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
