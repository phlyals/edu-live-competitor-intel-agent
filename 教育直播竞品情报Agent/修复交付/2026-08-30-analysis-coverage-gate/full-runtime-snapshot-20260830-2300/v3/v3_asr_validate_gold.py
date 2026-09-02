#!/usr/bin/env python3
"""Measure current ASR against a separately cleaned historical transcript."""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path


def edit_distance(left: list[str], right: list[str]) -> int:
    previous = list(range(len(right) + 1))
    for i, a in enumerate(left, 1):
        current = [i]
        for j, b in enumerate(right, 1):
            current.append(min(current[-1] + 1, previous[j] + 1, previous[j - 1] + (a != b)))
        previous = current
    return previous[-1]


def clean(value: str) -> str:
    return re.sub(r"[\s\[\]():：，。！？、,.!?;；‘’“”\-—]", "", value or "")


def main() -> int:
    root = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/asr-validation")
    gold_path = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/mvp-e/20260824-225108_陈兴笃学_4c6ce301/transcript_clean.md")
    current_path = Path(os.environ.get("ASR_CURRENT_TRANSCRIPT") or (root / "current-model.transcript.json"))
    gold_text = []
    gold_ends = []
    pattern = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2}):(\d{2})\]\s*(.*)")
    for line in gold_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        end = int(match.group(4)) * 3600 + int(match.group(5)) * 60 + int(match.group(6))
        if end <= 300:
            gold_text.append(match.group(7))
            gold_ends.append(end)
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current_text = [str(row.get("text") or "") for row in current.get("segments") or [] if float(row.get("start") or 0) < 300]
    gold = clean("".join(gold_text))
    predicted = clean("".join(current_text))
    cer = edit_distance(list(predicted), list(gold)) / max(1, len(gold))
    terms = ["直播", "选科", "文科", "理科", "新高考", "家长", "组合", "数学", "专业", "排名", "就业", "物化生"]
    term_hits = [term for term in terms if term in predicted]
    predicted_end = max((float(row.get("end") or 0) for row in current.get("segments") or [] if float(row.get("start") or 0) < 300), default=0)
    gold_end = max(gold_ends or [0])
    metrics = {"cer": cer, "gold_characters": len(gold), "predicted_characters": len(predicted), "term_recall": len(term_hits) / len(terms), "term_hits": term_hits, "term_total": len(terms), "gold_span_seconds": gold_end, "predicted_span_seconds": predicted_end, "span_error_seconds": abs(predicted_end - gold_end), "timestamp_metric": "span_error_only; exact per-segment timestamp alignment still requires human review"}
    report = {"status": "WAITING_HUMAN", "passed": False, "validation_level": "real_livestream_audio_sample_against_separate_cleaned_transcript", "model_name": os.environ.get("ASR_MODEL_NAME") or "faster-whisper-tiny", "production_ready": False, "source_audio": str(root / "human-gold-sample.wav"), "gold_transcript": str(gold_path), "gold_provenance": "historical_cleaned_transcript; independent human attribution is not signed in the artifact", "current_transcript": str(current_path), "accuracy_metrics": metrics, "keyword_hits": term_hits, "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"), "reason": "Metrics are measured, but the cleaned transcript is not an independently signed human gold artifact and timestamp alignment still needs human confirmation."}
    output = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/test_artifacts/chinese_asr_validation.json")
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
