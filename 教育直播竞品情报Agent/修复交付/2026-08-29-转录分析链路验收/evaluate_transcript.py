#!/usr/bin/env python3
"""Offline evaluation artifact; not imported by production code."""
from __future__ import annotations

import argparse
import difflib
import json
import re
import statistics
import unicodedata
from pathlib import Path


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "").lower()
    return "".join(re.findall(r"[\u3400-\u9fffA-Za-z0-9]+", value))


def distance(left: str, right: str) -> int:
    if len(left) < len(right):
        left, right = right, left
    previous = list(range(len(right) + 1))
    for i, a in enumerate(left, 1):
        current = [i]
        for j, b in enumerate(right, 1):
            current.append(min(current[-1] + 1, previous[j] + 1, previous[j - 1] + (a != b)))
        previous = current
    return previous[-1]


def bigrams(value: str) -> set[str]:
    return {value[i:i + 2] for i in range(max(0, len(value) - 1))}


def text_in(rows: list[dict], start: float, end: float) -> str:
    return "".join(str(row.get("normalized_text") or row.get("text") or "") for row in rows
                   if float(row.get("end") or 0) > start and float(row.get("start") or 0) < end)


def merge(rows: list[dict], duration: float) -> tuple[list[list[float]], list[list[float]]]:
    intervals = sorted((max(0.0, float(r["start"])), min(duration, float(r["end"]))) for r in rows
                       if r.get("start") is not None and r.get("end") is not None
                       and 0 <= float(r["start"]) < float(r["end"]) <= duration + .001)
    merged: list[list[float]] = []
    for start, end in intervals:
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    gaps: list[list[float]] = []
    cursor = 0.0
    for start, end in merged:
        if start > cursor:
            gaps.append([cursor, start])
        cursor = end
    if cursor < duration:
        gaps.append([cursor, duration])
    return merged, gaps


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--new", required=True, type=Path)
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--audio-duration", required=True, type=float)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    new = json.loads(args.new.read_text())
    reference = json.loads(args.reference.read_text())
    rows = new.get("segments") or []
    refs = reference.get("segments") or []
    duration = args.audio_duration
    merged, gaps = merge(rows, duration)
    covered = sum(end - start for start, end in merged)
    # Twelve 30-second windows, centered uniformly across the full hour.
    centers = [duration * ((2 * index - 1) / 24) for index in range(1, 13)]
    samples = []
    for index, center in enumerate(centers, 1):
        start, end = max(0, center - 15), min(duration, center + 15)
        ref_raw, new_raw = text_in(refs, start, end), text_in(rows, start, end)
        ref, candidate = normalize(ref_raw), normalize(new_raw)
        edit = distance(ref, candidate) if ref else None
        left, right = bigrams(ref), bigrams(candidate)
        f1 = (2 * len(left & right) / (len(left) + len(right))) if left or right else None
        samples.append({"sample": index, "start": start, "end": end, "reference_text": ref_raw,
                        "new_text": new_raw, "reference_chars": len(ref), "new_chars": len(candidate),
                        "edit_distance": edit, "character_accuracy_vs_reference": max(0, 1 - edit / len(ref)) if ref else None,
                        "sequence_similarity": difflib.SequenceMatcher(None, ref, candidate).ratio() if ref or candidate else None,
                        "bigram_f1": f1})
    valid = [s for s in samples if s["reference_chars"]]
    total_ref = sum(s["reference_chars"] for s in valid)
    total_edit = sum(s["edit_distance"] for s in valid)
    last_end = max((float(r.get("end") or 0) for r in rows), default=0)
    result = {
        "new_transcript": str(args.new), "reference_transcript": str(args.reference),
        "reference_is_human_ground_truth": False,
        "accuracy_label": "agreement_with_prior_full_session_whisper_transcript_not_human_accuracy",
        "audio_duration_seconds": duration, "asr_reported_duration_seconds": new.get("duration"),
        "segment_count": len(rows), "transcript_total_chars": sum(len(str(r.get("text") or "")) for r in rows),
        "last_segment_end_seconds": last_end, "timeline_span_rate": last_end / duration if duration else None,
        "covered_speech_seconds": covered, "timestamp_union_coverage_rate": covered / duration if duration else None,
        "gap_count": len(gaps), "longest_gap_seconds": max((b - a for a, b in gaps), default=0),
        "leading_gap_seconds": gaps[0][1] if gaps and gaps[0][0] == 0 else 0,
        "trailing_gap_seconds": duration - last_end,
        "sample_count": len(valid), "sample_window_seconds": 30,
        "weighted_character_accuracy_vs_reference": max(0, 1 - total_edit / total_ref) if total_ref else None,
        "macro_character_accuracy_vs_reference": statistics.mean(s["character_accuracy_vs_reference"] for s in valid),
        "macro_sequence_similarity": statistics.mean(s["sequence_similarity"] for s in valid),
        "macro_bigram_f1": statistics.mean(s["bigram_f1"] for s in valid if s["bigram_f1"] is not None),
        "samples": samples,
        "interpretation": [
            "Timestamp-union coverage measures timestamped speech, so silence is intentionally counted as a gap.",
            "Timeline span indicates whether ASR reached the end of the complete audio.",
            "Agreement with the prior Whisper transcript is reproducibility evidence, not human-audited accuracy."
        ]
    }
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ["segment_count", "transcript_total_chars", "last_segment_end_seconds",
          "timeline_span_rate", "timestamp_union_coverage_rate", "sample_count", "weighted_character_accuracy_vs_reference",
          "macro_sequence_similarity", "macro_bigram_f1"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
