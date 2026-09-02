#!/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/.venv/bin/python
"""Transcribe one explicit local media file with faster-whisper.

This wrapper is intentionally local-only. It will not download a model, open
a URL, launch a browser, record a stream, or write to Feishu/Obsidian.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def resolve_model_dir(path: Path) -> Path:
    """Accept either a concrete snapshot or the local HF model cache root."""
    if path.is_dir() and (path / "config.json").exists() and (path / "model.bin").exists():
        return path
    snapshots = sorted(path.glob("models--Systran--faster-whisper-*/snapshots/*"))
    usable = [candidate for candidate in snapshots if (candidate / "config.json").exists() and (candidate / "model.bin").exists()]
    if usable:
        return usable[-1]
    return path


def normalize_text(value: str) -> str:
    """Apply lossless text hygiene while preserving the raw ASR evidence."""
    value = unicodedata.normalize("NFKC", value or "")
    value = value.replace("\u200b", "").replace("\ufeff", "")
    return re.sub(r"\s+", " ", value).strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--model-dir", required=True, type=Path)
    parser.add_argument("--language", default=None)
    parser.add_argument("--beam-size", type=int, default=5)
    parser.add_argument("--hotwords", default="", help="comma-separated domain terms used during decoding")
    parser.add_argument("--initial-prompt", default="", help="domain context prompt; does not replace source evidence")
    parser.add_argument("--vad-silence-ms", type=int, default=500, help="minimum VAD silence duration")
    parser.add_argument("--speech-pad-ms", type=int, default=200, help="VAD speech padding")
    parser.add_argument("--condition-on-previous-text", action=argparse.BooleanOptionalAction, default=False)
    args = parser.parse_args()

    source = args.input.expanduser().resolve()
    model_dir = resolve_model_dir(args.model_dir.expanduser().resolve())
    output = args.output.expanduser().resolve()
    if not source.is_file():
        print(json.dumps({"ok": False, "status": "WAITING_TOOL", "reason": f"Input file does not exist: {source}"}, ensure_ascii=False))
        return 2
    if not model_dir.is_dir():
        print(json.dumps({"ok": False, "status": "WAITING_TOOL", "reason": f"Local model directory does not exist: {model_dir}"}, ensure_ascii=False))
        return 2
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        print(json.dumps({"ok": False, "status": "WAITING_TOOL", "reason": f"faster-whisper is unavailable: {exc}"}, ensure_ascii=False))
        return 2

    try:
        model = WhisperModel(str(model_dir), device="cpu", compute_type="int8")
        options = {
            "language": args.language,
            "beam_size": args.beam_size,
            "vad_filter": True,
            "vad_parameters": {"min_silence_duration_ms": max(100, args.vad_silence_ms), "speech_pad_ms": max(0, args.speech_pad_ms)},
            # Independent chunks prevent a low-confidence phrase from
            # contaminating the remainder of a long live session.
            "condition_on_previous_text": bool(args.condition_on_previous_text),
        }
        if args.hotwords.strip():
            options["hotwords"] = args.hotwords.strip()
        if args.initial_prompt.strip():
            options["initial_prompt"] = args.initial_prompt.strip()
        try:
            segments, info = model.transcribe(str(source), **options)
        except TypeError:
            # Older faster-whisper versions do not expose hotwords; preserve
            # the same deterministic transcript path without pretending the
            # dictionary was applied.
            options.pop("hotwords", None)
            segments, info = model.transcribe(str(source), **options)
        rows = []
        low_confidence = 0
        for segment in segments:
            text = (segment.text or "").strip()
            avg_logprob = getattr(segment, "avg_logprob", None)
            no_speech_prob = getattr(segment, "no_speech_prob", None)
            low = bool(avg_logprob is not None and avg_logprob < -1.0)
            if low:
                low_confidence += 1
            rows.append({"start": segment.start, "end": segment.end, "text": text, "normalized_text": normalize_text(text), "avg_logprob": avg_logprob, "no_speech_prob": no_speech_prob, "low_confidence": low})
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = {"ok": True, "status": "READY", "engine": "faster-whisper", "model_dir": str(model_dir), "source_path": str(source), "language": getattr(info, "language", None), "duration": getattr(info, "duration", None), "low_confidence_count": low_confidence, "hotwords": args.hotwords, "initial_prompt": args.initial_prompt, "decode_options": {"beam_size": args.beam_size, "condition_on_previous_text": bool(args.condition_on_previous_text), "vad_filter": True, "vad_parameters": {"min_silence_duration_ms": max(100, args.vad_silence_ms), "speech_pad_ms": max(0, args.speech_pad_ms)}}, "created_at": now(), "segments": rows}
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok": True, "status": "READY", "output": str(output), "segments": len(rows), "low_confidence_count": low_confidence}, ensure_ascii=False))
        return 0
    except Exception as exc:  # fail closed; caller gets a durable error, not a fabricated transcript
        output.parent.mkdir(parents=True, exist_ok=True)
        error = {"ok": False, "status": "PAUSED", "reason": str(exc), "source_path": str(source), "created_at": now()}
        output.write_text(json.dumps(error, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(error, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
