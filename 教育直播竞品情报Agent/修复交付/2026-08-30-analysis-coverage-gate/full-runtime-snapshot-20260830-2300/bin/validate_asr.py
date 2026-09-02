#!/usr/bin/env python3
"""Validate the local Chinese ASR pipeline with macOS synthetic speech only."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from runtime_common import ROOT, utc_now


SENTENCE = "这是直播竞品情报主管的中文转录能力测试。"
KEYWORDS = ("直播", "竞品", "中文", "转录", "测试")


def main() -> int:
    artifacts = ROOT / "test_artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    aiff = artifacts / "chinese_asr_validation.aiff"
    wav = artifacts / "chinese_asr_validation.wav"
    transcript_path = artifacts / "chinese_asr_validation.transcript.json"
    evidence_path = artifacts / "chinese_asr_validation.json"
    subprocess.run(["/usr/bin/say", "-v", "Tingting", "-o", str(aiff), SENTENCE], check=True, timeout=30)
    subprocess.run(["/opt/homebrew/bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", str(aiff), "-ac", "1", "-ar", "16000", str(wav)], check=True, timeout=30)
    subprocess.run([
        str(ROOT / ".venv" / "bin" / "python"),
        str(ROOT / "bin" / "transcribe.py"),
        "--input", str(wav),
        "--output", str(transcript_path),
        "--model-dir", str(ROOT / "models"),
        "--language", "zh",
    ], check=True, timeout=300)
    transcript = json.loads(transcript_path.read_text(encoding="utf-8"))
    text = "".join(str(segment.get("text", "")) for segment in transcript.get("segments", []))
    normalized = re.sub(r"\s+", "", text)
    hits = [keyword for keyword in KEYWORDS if keyword in normalized]
    passed = len(hits) >= 3
    evidence = {
        "status": "DEGRADED" if passed else "WAITING_TOOL",
        "passed": passed,
        "validation_level": "synthetic_tts",
        "production_ready": False,
        "reason": "Synthetic Chinese speech validates the local pipeline only; real livestream audio still requires an approved test",
        "expected_text": SENTENCE,
        "transcribed_text": text,
        "keyword_hits": hits,
        "keyword_total": len(KEYWORDS),
        "engine": transcript.get("engine"),
        "model_dir": transcript.get("model_dir"),
        "checked_at": utc_now(),
        "business_audio_used": False,
    }
    evidence_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    evidence_path.chmod(0o600)
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
