#!/usr/bin/env python3
"""Create the explicit human-review package required by the ASR release gate."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import connect, init_db, utc_now  # noqa: E402


EVIDENCE = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/test_artifacts/chinese_asr_validation.json")
OUT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/asr-validation/human-review-package.json")


def main() -> int:
    if not EVIDENCE.is_file():
        print(json.dumps({"status": "WAITING_TOOL", "reason": "ASR validation evidence is missing"}, ensure_ascii=False, indent=2))
        return 1
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    metrics = evidence.get("accuracy_metrics") or {}
    package = {
        "status": "PENDING_HUMAN_REVIEW",
        "review_type": "HUMAN_GOLD_AND_TIMESTAMP",
        "source_audio": evidence.get("source_audio"),
        "gold_transcript": evidence.get("gold_transcript"),
        "current_transcript": evidence.get("current_transcript"),
        "model_name": evidence.get("model_name"),
        "accuracy_metrics": metrics,
        "checklist": {"gold_text_attribution_confirmed": False, "per_segment_timestamp_alignment_confirmed": False, "production_acceptance": False},
        "instructions": "人工回听并确认金标准文本来源与逐段时间戳；未完成前不得把ASR标记为生产就绪。",
        "created_at": utc_now(),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(package, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    review_key = "asr-validation:chinese-livestream"
    review_id = "review_" + hashlib.sha256(review_key.encode()).hexdigest()[:24]
    init_db()
    with connect() as conn:
        conn.execute("INSERT INTO review_items(review_id,object_type,object_id,review_type,status,requested_at,requested_by,metadata_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(object_type,object_id,review_type) DO UPDATE SET status='PENDING',requested_at=excluded.requested_at,metadata_json=excluded.metadata_json", (review_id, "ASR_VALIDATION", review_key, "HUMAN_GOLD_AND_TIMESTAMP", "PENDING", package["created_at"], "runtime-v3", json.dumps({"package_path": str(OUT), "evidence_path": str(EVIDENCE), "metrics": metrics}, ensure_ascii=False)))
        conn.commit()
    print(json.dumps({"status": package["status"], "review_id": review_id, "package_path": str(OUT), "metrics": metrics}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
