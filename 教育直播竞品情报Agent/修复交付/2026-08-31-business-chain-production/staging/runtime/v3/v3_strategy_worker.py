#!/usr/bin/env python3
"""Create evidence-bound strategy candidates that always require review."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from v3_business_chain import (
    STRATEGY_SPEC_VERSION,
    artifact_references,
    load_bound_artifact,
    sha256_file,
    stable_id,
    write_json_atomic,
)
from v3_runtime import connect, init_db, utc_now


STRATEGY_ROOT = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/strategy-candidates"
)


def once(*, connect_fn=None, init_db_fn=None) -> dict:
    connection_factory = connect_fn or connect
    (init_db_fn or init_db)()
    with connection_factory() as conn:
        activation_rows = [dict(row) for row in conn.execute(
            "SELECT o.*,v.content_path AS version_path,v.content_hash AS version_hash,"
            "v.version_no,v.activation_count FROM version_observations o "
            "JOIN strategy_versions v ON v.version_id=o.active_version_id "
            "WHERE o.observation_state IN ('ACTIVE_CONFIRMED','HISTORICAL_RESTORED') "
            "ORDER BY o.ended_at,o.observation_id"
        ).fetchall()]
    created = skipped_evidence = 0
    for observation in activation_rows:
        source_digest = stable_id(
            "strategy_source_", observation["version_id"] if "version_id" in observation else observation["active_version_id"],
            observation["observation_id"], str(observation["activation_count"]),
            STRATEGY_SPEC_VERSION, length=64,
        )
        candidate_id = stable_id(
            "strategy_candidate_", observation["competitor_id"],
            observation["active_version_id"], observation["observation_id"],
            STRATEGY_SPEC_VERSION,
        )
        with connection_factory() as conn:
            if conn.execute(
                "SELECT 1 FROM strategy_candidates WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone():
                continue
            support = [dict(row) for row in conn.execute(
                "SELECT o.*,a.transcript_id,a.output_path,a.artifact_digest,"
                "a.session_id FROM version_observations o JOIN analyses a "
                "ON a.analysis_id=o.analysis_id WHERE o.competitor_id=? "
                "AND o.structure_digest=? AND o.ended_at<=? "
                "ORDER BY o.ended_at DESC,o.analysis_id DESC LIMIT 3",
                (
                    observation["competitor_id"], observation["structure_digest"],
                    observation["ended_at"],
                ),
            ).fetchall()]
            base = conn.execute(
                "SELECT * FROM knowledge_versions WHERE object_key=? "
                "AND status='APPROVED' ORDER BY version_no DESC LIMIT 1",
                (observation["competitor_id"],),
            ).fetchone()
        if len({row["session_id"] for row in support}) < 3:
            skipped_evidence += 1
            continue
        evidence_rows = []
        for row in support:
            artifact = load_bound_artifact(row)
            for ref in artifact_references(artifact):
                evidence_rows.append({
                    "analysis_id": row["analysis_id"],
                    "session_id": row["session_id"],
                    "transcript_id": row["transcript_id"],
                    **ref,
                })
        if not evidence_rows:
            skipped_evidence += 1
            continue
        version_path = Path(observation["version_path"])
        if (
            not version_path.is_file()
            or sha256_file(version_path) != observation["version_hash"]
        ):
            skipped_evidence += 1
            continue
        candidate = {
            "candidate_id": candidate_id,
            "strategy_spec_version": STRATEGY_SPEC_VERSION,
            "competitor_id": observation["competitor_id"],
            "version_id": observation["active_version_id"],
            "version_no": observation["version_no"],
            "activation_count": observation["activation_count"],
            "observation_id": observation["observation_id"],
            "comparison_id": observation.get("comparison_id"),
            "structure_digest": observation["structure_digest"],
            "supporting_sessions": [
                {
                    "session_id": row["session_id"],
                    "analysis_id": row["analysis_id"],
                    "artifact_digest": row["artifact_digest"],
                }
                for row in reversed(support)
            ],
            "recommendation": "该结构已连续三场出现；仅作为人工评审候选，不自动写入正式知识。",
            "status": "PENDING_REVIEW",
            "created_at": utc_now(),
        }
        candidate_path = STRATEGY_ROOT / f"{candidate_id}.json"
        candidate_digest = write_json_atomic(candidate_path, candidate)
        proposed_version_no = int(base["version_no"] if base else 0) + 1
        diff = {
            "candidate_id": candidate_id,
            "competitor_id": observation["competitor_id"],
            "strategy_version_id": observation["active_version_id"],
            "base_knowledge_version_id": base["version_id"] if base else None,
            "base_content_hash": base["content_hash"] if base else None,
            "proposed_version_no": proposed_version_no,
            "candidate_digest": candidate_digest,
            "approval_required": True,
            "created_at": utc_now(),
        }
        diff_id = stable_id(
            "knowledge_diff_", candidate_id, candidate_digest,
            str(base["version_id"] if base else "NONE"),
        )
        diff_path = STRATEGY_ROOT / "diffs" / f"{diff_id}.json"
        diff_hash = write_json_atomic(diff_path, diff)
        review_id = stable_id("review_strategy_", candidate_id, candidate_digest)
        with connection_factory() as conn:
            conn.execute("BEGIN IMMEDIATE")
            inserted = conn.execute(
                "INSERT OR IGNORE INTO strategy_candidates("
                "candidate_id,session_id,analysis_id,strategy_type,status,source_digest,"
                "content_path,lineage_state,competitor_id,version_id,comparison_id,"
                "candidate_digest,evidence_json,updated_at,created_at,metadata_json) "
                "VALUES(?,?,?,'competitor_structure','PENDING_REVIEW',?,?,'CURRENT',"
                "?,?,?,?,?,?,?,?)",
                (
                    candidate_id, observation["session_id"], observation["analysis_id"],
                    source_digest, str(candidate_path), observation["competitor_id"],
                    observation["active_version_id"], observation.get("comparison_id"),
                    candidate_digest, json.dumps({
                        "supporting_session_count": 3,
                        "reference_count": len(evidence_rows),
                    }, ensure_ascii=False, sort_keys=True), utc_now(), candidate["created_at"],
                    json.dumps({
                        "strategy_spec_version": STRATEGY_SPEC_VERSION,
                        "approval_version": candidate_digest,
                        "auto_publish": False,
                        "observation_state": observation["observation_state"],
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            if int(inserted.rowcount or 0) != 1:
                conn.rollback()
                continue
            for item in evidence_rows:
                conn.execute(
                    "INSERT INTO strategy_evidence(candidate_id,comparison_id,analysis_id,"
                    "session_id,transcript_id,source_segment_id,content_digest,"
                    "start_seconds,end_seconds) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        candidate_id, observation.get("comparison_id"),
                        item["analysis_id"], item["session_id"], item["transcript_id"],
                        item["source_segment_id"], item["content_digest"],
                        item["start"], item["end"],
                    ),
                )
            conn.execute(
                "INSERT INTO knowledge_diffs(diff_id,candidate_id,base_version_id,"
                "proposed_version_no,status,diff_path,diff_hash,competitor_id,version_id,"
                "candidate_digest,updated_at,created_at,metadata_json) "
                "VALUES(?,?,?,?,'PENDING_REVIEW',?,?,?,?,?,?,?,?)",
                (
                    diff_id, candidate_id, base["version_id"] if base else None,
                    proposed_version_no, str(diff_path), diff_hash,
                    observation["competitor_id"], observation["active_version_id"],
                    candidate_digest, utc_now(), diff["created_at"],
                    json.dumps({"approval_version": candidate_digest}, sort_keys=True),
                ),
            )
            conn.execute(
                "INSERT INTO review_items(review_id,object_type,object_id,review_type,"
                "status,requested_at,requested_by,metadata_json) VALUES(?,"
                "'strategy_candidate',?,'STRATEGY_AND_KNOWLEDGE_DIFF','PENDING',?,?,?)",
                (
                    review_id, candidate_id, utc_now(), "strategy-v3",
                    json.dumps({
                        "candidate_digest": candidate_digest,
                        "diff_id": diff_id, "diff_hash": diff_hash,
                        "approval_command_version": candidate_digest,
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()
        created += 1
    return {
        "created": created, "activation_observations": len(activation_rows),
        "skipped_evidence": skipped_evidence, "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
