#!/usr/bin/env python3
"""Publish formal knowledge only from explicitly approved strategy diffs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from v3_business_chain import (
    KNOWLEDGE_SPEC_VERSION,
    sha256_file,
    stable_id,
    write_json_atomic,
)
from v3_runtime import connect, init_db, utc_now


KNOWLEDGE_ROOT = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/knowledge"
)


def once(*, connect_fn=None, init_db_fn=None) -> dict:
    connection_factory = connect_fn or connect
    (init_db_fn or init_db)()
    with connection_factory() as conn:
        rows = [dict(row) for row in conn.execute(
            "SELECT p.*,c.competitor_id,c.candidate_digest,c.content_path,"
            "c.metadata_json AS candidate_metadata,"
            "c.status AS candidate_status,c.lineage_state,d.diff_id,d.base_version_id,"
            "d.proposed_version_no,d.status AS diff_status,d.diff_path,d.diff_hash "
            "FROM approvals p JOIN strategy_candidates c "
            "ON p.object_type='strategy_candidate' AND p.object_id=c.candidate_id "
            "JOIN knowledge_diffs d ON d.candidate_id=c.candidate_id "
            "LEFT JOIN knowledge_publish_receipts r ON r.approval_id=p.approval_id "
            "WHERE p.decision='APPROVED' AND r.approval_id IS NULL "
            "ORDER BY p.decided_at,p.approval_id"
        ).fetchall()]
    published = blocked = 0
    for row in rows:
        try:
            approval_metadata = json.loads(row["metadata_json"] or "{}")
            if (
                row["candidate_status"] != "PENDING_REVIEW"
                or row["lineage_state"] != "CURRENT"
                or row["diff_status"] != "PENDING_REVIEW"
                or row["requested_version"] != row["candidate_digest"]
                or not row["decided_by"] or not row["decided_at"]
                or approval_metadata.get("single_use") is not True
                or not approval_metadata.get("nonce_used_at")
            ):
                raise ValueError("approval/candidate/diff identity mismatch")
            candidate_path = Path(str(row["content_path"] or ""))
            diff_path = Path(str(row["diff_path"] or ""))
            if (
                not candidate_path.is_file()
                or sha256_file(candidate_path) != row["candidate_digest"]
                or not diff_path.is_file()
                or sha256_file(diff_path) != row["diff_hash"]
            ):
                raise ValueError("candidate or diff artifact hash mismatch")
            candidate_payload = json.loads(candidate_path.read_text(encoding="utf-8"))
            diff_payload = json.loads(diff_path.read_text(encoding="utf-8"))
            if (
                candidate_payload.get("candidate_id") != row["object_id"]
                or diff_payload.get("candidate_id") != row["object_id"]
                or diff_payload.get("candidate_digest") != row["candidate_digest"]
            ):
                raise ValueError("candidate or diff root identity mismatch")
            with connection_factory() as conn:
                evidence_sessions = int(conn.execute(
                    "SELECT count(DISTINCT session_id) FROM strategy_evidence "
                    "WHERE candidate_id=?", (row["object_id"],),
                ).fetchone()[0])
                current = conn.execute(
                    "SELECT * FROM knowledge_versions WHERE object_key=? "
                    "AND status='APPROVED' ORDER BY version_no DESC LIMIT 1",
                    (row["competitor_id"],),
                ).fetchone()
            if evidence_sessions < 3:
                raise ValueError("strategy candidate has fewer than three evidence sessions")
            current_id = current["version_id"] if current else None
            if current_id != row["base_version_id"]:
                raise ValueError("knowledge diff base is no longer current")
            knowledge_version_id = stable_id(
                "knowledge_version_", row["object_id"], row["candidate_digest"],
                row["approval_id"], KNOWLEDGE_SPEC_VERSION,
            )
            content = {
                "knowledge_version_id": knowledge_version_id,
                "knowledge_spec_version": KNOWLEDGE_SPEC_VERSION,
                "object_key": row["competitor_id"],
                "version_no": int(row["proposed_version_no"]),
                "candidate_id": row["object_id"],
                "candidate_digest": row["candidate_digest"],
                "diff_id": row["diff_id"],
                "diff_hash": row["diff_hash"],
                "approval_id": row["approval_id"],
                "approved_by": row["decided_by"],
                "approved_at": row["decided_at"],
                "supersedes_version_id": current_id,
                "evidence_session_count": evidence_sessions,
                "strategy": candidate_payload,
                "published_at": utc_now(),
            }
            output = KNOWLEDGE_ROOT / row["competitor_id"] / f"{knowledge_version_id}.json"
            content_hash = write_json_atomic(output, content)
            receipt_id = stable_id(
                "knowledge_publish_", row["approval_id"], knowledge_version_id,
                content_hash,
            )
            with connection_factory() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if conn.execute(
                    "SELECT 1 FROM knowledge_publish_receipts WHERE approval_id=?",
                    (row["approval_id"],),
                ).fetchone():
                    conn.rollback()
                    continue
                if current_id:
                    conn.execute(
                        "UPDATE knowledge_versions SET status='SUPERSEDED' "
                        "WHERE version_id=? AND status='APPROVED'",
                        (current_id,),
                    )
                conn.execute(
                    "INSERT INTO knowledge_versions(version_id,object_key,version_no,status,"
                    "content_path,content_hash,supersedes_version_id,approved_by,approved_at,"
                    "created_at,metadata_json) VALUES(?,?,?,'APPROVED',?,?,?,?,?,?,?)",
                    (
                        knowledge_version_id, row["competitor_id"],
                        int(row["proposed_version_no"]), str(output), content_hash,
                        current_id, row["decided_by"], row["decided_at"], utc_now(),
                        json.dumps({
                            "knowledge_spec_version": KNOWLEDGE_SPEC_VERSION,
                            "approval_id": row["approval_id"],
                            "candidate_id": row["object_id"],
                            "diff_id": row["diff_id"],
                            "evidence_session_count": evidence_sessions,
                        }, ensure_ascii=False, sort_keys=True),
                    ),
                )
                try:
                    candidate_metadata = json.loads(row["candidate_metadata"] or "{}")
                except (TypeError, json.JSONDecodeError):
                    candidate_metadata = {}
                candidate_metadata["knowledge_version_id"] = knowledge_version_id
                conn.execute(
                    "UPDATE strategy_candidates SET status='APPROVED',updated_at=?,"
                    "metadata_json=? WHERE candidate_id=? AND status='PENDING_REVIEW'",
                    (
                        utc_now(), json.dumps(
                            candidate_metadata, ensure_ascii=False, sort_keys=True
                        ), row["object_id"],
                    ),
                )
                conn.execute(
                    "UPDATE knowledge_diffs SET status='APPLIED',approval_id=?,updated_at=? "
                    "WHERE diff_id=? AND status='PENDING_REVIEW'",
                    (row["approval_id"], utc_now(), row["diff_id"]),
                )
                conn.execute(
                    "INSERT INTO knowledge_publish_receipts("
                    "publish_receipt_id,approval_id,candidate_id,diff_id,"
                    "knowledge_version_id,object_key,content_hash,status,published_at,"
                    "metadata_json) VALUES(?,?,?,?,?,?,?,'VERIFIED',?,?)",
                    (
                        receipt_id, row["approval_id"], row["object_id"],
                        row["diff_id"], knowledge_version_id, row["competitor_id"],
                        content_hash, utc_now(), json.dumps({
                            "approval_decision": "APPROVED",
                            "approved_by": row["decided_by"],
                            "candidate_digest": row["candidate_digest"],
                            "diff_hash": row["diff_hash"],
                        }, ensure_ascii=False, sort_keys=True),
                    ),
                )
                conn.commit()
            published += 1
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            blocked += 1
    return {
        "published": published, "blocked": blocked,
        "approved_pending": len(rows), "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
