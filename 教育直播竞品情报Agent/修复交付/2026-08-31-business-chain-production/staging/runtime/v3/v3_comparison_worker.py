#!/usr/bin/env python3
"""Build evidence-bound comparisons from the latest two qualified sessions."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from v3_business_chain import (
    COMPARISON_SPEC_VERSION,
    artifact_references,
    canonical_digest,
    compare_features,
    load_bound_artifact,
    qualified_analysis_rows,
    stable_id,
    structure_digest,
    structure_features,
    write_json_atomic,
)
from v3_runtime import connect, init_db, utc_now


COMPARISON_ROOT = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/comparisons"
)


def once(*, connect_fn=None, init_db_fn=None) -> dict:
    connection_factory = connect_fn or connect
    (init_db_fn or init_db)()
    with connection_factory() as conn:
        rows = qualified_analysis_rows(conn)
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["competitor_id"]].append(row)
    created = skipped_insufficient = 0
    for competitor_id, values in grouped.items():
        if len(values) < 2:
            skipped_insufficient += 1
            continue
        older, newer = values[-2], values[-1]
        comparison_id = stable_id(
            "comparison_", competitor_id, older["analysis_id"],
            older["artifact_digest"], newer["analysis_id"],
            newer["artifact_digest"], COMPARISON_SPEC_VERSION,
        )
        with connection_factory() as conn:
            if conn.execute(
                "SELECT 1 FROM comparisons WHERE comparison_id=?",
                (comparison_id,),
            ).fetchone():
                continue
        old_artifact = load_bound_artifact(older)
        new_artifact = load_bound_artifact(newer)
        old_features, new_features = (
            structure_features(old_artifact), structure_features(new_artifact)
        )
        old_structure, new_structure = (
            structure_digest(old_artifact), structure_digest(new_artifact)
        )
        score, changes = compare_features(old_features, new_features)
        old_refs, new_refs = (
            artifact_references(old_artifact), artifact_references(new_artifact)
        )
        artifact = {
            "comparison_id": comparison_id,
            "comparison_spec_version": COMPARISON_SPEC_VERSION,
            "competitor_id": competitor_id,
            "older": {
                "session_id": older["session_id"],
                "analysis_id": older["analysis_id"],
                "transcript_id": older["transcript_id"],
                "artifact_digest": older["artifact_digest"],
                "structure_digest": old_structure,
                "structure_features": old_features,
                "references": old_refs,
            },
            "newer": {
                "session_id": newer["session_id"],
                "analysis_id": newer["analysis_id"],
                "transcript_id": newer["transcript_id"],
                "artifact_digest": newer["artifact_digest"],
                "structure_digest": new_structure,
                "structure_features": new_features,
                "references": new_refs,
            },
            "similarity_state": "SAME" if old_structure == new_structure else "CHANGED",
            "similarity_score": score,
            "changes": changes,
            "created_at": utc_now(),
        }
        output = COMPARISON_ROOT / f"{comparison_id}.json"
        artifact_digest = write_json_atomic(output, artifact)
        with connection_factory() as conn:
            conn.execute("BEGIN IMMEDIATE")
            inserted = conn.execute(
                "INSERT OR IGNORE INTO comparisons("
                "comparison_id,competitor_id,older_session_id,newer_session_id,"
                "older_analysis_id,newer_analysis_id,older_artifact_digest,"
                "newer_artifact_digest,comparison_spec_version,status,similarity_state,"
                "similarity_score,older_structure_digest,newer_structure_digest,"
                "output_path,artifact_digest,scope,qualification_status,created_at,"
                "metadata_json) VALUES(?,?,?,?,?,?,?,?,?,'COMPLETE',?,?,?,?,?,?,"
                "'FULL_SESSION_PAIR','FULL_SESSION_PAIR_QUALIFIED',?,?)",
                (
                    comparison_id, competitor_id, older["session_id"],
                    newer["session_id"], older["analysis_id"], newer["analysis_id"],
                    older["artifact_digest"], newer["artifact_digest"],
                    COMPARISON_SPEC_VERSION, artifact["similarity_state"], score,
                    old_structure, new_structure, str(output), artifact_digest,
                    artifact["created_at"], json.dumps({
                        "older_ended_at": older["ended_at"],
                        "newer_ended_at": newer["ended_at"],
                        "source_analysis_count": 2,
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            if int(inserted.rowcount or 0) != 1:
                conn.rollback()
                continue
            for side, source, references in (
                ("OLDER", older, old_refs), ("NEWER", newer, new_refs)
            ):
                for ref in references:
                    conn.execute(
                        "INSERT INTO comparison_evidence("
                        "comparison_id,side,analysis_id,session_id,transcript_id,"
                        "source_segment_id,content_digest,start_seconds,end_seconds) "
                        "VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            comparison_id, side, source["analysis_id"],
                            source["session_id"], source["transcript_id"],
                            ref["source_segment_id"], ref["content_digest"],
                            ref["start"], ref["end"],
                        ),
                    )
            conn.execute(
                "INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,"
                "manifest_path,manifest_hash,verified_at,scope,qualification_status,"
                "metadata_json) VALUES(?, 'comparison',?,'VERIFIED',?,?,?,?,?,?) "
                "ON CONFLICT(object_type,object_id) DO NOTHING",
                (
                    "bundle:" + comparison_id, comparison_id, str(output),
                    artifact_digest, utc_now(), "FULL_SESSION_PAIR",
                    "FULL_SESSION_PAIR_QUALIFIED",
                    json.dumps({
                        "comparison_spec_version": COMPARISON_SPEC_VERSION,
                        "older_analysis_id": older["analysis_id"],
                        "newer_analysis_id": newer["analysis_id"],
                        "older_artifact_digest": older["artifact_digest"],
                        "newer_artifact_digest": newer["artifact_digest"],
                        "evidence_reference_count": len(old_refs) + len(new_refs),
                    }, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()
        created += 1
    return {
        "created": created,
        "eligible_competitors": len(grouped),
        "skipped_insufficient": skipped_insufficient,
        "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
