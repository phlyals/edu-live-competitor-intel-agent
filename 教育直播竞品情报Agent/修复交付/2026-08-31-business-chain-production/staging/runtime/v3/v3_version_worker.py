#!/usr/bin/env python3
"""Confirm strategy structures only after three consecutive qualified sessions."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from v3_business_chain import (
    VERSION_SPEC_VERSION,
    load_bound_artifact,
    qualified_analysis_rows,
    stable_id,
    structure_digest,
    structure_features,
    write_json_atomic,
)
from v3_runtime import connect, init_db, utc_now


VERSION_ROOT = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/strategy-versions"
)
MIN_CONFIRMATIONS = 3


def once(*, connect_fn=None, init_db_fn=None) -> dict:
    connection_factory = connect_fn or connect
    (init_db_fn or init_db)()
    with connection_factory() as conn:
        rows = qualified_analysis_rows(conn)
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["competitor_id"]].append(row)
    observations = activated = restored = experiments = stable = 0
    for competitor_id, values in grouped.items():
        for row in values:
            with connection_factory() as conn:
                if conn.execute(
                    "SELECT 1 FROM version_observations WHERE competitor_id=? "
                    "AND analysis_id=?", (competitor_id, row["analysis_id"]),
                ).fetchone():
                    continue
                previous = conn.execute(
                    "SELECT * FROM version_observations WHERE competitor_id=? "
                    "ORDER BY ended_at DESC,analysis_id DESC LIMIT 1",
                    (competitor_id,),
                ).fetchone()
                active = conn.execute(
                    "SELECT * FROM strategy_versions WHERE competitor_id=? "
                    "AND status='ACTIVE' ORDER BY version_no DESC LIMIT 1",
                    (competitor_id,),
                ).fetchone()
            artifact = load_bound_artifact(row)
            current_digest = structure_digest(artifact)
            previous_digest = previous["structure_digest"] if previous else None
            consecutive = (
                int(previous["consecutive_count"] or 0) + 1
                if previous and previous_digest == current_digest else 1
            )
            state = "EXPERIMENT"
            active_version_id = active["version_id"] if active else None
            version_payload = None
            version_id = None
            existing_version = None
            if active and active["structure_digest"] == current_digest:
                state = "STABLE"
                stable += 1
            elif consecutive >= MIN_CONFIRMATIONS:
                with connection_factory() as conn:
                    existing_version = conn.execute(
                        "SELECT * FROM strategy_versions WHERE competitor_id=? "
                        "AND structure_digest=?",
                        (competitor_id, current_digest),
                    ).fetchone()
                    recent = [dict(item) for item in conn.execute(
                        "SELECT session_id,analysis_id,ended_at FROM version_observations "
                        "WHERE competitor_id=? AND structure_digest=? "
                        "ORDER BY ended_at DESC,analysis_id DESC LIMIT ?",
                        (competitor_id, current_digest, MIN_CONFIRMATIONS - 1),
                    ).fetchall()]
                    next_version_no = int(conn.execute(
                        "SELECT COALESCE(max(version_no),0)+1 FROM strategy_versions "
                        "WHERE competitor_id=?", (competitor_id,),
                    ).fetchone()[0])
                support = list(reversed(recent)) + [{
                    "session_id": row["session_id"],
                    "analysis_id": row["analysis_id"],
                    "ended_at": row["ended_at"],
                }]
                version_id = (
                    existing_version["version_id"] if existing_version else
                    stable_id("strategy_version_", competitor_id, current_digest)
                )
                version_no = (
                    int(existing_version["version_no"])
                    if existing_version else next_version_no
                )
                state = (
                    "HISTORICAL_RESTORED" if existing_version
                    else "ACTIVE_CONFIRMED"
                )
                version_payload = {
                    "version_id": version_id,
                    "version_spec_version": VERSION_SPEC_VERSION,
                    "competitor_id": competitor_id,
                    "version_no": version_no,
                    "structure_digest": current_digest,
                    "structure_features": structure_features(artifact),
                    "supporting_sessions": support,
                    "activation_state": state,
                    "activated_at": utc_now(),
                }
                active_version_id = version_id
            else:
                experiments += 1

            version_path = None
            version_hash = None
            if version_payload:
                version_path = (
                    VERSION_ROOT / "activations" /
                    f"{version_id}.activation-{int(existing_version['activation_count']) + 1}.json"
                    if existing_version else VERSION_ROOT / f"{version_id}.json"
                )
                version_hash = write_json_atomic(version_path, version_payload)
            observation_id = stable_id(
                "version_observation_", competitor_id, row["analysis_id"],
                row["artifact_digest"], VERSION_SPEC_VERSION,
            )
            with connection_factory() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if version_payload:
                    now = utc_now()
                    if existing_version:
                        conn.execute(
                            "UPDATE strategy_versions SET status='SUPERSEDED',"
                            "superseded_at=? WHERE competitor_id=? AND status='ACTIVE' "
                            "AND version_id<>?",
                            (now, competitor_id, version_id),
                        )
                        conn.execute(
                            "UPDATE strategy_versions SET status='ACTIVE',last_session_id=?,"
                            "supporting_session_count=?,activation_count=activation_count+1,"
                            "activated_at=?,superseded_at=NULL,restored_from_version_id=?,"
                            "metadata_json=? WHERE version_id=?",
                            (
                                row["session_id"], len(version_payload["supporting_sessions"]),
                                now, version_id,
                                json.dumps({
                                    "last_activation_state": state,
                                    "supporting_sessions": version_payload["supporting_sessions"],
                                    "last_activation_artifact": str(version_path),
                                    "last_activation_artifact_hash": version_hash,
                                }, ensure_ascii=False, sort_keys=True), version_id,
                            ),
                        )
                        restored += 1
                    else:
                        conn.execute(
                            "UPDATE strategy_versions SET status='SUPERSEDED',"
                            "superseded_at=? WHERE competitor_id=? AND status='ACTIVE'",
                            (now, competitor_id),
                        )
                        conn.execute(
                            "INSERT INTO strategy_versions(version_id,competitor_id,version_no,"
                            "structure_digest,status,supporting_session_count,first_session_id,"
                            "last_session_id,activation_count,content_path,content_hash,activated_at,"
                            "created_at,metadata_json) VALUES(?,?,?,?, 'ACTIVE',?,?,?,?,?,?,?,?,?)",
                            (
                                version_id, competitor_id, version_payload["version_no"],
                                current_digest, len(version_payload["supporting_sessions"]),
                                version_payload["supporting_sessions"][0]["session_id"],
                                row["session_id"], 1, str(version_path), version_hash,
                                now, now, json.dumps({
                                    "version_spec_version": VERSION_SPEC_VERSION,
                                    "supporting_sessions": version_payload["supporting_sessions"],
                                }, ensure_ascii=False, sort_keys=True),
                            ),
                        )
                        activated += 1
                comparison = conn.execute(
                    "SELECT comparison_id FROM comparisons WHERE competitor_id=? "
                    "AND newer_analysis_id=? AND status='COMPLETE' "
                    "ORDER BY created_at DESC LIMIT 1",
                    (competitor_id, row["analysis_id"]),
                ).fetchone()
                conn.execute(
                    "INSERT INTO version_observations(observation_id,competitor_id,"
                    "session_id,analysis_id,comparison_id,ended_at,structure_digest,"
                    "previous_structure_digest,consecutive_count,observation_state,"
                    "active_version_id,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        observation_id, competitor_id, row["session_id"],
                        row["analysis_id"], comparison["comparison_id"] if comparison else None,
                        row["ended_at"], current_digest, previous_digest, consecutive,
                        state, active_version_id, utc_now(), json.dumps({
                            "analysis_artifact_digest": row["artifact_digest"],
                            "version_spec_version": VERSION_SPEC_VERSION,
                            "minimum_confirmations": MIN_CONFIRMATIONS,
                        }, ensure_ascii=False, sort_keys=True),
                    ),
                )
                conn.commit()
            observations += 1
    return {
        "observations": observations, "activated": activated,
        "restored": restored, "experiments": experiments, "stable": stable,
        "eligible_competitors": len(grouped), "checked_at": utc_now(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
