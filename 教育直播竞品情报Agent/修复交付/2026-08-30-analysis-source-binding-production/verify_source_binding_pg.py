#!/usr/bin/env python3
"""Run the strict source-ID upgrade against a disposable PostgreSQL clone."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg import conninfo, sql
from psycopg.rows import dict_row

STAGE = Path(__file__).parent / "staging" / "runtime" / "v3"
PRODUCTION = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3")
sys.path.insert(0, str(STAGE))
sys.path.append(str(PRODUCTION))

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence
import v3_pipeline_worker as pipeline
import v3_runtime
from v3_db import PostgresConnection
from v3_sample_analysis_migration import migration_health


def dsn_for(base: str, database: str) -> str:
    values = conninfo.conninfo_to_dict(base)
    values["dbname"] = database
    return conninfo.make_conninfo(**values)


def pg_env(base: str, database: str) -> dict[str, str]:
    values = conninfo.conninfo_to_dict(base)
    values["dbname"] = database
    env = os.environ.copy()
    for key, name in {
        "host": "PGHOST", "port": "PGPORT", "user": "PGUSER",
        "dbname": "PGDATABASE", "password": "PGPASSWORD",
    }.items():
        if values.get(key) is not None:
            env[name] = str(values[key])
    return env


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    base = v3_runtime._postgres_dsn()
    source_database = conninfo.conninfo_to_dict(base)["dbname"]
    clone = "edu_source_binding_test_" + uuid.uuid4().hex[:10]
    admin = psycopg.connect(dsn_for(base, "postgres"), autocommit=True)
    artifacts = Path(__file__).parent / "pg-integration-artifacts"
    artifacts.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(artifacts, 0o700)
    report: dict = {"temporary_database": clone, "production_writes": 0}
    with tempfile.TemporaryDirectory(prefix="analysis-coverage-pg-") as directory:
        dump = Path(directory) / "source.dump"
        pg_dump = shutil.which("pg_dump") or "/opt/homebrew/opt/postgresql@16/bin/pg_dump"
        pg_restore = shutil.which("pg_restore") or "/opt/homebrew/opt/postgresql@16/bin/pg_restore"
        try:
            exported = subprocess.run(
                [pg_dump, "--format=custom", "--no-owner", "--file", str(dump)],
                env=pg_env(base, source_database), text=True, capture_output=True,
            )
            if exported.returncode:
                raise RuntimeError("pg_dump failed: " + exported.stderr[-500:])
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(clone)))
            restored = subprocess.run(
                [pg_restore, "--no-owner", "--dbname", clone, str(dump)],
                env=pg_env(base, clone), text=True, capture_output=True,
            )
            if restored.returncode:
                raise RuntimeError("pg_restore failed: " + restored.stderr[-500:])

            test_dsn = dsn_for(base, clone)
            connect = lambda: PostgresConnection(test_dsn)
            with psycopg.connect(test_dsn, row_factory=dict_row) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT output_path FROM analyses WHERE analysis_type='single_session' "
                        "AND status='COMPLETE' AND lineage_state='CURRENT' "
                        "AND scope='FORMAL_SINGLE_SESSION'"
                    )
                    old_path = Path(cursor.fetchone()["output_path"])
            old_state = {"sha256": file_sha(old_path), "mtime_ns": old_path.stat().st_mtime_ns}

            with patch.object(pipeline, "connect", side_effect=connect):
                created = pipeline.create_analysis_tasks()
                replay_created = pipeline.create_analysis_tasks()
            if (created, replay_created) != (1, 0):
                raise RuntimeError(f"unexpected task creation: {created}, {replay_created}")

            model_calls = []

            def strict_response(chunk):
                model_calls.append(chunk["index"])
                payload = analysis.empty_chunk_result()
                payload["course_content"] = [{
                    "summary": "课程内容",
                    "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
                }]
                payload["hook"] = [{
                    "summary": "开场",
                    "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
                }]
                return analysis.validate_chunk_result(payload, chunk), {
                    "response_id": f"clone-{chunk['index']}",
                    "finish_reason": "stop",
                    "usage": {},
                    "attempt": 1,
                    "content_hash": "clone",
                }

            with patch.object(analysis, "connect", side_effect=connect), \
                    patch.object(analysis, "init_db"), \
                    patch.object(analysis, "upsert_heartbeat"), \
                    patch.object(analysis, "ANALYSIS_ROOT", artifacts), \
                    patch.object(
                        analysis, "request_chunk", side_effect=strict_response,
                    ):
                analysis_result = analysis.once()

            with patch.object(evidence, "connect", side_effect=connect), \
                    patch.object(evidence, "init_db"), \
                    patch.object(evidence, "upsert_heartbeat"):
                evidence_result = evidence.once()

            with psycopg.connect(test_dsn, row_factory=dict_row) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT analysis_id,status,lineage_state,scope,qualification_status,"
                        "analysis_spec_version,output_path,artifact_digest,metadata_json "
                        "FROM analyses WHERE analysis_type='single_session' ORDER BY analysis_id"
                    )
                    analyses = [dict(row) for row in cursor.fetchall()]
                    cursor.execute(
                        "SELECT object_id,status,scope,qualification_status,manifest_hash "
                        "FROM evidence_bundles WHERE object_type='analysis' ORDER BY object_id"
                    )
                    bundles = [dict(row) for row in cursor.fetchall()]
                    cursor.execute(
                        "SELECT object_id,status,scope,qualification_status,payload_json "
                        "FROM outbox WHERE object_type='semantic_projection' "
                        "AND status='PENDING' ORDER BY object_id"
                    )
                    pending = [dict(row) for row in cursor.fetchall()]
                    health = migration_health(cursor)

            current = [
                row for row in analyses
                if row["status"] == "COMPLETE" and row["lineage_state"] == "CURRENT"
                and row["analysis_spec_version"] == "single-session-evidence-v4-source-ids"
            ]
            old = [
                row for row in analyses
                if row["analysis_spec_version"] == "single-session-evidence-v3-coverage"
            ]
            if len(current) != 1 or len(old) != 1 or old[0]["lineage_state"] != "SUPERSEDED":
                raise RuntimeError("version promotion did not converge")
            artifact_path = Path(current[0]["output_path"])
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            coverage = artifact["result"]["analysis_coverage"]
            old_after = {"sha256": file_sha(old_path), "mtime_ns": old_path.stat().st_mtime_ns}
            report.update({
                "pipeline_created": created,
                "pipeline_replay_created": replay_created,
                "analysis_result": analysis_result,
                "evidence_result": evidence_result,
                "current_analysis": {key: current[0][key] for key in (
                    "analysis_id", "status", "lineage_state", "scope",
                    "qualification_status", "analysis_spec_version",
                    "output_path", "artifact_digest",
                )},
                "old_analysis": {key: old[0][key] for key in (
                    "analysis_id", "status", "lineage_state",
                    "analysis_spec_version", "output_path", "artifact_digest",
                )},
                "coverage": {
                    key: value for key, value in coverage.items()
                    if key not in {"successful_source_segment_ids", "covered_segments", "gaps"}
                },
                "gap_count": len(coverage["gaps"]),
                "model_chunk_calls": model_calls,
                "artifact_binding_status": analysis.artifact_binding_status(artifact),
                "reference_count": artifact["evidence"]["reference_count"],
                "nearest_segment_fallback": artifact["evidence"]["nearest_segment_fallback"],
                "model_generated_timestamps": artifact["evidence"]["model_generated_timestamps"],
                "old_artifact_unchanged": old_state == old_after,
                "new_artifact_sha256_matches_db": file_sha(artifact_path) == current[0]["artifact_digest"],
                "bundles": bundles,
                "pending_projection_count": len(pending),
                "pending_projection_objects": [row["object_id"] for row in pending],
                "migration_health": health,
            })
            if not (
                analysis_result["revalidated"] == 0
                and len(model_calls) == coverage["chunk_count"]
                and report["artifact_binding_status"] == "BOUND_V1"
                and report["reference_count"] > 0
                and report["nearest_segment_fallback"] is False
                and report["model_generated_timestamps"] is False
                and evidence_result["verified"] == 1
                and coverage["is_qualified"]
                and coverage["meets_target"]
                and report["old_artifact_unchanged"]
                and report["new_artifact_sha256_matches_db"]
                and health["healthy"]
            ):
                raise RuntimeError("coverage clone verification failed")
            print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
            return 0
        finally:
            with admin.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname=%s AND pid<>pg_backend_pid()", (clone,),
                )
                cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(clone)))
            admin.close()


if __name__ == "__main__":
    raise SystemExit(main())
