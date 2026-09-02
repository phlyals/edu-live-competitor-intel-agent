import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

PRODUCTION_V3 = Path(__file__).resolve().parent
sys.path.append(str(PRODUCTION_V3))

from test_v3_workflow import RuntimeCase

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence
import v3_pipeline_worker as pipeline
import v3_project_feishu as projector
import v3_recompute_worker as recompute
from v3_analysis_contract import (
    ANALYSIS_SCOPE_FORMAL,
    ANALYSIS_SPEC_VERSION,
    MODEL_VERSION,
    PROMPT_VERSION,
    QUALIFIED,
)


def coverage_rows(*intervals):
    return [
        {
            "segment_index": index,
            "source_segment_id": f"t:segment:{index:06d}",
            "start": start,
            "end": end,
            "text": "内容",
            "line": f"[{start:.2f}-{end:.2f}] 内容",
        }
        for index, (start, end) in enumerate(intervals)
    ]


class CoverageMathTests(RuntimeCase):
    def test_internal_gaps_are_not_counted_as_covered(self):
        rows = coverage_rows((0, 10), (90, 100))
        result = analysis.calculate_analysis_coverage(
            rows, [row["source_segment_id"] for row in rows], 100,
        )
        self.assertEqual(result["analysis_coverage_rate"], 0.2)
        self.assertEqual(result["gaps"], [{"start_time": 10, "end_time": 90}])
        self.assertFalse(result["is_qualified"])

    def test_successful_ids_drive_timeline_and_segment_rates(self):
        rows = coverage_rows((0, 50), (50, 100))
        result = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100,
        )
        self.assertEqual(result["analysis_coverage_rate"], 0.5)
        self.assertEqual(result["segment_coverage_rate"], 0.5)
        self.assertEqual(result["analyzed_unique_segment_count"], 1)
        self.assertFalse(result["is_qualified"])

    def test_minimum_and_target_are_distinct(self):
        rows = coverage_rows((0, 90))
        minimum = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100, minimum=0.90, target=0.95,
        )
        target = analysis.calculate_analysis_coverage(
            coverage_rows((0, 95)), ["t:segment:000000"], 100,
            minimum=0.90, target=0.95,
        )
        self.assertTrue(minimum["is_qualified"])
        self.assertFalse(minimum["meets_target"])
        self.assertTrue(target["is_qualified"])
        self.assertTrue(target["meets_target"])

    def test_sample_duplicate_unknown_and_transcript_mismatch_fail_closed(self):
        rows = coverage_rows((0, 50), (50, 100))
        sample = analysis.calculate_analysis_coverage(
            rows, [row["source_segment_id"] for row in rows], 100,
            coverage_scope="SAMPLE",
        )
        self.assertFalse(sample["is_qualified"])
        rows[1]["source_segment_id"] = rows[0]["source_segment_id"]
        invalid = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"], "unknown"], 100,
        )
        codes = [item["code"] for item in invalid["validation_errors"]]
        self.assertIn("DUPLICATE_SOURCE_SEGMENT_ID", codes)
        self.assertIn("UNKNOWN_SUCCESSFUL_SOURCE_SEGMENT_ID", codes)
        mismatch = analysis.calculate_analysis_coverage(
            coverage_rows((0, 95)), ["t:segment:000000"], 100,
            transcript_quality={
                "audio_duration_seconds": 100,
                "coverage_rate": 0.99,
                "timestamps_valid": True,
                "is_qualified": True,
            },
        )
        self.assertFalse(mismatch["is_qualified"])
        self.assertIn(
            "TRANSCRIPT_QUALITY_GATE_MISMATCH",
            [item["code"] for item in mismatch["validation_errors"]],
        )

    def test_free_timestamp_artifact_cannot_be_revalidated_into_source_binding(self):
        rows = coverage_rows((0, 50), (50, 100))
        text = "\n".join(row["line"] for row in rows)
        chunks = analysis.analysis_chunks(text, source_rows=rows)
        artifact = {
            "engine": {
                "source_content_hash": hashlib.sha256(text.encode()).hexdigest(),
                "chunk_diagnostics": [
                    {
                        "chunk_index": chunk["index"],
                        "start": chunk["start"],
                        "end": chunk["end"],
                        "row_count": chunk["row_count"],
                        "finish_reason": "stop",
                    }
                    for chunk in chunks
                ],
            },
            "result": {"modules": []},
        }
        with self.assertRaisesRegex(ValueError, "legacy free-timestamp"):
            analysis.revalidate_existing_artifact(
                artifact, text, rows, 100, "FULL_SESSION", None, 0.90, 0.95,
            )


class CoverageVersionIntegrationTests(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.pipeline_patch = patch.object(pipeline, "connect", side_effect=self.connect)
        self.analysis_patch = patch.object(analysis, "connect", side_effect=self.connect)
        self.recompute_patch = patch.object(recompute, "connect", side_effect=self.connect)
        self.pipeline_patch.start()
        self.analysis_patch.start()
        self.recompute_patch.start()
        self.media = self.root / "整场直播.ts"
        self.transcript = self.root / "full.transcript.json"
        self.old_artifact = self.root / "legacy.analysis.json"
        self.media.write_bytes(b"media")
        self.media_hash = hashlib.sha256(self.media.read_bytes()).hexdigest()
        self.transcript_id = "transcript-full"
        self.session_id = "session-full"

    def tearDown(self):
        self.recompute_patch.stop()
        self.analysis_patch.stop()
        self.pipeline_patch.stop()
        super().tearDown()

    def seed(self, intervals=((0, 95),), *, include_old=True):
        segments = [
            {"start": start, "end": end, "text": f"内容{index}"}
            for index, (start, end) in enumerate(intervals)
        ]
        quality = {
            "schema_version": 1,
            "audio_duration_seconds": 100,
            "coverage_rate": 0.95,
            "timestamps_valid": True,
            "is_qualified": True,
            "meets_target": True,
            "minimum_coverage_rate": 0.90,
            "target_coverage_rate": 0.95,
        }
        payload = {
            "status": "READY",
            "duration": 100,
            "coverage_scope": "FULL_SESSION",
            "segments": segments,
            "timestamp_coverage": quality,
        }
        self.transcript.write_text(json.dumps(payload, ensure_ascii=False))
        transcript_digest = hashlib.sha256(self.transcript.read_bytes()).hexdigest()
        source_digest = hashlib.sha256(
            ("FULL_SESSION:" + self.media_hash).encode()
        ).hexdigest()
        metadata = {
            "coverage_scope": "FULL_SESSION",
            "sample_only": False,
            "quality_gate_status": QUALIFIED,
            "source_segment_id": "canonical",
            "timestamp_coverage": quality,
        }
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO competitors(competitor_id,platform,platform_account_id,"
                "account_name,first_seen_at,last_seen_at) VALUES"
                "('c','test','c','test','2026-01-01','2026-01-01')"
            )
            conn.execute(
                "INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,"
                "live_url,live_status) VALUES"
                "('m','c','ACTIVE','https://example.invalid/live','OFFLINE_CONFIRMED')"
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,"
                "status,started_at,ended_at,completeness,source_url,metadata_json) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    self.session_id, "m", "room", "MEDIA_COMPLETE",
                    "2026-08-30T00:00:00Z", "2026-08-30T00:01:40Z",
                    "COMPLETE", "https://example.invalid/live",
                    json.dumps({"media_coverage": {"continuous_capture": True}}),
                ),
            )
            conn.execute(
                "INSERT INTO recording_segments(segment_id,session_id,path,checksum,"
                "captured_from,captured_to,status,bytes,lifecycle_status) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    "canonical", self.session_id, str(self.media), self.media_hash,
                    "2026-08-30T00:00:00Z", "2026-08-30T00:01:40Z",
                    "COMPLETE", len(self.media.read_bytes()), "CANONICAL_ACTIVE",
                ),
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,"
                "status,source_path,output_path,created_at,scope,qualification_status,"
                "metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.transcript_id, self.session_id, source_digest,
                    "faster-whisper", "small", "COMPLETE", str(self.media),
                    str(self.transcript), "2026-08-30T00:02:00Z",
                    "FULL_SESSION", QUALIFIED,
                    json.dumps(metadata),
                ),
            )
        if include_old:
            rows = analysis.source_rows_from_transcript(payload, self.transcript_id)
            text = "\n".join(row["line"] for row in rows)
            chunks = analysis.analysis_chunks(text, source_rows=rows)
            old_engine = {
                "provider": "deepseek",
                "model": MODEL_VERSION,
                "source_content_hash": hashlib.sha256(text.encode()).hexdigest(),
                "chunk_diagnostics": [
                    {
                        "chunk_index": chunk["index"],
                        "start": chunk["start"],
                        "end": chunk["end"],
                        "row_count": chunk["row_count"],
                        "finish_reason": "stop",
                    }
                    for chunk in chunks
                ],
            }
            old_payload = {
                "analysis_id": "analysis-old",
                "session_id": self.session_id,
                "transcript_id": self.transcript_id,
                "engine": old_engine,
                "result": {"modules": []},
            }
            self.old_artifact.write_text(json.dumps(old_payload, ensure_ascii=False))
            old_digest = hashlib.sha256(self.old_artifact.read_bytes()).hexdigest()
            with self.connect() as conn:
                conn.execute(
                    "INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,"
                    "source_digest,status,output_path,lineage_state,scope,qualification_status,"
                    "transcript_content_digest,analysis_spec_version,model_version,prompt_version,"
                    "artifact_digest,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "analysis-old", self.session_id, self.transcript_id,
                        "single_session", transcript_digest, "COMPLETE",
                        str(self.old_artifact), "CURRENT", ANALYSIS_SCOPE_FORMAL,
                        QUALIFIED, transcript_digest, "single-session-evidence-v2",
                        MODEL_VERSION, PROMPT_VERSION, old_digest,
                        json.dumps({
                            "qualification_state": QUALIFIED,
                            "formal_analysis_eligible": True,
                        }),
                    ),
                )
                conn.execute(
                    "INSERT INTO lineage_edges(edge_id,downstream_type,downstream_id,"
                    "upstream_type,upstream_id,upstream_version,binding_status,"
                    "upstream_engine_version,upstream_model_version,downstream_model_version,"
                    "downstream_prompt_version,downstream_schema_version,state,created_at,"
                    "updated_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "edge-old", "analysis", "analysis-old", "transcript",
                        self.transcript_id, transcript_digest,
                        "CONTENT_DIGEST_VERIFIED", "faster-whisper", "small",
                        MODEL_VERSION, PROMPT_VERSION, "single-session-evidence-v2",
                        "CURRENT", "2026-08-30T00:03:00Z",
                        "2026-08-30T00:03:00Z", "{}",
                    ),
                )
                conn.execute(
                    "INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,"
                    "manifest_path,manifest_hash,verified_at,scope,qualification_status,"
                    "metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        "bundle:analysis-old", "analysis", "analysis-old", "VERIFIED",
                        str(self.old_artifact), old_digest,
                        "2026-08-30T00:04:00Z", ANALYSIS_SCOPE_FORMAL,
                        QUALIFIED, "{}",
                    ),
                )
        return transcript_digest

    def create_candidate(self):
        with patch.object(
            pipeline, "load_pipeline_config",
            return_value={
                "transcript_quality": {
                    "full_session_min_timestamp_coverage_rate": 0.90,
                },
            },
        ):
            self.assertEqual(pipeline.create_analysis_tasks(), 1)
            self.assertEqual(pipeline.create_analysis_tasks(), 0)
        with patch.object(recompute, "init_db"), \
                patch.object(recompute, "upsert_heartbeat"):
            result = recompute.once()
        self.assertEqual(result["candidate_created"], 1)
        with self.connect() as conn:
            return dict(conn.execute(
                "SELECT * FROM analyses WHERE analysis_spec_version=?",
                (ANALYSIS_SPEC_VERSION,),
            ).fetchone())

    def test_new_source_binding_version_reruns_model_and_supersedes_old(self):
        self.seed()
        candidate = self.create_candidate()
        with self.connect() as conn:
            metadata = json.loads(candidate["metadata_json"])
            metadata.update({
                "error_type": "OldFailure",
                "error_message": "must be cleared",
                "checked_at": "old",
            })
            conn.execute(
                "UPDATE analyses SET metadata_json=? WHERE analysis_id=?",
                (json.dumps(metadata), candidate["analysis_id"]),
            )
        old_bytes = self.old_artifact.read_bytes()
        model_calls = []

        def bound_response(chunk, **_kwargs):
            model_calls.append(chunk["index"])
            payload = analysis.empty_chunk_result()
            payload["hook"] = [{
                "summary": "严格证据",
                "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
            }]
            return analysis.validate_chunk_result(payload, chunk), {
                "response_id": f"response-{chunk['index']}",
                "finish_reason": "stop",
                "usage": {},
                "attempt": 1,
                "content_hash": "test",
            }

        with patch.object(analysis, "init_db"), \
             patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
             patch.object(analysis, "load_analysis_quality_config", return_value=(0.90, 0.95)), \
             patch.object(analysis, "request_chunk", side_effect=bound_response), \
             patch.object(analysis, "upsert_heartbeat"):
            result = analysis.once()
        self.assertEqual(result["completed"], 1)
        self.assertEqual(result["revalidated"], 0)
        self.assertGreater(len(model_calls), 0)
        self.assertEqual(result["current_spec_evidence_pending"], 0)
        self.assertEqual(result["current_spec_waiting"], 1)
        self.assertEqual(self.old_artifact.read_bytes(), old_bytes)
        with self.connect() as conn:
            current = conn.execute(
                "SELECT * FROM analyses WHERE analysis_id=?",
                (candidate["analysis_id"],),
            ).fetchone()
            old = conn.execute(
                "SELECT * FROM analyses WHERE analysis_id='analysis-old'"
            ).fetchone()
            bundle = conn.execute(
                "SELECT * FROM evidence_bundles WHERE object_id=?",
                (candidate["analysis_id"],),
            ).fetchone()
            outbox = list(conn.execute(
                "SELECT object_id,status FROM outbox WHERE object_type='semantic_projection' "
                "ORDER BY object_id"
            ))
        self.assertEqual((current["status"], current["lineage_state"]), ("COMPLETE", "CANDIDATE"))
        current_metadata = json.loads(current["metadata_json"])
        self.assertNotIn("error_type", current_metadata)
        self.assertNotIn("error_message", current_metadata)
        self.assertEqual(old["lineage_state"], "STALE")
        artifact = json.loads(Path(current["output_path"]).read_text())
        coverage = artifact["result"]["analysis_coverage"]
        self.assertEqual(coverage["analysis_coverage_rate"], 0.95)
        self.assertTrue(coverage["meets_target"])
        self.assertEqual(analysis.artifact_binding_status(artifact), "BOUND_V1")
        self.assertFalse(artifact["result"]["evidence_binding"]["nearest_segment_fallback"])
        self.assertEqual(bundle["status"], "REQUIRED")
        self.assertEqual({row["object_id"] for row in outbox}, {candidate["analysis_id"], "analysis-old"})
        self.assertEqual(
            {row["object_id"]: row["status"] for row in outbox},
            {candidate["analysis_id"]: "HELD_EVIDENCE", "analysis-old": "PENDING"},
        )
        with patch.object(evidence, "connect", side_effect=self.connect), \
             patch.object(evidence, "init_db"), \
             patch.object(evidence, "upsert_heartbeat"):
            evidence_result = evidence.once()
        self.assertEqual(evidence_result["verified"], 1)
        self.assertEqual(evidence_result["released_projections"], 1)
        with self.connect() as conn:
            released = list(conn.execute(
                "SELECT status FROM outbox WHERE object_type='semantic_projection'"
            ))
        self.assertEqual({row["status"] for row in released}, {"PENDING"})
        with patch.object(analysis, "init_db"), \
             patch.object(analysis, "upsert_heartbeat") as heartbeat:
            replay = analysis.once()
        self.assertEqual(replay["current_spec_qualified"], 1)
        self.assertEqual(replay["current_spec_evidence_pending"], 0)
        self.assertEqual(replay["health_reasons"], [])
        self.assertEqual(heartbeat.call_args.args[1], "READY")

    def test_low_analysis_coverage_is_blocked_without_superseding_old(self):
        self.seed(intervals=((0, 47.5), (47.5, 95)), include_old=True)
        candidate = self.create_candidate()
        with self.connect() as conn:
            conn.execute(
                "UPDATE evidence_bundles SET status='BLOCKED_EVIDENCE' "
                "WHERE object_id='analysis-old'"
            )
        rows = coverage_rows((0, 47.5), (47.5, 95))
        low = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100,
            transcript_quality={
                "audio_duration_seconds": 100,
                "coverage_rate": 0.95,
                "timestamps_valid": True,
                "is_qualified": True,
            },
        )
        with patch.object(analysis, "init_db"), \
             patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
             patch.object(analysis, "load_analysis_quality_config", return_value=(0.90, 0.95)), \
             patch.object(analysis, "request_analysis", return_value=(
                 {"modules": [], "analysis_coverage": low},
                 {"source_content_hash": "new"},
             )), \
             patch.object(analysis, "upsert_heartbeat"):
            result = analysis.once()
        self.assertEqual(result["quality_blocked"], 1)
        with self.connect() as conn:
            blocked = conn.execute(
                "SELECT * FROM analyses WHERE analysis_id=?",
                (candidate["analysis_id"],),
            ).fetchone()
            old = conn.execute(
                "SELECT * FROM analyses WHERE analysis_id='analysis-old'"
            ).fetchone()
            bundle = conn.execute(
                "SELECT * FROM evidence_bundles WHERE object_id=?",
                (candidate["analysis_id"],),
            ).fetchone()
        self.assertEqual(blocked["status"], "QUALITY_BLOCKED")
        self.assertIsNone(blocked["output_path"])
        self.assertEqual(blocked["qualification_status"], "ANALYSIS_QUALITY_BLOCKED")
        self.assertEqual(old["lineage_state"], "STALE")
        self.assertEqual(bundle["status"], "QUALITY_BLOCKED")
        status, health = analysis.analysis_health_snapshot(0.90, 0.95)
        self.assertEqual(status, "DEGRADED")
        self.assertEqual(health["current_spec_blocked"], 1)
        self.assertIn("ANALYSIS_QUALITY_BLOCKED", health["health_reasons"])

    def test_superseded_projection_is_historical_not_sample(self):
        fields, qualification = projector.analysis_projection_fields({
            "analysis_id": "old",
            "session_id": "s",
            "analysis_type": "single_session",
            "status": "COMPLETE",
            "lineage_state": "SUPERSEDED",
            "scope": ANALYSIS_SCOPE_FORMAL,
            "qualification_status": QUALIFIED,
            "output_path": "/old.json",
            "metadata_json": json.dumps({
                "qualification_state": QUALIFIED,
                "formal_analysis_eligible": True,
                "superseded_by_analysis_id": "new",
            }),
        })
        self.assertEqual(qualification, "SUPERSEDED_FORMAL_VERSION")
        self.assertEqual(fields["分析类型"], "单场分析")
        self.assertEqual(fields["状态"], "完成")
        self.assertIn("superseded_by=new", fields["证据说明"])


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=2)
