#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence
import v3_pipeline_worker as pipeline
import v3_project_feishu as project
import v3_recompute_worker as recompute
from test_v3_workflow import RuntimeCase
from v3_long_jobs import ANALYSIS, RECOMPUTE, claim_next, utc_now as job_now
from v3_recompute import (
    CONTENT_DIGEST_VERIFIED,
    analysis_id_for,
    insert_verified_lineage_conn,
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LineageRecomputeTests(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.patches2 = [
            patch.object(pipeline, "connect", side_effect=self.connect),
            patch.object(recompute, "connect", side_effect=self.connect),
            patch.object(analysis, "connect", side_effect=self.connect),
            patch.object(evidence, "connect", side_effect=self.connect),
            patch.object(recompute, "init_db"),
            patch.object(evidence, "init_db"),
            patch.object(recompute, "upsert_heartbeat"),
            patch.object(analysis, "upsert_heartbeat"),
            patch.object(evidence, "upsert_heartbeat"),
        ]
        for item in self.patches2:
            item.start()
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO competitors(competitor_id,platform,platform_account_id,"
                "account_name,first_seen_at,last_seen_at) "
                "VALUES('c','buyin','c','test','2026-01-01','2026-01-01')"
            )

    def tearDown(self):
        for item in reversed(self.patches2):
            item.stop()
        super().tearDown()

    def add_session(
        self,
        suffix: str,
        *,
        old_spec: str | None = None,
        old_model: str | None = None,
        old_prompt: str | None = None,
        legacy_edge: bool = False,
        mutate_transcript: bool = False,
    ) -> dict:
        session_id = "s-" + suffix
        transcript_id = "t-" + suffix
        old_analysis_id = "old-" + suffix
        media = self.root / f"media-{suffix}" / "整场直播.ts"
        media.parent.mkdir(parents=True)
        media.write_bytes(("media-" + suffix).encode())
        media_hash = sha(media)
        transcript = self.root / f"transcript-{suffix}.json"
        payload = {
            "status": "READY",
            "duration": 2,
            "segments": [
                {"start": 0, "end": 1, "text": "第一段"},
                {"start": 1, "end": 2, "text": "第二段"},
            ],
        }
        transcript.write_text(json.dumps(payload, ensure_ascii=False))
        old_digest = sha(transcript)
        source_digest = hashlib.sha256(
            ("FULL_SESSION:" + media_hash).encode()
        ).hexdigest()
        quality = {
            "coverage_scope": "FULL_SESSION",
            "sample_only": False,
            "quality_gate_status": "FULL_SESSION_QUALIFIED",
            "source_segment_id": "seg-" + suffix,
            "timestamp_coverage": {
                "schema_version": 1,
                "audio_duration_seconds": 2,
                "coverage_rate": 1.0,
                "timestamps_valid": True,
                "is_qualified": True,
                "meets_target": True,
                "minimum_coverage_rate": 0.9,
                "target_coverage_rate": 0.95,
            },
        }
        old_output = self.root / f"old-analysis-{suffix}.json"
        old_output.write_text("{}")
        old_spec = old_spec or analysis.ANALYSIS_SPEC_VERSION
        old_model = old_model or analysis.MODEL_VERSION
        old_prompt = old_prompt or analysis.PROMPT_VERSION
        with self.connect() as conn:
            competitor_id = "c-" + suffix
            conn.execute(
                "INSERT INTO competitors(competitor_id,platform,platform_account_id,"
                "account_name,first_seen_at,last_seen_at) VALUES(?, 'buyin',?,?,"
                "'2026-01-01','2026-01-01')",
                (competitor_id, competitor_id, "test-" + suffix),
            )
            conn.execute(
                "INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,"
                "live_url,live_status) VALUES(?,?, 'ACTIVE',?,'OFFLINE_CONFIRMED')",
                ("m-" + suffix, competitor_id, "https://live.invalid/" + suffix),
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,"
                "platform_session_id,status,started_at,ended_at,completeness,"
                "source_url,metadata_json) VALUES(?,?,?,'MEDIA_COMPLETE',"
                "'2026-08-30T00:00:00Z','2026-08-30T00:00:02Z','COMPLETE',?,?)",
                (
                    session_id, "m-" + suffix, "room-" + suffix,
                    "https://live.invalid/" + suffix,
                    json.dumps({"media_coverage": {"continuous_capture": True}}),
                ),
            )
            conn.execute(
                "INSERT INTO recording_segments(segment_id,session_id,path,checksum,"
                "captured_from,captured_to,status,bytes,lifecycle_status) "
                "VALUES(?,?,?,?,?,?, 'COMPLETE',?,'CANONICAL_ACTIVE')",
                (
                    "seg-" + suffix, session_id, str(media), media_hash,
                    "2026-08-30T00:00:00Z", "2026-08-30T00:00:02Z",
                    media.stat().st_size,
                ),
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,"
                "engine,model,status,language,source_path,output_path,created_at,"
                "scope,qualification_status,metadata_json) "
                "VALUES(?,?,?,'faster-whisper','small','COMPLETE','zh',?,?,?,"
                "'FULL_SESSION','FULL_SESSION_QUALIFIED',?)",
                (
                    transcript_id, session_id, source_digest, str(media),
                    str(transcript), "2026-08-30T00:00:03Z",
                    json.dumps(quality),
                ),
            )
            conn.execute(
                "INSERT INTO analyses(analysis_id,session_id,transcript_id,"
                "analysis_type,source_digest,status,output_path,lineage_state,scope,"
                "qualification_status,transcript_content_digest,analysis_spec_version,"
                "model_version,prompt_version,artifact_digest,updated_at,metadata_json) "
                "VALUES(?,?,?,'single_session',?,'COMPLETE',?,'CURRENT',"
                "'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?,?,?,?,?,?,?)",
                (
                    old_analysis_id, session_id, transcript_id, old_digest,
                    str(old_output), old_digest, old_spec, old_model, old_prompt,
                    sha(old_output), "2026-08-30T00:00:04Z",
                    json.dumps({
                        "qualification_state": "FULL_SESSION_QUALIFIED",
                        "formal_analysis_eligible": True,
                    }),
                ),
            )
            if legacy_edge:
                conn.execute(
                    "INSERT INTO lineage_edges(edge_id,downstream_type,downstream_id,"
                    "upstream_type,upstream_id,upstream_version,binding_status,state,"
                    "created_at,metadata_json) VALUES(?, 'analysis',?, 'transcript',?,?,"
                    "'LEGACY_UNVERIFIED','CURRENT','2026-08-30T00:00:04Z','{}')",
                    ("edge-" + suffix, old_analysis_id, transcript_id, old_digest),
                )
            else:
                insert_verified_lineage_conn(
                    conn,
                    analysis_id=old_analysis_id,
                    transcript_id=transcript_id,
                    transcript_content_digest=old_digest,
                    state="CURRENT",
                    analysis_spec_version=old_spec,
                    model_version=old_model,
                    prompt_version=old_prompt,
                    metadata={"fixture": True},
                )
        if mutate_transcript:
            payload["segments"][1]["text"] = "第二段已经变化"
            transcript.write_text(json.dumps(payload, ensure_ascii=False))
        return {
            "session_id": session_id,
            "transcript_id": transcript_id,
            "transcript_path": transcript,
            "old_digest": old_digest,
            "new_digest": sha(transcript),
            "old_analysis_id": old_analysis_id,
        }

    def test_transcript_content_change_creates_one_versioned_request(self):
        item = self.add_session("content", mutate_transcript=True)
        self.assertEqual(pipeline.create_analysis_tasks(), 1)
        self.assertEqual(pipeline.create_analysis_tasks(), 0)
        with self.connect() as conn:
            requests = conn.execute("SELECT * FROM recompute_requests").fetchall()
            old = conn.execute(
                "SELECT status,lineage_state,output_path FROM analyses WHERE analysis_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
            edge = conn.execute(
                "SELECT state,binding_status FROM lineage_edges WHERE downstream_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
            reviews = conn.execute("SELECT * FROM review_items").fetchall()
        self.assertEqual(len(requests), 1)
        self.assertEqual(
            (requests[0]["old_upstream_digest"], requests[0]["new_upstream_digest"]),
            (item["old_digest"], item["new_digest"]),
        )
        self.assertEqual(tuple(old)[:2], ("COMPLETE", "STALE"))
        self.assertTrue(Path(old["output_path"]).is_file())
        self.assertEqual(tuple(edge), ("STALE", CONTENT_DIGEST_VERIFIED))
        self.assertEqual(len(reviews), 1)
        self.assertEqual(
            (reviews[0]["object_type"], reviews[0]["object_id"]),
            ("recompute_request", requests[0]["request_id"]),
        )

    def test_spec_model_and_prompt_changes_each_create_one_request(self):
        self.add_session("spec", old_spec="old-spec")
        self.add_session("model", old_model="old-model")
        self.add_session("prompt", old_prompt="old-prompt")
        self.assertEqual(pipeline.create_analysis_tasks(), 3)
        self.assertEqual(pipeline.create_analysis_tasks(), 0)
        with self.connect() as conn:
            values = [
                json.loads(row["metadata_json"])["reasons"]
                for row in conn.execute(
                    "SELECT metadata_json FROM recompute_requests ORDER BY request_id"
                )
            ]
        flattened = {reason for group in values for reason in group}
        self.assertIn("ANALYSIS_SPEC_VERSION_CHANGED", flattened)
        self.assertIn("MODEL_VERSION_CHANGED", flattened)
        self.assertIn("PROMPT_VERSION_CHANGED", flattened)

    def test_legacy_unverified_edge_never_auto_recomputes(self):
        item = self.add_session(
            "legacy", old_spec="legacy-spec", legacy_edge=True
        )
        self.assertEqual(pipeline.create_analysis_tasks(), 0)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT count(*) FROM recompute_requests").fetchone()[0], 0
            )
            row = conn.execute(
                "SELECT lineage_state FROM analyses WHERE analysis_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
        self.assertEqual(row["lineage_state"], "CURRENT")

    def test_digest_changes_again_obsoletes_old_request(self):
        item = self.add_session("again", mutate_transcript=True)
        self.assertEqual(pipeline.create_analysis_tasks(), 1)
        with self.connect() as conn:
            first = dict(conn.execute(
                "SELECT * FROM recompute_requests"
            ).fetchone())
        payload = json.loads(item["transcript_path"].read_text())
        payload["segments"][0]["text"] = "第三个版本"
        item["transcript_path"].write_text(json.dumps(payload, ensure_ascii=False))
        self.assertEqual(pipeline.create_analysis_tasks(), 1)
        with self.connect() as conn:
            job = claim_next(
                conn, RECOMPUTE, "test", now=job_now(),
                where_sql="request_id=?", where_params=(first["request_id"],),
            )
        self.assertEqual(recompute.process_request(job), "OBSOLETE")
        with self.connect() as conn:
            statuses = {
                row["request_id"]: row["status"]
                for row in conn.execute("SELECT request_id,status FROM recompute_requests")
            }
        self.assertEqual(statuses[first["request_id"]], "OBSOLETE")
        self.assertIn("PENDING", statuses.values())

    def _create_candidate(self, suffix: str = "flow") -> tuple[dict, dict]:
        item = self.add_session(suffix, mutate_transcript=True)
        self.assertEqual(pipeline.create_analysis_tasks(), 1)
        result = recompute.once()
        self.assertEqual(result["candidate_created"], 1)
        with self.connect() as conn:
            request = dict(conn.execute("SELECT * FROM recompute_requests").fetchone())
            candidate = dict(conn.execute(
                "SELECT * FROM analyses WHERE analysis_id=?",
                (request["candidate_analysis_id"],),
            ).fetchone())
            edge = conn.execute(
                "SELECT state,binding_status,upstream_version FROM lineage_edges "
                "WHERE downstream_id=?",
                (candidate["analysis_id"],),
            ).fetchone()
        self.assertEqual(request["status"], "CANDIDATE_CREATED")
        self.assertEqual(
            (candidate["status"], candidate["lineage_state"]),
            ("PENDING_RECOMPUTE", "CANDIDATE"),
        )
        self.assertEqual(
            tuple(edge),
            ("CANDIDATE", CONTENT_DIGEST_VERIFIED, item["new_digest"]),
        )
        return item, request

    def test_verified_candidate_switches_current_atomically(self):
        item, request = self._create_candidate()
        with self.connect() as conn:
            job = claim_next(
                conn, ANALYSIS, "analysis-test", now=job_now(),
                where_sql="analysis_id=?",
                where_params=(request["candidate_analysis_id"],),
            )

        def strict_response(chunk, **kwargs):
            before = kwargs.get("before_attempt")
            if before:
                before(1)
            payload = analysis.empty_chunk_result()
            payload["course_content"] = [{
                "summary": "课程内容",
                "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
            }]
            return analysis.validate_chunk_result(payload, chunk), {
                "response_id": "test", "finish_reason": "stop", "usage": {},
                "attempt": 1, "content_hash": "test",
            }

        with patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
                patch.object(analysis, "request_chunk", side_effect=strict_response):
            self.assertEqual(analysis.process_claim(job), "COMPLETE")
        with self.connect() as conn:
            old = conn.execute(
                "SELECT status,lineage_state FROM analyses WHERE analysis_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
            candidate = conn.execute(
                "SELECT status,lineage_state FROM analyses WHERE analysis_id=?",
                (request["candidate_analysis_id"],),
            ).fetchone()
            bundle = conn.execute(
                "SELECT status FROM evidence_bundles WHERE object_id=?",
                (request["candidate_analysis_id"],),
            ).fetchone()
        self.assertEqual(tuple(old), ("COMPLETE", "STALE"))
        self.assertEqual(tuple(candidate), ("COMPLETE", "CANDIDATE"))
        self.assertEqual(bundle["status"], "REQUIRED")

        result = evidence.once()
        self.assertEqual((result["verified"], result["blocked"]), (1, 0))
        with self.connect() as conn:
            old = conn.execute(
                "SELECT status,lineage_state FROM analyses WHERE analysis_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
            candidate = conn.execute(
                "SELECT status,lineage_state FROM analyses WHERE analysis_id=?",
                (request["candidate_analysis_id"],),
            ).fetchone()
            request_row = conn.execute(
                "SELECT status FROM recompute_requests WHERE request_id=?",
                (request["request_id"],),
            ).fetchone()
            review = conn.execute(
                "SELECT status FROM review_items WHERE object_id=?",
                (request["request_id"],),
            ).fetchone()
            held = conn.execute(
                "SELECT count(*) FROM outbox WHERE status='HELD_EVIDENCE'"
            ).fetchone()[0]
        self.assertEqual(tuple(old), ("COMPLETE", "SUPERSEDED"))
        self.assertEqual(tuple(candidate), ("COMPLETE", "CURRENT"))
        self.assertEqual(request_row["status"], "COMPLETE")
        self.assertEqual(review["status"], "RESOLVED")
        self.assertEqual(held, 0)
        self.assertEqual(evidence.once()["verified"], 0)

    def test_failed_candidate_keeps_old_result_readable_and_stale(self):
        item, request = self._create_candidate("failed")
        with self.connect() as conn:
            conn.execute(
                "UPDATE analyses SET status='FAILED_FINAL' WHERE analysis_id=?",
                (request["candidate_analysis_id"],),
            )
        result = recompute.reconcile_candidates()
        self.assertEqual(result["failed"], 1)
        with self.connect() as conn:
            old = conn.execute(
                "SELECT status,lineage_state,output_path FROM analyses WHERE analysis_id=?",
                (item["old_analysis_id"],),
            ).fetchone()
            req = conn.execute(
                "SELECT status FROM recompute_requests WHERE request_id=?",
                (request["request_id"],),
            ).fetchone()
            review = conn.execute(
                "SELECT status FROM review_items WHERE object_id=?",
                (request["request_id"],),
            ).fetchone()
        self.assertEqual((old["status"], old["lineage_state"]), ("COMPLETE", "STALE"))
        self.assertTrue(Path(old["output_path"]).is_file())
        self.assertEqual(req["status"], "FAILED_FINAL")
        self.assertEqual(review["status"], "PENDING")

    def test_analysis_identity_algorithm_remains_backward_compatible(self):
        digest = "a" * 64
        expected = "analysis_" + hashlib.sha256(
            (
                f"t:{digest}:{analysis.ANALYSIS_SPEC_VERSION}:"
                f"{analysis.MODEL_VERSION}:{analysis.PROMPT_VERSION}"
            ).encode()
        ).hexdigest()[:24]
        self.assertEqual(
            analysis_id_for(
                "t", digest, analysis.ANALYSIS_SPEC_VERSION,
                analysis.MODEL_VERSION, analysis.PROMPT_VERSION,
            ),
            expected,
        )

    def test_stale_projection_is_explicit_review_not_sample(self):
        fields, state = project.analysis_projection_fields({
            "analysis_id": "old", "session_id": "s",
            "analysis_type": "single_session", "status": "COMPLETE",
            "lineage_state": "STALE", "scope": "FORMAL_SINGLE_SESSION",
            "qualification_status": "FULL_SESSION_QUALIFIED",
            "output_path": "/kept.json",
            "metadata_json": json.dumps({
                "qualification_state": "FULL_SESSION_QUALIFIED",
                "formal_analysis_eligible": True,
                "recompute_request_id": "recompute-1",
            }),
        })
        self.assertEqual(state, "STALE_RECOMPUTE_REVIEW")
        self.assertEqual(fields["分析类型"], "单场分析")
        self.assertEqual(fields["状态"], "证据不足")
        self.assertIn("recompute-1", fields["证据说明"])


if __name__ == "__main__":
    import unittest
    unittest.main()
