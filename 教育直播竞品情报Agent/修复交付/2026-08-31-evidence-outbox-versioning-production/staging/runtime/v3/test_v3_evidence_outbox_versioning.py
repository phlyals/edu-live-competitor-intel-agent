#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence
import v3_projection_reconciler as reconciler
import v3_runtime as runtime
from test_v3_workflow import RuntimeCase
from v3_analysis_contract import ANALYSIS_SCOPE_FORMAL, QUALIFIED
from v3_long_jobs import ANALYSIS, claim_next, utc_now as job_now
from v3_recompute import insert_verified_lineage_conn


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class EvidenceOutboxTests(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.extra_patches = [
            patch.object(analysis, "connect", side_effect=self.connect),
            patch.object(evidence, "connect", side_effect=self.connect),
            patch.object(reconciler, "connect", side_effect=self.connect),
            patch.object(analysis, "upsert_heartbeat"),
            patch.object(evidence, "upsert_heartbeat"),
        ]
        for item in self.extra_patches:
            item.start()
        self.media = self.root / "整场直播.ts"
        self.media.write_bytes(b"media")
        self.media_hash = sha(self.media)
        self.transcript = self.root / "transcript.json"
        self.transcript.write_text(json.dumps({
            "status": "READY", "duration": 2,
            "segments": [
                {"start": 0, "end": 1, "text": "第一段"},
                {"start": 1, "end": 2, "text": "第二段"},
            ],
        }, ensure_ascii=False))
        self.transcript_digest = sha(self.transcript)
        self.analysis_id = "analysis-versioned"
        source_digest = hashlib.sha256(
            ("FULL_SESSION:" + self.media_hash).encode()
        ).hexdigest()
        quality = {
            "coverage_scope": "FULL_SESSION", "sample_only": False,
            "quality_gate_status": QUALIFIED, "source_segment_id": "seg",
            "timestamp_coverage": {
                "schema_version": 1, "audio_duration_seconds": 2,
                "coverage_rate": 1.0, "timestamps_valid": True,
                "is_qualified": True, "meets_target": True,
                "minimum_coverage_rate": 0.9, "target_coverage_rate": 0.95,
            },
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
                "('m','c','ACTIVE','https://invalid/live','OFFLINE_CONFIRMED')"
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,"
                "platform_session_id,status,started_at,ended_at,completeness,"
                "source_url,metadata_json) VALUES('s','m','room','MEDIA_COMPLETE',"
                "'2026-08-30T00:00:00Z','2026-08-30T00:00:02Z','COMPLETE',"
                "'https://invalid/live',?)",
                (json.dumps({"media_coverage": {"continuous_capture": True}}),),
            )
            conn.execute(
                "INSERT INTO recording_segments(segment_id,session_id,path,checksum,"
                "captured_from,captured_to,status,bytes,lifecycle_status) "
                "VALUES('seg','s',?,?, '2026-08-30T00:00:00Z',"
                "'2026-08-30T00:00:02Z','COMPLETE',1,'CANONICAL_ACTIVE')",
                (str(self.media), self.media_hash),
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,"
                "engine,model,status,source_path,output_path,created_at,scope,"
                "qualification_status,metadata_json) VALUES('t','s',?,"
                "'faster-whisper','small','COMPLETE',?,?,?,'FULL_SESSION',?,?)",
                (
                    source_digest, str(self.media), str(self.transcript),
                    "2026-08-30T00:00:03Z", QUALIFIED, json.dumps(quality),
                ),
            )
            conn.execute(
                "INSERT INTO analyses(analysis_id,session_id,transcript_id,"
                "analysis_type,source_digest,status,lineage_state,scope,"
                "qualification_status,transcript_content_digest,analysis_spec_version,"
                "model_version,prompt_version,metadata_json,updated_at,next_attempt_at) "
                "VALUES(?,?,?,'single_session',?,'PENDING','CURRENT',?,?,?,?,?,?,?, ?,?)",
                (
                    self.analysis_id, "s", "t", self.transcript_digest,
                    ANALYSIS_SCOPE_FORMAL, QUALIFIED, self.transcript_digest,
                    analysis.ANALYSIS_SPEC_VERSION, analysis.MODEL_VERSION,
                    analysis.PROMPT_VERSION,
                    json.dumps({
                        "qualification_state": QUALIFIED,
                        "formal_analysis_eligible": True,
                    }),
                    "2026-08-30T00:00:04Z", "2026-08-30T00:00:04Z",
                ),
            )
            insert_verified_lineage_conn(
                conn, analysis_id=self.analysis_id, transcript_id="t",
                transcript_content_digest=self.transcript_digest,
                state="CURRENT",
                analysis_spec_version=analysis.ANALYSIS_SPEC_VERSION,
                model_version=analysis.MODEL_VERSION,
                prompt_version=analysis.PROMPT_VERSION,
            )

    def tearDown(self):
        for item in reversed(self.extra_patches):
            item.stop()
        super().tearDown()

    @staticmethod
    def strict_response(chunk, **kwargs):
        before = kwargs.get("before_attempt")
        if before:
            before(1)
        payload = analysis.empty_chunk_result()
        payload["course_content"] = [{
            "summary": "课程",
            "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
        }]
        return analysis.validate_chunk_result(payload, chunk), {
            "response_id": "test", "finish_reason": "stop", "usage": {},
            "attempt": 1, "content_hash": "test",
        }

    def complete_analysis(self):
        with self.connect() as conn:
            job = claim_next(
                conn, ANALYSIS, "test", now=job_now(),
                where_sql="analysis_id=?", where_params=(self.analysis_id,),
            )
        with patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
                patch.object(analysis, "request_chunk", side_effect=self.strict_response):
            self.assertEqual(analysis.process_claim(job), "COMPLETE")
        with self.connect() as conn:
            row = dict(conn.execute(
                "SELECT * FROM analyses WHERE analysis_id=?", (self.analysis_id,)
            ).fetchone())
        return row

    def test_analysis_commit_has_required_evidence_but_no_outbox(self):
        self.complete_analysis()
        with self.connect() as conn:
            bundle = conn.execute(
                "SELECT status FROM evidence_bundles WHERE object_id=?",
                (self.analysis_id,),
            ).fetchone()
            count = conn.execute(
                "SELECT count(*) FROM outbox WHERE object_id=?",
                (self.analysis_id,),
            ).fetchone()[0]
        self.assertEqual(bundle["status"], "REQUIRED")
        self.assertEqual(count, 0)

    def test_evidence_verification_enqueues_one_version_and_replay_is_idempotent(self):
        row = self.complete_analysis()
        result = evidence.once()
        self.assertEqual((result["verified"], result["blocked"]), (1, 0))
        self.assertEqual(result["versioned_projections"], 1)
        with self.connect() as conn:
            outboxes = [dict(item) for item in conn.execute(
                "SELECT * FROM outbox WHERE object_id=?", (self.analysis_id,)
            )]
        self.assertEqual(len(outboxes), 1)
        outbox = outboxes[0]
        self.assertEqual(outbox["status"], "PENDING")
        self.assertEqual(outbox["artifact_digest"], row["artifact_digest"])
        self.assertEqual(outbox["projection_binding_status"], "VERSIONED_EVIDENCE")
        payload = json.loads(outbox["payload_json"])
        self.assertEqual(payload["projection_version"], outbox["projection_version"])
        self.assertEqual(payload["evidence_manifest_hash"], row["artifact_digest"])
        replay = evidence.once()
        self.assertEqual(replay["verified"], 0)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT count(*) FROM outbox WHERE object_id=?",
                    (self.analysis_id,),
                ).fetchone()[0], 1,
            )

    def test_evidence_failure_never_creates_projection(self):
        row = self.complete_analysis()
        Path(row["output_path"]).write_text("{}")
        result = evidence.once()
        self.assertEqual(result["blocked"], 1)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT count(*) FROM outbox WHERE object_id=?",
                    (self.analysis_id,),
                ).fetchone()[0], 0,
            )

    def test_crash_between_verify_and_enqueue_rolls_back_both(self):
        self.complete_analysis()
        with patch.object(
            evidence, "enqueue_verified_analysis_projection_conn",
            side_effect=RuntimeError("injected crash"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected crash"):
                evidence.once()
        with self.connect() as conn:
            bundle = conn.execute(
                "SELECT status,verified_at FROM evidence_bundles WHERE object_id=?",
                (self.analysis_id,),
            ).fetchone()
            count = conn.execute(
                "SELECT count(*) FROM outbox WHERE object_id=?",
                (self.analysis_id,),
            ).fetchone()[0]
        self.assertEqual((bundle["status"], bundle["verified_at"]), ("REQUIRED", None))
        self.assertEqual(count, 0)

    def test_new_artifact_digest_creates_new_projection_version(self):
        row = self.complete_analysis()
        self.assertEqual(evidence.once()["verified"], 1)
        artifact = json.loads(Path(row["output_path"]).read_text())
        artifact["revalidated_marker"] = 2
        second = self.root / "analysis-second.json"
        second.write_text(json.dumps(artifact, ensure_ascii=False))
        second_digest = sha(second)
        with self.connect() as conn:
            conn.execute(
                "UPDATE analyses SET output_path=?,artifact_digest=? WHERE analysis_id=?",
                (str(second), second_digest, self.analysis_id),
            )
            conn.execute(
                "UPDATE evidence_bundles SET status='REQUIRED',manifest_path=?,"
                "manifest_hash=?,verified_at=NULL WHERE object_id=?",
                (str(second), second_digest, self.analysis_id),
            )
        self.assertEqual(evidence.once()["verified"], 1)
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT projection_version,artifact_digest FROM outbox "
                "WHERE object_id=? ORDER BY projection_version",
                (self.analysis_id,),
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual({item["artifact_digest"] for item in rows}, {
            row["artifact_digest"], second_digest,
        })

    def test_reconciler_repairs_verified_bundle_missing_outbox(self):
        row = self.complete_analysis()
        with self.connect() as conn:
            conn.execute(
                "UPDATE evidence_bundles SET status='VERIFIED',verified_at=? "
                "WHERE object_id=?",
                ("2026-08-30T00:01:00Z", self.analysis_id),
            )
        result = reconciler.reconcile_once(connect_fn=self.connect)
        self.assertEqual(result["recreated_outbox"], 1)
        with self.connect() as conn:
            outbox = conn.execute(
                "SELECT artifact_digest,projection_binding_status FROM outbox "
                "WHERE object_id=?", (self.analysis_id,),
            ).fetchone()
        self.assertEqual(outbox["artifact_digest"], row["artifact_digest"])
        self.assertEqual(outbox["projection_binding_status"], "VERSIONED_EVIDENCE")

    def test_sent_without_receipt_is_reviewed_not_fabricated(self):
        self.complete_analysis(); evidence.once()
        with self.connect() as conn:
            conn.execute(
                "UPDATE outbox SET status='SENT' WHERE object_id=?",
                (self.analysis_id,),
            )
        result = reconciler.reconcile_once(connect_fn=self.connect)
        self.assertEqual(result["missing_receipts"], 1)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT count(*) FROM delivery_receipts").fetchone()[0], 0
            )
            review = conn.execute(
                "SELECT status FROM review_items WHERE object_type='outbox'"
            ).fetchone()
        self.assertEqual(review["status"], "PENDING")

    def test_verified_receipt_closes_delivery_pending_task(self):
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO tasks(task_id,task_type,dedupe_key,status,business_state,"
                "runtime_state,delivery_state,input_json,current_step,started_at,updated_at) "
                "VALUES('task-x','scan','dedupe-x','DELIVERY_PENDING','COMPLETE',"
                "'IDLE','PENDING','{}','DELIVERY','now','now')"
            )
            outbox_id = runtime.enqueue_outbox_conn(
                conn, object_type="scan_result", object_id="scan-x",
                destination="feishu_base", payload={"task_id": "task-x"},
            )
            conn.execute(
                "UPDATE outbox SET status='IN_FLIGHT',lease_owner='test',"
                "lease_until='2999-01-01T00:00:00Z' WHERE outbox_id=?",
                (outbox_id,),
            )
        runtime.complete_outbox(outbox_id, {"status": "VERIFIED"})
        result = reconciler.reconcile_once(connect_fn=self.connect)
        self.assertEqual(result["tasks_completed"], 1)
        with self.connect() as conn:
            task = conn.execute(
                "SELECT status,delivery_state FROM tasks WHERE task_id='task-x'"
            ).fetchone()
        self.assertEqual(tuple(task), ("COMPLETE", "VERIFIED"))


if __name__ == "__main__":
    import unittest
    unittest.main()
