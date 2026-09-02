import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence
import v3_pipeline_worker as pipeline
import v3_project_feishu as project
import v3_sample_analysis_migration as migration
import v3_analysis_contract as contract
import v3_runtime as runtime


def connect_factory(path: Path):
    def connect():
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn
    return connect


def full_meta(segment_id="seg-full"):
    return {
        "coverage_scope": "FULL_SESSION",
        "sample_only": False,
        "quality_gate_status": "FULL_SESSION_QUALIFIED",
        "source_segment_id": segment_id,
        "timestamp_coverage": {
            "audio_duration_seconds": 10,
            "coverage_rate": 1.0,
            "is_qualified": True,
            "timestamps_valid": True,
        },
    }


def session_meta():
    return {"media_coverage": {"continuous_capture": True}}


def canonical_source_digest(checksum="media-checksum"):
    return hashlib.sha256(("FULL_SESSION:" + checksum).encode()).hexdigest()


class PipelineGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "db.sqlite"
        with connect_factory(self.db)() as conn:
            conn.executescript("""
            CREATE TABLE live_sessions(session_id TEXT PRIMARY KEY,status TEXT,completeness TEXT,metadata_json TEXT);
            CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,status TEXT,lifecycle_status TEXT,checksum TEXT,path TEXT);
            CREATE TABLE transcripts(transcript_id TEXT PRIMARY KEY,session_id TEXT,source_digest TEXT,status TEXT,output_path TEXT,scope TEXT,qualification_status TEXT,metadata_json TEXT,engine TEXT,model TEXT);
            CREATE TABLE analyses(analysis_id TEXT PRIMARY KEY,session_id TEXT,transcript_id TEXT,analysis_type TEXT,source_digest TEXT,status TEXT,output_path TEXT,lineage_state TEXT,scope TEXT,qualification_status TEXT,transcript_content_digest TEXT,analysis_spec_version TEXT,model_version TEXT,prompt_version TEXT,artifact_digest TEXT,metadata_json TEXT,updated_at TEXT,attempts INTEGER NOT NULL DEFAULT 0,max_attempts INTEGER NOT NULL DEFAULT 5,next_attempt_at TEXT,last_attempt_at TEXT,lease_owner TEXT,lease_until TEXT,lease_epoch INTEGER NOT NULL DEFAULT 0,last_error_type TEXT,last_error TEXT,checkpoint_json TEXT NOT NULL DEFAULT '{}',UNIQUE(transcript_id,analysis_type,transcript_content_digest,analysis_spec_version,model_version,prompt_version));
            CREATE TABLE lineage_edges(edge_id TEXT PRIMARY KEY,downstream_type TEXT,downstream_id TEXT,upstream_type TEXT,upstream_id TEXT,upstream_version TEXT,binding_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',upstream_engine_version TEXT,upstream_model_version TEXT,downstream_model_version TEXT,downstream_prompt_version TEXT,downstream_schema_version TEXT,state TEXT DEFAULT 'CURRENT',created_at TEXT,updated_at TEXT,metadata_json TEXT NOT NULL DEFAULT '{}',UNIQUE(downstream_type,downstream_id,upstream_type,upstream_id,upstream_version));
            CREATE TABLE recompute_requests(request_id TEXT PRIMARY KEY,downstream_type TEXT,downstream_id TEXT,upstream_type TEXT,upstream_id TEXT,old_upstream_digest TEXT,new_upstream_digest TEXT,target_analysis_spec_version TEXT,target_model_version TEXT,target_prompt_version TEXT,status TEXT DEFAULT 'PENDING',candidate_analysis_id TEXT,attempts INTEGER DEFAULT 0,max_attempts INTEGER DEFAULT 5,next_attempt_at TEXT,last_attempt_at TEXT,lease_owner TEXT,lease_until TEXT,lease_epoch INTEGER DEFAULT 0,last_error_type TEXT,last_error TEXT,checkpoint_json TEXT DEFAULT '{}',created_at TEXT,updated_at TEXT,completed_at TEXT,metadata_json TEXT DEFAULT '{}',UNIQUE(downstream_type,downstream_id,upstream_type,upstream_id,old_upstream_digest,new_upstream_digest,target_analysis_spec_version,target_model_version,target_prompt_version));
            CREATE TABLE review_items(review_id TEXT PRIMARY KEY,object_type TEXT,object_id TEXT,review_type TEXT,status TEXT,requested_at TEXT,requested_by TEXT,decided_at TEXT,decided_by TEXT,decision_notes TEXT,metadata_json TEXT,UNIQUE(object_type,object_id,review_type));
            CREATE TABLE outbox(outbox_id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,object_type TEXT,object_id TEXT,destination TEXT,status TEXT,attempts INTEGER,max_attempts INTEGER,next_attempt_at TEXT,payload_hash TEXT,payload_json TEXT,scope TEXT,qualification_status TEXT);
            """)

    def tearDown(self):
        self.temp.cleanup()

    def test_sample_is_ignored_and_invalidated_history_does_not_block_full(self):
        sample = self.root / "sample.transcript.json"
        full = self.root / "full.transcript.json"
        sample.write_text("{}")
        full.write_text("{}")
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            conn.execute("INSERT INTO transcripts VALUES('sample','s','sample-digest','COMPLETE',?,'SAMPLE','SAMPLE_NONQUALIFYING',?,'faster-whisper','small')", (str(sample), json.dumps({"coverage_scope":"SAMPLE","sample_only":True,"sample_seconds":300,"source_segment_id":"seg-sample"})))
            conn.execute("INSERT INTO transcripts VALUES('full','s',?,'COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (canonical_source_digest(), str(full), json.dumps(full_meta())))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,output_path,lineage_state,scope,qualification_status,analysis_spec_version,metadata_json) VALUES('old-sample','s','sample','single_session','old','SAMPLE_NONQUALIFYING',NULL,'INVALIDATED','SAMPLE_AUXILIARY','SAMPLE_NONQUALIFYING','legacy',?)", (json.dumps({"qualification_state":"SAMPLE_NONQUALIFYING"}),))
        with patch.object(pipeline, "connect", connect_factory(self.db)), patch.object(
            pipeline, "load_pipeline_config", return_value={"transcript_quality":{"full_session_min_timestamp_coverage_rate":0.9}}
        ):
            self.assertEqual(pipeline.create_analysis_tasks(), 1)
            self.assertEqual(pipeline.create_analysis_tasks(), 0)
        with connect_factory(self.db)() as conn:
            rows = conn.execute("SELECT * FROM analyses ORDER BY analysis_id").fetchall()
            self.assertEqual(len(rows), 2)
            formal = next(row for row in rows if row["analysis_id"] != "old-sample")
            metadata = json.loads(formal["metadata_json"])
            self.assertEqual(metadata["source_transcript_id"], "full")
            self.assertEqual(metadata["qualification_state"], "FULL_SESSION_QUALIFIED")
            edge = conn.execute("SELECT * FROM lineage_edges WHERE downstream_id=?", (formal["analysis_id"],)).fetchone()
            self.assertEqual(edge["upstream_id"], "full")

    def test_noncanonical_full_transcript_is_ignored(self):
        artifact = self.root / "full.transcript.json"; artifact.write_text("{}")
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('different','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            conn.execute("INSERT INTO transcripts VALUES('full','s',?,'COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (canonical_source_digest(), str(artifact), json.dumps(full_meta("stale"))))
        with patch.object(pipeline, "connect", connect_factory(self.db)), patch.object(pipeline, "load_pipeline_config", return_value={}):
            self.assertEqual(pipeline.create_analysis_tasks(), 0)

    def test_same_canonical_segment_with_stale_source_digest_is_ignored(self):
        artifact = self.root / "stale.transcript.json"; artifact.write_text("{}")
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            conn.execute("INSERT INTO transcripts VALUES('stale','s','old-source-digest','COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (str(artifact), json.dumps(full_meta())))
        with patch.object(pipeline, "connect", connect_factory(self.db)), patch.object(pipeline, "load_pipeline_config", return_value={}):
            self.assertEqual(pipeline.create_analysis_tasks(), 0)

    def test_old_spec_does_not_block_new_full_identity_and_same_spec_is_idempotent(self):
        artifact = self.root / "full.transcript.json"; artifact.write_text("{}")
        content = contract.file_sha256(artifact)
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            conn.execute("INSERT INTO transcripts VALUES('full','s',?,'COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (canonical_source_digest(),str(artifact),json.dumps(full_meta())))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) VALUES('old','s','full','single_session',?,'COMPLETE','CURRENT','FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?,'old-spec','old-model','old-prompt','{}')",(content,content))
            conn.execute("INSERT INTO lineage_edges(edge_id,downstream_type,downstream_id,upstream_type,upstream_id,upstream_version,binding_status,state,created_at,metadata_json) VALUES('edge-old','analysis','old','transcript','full',?,'CONTENT_DIGEST_VERIFIED','CURRENT','2026-08-30T00:00:00Z','{}')",(content,))
        with patch.object(pipeline,"connect",connect_factory(self.db)),patch.object(pipeline,"load_pipeline_config",return_value={}):
            self.assertEqual(pipeline.create_analysis_tasks(),1)
            self.assertEqual(pipeline.create_analysis_tasks(),0)
        with connect_factory(self.db)() as conn:
            rows=conn.execute("SELECT analysis_id,lineage_state FROM analyses ORDER BY analysis_id").fetchall()
            requests=conn.execute("SELECT * FROM recompute_requests").fetchall()
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["lineage_state"],"STALE")
        self.assertEqual(len(requests),1)
        self.assertEqual(requests[0]["target_analysis_spec_version"],contract.ANALYSIS_SPEC_VERSION)
        self.assertEqual(requests[0]["target_model_version"],contract.MODEL_VERSION)
        self.assertEqual(requests[0]["target_prompt_version"],contract.PROMPT_VERSION)


class AnalysisConsumerGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name); self.db = self.root / "db.sqlite"
        with connect_factory(self.db)() as conn:
            conn.executescript("""
            CREATE TABLE live_sessions(session_id TEXT PRIMARY KEY,status TEXT,completeness TEXT,metadata_json TEXT);
            CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,status TEXT,lifecycle_status TEXT,checksum TEXT,path TEXT);
            CREATE TABLE transcripts(transcript_id TEXT PRIMARY KEY,session_id TEXT,source_digest TEXT,status TEXT,output_path TEXT,scope TEXT,qualification_status TEXT,metadata_json TEXT,engine TEXT,model TEXT);
            CREATE TABLE analyses(analysis_id TEXT PRIMARY KEY,session_id TEXT,transcript_id TEXT,analysis_type TEXT,source_digest TEXT,status TEXT,output_path TEXT,lineage_state TEXT,scope TEXT,qualification_status TEXT,transcript_content_digest TEXT,analysis_spec_version TEXT,model_version TEXT,prompt_version TEXT,artifact_digest TEXT,metadata_json TEXT,updated_at TEXT,attempts INTEGER NOT NULL DEFAULT 0,max_attempts INTEGER NOT NULL DEFAULT 5,next_attempt_at TEXT,last_attempt_at TEXT,lease_owner TEXT,lease_until TEXT,lease_epoch INTEGER NOT NULL DEFAULT 0,last_error_type TEXT,last_error TEXT,checkpoint_json TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE lineage_edges(edge_id TEXT PRIMARY KEY,downstream_type TEXT,downstream_id TEXT,upstream_type TEXT,upstream_id TEXT,upstream_version TEXT,binding_status TEXT,upstream_engine_version TEXT,upstream_model_version TEXT,downstream_model_version TEXT,downstream_prompt_version TEXT,downstream_schema_version TEXT,state TEXT,created_at TEXT);
            CREATE TABLE evidence_bundles(bundle_id TEXT PRIMARY KEY,object_type TEXT,object_id TEXT,status TEXT,manifest_path TEXT,manifest_hash TEXT,verified_at TEXT,scope TEXT,qualification_status TEXT,metadata_json TEXT,UNIQUE(object_type,object_id));
            CREATE TABLE outbox(outbox_id TEXT PRIMARY KEY,status TEXT,next_attempt_at TEXT,lease_owner TEXT,lease_until TEXT,payload_json TEXT);
            """)

    def tearDown(self): self.temp.cleanup()

    def run_once(self):
        heartbeat = Mock()
        with connect_factory(self.db)() as conn:
            conn.execute(
                "UPDATE analyses SET next_attempt_at=COALESCE(next_attempt_at,'1970-01-01T00:00:00.000Z'),"
                "updated_at=COALESCE(updated_at,'1970-01-01T00:00:00.000Z') "
                "WHERE status IN ('PENDING','WAITING_MODEL','RETRY_WAIT')"
            )
        with patch.object(analysis, "connect", connect_factory(self.db)), patch.object(analysis, "init_db"), patch.object(analysis, "upsert_heartbeat", heartbeat), patch.object(analysis, "request_analysis") as request:
            result = analysis.once()
        return result, request, heartbeat

    def test_manual_sample_analysis_is_invalidated_before_model_call(self):
        artifact = self.root / "sample.json"; artifact.write_text(json.dumps({"segments":[{"start":0,"end":1,"text":"sample"}]}))
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            content_digest=contract.file_sha256(artifact)
            conn.execute("INSERT INTO transcripts VALUES('sample','s','sample-digest','COMPLETE',?,'SAMPLE','SAMPLE_NONQUALIFYING',?,'faster-whisper','small')", (str(artifact), json.dumps({"coverage_scope":"SAMPLE","sample_only":True,"source_segment_id":"seg"})))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) VALUES('a','s','sample','single_session',?,'PENDING','CURRENT',?,?,?,?,?,?,?)", (content_digest,'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',content_digest,contract.ANALYSIS_SPEC_VERSION,contract.MODEL_VERSION,contract.PROMPT_VERSION,json.dumps({"source_transcript_id":"sample"})))
            conn.execute("INSERT INTO lineage_edges VALUES('e','analysis','a','transcript','sample',?,'CONTENT_DIGEST_VERIFIED','faster-whisper','small',?,?,?,'CURRENT','now')",(content_digest,contract.MODEL_VERSION,contract.PROMPT_VERSION,contract.ANALYSIS_SPEC_VERSION))
        result, request, _ = self.run_once()
        request.assert_not_called()
        self.assertEqual(result["source_qualification_blocked"], 1)
        with connect_factory(self.db)() as conn:
            row = conn.execute("SELECT status,lineage_state,metadata_json FROM analyses WHERE analysis_id='a'").fetchone()
            self.assertEqual((row["status"], row["lineage_state"]), ("SAMPLE_NONQUALIFYING", "INVALIDATED"))
            self.assertEqual(json.loads(row["metadata_json"])["qualification_state"], "SAMPLE_NONQUALIFYING")

    def test_lineage_mismatch_is_blocked_before_model_call(self):
        artifact = self.root / "full.json"; artifact.write_text(json.dumps({"segments":[{"start":0,"end":1,"text":"full"}]}))
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            content_digest=contract.file_sha256(artifact)
            conn.execute("INSERT INTO transcripts VALUES('full','s',?,'COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (canonical_source_digest(), str(artifact), json.dumps(full_meta())))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) VALUES('a','s','full','single_session',?,'PENDING','CURRENT',?,?,?,?,?,?,?)", (content_digest,'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',content_digest,contract.ANALYSIS_SPEC_VERSION,contract.MODEL_VERSION,contract.PROMPT_VERSION,json.dumps({"source_transcript_id":"full"})))
            conn.execute("INSERT INTO lineage_edges VALUES('e','analysis','a','transcript','other',?,'CONTENT_DIGEST_VERIFIED','faster-whisper','small',?,?,?,'CURRENT','now')",(content_digest,contract.MODEL_VERSION,contract.PROMPT_VERSION,contract.ANALYSIS_SPEC_VERSION))
        result, request, _ = self.run_once()
        request.assert_not_called(); self.assertEqual(result["source_qualification_blocked"], 1)
        with connect_factory(self.db)() as conn:
            self.assertEqual(conn.execute("SELECT status FROM analyses WHERE analysis_id='a'").fetchone()[0], "BLOCKED_SOURCE_QUALIFICATION")

    def test_exact_qualified_full_transcript_can_complete(self):
        transcript = self.root / "full.json"
        transcript.write_text(json.dumps({"duration":10,"segments":[{"start":0,"end":10,"text":"full"}]}))
        analysis_root = self.root / "analysis"
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)", (json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            content_digest=contract.file_sha256(transcript)
            conn.execute("INSERT INTO transcripts VALUES('full','s',?,'COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')", (canonical_source_digest(), str(transcript), json.dumps(full_meta())))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) VALUES('a','s','full','single_session',?,'PENDING','CURRENT',?,?,?,?,?,?,?)", (content_digest,'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',content_digest,contract.ANALYSIS_SPEC_VERSION,contract.MODEL_VERSION,contract.PROMPT_VERSION,json.dumps({"source_transcript_id":"full","qualification_state":"FULL_SESSION_QUALIFIED","formal_analysis_eligible":True})))
            conn.execute("INSERT INTO lineage_edges VALUES('e','analysis','a','transcript','full',?,'CONTENT_DIGEST_VERIFIED','faster-whisper','small',?,?,?,'CURRENT','now')",(content_digest,contract.MODEL_VERSION,contract.PROMPT_VERSION,contract.ANALYSIS_SPEC_VERSION))
            conn.execute("UPDATE analyses SET next_attempt_at='1970-01-01T00:00:00.000Z',updated_at='1970-01-01T00:00:00.000Z' WHERE analysis_id='a'")
        heartbeat = Mock(); enqueue = Mock(return_value="out-test")
        def strict_response(chunk, **_kwargs):
            payload = analysis.empty_chunk_result()
            payload["course_content"] = [{
                "summary": "课程",
                "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
            }]
            return analysis.validate_chunk_result(payload, chunk), {
                "response_id": "test", "finish_reason": "stop",
                "usage": {}, "attempt": 1, "content_hash": "test",
            }
        with patch.object(analysis, "connect", connect_factory(self.db)), patch.object(analysis, "init_db"), patch.object(analysis, "upsert_heartbeat", heartbeat), patch.object(analysis, "ANALYSIS_ROOT", analysis_root), patch.object(analysis, "request_chunk", side_effect=strict_response), patch.object(analysis, "enqueue_outbox_conn", enqueue):
            result = analysis.once()
        self.assertEqual(result["completed"], 1); enqueue.assert_called_once()
        with connect_factory(self.db)() as conn:
            row = conn.execute("SELECT status,lineage_state,metadata_json FROM analyses WHERE analysis_id='a'").fetchone()
            self.assertEqual((row["status"],row["lineage_state"]),("COMPLETE","CURRENT"))
            self.assertTrue(json.loads(row["metadata_json"])["formal_analysis_eligible"])
            bundle = conn.execute("SELECT status,metadata_json FROM evidence_bundles WHERE object_id='a'").fetchone()
            self.assertEqual(bundle["status"], "REQUIRED")
            self.assertEqual(json.loads(bundle["metadata_json"])["qualification_state"], "FULL_SESSION_QUALIFIED")

    def test_same_segment_id_with_stale_source_digest_is_blocked(self):
        artifact=self.root/"stale.json";artifact.write_text(json.dumps({"segments":[{"start":0,"end":1,"text":"stale"}]}))
        content_digest=contract.file_sha256(artifact)
        with connect_factory(self.db)() as conn:
            conn.execute("INSERT INTO live_sessions VALUES('s','MEDIA_COMPLETE','COMPLETE',?)",(json.dumps(session_meta()),))
            conn.execute("INSERT INTO recording_segments VALUES('seg-full','s','COMPLETE','CANONICAL_ACTIVE','media-checksum','media.ts')")
            conn.execute("INSERT INTO transcripts VALUES('full','s','old-source-digest','COMPLETE',?,'FULL_SESSION','FULL_SESSION_QUALIFIED',?,'faster-whisper','small')",(str(artifact),json.dumps(full_meta())))
            conn.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,metadata_json) VALUES('a','s','full','single_session',?,'PENDING','CURRENT','FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?,?,?,?,?)",(content_digest,content_digest,contract.ANALYSIS_SPEC_VERSION,contract.MODEL_VERSION,contract.PROMPT_VERSION,json.dumps({"source_transcript_id":"full"})))
            conn.execute("INSERT INTO lineage_edges VALUES('e','analysis','a','transcript','full',?,'CONTENT_DIGEST_VERIFIED','faster-whisper','small',?,?,?,'CURRENT','now')",(content_digest,contract.MODEL_VERSION,contract.PROMPT_VERSION,contract.ANALYSIS_SPEC_VERSION))
        result,request,_=self.run_once();request.assert_not_called();self.assertEqual(result["source_qualification_blocked"],1)
        with connect_factory(self.db)() as conn:self.assertEqual(conn.execute("SELECT status FROM analyses WHERE analysis_id='a'").fetchone()[0],"BLOCKED_SOURCE_QUALIFICATION")


class EvidenceAndProjectionTests(unittest.TestCase):
    def test_sample_projection_is_explicitly_nonformal(self):
        fields, state = project.analysis_projection_fields({"analysis_id":"a","session_id":"s","analysis_type":"single_session","status":"SAMPLE_NONQUALIFYING","lineage_state":"INVALIDATED","scope":"SAMPLE_AUXILIARY","qualification_status":"SAMPLE_NONQUALIFYING","output_path":"kept.json","metadata_json":json.dumps({"qualification_state":"SAMPLE_NONQUALIFYING","formal_analysis_eligible":False})})
        self.assertEqual(state, "SAMPLE_NONQUALIFYING")
        self.assertEqual(fields["分析类型"], "部分场辅助")
        self.assertEqual(fields["状态"], "证据不足")
        self.assertIn("SAMPLE_NONQUALIFYING", fields["证据说明"])
        self.assertEqual(fields["分析文档"], "kept.json")

    def test_qualified_projection_remains_formal(self):
        fields, state = project.analysis_projection_fields({"analysis_id":"a","session_id":"s","analysis_type":"single_session","status":"COMPLETE","lineage_state":"CURRENT","scope":"FORMAL_SINGLE_SESSION","qualification_status":"FULL_SESSION_QUALIFIED","output_path":"full.json","metadata_json":json.dumps({"qualification_state":"FULL_SESSION_QUALIFIED","formal_analysis_eligible":True})})
        self.assertEqual(state, "FULL_SESSION_QUALIFIED")
        self.assertEqual(fields["分析类型"], "单场分析")
        self.assertEqual(fields["状态"], "完成")

    def test_evidence_worker_invalidates_sample_bundle_without_deleting_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); db=root/"db.sqlite"; artifact=root/"sample-analysis.json";artifact.write_text("{}")
            with connect_factory(db)() as conn:
                conn.executescript("""
                CREATE TABLE analyses(analysis_id TEXT PRIMARY KEY,session_id TEXT,transcript_id TEXT,analysis_type TEXT,status TEXT,output_path TEXT,lineage_state TEXT,scope TEXT,qualification_status TEXT,transcript_content_digest TEXT,analysis_spec_version TEXT,model_version TEXT,prompt_version TEXT,artifact_digest TEXT,metadata_json TEXT);
                CREATE TABLE evidence_bundles(bundle_id TEXT PRIMARY KEY,object_type TEXT,object_id TEXT,status TEXT,manifest_path TEXT,manifest_hash TEXT,verified_at TEXT,scope TEXT,qualification_status TEXT,metadata_json TEXT);
                """)
                conn.execute("INSERT INTO analyses(analysis_id,status,lineage_state,scope,qualification_status,metadata_json) VALUES('a','SAMPLE_NONQUALIFYING','INVALIDATED','SAMPLE_AUXILIARY','SAMPLE_NONQUALIFYING',?)",(json.dumps({"qualification_state":"SAMPLE_NONQUALIFYING","formal_analysis_eligible":False}),))
                conn.execute("INSERT INTO evidence_bundles VALUES('b','analysis','a','REQUIRED',?,?,NULL,'SAMPLE_AUXILIARY','SAMPLE_NONQUALIFYING','{}')",(str(artifact),hashlib.sha256(artifact.read_bytes()).hexdigest()))
            with patch.object(evidence,"connect",connect_factory(db)),patch.object(evidence,"init_db"),patch.object(evidence,"upsert_heartbeat"):
                result=evidence.once()
            self.assertEqual(result["blocked"],1);self.assertTrue(artifact.is_file())
            with connect_factory(db)() as conn:
                self.assertEqual(conn.execute("SELECT status FROM evidence_bundles WHERE bundle_id='b'").fetchone()[0],"INVALIDATED_SAMPLE")

    def formal_evidence_case(self, directory, *, analysis_digest=None, bundle_digest=None, lineage_digest=None):
        root=Path(directory);db=root/"db.sqlite";transcript=root/"full.transcript.json";analysis_path=root/"analysis.json"
        transcript.write_text(json.dumps({"segments":[{"start":0,"end":1,"text":"x"}]}))
        transcript_digest=contract.file_sha256(transcript)
        analysis_path.write_text(json.dumps({"analysis_id":"a","session_id":"s","transcript_id":"t","result":{}},sort_keys=True))
        actual=contract.file_sha256(analysis_path)
        with connect_factory(db)() as conn:
            conn.executescript("""
            CREATE TABLE analyses(analysis_id TEXT PRIMARY KEY,session_id TEXT,transcript_id TEXT,analysis_type TEXT,status TEXT,output_path TEXT,lineage_state TEXT,scope TEXT,qualification_status TEXT,transcript_content_digest TEXT,analysis_spec_version TEXT,model_version TEXT,prompt_version TEXT,artifact_digest TEXT,metadata_json TEXT);
            CREATE TABLE transcripts(transcript_id TEXT PRIMARY KEY,output_path TEXT,scope TEXT,qualification_status TEXT,engine TEXT,model TEXT);
            CREATE TABLE lineage_edges(edge_id TEXT PRIMARY KEY,downstream_type TEXT,downstream_id TEXT,upstream_type TEXT,upstream_id TEXT,upstream_version TEXT,binding_status TEXT,upstream_engine_version TEXT,upstream_model_version TEXT,downstream_model_version TEXT,downstream_prompt_version TEXT,downstream_schema_version TEXT,state TEXT);
            CREATE TABLE evidence_bundles(bundle_id TEXT PRIMARY KEY,object_type TEXT,object_id TEXT,status TEXT,manifest_path TEXT,manifest_hash TEXT,verified_at TEXT,scope TEXT,qualification_status TEXT,metadata_json TEXT);
            """)
            meta={"qualification_state":"FULL_SESSION_QUALIFIED","formal_analysis_eligible":True,"artifact_identity_mode":"LEGACY_ROOT_IDS_PLUS_THREE_WAY_DIGEST"}
            conn.execute("INSERT INTO analyses VALUES('a','s','t','single_session','COMPLETE',?,'CURRENT','FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?,?,?,?,?,?)",(str(analysis_path),transcript_digest,"single-session-evidence-v2",contract.MODEL_VERSION,"deepseek-evidence-chunks-v3",analysis_digest or actual,json.dumps(meta)))
            conn.execute("INSERT INTO transcripts VALUES('t',?,'FULL_SESSION','FULL_SESSION_QUALIFIED','faster-whisper','small')",(str(transcript),))
            conn.execute("INSERT INTO lineage_edges VALUES('e','analysis','a','transcript','t',?,'CONTENT_DIGEST_VERIFIED','faster-whisper','small',?,?,?,'CURRENT')",(lineage_digest or transcript_digest,contract.MODEL_VERSION,'deepseek-evidence-chunks-v3','single-session-evidence-v2'))
            conn.execute("INSERT INTO evidence_bundles VALUES('b','analysis','a','REQUIRED',?,?,NULL,'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?)",(str(analysis_path),bundle_digest or actual,json.dumps({"audit_marker":"preserve-me"})))
        with patch.object(evidence,"connect",connect_factory(db)),patch.object(evidence,"init_db"),patch.object(evidence,"upsert_heartbeat"):
            result=evidence.once()
        with connect_factory(db)() as conn:row=conn.execute("SELECT status,metadata_json FROM evidence_bundles WHERE bundle_id='b'").fetchone()
        return result,row

    def test_formal_evidence_requires_three_way_artifact_digest_and_exact_lineage(self):
        cases=[("analysis","1"*64,None,None,"artifact_digest_three_way_mismatch"),("bundle",None,"2"*64,None,"artifact_digest_three_way_mismatch"),("lineage",None,None,"3"*64,"analysis_lineage_identity_mismatch")]
        for name,analysis_digest,bundle_digest,lineage_digest,reason in cases:
            with self.subTest(name=name),tempfile.TemporaryDirectory() as directory:
                result,row=self.formal_evidence_case(directory,analysis_digest=analysis_digest,bundle_digest=bundle_digest,lineage_digest=lineage_digest)
                self.assertEqual(result["blocked"],1);self.assertEqual(row["status"],"BLOCKED_EVIDENCE")
                metadata=json.loads(row["metadata_json"]);self.assertEqual(metadata["reason"],reason);self.assertEqual(metadata["audit_marker"],"preserve-me")

    def test_legacy_formal_artifact_reverifies_without_losing_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            result,row=self.formal_evidence_case(directory)
        self.assertEqual(result["verified"],1);self.assertEqual(row["status"],"VERIFIED")
        metadata=json.loads(row["metadata_json"]);self.assertEqual(metadata["audit_marker"],"preserve-me");self.assertEqual(metadata["identity_mode"],"LEGACY_ROOT_IDS_PLUS_THREE_WAY_DIGEST")


class MigrationHelperTests(unittest.TestCase):
    def test_sample_path_is_fail_closed_even_for_legacy_null_scope(self):
        row={"status":"COMPLETE","output_path":"x.sample300s.transcript.json","source_path":"x.sample300s.wav","metadata_json":"{}"}
        self.assertEqual(migration.transcript_kind(row,set()),"SAMPLE")

    def test_plan_hash_is_deterministic(self):
        value={"a":[2,1],"b":"中文"}
        self.assertEqual(migration.digest(value),migration.digest(value))

    def test_migration_rejects_wrong_artifact_root_even_when_transcript_matches(self):
        identity={"analysis_id":"copied","session_id":"s","transcript_id":"t"}
        self.assertFalse(migration.artifact_matches(identity,{"analysis_id":"a","session_id":"s"},["t"]))
        identity["analysis_id"]="a";identity["session_id"]="other"
        self.assertFalse(migration.artifact_matches(identity,{"analysis_id":"a","session_id":"s"},["t"]))


class OutboxQualificationTests(unittest.TestCase):
    def test_correction_receipt_preserves_sample_nonqualification(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/"runtime.sqlite"
            runtime.init_db(db)
            original=runtime.connect
            def local_connect(*_args,**_kwargs):return original(db)
            with local_connect() as conn:
                outbox_id=runtime.enqueue_outbox_conn(conn,object_type="semantic_projection",object_id="a",destination="feishu_base",payload={"analysis_id":"a","correction":True,"correction_version":1},scope="SAMPLE_AUXILIARY",qualification_status="SAMPLE_NONQUALIFYING")
            with patch.object(runtime,"connect",side_effect=local_connect):
                runtime.complete_outbox(outbox_id,{"status":"VERIFIED"})
            with local_connect() as conn:
                receipt=conn.execute("SELECT scope,qualification_status,status FROM delivery_receipts WHERE outbox_id=?",(outbox_id,)).fetchone()
            self.assertEqual(tuple(receipt),("SAMPLE_AUXILIARY","SAMPLE_NONQUALIFYING","VERIFIED"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
