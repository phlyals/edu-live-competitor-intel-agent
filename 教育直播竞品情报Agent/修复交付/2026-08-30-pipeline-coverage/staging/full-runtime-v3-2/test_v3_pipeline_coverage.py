import hashlib
import importlib.util
import json
import math
import sys
from pathlib import Path
from unittest.mock import patch

RUNTIME_V3 = Path(__file__).resolve().parent
sys.path.insert(0, str(RUNTIME_V3))

from test_v3_workflow import RuntimeCase


spec = importlib.util.spec_from_file_location(
    "staged_v3_pipeline_worker", Path(__file__).with_name("v3_pipeline_worker.py")
)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


class CoverageUnitTests(RuntimeCase):
    def test_complete_timeline_is_qualified(self):
        result = pipeline.timestamp_coverage(
            {"segments": [{"start": 0, "end": 300}, {"start": 300, "end": 600}]},
            600,
            0.90,
        )
        self.assertTrue(result["timestamps_valid"])
        self.assertTrue(result["is_qualified"])
        self.assertTrue(result["meets_target"])
        self.assertEqual(result["target_coverage_rate"], 0.95)
        self.assertEqual(result["covered_duration_seconds"], 600)
        self.assertEqual(result["coverage_rate"], 1)
        self.assertEqual(result["gaps"], [])

    def test_one_percent_timeline_is_quality_blocked(self):
        result = pipeline.timestamp_coverage({"segments": [{"start": 0, "end": 6}]}, 600, 0.90)
        self.assertTrue(result["timestamps_valid"])
        self.assertFalse(result["is_qualified"])
        self.assertAlmostEqual(result["coverage_rate"], 0.01)
        self.assertEqual(result["gaps"], [{"start_time": 6.0, "end_time": 600.0}])

    def test_internal_gap_uses_union_instead_of_first_to_last_span(self):
        result = pipeline.timestamp_coverage(
            {"segments": [{"start": 0, "end": 400}, {"start": 600, "end": 1000}]},
            1000,
            0.90,
        )
        self.assertEqual(result["covered_duration_seconds"], 800)
        self.assertAlmostEqual(result["coverage_rate"], 0.8)
        self.assertEqual(result["gaps"], [{"start_time": 400.0, "end_time": 600.0}])
        self.assertFalse(result["is_qualified"])

    def test_overlaps_are_not_double_counted(self):
        result = pipeline.timestamp_coverage(
            {"segments": [{"start": 0, "end": 80}, {"start": 20, "end": 100}]},
            100,
            0.90,
        )
        self.assertEqual(result["raw_covered_duration_seconds"], 160)
        self.assertEqual(result["covered_duration_seconds"], 100)
        self.assertEqual(result["raw_coverage_rate"], 1.6)
        self.assertEqual(result["coverage_rate"], 1)

    def test_invalid_segment_shapes_and_timestamps_fail_closed(self):
        fixtures = [
            ({}, "SEGMENTS_NOT_LIST"),
            ({"segments": []}, "SEGMENTS_EMPTY"),
            ({"segments": ["bad"]}, "SEGMENT_NOT_OBJECT"),
            ({"segments": [{"start": math.nan, "end": 1}]}, "NON_FINITE_TIMESTAMP"),
            ({"segments": [{"start": -1, "end": 1}]}, "TIMESTAMP_OUT_OF_RANGE"),
            ({"segments": [{"start": 10, "end": 10}]}, "TIMESTAMP_OUT_OF_RANGE"),
            ({"segments": [{"start": 0, "end": 101}]}, "TIMESTAMP_OUT_OF_RANGE"),
        ]
        for payload, expected in fixtures:
            with self.subTest(payload=payload):
                result = pipeline.timestamp_coverage(payload, 100, 0.90)
                self.assertFalse(result["timestamps_valid"])
                self.assertFalse(result["is_qualified"])
                self.assertIn(expected, [item["code"] for item in result["validation_errors"]])


class PipelineCoverageIntegrationTests(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.pipeline_patch = patch.object(pipeline, "connect", side_effect=self.connect)
        self.pipeline_patch.start()
        self.completed = self.root / "completed"
        self.completed.mkdir()
        self.media = self.completed / "整场直播.ts"
        self.media.write_bytes(b"canonical media")
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at) "
                "VALUES('c','buyin','c','test','2026-01-01','2026-01-01')"
            )
            conn.execute(
                "INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,live_url,live_status) "
                "VALUES('m','c','ACTIVE','https://live.invalid/1','OFFLINE_CONFIRMED')"
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,ended_at,completeness,source_url,metadata_json) "
                "VALUES('s','m','room','MEDIA_COMPLETE','2026-08-30T00:00:00Z','2026-08-30T00:10:00Z','COMPLETE','https://live.invalid/1',?)",
                (json.dumps({"media_coverage": {"continuous_capture": True}}),),
            )
            conn.execute(
                "INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,captured_to,status,bytes,lifecycle_status) "
                "VALUES('seg','s',?,'testhash','2026-08-30T00:00:00Z','2026-08-30T00:10:00Z','COMPLETE',10,'CANONICAL_ACTIVE')",
                (str(self.media),),
            )

    def tearDown(self):
        self.pipeline_patch.stop()
        super().tearDown()

    @staticmethod
    def source_digest():
        return hashlib.sha256(b"FULL_SESSION:testhash").hexdigest()

    def test_one_percent_full_session_cannot_be_complete(self):
        def extract(_media, audio, **_kwargs):
            audio.write_bytes(b"audio")
            return True

        def transcribe(command, **_kwargs):
            output = Path(command[command.index("--output") + 1])
            output.write_text(json.dumps({"status": "READY", "duration": 600, "segments": [{"start": 0, "end": 6, "text": "sample"}]}))
            return type("Process", (), {"stdout": json.dumps({"status": "READY"}), "stderr": "", "returncode": 0})()

        with patch.object(pipeline, "extract_audio", side_effect=extract), \
                patch.object(pipeline, "media_duration", return_value=600), \
                patch.object(pipeline.subprocess, "run", side_effect=transcribe):
            self.assertEqual(pipeline.transcribe_pending(), 0)
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM transcripts WHERE session_id='s'").fetchone()
        metadata = json.loads(row["metadata_json"])
        self.assertEqual(row["status"], "QUALITY_BLOCKED")
        self.assertAlmostEqual(metadata["timestamp_coverage"]["coverage_rate"], 0.01)
        self.assertEqual(metadata["quality_gate_status"], "QUALITY_BLOCKED")
        output_path = Path(row["output_path"])
        output_before = output_path.read_bytes()
        with patch.object(pipeline, "extract_audio") as extraction, \
                patch.object(pipeline, "timestamp_coverage") as gate_calculation, \
                patch.object(pipeline.subprocess, "run") as asr:
            self.assertEqual(pipeline.transcribe_pending(), 0)
        extraction.assert_not_called()
        gate_calculation.assert_not_called()
        asr.assert_not_called()
        self.assertEqual(output_path.read_bytes(), output_before)

    def test_existing_complete_artifact_without_coverage_is_revalidated_without_asr(self):
        audio = self.completed / "existing.opus"
        output = self.completed / "existing.transcript.json"
        audio.write_bytes(b"audio")
        output.write_text(json.dumps({"status": "READY", "duration": 600, "segments": [{"start": 0, "end": 6, "text": "old"}]}))
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,language,source_path,output_path,created_at,metadata_json) "
                "VALUES('old','s',?,'faster-whisper','small','COMPLETE','zh',?,?, '2026-08-30T00:11:00Z',?)",
                (self.source_digest(), str(audio), str(output), json.dumps({"coverage_scope": "FULL_SESSION"})),
            )
        with patch.object(pipeline, "extract_audio", return_value=True), \
                patch.object(pipeline, "media_duration", return_value=600), \
                patch.object(pipeline.subprocess, "run") as asr:
            self.assertEqual(pipeline.transcribe_pending(), 0)
        asr.assert_not_called()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM transcripts WHERE transcript_id='old'").fetchone()
        self.assertEqual(row["status"], "QUALITY_BLOCKED")
        self.assertIn("timestamp_coverage", json.loads(row["metadata_json"]))

    def test_qualified_full_session_is_complete(self):
        def extract(_media, audio, **_kwargs):
            audio.write_bytes(b"audio")
            return True

        def transcribe(command, **_kwargs):
            output = Path(command[command.index("--output") + 1])
            output.write_text(json.dumps({"status": "READY", "duration": 600, "segments": [{"start": 0, "end": 550, "text": "full"}]}))
            return type("Process", (), {"stdout": json.dumps({"status": "READY"}), "stderr": "", "returncode": 0})()

        with patch.object(pipeline, "extract_audio", side_effect=extract), \
                patch.object(pipeline, "media_duration", return_value=600), \
                patch.object(pipeline.subprocess, "run", side_effect=transcribe):
            self.assertEqual(pipeline.transcribe_pending(), 1)
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM transcripts WHERE session_id='s'").fetchone()
        quality = json.loads(row["metadata_json"])["timestamp_coverage"]
        self.assertEqual(row["status"], "COMPLETE")
        self.assertAlmostEqual(quality["coverage_rate"], 550 / 600)
        self.assertTrue(quality["is_qualified"])
        self.assertFalse(quality["meets_target"])
        self.assertEqual(quality["minimum_coverage_rate"], 0.90)
        self.assertEqual(quality["target_coverage_rate"], 0.95)

    def test_eligible_canonical_pending_is_degraded(self):
        status, details = pipeline.pipeline_health_snapshot()
        self.assertEqual(status, "DEGRADED")
        self.assertEqual(details["eligible_canonical_pending"], 1)
        self.assertIn("ELIGIBLE_CANONICAL_PENDING", details["health_reasons"])

    def test_heartbeat_reports_quality_and_unrecoverable_backlog_as_degraded(self):
        qualified_audio = self.completed / "qualified.opus"
        qualified_output = self.completed / "qualified.transcript.json"
        qualified_audio.write_bytes(b"audio")
        qualified_output.write_text("{}")
        quality = {"coverage_scope": "FULL_SESSION", "timestamp_coverage": {"is_qualified": True, "timestamps_valid": True, "coverage_rate": 0.95, "meets_target": True}}
        blocked_dir = self.completed / "blocked"
        blocked_dir.mkdir()
        blocked_media = blocked_dir / "整场直播.ts"
        blocked_media.write_bytes(b"blocked canonical media")
        blocked_digest = hashlib.sha256(b"FULL_SESSION:blockhash").hexdigest()
        blocked_quality = {
            "coverage_scope": "FULL_SESSION",
            "timestamp_coverage": {
                "schema_version": 1,
                "minimum_coverage_rate": 0.90,
                "target_coverage_rate": 0.95,
                "is_qualified": False,
                "timestamps_valid": True,
                "coverage_rate": 0.10,
                "meets_target": False,
            },
        }
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,output_path,created_at,metadata_json) "
                "VALUES('qualified','s',?,'faster-whisper','small','COMPLETE',?,?, '2026-08-30T00:11:00Z',?)",
                (self.source_digest(), str(qualified_audio), str(qualified_output), json.dumps(quality)),
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,completeness,source_url,metadata_json) "
                "VALUES('blocked-session','m','blocked-room','MEDIA_COMPLETE','COMPLETE','https://live.invalid/blocked',?)",
                (json.dumps({"media_coverage": {"continuous_capture": True}}),),
            )
            conn.execute(
                "INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,status,bytes,lifecycle_status) "
                "VALUES('blocked-seg','blocked-session',?,'blockhash','2026-08-30T00:00:00Z','COMPLETE',10,'CANONICAL_ACTIVE')",
                (str(blocked_media),),
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,output_path,created_at,metadata_json) "
                "VALUES('blocked','blocked-session',?,'faster-whisper','small','QUALITY_BLOCKED',?,?, '2026-08-30T00:12:00Z',?)",
                (blocked_digest, str(qualified_audio), str(qualified_output), json.dumps(blocked_quality)),
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,metadata_json) "
                "VALUES('historical-blocked','s','historical','faster-whisper','small','QUALITY_BLOCKED','/missing/history','2026-08-29T00:00:00Z','{}')"
            )
            conn.execute(
                "INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,completeness,source_url,metadata_json) "
                "VALUES('lost','m','lost-room','MEDIA_COMPLETE','PARTIAL','https://live.invalid/lost','{}')"
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,metadata_json) "
                "VALUES('lost-transcript','lost','lost','faster-whisper','small','WAITING_TOOL','/missing/source','2026-08-30T00:13:00Z','{}')"
            )
            conn.execute(
                "INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,source_path,created_at,metadata_json) "
                "VALUES('cancelled','lost','cancelled','faster-whisper','small','CANCELLED_SUPERSEDED_SOURCE','/missing/cancelled','2026-08-30T00:14:00Z','{}')"
            )

        status, details = pipeline.pipeline_health_snapshot()
        self.assertEqual(status, "DEGRADED")
        self.assertEqual(details["full_session_qualified"], 1)
        self.assertEqual(details["full_session_meets_target"], 1)
        self.assertEqual(details["quality_blocked"], 1)
        self.assertEqual(details["eligible_canonical_pending"], 1)
        self.assertEqual(details["stale_backlog"]["unrecoverable"], 1)
        self.assertEqual(details["stale_backlog"]["missing_source"], 1)
        self.assertEqual(details["stale_backlog"]["missing_output"], 1)
        self.assertEqual(
            details["health_reasons"],
            ["FULL_SESSION_QUALITY_BLOCKED", "UNRECOVERABLE_TRANSCRIPT_BACKLOG", "ELIGIBLE_CANONICAL_PENDING"],
        )

        with patch.object(pipeline, "register_segments", return_value=0), \
                patch.object(pipeline, "transcribe_pending", return_value=0), \
                patch.object(pipeline, "create_analysis_tasks", return_value=0), \
                patch.object(pipeline, "upsert_heartbeat") as heartbeat:
            result = pipeline.once()
        heartbeat.assert_called_once()
        self.assertEqual(heartbeat.call_args.args[1], "DEGRADED")
        self.assertEqual(result["full_session_qualified"], 1)


if __name__ == "__main__":
    import unittest

    unittest.main(verbosity=2)
