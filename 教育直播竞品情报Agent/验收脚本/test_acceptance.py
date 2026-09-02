"""Deterministic unit checks + isolated PostgreSQL/real FFprobe CLI integration.

All fixture writes go to a temporary, Unix-socket-only PostgreSQL cluster.
Never consumes the production DSN. Requires initdb, pg_ctl, ffprobe on PATH.
"""
from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import wave

import acceptance_common as common
import check_analysis as analysis
import check_recording as recording
import check_transcription as transcription


def report(refs=None):
    return {"modules": [{"name": name, "timestamps": refs if refs is not None else [{"start": 0, "end": 2}]}
                        for name in common.EXPECTED_MODULES]}


class CoverageTests(unittest.TestCase):
    def test_overlap_nested_adjacent_and_unordered(self):
        data = {"segments": [{"start": a, "end": b, "text": "字"} for a, b in [(4, 8), (0, 4), (2, 3), (9, 10)]]}
        result = transcription.calculate(data, 10)
        self.assertEqual(result["covered_segments"], [common.segment(0, 8), common.segment(9, 10)])
        self.assertEqual(result["gaps"], [common.segment(8, 9)])
        self.assertEqual(result["coverage_rate"], .9)
        self.assertEqual(result["transcript_total_chars"], 4)

    def test_leading_and_trailing_gaps(self):
        r = transcription.calculate({"segments": [{"start_time": 2, "end_time": 8, "text": "你好"}]}, 10)
        self.assertEqual(r["gaps"], [common.segment(0, 2), common.segment(8, 10)])
        self.assertEqual(r["coverage_rate"], .6)

    def test_empty_silence_is_zero(self):
        r = transcription.calculate({"segments": []}, 10)
        self.assertEqual(r["coverage_rate"], 0)
        self.assertEqual(r["gaps"], [common.segment(0, 10)])

    def test_missing_timestamps_are_not_zero_coverage(self):
        for data in [{"text": "无时间戳"}, {"segments": [{"text": "无时间戳"}]}, {"segments": [], "text": "正文"}]:
            with self.subTest(data=data):
                result = transcription.calculate(data, 10)
                self.assertIsNone(result["coverage_rate"])
                self.assertIn("无法计算覆盖率", result["message"])
                self.assertEqual(result["status"], "UNCOMPUTABLE")

    def test_partly_missing_timestamps_do_not_create_false_gaps(self):
        r = transcription.calculate({"segments": [{"start": 0, "end": 5, "text": "甲"}, {"text": "乙"}]}, 10)
        self.assertIsNone(r["coverage_rate"])
        self.assertEqual(r["covered_segments"], [common.segment(0, 5)])
        self.assertEqual(r["gaps"], [])
        self.assertEqual(r["transcript_total_chars"], 2)

    def test_invalid_timestamps(self):
        for row in [{"start": -1, "end": 5}, {"start": 6, "end": 5}, {"start": 0, "end": 20},
                    {"start": 10.0001, "end": 10.0002},
                    {"start": 0, "end": 0}, {"start": True, "end": 4}, {"start": "NaN", "end": 4},
                    {"start": 0, "end": float("inf")}, {"start": 0}, None,
                    {"start": 0, "end": 5, "start_time": 0, "end_time": 5}]:
            with self.subTest(row=row):
                self.assertIsNone(transcription.calculate({"segments": [row]}, 10)["coverage_rate"])

    def test_numeric_strings_and_rounding(self):
        r = transcription.calculate({"segments": [{"start": "0", "end": "3.333"}]}, 10)
        self.assertEqual(r["coverage_rate"], .33)

    def test_tiny_gap_is_preserved(self):
        r = transcription.calculate({"segments": [{"start": 0, "end": 5}, {"start": 5.001, "end": 10}]}, 10)
        self.assertEqual(r["gaps"], [common.segment(5, 5.001)])

    def test_text_not_double_counted(self):
        r = transcription.calculate({"text": "你好🙂", "segments": [{"start": 0, "end": 10, "text": "你好🙂"}]}, 10)
        self.assertEqual(r["transcript_total_chars"], 3)


class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.evidence = transcription.calculate({"segments": [{"start": 0, "end": 10}]}, 10)

    def test_all_modules(self):
        self.assertTrue(analysis.validate(report(), self.evidence)["is_complete"])

    def test_named_module_map(self):
        payload = {"result": {"modules": {r["name"]: {"evidence_refs": r["timestamps"]} for r in report()["modules"]}}}
        self.assertTrue(analysis.validate(payload, self.evidence)["is_complete"])

    def test_missing_module(self):
        payload = report()
        payload["modules"].pop()
        r = analysis.validate(payload, self.evidence)
        self.assertEqual(r["missing_modules"], ["答疑"])
        self.assertFalse(r["is_complete"])

    def test_missing_refs(self):
        payload = report()
        del payload["modules"][0]["timestamps"]
        r = analysis.validate(payload, self.evidence)
        self.assertFalse(r["modules_with_timestamps"][0]["has_timestamps"])
        self.assertFalse(r["is_complete"])

    def test_empty_refs(self):
        self.assertFalse(analysis.validate(report([]), self.evidence)["is_complete"])

    def test_whole_reference_must_be_covered(self):
        evidence = transcription.calculate({"segments": [{"start": 0, "end": 4}, {"start": 6, "end": 10}]}, 10)
        r = analysis.validate(report([{"start": 2, "end": 8}]), evidence)
        self.assertFalse(r["is_complete"])
        self.assertFalse(r["timestamps_in_coverage"][0]["in_coverage"])

    def test_any_bad_reference_fails(self):
        self.assertFalse(analysis.validate(report([{"start": 0, "end": 1}, {"start": 9, "end": 11}]), self.evidence)["is_complete"])

    def test_point_and_end_boundary(self):
        self.assertTrue(analysis.validate(report([{"start_time": 10, "end_time": 10}]), self.evidence)["is_complete"])

    def test_unknown_coverage_never_passes(self):
        r = analysis.validate(report(), transcription.defaults())
        self.assertFalse(r["is_complete"])
        self.assertIsNone(r["timestamps_in_coverage"][0]["references"][0]["in_coverage"])

    def test_unrecognised_report_format(self):
        for payload in [{"hook": "开场"}, {"modules": "开场"}, {"modules": [{"name": "开场"}, {"name": "开场"}]},
                        {"modules": ["开场"]}, {"modules": {"开场": "一段文本"}}, {"modules": {"开场": {"name": "干货"}}}]:
            with self.subTest(payload=payload), self.assertRaises(common.CheckError) as error:
                analysis.validate(payload, self.evidence)
            self.assertEqual(error.exception.status, "FORMAT_ERROR")

    def test_malformed_references(self):
        for refs in ["00:00", [{"start": 0}], [{"start": "01:02", "end": 10}], [{"start": -1, "end": 2}]]:
            with self.subTest(refs=refs), self.assertRaises(common.CheckError):
                analysis.validate(report(refs), self.evidence)


class RecordingTests(unittest.TestCase):
    def test_five_percent_inclusive(self):
        for duration, expected in [(95, True), (105, True), (94.99, False), (105.01, False)]:
            with self.subTest(duration=duration), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "recording.wav"
                path.write_bytes(b"test")
                repo = unittest.mock.Mock()
                repo.session.return_value = {"started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:01:40Z"}
                repo.recording_path.return_value = path
                with patch.object(recording, "probe", return_value=(duration, 1_234_567)):
                    result = recording.defaults()
                    recording.check(Namespace(session_id="s"), repo, result)
                self.assertEqual(result["is_complete"], expected)
                self.assertEqual(result["file_size_mb"], 1.18)

    def test_timezone_offset(self):
        self.assertEqual(common.expected_duration({"started_at": "2026-01-01T08:00:00+08:00", "ended_at": "2026-01-01T00:00:10Z"}), 10)

    def test_datetime_objects(self):
        self.assertEqual(common.expected_duration({"started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                                                   "ended_at": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)}), 60)

    def test_invalid_session_time(self):
        for end in [None, "invalid", "2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:01:00"]:
            with self.subTest(end=end), self.assertRaises(common.CheckError):
                common.expected_duration({"started_at": "2026-01-01T00:00:00Z", "ended_at": end})


class IOTests(unittest.TestCase):
    def test_nonlocal_paths_are_not_fetched(self):
        for value in [None, "https://example.com/media.wav", "file:///tmp/a.wav"]:
            with self.subTest(value=value), self.assertRaises(common.CheckError):
                common.local_path(value, Namespace(data_root=None))

    def test_relative_paths_need_explicit_root(self):
        with self.assertRaises(common.CheckError):
            common.local_path("a.wav", Namespace(data_root=None))
        self.assertEqual(common.local_path("a.wav", Namespace(data_root="/tmp")), Path("/tmp/a.wav").resolve())

    def test_filename_traversal_is_escaped(self):
        args = Namespace(session_id="../s/场次", output_dir="/tmp/acceptance")
        self.assertEqual(common.output_path(args, "check_recording").parent, Path(args.output_dir).resolve())

    def test_chinese_session_id_filename_preserved(self):
        args = Namespace(session_id="场次一", output_dir="/tmp/acceptance")
        self.assertEqual(common.output_path(args, "check_recording").name, "场次一_check_recording.json")

    def test_ffprobe_timeout(self):
        with tempfile.NamedTemporaryFile() as media:
            args = Namespace(ffprobe="ffprobe", probe_timeout=1)
            with patch.object(common.subprocess, "run", side_effect=subprocess.TimeoutExpired("ffprobe", 1)):
                with self.assertRaises(common.CheckError) as error:
                    common.probe(Path(media.name), args)
            self.assertEqual(error.exception.code, "FFPROBE_TIMEOUT")

    def test_no_audio_stream(self):
        with tempfile.NamedTemporaryFile() as media:
            args = Namespace(ffprobe="ffprobe", probe_timeout=1)
            response = Namespace(returncode=0, stdout=json.dumps({"format": {"duration": 10, "size": 100}, "streams": [{"codec_type": "video"}]}))
            with patch.object(common.subprocess, "run", return_value=response):
                with self.assertRaises(common.CheckError) as error:
                    common.probe(Path(media.name), args, audio=True)
            self.assertEqual(error.exception.code, "NO_AUDIO_STREAM")

    def test_strict_json(self):
        with self.assertRaises(common.CheckError):
            common.json_object("```json\n{}\n```", "report")


@contextmanager
def temporary_postgres():
    """This server cannot connect to or modify any existing PostgreSQL cluster."""
    for command in ("initdb", "pg_ctl", "ffprobe"):
        if not shutil.which(command):
            raise unittest.SkipTest(f"integration prerequisite missing: {command}")
    with tempfile.TemporaryDirectory(prefix="edu-acceptance-", dir="/tmp") as folder:
        root = Path(folder)
        data = root / "pgdata"
        subprocess.run(["initdb", "-D", str(data), "-A", "trust", "--no-locale", "-E", "UTF8"],
                       check=True, capture_output=True, timeout=60)
        started = False
        try:
            subprocess.run(["pg_ctl", "-D", str(data), "-l", str(root / "postgres.log"),
                            "-o", f"-F -k {root} -h '' -p 55439", "-w", "start"],
                           check=True, capture_output=True, timeout=60)
            started = True
            yield root, f"host={root} port=55439 dbname=postgres"
        finally:
            if started:
                subprocess.run(["pg_ctl", "-D", str(data), "-m", "fast", "-w", "stop"],
                               check=True, capture_output=True, timeout=60)


class PostgresCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2
        cls.server = temporary_postgres()
        cls.root, cls.dsn = cls.server.__enter__()
        cls.addClassCleanup(cls.server.__exit__, None, None, None)
        cls.conn = psycopg2.connect(cls.dsn)
        cls.conn.autocommit = True
        cls.addClassCleanup(cls.conn.close)
        cls.scripts = Path(__file__).resolve().parent
        cls.audio = cls.root / "known-complete-10s.wav"
        with wave.open(str(cls.audio), "wb") as f:
            f.setnchannels(1)
            f.setsampwidth(2)
            f.setframerate(8000)
            f.writeframes(b"\0\0" * 8000 * 10)
        with cls.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA canonical; CREATE SCHEMA runtime")
            cur.execute("CREATE TABLE canonical.sessions(session_id text PRIMARY KEY, started_at text, ended_at text, recording_path text, audio_path text)")
            cur.execute("CREATE TABLE canonical.transcripts(transcript_id text PRIMARY KEY, session_id text, status text, audio_path text, output_path text, transcript_json jsonb, metadata_json jsonb)")
            cur.execute("CREATE TABLE canonical.analysis_reports(report_id text PRIMARY KEY, session_id text, status text, report_json jsonb, transcript_id text)")
            cur.execute("CREATE TABLE runtime.live_sessions(session_id text PRIMARY KEY, started_at text, ended_at text)")
            cur.execute("CREATE TABLE runtime.recording_segments(session_id text, path text, status text)")
            cur.execute("CREATE TABLE runtime.transcripts(transcript_id text PRIMARY KEY, session_id text, status text, source_path text, output_path text, metadata_json jsonb)")
            cur.execute("CREATE TABLE runtime.analyses(analysis_id text PRIMARY KEY, session_id text, status text, output_path text, lineage_state text)")

    def setUp(self):
        from psycopg2.extras import Json
        self.payload = {"segments": [{"start": 0, "end": 10, "text": "合成测试，不是真实直播。"}]}
        self.artifact = self.root / "transcript.json"
        self.artifact.write_text(json.dumps(self.payload), encoding="utf-8")
        self.report_file = self.root / "report.json"
        self.report_file.write_text(json.dumps({"session_id": "fixture", "transcript_id": "t", "result": report()}), encoding="utf-8")
        with self.conn.cursor() as cur:
            for table in ["canonical.sessions", "canonical.transcripts", "canonical.analysis_reports", "runtime.live_sessions",
                          "runtime.recording_segments", "runtime.transcripts", "runtime.analyses"]:
                cur.execute(f"TRUNCATE {table}")
            cur.execute("INSERT INTO canonical.sessions VALUES ('fixture','2026-01-01T00:00:00Z','2026-01-01T00:00:10Z',%s,%s)", (str(self.audio), str(self.audio)))
            cur.execute("INSERT INTO canonical.transcripts VALUES ('t','fixture','COMPLETE',%s,NULL,%s,'{}')", (str(self.audio), Json(self.payload)))
            cur.execute("INSERT INTO canonical.analysis_reports VALUES ('a','fixture','COMPLETE',%s,'t')", (Json(report()),))
            cur.execute("INSERT INTO runtime.live_sessions VALUES ('fixture','2026-01-01T00:00:00Z','2026-01-01T00:00:10Z')")
            cur.execute("INSERT INTO runtime.recording_segments VALUES ('fixture',%s,'COMPLETE')", (str(self.audio),))
            cur.execute("INSERT INTO runtime.transcripts VALUES ('t','fixture','COMPLETE',%s,%s,%s)",
                        (str(self.audio), str(self.artifact), Json({"coverage_scope": "FULL_SESSION", "sample_only": False})))
            cur.execute("INSERT INTO runtime.analyses VALUES ('a','fixture','COMPLETE',%s,'CURRENT')", (str(self.report_file),))

    def update(self, statement, params=()):
        with self.conn.cursor() as cur:
            cur.execute(statement, params)

    def cli(self, script, *extra, schema="canonical", session="fixture", expected_exit=0):
        env = {**os.environ, "ACCEPTANCE_DATABASE_URL": self.dsn, "PYTHONDONTWRITEBYTECODE": "1"}
        out = self.root / "验收结果" / schema
        proc = subprocess.run([sys.executable, "-B", str(self.scripts / f"{script}.py"), session,
                               "--db-schema", schema, "--output-dir", str(out), *extra],
                              env=env, text=True, capture_output=True, timeout=30)
        self.assertEqual(proc.returncode, expected_exit, proc.stdout + proc.stderr)
        self.assertEqual(proc.stderr, "")
        result = json.loads(proc.stdout)
        path = common.output_path(Namespace(session_id=session, output_dir=str(out)), script)
        self.assertEqual(json.loads(path.read_text()), result)
        return result

    def test_canonical_all_three_pass_real_ffprobe(self):
        r = self.cli("check_recording")
        self.assertEqual(r["duration_seconds"], 10)
        self.assertTrue(r["is_complete"])
        self.assertEqual(self.cli("check_transcription")["coverage_rate"], 1)
        self.assertTrue(self.cli("check_analysis")["is_complete"])
        destination = os.environ.get("ACCEPTANCE_TEST_OUTPUT_DIR")
        if destination:
            for script in ["check_recording", "check_transcription", "check_analysis"]:
                source = self.root / "验收结果" / "canonical" / f"fixture_{script}.json"
                payload = json.loads(source.read_text())
                payload.update(test_fixture=True, test_note="合成的 10 秒 WAV、转录和七模块报告；不是实播验收；临时源文件测试后删除")
                common.write_result(Path(destination).resolve() / source.name, payload)

    def test_runtime_layout_all_three_pass_real_ffprobe(self):
        for script in ["check_recording", "check_transcription", "check_analysis"]:
            self.assertEqual(self.cli(script, schema="runtime")["status"], "PASS")

    def test_database_readonly_enforced(self):
        import psycopg2
        args = common.parser("test").parse_args(["fixture", "--db-schema", "canonical"])
        with patch.dict(os.environ, {"ACCEPTANCE_DATABASE_URL": self.dsn}):
            with common.repository(args) as repo:
                with repo.conn.cursor() as cur:
                    cur.execute("SHOW transaction_read_only")
                    self.assertEqual(cur.fetchone()[0], "on")
                    with self.assertRaises(psycopg2.errors.ReadOnlySqlTransaction):
                        cur.execute("UPDATE canonical.sessions SET ended_at=NULL")

    def test_business_data_and_media_unchanged(self):
        def snapshot():
            data = []
            with self.conn.cursor() as cur:
                for table in ["canonical.sessions", "canonical.transcripts", "canonical.analysis_reports"]:
                    cur.execute(f"SELECT row_to_json(t) FROM {table} t")
                    data.append(cur.fetchall())
            return json.dumps(data, sort_keys=True), hashlib.sha256(self.audio.read_bytes()).hexdigest()
        before = snapshot()
        for script in ["check_recording", "check_transcription", "check_analysis"]:
            self.cli(script)
        self.assertEqual(snapshot(), before)

    def test_missing_media(self):
        self.update("UPDATE canonical.sessions SET recording_path='/tmp/no-such-acceptance-file.wav'")
        r = self.cli("check_recording", expected_exit=1)
        self.assertFalse(r["file_exists"])
        self.assertIsNone(r["duration_seconds"])

    def test_unended_session(self):
        self.update("UPDATE canonical.sessions SET ended_at=NULL")
        self.assertEqual(self.cli("check_recording", expected_exit=1)["status"], "UNCOMPUTABLE")

    def test_missing_session_json(self):
        for script in ["check_recording", "check_transcription", "check_analysis"]:
            self.assertEqual(self.cli(script, session="missing", expected_exit=1)["error_code"], "SESSION_NOT_FOUND")

    def test_sql_injection_is_value_only(self):
        self.cli("check_recording", session="x'; DROP TABLE sessions; --", expected_exit=1)
        self.assertTrue(self.cli("check_recording")["is_complete"])

    def test_missing_ffprobe_json(self):
        self.assertEqual(self.cli("check_recording", "--ffprobe", "/no/such/ffprobe", expected_exit=2)["error_code"], "FFPROBE_NOT_FOUND")

    def test_corrupt_media_json(self):
        corrupt = self.root / "broken.wav"
        corrupt.write_bytes(b"not media")
        self.update("UPDATE canonical.sessions SET recording_path=%s", (str(corrupt),))
        self.assertEqual(self.cli("check_recording", expected_exit=2)["error_code"], "FFPROBE_FAILED")

    def test_missing_timestamps_cli(self):
        self.update("UPDATE canonical.transcripts SET transcript_json=%s", (json.dumps({"segments": [{"text": "正文"}]}),))
        r = self.cli("check_transcription", expected_exit=1)
        self.assertIsNone(r["coverage_rate"])
        self.assertFalse(self.cli("check_analysis", expected_exit=1)["is_complete"])

    def test_legacy_sample_never_passes_full_session(self):
        self.update("UPDATE runtime.transcripts SET metadata_json=%s", (json.dumps({"coverage_scope": "SAMPLE", "sample_only": True}),))
        self.assertEqual(self.cli("check_transcription", schema="runtime", expected_exit=1)["error_code"], "NO_FULL_TRANSCRIPT")
        r = self.cli("check_transcription", "--transcript-id", "t", schema="runtime", expected_exit=1)
        self.assertIsNone(r["coverage_rate"])
        self.assertFalse(self.cli("check_analysis", schema="runtime", expected_exit=1)["is_complete"])

    def test_multiple_transcript_versions_require_selection(self):
        self.update("INSERT INTO canonical.transcripts SELECT 't2',session_id,status,audio_path,output_path,transcript_json,metadata_json FROM canonical.transcripts")
        self.assertEqual(self.cli("check_transcription", expected_exit=1)["error_code"], "AMBIGUOUS_TRANSCRIPT")
        self.assertEqual(self.cli("check_transcription", "--transcript-id", "t")["coverage_rate"], 1)
        # Report uses its own source version rather than an arbitrary newest row.
        self.assertTrue(self.cli("check_analysis")["is_complete"])

    def test_report_transcript_version_conflict(self):
        self.assertEqual(self.cli("check_analysis", "--transcript-id", "wrong", expected_exit=1)["error_code"], "TRANSCRIPT_MISMATCH")

    def test_legacy_unmarked_transcript_cannot_bypass_scope(self):
        self.update("UPDATE runtime.transcripts SET metadata_json='{}'")
        self.assertEqual(self.cli("check_transcription", "--transcript-id", "t", schema="runtime", expected_exit=1)["status"], "UNCOMPUTABLE")

    def test_transcript_artifact_version_mismatch(self):
        self.artifact.write_text(json.dumps({**self.payload, "transcript_id": "another-version"}))
        self.assertEqual(self.cli("check_transcription", schema="runtime", expected_exit=1)["error_code"], "TRANSCRIPT_MISMATCH")

    def test_conflicting_full_and_sample_scope(self):
        self.artifact.write_text(json.dumps({**self.payload, "coverage_scope": "SAMPLE"}))
        self.assertEqual(self.cli("check_transcription", schema="runtime", expected_exit=1)["error_code"], "COVERAGE_SCOPE_MISMATCH")

    def test_multiple_reports_require_selection(self):
        self.update("INSERT INTO canonical.analysis_reports SELECT 'a2',session_id,status,report_json,transcript_id FROM canonical.analysis_reports")
        self.assertEqual(self.cli("check_analysis", expected_exit=1)["error_code"], "AMBIGUOUS_REPORT")
        self.assertTrue(self.cli("check_analysis", "--report-id", "a")["is_complete"])

    def test_stale_report_fails(self):
        self.update("UPDATE runtime.analyses SET lineage_state='STALE'")
        self.assertEqual(self.cli("check_analysis", schema="runtime", expected_exit=1)["error_code"], "REPORT_NOT_CURRENT")

    def test_missing_report(self):
        self.update("DELETE FROM canonical.analysis_reports")
        self.assertEqual(self.cli("check_analysis", expected_exit=1)["error_code"], "REPORT_NOT_FOUND")

    def test_legacy_report_format_not_mapped(self):
        self.update("UPDATE canonical.analysis_reports SET report_json=%s", (json.dumps({"result": {"hook": "开场", "claims": [], "evidence_refs": []}}),))
        r = self.cli("check_analysis", expected_exit=1)
        self.assertEqual(r["status"], "FORMAT_ERROR")
        self.assertIn("格式异常", r["message"])

    def test_remote_artifact_not_fetched(self):
        self.update("UPDATE runtime.analyses SET output_path='https://example.com/report.json'")
        self.assertEqual(self.cli("check_analysis", schema="runtime", expected_exit=2)["error_code"], "NONLOCAL_PATH")

    def test_invalid_json_file(self):
        self.report_file.write_text("not json")
        self.assertEqual(self.cli("check_analysis", schema="runtime", expected_exit=1)["status"], "FORMAT_ERROR")

    def test_partial_recording_segments_not_added_up(self):
        self.update("INSERT INTO runtime.recording_segments VALUES ('fixture','/tmp/another.ts','PARTIAL')")
        self.assertEqual(self.cli("check_recording", schema="runtime", expected_exit=1)["error_code"], "RECORDING_NOT_UNIQUE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
