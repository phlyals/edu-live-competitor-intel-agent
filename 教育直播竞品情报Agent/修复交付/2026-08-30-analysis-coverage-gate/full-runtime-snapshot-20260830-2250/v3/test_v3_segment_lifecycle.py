import contextlib
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch


SCHEMA = Path(__file__).resolve().parent / "v3_schema.sql"
import v3_worker as worker
import v3_pipeline_worker as pipeline


class CandidateCodeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="segment-lifecycle-code-")
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def connect(self, db):
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        return conn

    def test_fresh_schema_contains_fail_closed_lifecycle_columns(self):
        db = self.root / "schema.db"
        with self.connect(db) as conn:
            conn.executescript(SCHEMA.read_text(encoding="utf-8"))
            columns = {row[1]: row for row in conn.execute("PRAGMA table_info(recording_segments)")}
        self.assertIn("lifecycle_status", columns)
        self.assertEqual(columns["lifecycle_status"][4], "'UNCLASSIFIED'")
        self.assertIn("superseded_by_segment_id", columns)
        self.assertIn("lifecycle_updated_at", columns)

    def test_finalizer_relocates_retained_source_without_unique_conflict(self):
        db = self.root / "state.db"
        completed = self.root / "completed.ts.partial"
        completed.write_bytes(b"same")
        old = self.root / "partial.ts.partial"
        with self.connect(db) as conn:
            conn.execute("CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,path TEXT UNIQUE,checksum TEXT,lifecycle_status TEXT,superseded_by_segment_id TEXT,lifecycle_updated_at TEXT)")
            conn.execute("INSERT INTO recording_segments VALUES(?,?,?,?,?,?,?)", ("canonical", "s", str(self.root / "整场直播.ts"), "c", "CANONICAL_ACTIVE", None, None))
            conn.execute("INSERT INTO recording_segments VALUES(?,?,?,?,?,?,?)", ("source", "s", str(old), hashlib.sha256(b"same").hexdigest(), "UNCLASSIFIED", None, None))
            worker._classify_published_source_segments(conn, "s", "canonical", [{"path": str(completed), "original_path": str(old), "sha256": hashlib.sha256(b"same").hexdigest()}])
            row = conn.execute("SELECT * FROM recording_segments WHERE segment_id='source'").fetchone()
        self.assertEqual((row["path"], row["lifecycle_status"]), (str(completed), "SOURCE_RETAINED"))

    def test_finalizer_does_not_overwrite_existing_destination_owner(self):
        db = self.root / "conflict.db"
        completed = self.root / "completed.ts.partial"
        completed.write_bytes(b"same")
        old = self.root / "partial.ts.partial"
        digest = hashlib.sha256(b"same").hexdigest()
        with self.connect(db) as conn:
            conn.execute("CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,path TEXT,checksum TEXT,lifecycle_status TEXT,superseded_by_segment_id TEXT,lifecycle_updated_at TEXT,UNIQUE(session_id,path))")
            conn.executemany("INSERT INTO recording_segments VALUES(?,?,?,?,?,?,?)", [
                ("canonical", "s", str(self.root / "整场直播.ts"), "c", "CANONICAL_ACTIVE", None, None),
                ("owner", "s", str(completed), digest, "SOURCE_RETAINED", None, None),
                ("stale", "s", str(old), digest, "UNCLASSIFIED", None, None),
            ])
            worker._classify_published_source_segments(conn, "s", "canonical", [{"path": str(completed), "original_path": str(old), "sha256": digest}])
            rows = {row["segment_id"]: row for row in conn.execute("SELECT * FROM recording_segments")}
        self.assertEqual(rows["owner"]["lifecycle_status"], "SOURCE_RETAINED")
        self.assertEqual(rows["stale"]["lifecycle_status"], "SOURCE_SUPERSEDED")
        self.assertEqual(rows["stale"]["superseded_by_segment_id"], "canonical")

    def test_pipeline_only_attempts_canonical_active(self):
        db = self.root / "pipeline.db"
        canonical = self.root / "整场直播.ts"
        source = self.root / "source.ts"
        canonical.write_bytes(b"canonical")
        source.write_bytes(b"source")
        with self.connect(db) as conn:
            conn.executescript("""
                CREATE TABLE live_sessions(session_id TEXT PRIMARY KEY,status TEXT,ended_at TEXT,completeness TEXT,metadata_json TEXT);
                CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,path TEXT,checksum TEXT,status TEXT,lifecycle_status TEXT);
                CREATE TABLE transcripts(transcript_id TEXT PRIMARY KEY,session_id TEXT,source_digest TEXT,engine TEXT,model TEXT,status TEXT,language TEXT,source_path TEXT,output_path TEXT,low_confidence_count INTEGER DEFAULT 0,created_at TEXT,metadata_json TEXT,UNIQUE(session_id,source_digest));
            """)
            conn.execute("INSERT INTO live_sessions VALUES(?,?,?,?,?)", ("s", "MEDIA_COMPLETE", "2026-08-30T00:10:00Z", "COMPLETE", json.dumps({"media_coverage": {"continuous_capture": True}})))
            conn.executemany("INSERT INTO recording_segments VALUES(?,?,?,?,?,?)", [
                ("canonical", "s", str(canonical), "canonical-hash", "COMPLETE", "CANONICAL_ACTIVE"),
                ("source", "s", str(source), "source-hash", "COMPLETE", "SOURCE_RETAINED"),
            ])
        @contextlib.contextmanager
        def connect():
            conn = self.connect(db)
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
        with patch.object(pipeline, "connect", connect), patch.object(pipeline, "extract_audio", return_value=False):
            pipeline.transcribe_pending()
        with self.connect(db) as conn:
            rows = list(conn.execute("SELECT metadata_json FROM transcripts"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])["segment_id"], "canonical")


if __name__ == "__main__":
    unittest.main(verbosity=2)
