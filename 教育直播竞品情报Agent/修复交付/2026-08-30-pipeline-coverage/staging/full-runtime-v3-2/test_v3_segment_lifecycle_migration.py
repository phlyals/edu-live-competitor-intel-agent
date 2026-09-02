import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest


MIGRATION = Path(__file__).resolve().parent / "v3_segment_lifecycle_migration.py"
spec = importlib.util.spec_from_file_location("segment_lifecycle_migration", MIGRATION)
migration = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = migration
spec.loader.exec_module(migration)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SQLitePlanningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="segment-lifecycle-sqlite-")
        self.root = Path(self.temp.name)
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,path TEXT,checksum TEXT,status TEXT,bytes INTEGER);
            CREATE TABLE media_manifests(session_id TEXT,status TEXT,manifest_path TEXT,manifest_hash TEXT);
            CREATE TABLE transcripts(transcript_id TEXT,status TEXT,source_path TEXT,output_path TEXT,metadata_json TEXT);
        """)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def rows(self, table):
        return [dict(row) for row in self.db.execute(f"SELECT * FROM {table}")]

    def fixture(self):
        final = self.root / "整场直播.ts"
        retained = self.root / "整场直播.refresh0001.ts.partial"
        old_retained = self.root / "partial" / retained.name
        missing = self.root / "partial" / "整场直播.refresh0002.ts.partial"
        lost = self.root / "gone.ts"
        final.write_bytes(b"canonical")
        retained.write_bytes(b"retained")
        manifest = {
            "session_id": "s",
            "final_path": str(final),
            "sha256": sha(final),
            "retained_sources": [
                {"path": str(retained), "original_path": str(old_retained), "sha256": sha(retained)},
                {"path": str(self.root / "missing-completed.ts.partial"), "original_path": str(missing), "sha256": hashlib.sha256(b"missing").hexdigest()},
            ],
        }
        manifest_path = self.root / "media-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.db.execute("INSERT INTO media_manifests VALUES(?,?,?,?)", ("s", "VERIFIED", str(manifest_path), sha(manifest_path)))
        values = [
            ("canonical", "s", str(final), sha(final), "COMPLETE", final.stat().st_size),
            ("moved", "s", str(old_retained), sha(retained), "COMPLETE", retained.stat().st_size),
            ("superseded", "s", str(missing), hashlib.sha256(b"missing").hexdigest(), "COMPLETE", 7),
            ("lost", "unverified-session", str(lost), hashlib.sha256(b"lost").hexdigest(), "COMPLETE", 4),
        ]
        self.db.executemany("INSERT INTO recording_segments VALUES(?,?,?,?,?,?)", values)
        reason = "audio extraction/duration validation failed"
        self.db.executemany("INSERT INTO transcripts VALUES(?,?,?,?,?)", [
            ("cancel", "WAITING_TOOL", str(missing), None, json.dumps({"segment_id": "superseded", "original": "kept", "reason": reason})),
            ("review", "WAITING_TOOL", str(lost), None, json.dumps({"segment_id": "lost", "reason": reason})),
            ("bad-json", "WAITING_TOOL", str(missing), None, "{"),
        ])
        return retained

    def jobs(self):
        return [{"session_id": "s", "partial_dir": str(self.root / "partial"), "completed_dir": str(self.root)}]

    def test_classifies_canonical_move_supersede_and_lost(self):
        retained = self.fixture()
        desired, issues = migration.plan_segments(self.rows("recording_segments"), self.rows("media_manifests"), self.jobs())
        by_id = {item.segment_id: item for item in desired}
        self.assertEqual(by_id["canonical"].lifecycle_status, "CANONICAL_ACTIVE")
        self.assertEqual((by_id["moved"].lifecycle_status, by_id["moved"].path), ("SOURCE_RETAINED", str(retained)))
        self.assertEqual(by_id["superseded"].lifecycle_status, "SOURCE_SUPERSEDED")
        self.assertEqual(by_id["superseded"].superseded_by_segment_id, "canonical")
        self.assertEqual(by_id["lost"].lifecycle_status, "LOST_REVIEW")
        self.assertFalse(issues)

    def test_only_superseded_missing_waiting_transcript_is_cancelled(self):
        self.fixture()
        desired, _ = migration.plan_segments(self.rows("recording_segments"), self.rows("media_manifests"), self.jobs())
        actions, issues = migration.plan_transcripts(self.rows("transcripts"), desired, "2026-08-30T00:00:00.000Z")
        self.assertEqual([item["transcript_id"] for item in actions], ["cancel"])
        metadata = json.loads(actions[0]["metadata_json"])
        self.assertEqual(metadata["original"], "kept")
        self.assertEqual(metadata["segment_lifecycle_migration"]["superseded_by_segment_id"], "canonical")
        self.assertEqual(issues[0]["transcript_id"], "bad-json")

    def test_invalid_manifest_never_supersedes(self):
        self.fixture()
        self.db.execute("UPDATE media_manifests SET manifest_hash='wrong'")
        desired, issues = migration.plan_segments(self.rows("recording_segments"), self.rows("media_manifests"), self.jobs())
        by_id = {item.segment_id: item.lifecycle_status for item in desired}
        self.assertEqual(by_id["superseded"], "LOST_REVIEW")
        self.assertTrue(any("manifest file hash mismatch" in item["reason"] for item in issues))


if __name__ == "__main__":
    unittest.main(verbosity=2)
