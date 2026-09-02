#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import v3_analysis_worker as analysis
import v3_pipeline_worker as pipeline
from v3_long_jobs import (
    ANALYSIS,
    LeaseLostError,
    claim_next,
    fail_or_retry,
    parse_checkpoint,
    reconcile_exhausted,
    renew,
    save_checkpoint,
    versioned_output_path,
)


SCHEMA = """
CREATE TABLE analyses(
  analysis_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, analysis_type TEXT NOT NULL,
  source_digest TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', output_path TEXT,
  lineage_state TEXT NOT NULL DEFAULT 'CURRENT', metadata_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 5,
  next_attempt_at TEXT, last_attempt_at TEXT, lease_owner TEXT, lease_until TEXT,
  lease_epoch INTEGER NOT NULL DEFAULT 0, last_error_type TEXT, last_error TEXT,
  checkpoint_json TEXT NOT NULL DEFAULT '{}'
);
"""


def connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def insert_analysis(conn: sqlite3.Connection, job_id: str = "a", *, max_attempts: int = 5) -> None:
    conn.execute(
        "INSERT INTO analyses(analysis_id,session_id,analysis_type,source_digest,status,max_attempts,next_attempt_at) "
        "VALUES(?,?,?,?,'PENDING',?,?)",
        (job_id, "s", "single_session", "digest", max_attempts, "2026-08-30T00:00:00.000Z"),
    )


class DurableClaimTests(unittest.TestCase):
    def test_claim_increments_epoch_and_attempt(self):
        conn = connection(); insert_analysis(conn)
        row = claim_next(conn, ANALYSIS, "worker-a", now="2026-08-30T00:00:01.000Z", lease_seconds=60)
        self.assertEqual((row["status"], row["lease_owner"], row["lease_epoch"], row["attempts"]),
                         ("RUNNING", "worker-a", 1, 1))
        self.assertIsNone(claim_next(conn, ANALYSIS, "worker-b", now="2026-08-30T00:00:02.000Z"))

    def test_stale_owner_cannot_renew_or_checkpoint_after_reclaim(self):
        conn = connection(); insert_analysis(conn)
        first = claim_next(conn, ANALYSIS, "worker-a", now="2026-08-30T00:00:01.000Z", lease_seconds=5)
        second = claim_next(conn, ANALYSIS, "worker-b", now="2026-08-30T00:00:07.000Z", lease_seconds=60)
        self.assertEqual(second["lease_epoch"], 2)
        with self.assertRaises(LeaseLostError):
            renew(conn, ANALYSIS, first, now="2026-08-30T00:00:08.000Z")
        with self.assertRaises(LeaseLostError):
            save_checkpoint(conn, ANALYSIS, first, {"phase": "STALE"})
        save_checkpoint(conn, ANALYSIS, second, {"phase": "OWNED"})
        self.assertEqual(parse_checkpoint(conn.execute("SELECT checkpoint_json FROM analyses").fetchone()[0])["phase"], "OWNED")

    def test_bounded_retry_then_failed_final(self):
        conn = connection(); insert_analysis(conn, max_attempts=2)
        first = claim_next(conn, ANALYSIS, "w1", now="2026-08-30T00:00:01.000Z")
        self.assertEqual(fail_or_retry(
            conn, ANALYSIS, first, error_type="HTTP_500_SERVER", error_message="server",
            retryable=True, now="2026-08-30T00:00:02.000Z", base_delay_seconds=1,
        ), "RETRY_WAIT")
        second = claim_next(conn, ANALYSIS, "w2", now="2026-08-30T00:00:04.000Z")
        self.assertEqual(fail_or_retry(
            conn, ANALYSIS, second, error_type="HTTP_500_SERVER", error_message="server",
            retryable=True, now="2026-08-30T00:00:05.000Z", base_delay_seconds=1,
        ), "FAILED_FINAL")
        row = conn.execute("SELECT * FROM analyses").fetchone()
        self.assertEqual((row["status"], row["attempts"], row["lease_owner"]), ("FAILED_FINAL", 2, None))

    def test_permanent_auth_failure_is_not_retried(self):
        conn = connection(); insert_analysis(conn)
        job = claim_next(conn, ANALYSIS, "w", now="2026-08-30T00:00:01.000Z")
        self.assertEqual(fail_or_retry(
            conn, ANALYSIS, job, error_type="HTTP_401_AUTH", error_message="auth", retryable=False,
            now="2026-08-30T00:00:02.000Z",
        ), "FAILED_FINAL")

    def test_output_names_are_fenced_and_immutable(self):
        first = {"lease_epoch": 3, "attempts": 2}
        second = {"lease_epoch": 4, "attempts": 3}
        base = Path("/tmp/analysis_a.json")
        self.assertNotEqual(versioned_output_path(base, first), versioned_output_path(base, second))
        self.assertIn("lease-00000003.attempt-0002", str(versioned_output_path(base, first)))

    def test_orphan_running_without_lease_is_reclaimed(self):
        conn = connection(); insert_analysis(conn)
        conn.execute("UPDATE analyses SET status='RUNNING',attempts=1,lease_owner=NULL,lease_until=NULL")
        row = claim_next(conn, ANALYSIS, "recovery", now="2026-08-30T00:00:01.000Z")
        self.assertEqual((row["lease_owner"], row["lease_epoch"], row["attempts"]), ("recovery", 1, 2))

    def test_expired_final_attempt_is_reconciled(self):
        conn = connection(); insert_analysis(conn, max_attempts=1)
        conn.execute("UPDATE analyses SET status='RUNNING',attempts=1,lease_owner='dead',lease_until='2026-08-30T00:00:00.000Z'")
        self.assertEqual(reconcile_exhausted(conn, ANALYSIS, now="2026-08-30T00:00:01.000Z"), 1)
        row = conn.execute("SELECT status,last_error_type FROM analyses").fetchone()
        self.assertEqual((row["status"], row["last_error_type"]),
                         ("FAILED_FINAL", "LEASE_EXPIRED_ATTEMPTS_EXHAUSTED"))


class SubprocessLeaseTests(unittest.TestCase):
    def test_long_child_renews_lease_until_exit(self):
        calls = []
        proc = pipeline.run_process_with_lease(
            [sys.executable, "-c", "import time; time.sleep(0.12); print('done')"],
            timeout=2, poll_seconds=0.02, renew_callback=lambda: calls.append(time.monotonic()),
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn("done", proc.stdout)
        self.assertGreaterEqual(len(calls), 3)

    def test_lease_loss_terminates_child(self):
        def lost():
            raise LeaseLostError("stale")
        started = time.monotonic()
        with self.assertRaises(LeaseLostError):
            pipeline.run_process_with_lease(
                [sys.executable, "-c", "import time; time.sleep(5)"],
                timeout=10, poll_seconds=0.02, renew_callback=lost,
            )
        self.assertLess(time.monotonic() - started, 1)

    def test_invalid_existing_audio_is_atomically_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            media = Path(tmp) / "source.ts"
            audio = Path(tmp) / "source.opus"
            media.write_bytes(b"media")
            audio.write_bytes(b"invalid-old-audio")
            renewed = []

            def duration(path):
                path = Path(path)
                if path == audio and path.read_bytes() == b"invalid-old-audio":
                    return 1.0
                return 10.0

            def run(command, **_kwargs):
                Path(command[-1]).write_bytes(b"validated-new-audio")
                return type("Process", (), {"returncode": 0, "stdout": "", "stderr": ""})()

            with patch.object(pipeline, "FFMPEG", Path("/bin/echo")), \
                    patch.object(pipeline, "media_duration", side_effect=duration), \
                    patch.object(pipeline, "run_process_with_lease", side_effect=run):
                self.assertTrue(pipeline.extract_audio(
                    media, audio, attempt_tag="lease-1", renew_callback=lambda: renewed.append(True)
                ))
            self.assertEqual(audio.read_bytes(), b"validated-new-audio")
            self.assertTrue(renewed)


class FakeClient:
    def __init__(self, responses, timeout=None):
        self.responses = responses
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def post(self, *_args, **_kwargs):
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def response(status: int, payload=None, headers=None) -> httpx.Response:
    request = httpx.Request("POST", "https://api.deepseek.com/chat/completions")
    if payload is None:
        return httpx.Response(status, request=request, headers=headers)
    return httpx.Response(status, request=request, headers=headers, json=payload)


def valid_payload(finish_reason="stop", content=None):
    body = {field: [] for field in analysis.CHUNK_FIELDS}
    return {"id": "response-id", "choices": [{"finish_reason": finish_reason,
            "message": {"content": content if content is not None else json.dumps(body)}}], "usage": {}}


class ProviderClassificationTests(unittest.TestCase):
    def setUp(self):
        self.chunk = {"index": 0, "start": 0.0, "end": 2.0, "text": "[0.00-2.00] 你好", "rows": [], "row_count": 1}

    def invoke(self, items):
        shared = list(items)
        factory = lambda timeout=None: FakeClient(shared, timeout=timeout)
        with patch.object(analysis, "read_env_key", return_value="secret"):
            return analysis.request_chunk(self.chunk, client_factory=factory, sleep=lambda _seconds: None)

    def test_401_and_403_are_permanent_and_single_attempt(self):
        for status in (401, 403):
            items = [response(status), response(200, valid_payload())]
            with self.assertRaises(analysis.AnalysisRequestError) as caught:
                self.invoke(items)
            self.assertFalse(caught.exception.retryable)
            self.assertEqual(caught.exception.code, f"HTTP_{status}_AUTH")
            self.assertEqual(len(items), 2)  # caller list is copied; assertion documents no external mutation

    def test_429_and_5xx_retry_then_succeed(self):
        result, diagnostic = self.invoke([
            response(429, headers={"Retry-After": "2"}),
            response(503),
            response(200, valid_payload()),
        ])
        self.assertEqual(result, analysis.empty_chunk_result())
        self.assertEqual(diagnostic["attempt"], 3)

    def test_connect_and_timeout_retry(self):
        result, diagnostic = self.invoke([
            httpx.ConnectError("offline"),
            httpx.ReadTimeout("slow"),
            response(200, valid_payload()),
        ])
        self.assertEqual(result, analysis.empty_chunk_result())
        self.assertEqual(diagnostic["attempt"], 3)

    def test_finish_reason_json_and_schema_are_retryable(self):
        result, diagnostic = self.invoke([
            response(200, valid_payload(finish_reason="length")),
            response(200, valid_payload(content="{")),
            response(200, valid_payload()),
        ])
        self.assertEqual(result, analysis.empty_chunk_result())
        self.assertEqual(diagnostic["attempt"], 3)

    def test_content_filter_is_permanent(self):
        with self.assertRaises(analysis.AnalysisRequestError) as caught:
            self.invoke([response(200, valid_payload(finish_reason="content_filter"))])
        self.assertEqual(caught.exception.code, "MODEL_FINISH_BLOCKED")
        self.assertFalse(caught.exception.retryable)


class AnalysisCheckpointTests(unittest.TestCase):
    def test_completed_chunk_is_reused_after_crash(self):
        text = "[0.00-1.00] 一二三四五\n[1.00-2.00] 六七八九十"
        state = {}
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            def first_request(chunk, before_attempt=None):
                calls.append(chunk["index"])
                if chunk["index"] == 1:
                    raise analysis.AnalysisRequestError("HTTP_TIMEOUT", "timeout", retryable=True)
                return analysis.empty_chunk_result(), {"response_id": "one", "finish_reason": "stop", "attempt": 1}
            def persist(value):
                state.clear(); state.update(json.loads(json.dumps(value)))
            with self.assertRaises(analysis.AnalysisRequestError):
                analysis.request_analysis(
                    text, 2.0, checkpoint=state, checkpoint_dir=Path(tmp),
                    on_checkpoint=persist, request_fn=first_request, char_limit=20,
                )
            self.assertEqual(calls, [0, 1])
            calls.clear()
            def second_request(chunk, before_attempt=None):
                calls.append(chunk["index"])
                return analysis.empty_chunk_result(), {"response_id": "two", "finish_reason": "stop", "attempt": 1}
            merged, diagnostics = analysis.request_analysis(
                text, 2.0, checkpoint=state, checkpoint_dir=Path(tmp),
                on_checkpoint=persist, request_fn=second_request, char_limit=20,
            )
            self.assertEqual(calls, [1])
            self.assertEqual(diagnostics["chunk_count"], 2)
            self.assertTrue(merged["analysis_coverage"]["all_chunks_complete"])


class AnalysisProcessIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / "jobs.db"
        conn = self.connect()
        conn.executescript(SCHEMA + """
            CREATE TABLE transcripts(
              transcript_id TEXT PRIMARY KEY,session_id TEXT,status TEXT,output_path TEXT
            );
            CREATE TABLE lineage_edges(
              downstream_type TEXT,downstream_id TEXT,upstream_type TEXT,upstream_id TEXT,state TEXT
            );
            CREATE TABLE evidence_bundles(
              bundle_id TEXT PRIMARY KEY,object_type TEXT,object_id TEXT,status TEXT,
              manifest_path TEXT,manifest_hash TEXT,metadata_json TEXT,
              UNIQUE(object_type,object_id)
            );
            CREATE TABLE outbox(
              outbox_id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,object_type TEXT,object_id TEXT,
              destination TEXT,status TEXT,attempts INTEGER,max_attempts INTEGER,next_attempt_at TEXT,
              payload_hash TEXT,payload_json TEXT
            );
        """)
        self.transcript = self.root / "transcript.json"
        self.transcript.write_text(json.dumps({
            "duration": 2,
            "segments": [{"start": 0, "end": 1, "text": "一"}, {"start": 1, "end": 2, "text": "二"}],
        }))
        conn.execute("INSERT INTO transcripts VALUES('t','s','COMPLETE',?)", (str(self.transcript),))
        insert_analysis(conn)
        conn.execute("INSERT INTO lineage_edges VALUES('analysis','a','transcript','t','CURRENT')")
        conn.commit(); conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def connect(self):
        conn = sqlite3.connect(self.db, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def claim(self):
        conn = self.connect()
        try:
            return claim_next(conn, ANALYSIS, "integration", now="2026-08-30T00:00:01.000Z")
        finally:
            conn.close()

    def test_complete_publication_is_fenced_versioned_and_atomic(self):
        job = self.claim()
        engine = {"source_content_hash": "hash", "response_ids": [], "chunk_count": 1,
                  "chunk_diagnostics": [], "merge_engine": "test"}
        with patch.object(analysis, "connect", side_effect=self.connect), \
                patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
                patch.object(analysis, "request_analysis", return_value=({"modules": []}, engine)):
            self.assertEqual(analysis.process_claim(job), "COMPLETE")
        conn = self.connect()
        row = conn.execute("SELECT * FROM analyses WHERE analysis_id='a'").fetchone()
        self.assertEqual(row["status"], "COMPLETE")
        self.assertIsNone(row["lease_owner"])
        self.assertIn("lease-00000001.attempt-0001", row["output_path"])
        self.assertTrue(Path(row["output_path"]).is_file())
        self.assertEqual(conn.execute("SELECT count(*) FROM evidence_bundles").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT count(*) FROM outbox").fetchone()[0], 1)
        conn.close()

    def test_401_moves_job_directly_to_failed_final(self):
        job = self.claim()
        error = analysis.AnalysisRequestError("HTTP_401_AUTH", "auth", retryable=False)
        with patch.object(analysis, "connect", side_effect=self.connect), \
                patch.object(analysis, "ANALYSIS_ROOT", self.root / "analysis"), \
                patch.object(analysis, "request_analysis", side_effect=error):
            self.assertEqual(analysis.process_claim(job), "FAILED_FINAL")
        conn = self.connect()
        row = conn.execute("SELECT status,attempts,last_error_type FROM analyses WHERE analysis_id='a'").fetchone()
        self.assertEqual(tuple(row), ("FAILED_FINAL", 1, "HTTP_401_AUTH"))
        conn.close()


if __name__ == "__main__":
    unittest.main()
