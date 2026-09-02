#!/usr/bin/env python3
"""Concurrency/fencing tests against an isolated real PostgreSQL server."""

from __future__ import annotations

import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from v3_db import connect
from v3_long_jobs import ANALYSIS, LeaseLostError, claim_next, reconcile_exhausted, save_checkpoint


DSN = os.environ.get("V3_TEST_POSTGRES_DSN", "")


@unittest.skipUnless(DSN, "V3_TEST_POSTGRES_DSN is required")
class RealPostgresConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with connect(DSN) as conn:
            conn.execute("DROP TABLE IF EXISTS analyses")
            conn.execute("""
                CREATE TABLE analyses(
                  analysis_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                  analysis_type TEXT NOT NULL, source_digest TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'PENDING', output_path TEXT,
                  lineage_state TEXT NOT NULL DEFAULT 'CURRENT', metadata_json TEXT NOT NULL DEFAULT '{}',
                  updated_at TEXT, attempts BIGINT NOT NULL DEFAULT 0,
                  max_attempts BIGINT NOT NULL DEFAULT 5, next_attempt_at TEXT,
                  last_attempt_at TEXT, lease_owner TEXT, lease_until TEXT,
                  lease_epoch BIGINT NOT NULL DEFAULT 0, last_error_type TEXT,
                  last_error TEXT, checkpoint_json TEXT NOT NULL DEFAULT '{}'
                )
            """)
            conn.commit()

    def setUp(self):
        with connect(DSN) as conn:
            conn.execute("TRUNCATE analyses")
            conn.commit()

    def insert(self, count: int):
        with connect(DSN) as conn:
            for index in range(count):
                conn.execute(
                    "INSERT INTO analyses(analysis_id,session_id,analysis_type,source_digest,status,next_attempt_at) "
                    "VALUES(?,?,?,?,'PENDING',?)",
                    (f"a{index:03d}", "s", "single_session", f"d{index}", "2026-08-30T00:00:00.000Z"),
                )
            conn.commit()

    def test_eight_workers_claim_forty_jobs_exactly_once(self):
        self.insert(40)
        claimed = []
        lock = threading.Lock()

        def worker(index: int):
            local = []
            while True:
                with connect(DSN) as conn:
                    row = claim_next(
                        conn, ANALYSIS, f"worker-{index}",
                        now="2026-08-30T00:00:01.000Z", lease_seconds=600,
                    )
                if not row:
                    break
                local.append(row["analysis_id"])
            with lock:
                claimed.extend(local)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))
        self.assertEqual(len(claimed), 40)
        self.assertEqual(len(set(claimed)), 40)
        with connect(DSN) as conn:
            rows = conn.execute("SELECT status,attempts,lease_epoch FROM analyses").fetchall()
        self.assertTrue(all(r["status"] == "RUNNING" and r["attempts"] == 1 and r["lease_epoch"] == 1 for r in rows))

    def test_simultaneous_claim_has_one_winner(self):
        self.insert(1)
        barrier = threading.Barrier(2)

        def worker(name):
            barrier.wait()
            with connect(DSN) as conn:
                row = claim_next(conn, ANALYSIS, name, now="2026-08-30T00:00:01.000Z")
            return row["analysis_id"] if row else None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(worker, ("w1", "w2")))
        self.assertEqual(sorted(value for value in results if value), ["a000"])

    def test_expired_lease_reclaim_fences_old_epoch(self):
        self.insert(1)
        with connect(DSN) as conn:
            old = claim_next(conn, ANALYSIS, "old", now="2026-08-30T00:00:01.000Z", lease_seconds=2)
        with connect(DSN) as conn:
            new = claim_next(conn, ANALYSIS, "new", now="2026-08-30T00:00:04.000Z", lease_seconds=600)
        self.assertEqual((old["lease_epoch"], new["lease_epoch"]), (1, 2))
        with connect(DSN) as conn, self.assertRaises(LeaseLostError):
            save_checkpoint(conn, ANALYSIS, old, {"phase": "STALE"})
        with connect(DSN) as conn:
            save_checkpoint(conn, ANALYSIS, new, {"phase": "CURRENT"})
        with connect(DSN) as conn:
            row = conn.execute("SELECT checkpoint_json FROM analyses WHERE analysis_id='a000'").fetchone()
        self.assertIn("CURRENT", row["checkpoint_json"])

    def test_null_lease_orphan_is_reclaimed(self):
        self.insert(1)
        with connect(DSN) as conn:
            conn.execute("UPDATE analyses SET status='RUNNING',attempts=1,lease_owner=NULL,lease_until=NULL")
            conn.commit()
        with connect(DSN) as conn:
            row = claim_next(conn, ANALYSIS, "recovery", now="2026-08-30T00:00:01.000Z")
        self.assertEqual((row["lease_owner"], row["attempts"], row["lease_epoch"]), ("recovery", 2, 1))

    def test_expired_exhausted_job_is_failed_final(self):
        self.insert(1)
        with connect(DSN) as conn:
            conn.execute("UPDATE analyses SET status='RUNNING',attempts=max_attempts,lease_owner='dead',lease_until='2026-08-30T00:00:00.000Z'")
            conn.commit()
        with connect(DSN) as conn:
            self.assertEqual(reconcile_exhausted(conn, ANALYSIS, now="2026-08-30T00:00:01.000Z"), 1)
        with connect(DSN) as conn:
            row = conn.execute("SELECT status,last_error_type FROM analyses WHERE analysis_id='a000'").fetchone()
        self.assertEqual((row["status"], row["last_error_type"]),
                         ("FAILED_FINAL", "LEASE_EXPIRED_ATTEMPTS_EXHAUSTED"))


if __name__ == "__main__":
    unittest.main()
