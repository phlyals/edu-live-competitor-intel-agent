#!/usr/bin/env python3
from __future__ import annotations

import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from v3_db import connect
from v3_long_jobs import RECOMPUTE, LeaseLostError, claim_next, save_checkpoint


DSN = os.environ.get("V3_TEST_LINEAGE_POSTGRES_DSN", "")


@unittest.skipUnless(DSN, "V3_TEST_LINEAGE_POSTGRES_DSN is required")
class LineagePostgresTests(unittest.TestCase):
    def setUp(self):
        with connect(DSN) as conn:
            conn.execute(
                "DELETE FROM recompute_requests WHERE request_id LIKE 'test-recompute-%'"
            )
            conn.commit()

    def tearDown(self):
        with connect(DSN) as conn:
            conn.execute(
                "DELETE FROM recompute_requests WHERE request_id LIKE 'test-recompute-%'"
            )
            conn.commit()

    @staticmethod
    def insert(conn, request_id="test-recompute-one"):
        return conn.execute(
            "INSERT INTO recompute_requests("
            "request_id,downstream_type,downstream_id,upstream_type,upstream_id,"
            "old_upstream_digest,new_upstream_digest,target_analysis_spec_version,"
            "target_model_version,target_prompt_version,status,next_attempt_at,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'PENDING',?,?,?) "
            "ON CONFLICT DO NOTHING",
            (
                request_id, "analysis", "test-old", "transcript", "test-upstream",
                "a" * 64, "b" * 64, "test-spec", "test-model", "test-prompt",
                "2026-08-31T00:00:00.000Z", "2026-08-31T00:00:00.000Z",
                "2026-08-31T00:00:00.000Z",
            ),
        )

    def test_concurrent_duplicate_request_converges_to_one_row(self):
        barrier = threading.Barrier(8)

        def worker(_index):
            barrier.wait()
            with connect(DSN) as conn:
                cursor = self.insert(conn, "test-recompute-concurrent")
                conn.commit()
                return int(cursor.rowcount or 0)

        with ThreadPoolExecutor(max_workers=8) as pool:
            inserted = list(pool.map(worker, range(8)))
        self.assertEqual(sum(inserted), 1)
        with connect(DSN) as conn:
            count = conn.execute(
                "SELECT count(*) FROM recompute_requests "
                "WHERE request_id='test-recompute-concurrent'"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_recompute_identity_trigger_rejects_mutation(self):
        with connect(DSN) as conn:
            self.insert(conn)
            conn.commit()
        with self.assertRaises(Exception):
            with connect(DSN) as conn:
                conn.execute(
                    "UPDATE recompute_requests SET new_upstream_digest=? "
                    "WHERE request_id='test-recompute-one'",
                    ("c" * 64,),
                )
                conn.commit()
        with connect(DSN) as conn:
            digest = conn.execute(
                "SELECT new_upstream_digest FROM recompute_requests "
                "WHERE request_id='test-recompute-one'"
            ).fetchone()[0]
        self.assertEqual(digest, "b" * 64)

    def test_recompute_claim_is_atomic_and_expired_epoch_is_fenced(self):
        with connect(DSN) as conn:
            self.insert(conn, "test-recompute-claim")
            conn.commit()
        barrier = threading.Barrier(2)

        def worker(name):
            barrier.wait()
            with connect(DSN) as conn:
                return claim_next(
                    conn, RECOMPUTE, name,
                    now="2026-08-31T00:00:01.000Z", lease_seconds=1,
                    where_sql="request_id=?",
                    where_params=("test-recompute-claim",),
                )

        with ThreadPoolExecutor(max_workers=2) as pool:
            claimed = [row for row in pool.map(worker, ("one", "two")) if row]
        self.assertEqual(len(claimed), 1)
        old = claimed[0]
        with connect(DSN) as conn, self.assertRaises(LeaseLostError):
            save_checkpoint(
                conn, RECOMPUTE, old, {"phase": "STALE"},
                now="2026-08-31T00:00:03.000Z",
            )
        with connect(DSN) as conn:
            new = claim_next(
                conn, RECOMPUTE, "new",
                now="2026-08-31T00:00:03.000Z", lease_seconds=60,
                where_sql="request_id=?",
                where_params=("test-recompute-claim",),
            )
        self.assertEqual((new["attempts"], new["lease_epoch"]), (2, 2))

    def test_lineage_identity_is_immutable_but_state_is_mutable(self):
        with connect(DSN) as conn:
            edge = conn.execute(
                "SELECT edge_id,upstream_version,state FROM lineage_edges "
                "WHERE binding_status='CONTENT_DIGEST_VERIFIED' "
                "ORDER BY edge_id LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(edge)
        with self.assertRaises(Exception):
            with connect(DSN) as conn:
                conn.execute(
                    "UPDATE lineage_edges SET upstream_version=? WHERE edge_id=?",
                    ("f" * 64, edge["edge_id"]),
                )
                conn.commit()
        with connect(DSN) as conn:
            conn.execute("BEGIN")
            conn.execute(
                "UPDATE lineage_edges SET state='STALE' WHERE edge_id=?",
                (edge["edge_id"],),
            )
            changed = conn.execute(
                "SELECT state FROM lineage_edges WHERE edge_id=?",
                (edge["edge_id"],),
            ).fetchone()[0]
            conn.rollback()
        self.assertEqual(changed, "STALE")
        with connect(DSN) as conn:
            restored = conn.execute(
                "SELECT upstream_version,state FROM lineage_edges WHERE edge_id=?",
                (edge["edge_id"],),
            ).fetchone()
        self.assertEqual(restored["upstream_version"], edge["upstream_version"])
        self.assertEqual(restored["state"], edge["state"])

    def test_legacy_edges_are_explicit_and_never_seed_requests(self):
        with connect(DSN) as conn:
            counts = {
                row["binding_status"]: row["n"]
                for row in conn.execute(
                    "SELECT binding_status,count(*) AS n FROM lineage_edges "
                    "GROUP BY binding_status"
                )
            }
            invalid = conn.execute(
                "SELECT count(*) FROM recompute_requests r JOIN lineage_edges e "
                "ON e.downstream_type=r.downstream_type "
                "AND e.downstream_id=r.downstream_id "
                "AND e.upstream_type=r.upstream_type "
                "AND e.upstream_id=r.upstream_id "
                "AND e.upstream_version=r.old_upstream_digest "
                "WHERE e.binding_status='LEGACY_UNVERIFIED'"
            ).fetchone()[0]
        self.assertGreater(counts.get("CONTENT_DIGEST_VERIFIED", 0), 0)
        self.assertGreater(counts.get("LEGACY_UNVERIFIED", 0), 0)
        self.assertEqual(invalid, 0)


if __name__ == "__main__":
    unittest.main()
