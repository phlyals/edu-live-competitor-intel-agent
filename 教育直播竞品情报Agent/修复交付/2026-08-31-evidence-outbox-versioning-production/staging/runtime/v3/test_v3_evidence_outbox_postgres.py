#!/usr/bin/env python3
from __future__ import annotations

import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import patch

import v3_runtime as runtime
from v3_db import connect


DSN = os.environ.get("V3_TEST_OUTBOX_POSTGRES_DSN", "")


@contextmanager
def test_connect():
    conn = connect(DSN)
    try:
        yield conn
    finally:
        conn.close()


@unittest.skipUnless(DSN, "V3_TEST_OUTBOX_POSTGRES_DSN is required")
class VersionedOutboxPostgresTests(unittest.TestCase):
    def setUp(self):
        with connect(DSN) as conn:
            conn.execute(
                "DELETE FROM delivery_receipts WHERE outbox_id IN "
                "(SELECT outbox_id FROM outbox WHERE object_id LIKE 'test-analysis%')"
            )
            conn.execute("DELETE FROM outbox WHERE object_id LIKE 'test-analysis%'")
            conn.commit()

    def tearDown(self):
        self.setUp()

    @staticmethod
    def enqueue(conn, *, payload=None, version="projection-test-v1"):
        return runtime.enqueue_outbox_conn(
            conn,
            object_type="semantic_projection",
            object_id="test-analysis",
            destination="feishu_base",
            payload=payload or {
                "analysis_id": "test-analysis",
                "artifact_digest": "a" * 64,
                "evidence_bundle_id": "bundle:test-analysis",
                "evidence_manifest_hash": "a" * 64,
                "evidence_verified_at": "2026-08-31T00:00:00Z",
                "projection_version": version,
            },
            scope="FORMAL_SINGLE_SESSION",
            qualification_status="FULL_SESSION_QUALIFIED",
            projection_version=version,
            artifact_digest="a" * 64,
            evidence_bundle_id="bundle:test-analysis",
            evidence_manifest_hash="a" * 64,
            evidence_verified_at="2026-08-31T00:00:00Z",
            projection_binding_status="VERSIONED_EVIDENCE",
        )

    def test_concurrent_same_version_converges_to_one_outbox(self):
        barrier = threading.Barrier(8)

        def worker(_index):
            barrier.wait()
            with connect(DSN) as conn:
                value = self.enqueue(conn)
                conn.commit()
                return value

        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(worker, range(8)))
        self.assertEqual(len(set(values)), 1)
        with connect(DSN) as conn:
            count = conn.execute(
                "SELECT count(*) FROM outbox WHERE object_id='test-analysis'"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_same_version_with_different_payload_fails_closed(self):
        with connect(DSN) as conn:
            self.enqueue(conn)
            conn.commit()
        payload = {
            "analysis_id": "test-analysis", "different": True,
            "artifact_digest": "a" * 64,
            "evidence_bundle_id": "bundle:test-analysis",
            "evidence_manifest_hash": "a" * 64,
            "evidence_verified_at": "2026-08-31T00:00:00Z",
            "projection_version": "projection-test-v1",
        }
        with connect(DSN) as conn, self.assertRaises(RuntimeError):
            self.enqueue(conn, payload=payload)

    def test_projection_identity_trigger_allows_state_not_payload_mutation(self):
        with connect(DSN) as conn:
            outbox_id = self.enqueue(conn)
            conn.commit()
        with connect(DSN) as conn:
            conn.execute(
                "UPDATE outbox SET status='RETRY' WHERE outbox_id=?", (outbox_id,)
            )
            conn.commit()
        with self.assertRaises(Exception):
            with connect(DSN) as conn:
                conn.execute(
                    "UPDATE outbox SET payload_hash=? WHERE outbox_id=?",
                    ("b" * 64, outbox_id),
                )
                conn.commit()

    def test_receipt_copies_version_identity_and_is_immutable(self):
        with connect(DSN) as conn:
            outbox_id = self.enqueue(conn)
            conn.execute(
                "UPDATE outbox SET status='IN_FLIGHT',lease_owner='test',"
                "lease_until='2999-01-01T00:00:00Z' WHERE outbox_id=?",
                (outbox_id,),
            )
            conn.commit()
        with patch.object(runtime, "connect", side_effect=test_connect):
            runtime.complete_outbox(outbox_id, {"status": "VERIFIED"})
        with connect(DSN) as conn:
            row = conn.execute(
                "SELECT o.projection_version AS outbox_version,"
                "o.artifact_digest AS outbox_artifact,r.projection_version AS receipt_version,"
                "r.artifact_digest AS receipt_artifact,r.status FROM outbox o JOIN delivery_receipts r "
                "ON r.outbox_id=o.outbox_id WHERE o.outbox_id=?",
                (outbox_id,),
            ).fetchone()
        self.assertEqual(
            (
                row["outbox_version"], row["outbox_artifact"],
                row["receipt_version"], row["receipt_artifact"],
                row["status"],
            ),
            ("projection-test-v1", "a" * 64, "projection-test-v1", "a" * 64, "VERIFIED"),
        )
        with self.assertRaises(Exception):
            with connect(DSN) as conn:
                conn.execute(
                    "UPDATE delivery_receipts SET artifact_digest=? WHERE outbox_id=?",
                    ("b" * 64, outbox_id),
                )
                conn.commit()


if __name__ == "__main__":
    unittest.main()
