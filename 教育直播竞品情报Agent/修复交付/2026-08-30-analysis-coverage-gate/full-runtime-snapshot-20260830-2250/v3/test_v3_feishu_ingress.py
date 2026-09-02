#!/usr/bin/env python3
"""No-network regression tests for real Feishu product-share ingress."""

import unittest
import json
import subprocess
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import v3_feishu_ingress as ingress
import v3_project_feishu as projection
import v3_runtime
import v3_worker as worker


REAL_MESSAGE = "5.10 kpq:/ 06/06 C@H.ic 【抖音商城】https://v.douyin.com/zh2A3Q0Z0sY/ 初阶好家长诊断训练【爷爷奶奶勿拍】KCH 读书卡\n长按复制此条消息，打开抖音搜索，查看商品详情！:7pm"
SENDER = "ou_e2bb6eeeda749177d2b1191664831934"


class IngressTests(unittest.TestCase):
    def call(self, **overrides):
        values = dict(message_id="om_test", chat_id="oc_test", sender_id=SENDER, content=REAL_MESSAGE, message_type="text", chat_type="p2p")
        values.update(overrides)
        return ingress.handle_inbound_message(**values)

    def test_real_share_without_command_is_business(self):
        result = ingress.classify(REAL_MESSAGE)
        self.assertEqual(result["status"], "BUSINESS_CANDIDATE")
        self.assertEqual(result["candidate_urls"], ["https://v.douyin.com/zh2A3Q0Z0sY/"])

    def test_plain_chat_does_not_enter_v3(self):
        self.assertEqual(ingress.classify("今天怎么样？")["status"], "NOT_BUSINESS")

    def test_unrelated_url_does_not_enter_v3(self):
        self.assertEqual(ingress.classify("https://example.com/item")["status"], "NOT_BUSINESS")

    def test_explicit_numeric_command(self):
        self.assertEqual(ingress.classify("扫描商品 3838016038189006849")["status"], "BUSINESS_CANDIDATE")

    def test_lookalike_host_rejected(self):
        self.assertFalse(ingress.supported_business_url("https://douyin.com.evil.example/a"))

    def test_unauthorized_sender_never_calls_database(self):
        with patch.object(v3_runtime, "ingest_message") as ingest:
            self.assertEqual(self.call(sender_id="ou_not_allowed")["status"], "REJECTED")
            ingest.assert_not_called()

    def test_group_does_not_enter_private_pipeline(self):
        self.assertEqual(self.call(chat_type="group")["status"], "NOT_BUSINESS")

    def test_capture_keeps_exact_message_and_sender(self):
        with patch.object(v3_runtime, "ingest_message", return_value={"created": True, "task_id": "task_test"}) as ingest:
            result = self.call()
            self.assertEqual(result["status"], "CAPTURED")
            self.assertEqual(ingest.call_args.kwargs["message_id"], "om_test")
            self.assertEqual(ingest.call_args.kwargs["sender_id"], SENDER)
            self.assertEqual(ingest.call_args.kwargs["content"], REAL_MESSAGE)

    def test_duplicate_status_is_explicit(self):
        with patch.object(v3_runtime, "ingest_message", return_value={"created": False, "task_id": "task_test"}):
            self.assertEqual(self.call()["status"], "DUPLICATE")

    def test_capture_failure_is_not_duplicate(self):
        with patch.object(v3_runtime, "ingest_message", side_effect=RuntimeError("database unavailable")):
            result = self.call()
            self.assertEqual(result["status"], "CAPTURE_FAILED")
            self.assertIn("incident_id", result)
            self.assertNotIn("task_id", result)


class ProductResolverTests(unittest.TestCase):
    def test_numeric_id(self):
        value, evidence = worker.resolve_product_input("扫描商品 3838016038189006849")
        self.assertEqual(value, "3838016038189006849")
        self.assertEqual(evidence["resolution"], "numeric_id")

    def test_product_query(self):
        value, _ = worker.resolve_product_input("https://haohuo.jinritemai.com/ecommerce/trade/detail/index.html?id=3838016038189006849")
        self.assertEqual(value, "3838016038189006849")

    def test_real_short_link_redirect(self):
        class Response:
            def __init__(self, status, location=None):
                self.status_code = status
                self.headers = {"location": location} if location else {}
        with patch("httpx.Client") as client_type:
            client_type.return_value.__enter__.return_value.head.side_effect = [
                Response(302, "https://haohuo.jinritemai.com/ecommerce/trade/detail/index.html?id=3838016038189006849"),
                Response(200),
            ]
            value, evidence = worker.resolve_product_input(REAL_MESSAGE)
            self.assertEqual(value, "3838016038189006849")
            self.assertEqual(len(evidence["redirect_chain"]), 2)
            self.assertFalse(client_type.call_args.kwargs["trust_env"])

    def test_redirect_outside_allowlist_fails(self):
        class Response:
            status_code = 302
            headers = {"location": "https://127.0.0.1/internal"}
        with patch("httpx.Client") as client_type:
            client_type.return_value.__enter__.return_value.head.return_value = Response()
            with self.assertRaises(ValueError):
                worker.resolve_product_input(REAL_MESSAGE)


class DurableIngressTests(unittest.TestCase):
    def test_one_hundred_concurrent_deliveries_have_one_task_and_ack(self):
        with tempfile.TemporaryDirectory(prefix="edu-ingress-regression-") as folder:
            db_path = Path(folder) / "test.db"
            v3_runtime.init_db(db_path)
            original_connect = v3_runtime.connect
            def isolated_connect(*args, **kwargs):
                return original_connect(db_path)
            def deliver(_):
                return v3_runtime.ingest_message(message_id="om_concurrent", chat_id="oc_test", sender_id=SENDER, content=REAL_MESSAGE)
            with patch.object(v3_runtime, "identity_assertion"), patch.object(v3_runtime, "connect", side_effect=isolated_connect):
                with ThreadPoolExecutor(max_workers=8) as pool:
                    results = list(pool.map(deliver, range(100)))
            self.assertEqual(sum(result["created"] for result in results), 1)
            self.assertEqual(len({result["task_id"] for result in results}), 1)
            with original_connect(db_path) as conn:
                for table in ("inbox_messages", "tasks", "checkpoints", "domain_events", "outbox"):
                    self.assertEqual(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 1, table)
                ack = dict(conn.execute("SELECT * FROM outbox").fetchone())
                self.assertEqual(ack["object_type"], "ingress_ack")
                self.assertEqual(ack["status"], "IN_FLIGHT")

    def test_ack_retry_then_completion_is_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="edu-ack-regression-") as folder:
            db_path = Path(folder) / "test.db"
            v3_runtime.init_db(db_path)
            original_connect = v3_runtime.connect
            with patch.object(v3_runtime, "identity_assertion"), patch.object(v3_runtime, "connect", side_effect=lambda *a, **k: original_connect(db_path)):
                result = v3_runtime.ingest_message(message_id="om_ack", chat_id="oc_test", sender_id=SENDER, content=REAL_MESSAGE)
                ingress.finish_ack(outbox_id=result["ack_outbox_id"], success=False, error="test network failure")
                item = v3_runtime.claim_outbox("test:1")
                self.assertEqual(item["outbox_id"], result["ack_outbox_id"])
                ingress.finish_ack(outbox_id=item["outbox_id"], success=True, message_id="om_receipt")
                ingress.finish_ack(outbox_id=item["outbox_id"], success=True, message_id="om_receipt")
            with original_connect(db_path) as conn:
                self.assertEqual(conn.execute("SELECT status FROM outbox").fetchone()[0], "SENT")
                self.assertEqual(conn.execute("SELECT count(*) FROM delivery_receipts").fetchone()[0], 1)


class FakeCursor:
    def __init__(self, value):
        self.value = value
    def fetchone(self):
        return self.value


class OutboxDeliveryTests(unittest.TestCase):
    def test_notification_uses_explicit_node_path_and_reply_idempotency(self):
        item = {"outbox_id":"out_test","object_id":"task_test","object_type":"task_notification","payload_json":json.dumps({"source_message_id":"om_test","text":"waiting","idempotency_key":"stable-uuid"})}
        completed = subprocess.CompletedProcess([],0,json.dumps({"ok":True,"data":{}}),"")
        with patch.object(worker,"claim_outbox",return_value=item), patch.object(worker,"load_config",return_value={"lark_cli":"/profile/bin/lark-cli"}), patch.object(worker.subprocess,"run",return_value=completed) as run, patch.object(worker,"complete_outbox") as complete:
            result=worker.process_outbox_once()
            self.assertEqual(result["status"],"SENT")
            self.assertIn("/opt/homebrew/bin",run.call_args.kwargs["env"]["PATH"])
            self.assertIn("+messages-reply",run.call_args.args[0])
            self.assertIn("stable-uuid",run.call_args.args[0])
            complete.assert_called_once()


class FakeConnection:
    def __init__(self, session):
        self.session = session
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params=()):
        if "FROM live_sessions" in sql:
            return FakeCursor(self.session)
        if "FROM monitor_targets" in sql:
            return FakeCursor({"competitor_id": "acct_test"})
        return FakeCursor(None)


class ProjectionTests(unittest.TestCase):
    def test_all_session_states_define_scene_state(self):
        for status in ("RECORDING", "WAITING_CAPACITY", "WAITING_STREAM", "ENDED", "MEDIA_COMPLETE", "DUPLICATE_SUPERSEDED", "IMPORTED_FAILED", "UNKNOWN"):
            with self.subTest(status=status):
                session = dict(session_id="sess_test", monitor_target_id="mon_test", platform_session_id="platform_test", source_url="https://live.douyin.com/1", status=status, completeness="UNKNOWN", started_at="2026-08-27T00:00:00Z", ended_at=None)
                with patch.object(projection, "identity_assertion"), patch.object(projection, "connect", return_value=FakeConnection(session)), patch.object(projection, "upsert", return_value={}) as upsert:
                    result = projection.project_session("sess_test", dry_run=True)
                    self.assertEqual(result["status"], "DRY_RUN")
                    fields = upsert.call_args.args[4]
                    self.assertTrue(fields["场次状态"])

    def test_waiting_is_not_recording_failure(self):
        scene, recording, complete = projection.session_projection_states({"status": "WAITING_STREAM", "completeness": "UNKNOWN"})
        self.assertEqual((scene, recording, complete), ("检测到开播", "待开始", "UNKNOWN"))

    def test_one_segment_does_not_prove_full_recording(self):
        _, _, complete = projection.session_projection_states({"status": "ENDED", "completeness": "UNKNOWN"}, {"path": "one-segment.ts"})
        self.assertEqual(complete, "UNKNOWN")

    def test_missing_session_fails_explicitly(self):
        with patch.object(projection, "identity_assertion"), patch.object(projection, "connect", return_value=FakeConnection(None)):
            with self.assertRaisesRegex(RuntimeError, "session not found"):
                projection.project_session("missing", dry_run=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
