import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v3_analysis_worker as analysis


def chunk_payload(**values):
    payload = analysis.empty_chunk_result()
    payload.update(values)
    return payload


class FakeResponse:
    def __init__(self, content, finish_reason="stop", response_id="response"):
        self.payload = {"id": response_id, "choices": [{"finish_reason": finish_reason,
                        "message": {"content": content}}],
                        "usage": {"prompt_tokens": 100, "completion_tokens": 100}}

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeClient:
    responses = []

    def __init__(self, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def post(self, *_args, **_kwargs):
        return self.responses.pop(0)


class TranscriptChunkTests(unittest.TestCase):
    def test_default_transcript_text_is_not_truncated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transcript.json"
            path.write_text(json.dumps({"segments": [{"start": 0, "end": 1, "text": "字" * 17000}]}))
            text, rows = analysis.transcript_text(path)
            self.assertGreater(len(text), 16000)
            self.assertEqual(len(rows), 1)
            limited, _ = analysis.transcript_text(path, limit=16000)
            self.assertEqual(len(limited), 16000)

    def test_chunks_cover_every_timestamped_row_once(self):
        text = "\n".join(f"[{i:.2f}-{i + 1:.2f}] " + "字" * 40 for i in range(100))
        chunks = analysis.analysis_chunks(text, char_limit=500)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(sum(chunk["row_count"] for chunk in chunks), 100)
        self.assertEqual(chunks[0]["start"], 0)
        self.assertEqual(chunks[-1]["end"], 100)

    def test_merge_has_new_and_legacy_fields_with_coverage(self):
        chunks = [{"index": 0, "start": 0, "end": 50, "row_count": 1,
                   "rows": [{"segment_index": 0, "start": 0, "end": 10, "text": "开场"}]},
                  {"index": 1, "start": 50, "end": 99, "row_count": 1,
                   "rows": [{"segment_index": 1, "start": 50, "end": 70, "text": "互动"}]}]
        first = chunk_payload(instructor=[{"summary": "陈老师", "start": 1, "end": 2}],
                              course_content=[{"summary": "选科", "start": 3, "end": 4}])
        second = chunk_payload(interaction_patterns=[{"summary": "评论区答题", "start": 60, "end": 61}],
                               product_handoff=[{"summary": "小黄车", "start": 80, "end": 81}])
        result = analysis.merge_chunk_results([first, second], chunks, source_duration=100)
        self.assertEqual(result["schema_version"], "2.0")
        self.assertEqual(result["instructor"]["names"], ["陈老师"])
        self.assertEqual(result["interaction"], result["interaction_patterns"])
        self.assertEqual(result["product_fulfillment"], result["product_handoff"])
        self.assertEqual([module["name"] for module in result["modules"]],
                         ["开场", "干货", "需求", "信任", "商品承接", "成交", "答疑"])
        self.assertEqual(result["analysis_coverage"]["timeline_coverage_rate"], .99)
        self.assertEqual(result["analysis_coverage"]["segment_coverage_rate"], 1)
        self.assertIn("source_segment_index", result["modules"][1]["timestamps"][0])
        for key in ("course_content", "interaction", "product_handoff", "claims", "evidence_refs"):
            self.assertIn(key, result)

    def test_out_of_chunk_timestamp_is_rejected(self):
        value = chunk_payload(claims=[{"summary": "越界", "start": 99, "end": 100}])
        with self.assertRaises(ValueError):
            analysis.validate_chunk_result(value, {"start": 0, "end": 10})

    def test_atomic_json_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "analysis.json"
            analysis.write_json_atomic(path, {"中文": [1, 2, 3]})
            self.assertEqual(json.loads(path.read_text()), {"中文": [1, 2, 3]})
            self.assertFalse(list(path.parent.glob("*.tmp")))


class APIResponseTests(unittest.TestCase):
    def setUp(self):
        FakeClient.responses = []
        self.chunk = {"index": 0, "start": 0, "end": 10, "row_count": 1, "text": "[0.00-10.00] 测试"}
        self.valid = json.dumps(chunk_payload(course_content=[{"summary": "选科", "start": 0, "end": 10}]), ensure_ascii=False)

    def call(self):
        with patch.object(analysis, "read_env_key", return_value="configured"), \
             patch.object(analysis.httpx, "Client", FakeClient):
            return analysis.request_chunk(self.chunk)

    def test_valid_stop_response_is_accepted(self):
        FakeClient.responses = [FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(result["course_content"][0]["summary"], "选科")
        self.assertEqual(meta["finish_reason"], "stop")

    def test_length_response_is_never_parsed_as_success(self):
        FakeClient.responses = [FakeResponse('{"course_content":[', finish_reason="length"), FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(meta["attempt"], 2)
        self.assertEqual(result["course_content"][0]["summary"], "选科")

    def test_invalid_json_is_retried(self):
        FakeClient.responses = [FakeResponse("not-json"), FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(meta["attempt"], 2)
        self.assertTrue(result["course_content"])

    def test_three_invalid_responses_fail_closed(self):
        FakeClient.responses = [FakeResponse("bad") for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, "failed after retries"):
            self.call()


if __name__ == "__main__":
    unittest.main(verbosity=2)
