import hashlib
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


def bound_chunks(raw_rows, char_limit=5000):
    rows = analysis.bind_transcript_segments(raw_rows)
    text = "\n".join(row["line"] for row in rows if row["analysis_text"])
    return rows, analysis.analysis_chunks(text, char_limit=char_limit, source_rows=rows)


def cited(summary, *source_ids):
    return {"summary": summary, "source_segment_ids": list(source_ids)}


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
    requests = []

    def __init__(self, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def post(self, *_args, **kwargs):
        self.requests.append(kwargs)
        return self.responses.pop(0)


class TranscriptBindingTests(unittest.TestCase):
    def test_default_transcript_text_is_not_truncated_and_hides_timestamps(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transcript.json"
            path.write_text(json.dumps({"segments": [{"start": 0, "end": 1, "text": "字" * 17000}]}))
            text, rows = analysis.transcript_text(path)
            self.assertGreater(len(text), 16000)
            self.assertEqual(len(rows), 1)
            self.assertIn(rows[0]["source_segment_id"], text)
            self.assertNotIn("[0.00-1.00]", text)
            limited, _ = analysis.transcript_text(path, limit=16000)
            self.assertEqual(len(limited), 16000)

    def test_source_ids_are_stable_across_chunk_limits_and_normalized_text_changes(self):
        raw = [{"start": 1.25, "end": 2.5, "text": " 原 始 ", "normalized_text": "原始"},
               {"start": 3, "end": 4, "text": "第二句"}]
        first = analysis.bind_transcript_segments(raw)
        changed = analysis.bind_transcript_segments([{**raw[0], "normalized_text": "原始文本"}, raw[1]])
        self.assertEqual([row["source_segment_id"] for row in first],
                         [row["source_segment_id"] for row in changed])
        text = "\n".join(row["line"] for row in first)
        one = analysis.analysis_chunks(text, char_limit=10000, source_rows=first)
        many = analysis.analysis_chunks(text, char_limit=10, source_rows=first)
        self.assertEqual([row["source_segment_id"] for chunk in one for row in chunk["rows"]],
                         [row["source_segment_id"] for chunk in many for row in chunk["rows"]])

    def test_content_digest_uses_original_not_normalized_text(self):
        row = analysis.bind_transcript_segments([
            {"start": 0, "end": 1, "text": "原 始", "normalized_text": "原始"}
        ])[0]
        self.assertEqual(row["content_digest"], hashlib.sha256("原 始".encode()).hexdigest())

    def test_chunks_cover_every_timestamped_row_once(self):
        text = "\n".join(f"[{i:.2f}-{i + 1:.2f}] " + "字" * 40 for i in range(100))
        chunks = analysis.analysis_chunks(text, char_limit=500)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(sum(chunk["row_count"] for chunk in chunks), 100)
        ids = [source_id for chunk in chunks for source_id in chunk["source_segment_ids"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_invalid_original_timestamp_fails_closed(self):
        for raw in ([{"start": 1, "end": 1, "text": "bad"}],
                    [{"start": "nan", "end": 2, "text": "bad"}], [{}]):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                analysis.bind_transcript_segments(raw)


class ValidationAndMergeTests(unittest.TestCase):
    def setUp(self):
        self.rows, chunks = bound_chunks([
            {"start": 0, "end": 10, "text": "开场原文"},
            {"start": 10, "end": 20, "text": "课程原文"},
            {"start": 20, "end": 30, "text": "互动原文"},
        ], char_limit=40)
        self.chunks = chunks

    def test_exact_source_reference_is_server_bound(self):
        chunk = self.chunks[0]
        row = chunk["rows"][0]
        value = chunk_payload(hook=[cited("开场", row["source_segment_id"])])
        item = analysis.validate_chunk_result(value, chunk)["hook"][0]
        self.assertEqual(item["start"], row["start"])
        self.assertEqual(item["end"], row["end"])
        self.assertEqual(item["source_segments"][0]["source_text"], "开场原文")
        self.assertEqual(item["source_segments"][0]["content_digest"], row["content_digest"])

    def test_forged_and_cross_chunk_ids_fail_closed(self):
        first = self.chunks[0]
        candidates = ["srcseg_forged"]
        if len(self.chunks) > 1:
            candidates.append(self.chunks[-1]["rows"][0]["source_segment_id"])
        for source_id in candidates:
            with self.subTest(source_id=source_id), self.assertRaisesRegex(ValueError, "outside"):
                analysis.validate_chunk_result(
                    chunk_payload(claims=[cited("伪造", source_id)]), first
                )

    def test_up_to_eight_exact_ids_are_allowed_but_nine_are_rejected(self):
        rows, chunks = bound_chunks([
            {"start": index, "end": index + 1, "text": f"原文{index}"}
            for index in range(9)
        ], char_limit=10000)
        chunk = chunks[0]
        accepted = chunk_payload(claims=[
            cited("多段证据", *[row["source_segment_id"] for row in rows[:8]])
        ])
        self.assertEqual(
            len(analysis.validate_chunk_result(accepted, chunk)["claims"][0]["source_segment_ids"]),
            8,
        )
        rejected = chunk_payload(claims=[
            cited("过多证据", *[row["source_segment_id"] for row in rows])
        ])
        with self.assertRaisesRegex(ValueError, "invalid source_segment_ids"):
            analysis.validate_chunk_result(rejected, chunk)

    def test_free_timestamps_are_rejected_even_when_inside_chunk(self):
        value = chunk_payload(claims=[{"summary": "旧格式", "start": 1, "end": 2}])
        with self.assertRaisesRegex(ValueError, "summary/source_segment_ids"):
            analysis.validate_chunk_result(value, self.chunks[0])

    def test_missing_or_extra_schema_fields_fail_closed(self):
        row = self.chunks[0]["rows"][0]
        missing = {"hook": [cited("开场", row["source_segment_id"])]}
        with self.assertRaisesRegex(ValueError, "omitted"):
            analysis.validate_chunk_result(missing, self.chunks[0])
        extra = chunk_payload(extra=[])
        with self.assertRaisesRegex(ValueError, "unknown"):
            analysis.validate_chunk_result(extra, self.chunks[0])

    def test_merge_uses_exact_refs_and_has_no_nearest_fallback(self):
        first, second = self.rows[0], self.rows[-1]
        first_result = analysis.validate_chunk_result(
            chunk_payload(instructor=[cited("陈老师", first["source_segment_id"])],
                          hook=[cited("欢迎", first["source_segment_id"])]), self.chunks[0]
        )
        final_chunk = next(chunk for chunk in self.chunks if second["source_segment_id"] in chunk["source_segment_ids"])
        second_result = analysis.validate_chunk_result(
            chunk_payload(interaction_patterns=[cited("评论区答题", second["source_segment_id"])]), final_chunk
        )
        result = analysis.merge_chunk_results([first_result, second_result],
                                              [self.chunks[0], final_chunk], source_duration=30)
        self.assertEqual(result["schema_version"], "3.0")
        self.assertFalse(result["evidence_binding"]["nearest_segment_fallback"])
        opening = next(module for module in result["modules"] if module["name"] == "开场")
        self.assertEqual(opening["timestamps"][0]["source_segment_id"], first["source_segment_id"])
        self.assertEqual(opening["timestamps"][0]["start"], 0)
        self.assertNotIn("source_segment_index", opening["timestamps"][0])

    def test_reference_manifest_detects_text_digest_tampering(self):
        row = self.chunks[0]["rows"][0]
        validated = analysis.validate_chunk_result(
            chunk_payload(hook=[cited("开场", row["source_segment_id"])]), self.chunks[0]
        )
        result = analysis.merge_chunk_results([validated], [self.chunks[0]], source_duration=30)
        manifest = analysis.source_reference_manifest(result)
        artifact = {"result": result, "evidence": manifest}
        self.assertEqual(analysis.artifact_binding_status(artifact), "BOUND_V1")
        result["hook"][0]["source_segments"][0]["source_text"] = "篡改"
        self.assertEqual(analysis.artifact_binding_status(artifact), "INVALID")

    def test_legacy_artifact_stays_readable_but_unbound(self):
        legacy = {"result": {"schema_version": "2.0", "hook": [{"start": 1, "end": 2}]}}
        self.assertEqual(analysis.artifact_binding_status(legacy), "LEGACY_UNBOUND")

    def test_atomic_json_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "analysis.json"
            analysis.write_json_atomic(path, {"中文": [1, 2, 3]})
            self.assertEqual(json.loads(path.read_text()), {"中文": [1, 2, 3]})
            self.assertFalse(list(path.parent.glob("*.tmp")))


class APIResponseTests(unittest.TestCase):
    def setUp(self):
        FakeClient.responses = []
        FakeClient.requests = []
        rows, chunks = bound_chunks([{"start": 0, "end": 10, "text": "测试"}])
        self.row = rows[0]
        self.chunk = chunks[0]
        self.valid = json.dumps(chunk_payload(
            course_content=[cited("选科", self.row["source_segment_id"])]
        ), ensure_ascii=False)

    def call(self):
        with patch.object(analysis, "read_env_key", return_value="configured"), \
             patch.object(analysis.httpx, "Client", FakeClient):
            return analysis.request_chunk(self.chunk)

    def test_valid_stop_response_is_accepted_and_prompt_forbids_time(self):
        FakeClient.responses = [FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(result["course_content"][0]["summary"], "选科")
        self.assertEqual(meta["finish_reason"], "stop")
        body = FakeClient.requests[0]["json"]
        self.assertIn(self.row["source_segment_id"], body["messages"][1]["content"])
        self.assertNotIn("[0.00-10.00]", body["messages"][1]["content"])
        self.assertIn("不能生成、猜测或输出任何时间戳", body["messages"][0]["content"])

    def test_length_response_is_never_parsed_as_success(self):
        FakeClient.responses = [FakeResponse('{"course_content":[', finish_reason="length"), FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(meta["attempt"], 2)
        self.assertEqual(result["course_content"][0]["summary"], "选科")

    def test_legacy_timestamp_response_is_retried_not_nearest_mapped(self):
        legacy = json.dumps(chunk_payload(course_content=[{"summary": "旧", "start": 0, "end": 10}]))
        FakeClient.responses = [FakeResponse(legacy), FakeResponse(self.valid)]
        result, meta = self.call()
        self.assertEqual(meta["attempt"], 2)
        self.assertEqual(result["course_content"][0]["source_segment_ids"], [self.row["source_segment_id"]])

    def test_three_invalid_responses_fail_closed(self):
        FakeClient.responses = [FakeResponse("bad") for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, "failed after retries"):
            self.call()


if __name__ == "__main__":
    unittest.main(verbosity=2)
