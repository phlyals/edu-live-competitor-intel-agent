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
        # Coverage is the timestamp union (0-10 plus 50-70), not the false
        # outer span from the first timestamp to the last one.
        self.assertEqual(result["analysis_coverage"]["timeline_coverage_rate"], .30)
        self.assertEqual(result["analysis_coverage"]["segment_coverage_rate"], 1)
        self.assertFalse(result["analysis_coverage"]["is_qualified"])
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


class AnalysisCoverageGateTests(unittest.TestCase):
    @staticmethod
    def rows(*intervals):
        return [{"segment_index": index, "source_segment_id": f"t:segment:{index:06d}",
                 "start": start, "end": end, "text": "内容",
                 "line": f"[{start:.2f}-{end:.2f}] 内容"}
                for index, (start, end) in enumerate(intervals)]

    def test_internal_gaps_are_not_counted_as_covered(self):
        rows = self.rows((0, 10), (90, 100))
        coverage = analysis.calculate_analysis_coverage(
            rows, [row["source_segment_id"] for row in rows], 100,
        )
        self.assertEqual(coverage["analysis_coverage_rate"], .2)
        self.assertEqual(coverage["gaps"], [{"start_time": 10, "end_time": 90}])
        self.assertFalse(coverage["is_qualified"])

    def test_successful_source_ids_drive_both_timeline_and_segment_rates(self):
        rows = self.rows((0, 50), (50, 100))
        coverage = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100,
        )
        self.assertEqual(coverage["analysis_coverage_rate"], .5)
        self.assertEqual(coverage["segment_coverage_rate"], .5)
        self.assertEqual(coverage["analyzed_unique_segment_count"], 1)
        self.assertFalse(coverage["is_qualified"])

    def test_minimum_90_qualifies_but_does_not_claim_95_target(self):
        rows = self.rows((0, 90))
        coverage = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100, minimum=.90, target=.95,
        )
        self.assertTrue(coverage["is_qualified"])
        self.assertFalse(coverage["meets_target"])

    def test_target_95_is_reported_from_union(self):
        rows = self.rows((0, 95))
        coverage = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100, minimum=.90, target=.95,
        )
        self.assertTrue(coverage["is_qualified"])
        self.assertTrue(coverage["meets_target"])

    def test_sample_scope_can_never_be_formal(self):
        rows = self.rows((0, 100))
        coverage = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100, coverage_scope="SAMPLE",
        )
        self.assertFalse(coverage["is_qualified"])
        self.assertIn("SOURCE_SCOPE_NOT_FULL_SESSION",
                      [item["code"] for item in coverage["validation_errors"]])

    def test_duplicate_and_unknown_source_ids_fail_closed(self):
        rows = self.rows((0, 50), (50, 100))
        rows[1]["source_segment_id"] = rows[0]["source_segment_id"]
        coverage = analysis.calculate_analysis_coverage(rows, [rows[0]["source_segment_id"], "unknown"], 100)
        codes = [item["code"] for item in coverage["validation_errors"]]
        self.assertIn("DUPLICATE_SOURCE_SEGMENT_ID", codes)
        self.assertIn("UNKNOWN_SUCCESSFUL_SOURCE_SEGMENT_ID", codes)
        self.assertFalse(coverage["is_qualified"])

    def test_p1_6_transcript_gate_must_match_recomputed_union(self):
        rows = self.rows((0, 95))
        transcript_quality = {"audio_duration_seconds": 100, "coverage_rate": .99,
                              "timestamps_valid": True, "is_qualified": True}
        coverage = analysis.calculate_analysis_coverage(
            rows, [rows[0]["source_segment_id"]], 100,
            transcript_quality=transcript_quality,
        )
        self.assertFalse(coverage["is_qualified"])
        self.assertIn("TRANSCRIPT_QUALITY_GATE_MISMATCH",
                      [item["code"] for item in coverage["validation_errors"]])

    def test_source_rows_have_stable_ids_and_chunks_preserve_them(self):
        payload = {"segments": [{"start": 0, "end": 50, "text": "甲"},
                                {"start": 50, "end": 100, "text": "乙"}]}
        rows = analysis.source_rows_from_transcript(payload, "transcript_x")
        chunks = analysis.analysis_chunks("", char_limit=20, source_rows=rows)
        self.assertEqual(rows[0]["source_segment_id"], "transcript_x:segment:000000")
        self.assertEqual([item for chunk in chunks for item in chunk["source_segment_ids"]],
                         [row["source_segment_id"] for row in rows])

    def test_revalidation_requires_complete_matching_diagnostics(self):
        rows = self.rows((0, 50), (50, 100))
        text = "\n".join(row["line"] for row in rows)
        chunks = analysis.analysis_chunks(text, source_rows=rows)
        artifact = {"engine": {"source_content_hash": analysis.hashlib.sha256(text.encode()).hexdigest(),
                               "chunk_diagnostics": [
                                   {"chunk_index": chunk["index"], "start": chunk["start"],
                                    "end": chunk["end"], "row_count": chunk["row_count"],
                                    "finish_reason": "stop"} for chunk in chunks]},
                    "result": {"modules": [], "analysis_coverage": {"segment_coverage_rate": 1.0}}}
        updated, coverage = analysis.revalidate_existing_artifact(
            artifact, text, rows, 100, "FULL_SESSION", None, .90, .95,
        )
        self.assertTrue(coverage["is_qualified"])
        self.assertEqual(updated["result"]["analysis_coverage"]["analysis_coverage_rate"], 1.0)
        artifact["engine"]["chunk_diagnostics"][0]["row_count"] = 999
        with self.assertRaisesRegex(ValueError, "do not match"):
            analysis.revalidate_existing_artifact(
                artifact, text, rows, 100, "FULL_SESSION", None, .90, .95,
            )

    def test_prepare_source_rejects_sample_before_model(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.json"
            path.write_text(json.dumps({"duration": 100, "segments": [
                {"start": 0, "end": 100, "text": "样本"}
            ]}), encoding="utf-8")
            with self.assertRaises(analysis.AnalysisSourceQualityError) as caught:
                analysis.prepare_analysis_source(
                    path, "transcript_sample",
                    json.dumps({"coverage_scope": "SAMPLE", "sample_only": True}), .90, .95,
                )
            self.assertFalse(caught.exception.coverage["is_qualified"])

    def test_request_analysis_persists_successful_source_ids(self):
        rows = self.rows((0, 50), (50, 100))
        valid = json.dumps(chunk_payload(course_content=[
            {"summary": "课程", "start": 0, "end": 50}
        ]), ensure_ascii=False)
        FakeClient.responses = [FakeResponse(valid)]
        with patch.object(analysis, "read_env_key", return_value="configured"), \
             patch.object(analysis.httpx, "Client", FakeClient):
            result, engine = analysis.request_analysis(
                "\n".join(row["line"] for row in rows), source_duration=100,
                source_rows=rows, coverage_scope="FULL_SESSION",
            )
        expected = [row["source_segment_id"] for row in rows]
        self.assertEqual(result["analysis_coverage"]["successful_source_segment_ids"], expected)
        self.assertEqual(engine["successful_source_segment_count"], 2)
        self.assertEqual(engine["chunk_diagnostics"][0]["source_segment_ids"], expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
