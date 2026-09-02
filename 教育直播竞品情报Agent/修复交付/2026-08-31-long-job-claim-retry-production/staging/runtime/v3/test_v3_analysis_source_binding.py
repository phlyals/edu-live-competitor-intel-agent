import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import v3_analysis_worker as analysis
import v3_evidence_worker as evidence


class EvidenceBindingTests(unittest.TestCase):
    def artifact(self):
        directory = tempfile.TemporaryDirectory()
        transcript = Path(directory.name) / "transcript.json"
        payload = {
            "segments": [
                {"start": 0, "end": 1, "text": "原始甲"},
                {"start": 1, "end": 2, "text": "原始乙"},
            ]
        }
        transcript.write_text(json.dumps(payload, ensure_ascii=False))
        rows = analysis.bind_transcript_segments(payload["segments"])
        text = "\n".join(row["line"] for row in rows)
        chunks = analysis.analysis_chunks(text, source_rows=rows)
        chunk_results = []
        for chunk in chunks:
            value = analysis.empty_chunk_result()
            value["course_content"] = [{
                "summary": "课程",
                "source_segment_ids": [chunk["rows"][0]["source_segment_id"]],
            }]
            chunk_results.append(analysis.validate_chunk_result(value, chunk))
        result = analysis.merge_chunk_results(
            chunk_results, chunks, 2,
            successful_source_segment_ids=[row["source_segment_id"] for row in rows],
        )
        manifest = analysis.source_reference_manifest(result)
        transcript_digest = hashlib.sha256(transcript.read_bytes()).hexdigest()
        source_set_digest = analysis._json_digest([
            {
                "source_segment_id": row["source_segment_id"],
                "content_digest": row["content_digest"],
            }
            for row in rows
        ])
        artifact = {
            "result": result,
            "evidence": {
                **manifest,
                "transcript_id": "t",
                "transcript_artifact_sha256": transcript_digest,
                "source_content_hash": hashlib.sha256(text.encode()).hexdigest(),
                "source_segment_set_digest": source_set_digest,
                "model_generated_timestamps": False,
                "nearest_segment_fallback": False,
            },
        }
        db_row = {
            "transcript_id": "t",
            "transcript_content_digest": transcript_digest,
        }
        return directory, transcript, artifact, db_row

    def test_exact_source_binding_matches_immutable_transcript(self):
        directory, transcript, artifact, db_row = self.artifact()
        try:
            ok, reason, details = evidence.verify_strict_source_binding(
                artifact, transcript, db_row,
            )
            self.assertTrue(ok)
            self.assertEqual(reason, "strict_source_binding_verified")
            self.assertEqual(details["evidence_binding_status"], "BOUND_V1")
            self.assertGreater(details["referenced_source_segment_count"], 0)
        finally:
            directory.cleanup()

    def test_tampered_reference_cannot_be_resigned_into_valid_evidence(self):
        directory, transcript, artifact, db_row = self.artifact()
        try:
            item = artifact["result"]["course_content"][0]["source_segments"][0]
            item["start"] = 0.25
            # Recompute the artifact's self-manifest as an attacker might; the
            # immutable transcript comparison must still reject it.
            artifact["evidence"].update(
                analysis.source_reference_manifest(artifact["result"])
            )
            ok, reason, _ = evidence.verify_strict_source_binding(
                artifact, transcript, db_row,
            )
            self.assertFalse(ok)
            self.assertEqual(reason, "strict_source_binding_reference_mismatch")
        finally:
            directory.cleanup()

    def test_nearest_fallback_or_model_timestamp_policy_is_rejected(self):
        directory, transcript, artifact, db_row = self.artifact()
        try:
            artifact["result"]["evidence_binding"]["nearest_segment_fallback"] = True
            ok, reason, _ = evidence.verify_strict_source_binding(
                artifact, transcript, db_row,
            )
            self.assertFalse(ok)
            self.assertEqual(reason, "strict_source_binding_policy_mismatch")
        finally:
            directory.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
