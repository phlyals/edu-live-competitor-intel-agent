import importlib.util
import io
import json
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch


MODULE_PATH = Path(__file__).with_name("record_douyin_live.py")
spec = importlib.util.spec_from_file_location("candidate_recorder", MODULE_PATH)
recorder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recorder)


def live(token: str) -> dict:
    return {
        "is_live": True,
        "stream_url": f"https://cdn.example/live.flv?token={token}",
        "stream_urls": [],
        "stream_protocol": "FLV",
        "anchor_name": "test",
        "room_id": "room",
        "quality": "LD",
    }


def no_preresolution(*_args):
    return {
        "cancel": threading.Event(),
        "ready": threading.Event(),
        "info": None,
        "resolved_at": None,
        "error_kind": None,
        "thread": SimpleNamespace(join=lambda timeout=None: None),
    }


class ClassifierUnitTests(unittest.TestCase):
    def test_benign_tail(self):
        self.assertEqual(recorder.classify_ffmpeg_tail(""), "BENIGN")
        self.assertEqual(
            recorder.classify_ffmpeg_tail("[mpegts] deprecated pixel format used"),
            "BENIGN",
        )
        self.assertEqual(recorder.ffmpeg_tail_error_codes(""), [])

    def test_recovered_network_warning(self):
        tail = "\n".join(
            [
                "[http] HTTP error 503 Service Unavailable",
                "[http] Will reconnect at 1024 in 1 second(s), error=End of file.",
                "[http] Error reading HTTP response: Operation timed out",
            ]
        )
        self.assertEqual(recorder.classify_ffmpeg_tail(tail), "NETWORK_WARNING")
        self.assertEqual(
            recorder.ffmpeg_tail_error_codes(tail),
            ["HTTP_5XX", "NETWORK_EOF", "RECONNECT_ATTEMPT", "NETWORK_TIMEOUT"],
        )

    def test_integrity_error_matrix(self):
        cases = {
            "STREAM_PREMATURE_END": "Stream ends prematurely at 488, should be 999",
            "SEGMENT_SKIPPED": "Segment 123 of playlist 0 failed too many times, skipping",
            "SEGMENT_OPEN_FAILED": "Failed to open segment 123 of playlist 0",
            "PLAYLIST_RELOAD_FAILED": "Failed to reload playlist 0",
            "INPUT_OPEN_FAILED": "Error opening input files: Server returned 404 Not Found",
            "DEMUX_IO_ERROR": "Error during demuxing: Input/output error",
            "MUX_WRITE_ERROR": "Error writing trailer: Immediate exit requested",
            "INVALID_DATA": "Invalid data found when processing input",
            "PACKET_MISMATCH": "Packet mismatch 444409027 5002764 125906883",
            "PACKET_CORRUPT": "Packet corrupt (stream = 0, dts = 42)",
            "NON_MONOTONIC_DTS": "Non-monotonic DTS; previous: 5, current: 4",
            "INVALID_TIMESTAMP_DROP": "DTS 4292269698, next:899934938 st:0 invalid dropping",
            "TIMESTAMP_DISCONTINUITY": "timestamp discontinuity (stream id=0)",
            "EMPTY_SEGMENT": "Empty segment [https://example.invalid/a.ts]",
            "CODEC_PARAMETERS_MISSING": "could not find codec parameters",
            "UNABLE_SEEK_PACKET": "Unable to seek to the next packet",
        }
        for code, tail in cases.items():
            with self.subTest(code=code):
                self.assertEqual(recorder.classify_ffmpeg_tail(tail), "INTEGRITY_ERROR")
                self.assertIn(code, recorder.ffmpeg_tail_error_codes(tail))

    def test_unknown_ffmpeg_error_fails_closed(self):
        tail = "Encoder failed for an unrecognized reason"
        self.assertEqual(recorder.classify_ffmpeg_tail(tail), "INTEGRITY_ERROR")
        self.assertEqual(recorder.ffmpeg_tail_error_codes(tail), ["UNKNOWN_FFMPEG_ERROR"])


class PersistenceTests(unittest.TestCase):
    def test_sidecar_persists_safe_codes_and_no_url(self):
        warning = (
            "[http] HTTP error 503 for "
            "https://cdn.example/live.flv?token=DO_NOT_PERSIST\n"
            "[http] Will reconnect at 10 in 1 second(s), error=End of file.\n"
        )
        process = SimpleNamespace(
            pid=123,
            stderr=io.StringIO(warning),
            poll=lambda: 0,
            wait=lambda: 0,
        )
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "capture.ts"
            with patch.object(recorder.subprocess, "Popen", return_value=process):
                code, tail = recorder.record_stream(
                    "https://cdn.example/live.flv?token=INPUT_SECRET",
                    output,
                    900,
                    "ffmpeg",
                    stream_metadata={"url_sha256": "safe-digest"},
                )
            state = json.loads(recorder.recording_state_path(output).read_text())
        serialized = json.dumps(state, ensure_ascii=False)
        self.assertEqual(code, 0)
        self.assertEqual(state["ffmpeg_tail_class"], "NETWORK_WARNING")
        self.assertEqual(
            state["ffmpeg_error_codes"],
            ["HTTP_5XX", "NETWORK_EOF", "RECONNECT_ATTEMPT"],
        )
        self.assertNotIn("DO_NOT_PERSIST", serialized)
        self.assertNotIn("INPUT_SECRET", serialized)
        self.assertNotIn("cdn.example", serialized)
        self.assertIn("<stream-url>", tail)

    def test_early_integrity_error_survives_bounded_tail_eviction(self):
        lines = ["Packet mismatch 1 2 3"] + [f"ordinary warning {index}" for index in range(80)]
        process = SimpleNamespace(pid=123, stderr=io.StringIO("\n".join(lines) + "\n"), poll=lambda: 0, wait=lambda: 0)
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "capture.ts"
            with patch.object(recorder.subprocess, "Popen", return_value=process):
                recorder.record_stream("/tmp/replay.mp4", output, 0, "ffmpeg")
            state = json.loads(recorder.recording_state_path(output).read_text())
        self.assertNotIn("Packet mismatch", "\n".join(state["ffmpeg_tail"]))
        self.assertEqual(state["ffmpeg_tail_class"], "INTEGRITY_ERROR")
        self.assertIn("PACKET_MISMATCH", state["ffmpeg_error_codes"])


class ChunkAcceptanceTests(unittest.TestCase):
    def run_refresh(self, responses, record_side_effects):
        with tempfile.TemporaryDirectory() as folder:
            calls = []

            def record(_url, path, *_args):
                index = len(calls)
                calls.append(path)
                path.write_bytes(b"media")
                return record_side_effects[index]

            with (
                patch.object(recorder, "resolve_stream", AsyncMock(side_effect=responses)),
                patch.object(recorder, "schedule_stream_preresolution", side_effect=no_preresolution),
                patch.object(recorder, "record_stream", side_effect=record),
                patch.object(recorder, "probe_duration", return_value=900),
                patch.object(recorder.time, "sleep") as sleeps,
            ):
                result = recorder.record_live_with_refresh(
                    "https://live.douyin.com/123",
                    "LD",
                    Path(folder) / "whole.ts.partial",
                    "ffmpeg",
                    900,
                )
                # Paths disappear with TemporaryDirectory; return only metadata.
                return result[0], result[1]["attempts"], calls, [c.args[0] for c in sleeps.call_args_list]

    def test_code_zero_integrity_error_is_not_complete_chunk(self):
        responses = [live("one"), live("two"), {"is_live": False}, {"is_live": False}, {"is_live": False}]
        code, attempts, calls, sleeps = self.run_refresh(
            responses,
            [
                (0, "Stream ends prematurely at 100, should be 999"),
            ],
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(attempts[0]["ffmpeg_tail_class"], "INTEGRITY_ERROR")
        self.assertIn("STREAM_PREMATURE_END", attempts[0]["ffmpeg_error_codes"])
        self.assertIn(5, sleeps, "integrity failure must enter retry/backoff path")

    def test_full_duration_recovered_network_warning_remains_acceptable(self):
        responses = [
            live("one"),
            live("two"),
            {"is_live": False},
            {"is_live": False},
            {"is_live": False},
        ]
        code, attempts, calls, _ = self.run_refresh(
            responses,
            [
                (0, "HTTP error 503\nWill reconnect at 50 in 1 second(s), error=End of file."),
                (0, ""),
            ],
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 2, "network warning must not consume the next live result as a failure probe")
        self.assertEqual(attempts[0]["ffmpeg_tail_class"], "NETWORK_WARNING")
        self.assertEqual(attempts[1]["ffmpeg_tail_class"], "BENIGN")
        serialized = json.dumps(attempts, ensure_ascii=False)
        self.assertNotIn("token=one", serialized)
        self.assertNotIn("token=two", serialized)
        self.assertIn("url_sha256", serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
