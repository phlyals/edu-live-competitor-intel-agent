import asyncio
import importlib.util
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(os.environ["RECORDER_UNDER_TEST"])
spec = importlib.util.spec_from_file_location("recorder_under_test", MODULE_PATH)
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


class RefreshPreresolutionTests(unittest.TestCase):
    def test_next_url_resolution_begins_before_current_ffmpeg_returns(self):
        """The old sequential implementation fails this ordering assertion."""
        first_record_active = threading.Event()
        allow_first_record_to_end = threading.Event()
        second_resolution_started = threading.Event()
        resolve_count = 0
        record_urls = []
        result_holder = {}

        async def resolve(_url, _quality):
            nonlocal resolve_count
            resolve_count += 1
            if resolve_count == 1:
                return live("one")
            if resolve_count == 2:
                second_resolution_started.set()
                return live("two")
            return {"is_live": False, "stream_url": None}

        def record(stream_url, path, _duration, _ffmpeg, _segment, _metadata):
            record_urls.append(stream_url)
            path.write_bytes(b"media")
            if len(record_urls) == 1:
                first_record_active.set()
                if not allow_first_record_to_end.wait(2):
                    raise AssertionError("test did not release first recording")
                return 0, ""
            return 1, "simulated final short read"

        def run():
            try:
                with tempfile.TemporaryDirectory() as directory:
                    result_holder["result"] = recorder.record_live_with_refresh(
                        "https://live.douyin.com/123",
                        "LD",
                        Path(directory) / "whole.ts.partial",
                        "ffmpeg",
                        0.2,
                    )
            except BaseException as exc:
                result_holder["error"] = exc

        with (
            patch.object(recorder, "resolve_stream", side_effect=resolve),
            patch.object(recorder, "record_stream", side_effect=record),
            patch.object(recorder, "probe_duration", side_effect=[0.2, None]),
            patch.object(recorder.time, "sleep"),
            patch.object(recorder, "REFRESH_PRERESOLVE_LEAD_SECONDS", 0.1, create=True),
        ):
            thread = threading.Thread(target=run)
            thread.start()
            try:
                self.assertTrue(first_record_active.wait(1), "first FFmpeg stub never started")
                self.assertTrue(
                    second_resolution_started.wait(0.6),
                    "next URL was not resolved until after current FFmpeg returned",
                )
            finally:
                allow_first_record_to_end.set()
                thread.join(timeout=3)

        self.assertFalse(thread.is_alive())
        if "error" in result_holder:
            raise result_holder["error"]
        self.assertEqual(result_holder["result"][0], 0)
        self.assertEqual(record_urls[:2], [
            "https://cdn.example/live.flv?token=one",
            "https://cdn.example/live.flv?token=two",
        ])

    def test_prefetched_url_is_never_written_to_attempt_metadata(self):
        responses = [live("SECRET_ONE"), live("SECRET_TWO"),
                     {"is_live": False}, {"is_live": False}, {"is_live": False}]
        second_resolution_started = threading.Event()
        release = threading.Event()
        calls = 0

        async def resolve(_url, _quality):
            nonlocal calls
            value = responses[calls]
            calls += 1
            if calls == 2:
                second_resolution_started.set()
            return value

        def record(_url, path, _duration, _ffmpeg, _segment, _metadata):
            path.write_bytes(b"media")
            if calls == 1:
                release.wait(1)
                return 0, ""
            return 1, ""

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(recorder, "resolve_stream", side_effect=resolve),
            patch.object(recorder, "record_stream", side_effect=record),
            patch.object(recorder, "probe_duration", side_effect=[0.2, None]),
            patch.object(recorder.time, "sleep"),
            patch.object(recorder, "REFRESH_PRERESOLVE_LEAD_SECONDS", 0.1, create=True),
        ):
            holder = {}
            thread = threading.Thread(target=lambda: holder.setdefault(
                "result", recorder.record_live_with_refresh(
                    "https://live.douyin.com/123", "LD",
                    Path(directory) / "whole.ts.partial", "ffmpeg", 0.2
                )
            ))
            thread.start()
            self.assertTrue(second_resolution_started.wait(0.6))
            release.set()
            thread.join(timeout=3)
            attempts = holder["result"][1]["attempts"]

        serialized = repr(attempts)
        self.assertNotIn("SECRET_ONE", serialized)
        self.assertNotIn("SECRET_TWO", serialized)
        self.assertIn("url_sha256", serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
