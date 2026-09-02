import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
import sys
from unittest.mock import patch


RUNTIME_V3 = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3")
sys.path.insert(0, str(RUNTIME_V3))
from test_v3_workflow import RuntimeCase


import v3_worker as worker


def probe_payload(duration: float) -> str:
    return json.dumps({
        "format": {"duration": str(duration), "size": "100"},
        "streams": [
            {"index": 0, "codec_type": "video", "codec_name": "h264", "width": 640, "height": 360},
            {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2},
        ],
    })


class FinalizerEvidenceTests(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.partial = self.root / "partial"
        self.completed = self.root / "completed"
        self.config = self.root / "v3_config.json"
        self.config.write_text(json.dumps({"retention": {"video_hours": 72, "delete_enabled": False}}), encoding="utf-8")
        self.partial.mkdir()
        with self.connect() as conn:
            conn.execute("INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at) VALUES('c','buyin','c','test','2026-01-01','2026-01-01')")
            conn.execute("INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,live_url,live_status) VALUES('m','c','ACTIVE','https://live.douyin.com/123','OFFLINE_CONFIRMED')")
            conn.execute("INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,ended_at,source_url,metadata_json) VALUES('s','m','room','ENDED','2026-08-28T00:00:00Z','2026-08-28T00:00:20Z','https://live.douyin.com/123','{}')")
            conn.execute("INSERT INTO recording_jobs(job_id,session_id,status,pid,account_key,recording_key,partial_dir,completed_dir,started_at,updated_at,restart_count) VALUES('j','s','WAITING_STREAM',NULL,'acct','sess',?,?,'2026-08-28T00:00:00Z','2026-08-28T00:00:20Z',0)", (str(self.partial), str(self.completed)))
        worker.RECORDER_PROCESSES.clear()
        self.concat_order = []

    def make_sources(self, *, with_sidecars=True):
        base = self.partial / "整场直播.ts.partial"
        refresh = self.partial / "整场直播.refresh0001.ts.partial"
        base.write_bytes(b"base-source-evidence")
        refresh.write_bytes(b"refresh-source-evidence")
        if with_sidecars:
            Path(str(base) + ".recording-state.json").write_text(json.dumps({
                "started_at": "2026-08-28T00:00:00Z", "ended_at": "2026-08-28T00:00:10Z",
                "status": "COMPLETE", "return_code": 0, "exit_kind": "NORMAL",
            }), encoding="utf-8")
            Path(str(refresh) + ".recording-state.json").write_text(json.dumps({
                "started_at": "2026-08-28T00:00:10Z", "ended_at": "2026-08-28T00:00:20Z",
                "status": "COMPLETE", "return_code": 0, "exit_kind": "NORMAL",
            }), encoding="utf-8")
        # Deliberately make mtime contradict the capture chronology.
        os.utime(base, (2_000, 2_000))
        os.utime(refresh, (1_000, 1_000))
        return base, refresh

    def fake_subprocess(self, *, concat_failure=False, empty_output=False, merged_duration=20.0):
        def run(command, **_kwargs):
            executable = Path(command[0]).name
            if executable == "ffprobe":
                target = Path(command[-1])
                duration = merged_duration if target.name in {".整场直播.merge.tmp", "整场直播.merge.tmp", "整场直播.ts"} else 10.0
                return SimpleNamespace(returncode=0, stdout=probe_payload(duration), stderr="")
            if executable == "ffmpeg":
                concat_list = Path(command[command.index("-i") + 1])
                self.concat_order = [Path(line[6:-1]).name for line in concat_list.read_text(encoding="utf-8").splitlines()]
                if concat_failure:
                    return SimpleNamespace(returncode=1, stdout="", stderr="injected concat failure")
                output = Path(command[-1])
                if empty_output:
                    output.touch()
                else:
                    output.write_bytes(b"validated-merged-output")
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            raise AssertionError(f"unexpected subprocess: {command}")
        return run

    def finalize(self, **faults):
        with self.connect() as conn:
            session = dict(conn.execute("SELECT * FROM live_sessions WHERE session_id='s'").fetchone())
            job = dict(conn.execute("SELECT * FROM recording_jobs WHERE session_id='s'").fetchone())
            with patch.object(worker, "pid_alive", return_value=False), patch.object(worker, "V3_CONFIG", self.config), patch.object(worker.subprocess, "run", side_effect=self.fake_subprocess(**faults)):
                worker.finalize_media_for_session(conn, session, job)
            conn.commit()

    def assert_sources(self, root):
        for name in ("整场直播.ts.partial", "整场直播.refresh0001.ts.partial"):
            self.assertTrue((root / name).is_file(), name)

    def test_concat_failure_preserves_sources_and_preconcat_manifest(self):
        base, refresh = self.make_sources()
        expected = {base.name: hashlib.sha256(base.read_bytes()).hexdigest(), refresh.name: hashlib.sha256(refresh.read_bytes()).hexdigest()}
        self.finalize(concat_failure=True)
        self.assert_sources(self.partial)
        self.assertFalse((self.partial / "整场直播.ts").exists())
        manifest_path = self.partial / "source-segments.preconcat.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["phase"], "PRE_CONCAT")
        self.assertEqual({entry["name"]: entry["sha256"] for entry in manifest["sources"]}, expected)

    def test_empty_concat_output_is_not_published_and_sources_survive(self):
        self.make_sources()
        self.finalize(empty_output=True)
        self.assert_sources(self.partial)
        self.assertFalse((self.partial / "整场直播.ts").exists())
        self.assertTrue((self.partial / "source-segments.preconcat.json").is_file())
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT status FROM live_sessions WHERE session_id='s'").fetchone()[0], "ENDED")
            self.assertIn("concat failed", conn.execute("SELECT last_error FROM recording_jobs WHERE session_id='s'").fetchone()[0])

    def test_duration_mismatch_never_publishes_or_deletes_sources(self):
        self.make_sources()
        self.finalize(merged_duration=12.0)
        self.assert_sources(self.partial)
        self.assertFalse((self.partial / "整场直播.ts").exists())
        self.assertFalse(self.completed.exists())
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT status FROM live_sessions WHERE session_id='s'").fetchone()[0], "ENDED")
            self.assertIn("validation failed", conn.execute("SELECT last_error FROM recording_jobs WHERE session_id='s'").fetchone()[0])

    def test_sidecar_started_at_wins_over_wrong_mtime(self):
        self.make_sources()
        self.finalize()
        self.assertEqual(self.concat_order, ["整场直播.ts.partial", "整场直播.refresh0001.ts.partial"])
        manifest = json.loads((self.completed / "source-segments.preconcat.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["ordering_strategy"], "SIDECAR_STARTED_AT")
        self.assertEqual(manifest["phase"], "PUBLISHED_SOURCES_RETAINED")
        self.assertEqual([entry["name"] for entry in manifest["sources"]], self.concat_order)
        self.assertTrue(all(Path(entry["path"]).is_file() for entry in manifest["sources"]))

    def test_missing_sidecars_use_composite_part_and_refresh_generation(self):
        names = ["整场直播.ts.partial", "整场直播.refresh0001.ts.partial",
                 "整场直播.part02.ts.partial", "整场直播.part02.refresh0001.ts.partial"]
        for index, name in enumerate(reversed(names)):
            path = self.partial / name
            path.write_bytes(name.encode())
            os.utime(path, (1_000 + index, 1_000 + index))
        self.finalize(merged_duration=40.0)
        self.assertEqual(self.concat_order, names)
        manifest = json.loads((self.completed / "source-segments.preconcat.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["ordering_strategy"], "EXPLICIT_GENERATION_THEN_FILENAME")

    def test_success_publishes_final_but_retains_sources_and_sidecars(self):
        self.make_sources()
        self.finalize()
        self.assertTrue((self.completed / "整场直播.ts").is_file())
        self.assert_sources(self.completed)
        self.assertTrue((self.completed / "整场直播.ts.partial.recording-state.json").is_file())
        self.assertTrue((self.completed / "整场直播.refresh0001.ts.partial.recording-state.json").is_file())
        manifest = json.loads((self.completed / "media-manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(len(manifest["retained_sources"]), 2)
        self.assertTrue(all(Path(entry["path"]).is_file() for entry in manifest["retained_sources"]))
        self.assertEqual(manifest["source_manifest_sha256"], hashlib.sha256((self.completed / "source-segments.preconcat.json").read_bytes()).hexdigest())
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT status FROM live_sessions WHERE session_id='s'").fetchone()[0], "MEDIA_COMPLETE")
            retention = json.loads(conn.execute("SELECT payload_json FROM retention_jobs WHERE object_id='s'").fetchone()[0])
        self.assertEqual(len(retention["retained_sources"]), 2)

    def test_real_ffmpeg_concat_retains_sources_and_validates_audio_video(self):
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe unavailable")
        sources = []
        for index in range(2):
            path = self.partial / ("整场直播.ts.partial" if index == 0 else "整场直播.refresh0001.ts.partial")
            command = [
                ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=320x240:rate=15",
                "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
                "-t", "2", "-c:v", "libx264", "-preset", "ultrafast",
                "-g", "15", "-pix_fmt", "yuv420p", "-c:a", "aac",
                "-f", "mpegts", str(path),
            ]
            subprocess.run(command, check=True, capture_output=True, timeout=60)
            Path(str(path) + ".recording-state.json").write_text(json.dumps({
                "started_at": f"2026-08-28T00:00:0{index * 2}Z",
                "ended_at": f"2026-08-28T00:00:0{index * 2 + 2}Z",
                "status": "EXITED", "return_code": 0, "exit_kind": "NORMAL",
                "ffmpeg_tail_class": "BENIGN", "ffmpeg_error_codes": [],
            }), encoding="utf-8")
            sources.append(path)
        with self.connect() as conn:
            session = dict(conn.execute("SELECT * FROM live_sessions WHERE session_id='s'").fetchone())
            job = dict(conn.execute("SELECT * FROM recording_jobs WHERE session_id='s'").fetchone())
            with patch.object(worker, "pid_alive", return_value=False), patch.object(worker, "V3_CONFIG", self.config):
                worker.finalize_media_for_session(conn, session, job)
            conn.commit()
        final = self.completed / "整场直播.ts"
        self.assertTrue(final.is_file())
        self.assertTrue(all((self.completed / source.name).is_file() for source in sources))
        manifest = json.loads((self.completed / "media-manifest.json").read_text())
        self.assertEqual(manifest["final_probe"]["stream_types"], ["audio", "video"])
        self.assertEqual(len(manifest["retained_sources"]), 2)


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=2)
