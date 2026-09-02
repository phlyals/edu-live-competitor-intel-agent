"""Run the existing production SharedRecorderTests against the staged candidate."""

import importlib.util
from pathlib import Path
import sys
import unittest


V3_ROOT = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3")
STAGING_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(V3_ROOT))

import test_v3_recording_transcription as production_tests

spec = importlib.util.spec_from_file_location(
    "staged_candidate_recorder", STAGING_ROOT / "record_douyin_live.py"
)
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)
production_tests.shared_recorder = candidate

suite = unittest.defaultTestLoader.loadTestsFromTestCase(production_tests.SharedRecorderTests)
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
