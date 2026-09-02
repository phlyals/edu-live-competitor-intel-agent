import pathlib
import sys
import unittest

root = pathlib.Path('/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime')
sys.path.insert(0, str(root / 'v3'))
sys.path.insert(0, str(root / 'bin'))
import recorder
recorder.SHARED_RECORDER = pathlib.Path(__file__).resolve().parent / 'candidate' / 'record_douyin_live.py'
import test_v3_recording_transcription as tests
suite = unittest.defaultTestLoader.loadTestsFromTestCase(tests.SharedRecorderTests)
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
