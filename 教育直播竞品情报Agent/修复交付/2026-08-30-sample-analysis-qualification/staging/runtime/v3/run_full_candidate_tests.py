import v3_analysis_contract,v3_analysis_worker,v3_evidence_worker,v3_pipeline_worker,v3_project_feishu,v3_runtime,v3_sample_analysis_migration,v3_sample_analysis_rollback,test_v3_segment_lifecycle
import unittest
from pathlib import Path
prod=Path('/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3')
names=[p.stem for p in sorted(prod.glob('test_v3*.py'))]+['test_v3_sample_analysis_qualification']
result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromNames(names))
print(f'TOTAL={result.testsRun} FAILURES={len(result.failures)} ERRORS={len(result.errors)}')
raise SystemExit(0 if result.wasSuccessful() else 1)
