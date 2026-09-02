#!/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/.venv/bin/python
"""Retired MVP-C compatibility stub.

Production execution is owned by Runtime V3 Final.  The historical pure
helpers remain importable for audit/unit tests from the read-only archive, but
this file can never scan or deliver a production result.
"""

from __future__ import annotations

import json
from pathlib import Path

ARCHIVE = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/backups/retired-mvp-c-final-20260825/mvp_c_runner.py")
PROFILE_PIN = "--profile edu_live_competitor_intel"
if __name__ != "__main__" and ARCHIVE.is_file():
    # Execute the archived helper library in this module namespace so audit
    # tests can patch its transport functions without re-enabling its main
    # production entry point.
    exec(compile(ARCHIVE.read_text(encoding="utf-8"), str(ARCHIVE), "exec"), globals())


def main() -> int:
    print(json.dumps({
        "status": "FAILED",
        "error_type": "LEGACY_EXECUTION_DISABLED",
        "error_message": "旧MVP-C入口已退役；生产扫描必须由Runtime V3 Final执行",
        "delivery": {"status": "NOT_ATTEMPTED"},
    }, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
