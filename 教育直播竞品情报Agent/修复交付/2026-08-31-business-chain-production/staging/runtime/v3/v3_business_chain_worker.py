#!/usr/bin/env python3
"""Run comparison -> version -> strategy -> approved knowledge in order."""
from __future__ import annotations

import argparse
import json

import v3_comparison_worker as comparison
import v3_knowledge_worker as knowledge
import v3_strategy_worker as strategy
import v3_version_worker as version
from v3_runtime import utc_now


def once(*, connect_fn=None, init_db_fn=None) -> dict:
    result = {"errors": []}
    for name, function in (
        ("comparison", comparison.once),
        ("version", version.once),
        ("strategy", strategy.once),
        ("knowledge", knowledge.once),
    ):
        try:
            result[name] = function(connect_fn=connect_fn, init_db_fn=init_db_fn)
        except Exception as exc:  # each stage is fail-closed and independently retryable
            result[name] = {"status": "ERROR", "error_type": exc.__class__.__name__}
            result["errors"].append({"stage": name, "error_type": exc.__class__.__name__})
    result["status"] = "READY" if not result["errors"] else "DEGRADED"
    result["checked_at"] = utc_now()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("once",))
    args = parser.parse_args()
    print(json.dumps(once(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
