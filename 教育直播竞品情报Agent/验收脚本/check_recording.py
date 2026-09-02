#!/usr/bin/env python3
"""Read-only recording duration acceptance; no media mutation."""
from acceptance_common import expected_duration, probe, run_cli


def defaults():
    return dict(file_exists=False, file_path="", duration_seconds=None, file_size_mb=None,
                expected_duration_seconds=None, is_complete=False)


def check(args, repo, result):
    session = repo.session(args.session_id)
    path = repo.recording_path(session)
    result.update(file_path=str(path), file_exists=path.is_file())
    result["expected_duration_seconds"] = expected_duration(session)
    if not result["file_exists"]:
        result.update(status="FAIL", message="录制文件不存在")
        return
    duration, size = probe(path, args)
    expected = result["expected_duration_seconds"]
    complete = abs(duration - expected) / expected <= 0.05 + 1e-12
    result.update(duration_seconds=duration, file_size_mb=round(size / (1024 * 1024), 2),
                  is_complete=complete, status="PASS" if complete else "FAIL",
                  message="录制时长误差在 5% 以内" if complete else "录制时长误差超过 5%")


if __name__ == "__main__":
    raise SystemExit(run_cli("check_recording", "录制完整性检查（只读）", defaults, check))
