#!/usr/bin/env python3
"""MVP-D: verify one Buyin-to-Douyin mapping and run two read-only probes.

This deliberately does not start a persistent monitor or a recording.  The
interactive operator first opens the creator's QR card in the authenticated
Tabbit session, then passes the visible Douyin ID and current Buyin detail URL
to this small local entry point.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


PROFILE_ID = "edu_live_competitor_intel"
OUTPUT_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/mvp-d")
RUNTIME_ROOT = Path(__file__).resolve().parents[1]
PYTHON = RUNTIME_ROOT / ".venv" / "bin" / "python"
RECORDER = RUNTIME_ROOT / "bin" / "recorder.py"
STREAMGET_PROBE = RUNTIME_ROOT / "bin" / "streamget_probe.py"
UPSTREAM_RECORDER = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/DouyinLiveRecorder")


class MvpDFailure(RuntimeError):
    """A bounded validation failure that must not be converted to offline."""

    def __init__(self, error_type: str, message: str):
        super().__init__(message)
        self.error_type = error_type


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compact_time_id() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")


def json_bytes(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def atomic_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(json_bytes(payload))
    temporary.chmod(0o600)
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def confined_output(path: Path) -> Path:
    root = OUTPUT_ROOT.resolve()
    resolved = path.expanduser().resolve()
    if root not in resolved.parents:
        raise MvpDFailure("OUTPUT_PATH_REJECTED", "输出目录必须是MVP-D目录下的独立子目录")
    return resolved


def validate_buyin_uid(value: str, label: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"v2_[A-Za-z0-9_]{64,600}", value):
        raise MvpDFailure("BUYIN_UID_INVALID", f"{label}不是可验证的Buyin creator UID")
    return value


def validate_douyin_id(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[0-9]{5,30}", value):
        raise MvpDFailure("DOUYIN_ID_INVALID", "二维码卡片中的抖音号格式无效")
    return value


def canonical_buyin_detail_url(raw_url: str, expected_uid: str) -> str:
    parsed = urlparse(raw_url.strip())
    if (
        parsed.scheme != "https"
        or parsed.hostname != "buyin.jinritemai.com"
        or parsed.path != "/dashboard/followed-daren"
    ):
        raise MvpDFailure("BUYIN_DETAIL_URL_INVALID", "当前页面不是巨量百应达人详情页")
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    uids = [value for key, value in pairs if key == "uid"]
    if len(uids) != 1:
        raise MvpDFailure("BUYIN_UID_MISSING", "达人详情URL必须且只能包含一个uid参数")
    if uids[0] != expected_uid:
        raise MvpDFailure("BUYIN_UID_MISMATCH", "达人详情URL UID与当前目标UID不一致")
    # Keep only stable navigation fields; discard temporary page parameters.
    safe_pairs = [(key, value) for key, value in pairs if key in {"daren_type", "uid"}]
    return urlunparse(parsed._replace(query=urlencode(safe_pairs), fragment=""))


def parse_json_payload(text: str) -> dict:
    decoder = json.JSONDecoder()
    found: list[dict] = []
    for offset, character in enumerate(text or ""):
        if character != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[offset:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("status"), str):
            found.append(value)
    if not found:
        raise MvpDFailure("PROBE_OUTPUT_INVALID", "只读检测工具未返回结构化JSON")
    return found[-1]


def clean_probe_payload(payload: dict, return_code: int) -> dict:
    allowed = {
        "status", "checked_at", "anchor_name", "title", "room_id",
        "upstream_version", "shared_status", "authorization_mode",
        "recording_started", "reason",
    }
    cleaned = {key: payload.get(key) for key in allowed if key in payload}
    cleaned["process_return_code"] = return_code
    cleaned["recording_started"] = payload.get("recording_started") is True
    return cleaned


def run_probe(command: list[str]) -> dict:
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    payload = parse_json_payload(completed.stdout)
    return clean_probe_payload(payload, completed.returncode)


def strict_consensus(streamget: dict, recorder: dict) -> tuple[str, str]:
    states = [streamget.get("status"), recorder.get("status")]
    if states == ["LIVE", "LIVE"]:
        return "LIVE", "streamget_and_douyin_live_recorder_both_report_live"
    if states == ["OFFLINE_CONFIRMED", "OFFLINE_CONFIRMED"]:
        return "OFFLINE_CONFIRMED", "streamget_and_douyin_live_recorder_both_report_offline"
    return "UNKNOWN", "probe_results_do_not_form_strict_consensus"


def git_revision(path: Path) -> str | None:
    if not (path / ".git").exists():
        return None
    completed = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    revision = completed.stdout.strip()
    return revision if completed.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", revision) else None


def verify_probe_identity(competitor_name: str, probes: list[dict]) -> None:
    normalized = re.sub(r"\s+", "", competitor_name)
    for probe in probes:
        anchor = re.sub(r"\s+", "", str(probe.get("anchor_name") or ""))
        if not anchor or anchor != normalized:
            raise MvpDFailure("PROBE_IDENTITY_MISMATCH", "检测工具返回的主播名与二维码卡片账号不一致")
        if probe.get("recording_started") is True:
            raise MvpDFailure("UNEXPECTED_RECORDING", "只读检测路径意外报告已启动录制")


def self_test() -> int:
    sample_uid = "v2_" + "a" * 80
    checks = {
        "buyin_uid_validation": validate_buyin_uid(sample_uid, "test") == sample_uid,
        "douyin_id_validation": validate_douyin_id("71883486974") == "71883486974",
        "offline_consensus": strict_consensus(
            {"status": "OFFLINE_CONFIRMED"}, {"status": "OFFLINE_CONFIRMED"}
        )[0] == "OFFLINE_CONFIRMED",
        "disagreement_is_unknown": strict_consensus(
            {"status": "LIVE"}, {"status": "OFFLINE_CONFIRMED"}
        )[0] == "UNKNOWN",
        "recorder_wrapper_present": RECORDER.is_file(),
        "streamget_probe_present": STREAMGET_PROBE.is_file(),
        "runtime_python_present": PYTHON.is_file(),
    }
    payload = {
        "status": "READY" if all(checks.values()) else "WAITING_TOOL",
        "checked_at": utc_now(),
        "checks": checks,
        "network_request_performed": False,
        "recording_started": False,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["status"] == "READY" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--competitor-name")
    parser.add_argument("--buyin-uid")
    parser.add_argument("--buyin-detail-url")
    parser.add_argument("--douyin-id")
    parser.add_argument("--historical-buyin-uid")
    parser.add_argument("--qr-card-verified", action="store_true")
    parser.add_argument("--approved-read-only-probe", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    required = {
        "--competitor-name": args.competitor_name,
        "--buyin-uid": args.buyin_uid,
        "--buyin-detail-url": args.buyin_detail_url,
        "--douyin-id": args.douyin_id,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))
    if not args.qr_card_verified:
        parser.error("current Tabbit QR card verification requires --qr-card-verified")
    if not args.approved_read_only_probe:
        parser.error("network status probes require --approved-read-only-probe")

    output_dir = confined_output(args.output_dir or (OUTPUT_ROOT / compact_time_id()))
    output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    checked_at = utc_now()
    current_uid = validate_buyin_uid(args.buyin_uid, "当前页面UID")
    historical_uid = (
        validate_buyin_uid(args.historical_buyin_uid, "历史UID")
        if args.historical_buyin_uid else None
    )
    detail_url = canonical_buyin_detail_url(args.buyin_detail_url, current_uid)
    douyin_id = validate_douyin_id(args.douyin_id)
    live_monitor_url = f"https://live.douyin.com/{douyin_id}"

    account_mapping = {
        "schema_version": 1,
        "profile_id": PROFILE_ID,
        "mapped_at": checked_at,
        "competitor_name": args.competitor_name.strip(),
        "buyin_creator_uid": current_uid,
        "buyin_detail_url": detail_url,
        "historical_buyin_creator_uid": historical_uid,
        "historical_uid_conflict": bool(historical_uid and historical_uid != current_uid),
        "douyin_account_id": douyin_id,
        "douyin_account_url": None,
        "live_monitor_url": live_monitor_url,
        "mapping_source": "current_authenticated_buyin_detail_qr_card_visible_douyin_id",
        "qr_card_verified": True,
        "identity_mapping_status": "VERIFIED_CURRENT_PAGE_QR_CARD",
        "name_used_as_unique_identity": False,
    }

    errors: list[dict] = []
    try:
        streamget = run_probe([str(PYTHON), str(STREAMGET_PROBE), "--url", live_monitor_url])
        recorder = run_probe([
            str(PYTHON), str(RECORDER), "--url", live_monitor_url,
            "--check-only", "--approved-read-only-probe",
        ])
        verify_probe_identity(account_mapping["competitor_name"], [streamget, recorder])
        live_status, status_evidence = strict_consensus(streamget, recorder)
    except (MvpDFailure, subprocess.TimeoutExpired) as exc:
        streamget = locals().get("streamget", {"status": "UNKNOWN", "recording_started": False})
        recorder = locals().get("recorder", {"status": "UNKNOWN", "recording_started": False})
        live_status = "UNKNOWN"
        status_evidence = "probe_error_or_identity_mismatch"
        errors.append({
            "type": getattr(exc, "error_type", "PROBE_TIMEOUT"),
            "message": str(exc),
        })

    monitor_result = {
        "schema_version": 1,
        "profile_id": PROFILE_ID,
        "checked_at": checked_at,
        "competitor_name": account_mapping["competitor_name"],
        "buyin_creator_uid": current_uid,
        "douyin_account_id": douyin_id,
        "live_monitor_url": live_monitor_url,
        "identity_mapping_status": account_mapping["identity_mapping_status"],
        "live_status": live_status,
        "status_evidence": status_evidence,
        "probes": {
            "streamget": streamget,
            "douyin_live_recorder": recorder,
        },
        "errors": errors,
        "recording_started": False,
        "persistent_monitor_started": False,
    }

    status = "COMPLETE" if live_status in {"LIVE", "OFFLINE_CONFIRMED"} and not errors else "INCOMPLETE"
    mapping_path = output_dir / "account_mapping.json"
    result_path = output_dir / "monitor_result.json"
    manifest_path = output_dir / "mvp_d_manifest.json"
    atomic_write(mapping_path, account_mapping)
    atomic_write(result_path, monitor_result)
    manifest = {
        "schema_version": 1,
        "profile_id": PROFILE_ID,
        "status": status,
        "checked_at": checked_at,
        "competitor_name": account_mapping["competitor_name"],
        "identity_mapping_status": account_mapping["identity_mapping_status"],
        "live_status": live_status,
        "tooling": {
            "primary": "ihmily/DouyinLiveRecorder",
            "upstream_root": str(UPSTREAM_RECORDER),
            "upstream_commit": git_revision(UPSTREAM_RECORDER),
            "independent_probe": "streamget",
        },
        "outputs": {
            "account_mapping.json": str(mapping_path),
            "monitor_result.json": str(result_path),
            "mvp_d_manifest.json": str(manifest_path),
        },
        "file_sha256": {
            "account_mapping.json": sha256_file(mapping_path),
            "monitor_result.json": sha256_file(result_path),
            "mvp_d_manifest.json": None,
        },
        "side_effects": {
            "sqlite_written": False,
            "feishu_base_written": False,
            "runtime_worker_modified": False,
            "background_service_started": False,
            "recording_started": False,
            "frozen_files_modified": False,
        },
        "errors": errors,
    }
    canonical_digest = hashlib.sha256(json_bytes(manifest)).hexdigest()
    manifest["file_sha256"]["mvp_d_manifest.json"] = {
        "sha256": canonical_digest,
        "scope": "canonical manifest with this value set to null",
    }
    atomic_write(manifest_path, manifest)
    print(json.dumps({
        "status": status,
        "live_status": live_status,
        "output_dir": str(output_dir),
        "recording_started": False,
    }, ensure_ascii=False))
    return 0 if status == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
