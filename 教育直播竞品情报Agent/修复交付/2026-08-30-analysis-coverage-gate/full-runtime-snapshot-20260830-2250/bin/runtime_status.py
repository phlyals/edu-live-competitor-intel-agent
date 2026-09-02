#!/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/.venv/bin/python
"""Report fail-closed, evidence-bearing capability and Runtime health."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from runtime_common import DB_PATH, PROFILE_ID, ROOT, load_config, parse_time, pid_alive, safety_gate, storage_status, utc_now


CAPABILITY_STATE = ROOT / "capabilities.json"
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
ALL_BUSINESS_TABLES = (
    "inbox_messages", "tasks", "task_attempts", "checkpoints", "domain_events",
    "products", "competitors", "identities", "product_competitors", "monitor_targets",
    "scan_runs", "scan_observations", "live_sessions", "recording_jobs", "recording_segments", "transcripts", "analyses", "strategy_candidates", "knowledge_versions", "knowledge_diffs", "review_items", "retention_jobs", "approvals",
    "outbox", "delivery_receipts", "dead_letters", "lineage_edges", "evidence_bundles", "heartbeats",
    "task_leases", "identity_evidence", "identity_conflicts", "worker_nodes", "recording_leases", "recording_gaps", "media_manifests", "deployment_releases", "projection_reconciliations", "capacity_test_runs", "fault_drill_runs", "audit_log",
)


def package_version(package: str) -> str | None:
    if not VENV_PYTHON.exists():
        return None
    code = "import importlib.metadata as m; print(m.version(%r))" % package
    try:
        return subprocess.run(
            [str(VENV_PYTHON), "-c", code], check=True, capture_output=True,
            text=True, timeout=10,
        ).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def executable_status(name: str) -> dict:
    path = shutil.which(name)
    if not path:
        return {"status": "WAITING_TOOL", "reason": f"{name} is not installed"}
    try:
        flag = "-version" if name == "ffmpeg" else "--version"
        line = subprocess.run([path, flag], capture_output=True, text=True, timeout=10).stdout.splitlines()[0]
    except (OSError, subprocess.SubprocessError, IndexError):
        line = "installed; version probe failed"
    return {"status": "READY", "path": path, "version": line}


def db_status() -> dict:
    try:
        v3_config = json.loads((ROOT / "v3" / "v3_config.json").read_text(encoding="utf-8"))
        if str(v3_config.get("control_plane_backend")) == "postgresql":
            import sys
            v3_root = ROOT / "v3"
            if str(v3_root) not in sys.path:
                sys.path.insert(0, str(v3_root))
            import v3_runtime
            snapshot = v3_runtime.status_snapshot()
            return {"status": "READY", "path": str((v3_config.get("postgresql") or {}).get("dsn") or "postgresql"), "backend": "postgresql", "integrity": "server-verified", "schema_version": snapshot.get("schema_version"), "counts": snapshot.get("counts") or {}, "activation": snapshot.get("activation") or {}}
    except Exception as exc:
        return {"status": "ERROR", "backend": "postgresql", "reason": str(exc)}
    if not DB_PATH.exists():
        return {"status": "WAITING_TOOL", "path": str(DB_PATH), "reason": "Runtime database is not initialized"}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            version = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            missing = [name for name in ALL_BUSINESS_TABLES if name not in tables]
            if missing:
                return {"status": "WAITING_TOOL", "path": str(DB_PATH), "reason": "Runtime schema migration is incomplete", "missing_tables": missing}
            counts = {table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in ALL_BUSINESS_TABLES}
        status = "READY" if integrity == "ok" else "ERROR"
        return {"status": status, "path": str(DB_PATH), "integrity": integrity, "schema_version": version[0] if version else None, "counts": counts}
    except sqlite3.Error as exc:
        return {"status": "ERROR", "path": str(DB_PATH), "reason": str(exc)}


def implementation_status(filename: str, capability: str) -> dict:
    path = ROOT / "bin" / filename
    if path.is_file() and os.access(path, os.R_OK):
        return {"status": "READY", "path": str(path), "capability": capability}
    return {"status": "WAITING_TOOL", "path": str(path), "capability": capability, "reason": "Production implementation is not installed"}


def self_test_implementation_status(filename: str, capability: str) -> dict:
    path = ROOT / "bin" / filename
    if not path.is_file() or not VENV_PYTHON.is_file():
        return {"status": "WAITING_TOOL", "path": str(path), "capability": capability, "reason": "Production implementation is not installed"}
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), str(path), "--self-test"],
            capture_output=True, text=True, check=False, timeout=30,
        )
        payload = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        return {"status": "ERROR", "path": str(path), "capability": capability, "reason": f"Self-test failed: {exc}"}
    return {"path": str(path), "capability": capability, **payload}


def local_model_status() -> dict:
    candidates = sorted((ROOT / "models").glob("models--Systran--faster-whisper-*/snapshots/*"))
    usable = [p for p in candidates if (p / "config.json").exists() and (p / "model.bin").exists()]
    if not usable:
        return {"status": "WAITING_TOOL", "reason": "No local faster-whisper model snapshot"}
    return {"status": "READY", "model_path": str(usable[-1])}


def chinese_asr_status(path: Path) -> dict:
    if not path.is_file():
        return {"status": "WAITING_TOOL", "reason": "Chinese speech accuracy has not been validated", "expected_evidence_path": str(path)}
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "ERROR", "reason": f"Chinese ASR evidence is invalid: {exc}", "evidence_path": str(path)}
    status = evidence.get("status", "ERROR")
    if evidence.get("validation_level") == "synthetic_tts" and status == "READY":
        status = "DEGRADED"
    return {"status": status, "evidence_path": str(path), "validation_level": evidence.get("validation_level"), "production_ready": bool(evidence.get("production_ready")), "reason": evidence.get("reason"), "checked_at": evidence.get("checked_at"), "keyword_hits": evidence.get("keyword_hits", [])}


def cached_capability(name: str, fallback: dict, max_age_seconds: int | None = None) -> dict:
    try:
        stored = json.loads(CAPABILITY_STATE.read_text(encoding="utf-8")).get(name)
        if not isinstance(stored, dict):
            return fallback
        now = datetime.now(timezone.utc)
        expiry = parse_time(stored.get("expires_at"))
        verified = parse_time(stored.get("verified_at"))
        if expiry and expiry <= now:
            return {
                "status": "WAITING_HUMAN",
                "reason": "Cached verification has expired; a real read-only recheck is required",
                "last_verified_at": stored.get("verified_at"),
                "expired_at": stored.get("expires_at"),
            }
        if max_age_seconds and (not verified or (now - verified).total_seconds() > max_age_seconds):
            return {
                "status": "DEGRADED",
                "reason": "Cached verification is stale; a live recheck is required before writes",
                "last_verified_at": stored.get("verified_at"),
            }
        return {**stored, "source": "verified_cache"}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return fallback


def gateway_status() -> dict:
    pid_file = ROOT.parent / "gateway.pid"
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
        pid = int(json.loads(raw).get("pid")) if raw.startswith("{") else int(raw)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {"status": "WAITING_TOOL", "reason": "Target Gateway PID file is unavailable"}
    if not pid_alive(pid):
        return {"status": "ERROR", "pid": pid, "reason": "Target Gateway process is not alive"}
    try:
        command = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "command="], check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        command = ""
    explicit = f"--profile {PROFILE_ID}" in command
    return {
        "status": "READY" if explicit else "ERROR",
        "pid": pid,
        "explicit_profile": explicit,
        "reason": None if explicit else "Gateway process is not explicitly bound to the target Profile",
    }


def v3_status() -> dict:
    try:
        import sys
        v3_root = ROOT / "v3"
        if str(v3_root) not in sys.path:
            sys.path.insert(0, str(v3_root))
        import v3_runtime
        return v3_runtime.status_snapshot()
    except Exception as exc:  # noqa: BLE001
        return {"schema_version": None, "activation": {"ready": False, "reason": str(exc)}}


def live_cua_status() -> dict:
    socket = ROOT.parent / "home" / "Library" / "Caches" / "cua-driver" / "cua-driver.sock"
    binary = Path("/Users/mac/.local/bin/cua-driver")
    if not binary.is_file() or not socket.exists():
        return {"status": "WAITING_TOOL", "reason": "Profile Cua Driver socket is unavailable"}
    try:
        result = subprocess.run([str(binary), "call", "list_windows", '{"on_screen_only":true}', "--socket", str(socket)], capture_output=True, text=True, timeout=15, check=False)
        payload = json.loads(result.stdout)
        windows = payload.get("windows") or []
        tabbit = [item for item in windows if str(item.get("app_name") or "").lower() == "tabbit"]
        return {"status": "READY" if result.returncode == 0 and tabbit else "WAITING_HUMAN", "window_count": len(tabbit), "reason": None if tabbit else "No visible Tabbit window"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "WAITING_TOOL", "reason": f"Cua Driver live probe failed: {exc.__class__.__name__}"}


def feishu_workspace_status(config: dict) -> dict:
    try:
        v3_config = json.loads((ROOT / "v3" / "v3_config.json").read_text(encoding="utf-8"))
        cli = Path(v3_config["lark_cli"])
        table_id = v3_config["business_tables"]["10_Runtime状态"]
        result = subprocess.run([str(cli), "base", "+field-list", "--profile", v3_config["lark_cli_profile"], "--base-token", v3_config["business_base"], "--table-id", table_id, "--format", "json", "--as", "bot"], capture_output=True, text=True, timeout=60, check=False)
        payload = json.loads(result.stdout or result.stderr)
        return {"status": "READY" if result.returncode == 0 and payload.get("ok") else "WAITING_HUMAN", "profile_id": v3_config["lark_cli_profile"], "table_id": table_id, "reason": None if result.returncode == 0 and payload.get("ok") else "Profile-pinned Feishu schema probe failed"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "WAITING_TOOL", "reason": f"Feishu live probe failed: {exc.__class__.__name__}"}


def worker_status() -> dict:
    try:
        import sys
        v3_root = ROOT / "v3"
        if str(v3_root) not in sys.path:
            sys.path.insert(0, str(v3_root))
        import v3_runtime
        with v3_runtime.connect() as conn:
            row = conn.execute("SELECT pid,status,started_at,last_heartbeat_at,last_success_at,details_json FROM heartbeats WHERE service_name=?", ("runtime-v3",)).fetchone()
    except Exception as exc:
        return {"status": "WAITING_TOOL", "reason": f"Runtime heartbeat is unavailable: {exc}"}
    if not row:
        return {"status": "WAITING_TOOL", "reason": "Runtime supervisor has not emitted a real heartbeat"}
    if isinstance(row, dict):
        pid = row["pid"]
        status = row["status"]
        started_at = row["started_at"]
        last_heartbeat_at = row["last_heartbeat_at"]
        last_success_at = row["last_success_at"]
        details_json = row["details_json"]
    else:
        pid, status, started_at, last_heartbeat_at, last_success_at, details_json = row
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        pid = 0
    heartbeat = parse_time(last_heartbeat_at)
    age = (datetime.now(timezone.utc) - heartbeat).total_seconds() if heartbeat else None
    if not pid_alive(pid):
        status = "ERROR"
        reason = "Runtime supervisor PID is not alive"
    elif age is None or age > 180:
        status = "ERROR"
        reason = "Runtime supervisor heartbeat is stale"
    else:
        reason = None
    try:
        details = json.loads(details_json or "{}")
    except json.JSONDecodeError:
        details = {}
    return {"status": status, "pid": pid, "started_at": started_at, "last_heartbeat_at": last_heartbeat_at, "heartbeat_age_seconds": age, "last_success_at": last_success_at, "details": details, "reason": reason}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    parser.parse_args()
    try:
        config = load_config()
        config_status = {"status": "READY", "path": str(ROOT / "config.json"), "profile_id": config["profile_id"]}
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        config = {"safety": {}}
        config_status = {"status": "ERROR", "reason": str(exc)}
    model = local_model_status()
    asr_version = package_version("faster-whisper")
    streamlink_version = package_version("streamlink")
    ytdlp_version = package_version("yt-dlp")
    ffmpeg = executable_status("ffmpeg")
    stream_tools_status = "READY" if streamlink_version and ytdlp_version and ffmpeg["status"] == "READY" else "WAITING_TOOL"
    chinese_validation = ROOT / "test_artifacts" / "chinese_asr_validation.json"
    knowledge_path = (config.get("storage") or {}).get("directories", {}).get("knowledge_vault")
    douyin_profile_mapping = cached_capability("douyin_profile_mapping", {"status": "WAITING_HUMAN", "reason": "No real Buyin account QR image has been decoded and validated yet"})
    if douyin_profile_mapping.get("status") == "READY":
        live_source_mapping = {"status": "READY", "reason": None, "source": "validated Douyin profile mapping plus reused recorder profile resolver"}
    elif douyin_profile_mapping.get("status") == "DEGRADED" and douyin_profile_mapping.get("bound_buyin_account_count", 0) > 0:
        live_source_mapping = {
            "status": "DEGRADED",
            "reason": f"{douyin_profile_mapping.get('bound_buyin_account_count')} of {douyin_profile_mapping.get('target_account_count')} Buyin accounts have a validated Douyin monitor URL; remaining accounts are unmapped",
        }
    else:
        live_source_mapping = {
            "status": douyin_profile_mapping.get("status", "WAITING_HUMAN"),
            "reason": "The QR decoder and recorder resolver are installed, but the real decoded QR is not yet bound to a Buyin uid and has not produced a reliable anchor/room identity for monitoring",
        }
    result = {
        "profile_id": PROFILE_ID,
        "checked_at": utc_now(),
        "runtime_root": str(ROOT),
        "runtime_config": config_status,
        "profile_gateway": gateway_status(),
        "runtime": db_status(),
        "runtime_worker": worker_status(),
        "storage": storage_status(config) if config_status["status"] == "READY" else config_status,
        "local_asr": {"status": "READY" if asr_version and model["status"] == "READY" else "WAITING_TOOL", "package": "faster-whisper", "version": asr_version, "model": model},
        "asr_chinese_validation": chinese_asr_status(chinese_validation),
        "stream_tools": {"status": stream_tools_status, "streamlink": {"status": "READY" if streamlink_version else "WAITING_TOOL", "version": streamlink_version}, "yt_dlp": {"status": "READY" if ytdlp_version else "WAITING_TOOL", "version": ytdlp_version}, "ffmpeg": ffmpeg},
        "computer_use": cached_capability("computer_use", {"status": "WAITING_TOOL", "reason": "CuaDriver is not verified"}, 86400),
        "browser_giant_buyin": cached_capability("browser_giant_buyin", {"status": "WAITING_HUMAN", "reason": "A logged-in browser session must be verified"}),
        "feishu_business_workspace": cached_capability("feishu_business_workspace", {"status": "WAITING_HUMAN", "reason": "Feishu Base authorization has not been verified"}, 86400),
        "scanner_validation_implementation": implementation_status("scanner.py", "read-only scan draft validation"),
        "identity_resolution_implementation": implementation_status("identity_verifier.py", "Buyin detail-page uid identity validation"),
        "qr_profile_resolver_implementation": self_test_implementation_status("qr_profile_resolver.py", "saved QR image to validated Douyin profile URL"),
        "douyin_profile_mapping": douyin_profile_mapping,
        "homepage_monitor_bridge": cached_capability("homepage_monitor_bridge", {"status": "WAITING_TOOL", "reason": "No real Douyin account-homepage check-only probe has succeeded"}),
        "buyin_identity_resolution": cached_capability("buyin_identity_resolution", {"status": "WAITING_HUMAN", "reason": "Unique competitor accounts have not been resolved through detail-page uid plus QR-confirmed Douyin ID"}),
        "browser_scanner_implementation": implementation_status("browser_scanner.py", "repeatable browser product scanner with stable account IDs"),
        "scanner_implementation": implementation_status("browser_scanner.py", "production product scan"),
        "live_source_mapping": live_source_mapping,
        "monitor_implementation": self_test_implementation_status("monitor.py", "one-pass tri-state live monitor for mapped Douyin room URLs"),
        "recorder_implementation": self_test_implementation_status("recorder.py", "profile-isolated reuse of the proven shared Douyin recorder"),
        "transcription_implementation": implementation_status("transcribe.py", "local transcription"),
        "outbox_implementation": implementation_status("outbox_worker.py", "Feishu outbox delivery"),
        "knowledge_writer_implementation": implementation_status("knowledge_writer.py", "formal knowledge write"),
        "real_product_scan_gate": safety_gate(config, "real_product_scan", "real product scan"),
        "live_monitor_gate": safety_gate(config, "live_monitor", "continuous live monitor"),
        "recording_gate": safety_gate(config, "recording", "real live recording"),
        "feishu_business_write_gate": safety_gate(config, "feishu_business_write", "Feishu business record write"),
        "formal_knowledge_write_gate": safety_gate(config, "formal_knowledge_write", "formal knowledge publication"),
        "knowledge_root": {"status": "READY", "path": knowledge_path} if knowledge_path and Path(knowledge_path).is_dir() else {"status": "WAITING_TOOL", "reason": "Configured knowledge-vault directory is not initialized"},
    }
    # Runtime V3 is authoritative.  Do not report stale V1 capability caches
    # as the live production state after atomic activation.
    v3 = v3_status()
    activation = v3.get("activation") or {}
    if v3.get("schema_version") == "3":
        v3_config = json.loads((ROOT / "v3" / "v3_config.json").read_text(encoding="utf-8"))
        result["runtime"] = db_status()
        result["computer_use"] = live_cua_status()
        result["browser_giant_buyin"] = {"status": "READY", "source": "live_cua_and_v3_full_fleet"} if activation.get("ready") else result["browser_giant_buyin"]
        result["feishu_business_workspace"] = feishu_workspace_status(config)
        counts = v3.get("counts") or {}
        total = int(activation.get("total_competitors") or 0)
        verified = int(activation.get("verified_monitor_targets") or 0)
        result["douyin_profile_mapping"] = {"status": "READY" if total and verified == total else "WAITING_HUMAN", "verified_target_count": verified, "target_count": total, "source": "runtime_v3"}
        result["live_source_mapping"] = {"status": "READY" if total and verified == total else "WAITING_HUMAN", "verified_target_count": verified, "target_count": total, "source": "runtime_v3"}
        result["buyin_identity_resolution"] = {"status": "READY" if total and verified == total else "WAITING_HUMAN", "verified_target_count": verified, "target_count": total, "source": "runtime_v3"}
        result["runtime_worker"] = worker_status()
        if activation.get("ready"):
            result["real_product_scan_gate"] = {"status": "READY", "enabled": True, "capability": "real product scan", "source": "runtime_v3_atomic_activation"}
            result["live_monitor_gate"] = {"status": "READY", "enabled": True, "capability": "continuous live monitor", "source": "runtime_v3_atomic_activation", "full_fleet": True, "target_count": total, "interval_seconds": int((v3_config.get("atomic_activation") or {}).get("monitor_interval_seconds") or 30)}
            result["recording_gate"] = {"status": "READY", "enabled": True, "capability": "real live recording", "source": "runtime_v3_atomic_activation", "quality": v3_config.get("recording_quality"), "mode": v3_config.get("recording_mode"), "whole_session_file": v3_config.get("recording_single_final_file") is True, "segment_seconds": v3_config.get("recording_segment_seconds"), "duration_seconds": v3_config.get("recording_duration_seconds"), "normal_concurrency": v3_config.get("normal_recording_concurrency"), "expected_max_concurrency": v3_config.get("expected_max_recording_concurrency"), "capacity_test_concurrency": v3_config.get("capacity_test_concurrency"), "speech_audio_format": v3_config.get("asr_audio_format"), "speech_audio_bitrate_kbps": v3_config.get("asr_audio_bitrate_kbps"), "retention": v3_config.get("retention") or {}}
            result["feishu_business_write_gate"] = {"status": "READY", "enabled": True, "capability": "Feishu business record write", "source": "runtime_v3_atomic_activation"}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
