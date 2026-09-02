#!/usr/bin/env python3
"""Full post-cutover consistency check for Runtime V3 and Feishu projection."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import v3_runtime as v3  # noqa: E402
from v3_project_feishu import existing_records, field_names, record_rows  # noqa: E402

REPORT_PATH = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/final-self-check.json")


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def recorder_process_lines() -> list[str]:
    result = subprocess.run(["/bin/ps", "-axo", "pid=,command="], capture_output=True, text=True, timeout=15, check=False)
    return [line.strip() for line in result.stdout.splitlines() if "/runtime/bin/recorder.py" in line or "record_douyin_live.py" in line]


def positive_segment_processes(lines: list[str]) -> list[str]:
    pattern = re.compile(r"--segment-seconds\s+([0-9]+(?:\.[0-9]+)?)")
    return [line for line in lines if (match := pattern.search(line)) and float(match.group(1)) > 0]


def main() -> int:
    checks = []
    def add(name, passed, detail):
        checks.append({"name": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    try:
        identity = v3.identity_assertion(verify_cli=True)
        add("identity_lock", True, identity)
    except Exception as exc:  # noqa: BLE001
        add("identity_lock", False, str(exc))

    try:
        snapshot = v3.status_snapshot()
        activation = snapshot["activation"]
        counts = snapshot["counts"]
        with v3.connect() as conn:
            pending_outbox = conn.execute("SELECT count(*) FROM outbox WHERE status NOT IN ('SENT','DEAD_LETTER')").fetchone()[0]
            total_competitors = conn.execute("SELECT count(*) FROM competitors WHERE platform='buyin'").fetchone()[0]
            douyin_verified = conn.execute("SELECT count(DISTINCT competitor_id) FROM identities WHERE platform='douyin' AND verification_status='VERIFIED'").fetchone()[0]
            active_targets = conn.execute("SELECT count(*) FROM monitor_targets WHERE status='ACTIVE'").fetchone()[0]
        scalable_ready = total_competitors > 0 and douyin_verified == total_competitors and active_targets == total_competitors
        add("runtime_v3_counts", activation["ready"] and pending_outbox == 0 and scalable_ready, {"counts": counts, "total_buyin_competitors": total_competitors, "douyin_verified_competitors": douyin_verified, "active_targets": active_targets, "pending_outbox": pending_outbox, "activation": activation})
    except Exception as exc:  # noqa: BLE001
        add("runtime_v3_counts", False, str(exc))

    config = json.loads(v3.CONFIG_PATH.read_text(encoding="utf-8"))
    runtime_config = json.loads((v3.RUNTIME_ROOT / "config.json").read_text(encoding="utf-8"))
    table_keys = {
        "01_商品": ("商品ID", []),
        "02_同行账号": ("同行ID", []),
        "03_商品同行关系": ("关系ID", []),
        "04_账号监控": ("监控ID", []),
        "05_直播场次": ("场次ID", []),
        "06_转录证据": ("转录ID", []),
        "07_场次分析": ("分析ID", []),
        "08_打法版本": ("版本ID", []),
        "09_策略候选审批": ("候选ID", []),
        "10_Runtime状态": ("任务ID", ["runtime-v3"]),
    }
    try:
        with v3.connect() as conn:
            for row in conn.execute("SELECT product_id FROM products ORDER BY product_id"):
                table_keys["01_商品"][1].append(row[0])
            for row in conn.execute("SELECT competitor_id FROM competitors ORDER BY competitor_id"):
                table_keys["02_同行账号"][1].append(row[0])
            for row in conn.execute("SELECT relation_id FROM product_competitors ORDER BY relation_id"):
                table_keys["03_商品同行关系"][1].append(row[0])
            for row in conn.execute("SELECT monitor_target_id FROM monitor_targets ORDER BY monitor_target_id"):
                table_keys["04_账号监控"][1].append(row[0])
            for row in conn.execute("SELECT session_id FROM live_sessions WHERE status!='DUPLICATE_SUPERSEDED' ORDER BY session_id"):
                table_keys["05_直播场次"][1].append(row[0])
            for row in conn.execute("SELECT transcript_id FROM transcripts ORDER BY transcript_id"):
                table_keys["06_转录证据"][1].append(row[0])
            for row in conn.execute("SELECT analysis_id FROM analyses WHERE status!='SKIPPED_HISTORICAL' ORDER BY analysis_id"):
                table_keys["07_场次分析"][1].append(row[0])
            for row in conn.execute("SELECT version_id FROM knowledge_versions ORDER BY version_id"):
                table_keys["08_打法版本"][1].append(row[0])
            for row in conn.execute("SELECT candidate_id FROM strategy_candidates ORDER BY candidate_id"):
                table_keys["09_策略候选审批"][1].append(row[0])
        def key_text(value):
            if isinstance(value, list):
                return ",".join(str(item.get("id") if isinstance(item, dict) else item) for item in value)
            return str(value or "")
        for table_name, (key_field, keys) in table_keys.items():
            table_id = config["business_tables"][table_name]
            fields = field_names(config, table_id)
            rows = record_rows(config, table_id)
            expected_keys = {str(key) for key in keys}
            actual_values = [key_text(row["fields"].get(key_field)) for row in rows if key_text(row["fields"].get(key_field))]
            actual_keys = set(actual_values)
            duplicate_values = sorted(value for value in actual_keys if actual_values.count(value) > 1)
            missing = sorted(expected_keys - actual_keys)
            extra = sorted(actual_keys - expected_keys)
            orphan_count = sum(1 for row in rows if not key_text(row["fields"].get(key_field)))
            passed = key_field in fields and not missing and not extra and not duplicate_values and orphan_count == 0
            add(f"feishu_projection_{table_name}", passed, {"expected": len(expected_keys), "actual_key_count": len(actual_keys), "missing": missing, "extra": extra[:50], "duplicate_keys": duplicate_values[:50], "orphan_count": orphan_count, "record_count": len(rows)})
        system_table = config["business_tables"]["00_系统保留"]
        system_fields = field_names(config, system_table)
        system_rows = record_rows(config, system_table)
        add("feishu_projection_00_系统保留", bool(system_fields) and bool(system_rows), {"fields": sorted(system_fields), "record_count": len(system_rows)})
        forbidden_tokens = ("监控录制", "开启关闭", "控制指令", "控制状态", "停止录制", "开启录制", "关闭录制", "录制开关", "监控开关")
        forbidden_by_table = {}
        all_table_fields = {}
        for table_name, table_id in config["business_tables"].items():
            names = sorted(field_names(config, table_id))
            all_table_fields[table_name] = names
            found = sorted(name for name in names if any(token in name for token in forbidden_tokens))
            if found:
                forbidden_by_table[table_name] = found
        add("feishu_no_manual_control_switch", not forbidden_by_table, {"forbidden_control_fields_present": forbidden_by_table, "table_fields": all_table_fields})
    except Exception as exc:  # noqa: BLE001
        add("feishu_projection", False, str(exc))

    try:
        runtime_status = subprocess.run([str(v3.RUNTIME_ROOT / ".venv" / "bin" / "python"), str(v3.RUNTIME_ROOT / "bin" / "runtime_status.py"), "--json"], capture_output=True, text=True, timeout=120, check=False)
        payload = json.loads(runtime_status.stdout)
        required = ["runtime", "runtime_worker", "computer_use", "browser_giant_buyin", "feishu_business_workspace", "douyin_profile_mapping", "live_source_mapping", "buyin_identity_resolution", "asr_chinese_validation", "real_product_scan_gate", "live_monitor_gate", "recording_gate", "feishu_business_write_gate"]
        failed = {key: payload.get(key) for key in required if (payload.get(key) or {}).get("status") != "READY"}
        add("runtime_status_all_required_ready", not failed, failed or {"status": "all required capabilities READY"})
    except Exception as exc:  # noqa: BLE001
        add("runtime_status_all_required_ready", False, str(exc))

    try:
        review_package = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/asr-validation/human-review-package.json")
        with v3.connect() as conn:
            asr_review = conn.execute("SELECT review_id,status,metadata_json FROM review_items WHERE object_type='ASR_VALIDATION' AND object_id='asr-validation:chinese-livestream' AND review_type='HUMAN_GOLD_AND_TIMESTAMP'").fetchone()
        add("asr_human_review_package", review_package.is_file() and bool(asr_review) and asr_review["status"] == "PENDING", {"package_path": str(review_package), "review_id": asr_review["review_id"] if asr_review else None, "status": asr_review["status"] if asr_review else None})
    except Exception as exc:  # noqa: BLE001
        add("asr_human_review_package", False, str(exc))

    try:
        with v3.connect() as conn:
            active_sessions = conn.execute("SELECT monitor_target_id,count(*) AS n FROM live_sessions WHERE status IN ('RECORDING','DETECTED','WAITING_CAPACITY') GROUP BY monitor_target_id HAVING count(*)>1").fetchall()
            jobs = conn.execute("SELECT r.session_id,r.status,r.pid,s.status AS session_status FROM recording_jobs r JOIN live_sessions s ON s.session_id=r.session_id WHERE s.status IN ('RECORDING','DETECTED','WAITING_CAPACITY')").fetchall()
            live_job_issues = []
            for row in jobs:
                if row["status"] == "RUNNING" and (not row["pid"] or not pid_alive(row["pid"])):
                    live_job_issues.append(dict(row))
            evidence = conn.execute("SELECT count(*) FROM scan_runs WHERE evidence_state='COMPLETE'").fetchone()[0]
            observations = conn.execute("SELECT count(*) FROM scan_observations").fetchone()[0]
            transcript_rows = conn.execute("SELECT count(*) FROM transcripts WHERE status='COMPLETE'").fetchone()[0]
            analysis_rows = conn.execute("SELECT count(*) FROM analyses WHERE status='COMPLETE' AND metadata_json LIKE '%semantic_engine%'").fetchone()[0]
            lineage_rows = conn.execute("SELECT count(*) FROM lineage_edges WHERE state='CURRENT'").fetchone()[0]
            verified_bundles = conn.execute("SELECT count(*) FROM evidence_bundles WHERE status='VERIFIED'").fetchone()[0]
            complete_segments = conn.execute("SELECT count(*) FROM recording_segments WHERE status='COMPLETE' AND checksum IS NOT NULL").fetchone()[0]
        add("unique_live_session_and_recording_ownership", not active_sessions and not live_job_issues, {"duplicate_targets": [dict(row) for row in active_sessions], "job_issues": live_job_issues})
        add("scan_evidence_persisted", evidence > 0 and observations > 0, {"complete_scan_runs": evidence, "observations": observations})
        add("semantic_pipeline_evidence", transcript_rows > 0 and analysis_rows > 0 and lineage_rows >= analysis_rows and verified_bundles >= analysis_rows, {"transcripts": transcript_rows, "analyses": analysis_rows, "lineage_edges": lineage_rows, "verified_evidence_bundles": verified_bundles})
        add("media_segment_integrity", complete_segments > 0, {"complete_checksum_segments": complete_segments})
    except Exception as exc:  # noqa: BLE001
        add("unique_live_session_and_recording_ownership", False, str(exc))

    old_plist = Path("/Users/mac/Library/LaunchAgents/ai.hermes.runtime-edu_live_competitor_intel.plist")
    add("legacy_runtime_launchagent_retired", not old_plist.exists(), {"old_plist_exists": old_plist.exists()})
    worker_source = (v3.RUNTIME_ROOT / "v3" / "v3_worker.py").read_text(encoding="utf-8")
    old_dependency = "mvp_c_runner.py" in worker_source
    add("v3_does_not_use_legacy_runner_for_delivery", not old_dependency, {"legacy_runner_dependency": old_dependency})

    postgres = subprocess.run(["/bin/sh", "-lc", "command -v psql >/dev/null 2>&1"], capture_output=True, check=False).returncode == 0
    backend = str(config.get("control_plane_backend") or "sqlite_wal")
    pg_connection = False
    pg_database = None
    try:
        with v3.connect() as conn:
            row = conn.execute("SELECT current_database() AS database_name").fetchone()
            pg_database = row["database_name"]
            pg_connection = bool(pg_database)
    except Exception:
        pg_connection = False
    add("final_control_plane_infrastructure", postgres and backend == "postgresql" and pg_connection, {"postgresql_client_available": postgres, "configured_backend": backend, "pg_connection": pg_connection, "pg_database": pg_database, "sqlite_mirror": str(v3.DB_PATH)})

    keepawake_plist = Path("/Users/mac/Library/LaunchAgents/ai.hermes.keepawake-edu_live_competitor_intel.plist")
    keepawake_proc = subprocess.run(["/usr/bin/pgrep", "-af", r"caffeinate.*-dimsu"], capture_output=True, text=True, check=False).stdout.strip()
    add("single_device_keepawake", keepawake_plist.is_file() and bool(keepawake_proc), {"plist": str(keepawake_plist), "loaded_process": bool(keepawake_proc)})

    try:
        process_lines = recorder_process_lines()
        fixed_segment_processes = positive_segment_processes(process_lines)
        unexpected_processes = [line.split()[0] for line in process_lines if ("/runtime/bin/recorder.py" in line or "record_douyin_live.py" in line) and ("--duration 0" not in line or "--quality LD" not in line or ("record_douyin_live.py" in line and not re.search(r"--segment-seconds\s+0(?:\.0+)?(?:\s|$)", line)))]
        with v3.connect() as conn:
            running_jobs = [dict(row) for row in conn.execute("SELECT session_id,pid,recording_key,status FROM recording_jobs WHERE status='RUNNING'")]
        missing_configuration = []
        for job in running_jobs:
            key = str(job.get("recording_key") or "")
            related = [line for line in process_lines if key and key in line]
            wrapper = [line for line in related if "/runtime/bin/recorder.py" in line]
            child = [line for line in related if "record_douyin_live.py" in line]
            if not wrapper or not any("--duration 0" in line and "--quality LD" in line for line in wrapper):
                missing_configuration.append({"session_id": job["session_id"], "part": "profile_recorder", "pid": job.get("pid")})
            if not child or not any("--duration 0" in line and "--quality LD" in line and re.search(r"--segment-seconds\s+0(?:\.0+)?(?:\s|$)", line) for line in child):
                missing_configuration.append({"session_id": job["session_id"], "part": "shared_recorder", "pid": job.get("pid")})
        add("active_recording_configuration", not fixed_segment_processes and not unexpected_processes and not missing_configuration, {"running_jobs": len(running_jobs), "fixed_segment_processes": len(fixed_segment_processes), "unexpected_recorder_processes": unexpected_processes, "misconfigured_jobs": missing_configuration})
    except Exception as exc:  # noqa: BLE001
        add("active_recording_configuration", False, str(exc))

    try:
        with v3.connect() as conn:
            heartbeats = {row["service_name"]: dict(row) for row in conn.execute("SELECT * FROM heartbeats")}
            node_rows = [dict(row) for row in conn.execute("SELECT * FROM worker_nodes")]
            open_conflicts = conn.execute("SELECT count(*) FROM identity_conflicts WHERE status='OPEN'").fetchone()[0]
            active_leases = conn.execute("SELECT count(*) FROM recording_leases WHERE status='ACTIVE'").fetchone()[0]
            active_jobs = conn.execute("SELECT count(*) FROM recording_jobs WHERE status='RUNNING'").fetchone()[0]
            active_task_leases = conn.execute("SELECT count(*) FROM task_leases WHERE status='ACTIVE'").fetchone()[0]
            now_text = v3.utc_now()
            active_task_lease_rows = [dict(row) for row in conn.execute("SELECT lease_id,task_id,worker_id,lease_until FROM task_leases WHERE status='ACTIVE'")]
            stale_task_leases = [row for row in active_task_lease_rows if (row.get("lease_until") and row["lease_until"] < now_text) or not v3.worker_process_alive(row.get("worker_id"))]
            orphan_running_tasks = [dict(row) for row in conn.execute("SELECT task_id,status,lease_owner,lease_until FROM tasks WHERE status='RUNNING' AND (lease_owner IS NULL OR lease_until IS NULL)")]
            capacity_passes = conn.execute("SELECT count(*) FROM capacity_test_runs WHERE status='PASS' AND target_concurrency>=? AND metrics_json LIKE '%\"production_equivalent\": true%'", (int(config.get("capacity_test_concurrency") or 65),)).fetchone()[0]
            smoke_rows = conn.execute("SELECT target_concurrency,status,metrics_json,evidence_path FROM capacity_test_runs WHERE status='PASS' AND target_concurrency>=? ORDER BY ended_at DESC", (int(config.get("capacity_test_concurrency") or 65),)).fetchall()
            fault_passes = conn.execute("SELECT count(*) FROM fault_drill_runs WHERE status='PASS'").fetchone()[0]
        required_services = ["runtime-v3", "pipeline-v3", "recompute-v3", "retention-v3", "analysis-v3", "evidence-v3"]
        now_dt = v3.parse_time(v3.utc_now())
        heartbeat_issues = []
        for name in required_services:
            row = heartbeats.get(name)
            if not row or row.get("status") not in {"READY", "STARTING"}:
                heartbeat_issues.append({"service": name, "issue": "missing_or_not_ready"})
                continue
            age = (now_dt - v3.parse_time(row.get("last_heartbeat_at"))).total_seconds() if now_dt and v3.parse_time(row.get("last_heartbeat_at")) else None
            if not pid_alive(row.get("pid")):
                heartbeat_issues.append({"service": name, "issue": "pid_not_alive", "pid": row.get("pid")})
            elif age is None or age > 180:
                heartbeat_issues.append({"service": name, "issue": "heartbeat_stale", "age_seconds": age})
        add("worker_heartbeats", not heartbeat_issues, {"required": required_services, "issues": heartbeat_issues, "heartbeats": heartbeats})
        add("identity_conflict_closure", open_conflicts == 0, {"open_identity_conflicts": open_conflicts})
        add("recording_lease_plane", active_leases == active_jobs, {"active_recording_leases": active_leases, "active_recording_jobs": active_jobs, "active_task_leases": active_task_leases})
        add("task_lease_reconciliation", not stale_task_leases and not orphan_running_tasks, {"stale_task_leases": stale_task_leases, "orphan_running_tasks": orphan_running_tasks})
        single_node_ready = bool(config.get("single_node_mode") is True and config.get("single_node_risk_acknowledged") is True and int((config.get("atomic_activation") or {}).get("max_concurrent_recordings") or 0) >= int(config.get("capacity_test_concurrency") or 65))
        add("single_node_capacity_gate", single_node_ready and capacity_passes > 0 and fault_passes > 0, {"single_node_mode": config.get("single_node_mode"), "risk_acknowledged": config.get("single_node_risk_acknowledged"), "worker_nodes": node_rows, "capacity_passes": capacity_passes, "required_capacity": config.get("capacity_test_concurrency"), "fault_drill_passes": fault_passes})
        smoke_evidence = []
        for row in smoke_rows:
            try:
                metrics = json.loads(row["metrics_json"] or "{}")
            except json.JSONDecodeError:
                metrics = {}
            streams = ((metrics.get("sample_probe") or {}).get("streams") or [])
            has_low_video = any(int(stream.get("width") or 0) <= 720 and int(stream.get("height") or 0) <= 1280 for stream in streams if stream.get("codec_name") == "h264")
            has_audio = any(stream.get("codec_name") == "aac" for stream in streams)
            smoke_evidence.append({"target": row["target_concurrency"], "source_mode": metrics.get("source_mode"), "production_equivalent": metrics.get("production_equivalent"), "max_parallel_processes": metrics.get("max_parallel_processes"), "completed_outputs": metrics.get("completed_outputs"), "failed_processes": metrics.get("failed_processes"), "has_low_video": has_low_video, "has_audio": has_audio, "evidence_path": row["evidence_path"]})
        capacity_smoke_ready = any(item["source_mode"] == "local_ld_fixture_stream_copy" and item["production_equivalent"] is False and int(item["max_parallel_processes"] or 0) >= int(config.get("capacity_test_concurrency") or 65) and int(item["completed_outputs"] or 0) >= int(config.get("capacity_test_concurrency") or 65) and int(item["failed_processes"] or 0) == 0 and item["has_low_video"] and item["has_audio"] for item in smoke_evidence)
        add("single_device_recording_path_smoke", single_node_ready and capacity_smoke_ready, {"validated": capacity_smoke_ready, "required_capacity": config.get("capacity_test_concurrency"), "evidence": smoke_evidence, "note": "本机录制链路已验证；不替代不同远端直播源和网络容量证明"})
    except Exception as exc:  # noqa: BLE001
        add("worker_heartbeats", False, str(exc))

    add("tenant_identity_lock", bool(config.get("expected_tenant_key") and config.get("expected_tenant_name") and config.get("tenant_verification") == "PASS"), {"expected_tenant_key_configured": bool(config.get("expected_tenant_key")), "expected_tenant_name_configured": bool(config.get("expected_tenant_name")), "tenant_verification": config.get("tenant_verification")})
    add("final_production_release_gate", config.get("production_gate") == "READY" and config.get("deployment_state") == "ACTIVE", {"production_gate": config.get("production_gate"), "deployment_state": config.get("deployment_state"), "atomic_activation_state": (config.get("atomic_activation") or {}).get("activation_state")})
    retention = config.get("retention") or {}
    final_media_policy_ok = (
        int(retention.get("video_hours") or 0) == 72
        and int(retention.get("audio_hours") or 0) == 168
        and bool((runtime_config.get("retention") or {}).get("automatic_delete"))
        and int((runtime_config.get("retention") or {}).get("video_hours") or 0) == 72
        and int((runtime_config.get("retention") or {}).get("audio_hours") or 0) == 168
        and int(config.get("recording_segment_seconds") or 0) == 0
        and int(config.get("recording_duration_seconds") or 0) == 0
        and config.get("recording_quality") == "LD"
        and config.get("recording_mode") == "LOWEST_VIDEO_WITH_AUDIO"
        and config.get("recording_single_final_file") is True
        and str(config.get("asr_audio_format") or "").lower() == "opus"
        and int(config.get("asr_audio_bitrate_kbps") or 0) == 48
        and int(config.get("normal_recording_concurrency") or 0) == 30
        and int(config.get("expected_max_recording_concurrency") or 0) == 50
        and int(config.get("capacity_test_concurrency") or 0) >= 65
    )
    add("final_media_retention_policy", final_media_policy_ok, {"v3_video_hours": retention.get("video_hours"), "v3_audio_hours": retention.get("audio_hours"), "runtime_automatic_delete": (runtime_config.get("retention") or {}).get("automatic_delete"), "runtime_video_hours": (runtime_config.get("retention") or {}).get("video_hours"), "runtime_audio_hours": (runtime_config.get("retention") or {}).get("audio_hours"), "recording_segment_seconds": config.get("recording_segment_seconds"), "recording_duration_seconds": config.get("recording_duration_seconds"), "recording_quality": config.get("recording_quality"), "recording_mode": config.get("recording_mode"), "recording_single_final_file": config.get("recording_single_final_file"), "asr_audio_format": config.get("asr_audio_format"), "asr_audio_bitrate_kbps": config.get("asr_audio_bitrate_kbps"), "normal_recording_concurrency": config.get("normal_recording_concurrency"), "expected_max_recording_concurrency": config.get("expected_max_recording_concurrency"), "capacity_test_concurrency": config.get("capacity_test_concurrency")})

    source = (v3.RUNTIME_ROOT / "bin" / "mvp_c_runner.py").read_text(encoding="utf-8")
    archive = v3.RUNTIME_ROOT / "backups" / "retired-mvp-c-final-20260825" / "mvp_c_runner.py"
    skill = (v3.PROFILE_ROOT / "skills" / "live-competitor-runtime" / "SKILL.md").read_text(encoding="utf-8")
    add("legacy_entry_disabled", "LEGACY_EXECUTION_DISABLED" in source and archive.is_file() and "mvp_c_runner.py --feishu-trigger" in skill and "retired MVP-C" in skill, {"stub_refuses_execution": "LEGACY_EXECUTION_DISABLED" in source, "read_only_archive_exists": archive.is_file(), "archive_mode": oct(archive.stat().st_mode & 0o777) if archive.is_file() else None})
    gateway_source = Path("/Users/mac/AI/apps/hermes/source/gateway/platforms/feishu.py").read_text(encoding="utf-8")
    dedup_guard = "self._runtime_v3_db_dedup" in gateway_source and "return False" in gateway_source[gateway_source.find("def _is_duplicate"):gateway_source.find("def _is_duplicate") + 500]
    add("database_message_dedup_only", dedup_guard, {"profile_db_dedup_guard": dedup_guard, "json_path_retained_for_other_profiles": str(v3.PROFILE_ROOT / "feishu_seen_message_ids.json")})
    log_root = v3.PROFILE_ROOT / "logs"
    suspicious = []
    for log_path in log_root.glob("gateway*"):
        try:
            body = log_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for marker in ("access_key=", "ticket="):
            if marker in body.lower() and f"{marker}[redacted]" not in body.lower():
                suspicious.append(str(log_path))
    add("secret_redaction_logs", not suspicious, {"suspicious_logs": suspicious})
    active_error_logs = []
    for name in ("runtime-v3.error.log", "pipeline-v3.error.log", "retention-v3.error.log", "analysis-v3.error.log", "evidence-v3.error.log"):
        log_path = log_root / name
        try:
            if log_path.is_file() and log_path.stat().st_size > 0:
                active_error_logs.append({"path": str(log_path), "bytes": log_path.stat().st_size})
        except OSError as exc:
            active_error_logs.append({"path": str(log_path), "error": exc.__class__.__name__})
    add("active_worker_error_logs_clean", not active_error_logs, {"active_error_logs": active_error_logs, "archived_previous_errors": str(log_root / "archive-schema-lock-fix-20260826")})

    result = {"status": "PASS" if all(item["status"] == "PASS" for item in checks) else "FAIL", "checked_at": v3.utc_now(), "checks": checks, "activation": v3.activation_readiness(), "truthfulness": "PASS means all checked Runtime V3 invariants and Feishu stable-key projections agree; external human/network gates are never inferred."}
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
