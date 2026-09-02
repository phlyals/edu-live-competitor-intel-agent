#!/usr/bin/env python3
"""Fail-closed recording segment lifecycle migration.

Default mode is dry-run.  --apply requires an explicit expected database name
and backup directory.  No row is deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MIGRATION_KEY = "recording_segment_lifecycle_v1"
VALID = {"UNCLASSIFIED", "CANONICAL_ACTIVE", "SOURCE_RETAINED", "SOURCE_SUPERSEDED", "LOST_REVIEW"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_json(value: Any) -> dict | None:
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


@dataclass(frozen=True)
class DesiredSegment:
    segment_id: str
    path: str
    lifecycle_status: str
    superseded_by_segment_id: str | None
    reason: str


def verified_manifest(row: dict, *, full_media_hash: bool = False) -> tuple[dict | None, str | None]:
    if not row or row.get("status") != "VERIFIED":
        return None, "database manifest is not VERIFIED"
    manifest_path = Path(str(row.get("manifest_path") or ""))
    if not manifest_path.is_file():
        return None, "manifest file is missing"
    if not row.get("manifest_hash") or file_hash(manifest_path) != str(row["manifest_hash"]):
        return None, "manifest file hash mismatch"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "manifest JSON is invalid"
    if not isinstance(manifest, dict) or str(manifest.get("session_id")) != str(row.get("session_id")):
        return None, "manifest session mismatch"
    final_path = Path(str(manifest.get("final_path") or ""))
    if not final_path.is_file():
        return None, "canonical final file is missing"
    expected = str(manifest.get("sha256") or "")
    if not expected:
        return None, "canonical final hash is missing"
    if manifest.get("bytes") is not None and int(manifest["bytes"]) != final_path.stat().st_size:
        return None, "canonical final byte count mismatch"
    if full_media_hash and file_hash(final_path) != expected:
        return None, "canonical final hash mismatch"
    return manifest, None


def source_references(manifest: dict) -> dict[str, dict]:
    refs: dict[str, dict] = {}
    for entry in manifest.get("retained_sources") or []:
        if not isinstance(entry, dict):
            continue
        info = {
            "checksum": str(entry.get("sha256") or "") or None,
            "current_path": str(entry.get("path") or ""),
        }
        for key in ("path", "original_path"):
            if entry.get(key):
                refs[str(entry[key])] = info
    return refs


def _path_in_recording_roots(path: str, roots: list[str]) -> bool:
    try:
        candidate = Path(path).resolve(strict=False)
        return any(candidate.is_relative_to(Path(root).resolve(strict=False)) for root in roots if root)
    except (OSError, ValueError):
        return False


def plan_segments(segments: list[dict], manifests: list[dict], jobs: list[dict] | None = None,
                  *, full_media_hash: bool = False) -> tuple[list[DesiredSegment], list[dict]]:
    manifest_by_session = {str(row["session_id"]): row for row in manifests}
    validated: dict[str, tuple[dict, str]] = {}
    issues: list[dict] = []
    for session_id, row in manifest_by_session.items():
        manifest, error = verified_manifest(row, full_media_hash=full_media_hash)
        if error:
            issues.append({"session_id": session_id, "reason": error})
        else:
            validated[session_id] = (manifest, str(manifest["final_path"]))

    canonical_ids: dict[str, str] = {}
    for row in segments:
        session_id = str(row["session_id"])
        valid = validated.get(session_id)
        if not valid or str(row["path"]) != valid[1]:
            continue
        expected = str(valid[0].get("sha256") or "")
        if row.get("checksum") and str(row["checksum"]) != expected:
            issues.append({"session_id": session_id, "segment_id": row["segment_id"], "reason": "canonical row checksum mismatch"})
            continue
        if row.get("bytes") is not None and int(row["bytes"]) != Path(valid[1]).stat().st_size:
            issues.append({"session_id": session_id, "segment_id": row["segment_id"], "reason": "canonical row byte count mismatch"})
            continue
        if session_id in canonical_ids:
            issues.append({"session_id": session_id, "segment_id": row["segment_id"], "reason": "multiple canonical rows"})
            canonical_ids.pop(session_id, None)
            continue
        canonical_ids[session_id] = str(row["segment_id"])

    desired: list[DesiredSegment] = []
    roots_by_session = {
        str(row["session_id"]): [str(row.get("partial_dir") or ""), str(row.get("completed_dir") or "")]
        for row in (jobs or [])
    }
    path_owners = {(str(row["session_id"]), str(row["path"])): row for row in segments}
    for row in segments:
        segment_id, session_id, raw_path = str(row["segment_id"]), str(row["session_id"]), str(row["path"])
        canonical_id = canonical_ids.get(session_id)
        if canonical_id == segment_id:
            desired.append(DesiredSegment(segment_id, raw_path, "CANONICAL_ACTIVE", None, "verified manifest and canonical file hash"))
            continue
        path = Path(raw_path)
        if path.is_file():
            if row.get("bytes") is not None and int(row["bytes"]) != path.stat().st_size:
                desired.append(DesiredSegment(segment_id, raw_path, "LOST_REVIEW", None, "existing source byte count mismatch"))
            elif full_media_hash and row.get("checksum") and str(row["checksum"]) != file_hash(path):
                desired.append(DesiredSegment(segment_id, raw_path, "LOST_REVIEW", None, "existing source checksum mismatch"))
            else:
                desired.append(DesiredSegment(segment_id, raw_path, "SOURCE_RETAINED", None, "source file still exists"))
            continue
        refs = source_references(validated[session_id][0]) if canonical_id and session_id in validated else {}
        reference = refs.get(raw_path)
        reference_hash = (reference or {}).get("checksum")
        current_path = str((reference or {}).get("current_path") or "")
        row_hash = str(row.get("checksum") or "") or None
        compatible = not reference_hash or not row_hash or reference_hash == row_hash
        if canonical_id and reference and current_path and Path(current_path).is_file() and compatible:
            owner = path_owners.get((session_id, current_path))
            if not owner or str(owner["segment_id"]) == segment_id:
                desired.append(DesiredSegment(segment_id, current_path, "SOURCE_RETAINED", None, "source moved to retained completed path"))
            else:
                owner_hash = str(owner.get("checksum") or "") or None
                if not reference_hash or not owner_hash or reference_hash == owner_hash:
                    desired.append(DesiredSegment(segment_id, raw_path, "SOURCE_SUPERSEDED", canonical_id, "equivalent retained path already has an owner"))
                else:
                    desired.append(DesiredSegment(segment_id, raw_path, "LOST_REVIEW", None, "retained destination path conflicts"))
        elif canonical_id and (_path_in_recording_roots(raw_path, roots_by_session.get(session_id, [])) or raw_path in refs):
            # A verified canonical proves which media is now authoritative.  It
            # does not prove full capture, but it is sufficient to retire a
            # missing non-canonical source from pipeline eligibility.
            desired.append(DesiredSegment(segment_id, raw_path, "SOURCE_SUPERSEDED", canonical_id, "missing non-canonical source retired by verified canonical media"))
        else:
            desired.append(DesiredSegment(segment_id, raw_path, "LOST_REVIEW", None, "missing source lacks verified canonical provenance"))
    return desired, issues


def plan_transcripts(transcripts: list[dict], desired: list[DesiredSegment], migrated_at: str) -> tuple[list[dict], list[dict]]:
    superseded = {item.segment_id: item for item in desired if item.lifecycle_status == "SOURCE_SUPERSEDED"}
    actions, issues = [], []
    for row in transcripts:
        if str(row.get("status")) != "WAITING_TOOL" or Path(str(row.get("source_path") or "")).is_file():
            continue
        metadata = safe_json(row.get("metadata_json"))
        if metadata is None:
            issues.append({"transcript_id": row["transcript_id"], "reason": "metadata JSON invalid; not cancelled"})
            continue
        segment_id = str(metadata.get("segment_id") or metadata.get("source_segment_id") or "")
        source = superseded.get(segment_id)
        if not source:
            continue
        if str(row.get("source_path") or "") != source.path:
            issues.append({"transcript_id": row["transcript_id"], "reason": "source path does not match superseded segment"})
            continue
        if row.get("output_path"):
            issues.append({"transcript_id": row["transcript_id"], "reason": "output exists; not cancelled"})
            continue
        if metadata.get("reason") != "audio extraction/duration validation failed":
            issues.append({"transcript_id": row["transcript_id"], "reason": "failure reason does not match stale extraction task"})
            continue
        updated = dict(metadata)
        updated["segment_lifecycle_migration"] = {
            "reason": "source segment superseded by verified canonical media",
            "source_segment_id": segment_id,
            "superseded_by_segment_id": source.superseded_by_segment_id,
            "migrated_at": migrated_at,
        }
        actions.append({
            "transcript_id": str(row["transcript_id"]),
            "status": "CANCELLED_SUPERSEDED_SOURCE",
            "metadata_json": json.dumps(updated, ensure_ascii=False, sort_keys=True),
        })
    return actions, issues


def table_columns(conn, table: str) -> set[str]:
    rows = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (table,)).fetchall()
    return {str(row[0]) for row in rows}


def inventory(conn) -> tuple[list[dict], list[dict], list[dict], list[dict], set[str]]:
    from psycopg.rows import dict_row
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT segment_id,session_id,path,checksum,status,bytes,lifecycle_status,superseded_by_segment_id,lifecycle_updated_at FROM recording_segments" if "lifecycle_status" in table_columns(conn, "recording_segments") else "SELECT segment_id,session_id,path,checksum,status,bytes FROM recording_segments")
        segments = list(cur.fetchall())
        cur.execute("SELECT session_id,status,manifest_path,manifest_hash FROM media_manifests")
        manifests = list(cur.fetchall())
        cur.execute("SELECT transcript_id,status,source_path,output_path,metadata_json FROM transcripts WHERE status='WAITING_TOOL'")
        transcripts = list(cur.fetchall())
        cur.execute("SELECT session_id,partial_dir,completed_dir FROM recording_jobs")
        jobs = list(cur.fetchall())
    return segments, manifests, transcripts, jobs, table_columns(conn, "recording_segments")


def summary(desired: list[DesiredSegment], transcript_actions: list[dict], issues: list[dict]) -> dict:
    counts = {name: 0 for name in sorted(VALID)}
    for item in desired:
        counts[item.lifecycle_status] += 1
    return {"segment_counts": counts, "transcripts_to_cancel": len(transcript_actions), "issues": issues}


def plan_digest(desired: list[DesiredSegment], transcript_actions: list[dict], issues: list[dict]) -> str:
    stable = {
        "segments": [asdict(item) for item in sorted(desired, key=lambda item: item.segment_id)],
        "transcripts": sorted(
            ({"transcript_id": item["transcript_id"], "status": item["status"]} for item in transcript_actions),
            key=lambda item: item["transcript_id"],
        ),
        "issues": sorted(issues, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True)),
    }
    return hashlib.sha256(json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="default is read-only dry-run")
    parser.add_argument("--dsn-env", default="SEGMENT_MIGRATION_DSN")
    parser.add_argument("--expected-database")
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--full-media-hash", action="store_true")
    args = parser.parse_args()
    if args.apply and (not args.expected_database or not args.backup_dir or not args.expected_plan_sha256):
        parser.error("--apply requires --expected-database, --backup-dir and --expected-plan-sha256")
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        parser.error(f"DSN environment variable {args.dsn_env} is empty")

    import psycopg
    conn = psycopg.connect(dsn, autocommit=False)
    if not args.apply:
        conn.read_only = True
    try:
        database = conn.execute("SELECT current_database()").fetchone()[0]
        if args.apply and database != args.expected_database:
            raise RuntimeError(f"database mismatch: expected {args.expected_database!r}, got {database!r}")
        if args.apply:
            applied = conn.execute("SELECT value FROM schema_meta WHERE key=%s", (MIGRATION_KEY,)).fetchone()
            if applied and str(applied[0]) == "1":
                report = {"mode": "APPLY", "database": database, "status": "ALREADY_APPLIED",
                          "migration_key": MIGRATION_KEY}
                conn.rollback()
                if args.report:
                    args.report.parent.mkdir(parents=True, exist_ok=True)
                    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 0
        segments, manifests, transcripts, jobs, columns = inventory(conn)
        migrated_at = utc_now()
        desired, issues = plan_segments(segments, manifests, jobs, full_media_hash=args.full_media_hash)
        transcript_actions, transcript_issues = plan_transcripts(transcripts, desired, migrated_at)
        all_issues = issues + transcript_issues
        digest = plan_digest(desired, transcript_actions, all_issues)
        report = {"mode": "APPLY" if args.apply else "DRY_RUN", "database": database,
                  "transaction_read_only": not args.apply, "plan_sha256": digest,
                  "full_media_hash": args.full_media_hash,
                  **summary(desired, transcript_actions, all_issues)}
        if args.apply:
            if digest != args.expected_plan_sha256:
                raise RuntimeError(f"migration plan changed: expected {args.expected_plan_sha256}, got {digest}")
            args.backup_dir.mkdir(parents=True, exist_ok=True)
            backup_path = args.backup_dir / f"segment-lifecycle-before-{migrated_at.replace(':','')}.json"
            backup_path.write_text(json.dumps({"segments": segments, "transcripts": transcripts}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            os.chmod(backup_path, 0o600)
            ddl = Path(__file__).resolve().parents[1] / "sql" / "001_recording_segment_lifecycle.sql"
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (MIGRATION_KEY,))
            conn.execute("SET LOCAL lock_timeout='5s'")
            conn.execute("SET LOCAL statement_timeout='15min'")
            conn.execute(ddl.read_text(encoding="utf-8"))
            for item in desired:
                conn.execute("UPDATE recording_segments SET path=%s,lifecycle_status=%s,superseded_by_segment_id=%s,lifecycle_updated_at=%s WHERE segment_id=%s AND (path,lifecycle_status,superseded_by_segment_id) IS DISTINCT FROM (%s,%s,%s)", (item.path, item.lifecycle_status, item.superseded_by_segment_id, migrated_at, item.segment_id, item.path, item.lifecycle_status, item.superseded_by_segment_id))
            for item in transcript_actions:
                conn.execute("UPDATE transcripts SET status=%s,metadata_json=%s WHERE transcript_id=%s AND status='WAITING_TOOL'", (item["status"], item["metadata_json"], item["transcript_id"]))
            conn.execute("INSERT INTO schema_meta(key,value) VALUES(%s,%s) ON CONFLICT(key) DO NOTHING", (MIGRATION_KEY, "1"))
            conn.commit()
            report["backup_path"] = str(backup_path)
        else:
            conn.rollback()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
