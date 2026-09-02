#!/usr/bin/env python3
"""Durable claim, fencing and retry primitives for long pipeline jobs.

The functions in this module deliberately take an open connection.  A claim is
one PostgreSQL statement using ``FOR UPDATE SKIP LOCKED``; all later mutations
are compare-and-swap operations over ``(job_id, lease_owner, lease_epoch)``.
Consequently, a timed-out worker may finish local work, but it cannot publish a
database result after a newer worker has reclaimed the job.
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


TRANSCRIPT = "transcript"
ANALYSIS = "analysis"
RECOMPUTE = "recompute"


@dataclass(frozen=True)
class JobSpec:
    table: str
    id_column: str
    due_statuses: tuple[str, ...]


SPECS = {
    TRANSCRIPT: JobSpec(
        table="transcripts",
        id_column="transcript_id",
        due_statuses=("PENDING", "WAITING_TOOL", "PAUSED", "RETRY_WAIT"),
    ),
    ANALYSIS: JobSpec(
        table="analyses",
        id_column="analysis_id",
        due_statuses=("PENDING", "PENDING_RECOMPUTE", "WAITING_MODEL", "RETRY_WAIT"),
    ),
    RECOMPUTE: JobSpec(
        table="recompute_requests",
        id_column="request_id",
        due_statuses=("PENDING", "RETRY_WAIT"),
    ),
}


class LeaseLostError(RuntimeError):
    """Raised when a stale worker tries to mutate a reclaimed job."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def after_seconds(seconds: float, *, now: str | None = None) -> str:
    base = datetime.fromisoformat((now or utc_now()).replace("Z", "+00:00"))
    return (base + timedelta(seconds=max(0.0, float(seconds)))).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_checkpoint(value: Any) -> dict:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _spec(kind: str) -> JobSpec:
    try:
        return SPECS[kind]
    except KeyError:
        raise ValueError(f"unsupported durable job kind: {kind}") from None


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def claim_next(
    conn: Any,
    kind: str,
    worker_id: str,
    *,
    now: str | None = None,
    lease_seconds: int = 600,
    where_sql: str = "",
    where_params: Iterable[Any] = (),
) -> dict | None:
    """Atomically claim one due or expired job and return its fenced row."""
    spec = _spec(kind)
    now = now or utc_now()
    lease_until = after_seconds(lease_seconds, now=now)
    status_marks = ",".join("?" for _ in spec.due_statuses)
    extra = f" AND ({where_sql})" if where_sql.strip() else ""
    eligibility = (
        f"(((status IN ({status_marks})) AND (next_attempt_at IS NULL OR next_attempt_at<=?)) "
        f"OR (status='RUNNING' AND (lease_until IS NULL OR lease_until<=?))) "
        f"AND attempts<max_attempts{extra}"
    )
    eligibility_params = (*spec.due_statuses, now, now, *tuple(where_params))

    if _is_sqlite(conn):
        conn.execute("BEGIN IMMEDIATE")
        candidate = conn.execute(
            f"SELECT {spec.id_column} FROM {spec.table} WHERE {eligibility} "
            f"ORDER BY COALESCE(next_attempt_at,''),{spec.id_column} LIMIT 1",
            eligibility_params,
        ).fetchone()
        if not candidate:
            conn.commit()
            return None
        job_id = candidate[spec.id_column] if hasattr(candidate, "keys") else candidate[0]
        updated = conn.execute(
            f"UPDATE {spec.table} SET status='RUNNING',lease_owner=?,lease_until=?,"
            "lease_epoch=lease_epoch+1,attempts=attempts+1,last_attempt_at=?,updated_at=?,"
            f"last_error_type=NULL,last_error=NULL WHERE {spec.id_column}=? AND {eligibility}",
            (worker_id, lease_until, now, now, job_id, *eligibility_params),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return None
        row = conn.execute(f"SELECT * FROM {spec.table} WHERE {spec.id_column}=?", (job_id,)).fetchone()
        conn.commit()
        return dict(row)

    # PostgreSQL performs selection and mutation in one statement.  SKIP
    # LOCKED means concurrent workers never wait on or duplicate the same job.
    row = conn.execute(
        f"WITH candidate AS (SELECT {spec.id_column} FROM {spec.table} "
        f"WHERE {eligibility} ORDER BY COALESCE(next_attempt_at,''),{spec.id_column} "
        f"FOR UPDATE SKIP LOCKED LIMIT 1) "
        f"UPDATE {spec.table} AS target SET status='RUNNING',lease_owner=?,lease_until=?,"
        "lease_epoch=target.lease_epoch+1,attempts=target.attempts+1,last_attempt_at=?,updated_at=?,"
        "last_error_type=NULL,last_error=NULL FROM candidate "
        f"WHERE target.{spec.id_column}=candidate.{spec.id_column} RETURNING target.*",
        (*eligibility_params, worker_id, lease_until, now, now),
    ).fetchone()
    conn.commit()
    return dict(row) if row else None


def reconcile_exhausted(conn: Any, kind: str, *, now: str | None = None) -> int:
    """Fail an expired RUNNING job that no longer has an allowed attempt."""
    spec = _spec(kind)
    now = now or utc_now()
    cursor = conn.execute(
        f"UPDATE {spec.table} SET status='FAILED_FINAL',lease_owner=NULL,lease_until=NULL,"
        "next_attempt_at=NULL,last_error_type='LEASE_EXPIRED_ATTEMPTS_EXHAUSTED',"
        "last_error='worker lease expired after the final permitted attempt',updated_at=? "
        "WHERE status='RUNNING' AND (lease_until IS NULL OR lease_until<=?) AND attempts>=max_attempts",
        (now, now),
    )
    conn.commit()
    return max(0, int(cursor.rowcount or 0))


def _fenced_update(
    conn: Any,
    kind: str,
    job: dict,
    assignments: dict[str, Any],
    *,
    ownership_now: str | None = None,
    commit_transaction: bool = True,
) -> None:
    spec = _spec(kind)
    allowed = {
        "status", "lease_owner", "lease_until", "next_attempt_at", "last_error_type",
        "last_error", "checkpoint_json", "updated_at", "output_path", "metadata_json",
        "lineage_state", "language", "low_confidence_count", "source_path",
        "scope", "qualification_status", "artifact_digest",
        "candidate_analysis_id", "completed_at",
    }
    unknown = set(assignments) - allowed
    if unknown:
        raise ValueError(f"unsafe durable job columns: {sorted(unknown)}")
    clauses = [f"{name}=?" for name in assignments]
    params = list(assignments.values())
    ownership_now = ownership_now or str(assignments.get("updated_at") or utc_now())
    params.extend([
        job[spec.id_column], job["lease_owner"], int(job["lease_epoch"]),
        ownership_now,
    ])
    identity_sql = ""
    if kind == ANALYSIS:
        for column in (
            "transcript_id", "transcript_content_digest", "analysis_spec_version",
            "model_version", "prompt_version", "scope", "qualification_status",
        ):
            if column in job and job.get(column) is not None:
                identity_sql += f" AND {column}=?"
                params.append(job[column])
    cursor = conn.execute(
        f"UPDATE {spec.table} SET {','.join(clauses)} WHERE {spec.id_column}=? "
        "AND status='RUNNING' AND lease_owner=? AND lease_epoch=? "
        f"AND lease_until IS NOT NULL AND lease_until>?{identity_sql}",
        params,
    )
    if cursor.rowcount != 1:
        conn.rollback()
        raise LeaseLostError(
            f"{kind} lease lost: {job[spec.id_column]} owner={job['lease_owner']} epoch={job['lease_epoch']}"
        )
    if commit_transaction:
        conn.commit()


def renew(conn: Any, kind: str, job: dict, *, lease_seconds: int = 600, now: str | None = None) -> str:
    now = now or utc_now()
    lease_until = after_seconds(lease_seconds, now=now)
    _fenced_update(
        conn, kind, job, {"lease_until": lease_until, "updated_at": now},
        ownership_now=now,
    )
    job["lease_until"] = lease_until
    return lease_until


def save_checkpoint(conn: Any, kind: str, job: dict, checkpoint: dict, *, now: str | None = None) -> None:
    now = now or utc_now()
    serialized = json.dumps(checkpoint, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    _fenced_update(
        conn, kind, job, {"checkpoint_json": serialized, "updated_at": now},
        ownership_now=now,
    )
    job["checkpoint_json"] = serialized


def finish(
    conn: Any,
    kind: str,
    job: dict,
    status: str,
    assignments: dict[str, Any],
    *,
    now: str | None = None,
    commit_transaction: bool = True,
) -> None:
    now = now or utc_now()
    values = dict(assignments)
    values.update({
        "status": status,
        "lease_owner": None,
        "lease_until": None,
        "next_attempt_at": None,
        "last_error_type": None,
        "last_error": None,
        "updated_at": now,
    })
    _fenced_update(
        conn, kind, job, values, ownership_now=now,
        commit_transaction=commit_transaction,
    )


def complete(
    conn: Any,
    kind: str,
    job: dict,
    assignments: dict[str, Any],
    *,
    now: str | None = None,
    commit_transaction: bool = True,
) -> None:
    finish(
        conn, kind, job, "COMPLETE", assignments,
        now=now, commit_transaction=commit_transaction,
    )


def fail_or_retry(
    conn: Any,
    kind: str,
    job: dict,
    *,
    error_type: str,
    error_message: str,
    retryable: bool,
    retry_after_seconds: float | None = None,
    checkpoint: dict | None = None,
    now: str | None = None,
    base_delay_seconds: int = 30,
    max_delay_seconds: int = 900,
) -> str:
    """Release a claim to RETRY_WAIT, or fail it finally, under fencing."""
    now = now or utc_now()
    attempts = int(job.get("attempts") or 0)
    max_attempts = int(job.get("max_attempts") or 1)
    can_retry = retryable and attempts < max_attempts
    if can_retry:
        exponential = min(max_delay_seconds, base_delay_seconds * (2 ** max(0, attempts - 1)))
        requested = float(retry_after_seconds or 0)
        delay = min(max_delay_seconds, max(float(exponential), requested))
        status = "RETRY_WAIT"
        next_attempt_at = after_seconds(delay, now=now)
    else:
        status = "FAILED_FINAL"
        next_attempt_at = None
    assignments = {
        "status": status,
        "lease_owner": None,
        "lease_until": None,
        "next_attempt_at": next_attempt_at,
        "last_error_type": str(error_type)[:120],
        "last_error": str(error_message)[:2000],
        "updated_at": now,
    }
    if checkpoint is not None:
        assignments["checkpoint_json"] = json.dumps(
            checkpoint, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    _fenced_update(conn, kind, job, assignments, ownership_now=now)
    return status


def versioned_output_path(base: Path, job: dict) -> Path:
    """Return a unique, immutable result path for this fenced attempt."""
    suffix = base.suffix or ".json"
    stem = base.name[:-len(suffix)] if suffix else base.name
    return base.with_name(
        f"{stem}.lease-{int(job['lease_epoch']):08d}.attempt-{int(job['attempts']):04d}{suffix}"
    )


def finite_retry_after(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None
