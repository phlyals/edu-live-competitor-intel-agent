#!/usr/bin/env python3
"""Validate one explicitly authorized, local read-only product-scan draft.

The validator accepts any positive, consecutive number of pages/batches.  It
does not control a browser and never writes Runtime business data.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from runtime_common import load_config, utc_now


REQUIRED_ROW_FIELDS = (
    "live_title", "live_date", "account_name", "follower_count", "location",
    "sales", "settlement_amount", "total_views", "peak_popularity",
    "click_rate", "order_conversion_rate",
)

TRUSTED_END_SIGNAL_TYPES = {
    "next_disabled",
    "load_more_disabled",
    "no_more_results_message",
    "platform_page_count_exhausted",
}


def verified_identity(record: dict) -> bool:
    """Require a verified Buyin creator uid that matches the detail-page URL."""
    if not isinstance(record, dict):
        return False
    profile_url = str(record.get("profile_url") or "")
    buyin_uid = str(record.get("buyin_creator_uid") or "")
    try:
        url_uid = (parse_qs(urlparse(profile_url).query).get("uid") or [""])[0]
    except (TypeError, ValueError):
        return False
    return bool(
        buyin_uid
        and url_uid == buyin_uid
        and record.get("detail_page_verified") is True
    )


def confined(path: Path, root: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"{label} must stay inside the configured analysis_drafts directory")
    return resolved


def observation_key(row: dict) -> str:
    """Return a content key; creator identity is deliberately not the row key."""
    preferred = ("account_name", "live_date", "live_title", "total_views")
    if all(row.get(field) for field in preferred):
        return "|".join(str(row[field]) for field in preferred)
    return "|".join(
        str(row.get(field) or "")
        for field in ("account_name", "live_date", "live_title", "sales", "source_position")
    )


def validate_pages(
    payload: dict,
    *,
    expected_total: int | None = None,
    require_complete: bool = False,
    require_legacy_fields: bool = True,
    require_unique_observations: bool = True,
) -> dict:
    """Validate dynamic pages/batches and return normalized counts and rows."""
    pages = payload.get("pages")
    if not isinstance(pages, list) or not pages:
        raise ValueError("pages must be a non-empty list")
    page_numbers = [page.get("page") for page in pages if isinstance(page, dict)]
    if len(page_numbers) != len(pages) or page_numbers != list(range(1, len(pages) + 1)):
        raise ValueError("Pages must be positive, consecutive, and start at 1")

    rows: list[dict] = []
    missing_fields = []
    page_counts = []
    for page in pages:
        page_rows = page.get("rows")
        if not isinstance(page_rows, list):
            raise ValueError(f"Page {page.get('page')} rows are invalid")
        page_counts.append(len(page_rows))
        for offset, row in enumerate(page_rows, start=1):
            if not isinstance(row, dict):
                raise ValueError(f"Page {page.get('page')} row {offset} is invalid")
            if require_legacy_fields:
                missing = [field for field in REQUIRED_ROW_FIELDS if not row.get(field)]
                if missing:
                    missing_fields.append({"page": page["page"], "row": offset, "fields": missing})
            rows.append(row)
    if missing_fields:
        raise ValueError(f"Required fields are missing: {missing_fields}")

    keys = [observation_key(row) for row in rows]
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    if duplicates and require_unique_observations:
        raise ValueError(f"Duplicate observations found: {duplicates}")

    reported_total = (payload.get("product") or {}).get("reported_live_count")
    target_total = expected_total if expected_total is not None else reported_total
    if target_total is not None and len(rows) != target_total:
        raise ValueError(f"Completeness check failed: rows={len(rows)}, expected={target_total}")
    if expected_total is not None and reported_total is not None and reported_total != expected_total:
        raise ValueError("Page-reported live count does not match the expected total")

    end_signal = payload.get("end_signal") or {}
    end_signal_ok = bool(
        isinstance(end_signal, dict)
        and end_signal.get("verified") is True
        and end_signal.get("type") in TRUSTED_END_SIGNAL_TYPES
    )
    if require_complete and not end_signal_ok:
        raise ValueError("A trusted platform no-more-results signal is required")

    return {
        "pages": pages,
        "rows": rows,
        "page_counts": page_counts,
        "keys": keys,
        "duplicate_content_keys": duplicates,
        "reported_total": reported_total,
        "end_signal": end_signal,
        "end_signal_verified": end_signal_ok,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--identity-input", type=Path, help="Optional read-only identity evidence inside analysis_drafts")
    parser.add_argument("--expected-total", type=int)
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Require a verified platform no-more-results signal",
    )
    args = parser.parse_args()

    config = load_config()
    draft_root = Path(config["storage"]["directories"]["analysis_drafts"]).resolve()
    source = confined(args.input, draft_root, "input")
    destination = confined(args.output, draft_root, "output")
    payload = json.loads(source.read_text(encoding="utf-8"))
    identity_source = confined(args.identity_input, draft_root, "identity input") if args.identity_input else None
    identity_payload = json.loads(identity_source.read_text(encoding="utf-8")) if identity_source else None

    if payload.get("dry_run") is not True:
        raise ValueError("Input is not explicitly marked as a dry run")
    scope = payload.get("authorization_scope") or {}
    if scope.get("read_only_current_5yuan_product") is not True:
        raise ValueError("The recorded authorization scope does not permit this dry run")
    forbidden = ("recording", "feishu_business_write", "formal_knowledge_base_write")
    if any(scope.get(name) is not False for name in forbidden):
        raise ValueError("A prohibited side effect is not explicitly disabled")

    validated = validate_pages(
        payload,
        expected_total=args.expected_total,
        require_complete=args.require_complete,
        require_legacy_fields=True,
    )
    pages = validated["pages"]
    rows = validated["rows"]
    keys = validated["keys"]

    accounts = Counter(row["account_name"] for row in rows)
    if identity_payload is not None:
        if identity_payload.get("dry_run") is not True:
            raise ValueError("Identity evidence is not explicitly marked as a dry run")
        identity_side_effects = identity_payload.get("side_effects") or {}
        identity_forbidden = ("recording_started", "feishu_business_records_written", "formal_knowledge_base_written", "runtime_business_records_written")
        if any(identity_side_effects.get(name) is not False for name in identity_forbidden):
            raise ValueError("Identity evidence does not explicitly disable every prohibited side effect")
    identity_records = (identity_payload or {}).get("accounts") or payload.get("identity_records") or []
    if not isinstance(identity_records, list):
        raise ValueError("identity_records must be a list when supplied")
    identity_by_name = {}
    for record in identity_records:
        if not isinstance(record, dict) or not record.get("account_name"):
            raise ValueError("Every identity record must contain account_name")
        name = str(record["account_name"])
        if name in identity_by_name:
            raise ValueError(f"Duplicate identity record for account_name={name}")
        identity_by_name[name] = record
    unexpected_names = sorted(set(identity_by_name) - set(accounts))
    if unexpected_names:
        raise ValueError(f"Identity records contain accounts absent from the scan: {unexpected_names}")
    verified_names = sorted(name for name in accounts if verified_identity(identity_by_name.get(name)))
    unresolved_names = sorted(set(accounts) - set(verified_names))
    stable_ids_available = not unresolved_names
    identity_quality = "stable_buyin_creator_uid" if stable_ids_available else "partial_or_display_name_only"
    result = {
        "schema_version": 1,
        "status": "READY" if stable_ids_available else "DEGRADED",
        "integrity_check": "READY",
        "validated_at": utc_now(),
        "profile_id": config["profile_id"],
        "mode": "read_only_dry_run",
        "source_draft": str(source),
        "identity_evidence": str(identity_source) if identity_source else None,
        "product": payload.get("product"),
        "page_counts": [len(page["rows"]) for page in pages],
        "row_count": len(rows),
        "unique_row_count": len(set(keys)),
        "unique_account_display_names": len(accounts),
        "account_row_counts": dict(sorted(accounts.items(), key=lambda item: (-item[1], item[0]))),
        "identity_quality": identity_quality,
        "identity_resolution": {
            "required_method": "nickname_to_detail_page_then_capture_buyin_creator_uid",
            "verified_account_count": len(verified_names),
            "total_account_count": len(accounts),
            "verified_account_names": verified_names,
            "unresolved_account_names": unresolved_names,
            "monitor_key_policy": "buyin_creator_uid is canonical for Buyin monitoring; douyin_account_id is optional cross-platform enrichment",
        },
        "identity_ready_for_production_monitor": stable_ids_available,
        "ready_for_production_monitor": False,
        "monitor_runtime_preflight_required": True,
        "end_signal": validated["end_signal"],
        "end_signal_verified": validated["end_signal_verified"],
        "limitations": [] if stable_ids_available else [
            "Every unique account must be opened from its nickname and retain a verified Buyin uid from the detail-page URL before production monitoring."
        ],
        "side_effects": {
            "recording_started": False,
            "feishu_business_records_written": False,
            "formal_knowledge_base_written": False,
            "runtime_business_records_written": False,
        },
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(destination)
    print(json.dumps({**result, "output": str(destination)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
