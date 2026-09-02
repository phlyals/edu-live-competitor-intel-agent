#!/usr/bin/env python3
"""Materialise identity evidence and close audited historical conflicts."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from v3_runtime import connect, init_db, new_id, utc_now  # noqa: E402


def main() -> int:
    mapping_path = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/batch-monitor/20260825-atomic-identity-closure/candidate_mappings.json")
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    mapping_digest = hashlib.sha256(mapping_path.read_bytes()).hexdigest()
    by_name = {str(item.get("name") or ""): item for item in mapping.get("targets") or []}
    init_db()
    report = {"mapping_path": str(mapping_path), "mapping_digest": mapping_digest, "evidence_rows": 0, "resolved_conflicts": 0, "open_conflicts": 0}
    with connect() as conn:
        rows = conn.execute("SELECT i.*,c.account_name FROM identities i JOIN competitors c ON c.competitor_id=i.competitor_id ORDER BY i.identity_id").fetchall()
        for row in rows:
            item = by_name.get(str(row["account_name"] or "")) or {}
            historical = {str(v) for v in item.get("historical_buyin_uids") or []}
            is_historical = row["platform"] == "buyin" and (
                str(row["stable_id"]) in historical
                or (str(item.get("mapping_status") or "").startswith("VERIFIED") and bool(item.get("current_buyin_uid")))
            )
            if row["verification_status"] == "IDENTITY_CONFLICT" and is_historical:
                conn.execute("UPDATE identities SET verification_status='HISTORICAL_ALIAS',verified_at=COALESCE(verified_at,?) WHERE identity_id=?", (utc_now(), row["identity_id"]))
                row_status = "HISTORICAL_ALIAS"
            else:
                row_status = str(row["verification_status"])
            evidence = json.loads(row["evidence_json"] or "{}") if row["evidence_json"] else {}
            payload = {"mapping_item": item, "identity_evidence": evidence, "source": str(mapping_path)}
            evidence_id = "evidence:" + hashlib.sha256(f"{row['identity_id']}:{mapping_digest}".encode()).hexdigest()[:28]
            conn.execute(
                "INSERT INTO identity_evidence(evidence_id,identity_id,evidence_type,source_path,source_digest,captured_at,verification_status,metadata_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(identity_id,evidence_type,source_digest) DO UPDATE SET verification_status=excluded.verification_status,metadata_json=excluded.metadata_json",
                (evidence_id, row["identity_id"], "audited_mapping", str(mapping_path), mapping_digest, utc_now(), "VERIFIED" if row_status in {"VERIFIED", "HISTORICAL_ALIAS"} else "PENDING", json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            )
            report["evidence_rows"] += 1
            if row["verification_status"] == "IDENTITY_CONFLICT":
                conflict_id = "conflict:" + hashlib.sha256(str(row["identity_id"]).encode()).hexdigest()[:28]
                resolved = is_historical and bool(item.get("current_buyin_uid"))
                conn.execute(
                    "INSERT INTO identity_conflicts(conflict_id,competitor_id,identity_id,conflict_type,status,detected_at,resolved_at,resolution_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(conflict_id) DO UPDATE SET status=excluded.status,resolved_at=excluded.resolved_at,resolution_json=excluded.resolution_json",
                    (conflict_id, row["competitor_id"], row["identity_id"], "HISTORICAL_UID_ALIAS", "RESOLVED" if resolved else "OPEN", row["verified_at"] or utc_now(), utc_now() if resolved else None, json.dumps({"current_buyin_uid": item.get("current_buyin_uid"), "mapping_status": item.get("mapping_status"), "resolution_basis": "audited_current_identity_supersedes_conflicting_historical_observation", "source": str(mapping_path)}, ensure_ascii=False, sort_keys=True)),
                )
                if resolved:
                    report["resolved_conflicts"] += 1
                else:
                    report["open_conflicts"] += 1
        conn.commit()
    out = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3/identity-closure-manifest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["open_conflicts"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
