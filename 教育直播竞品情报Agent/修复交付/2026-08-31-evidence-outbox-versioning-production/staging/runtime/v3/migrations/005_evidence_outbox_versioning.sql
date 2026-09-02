-- Migration 005: evidence-gated versioned semantic projections.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('edu_live_v3_evidence_outbox_v1'));

ALTER TABLE outbox ADD COLUMN IF NOT EXISTS projection_version TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS artifact_digest TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS evidence_bundle_id TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS evidence_manifest_hash TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS evidence_verified_at TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS projection_binding_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS projection_version TEXT;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS artifact_digest TEXT;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS evidence_bundle_id TEXT;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS evidence_manifest_hash TEXT;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS evidence_verified_at TEXT;
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS projection_binding_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_projection_version
ON outbox(destination,object_type,object_id,projection_version)
WHERE projection_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_projection_binding
ON outbox(object_type,projection_binding_status,status,projection_version);
CREATE INDEX IF NOT EXISTS idx_delivery_receipts_projection_version
ON delivery_receipts(object_type,object_id,projection_version,status);

CREATE OR REPLACE FUNCTION v3_guard_projection_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.projection_version IS NOT NULL AND (
       NEW.dedupe_key IS DISTINCT FROM OLD.dedupe_key
       OR NEW.object_type IS DISTINCT FROM OLD.object_type
       OR NEW.object_id IS DISTINCT FROM OLD.object_id
       OR NEW.destination IS DISTINCT FROM OLD.destination
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.payload_json IS DISTINCT FROM OLD.payload_json
       OR NEW.projection_version IS DISTINCT FROM OLD.projection_version
       OR NEW.artifact_digest IS DISTINCT FROM OLD.artifact_digest
       OR NEW.evidence_bundle_id IS DISTINCT FROM OLD.evidence_bundle_id
       OR NEW.evidence_manifest_hash IS DISTINCT FROM OLD.evidence_manifest_hash
       OR NEW.evidence_verified_at IS DISTINCT FROM OLD.evidence_verified_at
       OR NEW.projection_binding_status IS DISTINCT FROM OLD.projection_binding_status
    ) THEN
        RAISE EXCEPTION 'versioned projection identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_v3_guard_projection_identity ON outbox;
CREATE TRIGGER trg_v3_guard_projection_identity
BEFORE UPDATE ON outbox FOR EACH ROW
EXECUTE FUNCTION v3_guard_projection_identity();

CREATE OR REPLACE FUNCTION v3_guard_receipt_projection_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.projection_version IS NOT NULL AND (
       NEW.outbox_id IS DISTINCT FROM OLD.outbox_id
       OR NEW.destination IS DISTINCT FROM OLD.destination
       OR NEW.object_type IS DISTINCT FROM OLD.object_type
       OR NEW.object_id IS DISTINCT FROM OLD.object_id
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.projection_version IS DISTINCT FROM OLD.projection_version
       OR NEW.artifact_digest IS DISTINCT FROM OLD.artifact_digest
       OR NEW.evidence_bundle_id IS DISTINCT FROM OLD.evidence_bundle_id
       OR NEW.evidence_manifest_hash IS DISTINCT FROM OLD.evidence_manifest_hash
       OR NEW.evidence_verified_at IS DISTINCT FROM OLD.evidence_verified_at
       OR NEW.projection_binding_status IS DISTINCT FROM OLD.projection_binding_status
    ) THEN
        RAISE EXCEPTION 'versioned receipt identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_v3_guard_receipt_projection_identity ON delivery_receipts;
CREATE TRIGGER trg_v3_guard_receipt_projection_identity
BEFORE UPDATE ON delivery_receipts FOR EACH ROW
EXECUTE FUNCTION v3_guard_receipt_projection_identity();

INSERT INTO schema_meta(key,value)
VALUES('evidence_outbox_versioning_revision','1')
ON CONFLICT(key) DO UPDATE SET value=excluded.value,
updated_at=to_char(current_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');

COMMIT;
