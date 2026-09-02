-- Migration 004: content-digest lineage and durable recompute requests.
-- Apply while pipeline, analysis, evidence and recompute workers are stopped.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('edu_live_v3_lineage_recompute_v1'));

ALTER TABLE lineage_edges
ADD COLUMN IF NOT EXISTS binding_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS upstream_engine_version TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS upstream_model_version TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS downstream_model_version TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS downstream_prompt_version TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS downstream_schema_version TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS updated_at TEXT;
ALTER TABLE lineage_edges
ADD COLUMN IF NOT EXISTS metadata_json TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS recompute_requests (
    request_id TEXT PRIMARY KEY,
    downstream_type TEXT NOT NULL,
    downstream_id TEXT NOT NULL,
    upstream_type TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    old_upstream_digest TEXT NOT NULL,
    new_upstream_digest TEXT NOT NULL,
    target_analysis_spec_version TEXT NOT NULL,
    target_model_version TEXT NOT NULL,
    target_prompt_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    candidate_analysis_id TEXT REFERENCES analyses(analysis_id),
    attempts BIGINT NOT NULL DEFAULT 0,
    max_attempts BIGINT NOT NULL DEFAULT 5,
    next_attempt_at TEXT,
    last_attempt_at TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    last_error_type TEXT,
    last_error TEXT,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(
        downstream_type, downstream_id, upstream_type, upstream_id,
        old_upstream_digest, new_upstream_digest,
        target_analysis_spec_version, target_model_version,
        target_prompt_version
    )
);

UPDATE lineage_edges
SET binding_status='LEGACY_UNVERIFIED',
    updated_at=COALESCE(updated_at,created_at),
    metadata_json=COALESCE(NULLIF(metadata_json,''),'{}');

UPDATE lineage_edges AS edge
SET binding_status='CONTENT_DIGEST_VERIFIED',
    upstream_engine_version=transcript.engine,
    upstream_model_version=transcript.model,
    downstream_model_version=analysis.model_version,
    downstream_prompt_version=analysis.prompt_version,
    downstream_schema_version=analysis.analysis_spec_version,
    updated_at=COALESCE(edge.updated_at,edge.created_at)
FROM analyses AS analysis
JOIN transcripts AS transcript
  ON transcript.transcript_id=analysis.transcript_id
WHERE edge.downstream_type='analysis'
  AND edge.downstream_id=analysis.analysis_id
  AND edge.upstream_type='transcript'
  AND edge.upstream_id=analysis.transcript_id
  AND edge.upstream_version=analysis.transcript_content_digest
  AND analysis.transcript_content_digest ~ '^[0-9a-f]{64}$';

CREATE INDEX IF NOT EXISTS idx_lineage_upstream_version
ON lineage_edges(upstream_type,upstream_id,upstream_version,binding_status,state);
CREATE INDEX IF NOT EXISTS idx_recompute_requests_due
ON recompute_requests(status,next_attempt_at,lease_until);
CREATE INDEX IF NOT EXISTS idx_recompute_requests_candidate
ON recompute_requests(candidate_analysis_id,status);

CREATE OR REPLACE FUNCTION v3_guard_lineage_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.downstream_type IS DISTINCT FROM OLD.downstream_type
       OR NEW.downstream_id IS DISTINCT FROM OLD.downstream_id
       OR NEW.upstream_type IS DISTINCT FROM OLD.upstream_type
       OR NEW.upstream_id IS DISTINCT FROM OLD.upstream_id
       OR NEW.upstream_version IS DISTINCT FROM OLD.upstream_version
       OR NEW.binding_status IS DISTINCT FROM OLD.binding_status
       OR NEW.upstream_engine_version IS DISTINCT FROM OLD.upstream_engine_version
       OR NEW.upstream_model_version IS DISTINCT FROM OLD.upstream_model_version
       OR NEW.downstream_model_version IS DISTINCT FROM OLD.downstream_model_version
       OR NEW.downstream_prompt_version IS DISTINCT FROM OLD.downstream_prompt_version
       OR NEW.downstream_schema_version IS DISTINCT FROM OLD.downstream_schema_version
    THEN
        RAISE EXCEPTION 'lineage identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_v3_guard_lineage_identity ON lineage_edges;
CREATE TRIGGER trg_v3_guard_lineage_identity
BEFORE UPDATE ON lineage_edges
FOR EACH ROW EXECUTE FUNCTION v3_guard_lineage_identity();

CREATE OR REPLACE FUNCTION v3_guard_recompute_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.downstream_type IS DISTINCT FROM OLD.downstream_type
       OR NEW.downstream_id IS DISTINCT FROM OLD.downstream_id
       OR NEW.upstream_type IS DISTINCT FROM OLD.upstream_type
       OR NEW.upstream_id IS DISTINCT FROM OLD.upstream_id
       OR NEW.old_upstream_digest IS DISTINCT FROM OLD.old_upstream_digest
       OR NEW.new_upstream_digest IS DISTINCT FROM OLD.new_upstream_digest
       OR NEW.target_analysis_spec_version IS DISTINCT FROM OLD.target_analysis_spec_version
       OR NEW.target_model_version IS DISTINCT FROM OLD.target_model_version
       OR NEW.target_prompt_version IS DISTINCT FROM OLD.target_prompt_version
    THEN
        RAISE EXCEPTION 'recompute request identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_v3_guard_recompute_identity ON recompute_requests;
CREATE TRIGGER trg_v3_guard_recompute_identity
BEFORE UPDATE ON recompute_requests
FOR EACH ROW EXECUTE FUNCTION v3_guard_recompute_identity();

INSERT INTO schema_meta(key,value)
VALUES('lineage_recompute_revision','1')
ON CONFLICT(key) DO UPDATE SET value=excluded.value,
updated_at=to_char(current_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');

COMMIT;
