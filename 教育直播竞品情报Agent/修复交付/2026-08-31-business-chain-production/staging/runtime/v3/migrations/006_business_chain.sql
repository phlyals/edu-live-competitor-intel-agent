-- Migration 006: qualified comparisons, three-session versions and approved knowledge.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('edu_live_v3_business_chain_v1'));

ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS competitor_id TEXT REFERENCES competitors(competitor_id);
ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS version_id TEXT;
ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS comparison_id TEXT;
ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS candidate_digest TEXT;
ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS evidence_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE strategy_candidates ADD COLUMN IF NOT EXISTS updated_at TEXT;

ALTER TABLE knowledge_diffs ADD COLUMN IF NOT EXISTS competitor_id TEXT REFERENCES competitors(competitor_id);
ALTER TABLE knowledge_diffs ADD COLUMN IF NOT EXISTS version_id TEXT;
ALTER TABLE knowledge_diffs ADD COLUMN IF NOT EXISTS candidate_digest TEXT;
ALTER TABLE knowledge_diffs ADD COLUMN IF NOT EXISTS approval_id TEXT REFERENCES approvals(approval_id);
ALTER TABLE knowledge_diffs ADD COLUMN IF NOT EXISTS updated_at TEXT;

CREATE TABLE IF NOT EXISTS comparisons (
    comparison_id TEXT PRIMARY KEY, competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    older_session_id TEXT NOT NULL REFERENCES live_sessions(session_id), newer_session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    older_analysis_id TEXT NOT NULL REFERENCES analyses(analysis_id), newer_analysis_id TEXT NOT NULL REFERENCES analyses(analysis_id),
    older_artifact_digest TEXT NOT NULL, newer_artifact_digest TEXT NOT NULL,
    comparison_spec_version TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'COMPLETE',
    similarity_state TEXT NOT NULL, similarity_score DOUBLE PRECISION NOT NULL,
    older_structure_digest TEXT NOT NULL, newer_structure_digest TEXT NOT NULL,
    output_path TEXT NOT NULL, artifact_digest TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'FULL_SESSION_PAIR',
    qualification_status TEXT NOT NULL DEFAULT 'FULL_SESSION_PAIR_QUALIFIED',
    created_at TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(competitor_id,older_analysis_id,newer_analysis_id,comparison_spec_version)
);
CREATE TABLE IF NOT EXISTS comparison_evidence (
    comparison_id TEXT NOT NULL REFERENCES comparisons(comparison_id), side TEXT NOT NULL,
    analysis_id TEXT NOT NULL REFERENCES analyses(analysis_id), session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    transcript_id TEXT NOT NULL REFERENCES transcripts(transcript_id), source_segment_id TEXT NOT NULL,
    content_digest TEXT NOT NULL, start_seconds DOUBLE PRECISION NOT NULL, end_seconds DOUBLE PRECISION NOT NULL,
    PRIMARY KEY(comparison_id,side,analysis_id,source_segment_id)
);
CREATE TABLE IF NOT EXISTS strategy_versions (
    version_id TEXT PRIMARY KEY, competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    version_no BIGINT NOT NULL, structure_digest TEXT NOT NULL, status TEXT NOT NULL,
    supporting_session_count BIGINT NOT NULL DEFAULT 3,
    first_session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    last_session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    restored_from_version_id TEXT, activation_count BIGINT NOT NULL DEFAULT 1,
    content_path TEXT NOT NULL, content_hash TEXT NOT NULL, activated_at TEXT NOT NULL,
    superseded_at TEXT, created_at TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(competitor_id,structure_digest), UNIQUE(competitor_id,version_no)
);
CREATE TABLE IF NOT EXISTS version_observations (
    observation_id TEXT PRIMARY KEY, competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id), analysis_id TEXT NOT NULL REFERENCES analyses(analysis_id),
    comparison_id TEXT REFERENCES comparisons(comparison_id), ended_at TEXT NOT NULL,
    structure_digest TEXT NOT NULL, previous_structure_digest TEXT,
    consecutive_count BIGINT NOT NULL, observation_state TEXT NOT NULL,
    active_version_id TEXT REFERENCES strategy_versions(version_id), created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}', UNIQUE(competitor_id,analysis_id)
);
CREATE TABLE IF NOT EXISTS strategy_evidence (
    candidate_id TEXT NOT NULL REFERENCES strategy_candidates(candidate_id), comparison_id TEXT REFERENCES comparisons(comparison_id),
    analysis_id TEXT NOT NULL REFERENCES analyses(analysis_id), session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    transcript_id TEXT NOT NULL REFERENCES transcripts(transcript_id), source_segment_id TEXT NOT NULL,
    content_digest TEXT NOT NULL, start_seconds DOUBLE PRECISION NOT NULL, end_seconds DOUBLE PRECISION NOT NULL,
    PRIMARY KEY(candidate_id,analysis_id,source_segment_id)
);
CREATE TABLE IF NOT EXISTS knowledge_publish_receipts (
    publish_receipt_id TEXT PRIMARY KEY, approval_id TEXT NOT NULL REFERENCES approvals(approval_id),
    candidate_id TEXT NOT NULL REFERENCES strategy_candidates(candidate_id), diff_id TEXT NOT NULL REFERENCES knowledge_diffs(diff_id),
    knowledge_version_id TEXT NOT NULL REFERENCES knowledge_versions(version_id), object_key TEXT NOT NULL,
    content_hash TEXT NOT NULL, status TEXT NOT NULL, published_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}', UNIQUE(approval_id), UNIQUE(candidate_id,diff_id)
);

UPDATE strategy_candidates SET evidence_json=COALESCE(NULLIF(evidence_json,''),'{}'),updated_at=COALESCE(updated_at,created_at);
UPDATE knowledge_diffs SET updated_at=COALESCE(updated_at,created_at);

CREATE INDEX IF NOT EXISTS idx_comparisons_competitor_newer ON comparisons(competitor_id,newer_session_id,status);
CREATE INDEX IF NOT EXISTS idx_comparison_evidence_analysis ON comparison_evidence(analysis_id,source_segment_id);
CREATE INDEX IF NOT EXISTS idx_version_observations_competitor ON version_observations(competitor_id,ended_at,analysis_id);
CREATE INDEX IF NOT EXISTS idx_strategy_versions_active ON strategy_versions(competitor_id,status,version_no);
CREATE INDEX IF NOT EXISTS idx_strategy_evidence_candidate ON strategy_evidence(candidate_id,session_id);
CREATE INDEX IF NOT EXISTS idx_knowledge_publish_receipts_candidate ON knowledge_publish_receipts(candidate_id,status);

CREATE OR REPLACE FUNCTION v3_guard_comparison_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.competitor_id IS DISTINCT FROM OLD.competitor_id OR NEW.older_session_id IS DISTINCT FROM OLD.older_session_id
 OR NEW.newer_session_id IS DISTINCT FROM OLD.newer_session_id OR NEW.older_analysis_id IS DISTINCT FROM OLD.older_analysis_id
 OR NEW.newer_analysis_id IS DISTINCT FROM OLD.newer_analysis_id OR NEW.older_artifact_digest IS DISTINCT FROM OLD.older_artifact_digest
 OR NEW.newer_artifact_digest IS DISTINCT FROM OLD.newer_artifact_digest OR NEW.comparison_spec_version IS DISTINCT FROM OLD.comparison_spec_version
 OR NEW.output_path IS DISTINCT FROM OLD.output_path OR NEW.artifact_digest IS DISTINCT FROM OLD.artifact_digest THEN
  RAISE EXCEPTION 'comparison identity is immutable';
 END IF; RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS trg_v3_guard_comparison_identity ON comparisons;
CREATE TRIGGER trg_v3_guard_comparison_identity BEFORE UPDATE ON comparisons FOR EACH ROW EXECUTE FUNCTION v3_guard_comparison_identity();

CREATE OR REPLACE FUNCTION v3_guard_strategy_version_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.competitor_id IS DISTINCT FROM OLD.competitor_id OR NEW.version_no IS DISTINCT FROM OLD.version_no
 OR NEW.structure_digest IS DISTINCT FROM OLD.structure_digest OR NEW.content_path IS DISTINCT FROM OLD.content_path
 OR NEW.content_hash IS DISTINCT FROM OLD.content_hash THEN RAISE EXCEPTION 'strategy version identity is immutable'; END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS trg_v3_guard_strategy_version_identity ON strategy_versions;
CREATE TRIGGER trg_v3_guard_strategy_version_identity BEFORE UPDATE ON strategy_versions FOR EACH ROW EXECUTE FUNCTION v3_guard_strategy_version_identity();

CREATE OR REPLACE FUNCTION v3_guard_knowledge_publish_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'knowledge publish receipt is immutable'; END; $$;
DROP TRIGGER IF EXISTS trg_v3_guard_knowledge_publish_receipt ON knowledge_publish_receipts;
CREATE TRIGGER trg_v3_guard_knowledge_publish_receipt BEFORE UPDATE ON knowledge_publish_receipts FOR EACH ROW EXECUTE FUNCTION v3_guard_knowledge_publish_receipt();

INSERT INTO schema_meta(key,value) VALUES('business_chain_revision','1')
ON CONFLICT(key) DO UPDATE SET value=excluded.value,
updated_at=to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
COMMIT;
