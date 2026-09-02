ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

ALTER TABLE analyses ADD COLUMN IF NOT EXISTS transcript_id TEXT REFERENCES transcripts(transcript_id);
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS transcript_content_digest TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS analysis_spec_version TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS model_version TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS prompt_version TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS artifact_digest TEXT;
ALTER TABLE analyses DROP CONSTRAINT IF EXISTS analyses_session_id_analysis_type_source_digest_key;
CREATE INDEX IF NOT EXISTS idx_analyses_transcript_id ON analyses(transcript_id);
CREATE INDEX IF NOT EXISTS idx_analyses_qualification ON analyses(scope, qualification_status, lineage_state, status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_analyses_transcript_spec
ON analyses(transcript_id, analysis_type, transcript_content_digest, analysis_spec_version, model_version, prompt_version)
WHERE transcript_id IS NOT NULL AND transcript_content_digest IS NOT NULL
  AND analysis_spec_version IS NOT NULL AND model_version IS NOT NULL AND prompt_version IS NOT NULL;

ALTER TABLE evidence_bundles ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE evidence_bundles ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
CREATE INDEX IF NOT EXISTS idx_outbox_qualification ON outbox(scope, qualification_status, status);
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED';
ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

CREATE OR REPLACE FUNCTION v3_guard_analysis_immutable_identity() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.scope IN ('FORMAL_SINGLE_SESSION', 'SAMPLE_AUXILIARY') AND (
       NEW.session_id IS DISTINCT FROM OLD.session_id
       OR NEW.transcript_id IS DISTINCT FROM OLD.transcript_id
       OR NEW.analysis_type IS DISTINCT FROM OLD.analysis_type
       OR NEW.source_digest IS DISTINCT FROM OLD.source_digest
       OR NEW.transcript_content_digest IS DISTINCT FROM OLD.transcript_content_digest
       OR NEW.analysis_spec_version IS DISTINCT FROM OLD.analysis_spec_version
       OR NEW.model_version IS DISTINCT FROM OLD.model_version
       OR NEW.prompt_version IS DISTINCT FROM OLD.prompt_version
       OR (OLD.output_path IS NOT NULL AND NEW.output_path IS DISTINCT FROM OLD.output_path)
       OR (OLD.artifact_digest IS NOT NULL AND NEW.artifact_digest IS DISTINCT FROM OLD.artifact_digest)
       OR (OLD.output_path IS NULL AND NEW.output_path IS NOT NULL AND NEW.status <> 'COMPLETE')
       OR (OLD.artifact_digest IS NULL AND NEW.artifact_digest IS NOT NULL AND NEW.status <> 'COMPLETE')) THEN
        RAISE EXCEPTION 'analysis immutable identity cannot change: %', OLD.analysis_id;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_v3_guard_analysis_immutable_identity ON analyses;
CREATE TRIGGER trg_v3_guard_analysis_immutable_identity
BEFORE UPDATE ON analyses
FOR EACH ROW EXECUTE FUNCTION v3_guard_analysis_immutable_identity();
