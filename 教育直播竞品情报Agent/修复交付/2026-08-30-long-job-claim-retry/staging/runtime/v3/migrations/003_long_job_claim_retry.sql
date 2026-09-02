-- Migration 003 (after 002_analysis_qualification): explicit, idempotent
-- PostgreSQL migration for long transcript/analysis jobs.
-- Apply in one transaction while the two affected workers are stopped.
-- Recording/runtime workers do not depend on these columns.

BEGIN;
SELECT pg_advisory_xact_lock(hashtext('edu_live_v3_long_job_claim_retry_v1'));

ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS updated_at TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS attempts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS max_attempts BIGINT NOT NULL DEFAULT 5;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS next_attempt_at TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS last_attempt_at TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS lease_until TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS lease_epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS last_error_type TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS checkpoint_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE analyses ADD COLUMN IF NOT EXISTS updated_at TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS attempts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS max_attempts BIGINT NOT NULL DEFAULT 5;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS next_attempt_at TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS last_attempt_at TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS lease_until TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS lease_epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS last_error_type TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS checkpoint_json TEXT NOT NULL DEFAULT '{}';

UPDATE transcripts
SET updated_at=COALESCE(updated_at,created_at),
    next_attempt_at=CASE
      WHEN status IN ('PENDING','WAITING_TOOL','PAUSED','RETRY_WAIT')
      THEN COALESCE(next_attempt_at,created_at)
      ELSE next_attempt_at END;

UPDATE analyses
SET updated_at=COALESCE(updated_at,to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
    next_attempt_at=CASE
      WHEN status IN ('PENDING','WAITING_MODEL','RETRY_WAIT')
      THEN COALESCE(next_attempt_at,to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
      ELSE next_attempt_at END;

CREATE INDEX IF NOT EXISTS idx_transcripts_long_job_due
ON transcripts(status,next_attempt_at,lease_until);
CREATE INDEX IF NOT EXISTS idx_analyses_long_job_due
ON analyses(status,next_attempt_at,lease_until);

INSERT INTO schema_meta(key,value)
VALUES('long_job_claim_retry_revision','1')
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');

COMMIT;
