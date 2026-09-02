-- Additive, idempotent PostgreSQL DDL.  The migration CLI executes this in a
-- transaction while pipeline/finalizer are paused.
ALTER TABLE recording_segments
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

ALTER TABLE recording_segments
    ADD COLUMN IF NOT EXISTS superseded_by_segment_id TEXT;

ALTER TABLE recording_segments
    ADD COLUMN IF NOT EXISTS lifecycle_updated_at TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'recording_segments_superseded_by_fkey'
          AND conrelid = 'recording_segments'::regclass
    ) THEN
        ALTER TABLE recording_segments
            ADD CONSTRAINT recording_segments_superseded_by_fkey
            FOREIGN KEY (superseded_by_segment_id)
            REFERENCES recording_segments(segment_id)
            ON DELETE RESTRICT;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_recording_segments_lifecycle
    ON recording_segments(lifecycle_status, session_id);

