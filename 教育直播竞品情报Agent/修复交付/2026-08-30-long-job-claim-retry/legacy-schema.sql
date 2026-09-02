CREATE TABLE schema_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT);
CREATE TABLE transcripts(
 transcript_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,source_digest TEXT NOT NULL,
 engine TEXT NOT NULL,model TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',language TEXT,
 source_path TEXT,output_path TEXT,low_confidence_count BIGINT NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE analyses(
 analysis_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,analysis_type TEXT NOT NULL,
 source_digest TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',output_path TEXT,
 lineage_state TEXT NOT NULL DEFAULT 'CURRENT',metadata_json TEXT NOT NULL DEFAULT '{}');
INSERT INTO transcripts VALUES('t','s','d','faster-whisper','small','PENDING',NULL,NULL,NULL,0,'2026-08-30T00:00:00.000Z','{}');
INSERT INTO analyses VALUES('a','s','single_session','d','WAITING_MODEL',NULL,'CURRENT','{}');
