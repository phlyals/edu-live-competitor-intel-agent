import json
from pathlib import Path
import psycopg
cfg=json.loads(Path('/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3/v3_config.json').read_text())
with psycopg.connect(cfg['postgresql']['dsn']) as conn:
    conn.read_only=True
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SHOW transaction_read_only")
        read_only=cur.fetchone()[0]
        cur.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public' AND table_name IN ('transcripts','analyses') AND column_name IN ('attempts','max_attempts','next_attempt_at','lease_owner','lease_until','lease_epoch','last_error_type','last_error','checkpoint_json','updated_at') ORDER BY table_name,column_name")
        columns=cur.fetchall()
        cur.execute("SELECT status,count(*) FROM transcripts GROUP BY status ORDER BY status")
        transcripts=cur.fetchall()
        cur.execute("SELECT status,count(*) FROM analyses GROUP BY status ORDER BY status")
        analyses=cur.fetchall()
        cur.execute("SELECT count(*) FROM analyses WHERE status IN ('PENDING','WAITING_MODEL','RUNNING','RETRY_WAIT')")
        active_analysis=cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM transcripts WHERE status IN ('PENDING','WAITING_TOOL','PAUSED','RUNNING','RETRY_WAIT')")
        active_transcript=cur.fetchone()[0]
print(json.dumps({
 'transaction_read_only':read_only,
 'durable_columns_present':[{'table':a,'column':b} for a,b in columns],
 'transcript_status_counts':dict(transcripts),
 'analysis_status_counts':dict(analyses),
 'current_actionable_transcript_rows':active_transcript,
 'current_actionable_analysis_rows':active_analysis,
},ensure_ascii=False,indent=2))
