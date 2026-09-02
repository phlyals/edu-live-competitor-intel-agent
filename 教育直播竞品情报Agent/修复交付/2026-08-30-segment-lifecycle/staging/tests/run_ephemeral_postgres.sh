#!/bin/zsh
set -euo pipefail

ROOT="${0:A:h:h}"
WORK="/tmp/segment-lifecycle-pg-$$"
mkdir -p "$WORK"
DATA="$WORK/data"
SOCKET="$WORK/socket"
BACKUP="$WORK/backups"
mkdir -p "$SOCKET" "$BACKUP"
PORT=$((56000 + $$ % 5000))
cleanup() {
  /opt/homebrew/bin/pg_ctl -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

/opt/homebrew/bin/initdb -D "$DATA" -A trust -U postgres --no-locale --encoding=UTF8 >/dev/null
/opt/homebrew/bin/pg_ctl -D "$DATA" -l "$WORK/postgres.log" -o "-k $SOCKET -p $PORT -F" -w start >/dev/null
/opt/homebrew/bin/createdb -h "$SOCKET" -p "$PORT" -U postgres segment_lifecycle_test
export SEGMENT_MIGRATION_DSN="host=$SOCKET port=$PORT user=postgres dbname=segment_lifecycle_test"
export SEGMENT_FIXTURE_ROOT="$WORK/fixture"
mkdir -p "$SEGMENT_FIXTURE_ROOT"

PYTHON=/Users/mac/.local/share/edu-live-runtime-v3-venv/bin/python
$PYTHON - <<'PY'
import hashlib,json,os
from pathlib import Path
import psycopg
root=Path(os.environ['SEGMENT_FIXTURE_ROOT'])
final=root/'整场直播.ts'; final.write_bytes(b'canonical')
missing=root/'partial.refresh.ts.partial'
manifest={'session_id':'s','final_path':str(final),'sha256':hashlib.sha256(final.read_bytes()).hexdigest(),'retained_sources':[{'path':str(root/'completed.refresh.ts.partial'),'original_path':str(missing),'sha256':hashlib.sha256(b'missing').hexdigest()}]}
mp=root/'media-manifest.json';mp.write_text(json.dumps(manifest))
with psycopg.connect(os.environ['SEGMENT_MIGRATION_DSN']) as c:
 c.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT)")
 c.execute("CREATE TABLE recording_segments(segment_id TEXT PRIMARY KEY,session_id TEXT,path TEXT,checksum TEXT,status TEXT,bytes BIGINT,UNIQUE(session_id,path))")
 c.execute("CREATE TABLE media_manifests(session_id TEXT PRIMARY KEY,status TEXT,manifest_path TEXT,manifest_hash TEXT)")
 c.execute("CREATE TABLE transcripts(transcript_id TEXT PRIMARY KEY,status TEXT,source_path TEXT,output_path TEXT,metadata_json TEXT)")
 c.execute("CREATE TABLE recording_jobs(session_id TEXT PRIMARY KEY,partial_dir TEXT,completed_dir TEXT)")
 c.execute("INSERT INTO recording_segments VALUES(%s,%s,%s,%s,%s,%s)",('canonical','s',str(final),manifest['sha256'],'COMPLETE',len(final.read_bytes())))
 c.execute("INSERT INTO recording_segments VALUES(%s,%s,%s,%s,%s,%s)",('old','s',str(missing),manifest['retained_sources'][0]['sha256'],'COMPLETE',7))
 c.execute("INSERT INTO media_manifests VALUES(%s,%s,%s,%s)",('s','VERIFIED',str(mp),hashlib.sha256(mp.read_bytes()).hexdigest()))
 c.execute("INSERT INTO transcripts VALUES(%s,%s,%s,%s,%s)",('t','WAITING_TOOL',str(missing),None,json.dumps({'segment_id':'old','preserve':True,'reason':'audio extraction/duration validation failed'})))
 c.execute("INSERT INTO recording_jobs VALUES(%s,%s,%s)",('s',str(root/'partial'),str(root)))
PY

MIGRATION="$ROOT/migration/segment_lifecycle_migration.py"
$PYTHON "$MIGRATION" --report "$ROOT/test-results/pg-dry-run.json" >/dev/null

# Dry-run must not evolve the schema.
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT count(*) FROM information_schema.columns WHERE table_name='recording_segments' AND column_name='lifecycle_status'" | grep -qx 0

PLAN_SHA="$($PYTHON -c 'import json,sys;print(json.load(open(sys.argv[1]))["plan_sha256"])' "$ROOT/test-results/pg-dry-run.json")"
if $PYTHON "$MIGRATION" --apply --full-media-hash --expected-database segment_lifecycle_test --backup-dir "$BACKUP" --expected-plan-sha256 deadbeef >/dev/null 2>&1; then
  echo "wrong plan digest unexpectedly applied" >&2
  exit 1
fi
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT count(*) FROM information_schema.columns WHERE table_name='recording_segments' AND column_name='lifecycle_status'" | grep -qx 0
$PYTHON "$MIGRATION" --apply --full-media-hash --expected-database segment_lifecycle_test --backup-dir "$BACKUP" --expected-plan-sha256 "$PLAN_SHA" --report "$ROOT/test-results/pg-apply-1.json" >/dev/null
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT segment_id||':'||lifecycle_status||':'||coalesce(superseded_by_segment_id,'') FROM recording_segments ORDER BY segment_id" > "$WORK/snapshot1"
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT transcript_id||':'||status||':'||metadata_json FROM transcripts ORDER BY transcript_id" >> "$WORK/snapshot1"

$PYTHON "$MIGRATION" --apply --full-media-hash --expected-database segment_lifecycle_test --backup-dir "$BACKUP" --expected-plan-sha256 "$PLAN_SHA" --report "$ROOT/test-results/pg-apply-2.json" >/dev/null
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT segment_id||':'||lifecycle_status||':'||coalesce(superseded_by_segment_id,'') FROM recording_segments ORDER BY segment_id" > "$WORK/snapshot2"
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -Atc "SELECT transcript_id||':'||status||':'||metadata_json FROM transcripts ORDER BY transcript_id" >> "$WORK/snapshot2"
cmp "$WORK/snapshot1" "$WORK/snapshot2"
grep -q 'canonical:CANONICAL_ACTIVE:' "$WORK/snapshot1"
grep -q 'old:SOURCE_SUPERSEDED:canonical' "$WORK/snapshot1"
grep -q 't:CANCELLED_SUPERSEDED_SOURCE:' "$WORK/snapshot1"
test "$(find "$BACKUP" -type f | wc -l | tr -d ' ')" -eq 1
grep -q 'ALREADY_APPLIED' "$ROOT/test-results/pg-apply-2.json"
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -v ON_ERROR_STOP=1 -f "$ROOT/sql/001_recording_segment_lifecycle.sql" >/dev/null
/opt/homebrew/bin/psql "$SEGMENT_MIGRATION_DSN" -v ON_ERROR_STOP=1 -f "$ROOT/sql/001_recording_segment_lifecycle.sql" >/dev/null
test "$(stat -f '%Lp' "$(find "$BACKUP" -type f | head -1)")" = 600
echo '{"status":"PASS","dry_run_read_only":true,"plan_digest_gate":true,"apply_idempotent":true,"ddl_idempotent":true,"backup_mode":"0600"}' > "$ROOT/test-results/pg-integration-summary.json"
