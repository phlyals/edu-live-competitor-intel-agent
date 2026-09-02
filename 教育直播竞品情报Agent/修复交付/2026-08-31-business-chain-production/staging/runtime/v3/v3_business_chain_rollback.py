#!/usr/bin/env python3
"""Rollback migration 006 before qualified business-chain activity exists."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
import v3_runtime
from v3_business_chain_migration import KEY,TABLES
COLS={'strategy_candidates':('competitor_id','version_id','comparison_id','candidate_digest','evidence_json','updated_at'),'knowledge_diffs':('competitor_id','version_id','candidate_digest','approval_id','updated_at')}
def rollback(path,expected,dsn=None):
 if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:raise RuntimeError('backup missing or hash mismatch')
 b=json.loads(path.read_text());c=psycopg.connect(dsn or v3_runtime._postgres_dsn(),row_factory=dict_row);c.isolation_level=IsolationLevel.SERIALIZABLE
 with c,c.cursor() as x:
  x.execute("select pg_advisory_xact_lock(hashtext('edu_live_v3_business_chain_v1'))")
  activity={}
  for t in TABLES:x.execute(f'select count(*) n from {t}');activity[t]=x.fetchone()['n']
  if any(activity.values()):raise RuntimeError('business-chain activity exists; restore full dump')
  x.execute('drop trigger if exists trg_v3_guard_comparison_identity on comparisons');x.execute('drop trigger if exists trg_v3_guard_strategy_version_identity on strategy_versions');x.execute('drop trigger if exists trg_v3_guard_knowledge_publish_receipt on knowledge_publish_receipts')
  for t,idcol in (('strategy_candidates','candidate_id'),('knowledge_diffs','diff_id')):
   old={r[idcol]:r for r in b['rows'][t]};x.execute(f'select {idcol} from {t}');current={r[idcol] for r in x.fetchall()}
   if current!=set(old):raise RuntimeError(t+' row set changed')
   for oid,row in old.items():
    cols=COLS[t];vals=[row.get(k) for k in cols];
    if 'evidence_json' in cols:vals[cols.index('evidence_json')]=vals[cols.index('evidence_json')] or '{}'
    x.execute(f"update {t} set "+','.join(k+'=%s' for k in cols)+f' where {idcol}=%s',(*vals,oid))
  x.execute('delete from schema_meta where key=%s',(KEY,))
  for row in b['rows']['schema_meta']:
   if row['key']==KEY:x.execute('insert into schema_meta(key,value,updated_at) values(%s,%s,%s)',(row['key'],row['value'],row.get('updated_at')))
  for name in ('trg_v3_guard_comparison_identity','trg_v3_guard_strategy_version_identity','trg_v3_guard_knowledge_publish_receipt'):pass
  for t in reversed(TABLES):x.execute(f'drop table {t} cascade')
  x.execute('drop function if exists v3_guard_comparison_identity()');x.execute('drop function if exists v3_guard_strategy_version_identity()');x.execute('drop function if exists v3_guard_knowledge_publish_receipt()')
  return {'status':'ROLLED_BACK_ROWS','activity':activity,'columns_retained':True}
def main():
 p=argparse.ArgumentParser();p.add_argument('--backup',type=Path,required=True);p.add_argument('--expected-backup-sha256',required=True);p.add_argument('--dsn',help=argparse.SUPPRESS);a=p.parse_args();print(json.dumps(rollback(a.backup,a.expected_backup_sha256,a.dsn),ensure_ascii=False,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
