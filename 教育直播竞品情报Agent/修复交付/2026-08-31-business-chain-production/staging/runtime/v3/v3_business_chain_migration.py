#!/usr/bin/env python3
"""Migration 006 for the comparison/version/strategy/knowledge chain."""
from __future__ import annotations
import argparse, hashlib, json, os
from datetime import datetime, timezone
from pathlib import Path
import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
import v3_runtime

ROOT=Path(__file__).resolve().parent;DDL_PATH=ROOT/'migrations/006_business_chain.sql'
KEY='business_chain_revision';VERSION='1'
TABLES=('comparisons','comparison_evidence','strategy_versions','version_observations','strategy_evidence','knowledge_publish_receipts')
INDEXES={'idx_comparisons_competitor_newer','idx_comparison_evidence_analysis','idx_version_observations_competitor','idx_strategy_versions_active','idx_strategy_evidence_candidate','idx_knowledge_publish_receipts_candidate'}
SC={'strategy_candidates':{'competitor_id','version_id','comparison_id','candidate_digest','evidence_json','updated_at'},'knowledge_diffs':{'competitor_id','version_id','candidate_digest','approval_id','updated_at'}}
def now():return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
def canon(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str)
def dg(v):return hashlib.sha256(canon(v).encode()).hexdigest()
def ddl():return '\n'.join(x for x in DDL_PATH.read_text().splitlines() if x.strip().upper() not in {'BEGIN;','COMMIT;'})
def exists(c,t):c.execute('select to_regclass(%s) as n',('public.'+t,));return c.fetchone()['n'] is not None
def state(c):
 missing_tables=[t for t in TABLES if not exists(c,t)];missing_cols={}
 for t,want in SC.items():
  c.execute("select column_name from information_schema.columns where table_schema='public' and table_name=%s",(t,));miss=sorted(want-{r['column_name'] for r in c.fetchall()})
  if miss:missing_cols[t]=miss
 return {'missing_tables':missing_tables,'missing_columns':missing_cols}
def plan(c):
 rows={}
 for t in ('strategy_candidates','knowledge_versions','knowledge_diffs','approvals'):
  c.execute(f'select * from {t} order by 1');rows[t]=[dict(r) for r in c.fetchall()]
 issues=[]
 c.execute("select analysis_id from analyses where status in ('RUNNING','PENDING_RECOMPUTE','WAITING_MODEL','RETRY_WAIT')");active=[r['analysis_id'] for r in c.fetchall()]
 if active:issues.append({'reason':'analysis active','ids':active})
 core={'key':KEY,'version':VERSION,'ddl_sha256':hashlib.sha256(DDL_PATH.read_bytes()).hexdigest(),'schema':state(c),'rows':rows,'issues':issues}
 return {**core,'plan_sha256':dg(core)}
def health(c):
 s=state(c);c.execute("select indexname from pg_indexes where schemaname='public' and indexname=any(%s)",(list(INDEXES),));idx={r['indexname'] for r in c.fetchall()};c.execute('select value from schema_meta where key=%s',(KEY,));r=c.fetchone();marker=r['value'] if r else None;errors=[]
 if not s['missing_tables']:
  c.execute("select competitor_id,count(*) n from strategy_versions where status='ACTIVE' group by competitor_id having count(*)>1");errors += [{'reason':'multiple active versions',**dict(r)} for r in c.fetchall()]
  c.execute("select candidate_id from strategy_candidates where candidate_digest is not null and (status not in ('PENDING_REVIEW','APPROVED','REJECTED') or competitor_id is null or version_id is null)");errors += [{'reason':'invalid candidate','id':r['candidate_id']} for r in c.fetchall()]
  c.execute("select p.publish_receipt_id from knowledge_publish_receipts p join approvals a on a.approval_id=p.approval_id where a.decision<>'APPROVED' or p.status<>'VERIFIED'");errors += [{'reason':'invalid publish receipt','id':r['publish_receipt_id']} for r in c.fetchall()]
 ok=not s['missing_tables'] and not s['missing_columns'] and idx==INDEXES and marker==VERSION and not errors
 return {'healthy':ok,'schema':s,'indexes':sorted(idx),'marker':marker,'errors':errors}
def readonly(dsn=None):
 c=psycopg.connect(dsn or v3_runtime._postgres_dsn(),row_factory=dict_row);c.read_only=True;c.isolation_level=IsolationLevel.SERIALIZABLE
 with c,c.cursor() as x:x.execute('show transaction_read_only');ro=x.fetchone()['transaction_read_only'];return {**plan(x),'transaction_read_only':ro}
def apply(expected,backup_dir,dsn=None):
 c=psycopg.connect(dsn or v3_runtime._postgres_dsn(),row_factory=dict_row);c.isolation_level=IsolationLevel.SERIALIZABLE
 with c,c.cursor() as x:
  x.execute("select pg_advisory_xact_lock(hashtext('edu_live_v3_business_chain_v1'))");x.execute('select value from schema_meta where key=%s',(KEY,));m=x.fetchone()
  if m and m['value']==VERSION:
   h=health(x)
   if not h['healthy']:raise RuntimeError('migration drift '+canon(h))
   return {'status':'ALREADY_APPLIED','health':h}
  p=plan(x)
  if p['issues'] or p['plan_sha256']!=expected:raise RuntimeError('plan changed or blocked')
  backup_dir.mkdir(parents=True,exist_ok=True);os.chmod(backup_dir,0o700);rows={}
  for t in ('strategy_candidates','knowledge_versions','knowledge_diffs','approvals','schema_meta'):
   x.execute(f'select * from {t}');rows[t]=[dict(r) for r in x.fetchall()]
  path=backup_dir/('pre-business-chain-'+datetime.now().strftime('%Y%m%d-%H%M%S-%f')+'.json');path.write_text(json.dumps({'plan':p,'rows':rows},ensure_ascii=False,indent=2,default=str)+'\n');os.chmod(path,0o600);sha=hashlib.sha256(path.read_bytes()).hexdigest();x.execute(ddl());h=health(x)
  if not h['healthy']:raise RuntimeError('postcondition '+canon(h))
  return {'status':'APPLIED','plan_sha256':p['plan_sha256'],'backup_path':str(path),'backup_sha256':sha,'postconditions':h}
def main():
 q=argparse.ArgumentParser();q.add_argument('--apply',action='store_true');q.add_argument('--expected-plan-sha256');q.add_argument('--backup-dir',type=Path);q.add_argument('--dsn',help=argparse.SUPPRESS);a=q.parse_args();r=apply(a.expected_plan_sha256,a.backup_dir,a.dsn) if a.apply else readonly(a.dsn);print(json.dumps(r,ensure_ascii=False,indent=2,default=str));return 0
if __name__=='__main__':raise SystemExit(main())
