#!/usr/bin/env python3
"""Destructive integration test against a disposable PostgreSQL database only."""
from __future__ import annotations
import json, os, uuid
from pathlib import Path
import psycopg
from psycopg import conninfo, sql
from psycopg.rows import dict_row
import v3_runtime
import v3_sample_analysis_migration as migration
import v3_sample_analysis_rollback as rollback_module
from v3_db import PostgresConnection
import v3_evidence_worker as evidence_worker
from unittest.mock import patch

OLD_SCHEMA=Path('/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3/v3_schema.sql')
SAMPLE_ANALYSES=['analysis_8801cb766ceca4ab6327511a','analysis_f261f8101b4ea725a954c23f']
FORMAL_ANALYSIS='analysis_16bdca57a497943fd6eb0c05'
ANALYSES=SAMPLE_ANALYSES+[FORMAL_ANALYSIS]

def dsn_for(base,name):
    params=conninfo.conninfo_to_dict(base);params['dbname']=name
    return conninfo.make_conninfo(**params)

def rows(cur,query,params=()):
    cur.execute(query,params);return [dict(r) for r in cur.fetchall()]

def main():
    base=v3_runtime._postgres_dsn(); params=conninfo.conninfo_to_dict(base)
    dbname='edu_sample_qual_test_'+uuid.uuid4().hex[:10]
    admin=psycopg.connect(dsn_for(base,'postgres'),autocommit=True)
    source=psycopg.connect(base,row_factory=dict_row);source.read_only=True
    report={'temporary_database':dbname,'production_writes':0}
    try:
        with admin.cursor() as c:c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))
        test_dsn=dsn_for(base,dbname)
        pg=PostgresConnection(test_dsn)
        try:pg.executescript(OLD_SCHEMA.read_text());pg.commit()
        finally:pg.close()
        with source,source.cursor() as c:
            c.execute('SHOW transaction_read_only');assert c.fetchone()['transaction_read_only']=='on'
            analyses=rows(c,'SELECT * FROM analyses WHERE analysis_id=ANY(%s)',(ANALYSES,))
            transcript_ids=[]
            for a in analyses:
                p=Path(str(a.get('output_path') or ''))
                if p.is_file():transcript_ids.append(json.loads(p.read_text())['transcript_id'])
            transcripts=rows(c,'SELECT * FROM transcripts WHERE transcript_id=ANY(%s)',(transcript_ids,))
            sessions=rows(c,'SELECT * FROM live_sessions WHERE session_id=ANY(%s)',([t['session_id'] for t in transcripts],))
            lineages=rows(c,"SELECT * FROM lineage_edges WHERE downstream_type='analysis' AND downstream_id=ANY(%s) AND upstream_type='transcript'",(ANALYSES,))
            bundles=rows(c,"SELECT * FROM evidence_bundles WHERE object_type='analysis' AND object_id=ANY(%s)",(ANALYSES,))
            segments=rows(c,"SELECT * FROM recording_segments WHERE segment_id=ANY(%s)",([json.loads(t['metadata_json'] or '{}').get('source_segment_id') for t in transcripts],))
        target_ids={s['monitor_target_id'] for s in sessions}
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            for i,target_id in enumerate(target_ids):
                competitor_id=f'c{i}'
                c.execute("INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at) VALUES(%s,'test',%s,'test','2026-01-01','2026-01-01')",(competitor_id,competitor_id))
                c.execute("INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,live_url,live_status,metadata_json) VALUES(%s,%s,'ACTIVE',%s,'OFFLINE_CONFIRMED','{}')",(target_id,competitor_id,f'https://example.invalid/{i}'))
            for s in sessions:
                c.execute("INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,ended_at,completeness,source_url,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",(s['session_id'],s['monitor_target_id'],s['platform_session_id'],s['status'],s['started_at'],s['ended_at'],s['completeness'],'https://example.invalid/live',s['metadata_json']))
            for seg in segments:
                c.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,captured_to,status,bytes,lifecycle_status,superseded_by_segment_id,lifecycle_updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",tuple(seg[k] for k in ('segment_id','session_id','path','checksum','captured_from','captured_to','status','bytes','lifecycle_status','superseded_by_segment_id','lifecycle_updated_at')))
            for t in transcripts:
                c.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,language,source_path,output_path,low_confidence_count,created_at,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",tuple(t.get(k) for k in ('transcript_id','session_id','source_digest','engine','model','status','language','source_path','output_path','low_confidence_count','created_at','metadata_json')))
            for a in analyses:
                c.execute("INSERT INTO analyses(analysis_id,session_id,analysis_type,source_digest,status,output_path,lineage_state,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",tuple(a[k] for k in ('analysis_id','session_id','analysis_type','source_digest','status','output_path','lineage_state','metadata_json')))
            for e in lineages:
                c.execute("INSERT INTO lineage_edges(edge_id,downstream_type,downstream_id,upstream_type,upstream_id,upstream_version,state,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",tuple(e[k] for k in ('edge_id','downstream_type','downstream_id','upstream_type','upstream_id','upstream_version','state','created_at')))
            for b in bundles:
                c.execute("INSERT INTO evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,verified_at,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",tuple(b[k] for k in ('bundle_id','object_type','object_id','status','manifest_path','manifest_hash','verified_at','metadata_json')))
            for i,aid in enumerate(ANALYSES):
                oid=f'old-out-{i}';payload=json.dumps({'analysis_id':aid})
                ph=migration.digest({'analysis_id':aid})
                c.execute("INSERT INTO outbox(outbox_id,dedupe_key,object_type,object_id,destination,status,attempts,max_attempts,next_attempt_at,payload_hash,payload_json,receipt_json) VALUES(%s,%s,'semantic_projection',%s,'feishu_base','SENT',1,8,'2026-08-30T00:00:00Z',%s,%s,'{}')",(oid,'old:'+aid,aid,ph,payload))
                c.execute("INSERT INTO delivery_receipts(receipt_id,outbox_id,destination,object_type,object_id,status,payload_hash,sent_at,verified_at,receipt_json) VALUES(%s,%s,'feishu_base','semantic_projection',%s,'VERIFIED',%s,'2026-08-30T00:00:00Z','2026-08-30T00:00:00Z','{}')",('receipt:'+oid,oid,aid,ph))
        plan=migration.readonly_plan(test_dsn)
        report['dry_run']={'read_only':plan['transaction_read_only'],'sample_actions':len(plan['sample_actions']),'formal_annotations':len(plan['formal_annotations']),'issues':plan['issues'],'plan_sha256':plan['plan_sha256']}
        try:migration.apply('0'*64,Path('/tmp/never-created'),test_dsn)
        except RuntimeError as exc:report['wrong_hash_rollback']=str(exc).startswith('plan changed:')
        backup=Path('/Users/mac/Documents/agent架构师/教育直播竞品情报Agent/修复交付/2026-08-30-sample-analysis-qualification/pg-integration-backup')
        applied=migration.apply(plan['plan_sha256'],backup,test_dsn);report['first_apply']=applied
        report['backup_mode']=oct(Path(applied['backup_path']).stat().st_mode & 0o777)
        report['first_idempotent']=migration.apply(plan['plan_sha256'],backup,test_dsn)['status']
        report['rollback']=rollback_module.rollback(Path(applied['backup_path']),applied['backup_sha256'],test_dsn)
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            c.execute("SELECT analysis_id,status,lineage_state FROM analyses WHERE analysis_id=ANY(%s) ORDER BY analysis_id",(ANALYSES,));report['rollback_rows']=[dict(r) for r in c.fetchall()]
            c.execute("SELECT count(*) AS n FROM pg_constraint WHERE conrelid='analyses'::regclass AND conname='analyses_session_id_analysis_type_source_digest_key'");report['rollback_legacy_unique_restored']=c.fetchone()['n']==1
        plan_after_rollback=migration.readonly_plan(test_dsn)
        reapplied=migration.apply(plan_after_rollback['plan_sha256'],backup,test_dsn);report['reapply_after_rollback']=reapplied
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            c.execute("SELECT count(*) AS n FROM pg_constraint WHERE conrelid='analyses'::regclass AND conname='analyses_session_id_analysis_type_source_digest_key'");report['reapply_legacy_unique_removed']=c.fetchone()['n']==0
        report['idempotent_after_reapply']=migration.apply(plan_after_rollback['plan_sha256'],backup,test_dsn)['status']
        with patch.object(evidence_worker,'connect',side_effect=lambda:PostgresConnection(test_dsn)),patch.object(evidence_worker,'init_db'),patch.object(evidence_worker,'upsert_heartbeat'):
            report['evidence_reverification']=evidence_worker.once()
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            c.execute("SELECT analysis_id,status,lineage_state,scope,qualification_status,transcript_id,transcript_content_digest,artifact_digest FROM analyses ORDER BY analysis_id")
            report['analyses']=[dict(r) for r in c.fetchall()]
            c.execute("SELECT object_id,status,scope,qualification_status FROM outbox WHERE object_type='semantic_projection' ORDER BY object_id,status")
            report['outbox']=[dict(r) for r in c.fetchall()]
            c.execute("SELECT object_id,status,scope,qualification_status FROM delivery_receipts ORDER BY object_id")
            report['receipts']=[dict(r) for r in c.fetchall()]
            try:
                c.execute("UPDATE analyses SET transcript_content_digest='tampered' WHERE analysis_id=%s",(FORMAL_ANALYSIS,))
            except psycopg.Error:
                tc.rollback();report['immutable_trigger']=True
            else:report['immutable_trigger']=False
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            c.execute("SELECT * FROM analyses WHERE analysis_id=%s",(FORMAL_ANALYSIS,));a=dict(c.fetchone());a['analysis_id']='duplicate'
            try:
                c.execute("INSERT INTO analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,artifact_digest,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",tuple(a[k] for k in ('analysis_id','session_id','transcript_id','analysis_type','source_digest','status','lineage_state','scope','qualification_status','transcript_content_digest','analysis_spec_version','model_version','prompt_version','artifact_digest','metadata_json')))
            except psycopg.errors.UniqueViolation:
                tc.rollback();report['unique_transcript_spec']=True
            else:report['unique_transcript_spec']=False
        with psycopg.connect(test_dsn,row_factory=dict_row) as tc,tc.cursor() as c:
            c.execute("SELECT outbox_id,object_id FROM outbox WHERE payload_json::jsonb->>'correction_version'='1' ORDER BY outbox_id LIMIT 1");drift=dict(c.fetchone())
            c.execute("UPDATE outbox SET object_id='drifted-missing-analysis' WHERE outbox_id=%s",(drift['outbox_id'],))
        try:
            rollback_module.rollback(Path(reapplied['backup_path']),reapplied['backup_sha256'],test_dsn)
        except RuntimeError as exc:
            report['rollback_missing_correction_refused']='missing, duplicated, or drifted' in str(exc)
        else:
            report['rollback_missing_correction_refused']=False
        with psycopg.connect(test_dsn) as tc,tc.cursor() as c:
            c.execute("UPDATE outbox SET object_id=%s WHERE outbox_id=%s",(drift['object_id'],drift['outbox_id']))
        with psycopg.connect(test_dsn) as tc,tc.cursor() as c:
            c.execute("UPDATE outbox SET status='RETRY',attempts=1,last_attempt_at='2026-08-30T00:00:00Z' WHERE payload_json::jsonb->>'correction_version'='1'")
        try:
            rollback_module.rollback(Path(reapplied['backup_path']),reapplied['backup_sha256'],test_dsn)
        except RuntimeError as exc:
            report['rollback_after_attempt_refused']='compensating projection' in str(exc)
        else:
            report['rollback_after_attempt_refused']=False
        print(json.dumps(report,ensure_ascii=False,indent=2,default=str))
    finally:
        source.close()
        with admin.cursor() as c:
            c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s AND pid<>pg_backend_pid()",(dbname,))
            c.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(dbname)))
        admin.close()

if __name__=='__main__':main()
