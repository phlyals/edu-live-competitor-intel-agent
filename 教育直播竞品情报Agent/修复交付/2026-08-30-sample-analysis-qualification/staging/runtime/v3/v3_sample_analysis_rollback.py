#!/usr/bin/env python3
"""Restore the pre-migration row snapshot before correction delivery occurs.

DDL columns and indexes are intentionally retained.  If a correction outbox may
already have reached Feishu, physical rollback is refused: an external delivery
fact can only be reversed by a new compensating projection.
"""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
import v3_runtime
from v3_sample_analysis_migration import MIGRATION_KEY, MIGRATION_VERSION, DDL_PATH, canonical_json, database_identity


def rollback(backup_path: Path, expected_backup_sha256: str, dsn: str | None = None) -> dict:
    if not backup_path.is_file():raise RuntimeError('backup file missing')
    if backup_path.stat().st_mode & 0o077:
        raise RuntimeError('backup permissions are not private (expected mode 0600)')
    actual=hashlib.sha256(backup_path.read_bytes()).hexdigest()
    if actual!=expected_backup_sha256:raise RuntimeError(f'backup hash mismatch: {actual}')
    backup=json.loads(backup_path.read_text(encoding='utf-8'))
    plan=backup.get('plan') or {}
    if plan.get('migration_key')!=MIGRATION_KEY or plan.get('migration_version')!=MIGRATION_VERSION:
        raise RuntimeError('backup migration identity mismatch')
    if plan.get('ddl_sha256')!=hashlib.sha256(DDL_PATH.read_bytes()).hexdigest():
        raise RuntimeError('backup DDL hash does not match the installed rollback contract')
    sample_ids=[row['analysis_id'] for row in backup['plan']['sample_actions']]
    conn=psycopg.connect(dsn or v3_runtime._postgres_dsn(),autocommit=False,row_factory=dict_row)
    conn.isolation_level=IsolationLevel.SERIALIZABLE
    with conn,conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('edu_sample_analysis_qualification'))")
        current_identity=database_identity(cur)
        expected_identity=backup.get('database_identity') or {}
        identity_keys=('database_name','database_user','server_address','server_port','server_version_num','system_identifier')
        if any(str(current_identity.get(key))!=str(expected_identity.get(key)) for key in identity_keys):
            raise RuntimeError('backup database identity does not match the connected database')
        if sample_ids:
            cur.execute("SELECT outbox_id,object_id,status,attempts,last_attempt_at FROM outbox WHERE object_type='semantic_projection' AND object_id=ANY(%s) AND payload_json::jsonb->>'correction_version'='1'",(sample_ids,))
            correction=[dict(row) for row in cur.fetchall()]
            correction_ids=[str(row['object_id']) for row in correction]
            if len(correction)!=len(sample_ids) or sorted(correction_ids)!=sorted(sample_ids) or len(set(correction_ids))!=len(correction_ids):
                raise RuntimeError('correction set is missing, duplicated, or drifted; use a compensating projection instead of physical rollback')
            unsafe=[row for row in correction if row['status']!='PENDING' or int(row.get('attempts') or 0)!=0 or row.get('last_attempt_at') is not None]
            if correction:
                cur.execute("SELECT outbox_id FROM delivery_receipts WHERE outbox_id=ANY(%s)",([row['outbox_id'] for row in correction],))
                receipts=[row['outbox_id'] for row in cur.fetchall()]
            else:receipts=[]
            if unsafe or receipts:
                raise RuntimeError('correction may already be externally visible; use a compensating projection instead of physical rollback')
            if correction:
                cur.execute("DELETE FROM outbox WHERE outbox_id=ANY(%s)",([row['outbox_id'] for row in correction],))
            cur.execute("DELETE FROM domain_events WHERE event_type='SAMPLE_ANALYSIS_INVALIDATED' AND object_type='analysis' AND object_id=ANY(%s)",(sample_ids,))
        cur.execute("DROP TRIGGER IF EXISTS trg_v3_guard_analysis_immutable_identity ON analyses")
        rows=backup['rows']
        for row in rows['transcripts']:
            cur.execute("UPDATE transcripts SET source_digest=%s,engine=%s,model=%s,status=%s,language=%s,source_path=%s,output_path=%s,low_confidence_count=%s,created_at=%s,metadata_json=%s,scope='UNCLASSIFIED',qualification_status='UNCLASSIFIED' WHERE transcript_id=%s",(row['source_digest'],row['engine'],row['model'],row['status'],row.get('language'),row.get('source_path'),row.get('output_path'),row['low_confidence_count'],row['created_at'],row['metadata_json'],row['transcript_id']))
        for row in rows['analyses']:
            cur.execute("UPDATE analyses SET session_id=%s,analysis_type=%s,source_digest=%s,status=%s,output_path=%s,lineage_state=%s,metadata_json=%s,transcript_id=NULL,scope='UNCLASSIFIED',qualification_status='UNCLASSIFIED',transcript_content_digest=NULL,analysis_spec_version=NULL,model_version=NULL,prompt_version=NULL,artifact_digest=NULL WHERE analysis_id=%s",(row['session_id'],row['analysis_type'],row['source_digest'],row['status'],row.get('output_path'),row['lineage_state'],row['metadata_json'],row['analysis_id']))
        for row in rows['lineage_edges']:
            cur.execute("UPDATE lineage_edges SET downstream_type=%s,downstream_id=%s,upstream_type=%s,upstream_id=%s,upstream_version=%s,state=%s,created_at=%s WHERE edge_id=%s",(row['downstream_type'],row['downstream_id'],row['upstream_type'],row['upstream_id'],row['upstream_version'],row['state'],row['created_at'],row['edge_id']))
        for row in rows['evidence_bundles']:
            cur.execute("UPDATE evidence_bundles SET object_type=%s,object_id=%s,status=%s,manifest_path=%s,manifest_hash=%s,verified_at=%s,metadata_json=%s,scope='UNCLASSIFIED',qualification_status='UNCLASSIFIED' WHERE bundle_id=%s",(row['object_type'],row['object_id'],row['status'],row.get('manifest_path'),row.get('manifest_hash'),row.get('verified_at'),row['metadata_json'],row['bundle_id']))
        for row in rows['outbox']:
            cur.execute("UPDATE outbox SET scope='UNCLASSIFIED',qualification_status='UNCLASSIFIED' WHERE outbox_id=%s",(row['outbox_id'],))
        for row in rows['delivery_receipts']:
            cur.execute("UPDATE delivery_receipts SET scope='UNCLASSIFIED',qualification_status='UNCLASSIFIED' WHERE receipt_id=%s",(row['receipt_id'],))
        cur.execute("SELECT session_id,analysis_type,source_digest,count(*) AS n FROM analyses GROUP BY session_id,analysis_type,source_digest HAVING count(*)>1")
        legacy_unique_conflicts=[dict(row) for row in cur.fetchall()]
        if legacy_unique_conflicts:
            raise RuntimeError('cannot restore legacy analysis unique constraint: '+canonical_json(legacy_unique_conflicts))
        cur.execute("SELECT count(*) AS n FROM pg_constraint WHERE conrelid='analyses'::regclass AND conname='analyses_session_id_analysis_type_source_digest_key'")
        if cur.fetchone()['n']==0:
            cur.execute("ALTER TABLE analyses ADD CONSTRAINT analyses_session_id_analysis_type_source_digest_key UNIQUE(session_id,analysis_type,source_digest)")
        cur.execute("DELETE FROM schema_meta WHERE key=%s",(MIGRATION_KEY,))
        prior=rows.get('schema_meta') or []
        for row in prior:
            cur.execute("INSERT INTO schema_meta(key,value,updated_at) VALUES(%s,%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",(row['key'],row['value'],row.get('updated_at')))
        return {'status':'ROLLED_BACK_ROWS','backup_sha256':actual,'correction_outboxes_removed':len(correction) if sample_ids else 0,'ddl_columns_retained':True,'legacy_unique_constraint_restored':True,'external_compensation_required':False}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--backup',type=Path,required=True);p.add_argument('--expected-backup-sha256',required=True);p.add_argument('--dsn',help=argparse.SUPPRESS);a=p.parse_args()
    print(json.dumps(rollback(a.backup,a.expected_backup_sha256,a.dsn),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
