"""Atomic immutable scan import; names are labels, never identity keys."""
from __future__ import annotations
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs


def scan_identity(product_id: str, result_digest: str, task_id: str | None = None) -> str:
    return 'scan:'+hashlib.sha256(f'{product_id}:{task_id or ""}:{result_digest}'.encode()).hexdigest()[:40]


def canonical_monitor_url(value: str) -> bool:
    u=urlparse(value)
    if u.scheme!='https' or u.username or u.password or u.query or u.fragment or u.port not in {None,443}:return False
    return bool((u.hostname=='live.douyin.com' and re.fullmatch(r'/[A-Za-z0-9_.-]+',u.path)) or (u.hostname in {'www.douyin.com','douyin.com'} and re.fullmatch(r'/(?:share/)?user/[A-Za-z0-9_-]+',u.path)))


def validate_scan(result: dict) -> tuple[str, list[dict], dict[str,dict]]:
    product=result.get('product') or {}; summary=result.get('scan_summary') or {}
    pid=str(product.get('target_product_id') or '')
    if not pid.isdigit() or product.get('final_page_product_id')!=pid or product.get('page_verified') is not True:
        raise ValueError('PRODUCT_IDENTITY_NOT_VERIFIED')
    rows=result.get('observations') or []
    if summary.get('status')!='COMPLETE' or summary.get('filter_verified') is not True or summary.get('content_type')!='live':
        raise ValueError('SCAN_SCOPE_NOT_COMPLETE')
    if summary.get('page_reported_result_count')!=len(rows) or not (summary.get('end_signal') or {}).get('verified'):
        raise ValueError('SCAN_COUNT_OR_END_NOT_VERIFIED')
    identities={}
    for item in result.get('unique_creators') or []:
        uid=str(item.get('buyin_creator_uid') or '')
        stable=str(item.get('douyin_stable_id') or '')
        canonical=str(item.get('canonical_profile_url') or '')
        if not uid or not stable or item.get('verification_status')!='VERIFIED' or not canonical_monitor_url(canonical):
            raise ValueError('IDENTITY_CLOSURE_REQUIRED')
        path_id=urlparse(canonical).path.rstrip('/').split('/')[-1]
        if stable not in {'uid:'+path_id,'sec_uid:'+path_id}:
            raise ValueError('DOUYIN_IDENTITY_MISMATCH')
        if item.get('monitor_verified') is not True or not re.fullmatch(r'https://live\.douyin\.com/[A-Za-z0-9_.-]+',str(item.get('monitor_url') or '')):
            raise ValueError('MONITOR_IDENTITY_NOT_VERIFIED')
        if uid in identities and identities[uid]['douyin_stable_id']!=stable:
            raise ValueError('BUYIN_IDENTITY_CONFLICT')
        detail=urlparse(str(item.get('buyin_detail_url') or ''))
        if detail.hostname!='buyin.jinritemai.com' or parse_qs(detail.query).get('uid')!=[uid]:
            raise ValueError('BUYIN_DETAIL_EVIDENCE_MISMATCH')
        probe=item.get('monitor_probe') or {}
        observed=probe.get('platform_user_id') if stable.startswith('uid:') else probe.get('sec_uid')
        same_identity=(str(observed)==path_id) if observed else re.sub(r'\s+','',str(probe.get('anchor_name') or ''))==re.sub(r'\s+','',str(item.get('account_name') or ''))
        if probe.get('status') not in {'LIVE','OFFLINE_CONFIRMED'} or not same_identity:
            raise ValueError('MONITOR_PROBE_IDENTITY_MISMATCH')
        identities[uid]=item
    if any(not row.get('_detail_verified') or row.get('buyin_creator_uid') not in identities for row in rows):
        raise ValueError('IDENTITY_CLOSURE_REQUIRED')
    if set(identities)!={row['buyin_creator_uid'] for row in rows}:
        raise ValueError('IDENTITY_SET_MISMATCH')
    return 'douyin:'+pid,rows,identities


def import_scan(result_path: Path, *, task_id: str | None = None) -> dict:
    import v3_runtime as v3
    raw=result_path.read_bytes(); result=json.loads(raw)
    product_id,rows,identities=validate_scan(result)
    task_id=task_id or result.get('source_task_id')
    result_digest=hashlib.sha256(raw).hexdigest()
    proposed=scan_identity(product_id,result_digest,task_id)
    product=result['product']; summary=result['scan_summary']; now=v3.utc_now()
    with v3.connect() as conn:
        v3._begin(conn)
        existing=conn.execute('SELECT scan_id,product_id FROM scan_runs WHERE result_digest=?',(result_digest,)).fetchone()
        if existing:
            if existing['product_id']!=product_id:raise ValueError('SCAN_DIGEST_PRODUCT_CONFLICT')
            conn.commit()
            return {'scan_id':existing['scan_id'],'product_id':product_id,'observations_imported':len(rows),'identities_imported':len(identities),'reused':True}
        previous_product=conn.execute('SELECT metadata_json FROM products WHERE product_id=?',(product_id,)).fetchone()
        metadata=json.loads(previous_product['metadata_json'] or '{}') if previous_product else {}
        metadata.update(source_result=str(result_path),latest_scan_id=proposed)
        conn.execute("INSERT INTO products(product_id,platform,platform_product_id,title,source_url,first_seen_at,last_seen_at,metadata_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET title=excluded.title,source_url=excluded.source_url,last_seen_at=excluded.last_seen_at,metadata_json=excluded.metadata_json",(product_id,'buyin',product_id.split(':',1)[1],product.get('name') or '',product.get('original_input') or '',now,now,v3.json_text(metadata)))
        conn.execute("INSERT INTO scan_runs(scan_id,task_id,product_id,status,evidence_state,started_at,ended_at,imported_at,filter_label,filter_verified,reported_total,observed_count,result_digest,result_path,manifest_path,payload_json) VALUES(?,?,?,'COMPLETE','COMPLETE',?,?,?,?,1,?,?,?,?,?,?)",(proposed,task_id,product_id,summary.get('started_at'),summary.get('ended_at'),now,summary.get('filter_label'),len(rows),len(rows),result_digest,str(result_path),str(result_path.parent/'scan_manifest.json'),v3.json_text({'summary':summary,'evidence':result.get('evidence') or {}})))
        by_uid={}
        for uid,item in identities.items():
            # A changing Buyin token can still point to the same verified Douyin user.
            known=conn.execute("SELECT competitor_id FROM identities WHERE platform='douyin' AND stable_id=?",(item['douyin_stable_id'],)).fetchone()
            if not known:
                legacy=[r for r in conn.execute("SELECT DISTINCT competitor_id FROM identities WHERE platform='douyin' AND verification_status='VERIFIED' AND canonical_url=?",(item['monitor_url'],))]
                if len(legacy)>1:raise ValueError('IDENTITY_CONFLICT')
                known=legacy[0] if legacy else None
            same_buyin=conn.execute("SELECT competitor_id FROM identities WHERE platform='buyin' AND stable_id=?",(uid,)).fetchone()
            if known and same_buyin and known['competitor_id']!=same_buyin['competitor_id']:
                raise ValueError('IDENTITY_CONFLICT')
            competitor_id=(known or same_buyin)['competitor_id'] if (known or same_buyin) else 'buyin:'+uid
            previous=conn.execute('SELECT competitor_id FROM competitors WHERE competitor_id=?',(competitor_id,)).fetchone()
            if not previous:
                conn.execute("INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at,metadata_json) VALUES(?,?,?,?,?,?,?)",(competitor_id,'buyin',uid,item['account_name'],now,now,v3.json_text({'source_scan':proposed})))
            else:
                conn.execute('UPDATE competitors SET account_name=?,last_seen_at=? WHERE competitor_id=?',(item['account_name'],now,competitor_id))
            by_uid[uid]=competitor_id
            v3._upsert_identity_conn(conn,competitor_id=competitor_id,platform='buyin',stable_id=uid,canonical_url=item['buyin_detail_url'],verification_status='VERIFIED',evidence={**item,'scan_id':proposed})
            douyin_identity_id=v3._upsert_identity_conn(conn,competitor_id=competitor_id,platform='douyin',stable_id=item['douyin_stable_id'],canonical_url=item['canonical_profile_url'],verification_status='VERIFIED',evidence={**item,'scan_id':proposed})
            conn.execute("INSERT INTO identity_evidence(evidence_id,identity_id,evidence_type,source_url,source_path,source_digest,captured_at,verification_status,metadata_json) VALUES(?,?,?,?,?,?,?,'VERIFIED',?) ON CONFLICT(identity_id,evidence_type,source_digest) DO NOTHING",('evidence:'+v3.digest([proposed,uid]),douyin_identity_id,'BUYIN_QR_PROFILE',item['canonical_profile_url'],item['qr_path'],v3.digest(item),now,v3.json_text(item)))
            v3._upsert_monitor_target_conn(conn,competitor_id=competitor_id,live_url=item['monitor_url'],metadata={'source_scan':proposed,'source_kind':'verified_qr_and_monitor_probe','account_name':item['account_name'],'douyin_stable_id':item['douyin_stable_id'],'canonical_profile_url':item['canonical_profile_url'],'monitor_probe':item.get('monitor_probe') or {}})
            conn.execute("INSERT INTO product_competitors(relation_id,product_id,competitor_id,relation_status,first_seen_at,last_seen_at,last_scan_id,metadata_json) VALUES(?,?,?,'ACTIVE',?,?,?,?) ON CONFLICT(product_id,competitor_id) DO UPDATE SET relation_status='ACTIVE',last_seen_at=excluded.last_seen_at,last_scan_id=excluded.last_scan_id",('relation:'+v3.digest([product_id,competitor_id]),product_id,competitor_id,now,now,proposed,v3.json_text({'source_result':str(result_path)})))
        for index,row in enumerate(rows,1):
            uid=row['buyin_creator_uid']
            conn.execute("INSERT INTO scan_observations(observation_id,scan_id,product_id,competitor_id,observation_index,source_page,source_batch,source_position,platform_observation_key,account_name,buyin_creator_uid,live_title,live_date,collected_at,identity_state,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'VERIFIED_UID',?)",(f'obs:{proposed}:{index}',proposed,product_id,by_uid[uid],index,row.get('source_page'),row.get('source_batch'),row.get('source_position'),v3.digest([proposed,index,uid,row.get('live_date'),row.get('live_title')]),row['account_name'],uid,row.get('live_title'),row.get('live_date'),row.get('collected_at') or now,v3.json_text(row)))
        if task_id:
            conn.execute('UPDATE tasks SET product_id=?,updated_at=? WHERE task_id=?',(product_id,now,task_id))
            v3.checkpoint(conn,task_id,'SCAN_AND_IDENTITIES_COMMITTED','CURRENT',{'scan_id':proposed,'result_path':str(result_path),'product_id':product_id})
        v3.record_event(conn,'SCAN_AND_IDENTITIES_COMMITTED',task_id=task_id,object_type='scan',object_id=proposed,payload={'product_id':product_id,'rows':len(rows),'identities':len(identities)})
        conn.commit()
    return {'scan_id':proposed,'product_id':product_id,'observations_imported':len(rows),'identities_imported':len(identities),'reused':False}
