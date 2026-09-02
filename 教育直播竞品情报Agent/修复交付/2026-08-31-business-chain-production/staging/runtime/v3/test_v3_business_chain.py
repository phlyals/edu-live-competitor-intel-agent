#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json
from pathlib import Path
from unittest.mock import patch
import v3_comparison_worker as comparison
import v3_knowledge_worker as knowledge
import v3_strategy_worker as strategy
import v3_version_worker as version
from test_v3_workflow import RuntimeCase

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

class BusinessChainTests(RuntimeCase):
 def setUp(self):
  super().setUp();self.comparison_root=self.root/'comparisons';self.version_root=self.root/'versions';self.strategy_root=self.root/'strategies';self.knowledge_root=self.root/'knowledge'
  self.root_patches=[patch.object(comparison,'COMPARISON_ROOT',self.comparison_root),patch.object(version,'VERSION_ROOT',self.version_root),patch.object(strategy,'STRATEGY_ROOT',self.strategy_root),patch.object(knowledge,'KNOWLEDGE_ROOT',self.knowledge_root)]
  [p.start() for p in self.root_patches]
  with self.connect() as c:
   c.execute("insert into competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at) values('c','test','c','c','2026','2026')")
   c.execute("insert into monitor_targets(monitor_target_id,competitor_id,status,live_url,live_status) values('m','c','ACTIVE','https://invalid','OFFLINE_CONFIRMED')")
 def tearDown(self):
  [p.stop() for p in reversed(self.root_patches)];super().tearDown()
 def add(self,n,structure):
  sid=f's{n:02d}';tid=f't{n:02d}';aid=f'a{n:02d}';tp=self.root/f't{n}.json';tp.write_text(json.dumps({'duration':10,'segments':[{'start':0,'end':10,'text':'x'}]}));ap=self.root/f'a{n}.json'
  ref={'source_segment_id':f'src-{n}','content_digest':hashlib.sha256(f'text-{n}'.encode()).hexdigest(),'start':0,'end':10,'source_text':'x'}
  artifact={'analysis_id':aid,'session_id':sid,'transcript_id':tid,'result':{'strategy_structure':structure,'modules':[]},'evidence':{'references':[ref]}}
  ap.write_text(json.dumps(artifact,sort_keys=True));ad=sha(ap)
  with self.connect() as c:
   c.execute("insert into live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,ended_at,completeness,source_url,metadata_json) values(?,?,?,'MEDIA_COMPLETE',?,?, 'COMPLETE','x',?)",(sid,'m',sid,f'2026-01-{n:02d}T00:00:00Z',f'2026-01-{n:02d}T01:00:00Z',json.dumps({'media_coverage':{'continuous_capture':True}})))
   c.execute("insert into transcripts(transcript_id,session_id,source_digest,engine,model,status,output_path,created_at,scope,qualification_status,metadata_json) values(?,?,?,'faster-whisper','small','COMPLETE',?,?,'FULL_SESSION','FULL_SESSION_QUALIFIED','{}')",(tid,sid,'d'+str(n),str(tp),f'2026-01-{n:02d}T01:01:00Z'))
   c.execute("insert into analyses(analysis_id,session_id,transcript_id,analysis_type,source_digest,status,output_path,lineage_state,scope,qualification_status,transcript_content_digest,analysis_spec_version,model_version,prompt_version,artifact_digest,metadata_json,updated_at) values(?,?,?,'single_session',?,'COMPLETE',?,'CURRENT','FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED',?,'spec','model','prompt',?,?,?)",(aid,sid,tid,sha(tp),str(ap),sha(tp),ad,json.dumps({'qualification_state':'FULL_SESSION_QUALIFIED','formal_analysis_eligible':True}),f'2026-01-{n:02d}T01:02:00Z'))
   c.execute("insert into evidence_bundles(bundle_id,object_type,object_id,status,manifest_path,manifest_hash,verified_at,scope,qualification_status,metadata_json) values(?, 'analysis',?,'VERIFIED',?,?,?,'FORMAL_SINGLE_SESSION','FULL_SESSION_QUALIFIED','{}')",('bundle:'+aid,aid,str(ap),ad,f'2026-01-{n:02d}T01:03:00Z'))
  return {'session':sid,'analysis':aid,'digest':ad}
 def run_comparison(self):return comparison.once(connect_fn=self.connect,init_db_fn=lambda:None)
 def run_version(self):return version.once(connect_fn=self.connect,init_db_fn=lambda:None)
 def run_strategy(self):return strategy.once(connect_fn=self.connect,init_db_fn=lambda:None)
 def run_knowledge(self):return knowledge.once(connect_fn=self.connect,init_db_fn=lambda:None)
 def test_comparison_uses_only_latest_two_and_is_evidence_bound(self):
  self.add(1,{'flow':'A'});self.add(2,{'flow':'B'});self.add(3,{'flow':'C'});self.assertEqual(self.run_comparison()['created'],1)
  with self.connect() as c:
   row=c.execute('select * from comparisons').fetchone();ev=c.execute('select * from comparison_evidence order by side').fetchall();bundle=c.execute("select status from evidence_bundles where object_type='comparison'").fetchone()
  self.assertEqual((row['older_session_id'],row['newer_session_id']),('s02','s03'));self.assertEqual(row['similarity_state'],'CHANGED');self.assertEqual(len(ev),2);self.assertEqual(bundle['status'],'VERIFIED')
 def test_same_structure_comparison_is_same(self):
  self.add(1,{'flow':'A'});self.add(2,{'flow':'A'});self.run_comparison()
  with self.connect() as c:r=c.execute('select similarity_state,similarity_score from comparisons').fetchone()
  self.assertEqual(r['similarity_state'],'SAME');self.assertEqual(r['similarity_score'],1.0)
 def activate_a(self):
  for n in (1,2,3):self.add(n,{'flow':'A'})
  r=self.run_version();self.assertEqual(r['activated'],1)
 def test_single_experiment_then_restore_does_not_create_version(self):
  self.activate_a();self.add(4,{'flow':'B'});self.run_version();self.add(5,{'flow':'A'});self.run_version()
  with self.connect() as c:versions=c.execute('select * from strategy_versions').fetchall();obs=c.execute("select observation_state from version_observations where session_id in ('s04','s05') order by session_id").fetchall()
  self.assertEqual(len(versions),1);self.assertEqual([r[0] for r in obs],['EXPERIMENT','STABLE'])
 def test_three_new_sessions_activate_and_three_old_restore_history(self):
  self.activate_a()
  for n in (4,5,6):self.add(n,{'flow':'B'})
  self.assertEqual(self.run_version()['activated'],1)
  for n in (7,8,9):self.add(n,{'flow':'A'})
  self.assertEqual(self.run_version()['restored'],1)
  with self.connect() as c:rows=c.execute('select structure_digest,status,activation_count from strategy_versions order by version_no').fetchall();last=c.execute("select observation_state from version_observations where session_id='s09'").fetchone()[0]
  self.assertEqual([(r['status'],r['activation_count']) for r in rows],[('ACTIVE',2),('SUPERSEDED',1)]);self.assertEqual(last,'HISTORICAL_RESTORED')
 def test_candidate_always_pending_and_knowledge_requires_approval(self):
  self.activate_a();self.assertEqual(self.run_strategy()['created'],1)
  with self.connect() as c:
   candidate=c.execute('select * from strategy_candidates').fetchone();diff=c.execute('select * from knowledge_diffs').fetchone();sessions=c.execute('select count(distinct session_id) from strategy_evidence').fetchone()[0]
  self.assertEqual(candidate['status'],'PENDING_REVIEW');self.assertEqual(diff['status'],'PENDING_REVIEW');self.assertGreaterEqual(sessions,3);self.assertEqual(self.run_knowledge()['published'],0)
  with self.connect() as c:c.execute("insert into approvals(approval_id,object_type,object_id,requested_version,decision,decided_by,decided_at,metadata_json) values('p','strategy_candidate',?,?,'APPROVED','human','2026-02-01T00:00:00Z',?)",(candidate['candidate_id'],candidate['candidate_digest'],json.dumps({'single_use':True,'nonce_used_at':'2026-02-01T00:00:00Z'})))
  self.assertEqual(self.run_knowledge()['published'],1);self.assertEqual(self.run_knowledge()['published'],0)
  with self.connect() as c:kv=c.execute("select * from knowledge_versions where status='APPROVED'").fetchone();receipt=c.execute('select * from knowledge_publish_receipts').fetchone();candidate2=c.execute('select status from strategy_candidates').fetchone()[0]
  self.assertTrue(Path(kv['content_path']).is_file());self.assertEqual(receipt['status'],'VERIFIED');self.assertEqual(candidate2,'APPROVED')
 def test_rejected_or_tampered_candidate_never_publishes(self):
  self.activate_a();self.run_strategy()
  with self.connect() as c:candidate=c.execute('select * from strategy_candidates').fetchone();c.execute("insert into approvals(approval_id,object_type,object_id,requested_version,decision,decided_by,decided_at,metadata_json) values('p','strategy_candidate',?,?,'APPROVED','human','2026-02-01',?)",(candidate['candidate_id'],candidate['candidate_digest'],json.dumps({'single_use':True,'nonce_used_at':'2026-02-01'})))
  Path(candidate['content_path']).write_text('{}');self.assertEqual(self.run_knowledge()['published'],0)
  with self.connect() as c:self.assertEqual(c.execute('select count(*) from knowledge_publish_receipts').fetchone()[0],0);self.assertEqual(c.execute("select count(*) from knowledge_versions where status='APPROVED'").fetchone()[0],0)

if __name__=='__main__':import unittest;unittest.main()
