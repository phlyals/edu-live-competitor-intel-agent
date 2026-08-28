import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import v3_runtime as v3
import v3_worker as worker
import v3_pipeline_worker as pipeline
from test_v3_workflow import RuntimeCase
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'bin'))
import recorder


class RecordingCase(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.partial=self.root/'partial';self.partial.mkdir()
        self.completed=self.root/'completed'
        with self.connect() as c:
            c.execute("INSERT INTO competitors(competitor_id,platform,platform_account_id,account_name,first_seen_at,last_seen_at) VALUES('c','buyin','c','test','2026-01-01','2026-01-01')")
            c.execute("INSERT INTO monitor_targets(monitor_target_id,competitor_id,status,live_url,live_status) VALUES('m','c','ACTIVE','https://live.douyin.com/123','LIVE')")
            c.execute("INSERT INTO live_sessions(session_id,monitor_target_id,platform_session_id,status,started_at,source_url,metadata_json) VALUES('s','m','room','WAITING_STREAM','2026-08-28T00:00:00Z','https://live.douyin.com/123','{}')")
            c.execute("INSERT INTO recording_jobs(job_id,session_id,status,pid,account_key,recording_key,partial_dir,completed_dir,started_at,updated_at) VALUES('j','s','STARTING',123456,'acct','sess',?,?,'2026-08-28T00:00:00Z','2026-08-28T00:00:00Z')",(str(self.partial),str(self.completed)))
        self.worker_patch=patch.object(worker,'connect',side_effect=self.connect);self.worker_patch.start()
        self.pipeline_patch=patch.object(pipeline,'connect',side_effect=self.connect);self.pipeline_patch.start()
    def tearDown(self):
        self.pipeline_patch.stop();self.worker_patch.stop();super().tearDown()
    def rows(self,c):
        return dict(c.execute("SELECT * FROM live_sessions WHERE session_id='s'").fetchone()),dict(c.execute("SELECT * FROM recording_jobs WHERE session_id='s'").fetchone())


class RecorderTests(RecordingCase):
    def call_recorder(self,args,shared):
        cfg=self.root/'config.json';cfg.write_text(json.dumps({'recording_duration_seconds':0,'recording_quality':'LD'}))
        config={'safety':{'recording':True,'live_monitor':True},'storage':{'directories':{'media_partial':str(self.partial),'media_completed':str(self.completed)}}}
        with patch.object(recorder,'V3_CONFIG',cfg),patch.object(recorder,'load_config',return_value=config),patch.object(recorder,'storage_status',return_value={'status':'READY'}),patch.object(recorder,'run_shared',side_effect=shared) as run,patch.object(sys,'argv',['recorder',*args]),contextlib.redirect_stdout(io.StringIO()) as output:
            recorder.main()
        return json.loads(output.getvalue()),run
    def test_probe_without_duration_never_passes_none(self):
        result,run=self.call_recorder(['--url','https://live.douyin.com/123','--check-only'],lambda cmd:(2,{'status':'not_live'}))
        self.assertEqual(result['status'],'OFFLINE_CONFIRMED')
        self.assertNotIn('None',run.call_args.args[0]);self.assertNotIn('--duration',run.call_args.args[0])
    def test_empty_directory_can_start_without_resume(self):
        folder=self.partial/'acct'/'sess';folder.mkdir(parents=True)
        def shared(cmd):
            (Path(cmd[cmd.index('--output-dir')+1])/cmd[cmd.index('--filename')+1]).write_bytes(b'test-only')
            return 0,{'status':'recorded'}
        result,_=self.call_recorder(['--url','https://live.douyin.com/123','--account-id','acct','--session-id','sess','--approved'],shared)
        self.assertEqual(result['status'],'RECORDED')
    def test_nonempty_media_is_never_overwritten_without_resume(self):
        folder=self.partial/'acct'/'sess';folder.mkdir(parents=True);p=folder/'整场直播.ts.partial';p.write_bytes(b'preserve')
        result,run=self.call_recorder(['--url','https://live.douyin.com/123','--account-id','acct','--session-id','sess','--approved'],lambda cmd:(0,{}))
        self.assertEqual(result['status'],'ERROR');run.assert_not_called();self.assertEqual(p.read_bytes(),b'preserve')


class RecordingStateTests(RecordingCase):
    def test_process_without_media_is_not_running(self):
        with self.connect() as c:
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:00:00Z'),'STARTING')
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:02:01Z'),'STALLED')
    def test_existing_static_file_is_not_success_and_growth_is(self):
        p=self.partial/'整场直播.ts.partial';p.write_bytes(b'old')
        with self.connect() as c:
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:00:00Z'),'STARTING')
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:00:30Z'),'STARTING')
            p.write_bytes(b'old plus new bytes')
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:01:00Z'),'RUNNING')
            s,j=self.rows(c);self.assertEqual(worker.recording_progress(c,s,j,'2026-08-28T00:03:01Z'),'STALLED')
    def test_four_failures_back_off_ten_minutes(self):
        with self.connect() as c:
            for index in range(4):
                s,j=self.rows(c)
                with patch.object(v3,'utc_now',return_value=f'2026-08-28T00:00:0{index}Z'):
                    worker.recording_failure(c,s,j,'test failure','2026-08-28T00:00:00Z')
            s,j=self.rows(c);health=json.loads(s['metadata_json'])['recording_health']
            self.assertEqual(health['next_retry_at'],'2026-08-28T00:10:00.000Z')
            self.assertEqual(health['consecutive_failures'],4)
        with patch.object(worker,'utc_now',return_value='2026-08-28T00:01:00Z'),patch.object(worker,'recording_keys',return_value=('acct','sess',self.partial,self.completed)),patch.object(worker,'discover_recorder_pid',return_value=None),patch.object(worker.subprocess,'Popen') as spawn:
            result=worker.ensure_recording_job(s,{'competitor_id':'c'})
            self.assertEqual(result['reason'],'recording backoff');spawn.assert_not_called()
    def test_live_does_not_prove_recording_and_unknown_does_not_end(self):
        with self.connect() as c:
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'LIVE','2026-08-28T00:00:00Z')
            self.assertEqual(self.rows(c)[0]['status'],'WAITING_STREAM')
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'UNKNOWN','2026-08-28T01:00:00Z')
            self.assertEqual(self.rows(c)[0]['status'],'WAITING_STREAM')
    def test_offline_grace_excluded_from_recorded_duration(self):
        with self.connect() as c:
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:10:00Z')
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:15:01Z')
            s,_=self.rows(c);self.assertEqual(s['status'],'ENDED');self.assertEqual(s['ended_at'],'2026-08-28T00:10:00Z')
    def test_finalizer_never_promotes_live_media(self):
        p=self.partial/'整场直播.ts.partial';p.write_bytes(b'unit fixture')
        with self.connect() as c:
            s,j=self.rows(c);worker.finalize_media_for_session(c,s,j)
            self.assertTrue(p.exists());self.assertFalse(self.completed.exists())
    def test_finalizer_does_not_call_gapped_capture_complete(self):
        (self.partial/'整场直播.ts.partial').write_bytes(b'unit fixture')
        with self.connect() as c:
            c.execute("UPDATE live_sessions SET status='ENDED',ended_at='2026-08-28T00:10:00Z'")
            c.execute('UPDATE recording_jobs SET pid=NULL,restart_count=1')
            s,j=self.rows(c)
            fake=type('Process',(),{'returncode':0,'stdout':json.dumps({'format':{'duration':'600'}})})()
            with patch.object(worker.subprocess,'run',return_value=fake):worker.finalize_media_for_session(c,s,j)
            self.assertEqual(self.rows(c)[0]['completeness'],'PARTIAL')


class FullTranscriptTests(RecordingCase):
    def prepare(self):
        self.completed.mkdir();media=self.completed/'整场直播.ts';media.write_bytes(b'unit media')
        with self.connect() as c:
            c.execute("UPDATE live_sessions SET status='MEDIA_COMPLETE',completeness='COMPLETE',ended_at='2026-08-28T00:10:00Z'")
            c.execute('UPDATE live_sessions SET metadata_json=?',(json.dumps({'media_coverage':{'continuous_capture':True}}),))
            c.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,status,bytes) VALUES('seg','s',?,'testhash','2026-08-28T00:00:00Z','COMPLETE',10)",(str(media),))
            c.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,created_at,metadata_json) VALUES('sample','s','testhash','faster-whisper','small','COMPLETE','2026-08-28T00:00:00Z',?)",(json.dumps({'sample_only':True,'sample_seconds':300}),))
        return media
    def run_asr(self,duration):
        def extract(media,audio,**kwargs):audio.write_bytes(b'unit audio');return True
        def transcribe(cmd,**kwargs):
            Path(cmd[cmd.index('--output')+1]).write_text(json.dumps({'status':'READY','duration':duration,'segments':[{'start':0,'end':duration,'text':'unit fixture'}]}))
            return type('Process',(),{'stdout':json.dumps({'status':'READY'}),'returncode':0})()
        with patch.object(pipeline,'extract_audio',side_effect=extract) as extraction,patch.object(pipeline,'media_duration',return_value=600),patch.object(pipeline.subprocess,'run',side_effect=transcribe):
            result=pipeline.transcribe_pending()
        return result,extraction
    def test_sample_cannot_block_full_transcript_and_replay_is_idempotent(self):
        self.prepare();created,extraction=self.run_asr(600)
        self.assertEqual(created,1);self.assertEqual(extraction.call_args.kwargs['max_seconds'],0)
        with self.connect() as c:
            rows=c.execute("SELECT * FROM transcripts WHERE status='COMPLETE'").fetchall();self.assertEqual(len(rows),2)
            full=next(r for r in rows if r['transcript_id']!='sample');meta=json.loads(full['metadata_json'])
            self.assertFalse(meta['sample_only']);self.assertEqual(meta['coverage_scope'],'FULL_SESSION');self.assertEqual(meta['covered_audio_seconds'],600)
        self.assertEqual(self.run_asr(600)[0],0)
    def test_short_asr_result_is_not_complete(self):
        self.prepare();created,_=self.run_asr(300);self.assertEqual(created,0)
        with self.connect() as c:self.assertEqual(c.execute("SELECT status FROM transcripts WHERE transcript_id!='sample'").fetchone()[0],'PAUSED')
    def test_partial_capture_not_mislabeled_full_session(self):
        self.prepare()
        with self.connect() as c:c.execute("UPDATE live_sessions SET completeness='PARTIAL'")
        created,extraction=self.run_asr(600);self.assertEqual(created,0);extraction.assert_not_called()
    def test_legacy_complete_label_without_coverage_is_not_accepted(self):
        self.prepare()
        with self.connect() as c:c.execute("UPDATE live_sessions SET metadata_json='{}'")
        created,extraction=self.run_asr(600);self.assertEqual(created,0);extraction.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
