import contextlib
import io
import hashlib
import importlib.util
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import shutil
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import v3_runtime as v3
import v3_worker as worker
import v3_pipeline_worker as pipeline
from test_v3_workflow import RuntimeCase
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'bin'))
import recorder
spec = importlib.util.spec_from_file_location('shared_recorder_test', recorder.SHARED_RECORDER)
shared_recorder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared_recorder)


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
        self.log_patch=patch.object(worker,'RECORDING_LOG_ROOT',self.root/'logs');self.log_patch.start()
    def tearDown(self):
        self.log_patch.stop();worker.RECORDER_PROCESSES.clear()
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
    def test_zero_byte_failed_output_gets_a_new_filename(self):
        folder=self.partial/'acct'/'sess';folder.mkdir(parents=True)
        original=folder/'整场直播.ts.partial';original.touch()
        def shared(cmd):
            self.assertNotEqual(cmd[cmd.index('--filename')+1],original.name)
            (folder/cmd[cmd.index('--filename')+1]).write_bytes(b'recovered')
            return 0,{'status':'recorded'}
        result,_=self.call_recorder(['--url','https://live.douyin.com/123','--account-id','acct','--session-id','sess','--approved'],shared)
        self.assertEqual(result['status'],'RECORDED');self.assertEqual(original.stat().st_size,0)
    def test_sidecar_reserves_failed_output_filename(self):
        folder=self.partial/'acct'/'sess';folder.mkdir(parents=True)
        (folder/'整场直播.ts.partial.recording-state.json').write_text('{}')
        def shared(cmd):
            name=cmd[cmd.index('--filename')+1]
            self.assertNotEqual(name,'整场直播.ts.partial')
            (folder/name).write_bytes(b'recovered')
            return 0,{'status':'recorded'}
        result,run=self.call_recorder(['--url','https://live.douyin.com/123','--account-id','acct','--session-id','sess','--approved'],shared)
        self.assertEqual(result['status'],'RECORDED')
        self.assertIn('--refresh-seconds',run.call_args.args[0])


class SharedRecorderTests(unittest.TestCase):
    def test_actual_ffmpeg_reconnects_after_http_503(self):
        ffmpeg=shutil.which('ffmpeg')
        if not ffmpeg:self.skipTest('ffmpeg unavailable')
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'source.ts';output=Path(d)/'capture.ts'
            subprocess.run([ffmpeg,'-v','error','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','3','-c:a','aac','-f','mpegts',str(source)],check=True,capture_output=True,timeout=20)
            data=source.read_bytes();requests=[]
            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    requests.append(self.path)
                    if len(requests)==1:
                        self.send_response(503);self.send_header('Content-Length','0');self.end_headers();return
                    self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
                def log_message(self,*_args):pass
            server=HTTPServer(('127.0.0.1',0),Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                code,_=shared_recorder.record_stream(f'http://127.0.0.1:{server.server_port}/live',output,2,ffmpeg)
            finally:
                server.shutdown();server.server_close();thread.join(timeout=5)
            self.assertEqual(code,0);self.assertGreaterEqual(len(requests),2)
            self.assertGreater(output.stat().st_size,0)
            state=json.loads(shared_recorder.recording_state_path(output).read_text())
            self.assertTrue(any('503' in line for line in state['ffmpeg_tail']))
    def test_actual_ffmpeg_failure_records_exit_code(self):
        ffmpeg=shutil.which('ffmpeg')
        if not ffmpeg:self.skipTest('ffmpeg unavailable')
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'broken.mp4';source.write_bytes(b'broken input')
            output=Path(d)/'capture.ts'
            code,_=shared_recorder.record_stream(str(source),output,0,ffmpeg)
            state=json.loads(shared_recorder.recording_state_path(output).read_text())
            self.assertNotEqual(code,0);self.assertEqual(state['return_code'],code)
            self.assertEqual(state['exit_kind'],'ERROR');self.assertTrue(state['ffmpeg_tail'])
    def test_http_retry_is_bounded_without_reusing_url_at_eof(self):
        with tempfile.TemporaryDirectory() as d:
            process=SimpleNamespace(pid=123,stderr=io.StringIO(''),poll=lambda:0,wait=lambda:0)
            with patch.object(shared_recorder.subprocess,'Popen',return_value=process) as run:
                shared_recorder.record_stream('https://example.invalid/live',Path(d)/'capture.ts',0,'ffmpeg')
            command=run.call_args.args[0]
            self.assertNotIn('-reconnect_at_eof',command);self.assertIn('-reconnect_on_network_error',command)
            self.assertIn('-reconnect_delay_total_max',command);self.assertIn('-n',command)
            self.assertNotIn('-t',command)
    def test_stream_fingerprint_never_persists_query_values(self):
        url='https://cdn.example/live/path.flv?token=SECRET&expire=12345'
        value=shared_recorder.stream_fingerprint(url)
        self.assertEqual(value['hostname'],'cdn.example')
        self.assertEqual(value['query_keys'],['expire','token'])
        self.assertNotIn('SECRET',json.dumps(value))
        self.assertNotIn('12345',json.dumps(value))
    def test_flv_is_preferred_over_hls(self):
        self.assertEqual(shared_recorder.ordered_stream_urls({'m3u8_url':'hls','flv_url':'flv'}),['flv','hls'])
    def test_live_recording_refreshes_url_and_uses_unique_chunks(self):
        def live(token):
            return {'is_live':True,'stream_url':f'https://cdn.example/live.flv?token={token}','stream_urls':[],
                    'stream_protocol':'FLV','anchor_name':'test','room_id':'room','quality':'LD'}
        responses=[live('one'),live('two'),{'is_live':False},{'is_live':False},{'is_live':False}]
        with tempfile.TemporaryDirectory() as d,patch.object(shared_recorder,'resolve_stream',AsyncMock(side_effect=responses)),patch.object(shared_recorder.time,'sleep'):
            def record(url,path,duration,ffmpeg,segment,metadata):
                path.write_bytes(b'media');return 0,''
            with patch.object(shared_recorder,'record_stream',side_effect=record) as calls,patch.object(shared_recorder,'probe_duration',return_value=900):
                code,result=shared_recorder.record_live_with_refresh('https://live.douyin.com/123','LD',Path(d)/'whole.ts.partial','ffmpeg',900)
        self.assertEqual(code,0);self.assertEqual(len(result['outputs']),2)
        self.assertNotEqual(result['outputs'][0],result['outputs'][1])
        self.assertNotEqual(calls.call_args_list[0].args[0],calls.call_args_list[1].args[0])
        self.assertEqual(calls.call_args_list[0].args[5]['protocol'],'FLV')
    def test_next_url_resolution_begins_before_current_recording_returns(self):
        def live(token):
            return {'is_live':True,'stream_url':f'https://cdn.example/live.flv?token={token}',
                    'stream_urls':[],'stream_protocol':'FLV','anchor_name':'test','room_id':'room','quality':'LD'}
        first_record_active=threading.Event();release_first=threading.Event();second_resolution_started=threading.Event()
        resolve_count=0;record_urls=[];holder={}
        async def resolve(_url,_quality):
            nonlocal resolve_count
            resolve_count+=1
            if resolve_count==1:return live('one')
            if resolve_count==2:second_resolution_started.set();return live('two')
            return {'is_live':False,'stream_url':None}
        def record(url,path,*_args):
            record_urls.append(url);path.write_bytes(b'media')
            if len(record_urls)==1:
                first_record_active.set()
                if not release_first.wait(2):raise AssertionError('test did not release first recording')
                return 0,''
            return 1,'simulated final short read'
        def run():
            try:
                with tempfile.TemporaryDirectory() as d:
                    holder['result']=shared_recorder.record_live_with_refresh('https://live.douyin.com/123','LD',Path(d)/'whole.ts.partial','ffmpeg',0.2)
            except BaseException as exc:holder['error']=exc
        with patch.object(shared_recorder,'resolve_stream',side_effect=resolve),patch.object(shared_recorder,'record_stream',side_effect=record),patch.object(shared_recorder,'probe_duration',side_effect=[0.2,None]),patch.object(shared_recorder.time,'sleep'),patch.object(shared_recorder,'REFRESH_PRERESOLVE_LEAD_SECONDS',0.1):
            thread=threading.Thread(target=run);thread.start()
            try:
                self.assertTrue(first_record_active.wait(1))
                self.assertTrue(second_resolution_started.wait(0.6),'next URL was not resolved while current recording was active')
            finally:
                release_first.set();thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        if 'error' in holder:raise holder['error']
        self.assertEqual(holder['result'][0],0)
        self.assertEqual(record_urls[:2],['https://cdn.example/live.flv?token=one','https://cdn.example/live.flv?token=two'])
    def test_prefetched_url_is_never_written_to_attempt_metadata(self):
        def live(token):
            return {'is_live':True,'stream_url':f'https://cdn.example/live.flv?token={token}',
                    'stream_urls':[],'stream_protocol':'FLV','anchor_name':'test','room_id':'room','quality':'LD'}
        responses=[live('SECRET_ONE'),live('SECRET_TWO'),{'is_live':False},{'is_live':False},{'is_live':False}]
        second_resolution_started=threading.Event();release=threading.Event();calls=0
        async def resolve(_url,_quality):
            nonlocal calls
            value=responses[calls];calls+=1
            if calls==2:second_resolution_started.set()
            return value
        def record(_url,path,*_args):
            path.write_bytes(b'media')
            if calls==1:release.wait(1);return 0,''
            return 1,''
        with tempfile.TemporaryDirectory() as d,patch.object(shared_recorder,'resolve_stream',side_effect=resolve),patch.object(shared_recorder,'record_stream',side_effect=record),patch.object(shared_recorder,'probe_duration',side_effect=[0.2,None]),patch.object(shared_recorder.time,'sleep'),patch.object(shared_recorder,'REFRESH_PRERESOLVE_LEAD_SECONDS',0.1):
            holder={};thread=threading.Thread(target=lambda:holder.setdefault('result',shared_recorder.record_live_with_refresh('https://live.douyin.com/123','LD',Path(d)/'whole.ts.partial','ffmpeg',0.2)));thread.start()
            self.assertTrue(second_resolution_started.wait(0.6));release.set();thread.join(timeout=3)
            attempts=holder['result'][1]['attempts']
        serialized=repr(attempts)
        self.assertNotIn('SECRET_ONE',serialized);self.assertNotIn('SECRET_TWO',serialized);self.assertIn('url_sha256',serialized)
    def test_local_replay_does_not_use_http_options_or_duration_limit(self):
        with tempfile.TemporaryDirectory() as d:
            process=SimpleNamespace(pid=123,stderr=io.StringIO(''),poll=lambda:0,wait=lambda:0)
            with patch.object(shared_recorder.subprocess,'Popen',return_value=process) as run:
                shared_recorder.record_stream('/tmp/replay.mp4',Path(d)/'capture.ts',0,'ffmpeg')
            command=run.call_args.args[0]
            self.assertNotIn('-reconnect',command);self.assertNotIn('-t',command)
    def test_error_tail_is_bounded_redacted_and_persisted_even_on_success(self):
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'capture.ts'
            warnings=''.join(f'WARNING {n} https://secret.invalid/token\n' for n in range(200))
            process=SimpleNamespace(pid=123,stderr=io.StringIO(warnings),poll=lambda:0,wait=lambda:0)
            with patch.object(shared_recorder.subprocess,'Popen',return_value=process):
                code,tail=shared_recorder.record_stream('/tmp/replay.mp4',output,0,'ffmpeg')
            state=json.loads(shared_recorder.recording_state_path(output).read_text())
            self.assertEqual(code,0);self.assertEqual(len(state['ffmpeg_tail']),50)
            self.assertNotIn('secret.invalid',tail);self.assertEqual(state['exit_kind'],'NORMAL')
            self.assertTrue(state['started_at'])
            self.assertTrue(state['ended_at'])
    def test_live_without_stream_url_is_unknown_not_offline(self):
        with patch.object(shared_recorder,'resolve_stream',AsyncMock(return_value={'is_live':True,'stream_url':None})),patch.object(sys,'argv',['record','--url','https://live.douyin.com/123','--check-only']),contextlib.redirect_stdout(io.StringIO()) as output:
            code=shared_recorder.main()
        self.assertEqual(code,5);self.assertEqual(json.loads(output.getvalue())['status'],'error')
    def test_existing_replay_output_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            source=Path(d)/'source.mp4';source.write_bytes(b'source')
            dest=Path(d)/'output.ts';dest.write_bytes(b'preserve')
            with patch.object(sys,'argv',['record','--input-file',str(source),'--output-dir',d,'--filename',dest.name]),contextlib.redirect_stdout(io.StringIO()) as output:
                code=shared_recorder.main()
            self.assertEqual(code,5);self.assertEqual(dest.read_bytes(),b'preserve')


class RecordingStateTests(RecordingCase):
    def test_segment_registration_survives_directory_move(self):
        p=self.partial/'整场直播.ts';p.write_bytes(b'unit media')
        self.assertEqual(pipeline.register_segments(),1)
        with self.connect() as c:original=c.execute('SELECT segment_id FROM recording_segments').fetchone()[0]
        self.partial.rename(self.completed)
        self.assertEqual(pipeline.register_segments(),1)
        with self.connect() as c:
            rows=c.execute('SELECT segment_id,path FROM recording_segments').fetchall()
            self.assertEqual(len(rows),1);self.assertEqual(rows[0]['segment_id'],original)
            self.assertEqual(rows[0]['path'],str(self.completed/'整场直播.ts'))
    def test_finalizer_reuses_registered_segment_after_move(self):
        p=self.partial/'整场直播.ts';p.write_bytes(b'unit media')
        pipeline.register_segments()
        with self.connect() as c:
            original=c.execute('SELECT segment_id FROM recording_segments').fetchone()[0]
            c.execute("UPDATE live_sessions SET status='ENDED',ended_at='2026-08-28T00:10:00Z'")
            c.execute('UPDATE recording_jobs SET pid=NULL')
            s,j=self.rows(c)
            fake=type('Process',(),{'returncode':0,'stdout':json.dumps({'format':{'duration':'600'},'streams':[{'index':0,'codec_type':'video','codec_name':'h264'},{'index':1,'codec_type':'audio','codec_name':'aac'}]}),'stderr':''})()
            with patch.object(worker.subprocess,'run',return_value=fake):worker.finalize_media_for_session(c,s,j)
            row=c.execute('SELECT segment_id,path FROM recording_segments').fetchone()
            self.assertEqual(row['segment_id'],original);self.assertEqual(row['path'],str(self.completed/'整场直播.ts'))
            self.assertEqual(self.rows(c)[0]['status'],'MEDIA_COMPLETE')
    def test_existing_canonical_segment_id_is_preserved(self):
        p=self.partial/'整场直播.ts';p.write_bytes(b'unit media')
        with self.connect() as c:c.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,status,bytes) VALUES('legacy-final','s',?,?,'2026-08-28T00:00:00Z','PARTIAL',10)",(str(p),hashlib.sha256(p.read_bytes()).hexdigest()))
        pipeline.register_segments()
        with self.connect() as c:self.assertEqual(c.execute('SELECT segment_id FROM recording_segments').fetchone()[0],'legacy-final')
    def test_conflicting_live_paths_are_not_overwritten(self):
        old=self.partial/'整场直播.ts';old.write_bytes(b'old media')
        pipeline.register_segments()
        self.completed.mkdir();new=self.completed/'整场直播.ts';new.write_bytes(b'different media')
        self.assertEqual(pipeline.register_segments(),0)
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT path FROM recording_segments').fetchone()[0],str(old))
            self.assertIn('relocation conflict',self.rows(c)[1]['last_error'])
        self.assertEqual(old.read_bytes(),b'old media');self.assertEqual(new.read_bytes(),b'different media')
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
            for minute in range(10,16):
                worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED',f'2026-08-28T00:{minute}:00Z')
            s,_=self.rows(c);self.assertEqual(s['status'],'ENDED');self.assertEqual(s['ended_at'],'2026-08-28T00:10:00Z')
    def test_unknown_breaks_offline_confirmation_window(self):
        with self.connect() as c:
            for minute,state in [(0,'OFFLINE_CONFIRMED'),(1,'UNKNOWN'),(6,'OFFLINE_CONFIRMED')]:
                worker.update_session_liveness(c,{'monitor_target_id':'m'},state,f'2026-08-28T00:0{minute}:00Z')
            s,_=self.rows(c);self.assertIsNone(s['ended_at'])
            self.assertEqual(json.loads(s['metadata_json'])['down_since'],'2026-08-28T00:06:00Z')
    def test_sparse_offline_samples_do_not_prove_continuity(self):
        with self.connect() as c:
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:00:00Z')
            worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:06:00Z')
            self.assertIsNone(self.rows(c)[0]['ended_at'])
    def test_active_media_growth_vetoes_once_and_consumes_the_baseline(self):
        with self.connect() as c:
            c.execute('UPDATE live_sessions SET metadata_json=?',(json.dumps({'recording_health':{'bytes':0}}),))
            (self.partial/'capture.ts.partial').write_bytes(b'still receiving')
            with patch.object(worker,'pid_alive',return_value=True):
                worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:00:00Z')
            s,_=self.rows(c);meta=json.loads(s['metadata_json'])
            self.assertNotIn('down_since',meta);self.assertIn('offline_rejected_media_growth_at',meta)
            self.assertEqual(meta['recording_health']['bytes'],len(b'still receiving'))
            with patch.object(worker,'pid_alive',return_value=True):
                worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED','2026-08-28T00:01:00Z')
            s,_=self.rows(c);meta=json.loads(s['metadata_json'])
            self.assertEqual(meta['down_since'],'2026-08-28T00:01:00Z')
            self.assertEqual(c.execute("SELECT count(*) FROM domain_events WHERE event_type='OFFLINE_REJECTED_MEDIA_GROWTH'").fetchone()[0],1)
    def test_dead_recorder_final_tail_does_not_block_natural_end(self):
        with self.connect() as c:
            c.execute('UPDATE live_sessions SET metadata_json=?',(json.dumps({'recording_health':{'bytes':0}}),))
            c.execute("UPDATE recording_jobs SET status='WAITING_STREAM',pid=NULL")
            (self.partial/'capture.ts.partial').write_bytes(b'final flushed bytes')
            with patch.object(worker,'pid_alive',return_value=False):
                for minute in range(6):
                    worker.update_session_liveness(c,{'monitor_target_id':'m'},'OFFLINE_CONFIRMED',f'2026-08-28T00:0{minute}:00Z')
            s,_=self.rows(c);meta=json.loads(s['metadata_json'])
            self.assertEqual(s['status'],'ENDED');self.assertEqual(s['ended_at'],'2026-08-28T00:00:00Z')
            self.assertEqual(meta['recording_health']['bytes'],len(b'final flushed bytes'))
            self.assertEqual(c.execute("SELECT count(*) FROM domain_events WHERE event_type='OFFLINE_REJECTED_MEDIA_GROWTH'").fetchone()[0],0)
            self.assertEqual(c.execute("SELECT count(*) FROM domain_events WHERE event_type='OFFLINE_MEDIA_TAIL_CONSUMED'").fetchone()[0],1)
    def test_dead_recorder_restarts_for_existing_unknown_session(self):
        with self.connect() as c:c.execute("UPDATE monitor_targets SET live_status='UNKNOWN'")
        with patch.object(worker,'pid_alive',return_value=False),patch.object(worker,'ensure_recording_job',return_value={'started':True}) as restart:
            result=worker.reconcile_recording_jobs()
        self.assertEqual(result['restarted'],1);restart.assert_called_once()
        self.assertEqual(restart.call_args.args[0]['session_id'],'s')
    def test_dead_recorder_does_not_restart_after_confirmed_offline(self):
        with self.connect() as c:c.execute("UPDATE monitor_targets SET live_status='OFFLINE_CONFIRMED'")
        with patch.object(worker,'pid_alive',return_value=False),patch.object(worker,'ensure_recording_job') as restart:
            worker.reconcile_recording_jobs()
        restart.assert_not_called()
    def test_stale_session_cannot_restart_after_end(self):
        with self.connect() as c:
            s,_=self.rows(c);c.execute("UPDATE live_sessions SET status='ENDED'")
        with patch.object(worker,'recording_keys',return_value=('acct','sess',self.partial,self.completed)),patch.object(worker.subprocess,'Popen') as spawn:
            result=worker.ensure_recording_job(s,{'competitor_id':'c'})
        spawn.assert_not_called();self.assertFalse(result['started'])
    def test_owned_exited_process_is_reaped_not_treated_as_running(self):
        process=Mock();process.poll.return_value=0;worker.RECORDER_PROCESSES[987654]=process
        with patch.object(worker.os,'kill') as kill:
            self.assertFalse(worker.pid_alive(987654));kill.assert_not_called()
        self.assertNotIn(987654,worker.RECORDER_PROCESSES)
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
            fake=type('Process',(),{'returncode':0,'stdout':json.dumps({'format':{'duration':'600'},'streams':[{'index':0,'codec_type':'video','codec_name':'h264'},{'index':1,'codec_type':'audio','codec_name':'aac'}]}),'stderr':''})()
            with patch.object(worker.subprocess,'run',return_value=fake):worker.finalize_media_for_session(c,s,j)
            self.assertEqual(self.rows(c)[0]['completeness'],'PARTIAL')


class FullTranscriptTests(RecordingCase):
    def prepare(self):
        self.completed.mkdir();media=self.completed/'整场直播.ts';media.write_bytes(b'unit media')
        with self.connect() as c:
            c.execute("UPDATE live_sessions SET status='MEDIA_COMPLETE',completeness='COMPLETE',ended_at='2026-08-28T00:10:00Z'")
            c.execute('UPDATE live_sessions SET metadata_json=?',(json.dumps({'media_coverage':{'continuous_capture':True}}),))
            c.execute("INSERT INTO recording_segments(segment_id,session_id,path,checksum,captured_from,status,bytes,lifecycle_status) VALUES('seg','s',?,'testhash','2026-08-28T00:00:00Z','COMPLETE',10,'CANONICAL_ACTIVE')",(str(media),))
            c.execute("INSERT INTO transcripts(transcript_id,session_id,source_digest,engine,model,status,created_at,metadata_json) VALUES('sample','s','testhash','faster-whisper','small','COMPLETE','2026-08-28T00:00:00Z',?)",(json.dumps({'sample_only':True,'sample_seconds':300}),))
        return media
    def run_asr(self,duration):
        def extract(media,audio,**kwargs):audio.write_bytes(b'unit audio');return True
        def transcribe(cmd,**kwargs):
            Path(cmd[cmd.index('--output')+1]).write_text(json.dumps({'status':'READY','duration':duration,'segments':[{'start':0,'end':duration,'text':'unit fixture'}]}))
            return type('Process',(),{'stdout':json.dumps({'status':'READY'}),'returncode':0})()
        with patch.object(pipeline,'extract_audio',side_effect=extract) as extraction,patch.object(pipeline,'media_duration',return_value=600),patch.object(pipeline,'run_process_with_lease',side_effect=transcribe):
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
        with self.connect() as c:self.assertEqual(c.execute("SELECT status FROM transcripts WHERE transcript_id!='sample'").fetchone()[0],'RETRY_WAIT')
    def test_partial_capture_not_mislabeled_full_session(self):
        self.prepare()
        with self.connect() as c:c.execute("UPDATE live_sessions SET completeness='PARTIAL'")
        created,extraction=self.run_asr(600);self.assertEqual(created,0);extraction.assert_not_called()
    def test_legacy_complete_label_without_coverage_is_not_accepted(self):
        self.prepare()
        with self.connect() as c:c.execute("UPDATE live_sessions SET metadata_json='{}'")
        created,extraction=self.run_asr(600);self.assertEqual(created,0);extraction.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
