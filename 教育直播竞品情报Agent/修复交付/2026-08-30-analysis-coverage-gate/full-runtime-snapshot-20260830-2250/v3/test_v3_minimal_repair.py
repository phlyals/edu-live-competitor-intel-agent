"""Regression coverage for the observed refresh/retry/scheduled-control failure."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import v3_runtime as v3
import v3_worker as worker
from test_v3_workflow import RuntimeCase, USER
from v3_task_control import authorized_tasks, product_scope, task_summary

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'bin'))
from tabbit_scanner import scan_error_type


class ScheduledControlTests(RuntimeCase):
    def setup_product(self,product='douyin:3836325491917324425',chat='oc_1'):
        scope={'chat_id':chat,'sender_id':USER,'message_id':'om_legacy_authorization'}
        with self.connect() as c:
            c.execute("INSERT INTO products(product_id,platform,platform_product_id,title,source_url,status,first_seen_at,last_seen_at,metadata_json) VALUES(?,?,?,?,?,'ACTIVE',?,?,?)",(product,'buyin',product.split(':')[1],'测试商品','https://alliance.jinritemai.com/?promotion_id='+product.split(':')[1],'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',json.dumps({'control_scope':scope})))
        with patch.object(worker,'connect',side_effect=self.connect):worker.schedule_product_rescans()
        with self.connect() as c:return dict(c.execute('SELECT * FROM tasks WHERE product_id=?',(product,)).fetchone())

    def test_scheduled_resume_and_notification_scope(self):
        task=self.setup_product()
        with self.connect() as c:c.execute("UPDATE tasks SET status='WAITING_HUMAN' WHERE task_id=?",(task['task_id'],))
        result=self.control('继续 '+task['task_id'])
        self.assertEqual(result['task_id'],task['task_id'])
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT status FROM tasks').fetchone()[0],'RETRY_WAIT')
            self.assertEqual(c.execute('SELECT count(*) FROM tasks').fetchone()[0],1)
        body=json.loads(task['input_json'])
        self.assertTrue(body['message_id'].startswith('schedule:'))
        self.assertTrue(body['notification_message_id'].startswith('om_'))

    def test_scheduled_scope_cannot_cross_chat(self):
        task=self.setup_product()
        self.assertIsNone(self.control('继续 '+task['task_id'],chat='oc_other')['task_id'])

    def test_scheduled_scope_cannot_cross_sender(self):
        self.setup_product()
        with self.connect() as c:self.assertEqual(authorized_tasks(c,'oc_1','ou_other'),[])

    def test_no_new_rescan_while_previous_waits(self):
        self.setup_product()
        with self.connect() as c:c.execute("UPDATE tasks SET status='WAITING_HUMAN',dedupe_key='earlier-cycle'")
        with patch.object(worker,'connect',side_effect=self.connect):self.assertEqual(worker.schedule_product_rescans(),0)

    def test_completed_old_cycle_does_not_hide_new_cycle(self):
        task=self.setup_product()
        with self.connect() as c:
            c.execute("UPDATE tasks SET task_id='task_earlier_cycle',status='COMPLETE',last_success_at='2026-01-02T00:00:00Z',started_at='2026-01-01T00:00:00Z',dedupe_key='earlier-cycle'")
        with patch.object(worker,'connect',side_effect=self.connect):self.assertEqual(worker.schedule_product_rescans(),1)
        result=self.control('继续')
        self.assertNotEqual(result['task_id'],'task_earlier_cycle')

    def test_rescan_inherits_ingress_owner(self):
        original=self.task()
        with self.connect() as c:
            c.execute("UPDATE tasks SET product_id='douyin:3838016038189006849' WHERE task_id=?",(original,))
            self.assertEqual(product_scope(c,'douyin:3838016038189006849')['sender_id'],USER)

    def test_historical_failure_covered_by_success_does_not_block_schedule(self):
        task=self.setup_product()
        with self.connect() as c:
            c.execute("UPDATE tasks SET task_id='task_historical',status='FAILED_FINAL',started_at='2026-01-01T00:00:00Z',dedupe_key='historical'")
        original=self.task('om_done','3836325491917324425')
        with self.connect() as c:c.execute("UPDATE tasks SET product_id=?,status='COMPLETE',last_success_at='2026-01-02T00:00:00Z' WHERE task_id=?",(task['product_id'],original))
        with patch.object(worker,'connect',side_effect=self.connect):self.assertEqual(worker.schedule_product_rescans(),1)

    def test_temporary_error_retries_but_keeps_evidence_guard(self):
        task=self.setup_product()
        payload={'status':'INCOMPLETE','error_type':'SCAN_DATA_CHANGED','error_message':'result count changed: 55 != 4'}
        fake=type('Process',(),{'stdout':json.dumps(payload),'stderr':''})()
        with patch.object(worker,'connect',side_effect=self.connect),patch.object(worker,'resolve_product_input',return_value=('3836325491917324425',{})),patch.object(worker.subprocess,'run',return_value=fake),patch.object(worker.Path,'mkdir'):
            result=worker.process_task_once()
        self.assertEqual(result['status'],'RETRY_WAIT')
        with self.connect() as c:
            self.assertIsNotNone(c.execute('SELECT next_attempt_at FROM tasks').fetchone()[0])
            self.assertEqual(c.execute('SELECT count(*) FROM scan_runs').fetchone()[0],0)

    def test_retry_exhaustion_reports_actual_status(self):
        self.setup_product()
        with self.connect() as c:c.execute('UPDATE tasks SET attempts=4,max_attempts=5')
        fake=type('Process',(),{'stdout':json.dumps({'status':'INCOMPLETE','error_type':'SCAN_PAGE_NOT_READY'}),'stderr':''})()
        with patch.object(worker,'connect',side_effect=self.connect),patch.object(worker,'resolve_product_input',return_value=('3836325491917324425',{})),patch.object(worker.subprocess,'run',return_value=fake),patch.object(worker.Path,'mkdir'):
            result=worker.process_task_once()
        self.assertEqual(result['status'],'FAILED_FINAL')

    def test_status_uses_beijing_time_and_no_stale_failure_for_complete(self):
        row=self.setup_product();row.update(status='COMPLETE',updated_at='2026-08-28T06:03:00Z',error_message='old failure')
        text=task_summary(row)
        self.assertIn('14:03:00',text)
        self.assertNotIn('old failure',text)


class ErrorClassificationTests(unittest.TestCase):
    def test_observed_count_races_are_retryable(self):
        for message in ('result count changed during identity lookup\n55 !== 4','result count changed during scan\n80 !== 0'):
            self.assertEqual(scan_error_type(RuntimeError(message)),'SCAN_DATA_CHANGED')
    def test_delayed_render_and_http(self):
        self.assertEqual(scan_error_type(RuntimeError('SCAN_RENDER_TIMEOUT: rows')),'SCAN_PAGE_NOT_READY')
        self.assertEqual(scan_error_type(RuntimeError('SCAN_REFRESH_HTTP: 503')),'SCAN_TEMPORARY_HTTP_ERROR')
        self.assertEqual(scan_error_type(RuntimeError('SCAN_REFRESH_HTTP: 403')),'TABBIT_ACQUISITION_BLOCKED')
    def test_unknown_and_identity_not_blindly_retried(self):
        self.assertEqual(scan_error_type(RuntimeError('unknown code defect')),'TABBIT_ACQUISITION_BLOCKED')
        self.assertEqual(scan_error_type(RuntimeError('QR_IDENTITY_UNRESOLVED')),'QR_IDENTITY_UNRESOLVED')


class DeliveryRetryTests(unittest.TestCase):
    def process(self,payload,code):
        return type('Process',(),{'stdout':json.dumps(payload),'stderr':'','returncode':code})()
    def test_exact_record_update_retries_eof(self):
        import v3_project_feishu as project
        failure=self.process({'ok':False,'error':{'type':'network','message':'EOF'}},1)
        success=self.process({'ok':True,'data':{}},0)
        with patch.object(project.subprocess,'run',side_effect=[failure,success]) as run,patch.object(project.time,'sleep'):
            self.assertTrue(project.call(['lark-cli','base','+record-upsert','--record-id','rec_exact'])['ok'])
            self.assertEqual(run.call_count,2)
            self.assertEqual(run.call_args_list[0].args,run.call_args_list[1].args)
    def test_uncertain_create_not_blindly_repeated(self):
        import v3_project_feishu as project
        failure=self.process({'ok':False,'error':{'type':'network','message':'EOF'}},1)
        with patch.object(project.subprocess,'run',return_value=failure) as run,patch.object(project.time,'sleep'):
            with self.assertRaises(RuntimeError):project.call(['lark-cli','base','+record-upsert'])
            self.assertEqual(run.call_count,1)


if __name__=='__main__':unittest.main(verbosity=2)
