import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

import v3_runtime as v3
import v3_retention_worker as retention_worker
from v3_task_control import parse_control, ingest_control, select_task
from v3_scan_import import import_scan, scan_identity, validate_scan

USER='ou_e2bb6eeeda749177d2b1191664831934'


class RetentionKillSwitchTests(unittest.TestCase):
    def test_delete_requires_literal_true(self):
        self.assertTrue(retention_worker.retention_delete_enabled({"retention": {"delete_enabled": True}}))
        for value in (None, False, 1, "true", "TRUE", {}):
            with self.subTest(value=value):
                config = {} if value == {} else {"retention": {"delete_enabled": value}}
                self.assertFalse(retention_worker.retention_delete_enabled(config))

    def test_disabled_returns_before_database_or_filesystem_work(self):
        for config in ({}, {"retention": {}}, {"retention": {"delete_enabled": False}}):
            with self.subTest(config=config), \
                    patch.object(retention_worker, "load_config", return_value=config), \
                    patch.object(retention_worker, "init_db") as init_db, \
                    patch.object(retention_worker, "connect") as connect, \
                    patch.object(Path, "unlink") as unlink, \
                    patch.object(retention_worker, "upsert_heartbeat") as heartbeat:
                result = retention_worker.once(heartbeat_service="retention-kill-switch-test")
                self.assertEqual(result["mode"], "DELETE_DISABLED")
                self.assertFalse(result["delete_enabled"])
                self.assertEqual(result["deleted"], 0)
                init_db.assert_not_called()
                connect.assert_not_called()
                unlink.assert_not_called()
                heartbeat.assert_called_once()


class RuntimeCase(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='v3-workflow-test-')
        self.root=Path(self.temp.name)
        self.db=self.root/'runtime.db'
        v3.init_db(self.db)
        original=v3.connect
        self.connect=lambda *a,**k:original(self.db)
        self.patches=[patch.object(v3,'connect',side_effect=self.connect),patch.object(v3,'identity_assertion')]
        for p in self.patches:p.start()
    def tearDown(self):
        for p in self.patches:p.stop()
        self.temp.cleanup()
    def task(self,message='om_1',product='3838016038189006849',chat='oc_1'):
        return v3.ingest_message(message_id=message,chat_id=chat,sender_id=USER,content='扫描商品 '+product)['task_id']
    def control(self,text,message='om_control',chat='oc_1'):
        return ingest_control(message_id=message,chat_id=chat,sender_id=USER,content=text,control=parse_control(text))


class ControlTests(RuntimeCase):
    def test_plain_continue_is_control(self):
        self.assertEqual(parse_control('继续处理。')['action'],'resume')
        self.assertEqual(parse_control('查询进度')['action'],'status')
        self.assertIsNone(parse_control('这个人继续直播时说了什么'))
    def test_resume_same_task_preserves_attempt_fencing(self):
        task=self.task()
        with self.connect() as c:c.execute("UPDATE tasks SET status='FAILED_FINAL',attempts=5,max_attempts=5 WHERE task_id=?",(task,))
        result=self.control('继续处理。')
        self.assertEqual(result['task_id'],task)
        with self.connect() as c:
            row=c.execute('SELECT * FROM tasks WHERE task_id=?',(task,)).fetchone()
            self.assertEqual(row['status'],'RETRY_WAIT')
            self.assertEqual(row['attempts'],5)
            self.assertGreater(row['max_attempts'],5)
            self.assertEqual(c.execute('SELECT count(*) FROM tasks').fetchone()[0],1)
    def test_duplicate_control_is_not_reapplied(self):
        task=self.task()
        with self.connect() as c:c.execute("UPDATE tasks SET status='FAILED_FINAL',attempts=5 WHERE task_id=?",(task,))
        self.control('继续')
        with self.connect() as c:c.execute("UPDATE tasks SET status='RUNNING' WHERE task_id=?",(task,))
        self.assertEqual(self.control('继续')['status'],'DUPLICATE')
        with self.connect() as c:self.assertEqual(c.execute('SELECT status FROM tasks').fetchone()[0],'RUNNING')
    def test_status_does_not_resume(self):
        task=self.task()
        with self.connect() as c:c.execute("UPDATE tasks SET status='FAILED_FINAL' WHERE task_id=?",(task,))
        result=self.control('任务状态')
        self.assertIn('FAILED_FINAL',result['ack_text'])
        with self.connect() as c:self.assertEqual(c.execute('SELECT status FROM tasks').fetchone()[0],'FAILED_FINAL')
    def test_cross_chat_cannot_control(self):
        task=self.task()
        result=self.control('继续 '+task,chat='oc_other')
        self.assertIsNone(result['task_id'])
    def test_distinct_products_require_target(self):
        self.task('om_a','111111111111')
        self.task('om_b','222222222222')
        result=self.control('继续')
        self.assertIsNone(result['task_id'])
        self.assertIn('多个不同商品',result['ack_text'])
    def test_repeated_product_share_chooses_original(self):
        first=self.task('om_a')
        self.task('om_b')
        result=self.control('继续')
        self.assertEqual(result['task_id'],first)
    def test_completed_original_covers_earlier_resend(self):
        first=self.task('om_a');self.task('om_b')
        with self.connect() as c:c.execute("UPDATE tasks SET status='COMPLETE',last_success_at=? WHERE task_id=?",(v3.utc_now(),first))
        result=self.control('继续')
        self.assertEqual(result['task_id'],first)
        self.assertIn('已完成交付',result['ack_text'])
    def test_identity_conflict_not_overridden(self):
        task=self.task()
        with self.connect() as c:c.execute("UPDATE tasks SET status='FAILED_FINAL',error_type='IDENTITY_CONFLICT' WHERE task_id=?",(task,))
        self.control('继续')
        with self.connect() as c:self.assertEqual(c.execute('SELECT status FROM tasks').fetchone()[0],'FAILED_FINAL')
    def test_running_task_does_not_restart(self):
        task=self.task()
        with self.connect() as c:c.execute("UPDATE tasks SET status='RUNNING' WHERE task_id=?",(task,))
        self.assertIn('不会重复启动',self.control('继续')['ack_text'])
    def test_no_task_is_explicit(self):
        result=self.control('继续')
        self.assertIsNone(result['task_id'])
        self.assertIn('没有可操作',result['ack_text'])
    def test_concurrent_duplicate_control_has_one_event(self):
        self.task()
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(lambda _:self.control('继续'),range(100)))
        self.assertEqual(sum(r['status']=='CAPTURED' for r in results),1)
        with self.connect() as c:self.assertEqual(c.execute("SELECT count(*) FROM domain_events WHERE event_type='TASK_CONTROL_RECEIVED'").fetchone()[0],1)


class ImportTests(RuntimeCase):
    def result(self,product='3838016038189006849',uid='v2_creator',stable='uid:123456789012345',name='同名账号'):
        task=self.task('om_'+product,product)
        result={'profile_id':v3.PROFILE_ID,'source_task_id':task,'product':{'target_product_id':product,'final_page_product_id':product,'page_verified':True,'name':'测试商品','original_input':'https://alliance.jinritemai.com/?promotion_id='+product},'scan_summary':{'status':'COMPLETE','filter_verified':True,'content_type':'live','filter_label':'近30天','page_reported_result_count':1,'end_signal':{'verified':True}},'observations':[{'buyin_creator_uid':uid,'account_name':name,'_detail_verified':True,'live_title':'直播','live_date':'2026/08/28'}],'unique_creators':[{'buyin_creator_uid':uid,'account_name':name,'douyin_stable_id':stable,'verification_status':'VERIFIED','canonical_profile_url':'https://www.douyin.com/share/user/'+stable.split(':')[1],'buyin_detail_url':'https://buyin.jinritemai.com/dashboard/followed-daren?uid='+uid,'qr_path':str(self.root/'qr.png'),'monitor_probe':{'status':'OFFLINE_CONFIRMED','anchor_name':name,'platform_user_id':stable.split(':')[1]},'monitor_verified':True,'monitor_url':'https://live.douyin.com/12345678901'}]}
        folder=self.root/product;folder.mkdir(exist_ok=True);path=folder/'result.json';path.write_text(json.dumps(result,ensure_ascii=False))
        return path,result
    def test_same_filename_two_products_no_collision(self):
        p1,_=self.result('111111111111');p2,_=self.result('222222222222')
        a=import_scan(p1);b=import_scan(p2)
        self.assertNotEqual(a['scan_id'],b['scan_id'])
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM scan_runs').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM scan_observations').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM competitors').fetchone()[0],1)
    def test_reimport_same_result_no_duplicate(self):
        path,_=self.result();a=import_scan(path);b=import_scan(path)
        self.assertEqual(a['scan_id'],b['scan_id']);self.assertTrue(b['reused'])
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM scan_runs').fetchone()[0],1)
    def test_committed_scan_recovers_to_delivery_without_browser(self):
        import v3_worker as worker
        path,data=self.result();imported=import_scan(path)
        with patch.object(worker,'connect',side_effect=self.connect),patch.object(worker.subprocess,'run') as command:
            resumed=worker.process_task_once()
            self.assertEqual(resumed['status'],'DELIVERY_PENDING')
            command.assert_not_called()
        with self.connect() as c:
            row=c.execute("SELECT payload_json FROM outbox WHERE object_type='scan_result'").fetchone()
            body=json.loads(row['payload_json'])
            self.assertEqual(body['task_id'],data['source_task_id'])
            self.assertEqual(body['scan_id'],imported['scan_id'])
    def test_same_product_new_content_keeps_history(self):
        path,data=self.result();a=import_scan(path)
        data['scan_run_id']='new-run';path.write_text(json.dumps(data));b=import_scan(path)
        self.assertNotEqual(a['scan_id'],b['scan_id'])
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM scan_runs').fetchone()[0],2)
    def test_rescan_keeps_legacy_control_scope(self):
        path,data=self.result();a=import_scan(path)
        scope={'chat_id':'oc_1','sender_id':USER,'message_id':'om_source'}
        with self.connect() as c:c.execute('UPDATE products SET metadata_json=? WHERE product_id=?',(json.dumps({'control_scope':scope}),a['product_id']))
        data['scan_run_id']='new-run';path.write_text(json.dumps(data));import_scan(path)
        with self.connect() as c:self.assertEqual(json.loads(c.execute('SELECT metadata_json FROM products').fetchone()[0])['control_scope'],scope)
    def test_incomplete_never_creates_business_objects(self):
        path,data=self.result();data['scan_summary']['filter_verified']=False;path.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'SCAN_SCOPE'):import_scan(path)
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM products').fetchone()[0],0)
    def test_missing_uid_blocks_import(self):
        path,data=self.result();data['observations'][0]['buyin_creator_uid']=None;path.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'IDENTITY_CLOSURE'):import_scan(path)
    def test_unverified_monitor_blocks(self):
        path,data=self.result();data['unique_creators'][0]['monitor_verified']=False;path.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'MONITOR_IDENTITY'):import_scan(path)
    def test_same_name_different_uid_not_merged(self):
        p1,_=self.result('111111111111','v2_a','uid:111111111111111');p2,_=self.result('222222222222','v2_b','uid:222222222222222')
        import_scan(p1);import_scan(p2)
        with self.connect() as c:self.assertEqual(c.execute('SELECT count(*) FROM competitors').fetchone()[0],2)
    def test_conflict_rolls_back_scan_and_product(self):
        p1,_=self.result('111111111111','v2_a','uid:111111111111111');p2,_=self.result('222222222222','v2_b','uid:222222222222222')
        import_scan(p1);import_scan(p2)
        path,data=self.result('333333333333','v2_a','uid:222222222222222')
        with self.assertRaisesRegex(ValueError,'IDENTITY_CONFLICT'):import_scan(path)
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM scan_runs').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM products').fetchone()[0],2)


if __name__=='__main__':unittest.main(verbosity=2)
