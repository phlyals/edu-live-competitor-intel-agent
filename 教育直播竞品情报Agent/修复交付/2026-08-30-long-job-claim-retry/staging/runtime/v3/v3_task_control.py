"""Deterministic, chat-scoped task control. Never dispatch a control to an LLM."""
from __future__ import annotations

import json
import re
import os
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CONTROL = re.compile(r"^(继续(?:处理|任务)?|恢复(?:处理|任务)?|查询进度|查看进度|任务状态|进度|状态|怎么样了|处理得怎么样了)(?:\s+(task_[A-Za-z0-9_-]+))?[。！!？?\s]*$")
ACTIVE = {"RUNNING", "RECEIVED", "RETRY_WAIT", "DELIVERY_PENDING"}
RESUMABLE = {"FAILED_FINAL", "WAITING_HUMAN", "WAITING_TOOL", "WAITING_IDENTITY", "PARTIAL_SUCCESS"}


def product_scope(conn, product_id: str) -> dict:
    """Inherit a real ingress owner, or an explicitly audited legacy binding."""
    source=conn.execute("SELECT i.chat_id,i.sender_id,i.message_id FROM inbox_messages i JOIN tasks t ON t.task_id=i.task_id WHERE t.product_id=? AND t.task_type='feishu_command' AND i.profile_id='edu_live_competitor_intel' ORDER BY i.received_at LIMIT 1",(product_id,)).fetchone()
    if source:return dict(source)
    product=conn.execute('SELECT metadata_json FROM products WHERE product_id=?',(product_id,)).fetchone()
    return (json.loads(product['metadata_json'] or '{}').get('control_scope') or {}) if product else {}


def authorized_tasks(conn, chat_id: str, sender_id: str) -> list[dict]:
    rows={r['task_id']:dict(r) for r in conn.execute("SELECT DISTINCT t.* FROM tasks t JOIN inbox_messages i ON i.task_id=t.task_id WHERE i.chat_id=? AND i.sender_id=? AND t.task_type='feishu_command' ORDER BY t.started_at,t.task_id",(chat_id,sender_id))}
    for row in conn.execute("SELECT * FROM tasks WHERE task_type='product_rescan'"):
        scope=product_scope(conn,row['product_id'])
        if scope.get('chat_id')==chat_id and scope.get('sender_id')==sender_id:rows[row['task_id']]=dict(row)
    return sorted(rows.values(),key=lambda r:(r.get('started_at') or '',r['task_id']))


def task_summary(task: dict) -> str:
    text=f"商品：{_product_key(task)}\n任务 {task['task_id']}\n状态：{task['status']}\n当前步骤：{task.get('current_step') or '待处理'}"
    if task.get('updated_at'):
        stamp=datetime.fromisoformat(task['updated_at'].replace('Z','+00:00')).astimezone(ZoneInfo('Asia/Shanghai'))
        text+='\n更新时间：'+stamp.strftime('%Y-%m-%d %H:%M:%S')+'（北京时间）'
    if task['status']=='COMPLETE':return text+'\n已完成交付，不重复扫描。'
    if task.get('error_message'):text+='\n原因：'+str(task['error_message'])
    if task.get('next_attempt_at'):text+='\n已安排自动重试，无需重发商品。'
    return text


def parse_control(content: str) -> dict | None:
    match = CONTROL.fullmatch(content.strip())
    if not match:
        return None
    return {"action": "resume" if match[1].startswith(("继续", "恢复")) else "status", "requested_task_id": match[2]}


def _product_key(task: dict) -> str:
    data = json.loads(task.get("input_json") or "{}")
    explicit=(data.get('product_resolution') or {}).get('resolved_input') or task.get('product_id')
    if explicit:return str(explicit).removeprefix('douyin:')
    raw=str(data.get('content') or '')
    number=re.search(r'(?<!\d)\d{10,22}(?!\d)',raw)
    return number.group(0) if number else raw or task['task_id']


def current_tasks(rows: list[dict]) -> list[dict]:
    by_product: dict[str,list[dict]] = {}
    for row in rows:by_product.setdefault(_product_key(row),[]).append(row)
    relevant=[]
    for group in by_product.values():
        finished=[r for r in group if r['status']=='COMPLETE' and r.get('last_success_at')]
        latest=max(finished,key=lambda r:r['last_success_at']) if finished else None
        # A resend made before the original finished is not a new scan request.
        pending_group=[r for r in group if r['status'] not in {'COMPLETE','SUPERSEDED'} and (not latest or (r.get('started_at') or '')>latest['last_success_at'])]
        relevant.extend(pending_group or ([latest] if latest else group))
    return relevant


def select_task(rows: list[dict], requested: str | None, action: str) -> tuple[dict | None, str]:
    if requested:
        return next((r for r in rows if r["task_id"] == requested), None), "explicit"
    relevant=current_tasks(rows)
    pending = [r for r in relevant if r["status"] not in {"COMPLETE","SUPERSEDED"}]
    if action == "status" and not pending:
        return (relevant[-1] if relevant else None), "latest_complete"
    groups: dict[str, list[dict]] = {}
    for row in pending:
        groups.setdefault(_product_key(row), []).append(row)
    if len(groups) > 1:
        return None, "ambiguous"
    # Multiple shares of the same product resume the oldest original task.
    return (next(iter(groups.values()))[0] if groups else (relevant[-1] if relevant else None)), "original"


def ingest_control(*, message_id: str, chat_id: str, sender_id: str, content: str, control: dict) -> dict:
    import v3_runtime as v3
    v3.identity_assertion()
    if sender_id not in v3.load_config().get("allowed_sender_ids", []):
        return {"status": "REJECTED"}
    if not all((message_id, chat_id, sender_id)):
        raise ValueError("exact message, chat and sender are required")
    with v3.connect() as conn:
        v3._begin(conn)
        inbox_id = "control:" + message_id
        inserted = conn.execute("INSERT INTO inbox_messages(inbox_id,platform,message_id,profile_id,app_id,chat_id,sender_id,content,received_at,parsed_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(platform,message_id) DO NOTHING", (inbox_id, v3.PLATFORM, message_id, v3.PROFILE_ID, v3.APP_ID, chat_id, sender_id, content, v3.utc_now(), v3.json_text({"control": control})))
        if inserted.rowcount == 0:
            prior = conn.execute("SELECT task_id FROM inbox_messages WHERE platform=? AND message_id=?", (v3.PLATFORM, message_id)).fetchone()
            conn.commit()
            return {"status": "DUPLICATE", "created": False, "task_id": prior["task_id"]}
        rows = authorized_tasks(conn,chat_id,sender_id)
        task, reason = select_task(rows, control.get("requested_task_id"), control["action"])
        task_id = task["task_id"] if task else None
        if not task:
            text = "有多个不同商品任务待处理，请指定任务ID（继续 task_…）：\n" + "\n\n".join(task_summary(r) for r in current_tasks(rows) if r["status"] not in {'COMPLETE','SUPERSEDED'}) if reason == "ambiguous" else "当前会话没有可操作的任务；不会创建新的扫描任务。"
        elif control["action"] == "resume" and task["status"] in RESUMABLE:
            if task.get("error_type") in {"IDENTITY_CONFIGURATION_ERROR", "IDENTITY_CONFLICT", "UNAUTHORIZED"}:
                text = f"任务 {task_id} 存在身份/配置冲突，不能仅凭继续指令放行：{task.get('error_type')}。"
            else:
                attempts = int(task.get("attempts") or 0)
                # Preserve fencing epochs/history; grant a fresh bounded retry budget.
                conn.execute("UPDATE tasks SET status='RETRY_WAIT',runtime_state='IDLE',next_attempt_at=?,max_attempts=?,lease_owner=NULL,lease_until=NULL,error_type=NULL,error_message=NULL,updated_at=? WHERE task_id=? AND status=?", (v3.utc_now(), max(int(task.get("max_attempts") or 0), attempts+v3.TASK_MAX_ATTEMPTS), v3.utc_now(), task_id, task["status"]))
                conn.execute("UPDATE task_leases SET status='RELEASED',released_at=? WHERE task_id=? AND status='ACTIVE'", (v3.utc_now(), task_id))
                v3.record_event(conn, "USER_TASK_RESUMED", task_id=task_id, payload={"message_id": message_id, "previous_status": task["status"], "previous_attempts": attempts})
                text = f"已将原任务 {task_id} 恢复到队列；不会重复建单。已完整提交的结果仅补交付，未完成的扫描会重新核验。"
        else:
            state = task["status"]
            text = task_summary(task)
            if state in ACTIVE:
                text += "\n任务已在执行或排队，本次不会重复启动。"
        conn.execute("UPDATE inbox_messages SET task_id=? WHERE inbox_id=?", (task_id, inbox_id))
        ack_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, "runtime-v3-control:"+message_id))
        ack_id = v3.enqueue_outbox_conn(conn, object_type="ingress_ack", object_id=inbox_id, destination="feishu_chat", payload={"task_id":task_id,"chat_id":chat_id,"source_message_id":message_id,"text":text,"idempotency_key":ack_uuid,"profile_id":v3.PROFILE_ID})
        until = (datetime.now(timezone.utc)+timedelta(seconds=30)).isoformat(timespec="milliseconds").replace("+00:00","Z")
        conn.execute("UPDATE outbox SET status='IN_FLIGHT',lease_owner=?,lease_until=?,attempts=1,last_attempt_at=? WHERE outbox_id=?",(f"gateway:{os.getpid()}",until,v3.utc_now(),ack_id))
        v3.record_event(conn, "TASK_CONTROL_RECEIVED", task_id=task_id, payload={"message_id":message_id, **control})
        conn.commit()
    return {"status":"CAPTURED","created":True,"task_id":task_id,"ack_outbox_id":ack_id,"ack_text":text,"ack_uuid":ack_uuid,"control":control["action"]}
