#!/usr/bin/env python3
"""Task-isolated Tabbit scanner. No CUA/CDP fallback and no global tabs."""
from __future__ import annotations
import argparse
import json
import os
import selectors
import subprocess
import time
import uuid
import sys
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from PIL import Image
import zxingcpp

LAUNCHER = Path('/Users/mac/.local/bin/tabbit-cli')
PROGRAM = Path(__file__).with_name('tabbit_scan.js')
STORAGE = Path('/Volumes/ExternalStorage/同行直播录制/analysis/drafts/runtime-v3')


def scan_error_type(exc: Exception) -> str:
    message=str(exc)
    if any(marker in message for marker in ('result count changed', 'pagination repeated a page', 'end-page count mismatch')):
        return 'SCAN_DATA_CHANGED'
    if any(marker in message for marker in ('SCAN_REFRESH_TIMEOUT', 'SCAN_RENDER_TIMEOUT')):
        return 'SCAN_PAGE_NOT_READY'
    if 'SCAN_REFRESH_HTTP:' in message and re.search(r'\b(?:429|5\d\d)\b',message):
        return 'SCAN_TEMPORARY_HTTP_ERROR'
    # Unknown, auth and evidence failures remain blocked; do not guess retryability.
    for marker in ('STORAGE_UNAVAILABLE','STORAGE_LOW_SPACE','MONITOR_BINDING_UNKNOWN','QR_IDENTITY_UNRESOLVED'):
        if marker in message:return marker
    return 'TABBIT_ACQUISITION_BLOCKED'


def verify_monitor(identity: dict) -> dict:
    display=str(identity.get('douyin_display_id') or '')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+',display):
        return {'uid':identity['buyin_creator_uid'],'binding':{'monitor_verified':False,'monitor_error':'missing visible Douyin ID'}}
    url='https://live.douyin.com/'+display
    try:
        env=dict(os.environ);env['PATH']='/opt/homebrew/bin:/Users/mac/.local/bin:/usr/bin:/bin'
        proc=subprocess.run([sys.executable,str(Path(__file__).with_name('streamget_probe.py')),'--url',url],capture_output=True,text=True,timeout=45,check=False,env=env)
        probe=json.loads(proc.stdout)
        expected=identity['douyin_stable_id']
        observed=probe.get('platform_user_id') if expected.startswith('uid:') else probe.get('sec_uid')
        same_id=bool(observed and str(observed)==expected.split(':',1)[1])
        same_name=re.sub(r'\s+','',str(probe.get('anchor_name') or ''))==re.sub(r'\s+','',identity['account_name'])
        verified=probe.get('status') in {'LIVE','OFFLINE_CONFIRMED'} and (same_id or (not observed and same_name))
        return {'uid':identity['buyin_creator_uid'],'binding':{'monitor_url':url,'monitor_verified':verified,'monitor_probe':probe,'monitor_error':None if verified else 'probe identity unavailable or mismatched'}}
    except Exception as exc:
        return {'uid':identity['buyin_creator_uid'],'binding':{'monitor_verified':False,'monitor_error':exc.__class__.__name__}}


def verify_monitors(identities: list[dict]) -> list[dict]:
    groups={}
    for item in identities:
        groups.setdefault((item['douyin_stable_id'],item.get('douyin_display_id')),[]).append(item)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results=list(pool.map(verify_monitor,[items[0] for items in groups.values()]))
    return [{'uid':item['buyin_creator_uid'],'binding':result['binding']} for items,result in zip(groups.values(),results) for item in items]


class TabbitProtocol:
    def __init__(self, output: Path):
        self.log = (output/'tabbit.stderr.log').open('ab')
        env = dict(os.environ)
        env['PATH']='/opt/homebrew/bin:/Users/mac/.local/bin:/usr/bin:/bin'
        self.proc = subprocess.Popen([str(LAUNCHER),'persistent'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,env=env)
        self.selector=selectors.DefaultSelector()
        self.selector.register(self.proc.stdout,selectors.EVENT_READ)
        self.buffer=b''
        self.finished=False
        self.receipts=[]

    def frame(self, payload: dict, timeout: float = 130) -> dict:
        if self.proc.poll() is not None:
            raise RuntimeError('Tabbit transport ended; no backend fallback allowed')
        self.proc.stdin.write((json.dumps(payload,ensure_ascii=False)+'\n').encode())
        self.proc.stdin.flush()
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            if b'\n' not in self.buffer:
                if not self.selector.select(min(2,max(0,deadline-time.monotonic()))):continue
                chunk=os.read(self.proc.stdout.fileno(),65536)
                if not chunk:raise RuntimeError('Tabbit transport interrupted')
                self.buffer+=chunk
            while b'\n' in self.buffer:
                line,self.buffer=self.buffer.split(b'\n',1)
                try: result=json.loads(line)
                except (ValueError,UnicodeDecodeError):continue
                self.receipts.append(result)
                return result
        raise TimeoutError('Tabbit receipt timeout; operation not resubmitted')

    def execute(self, frame: dict) -> dict:
        deadline=time.monotonic()+float(frame.get('timeoutMs',120000))/1000+30
        receipt=self.frame(frame)
        while receipt.get('status') in {'queued','running'}:
            if time.monotonic()>deadline:raise TimeoutError('Tabbit operation exceeded its bounded runtime; not replayed')
            receipt=self.frame({'op':'inspect','requestId':frame['requestId'],'waitMs':20000})
        if receipt.get('status')!='succeeded':
            # Preserve uncertain mutation evidence before releasing task-owned tabs.
            if self.proc.poll() is None:
                self.frame({'op':'receipt','requestId':frame['requestId']})
                self.frame({'op':'checkpoint'})
                self.frame({'op':'run','requestId':frame['requestId']+'-observe','code':'return {url:page.url(),pages:pages().length};'})
            raise RuntimeError(str((receipt.get('result') or {}).get('error') or receipt.get('error') or receipt))
        result=receipt.get('result') or {}
        if 'value' not in result:
            raise RuntimeError('Unexpected oversized scanner receipt; use stored acquisition evidence')
        return result['value']

    def finish(self) -> None:
        if self.finished:return
        self.finished=True
        try:
            if self.proc.poll() is None:
                receipt=self.frame({'op':'finish','keep':False})
                if receipt.get('status') in {'failed','interrupted'}:
                    raise RuntimeError('Tabbit finish failed: '+str(receipt))
        finally:
            if self.proc.stdin:self.proc.stdin.close()
            try:self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:self.proc.terminate();self.proc.wait(timeout=10)
            self.selector.close();self.log.close()


def scan(product_id: str, task_id: str, output: Path) -> dict:
    if not product_id.isdigit() or not 8<=len(product_id)<=30:
        raise ValueError('expected a verified numeric product ID')
    output=output.resolve()
    if STORAGE not in output.parents:raise ValueError('output escapes dedicated runtime storage')
    if not os.path.ismount('/Volumes/ExternalStorage'):raise RuntimeError('STORAGE_UNAVAILABLE')
    if shutil.disk_usage(STORAGE).free < 50*1024**3:raise RuntimeError('STORAGE_LOW_SPACE: less than 50GiB free')
    output.mkdir(parents=True,exist_ok=True)
    run_id='scan_'+uuid.uuid4().hex
    input_data={'taskId':task_id,'scanId':run_id,'productId':product_id,'productUrl':f'https://buyin.jinritemai.com/dashboard/merch-picking-library/merch-promoting?id={product_id}','outputDir':str(output)}
    protocol=TabbitProtocol(output)
    def code(action: str, **extra) -> str:
        data={**input_data,'action':action,**extra}
        return 'globalThis.scanInput='+json.dumps(data,ensure_ascii=False)+'; return await eval("(async()=>{"+await (await import("node:fs/promises")).readFile('+json.dumps(str(PROGRAM))+',"utf8")+"\\n})()");'
    try:
        protocol.execute({'op':'bootstrap','taskName':'同行扫描-'+task_id[-12:],'requestId':'prepare','code':code('prepare'),'timeoutMs':60000})
        for n in range(100):
            result=protocol.execute({'op':'run','requestId':f'page-{n+1:03d}','code':code('scan_page'),'timeoutMs':120000})
            if result.get('done'):break
        else:raise RuntimeError('page limit exceeded')
        acquired=json.loads((output/'acquisition.json').read_text())
        identities=list(acquired['identities'].values())
        decoded=[]
        for identity in identities:
            with Image.open(identity['qr_path']) as image:
                codes=zxingcpp.read_barcodes(image,formats=zxingcpp.BarcodeFormat.QRCode)
            if len(codes)!=1:raise RuntimeError('QR_IDENTITY_UNRESOLVED: expected one QR code')
            decoded.append({'uid':identity['buyin_creator_uid'],'qr_url':codes[0].text})
        for offset in range(0,len(decoded),4):
            protocol.execute({'op':'run','requestId':f'profiles-{offset:03d}','code':code('resolve_profiles',items=decoded[offset:offset+4]),'timeoutMs':90000})
        acquired=json.loads((output/'acquisition.json').read_text())
        bindings=verify_monitors(list(acquired['identities'].values()))
        protocol.execute({'op':'run','requestId':'bind-monitors','code':code('bind_monitors',items=bindings),'timeoutMs':30000})
        if not all(item['binding']['monitor_verified'] for item in bindings):
            raise RuntimeError('MONITOR_BINDING_UNKNOWN: '+str(sum(not item['binding']['monitor_verified'] for item in bindings))+' identities need a successful live probe; evidence preserved')
        result=protocol.execute({'op':'run','requestId':'finalize','code':code('finalize'),'timeoutMs':30000})
    finally:
        protocol.finish()
        (output/'tabbit-receipts.json').write_text(json.dumps(protocol.receipts,ensure_ascii=False,indent=2))
    return result


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('product')
    parser.add_argument('--task-id',required=True)
    parser.add_argument('--output-dir',required=True,type=Path)
    args=parser.parse_args()
    try:
        result=scan(args.product,args.task_id,args.output_dir)
    except Exception as exc:
        result={'status':'INCOMPLETE','output_dir':str(args.output_dir),'error_type':scan_error_type(exc),'error_message':str(exc)[-1200:]}
    print(json.dumps(result,ensure_ascii=False))
    return 0 if result['status']=='COMPLETE' else 2


if __name__=='__main__':raise SystemExit(main())
