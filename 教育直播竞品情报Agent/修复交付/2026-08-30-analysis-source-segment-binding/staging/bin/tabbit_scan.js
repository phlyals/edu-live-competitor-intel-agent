// Executed only by the stable tabbit-cli in its task-owned Playwright realm.
const input = globalThis.scanInput;
const fs = await import('node:fs/promises');
const crypto = await import('node:crypto');
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const content = () => page.getByRole('tabpanel', {name:'带货内容', exact:true});
const rowsLocator = () => content().locator('table tbody tr').filter({hasText:'直播时间'});
const selectedFilter = () => content().getByText('近30天', {exact:true});
const totalLocator = () => content().getByText(/^共\d+个直播$/);
const activeNumber = () => content().locator('.auxo-pagination-item-active');
const normalizeUrl = value => {const u=new URL(value);return u.origin+u.pathname;};
// The selected button changes before its rows. Await this exact UI request,
// then match the rendered rows to its response (not to yesterday's total).
const refreshList = async (range, pageNo, click) => {
  const responsePromise=page.waitForResponse(r=>{
    if(new URL(r.url()).pathname!=='/pc/selection/decision/pack_detail')return false;
    try {
      const body=r.request().postDataJSON(), p=body?.dynamic_params?.sale_content_data_params;
      return String(body?.biz_id)===input.productId && p?.content_type==='live' && String(p.time_range)===range && Number(p.page_no)===pageNo;
    } catch {return false;}
  },{timeout:25000}).catch(error=>({error}));
  await click();
  const response=await responsePromise;
  if(response.error)throw new Error('SCAN_REFRESH_TIMEOUT: matching list response not received');
  if(!response.ok())throw new Error('SCAN_REFRESH_HTTP: '+response.status());
  const body=await response.json(), data=body?.data?.model?.sale_content_data;
  assert(body.code===0 && data?.code===0,'SCAN_REFRESH_REJECTED: list response rejected');
  assert(Number.isInteger(data.count) && Array.isArray(data.content_list),'SCAN_RESPONSE_INVALID: missing count or rows');
  const expected=data.content_list.map(r=>({title:r.content_title,name:r.author_name}));
  await expect.poll(async()=>({
    total:await totalLocator().innerText(),
    rows:await rowsLocator().evaluateAll(es=>es.map(e=>({title:e.querySelector('td:nth-child(1) [class*="__title___"]')?.innerText.trim(),name:e.querySelector('td:nth-child(2) [class*="__name___"]')?.innerText.trim()}))),
    loading:await content().locator('.auxo-spin-spinning:visible').count()
  }),{timeout:15000,message:'SCAN_RENDER_TIMEOUT: rendered list does not match response'}).toEqual({total:`共${data.count}个直播`,rows:expected,loading:0});
  if(data.count>0)await expect(activeNumber()).toHaveAttribute('title',String(pageNo));
  return {range,page:pageNo,total:data.count,rowCount:expected.length,responseVerified:true};
};
const verifySurface = async () => {
  assert.equal(new URL(page.url()).searchParams.get('id'),input.productId);
  await expect(page.getByRole('tab',{name:'带货内容',exact:true})).toHaveAttribute('aria-selected','true');
  await expect(page.getByRole('tab',{name:'直播',exact:true})).toHaveAttribute('aria-selected','true');
  await expect(selectedFilter()).toHaveClass(/(?:^|\s)\S*__activeItem___\S*(?:\s|$)/);
  await expect(content().getByText('近7天',{exact:true})).not.toHaveClass(/__activeItem___/);
  await expect(totalLocator()).toBeVisible({timeout:15000});
  return Number((await totalLocator().innerText()).match(/\d+/)[0]);
};

if(input.action==='prepare') {
  if(new URL(page.url()).searchParams.get('id')!==input.productId) {
    await page.goto(input.productUrl,{waitUntil:'domcontentloaded',timeout:30000});
  }
  const later=page.getByText('暂不开启',{exact:true});
  if(await later.isVisible().catch(()=>false)) await later.click();
  const productTitle=page.locator('div[class*="leftPart"] > span[class*="title"]:visible').first();
  await expect(productTitle).toBeVisible({timeout:20000});
  const name=(await productTitle.innerText()).trim();
  await page.getByRole('tab',{name:'带货内容',exact:true}).click();
  await page.getByRole('tab',{name:'直播',exact:true}).click();
  await expect(content().locator('table')).toBeVisible({timeout:15000});
  const seven=content().getByText('近7天',{exact:true});
  if(!/__activeItem___/.test(await seven.getAttribute('class')||''))await refreshList('7',1,()=>seven.click());
  await expect(seven).toHaveClass(/__activeItem___/);
  await expect(selectedFilter()).not.toHaveClass(/__activeItem___/);
  const refreshEvidence=await refreshList('30',1,()=>selectedFilter().click());
  const total=await verifySurface();
  const first=content().locator('.auxo-pagination-item[title="1"]');
  if(total>0 && await activeNumber().getAttribute('title')!=='1') {
    await first.click();
    await expect(activeNumber()).toHaveAttribute('title','1');
  }
  const filterEvidence={...await selectedFilter().evaluate(e=>({label:e.textContent,selectedClass:e.className,siblings:[...e.parentElement.children].map(x=>({label:x.textContent,class:x.className}))})),refresh:refreshEvidence};
  globalThis.scanState={productId:input.productId,name,sourceUrl:page.url(),startedAt:new Date().toISOString(),total,page:1,observations:[],identities:{},pageEvidence:[],opened:0,closed:0,maxOpen:1,filterEvidence,done:false};
  await fs.mkdir(input.outputDir+'/identities',{recursive:true});
  return {status:'PREPARED',productId:input.productId,name,total,filterVerified:true,filterEvidence};
}

if(input.action==='scan_page') {
  const s=globalThis.scanState;
  assert(s && s.productId===input.productId && !s.done,'scan is not prepared');
  assert(s.page<=100 && s.observations.length<=5000,'scan safety bound exceeded');
  assert.equal(await verifySurface(),s.total,'result count changed during scan');
  if(s.total===0){s.done=true;s.pageEvidence.push({page:1,count:0,total:0,filterVerified:true,nextDisabled:true});await fs.writeFile(input.outputDir+'/acquisition.json',JSON.stringify(s,null,2));return {status:'PAGE_COMPLETE',done:true,count:0,identities:0};}
  await expect(activeNumber()).toHaveAttribute('title',String(s.page));
  const currentRows=rowsLocator();
  await expect(currentRows.first()).toBeVisible({timeout:15000});
  const raw=await currentRows.evaluateAll(es=>es.map(e=>{
    const cells=[...e.querySelectorAll('td')];
    const text=cells.map(x=>x.innerText.trim());
    const title=cells[0].querySelector('[class*="__title___"]')?.innerText.trim();
    const name=cells[1].querySelector('[class*="__name___"]')?.innerText.trim();
    return {live_title:title,live_date:text[0].match(/\d{4}\/\d{2}\/\d{2}/)?.[0],account_name:name,follower_count:text[1].match(/粉丝数\s*([^\n]+)/)?.[1],location:text[1].split('\n').at(-1),sales:text[2],settlement_amount:text[3],total_views:text[4],peak_popularity:text[5],click_rate:text[6],order_conversion_rate:text[7]};
  }));
  assert(raw.length>0 && raw.every(r=>r.account_name&&r.live_title&&r.live_date),'invalid live row structure');
  const fingerprint=hash(raw);
  assert(!s.pageEvidence.some(p=>p.fingerprint===fingerprint),'pagination repeated a page');
  for(let i=0;i<raw.length;i++) {
    const row=raw[i], base=page, before=pages().slice();
    let detail;
    try {
      const wait=context.waitForEvent('page',{timeout:12000});
      await currentRows.nth(i).locator('td').nth(1).locator('[class*="__name___"]').click();
      detail=await wait;
      s.opened++; s.maxOpen=Math.max(s.maxOpen,pages().length);
      await expect(detail).toHaveURL(/buyin\.jinritemai\.com\/dashboard\/followed-daren\?.*uid=/,{timeout:15000});
      const u=new URL(detail.url()), uid=u.searchParams.get('uid');
      assert(uid && /^v2_|^\d+$/.test(uid),'invalid Buyin UID');
      row.buyin_creator_uid=uid;
      row._profile_url=detail.url();
      if(!s.identities[uid]) {
        await expect(detail.locator('[class*="__nickname___"]')).toHaveText(row.account_name,{timeout:15000});
        await detail.locator('[class*="daren-overview-selection-qrcode-icon"]').click();
        const tooltip=detail.getByRole('tooltip');
        await expect(tooltip).toContainText('抖音号：',{timeout:15000});
        const tooltipText=await tooltip.innerText();
        const displayId=tooltipText.match(/抖音号：\s*([^\s]+)/)?.[1];
        const canvas=tooltip.locator('canvas');
        await expect(canvas).toBeVisible();
        const data=await canvas.evaluate(e=>e.toDataURL('image/png'));
        const qrPath=input.outputDir+'/identities/'+hash(uid)+'.png';
        await fs.writeFile(qrPath,Buffer.from(data.split(',')[1],'base64'));
        s.identities[uid]={account_name:row.account_name,buyin_creator_uid:uid,buyin_detail_url:detail.url(),douyin_display_id:displayId,tooltip_text:tooltipText,qr_path:qrPath,verified_at:new Date().toISOString(),verification_method:'clicked_row_uid_and_visible_qr_card'};
        await fs.writeFile(input.outputDir+'/identities/'+hash(uid)+'.json',JSON.stringify(s.identities[uid],null,2));
      }
      row._detail_verified=true;
      row.identity_status='VERIFIED_BUYIN';
    } finally {
      for(const p of pages().filter(p=>!before.includes(p))) {
        if(!p.isClosed()){await p.close();s.closed++;}
      }
      usePage(base);
      assert.equal(pages().length,before.length,'owned detail tab did not close');
    }
    s.observations.push({...row,source_page:s.page,source_batch:s.page,source_position:i+1,collected_at:new Date().toISOString()});
  }
  assert.equal(await verifySurface(),s.total,'result count changed during identity lookup');
  const next=content().locator('.auxo-pagination-next');
  const end=(await next.getAttribute('aria-disabled'))==='true';
  s.pageEvidence.push({page:s.page,count:raw.length,total:s.total,fingerprint,filterVerified:true,nextDisabled:end});
  if(end){assert.equal(s.observations.length,s.total,'end-page count mismatch');s.done=true;}
  else {
    const previous=await rowsLocator().allTextContents();
    await refreshList('30',s.page+1,()=>next.locator('button').click());
    s.page++;
    await expect(activeNumber()).toHaveAttribute('title',String(s.page));
    await expect.poll(()=>rowsLocator().allTextContents(),{timeout:15000}).not.toEqual(previous);
  }
  await fs.writeFile(input.outputDir+'/acquisition.json',JSON.stringify(s,null,2));
  return {status:'PAGE_COMPLETE',page:s.pageEvidence.length,done:s.done,observations:s.observations.length,total:s.total,identities:Object.keys(s.identities).length,opened:s.opened,closed:s.closed,pages:pages().length};
}

if(input.action==='resolve_profiles') {
  const s=globalThis.scanState;
  assert(s?.done,'list acquisition incomplete');
  let resolved=0;
  const allowed=value=>{const u=new URL(value);return u.protocol==='https:' && !u.username && !u.password && (!u.port||u.port==='443') && (u.hostname==='douyin.com'||u.hostname.endsWith('.douyin.com'));};
  for(const item of input.items) {
    assert(s.identities[item.uid],'unknown Buyin identity');
    let current=item.qr_url,chain=[current],matched;
    for(let hop=0;hop<=5;hop++) {
      assert(allowed(current),'QR redirect left the Douyin allowlist');
      const u=new URL(current);
      matched=u.pathname.match(/^\/(?:share\/)?user\/([A-Za-z0-9_-]+)\/?$/);
      if(matched)break;
      const res=await context.request.get(current,{maxRedirects:0,timeout:15000});
      if(![301,302,303,307,308].includes(res.status()))throw new Error('QR did not resolve to a user profile');
      current=new URL(res.headers()['location'],current).href;chain.push(current);
    }
    assert(matched,'QR exceeded redirect bound');
    const profile=normalizeUrl(current), stable=matched[1];
    Object.assign(s.identities[item.uid],{qr_url:item.qr_url,douyin_stable_id:(/^\d+$/.test(stable)?'uid:':'sec_uid:')+stable,canonical_profile_url:profile,monitor_url:profile,redirect_chain:chain,verification_status:'VERIFIED'});
    resolved++;
  }
  await fs.writeFile(input.outputDir+'/acquisition.json',JSON.stringify(s,null,2));
  return {resolved,verified:Object.values(s.identities).filter(i=>i.verification_status==='VERIFIED').length,total:Object.keys(s.identities).length,pages:pages().length};
}

if(input.action==='bind_monitors') {
  const s=globalThis.scanState;
  for(const item of input.items){assert(s.identities[item.uid]);Object.assign(s.identities[item.uid],item.binding);}
  await fs.writeFile(input.outputDir+'/acquisition.json',JSON.stringify(s,null,2));
  return {verified:Object.values(s.identities).filter(i=>i.monitor_verified===true).length,total:Object.keys(s.identities).length};
}

if(input.action==='finalize') {
  const s=globalThis.scanState;
  assert(s?.done && s.observations.length===s.total,'incomplete acquisition');
  assert.equal(await verifySurface(),s.total);
  const identities=Object.values(s.identities);
  assert(identities.every(i=>i.verification_status==='VERIFIED'),'identity closure incomplete');
  assert(identities.every(i=>i.monitor_verified===true),'monitor identity binding incomplete');
  assert.equal(s.opened,s.closed,'temporary tabs leaked');
  const result={schema_version:3,profile_id:'edu_live_competitor_intel',source_task_id:input.taskId,scan_run_id:input.scanId,product:{original_input:input.productUrl,target_product_id:s.productId,final_page_product_id:s.productId,name:s.name,page_verified:true,final_navigation_url_redacted:s.sourceUrl},scan_summary:{status:'COMPLETE',started_at:s.startedAt,ended_at:new Date().toISOString(),content_type:'live',time_filter:'last_30_days',filter_label:'近30天',filter_verified:true,page_reported_result_count:s.total,live_observation_count:s.observations.length,unique_creator_count:new Set(identities.map(i=>i.douyin_stable_id)).size,buyin_alias_count:identities.length,unresolved_identity_count:0,page_or_batch_count:s.pageEvidence.length,end_signal:{type:'next_disabled',verified:true}},observations:s.observations,unique_creators:identities,unresolved_identities:[],errors:[],evidence:{filter:s.filterEvidence,pages:s.pageEvidence,temporary_tabs:{opened:s.opened,closed:s.closed,max_open:s.maxOpen},backend:'tabbit-cli-playwright'}};
  await fs.writeFile(input.outputDir+'/result.json',JSON.stringify(result,null,2));
  await fs.writeFile(input.outputDir+'/scan_manifest.json',JSON.stringify({scan_id:input.scanId,task_id:input.taskId,status:'COMPLETE',product_id:s.productId,filter_verified:true,total:s.total,observations:s.observations.length,verified_identities:new Set(identities.map(i=>i.douyin_stable_id)).size,buyin_aliases:identities.length,temporary_tabs_closed:s.opened===s.closed,result_sha256:hash(result)},null,2));
  return {status:'COMPLETE',output_dir:input.outputDir,rows:s.total,identities:new Set(identities.map(i=>i.douyin_stable_id)).size,buyin_aliases:identities.length,temporary_tabs_closed:true};
}
throw new Error('unsupported scanner action');
