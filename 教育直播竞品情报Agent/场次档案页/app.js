const state = { sessions: [], selected: null, transcriptOffset: 0, transcriptQuery: '' };
const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtDate = (v) => v ? new Date(v).toLocaleString('zh-CN', {hour12:false}) : '—';
const statusLabel = (v) => ({MEDIA_COMPLETE:'处理完成',ENDED:'已下播',RECORDING:'录制中',DETECTED:'检测到开播',WAITING_STREAM:'等待媒体',COMPLETE:'完成',RUNNING:'处理中',QUALITY_BLOCKED:'质量不合格'}[v] || v || '—');
const badge = (v, good=false) => `<span class="badge ${good?'good':''}">${esc(statusLabel(v))}</span>`;

async function getJson(url) { const r = await fetch(url, {cache:'no-store'}); return r.json(); }
async function load() {
  try { const h = await getJson('/api/health'); $('health').textContent = h.ok ? '实时连接' : '连接异常'; $('health').className = `pill ${h.ok?'good':''}`; } catch { $('health').textContent='连接异常'; }
  const data = await getJson('/api/sessions?limit=200'); state.sessions = data.sessions || []; renderList();
  const pathId = location.pathname.startsWith('/session/') ? decodeURIComponent(location.pathname.split('/')[2]) : '';
  if (pathId) select(pathId); else if (state.sessions[0]) select(state.sessions[0].session_id);
}
function renderList() {
  const q = ($('search').value || '').trim().toLowerCase();
  const rows = state.sessions.filter(s => !q || `${s.account_name||''} ${s.session_id}`.toLowerCase().includes(q));
  $('session-list').innerHTML = rows.map(s => `<button class="session-row ${s.session_id===state.selected?'active':''}" data-id="${esc(s.session_id)}"><div class="row-top"><strong>${esc(s.account_name||'未识别账号')}</strong>${badge(s.status, s.status==='MEDIA_COMPLETE')}</div><span>${fmtDate(s.started_at)}</span><small>${esc(s.session_id)}</small></button>`).join('') || '<div class="empty small">没有匹配场次</div>';
  document.querySelectorAll('.session-row').forEach(b => b.addEventListener('click', () => select(b.dataset.id)));
}
async function select(id) {
  state.selected = id; state.transcriptOffset = 0; state.transcriptQuery = ''; renderList();
  history.replaceState({}, '', `/session/${encodeURIComponent(id)}`);
  const item = state.sessions.find(s => s.session_id === id); if (!item) return;
  $('detail').innerHTML = `<div class="loading">正在加载场次档案…</div>`;
  const data = await getJson(`/api/sessions/${encodeURIComponent(id)}`); renderDetail(data.session || item);
  loadTranscript(id);
}
function renderDetail(s) {
  const mediaPath = s.completed_dir ? `${s.completed_dir}/整场直播.ts` : (s.partial_dir ? `${s.partial_dir}/整场直播.ts` : '尚未生成');
  const mediaExists = s.recording_status === 'COMPLETE' || s.status === 'MEDIA_COMPLETE';
  const analysis = s.analysis || {status:'NOT_READY',summary:{}};
  const summaryLabels = {hook:'开场钩子',pain_points:'家长痛点',course_content:'课程内容',interaction_patterns:'互动模式',product_handoff:'产品承接',cta:'行动号召',claims:'核心主张',risks:'风险提示'};
  const analysisGroups = Object.entries(analysis.summary||{}).map(([key,items]) => `<div class="analysis-group"><h4>${esc(summaryLabels[key]||key)}</h4>${items.map(item=>`<div class="analysis-item"><span class="time-tag">${item.start!=null?`${Number(item.start).toFixed(1)}s`:''}</span><span>${esc(item.text)}</span></div>`).join('')}</div>`).join('');
  $('detail').innerHTML = `<div class="detail-head"><div><span class="eyebrow">SESSION ARCHIVE</span><h2>${esc(s.account_name||'未识别账号')}</h2><p class="mono">${esc(s.session_id)}</p></div><div>${badge(s.status, mediaExists)}</div></div>
    <div class="facts"><div><span>开始时间</span><strong>${fmtDate(s.started_at)}</strong></div><div><span>结束时间</span><strong>${fmtDate(s.ended_at)}</strong></div><div><span>场次状态</span><strong>${badge(s.status)}</strong></div><div><span>录制状态</span><strong>${badge(s.recording_status, mediaExists)}</strong></div></div>
    <section class="artifact"><div class="section-title"><h3>原始媒体</h3><span class="badge ${mediaExists?'good':''}">${mediaExists?'文件可用':'等待文件'}</span></div><div class="path-box"><code>${esc(mediaPath)}</code><button id="copy-path" data-path="${esc(mediaPath)}">复制路径</button></div><p class="hint">本地路径只能在录制所在的 Mac 上打开；在 Finder 中按 Command + Shift + G 后粘贴。</p></section>
    <section class="artifact"><div class="section-title"><h3>场次分析</h3><span class="badge ${analysis.status==='COMPLETE'?'good':''}">${esc(statusLabel(analysis.status))}</span></div>${analysis.doc_url?`<p><a class="doc-link" href="${esc(analysis.doc_url)}" target="_blank" rel="noreferrer">在飞书打开分析文档 ↗</a></p>`:''}<div class="analysis-grid">${analysisGroups||'<div class="empty small">暂无可展示的分析摘要</div>'}</div></section>
    <section class="artifact"><div class="section-title"><h3>完整逐字稿</h3><span id="transcript-status" class="badge">加载中</span></div><div class="transcript-tools"><input id="transcript-search" placeholder="搜索逐字稿关键词"><button id="transcript-search-btn">搜索</button></div><div id="transcript" class="transcript"><div class="loading">加载中…</div></div></section>`;
  $('copy-path').addEventListener('click', async e => { await navigator.clipboard.writeText(e.currentTarget.dataset.path); e.currentTarget.textContent='已复制'; setTimeout(()=>e.currentTarget.textContent='复制路径',1200); });
  $('transcript-search-btn').addEventListener('click', () => { state.transcriptQuery=$('transcript-search').value; state.transcriptOffset=0; loadTranscript(s.session_id); });
  $('transcript-search').addEventListener('keydown', e => { if(e.key==='Enter') $('transcript-search-btn').click(); });
}
async function loadTranscript(id) {
  const box = $('transcript'); if (!box) return; const data = await getJson(`/api/sessions/${encodeURIComponent(id)}/transcript?offset=${state.transcriptOffset}&limit=100&q=${encodeURIComponent(state.transcriptQuery)}`);
  const status = data.status || 'COMPLETE'; const statusEl=$('transcript-status'); if(statusEl){statusEl.textContent=status==='COMPLETE'?'已完成':statusLabel(status);statusEl.className=`badge ${status==='COMPLETE'?'good':''}`;}
  if(!data.segments?.length){box.innerHTML='<div class="empty small">暂无可查看的逐字稿</div>';return;}
  box.innerHTML = data.segments.map(seg => `<div class="line"><time>${Number(seg.start||0).toFixed(2)}s</time><span>${esc(seg.text||'')}</span></div>`).join('') + `<div class="pager"><button ${data.offset<=0?'disabled':''} id="prev">上一页</button><span>${data.offset+1}–${Math.min(data.offset+data.segments.length,data.total)} / ${data.total}</span><button ${data.offset+data.segments.length>=data.total?'disabled':''} id="next">下一页</button></div>`;
  $('prev')?.addEventListener('click',()=>{state.transcriptOffset=Math.max(0,state.transcriptOffset-100);loadTranscript(id)}); $('next')?.addEventListener('click',()=>{state.transcriptOffset+=100;loadTranscript(id)});
}
$('search').addEventListener('input', renderList); load(); setInterval(()=>{ if(!state.selected) return; getJson('/api/sessions?limit=200').then(d=>{state.sessions=d.sessions||[];renderList();}); }, 30000);
