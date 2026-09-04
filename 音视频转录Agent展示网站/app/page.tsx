const capabilities = [
  ['01', '直播预检', '检查链接、直播状态、录制工具、磁盘空间和飞书连接。'],
  ['02', '指定时长录制', '用户明确回复“继续”后开始，防重复、不覆盖、保留原始录像。'],
  ['03', '本地中文转录', '使用 Whisper 生成带时间戳逐字稿，并标记低置信度片段。'],
  ['04', '保守清洗', '整理明显 ASR 噪声，保留原意、时间戳和原始逐字稿。'],
  ['05', '单场内容分析', '分析主题、阶段、需求、信任和 5 元课包承接逻辑。'],
  ['06', '失败后恢复', '通过任务 ID 和 Checkpoint 从已完成的位置继续处理。'],
];

const boundaries = [
  '不自动寻找同行',
  '不长期监控主播',
  '不进行跨场比较',
  '不生成话术迁移',
  '不读取销量或转化率',
  '展示页不连接真实 Agent',
];

const steps = ['直播链接', '预检', '等待确认', '录制', '转录', '清洗', '单场分析'];

export default function Home() {
  return (
    <main>
      <header className="nav shell">
        <a className="brand" href="#top" aria-label="音视频转录 Agent 首页">
          <span className="brandMark">AV</span>
          <span>音视频转录 Agent</span>
        </a>
        <span className="demoBadge">公开展示 · 无 API 连接</span>
      </header>

      <section className="hero shell" id="top">
        <div className="heroCopy">
          <p className="eyebrow">SINGLE LIVE RESEARCH AGENT</p>
          <h1>把一场抖音直播，变成可回看、可检索、可分析的内容档案。</h1>
          <p className="lead">
            输入一个正在直播的抖音链接，自动完成录制、音频提取、中文转录、保守清洗和单场内容分析。
          </p>
          <div className="heroActions">
            <a className="button primary" href="#capabilities">查看功能</a>
            <a className="button secondary" href="#sample">查看真实样例</a>
          </div>
          <p className="privacyNote">本页面仅用于能力展示，不调用真实 Agent，不保存访客数据。</p>
        </div>

        <div className="resultCard" aria-label="Agent 任务结果示例">
          <div className="resultTop">
            <div>
              <span className="muted">任务状态</span>
              <strong>AV-DEMO-001</strong>
            </div>
            <span className="statusDot">已完成</span>
          </div>
          <div className="progressLine"><span /></div>
          <div className="resultStats">
            <div><span>请求录制</span><strong>20 分钟</strong></div>
            <div><span>完整性</span><strong>COMPLETE</strong></div>
            <div><span>输出文件</span><strong>6 个</strong></div>
          </div>
          <div className="fileList">
            <span>00_直播录像.ts</span>
            <span>02_逐字稿_带时间戳.md</span>
            <span>04_竞品直播分析.md</span>
          </div>
        </div>
      </section>

      <section className="flowSection">
        <div className="shell">
          <p className="sectionLabel">工作流程</p>
          <div className="flow" aria-label="直播处理流程">
            {steps.map((step, index) => (
              <div className="flowItem" key={step}>
                <span>{String(index + 1).padStart(2, '0')}</span>
                <strong>{step}</strong>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="section shell" id="capabilities">
        <div className="sectionHeading">
          <p className="sectionLabel">核心能力</p>
          <h2>从链接到研究档案，一条链路完成。</h2>
        </div>
        <div className="capabilityGrid">
          {capabilities.map(([number, title, description]) => (
            <article className="capabilityCard" key={number}>
              <span className="cardNumber">{number}</span>
              <h3>{title}</h3>
              <p>{description}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="sampleSection" id="sample">
        <div className="shell sampleGrid">
          <div>
            <p className="sectionLabel">真实样例</p>
            <h2>用真实直播素材完成过链路验证。</h2>
            <p className="sampleIntro">
              一次 5 分钟真实直播任务生成了录像、音频和两版逐字稿；新版分析器随后基于该真实逐字稿完成单场分析并通过自动验收。
            </p>
          </div>
          <div className="metrics">
            <div><strong>63.9 MB</strong><span>真实直播录像</span></div>
            <div><strong>37</strong><span>时间戳片段</span></div>
            <div><strong>107</strong><span>分析定位时间戳</span></div>
            <div><strong>通过</strong><span>自动分析验收</span></div>
          </div>
        </div>
      </section>

      <section className="section shell boundarySection">
        <div className="sectionHeading">
          <p className="sectionLabel">能力边界</p>
          <h2>它明确知道什么不做。</h2>
          <p>边界清楚，才能让展示可信，也避免暴露真实权限和数据。</p>
        </div>
        <div className="boundaryList">
          {boundaries.map((item) => <span key={item}>{item}</span>)}
        </div>
      </section>

      <footer>
        <div className="shell footerInner">
          <div>
            <strong>音视频转录 Agent</strong>
            <p>单场直播录制、转录与内容分析。</p>
          </div>
          <p className="footerNotice">静态展示页面 · 无登录 · 无数据库 · 无 API Key</p>
        </div>
      </footer>
    </main>
  );
}
