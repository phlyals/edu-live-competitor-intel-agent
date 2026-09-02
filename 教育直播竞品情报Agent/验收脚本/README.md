# 教育直播竞品情报 Agent 验收脚本

Python 3.11+，仅新增验收工具，不导入或修改生产 worker。三个入口可以分别运行；请一起保留同目录的 `acceptance_common.py` 和 `check_transcription.py`。脚本三直接调用脚本二的 `coverage()`，在**同一个只读数据库快照**内取得相同结构的输出，不需要先生成覆盖率文件，也不会使用旧的缓存结果。

## 安装与运行

需要本机已安装 FFprobe。Python 驱动使用 `psycopg2`；`psycopg2-binary` 是其预编译发行包，不依赖 `ffmpeg-python`。

```bash
cd '/Users/mac/Documents/agent架构师/教育直播竞品情报Agent/验收脚本'
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

.venv/bin/python -B check_recording.py '场次ID'
.venv/bin/python -B check_transcription.py '场次ID'
.venv/bin/python -B check_analysis.py '场次ID'
```

默认只读取 `~/.hermes/profiles/edu_live_competitor_intel/runtime/v3/v3_config.json` 的 `postgresql.dsn`，可通过 `--config /绝对路径/config.json` 覆盖；环境变量 `ACCEPTANCE_DATABASE_URL` 优先于配置文件。不要把密码写入脚本、提交版本库或放入命令行参数。建议配置数据库的专用只读角色。

输出同时打印到 stdout 并原子写入同级 `../验收结果/`：

```text
{场次ID}_check_recording.json
{场次ID}_check_transcription.json
{场次ID}_check_analysis.json
```

`--output-dir` 可修改结果目录；重复运行覆盖同一场次的同名验收 JSON，不会覆盖源产物。普通场次 ID（包括中文）原样保留；斜杠、反斜杠、百分号、控制字符进行百分号编码以防路径穿越。超过文件系统 255 字节文件名限制的 ID 返回 JSON 错误，不截断、不生成可能冲突的文件名。

其他参数：`--db-schema public`、`--layout auto|runtime-v3|canonical`、`--data-root /媒体根目录`、`--ffprobe /FFprobe路径`、`--probe-timeout 60`。数据库中的相对文件路径必须提供 `--data-root`，不会按当前 shell 目录猜测。

多个转录版本需传 `--transcript-id`；多个报告需传 `--report-id`。脚本三优先使用报告明确引用的转录 ID，显式参数与之冲突会失败，不会拿另一个版本凑足覆盖。

## JSON 与退出码

始终保留任务要求的字段，并补充 `session_id`、`script`、`checked_at`、`status`、`message`，异常时带 `error_code`。无法获取的时长、大小或覆盖率为 `null`，不伪装成零。

| status | 退出码 | 含义 |
|---|---:|---|
| PASS | 0 | 检查成功；脚本一/三符合条件，脚本二完成覆盖率计算 |
| FAIL | 1 | 不符合完整性条件，或必要产物不存在/已失效 |
| UNCOMPUTABLE | 1 | 数据不足，无法计算，不抛 traceback |
| FORMAT_ERROR | 1 | 格式异常，不强行解析 |
| ERROR | 2 | 数据库、依赖、FFprobe 或输出目录等运行问题 |

**脚本二没有自设合格率门槛。** `PASS` 表示计算成功，覆盖率为 0 也可能 `PASS`，应读取 `coverage_rate` 和 `gaps`。`--help` 为正常帮助文本；缺少场次 ID、参数无法解析或目标目录不可写时，只能向 stdout 返回 JSON，无法保证产生结果文件。

## 判定口径

### 录制

- `expected_duration_seconds = ended_at - started_at`，不读取数据库的 COMPLETE 标签作为通过依据。
- `abs(实际时长 - 预期时长) / 预期时长 <= 0.05` 才通过，边界包含 5%。比较前不舍入。
- 时长和大小来自本地 FFprobe。`file_size_mb` 使用 `bytes / 1024²` 并四舍五入至两位；JSON 数字不保证显示末尾的零。
- 缺少结束时间、无效/非正的预期时长、时区不一致均不能通过。
- 只检查时长相符，**不能证明从真实开播开始录制、没有重复画面、音画质量正常或不存在互相抵消的断点**，仍需人工抽查。

### 转录

- 支持 `{"segments":[{"start":0,"end":10,"text":"正文"}]}`，也支持明确的 `start_time/end_time` 字段。单位只能是秒，允许数值字符串；不猜测毫秒、时钟文本或 Markdown。
- 原文字符数按片段 `text` 的 Unicode 字符数相加，包含空白/标点，不重复累加顶层全文；没有片段时可统计顶层 `text`。
- 时间戳排序后只合并重叠或严格相邻片段，不填平静音；`covered_segments` 返回合并后的范围。`gaps` 包含开头、中间、结尾的所有空白。
- `coverage_rate` 是 0～1 的比例，保留两位，不是百分数；额外保留未舍入的 `covered_duration_seconds`。四舍五入到 `1.0` 不代表无缺口，应查看 `gaps`。
- 任一片段时间戳缺失/无效/反向/越界都标记“无法计算覆盖率”，`coverage_rate=null`，保留可确认的片段，`gaps=[]` 表示未计算，**不是无缺口**。容许末端不超过 1 毫秒的容器舍入差，并裁到音频终点。
- 空片段列表且没有正文时表示零覆盖；有正文却没有时间戳不能算零覆盖。
- 音频总时长由 FFprobe 实测，必须有音频流。没有音频文件不以 ASR 自报时长代替。
- V3 仅自动选择 `FULL_SESSION` 转录；样本或没有整场标记的历史转录不能代表整场。显式 ID 也不能绕过该要求。不同分段的局部时间轴不会直接拼成整场。
- 覆盖率是时间戳覆盖度，静音也在分母内，**不等于识别准确率**，也不能独立证明上游录制完整。

### 分析

明确支持如下七模块结构，以及可选的 `{"result": {...}}` 包装：

```json
{
  "session_id": "场次ID",
  "transcript_id": "转录版本ID",
  "modules": [
    {"name": "开场", "timestamps": [{"start": 0, "end": 10}]},
    {"name": "干货", "timestamps": [{"start": 10, "end": 20}]},
    {"name": "需求", "timestamps": [{"start": 20, "end": 30}]},
    {"name": "信任", "timestamps": [{"start": 30, "end": 40}]},
    {"name": "商品承接", "timestamps": [{"start": 40, "end": 50}]},
    {"name": "成交", "timestamps": [{"start": 50, "end": 60}]},
    {"name": "答疑", "timestamps": [{"start": 60, "end": 70}]}
  ]
}
```

也支持 `modules` 为 `{"开场":{"timestamps":[...]}, ...}` 的名称映射；引用数组可用 `evidence_refs` 替代 `timestamps`，不能同时提供。引用支持 `start_time/end_time`，点引用使用相等的起止秒数。

`modules_with_timestamps` 元素为 `{"module":"开场","has_timestamps":true}`；`timestamps_in_coverage` 元素包含模块级 `in_coverage` 和逐条 `references`。缺少模块/引用、任一引用跨过缺口或越界、转录覆盖无法计算，都不能通过。无法验证的逐条引用值为 `null`，模块级 `in_coverage=false`。

实际 V3 的 `hook / pain_points / claims / cta / interaction_patterns / risks / evidence_refs` 旧格式标记 **FORMAT_ERROR / 格式异常**；不把 `hook` 猜成“开场”，不将全局引用复制给每个模块。格式异常时模块列表为空、七模块视为尚未验证，不表示源文件完全没有相关内容。多个报告、重复模块、字符串模块、非法引用也不强行解析。

## 两种存储适配

| 数据 | runtime-v3（当前系统） | canonical（需求中的标准表） |
|---|---|---|
| 场次 | `live_sessions` | `sessions` |
| 录像路径 | `recording_segments.path`；优先唯一的 `整场直播.ts`，否则仅接受唯一文件 | `sessions.recording_path` |
| 转录 | `transcripts.output_path` 指向本地 JSON；无路径时可读 `metadata_json.segments` | `transcripts.transcript_json`，或 `output_path` 本地 JSON，二者不能同时存在 |
| 音频 | `transcripts.source_path` | `transcripts.audio_path`，其次 `source_path`，其次 `sessions.audio_path` |
| 报告 | `analyses.output_path` | `analysis_reports.report_json`，或 `output_path` 本地 JSON，二者不能同时存在 |

两种布局都以 `session_id` 查询。标准表还使用 `transcript_id`、`report_id`；可选 `status` 缺省视为 COMPLETE、报告 `lineage_state` 缺省为 CURRENT。若存在这些状态字段，则必须为 COMPLETE/CURRENT。V3 的报告 ID 是 `analysis_id`。`auto` 仅在指定 schema 恰好存在一种场次表时选择布局，表名不匹配不会自行迁移或建表。

## 只读边界

- PostgreSQL 连接启用 `default_transaction_read_only=on`，事务为 `READ ONLY + REPEATABLE READ`；只发 SELECT，并在结束时 rollback/close。参数化查询防止 ID 注入。
- 不调用业务初始化、录制、音频提取、转录、分析或飞书接口；不启动现有 worker。
- FFprobe 只允许 `file,pipe` 协议，拒绝 URL 媒体及远程报告；不会联网下载文件。
- 不修改任何业务表、录制、音频、转录或报告。脚本自身唯一的显式磁盘写入是验收 JSON；推荐用 `python -B` 避免解释器缓存。
- 只读数据库快照不锁定外部文件。应在直播结束、录制收尾、转录及报告已落盘后执行；有并发写入时稍后重跑。
- 不内置轮询、定时任务或直播结束事件订阅。本交付没有修改现有调度；未来可由已有完成事件调用这些入口。

## 测试

```bash
.venv/bin/python -B -m unittest discover -s . -p 'test_*.py' -v
```

集成测试另需 `initdb`、`pg_ctl`。测试自行创建仅监听临时 Unix socket 的 PostgreSQL 集群，绝不使用生产 DSN；测试夹具写入仅发生在临时集群。真实 FFprobe 读取标准库生成的 10 秒 WAV，验证两个数据库布局的三个 CLI、JSON 文件、数据库写操作被拒绝、数据库内容和媒体哈希保持不变。测试结束关闭并清理集群。

**10 秒 WAV 是合成测试数据，不能用作真实直播验收。** 真实场次运行情况见 `../验收结果/验收说明.md`。
