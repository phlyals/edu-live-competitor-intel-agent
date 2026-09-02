# 竞品情报 Agent 入站修复与双轮验收

更新：本报告下述浏览器等待条件已在2026-08-28解除，原任务已完成。最新结果见同目录 `WORKFLOW_REPAIR_20260828.md`；不要继续把本报告中的历史WAITING_HUMAN当成当前状态。

日期：2026-08-27

## 已完成

- Profile 本地入站模块识别抖音商品分享短链、百应链接及明确业务命令。
- CAPTURED / DUPLICATE / NOT_BUSINESS / REJECTED / CAPTURE_FAILED 独立分支；业务消息不再误入通用聊天。
- Gateway 通过 V3 专属 Python 子进程访问 PostgreSQL，避免共享 Gateway 缺少 psycopg。
- 当前用户授权名单与 V3 配置一致；其他 Agent 不进入该数据库。
- Inbox、Task、Checkpoint、Event 和受理回执 Outbox 同事务生成。
- 受理回执使用稳定幂等键；重复投递不重复回复；失败可自动补交。
- 飞书子进程显式设置 Node PATH 和飞书域名 NO_PROXY。
- 新增真实 WebSocket 状态心跳 feishu-ingress-v3。
- 短链按白名单逐跳解析，已验证原商品ID为 3838016038189006849。
- 修复场次投影 scene_state 未赋值；等待流不再显示失败，单个媒体段不再被当作整场完整。
- 10条该异常导致的投影死信已补交并回读验证，保留审计记录并标记 resolved。
- 扫描器验证实际输出目录可写，而不是要求外置卷根目录可写；保留50GB空间门槛。
- 扫描器固定使用竞品情报 Profile 专用 CUA socket，不回退全局实例。
- 浏览器未就绪时保留任务为 WAITING_HUMAN，并通过飞书说明原因。

## 两轮程序自检

- 第一轮：生产启动解释器执行21项回归测试，全部通过；编译检查通过。
- 第二轮：外置盘解释器再次执行21项回归测试，全部通过；真实 Gateway 解释器跨环境调用通过。
- 隔离数据库100次并发投递：1个Inbox、1个Task、1个Checkpoint、1个Event、1条受理Outbox。
- 真实PostgreSQL原消息重复100次：仍为1个Inbox、1个Task、1条受理回执。
- 当前19个场次全部通过真实飞书字段 dry-run。
- 原消息受理回执与等待浏览器通知均已从正确 App ID 回读确认。
- feishu-ingress-v3 为 READY；竞品情报与音视频 Agent Gateway 均存活。

## 未完成的端到端外部前置条件

原消息ID：om_x100b67ccc35210a4c4c5034d5d73e58

保留任务ID：task_f3f2e806c9514e65a4c8153a93683a9a

当前状态：WAITING_HUMAN / BUYIN_LOGIN_REQUIRED。

Tabbit截图及原生页面检查显示抖音电商门户与登录入口，没有可验证的已登录百应商品决策页。
因此尚未宣称该商品完整扫描成功，也未将整条业务流水线标记为验收通过。
需要用户在Tabbit登录巨量百应并打开商品决策页，再恢复同一任务；不需要重新发送商品。

备份：/Volumes/ExternalStorage/AgentInfrastructure/backups/edu-v3-ingress-fix-20260827
