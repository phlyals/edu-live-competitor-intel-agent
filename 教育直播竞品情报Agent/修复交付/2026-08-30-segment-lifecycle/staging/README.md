# recording_segments 生命周期治理（隔离候选）

本目录只包含候选代码、迁移和测试。它没有被安装到生产 Runtime，也没有连接或修改生产 PostgreSQL、服务及媒体。

## 目标状态

- `CANONICAL_ACTIVE`：唯一允许进入转录 pipeline 的整场主媒体。
- `SOURCE_RETAINED`：仍在磁盘上的来源/刷新分段，只作为证据保留。
- `SOURCE_SUPERSEDED`：已由校验通过的 canonical 媒体取代、且原路径已不存在的来源分段。
- `LOST_REVIEW`：路径缺失或哈希异常，但没有足够证据证明已被 canonical 安全取代，必须人工复核。
- `UNCLASSIFIED`：新增列的 fail-closed 默认值；迁移完成前 pipeline 不消费。

## 安全不变量

1. 仅 `media_manifests.status='VERIFIED'`、manifest 文件哈希匹配、manifest JSON 内 final 文件哈希匹配时，才允许产生 `SOURCE_SUPERSEDED`。
2. 没有可验证 canonical 的缺失分段一律 `LOST_REVIEW`。
3. 历史 transcript 只在 `WAITING_TOOL + source_path 缺失 + metadata.segment_id/source_segment_id 指向 SOURCE_SUPERSEDED` 三项同时成立时改为 `CANCELLED_SUPERSEDED_SOURCE`；不删除行、不覆盖原 metadata。
4. dry-run 不执行 DDL/DML；apply 在单一事务内执行，重复 apply 必须无业务变化。
5. 部署顺序必须是：备份 → 暂停 pipeline/finalizer → migration dry-run → 人工核对计数 → apply → 部署代码 → 回归 → 恢复服务。

## 目录

- `candidate/runtime/v3/`：生产文件的候选副本。
- `sql/001_recording_segment_lifecycle.sql`：显式、幂等 PostgreSQL DDL。
- `migration/segment_lifecycle_migration.py`：默认 dry-run 的 PG 历史分类和 transcript 治理工具。
- `tests/`：SQLite 分类测试和临时 PostgreSQL 集成测试。
- `patches/segment-lifecycle.patch`：相对当前生产文件的候选补丁。
- `上线与回滚.md`：备份、灰度、回滚和 trade-off。

