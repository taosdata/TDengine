# TDengine 数据修复工具设计文档（`taosd -r` 扩展）

## 1. 背景与目标
- 需求来源：`/Projects/work/TDengine/.vscode/dev/数据修复工具 - RS.md`
- 目标：在不新增独立程序的前提下，把 `taosd -r` 扩展为可控、可追踪、可恢复的数据修复工具。
- 首期范围：`--node-type=vnode`，`--file-type=wal|tsdb|meta`，支持 `force/replica/copy` 三模式编排。
- 术语约定：本文中的 `META` 即“时序数据元数据”（历史文档中的 `TDB`）。

## 2. 方案比较（2-3 种）

### 方案 A：增量扩展 `taosd -r`（推荐）
- 做法：
  - 在 `dmMain.c` 增加新参数解析与校验；
  - 新增 repair session 编排层；
  - WAL 修复复用现有 `walCheckAndRepair*`；
  - TSDB/META 逐步补齐 repair handler。
- 优点：
  - 改动路径短，复用现有启动链与 vnode 生命周期；
  - 适合分阶段交付（先 WAL，再 TSDB/META）；
  - 运维入口统一（符合需求）。
- 缺点：
  - 启动流程与修复流程耦合，需要谨慎处理正常启动路径回归风险。

### 方案 B：新增独立 repair 子程序
- 做法：新增 `taosrepair` 风格工具，绕开 `taosd` 启动路径。
- 优点：
  - 模块边界清晰，便于封闭测试。
- 缺点：
  - 与需求“基于 taosd -r 扩展”冲突；
  - 需要重复接入大量现有内部模块和配置解析逻辑。

### 方案 C：按 SQL 管理命令驱动修复（类似 restore dnode）
- 做法：走 mnode 事务，远程驱动 dnode 执行文件修复。
- 优点：
  - 统一集群控制平面。
- 缺点：
  - 文件级修复语义不适合纯远程事务；
  - 社区版/企业版分叉明显，落地周期长。

## 3. 推荐方案
- 采用方案 A。
- 原因：最贴近需求、复用度最高、可按 1 小时任务切片渐进落地。

## 4. 目标架构

### 4.1 总体模块
- `CLI Parser`：解析 `--node-type/--file-type/--vnode-id/--backup-path/--mode/--replica-node`。
- `Validator`：参数合法性和组合规则校验。
- `Repair Session`：会话上下文、任务分解、并发控制、状态持久化。
- `Preflight`：空间检查、文件存在性检查、权限检查。
- `Backup Manager`：修复前备份原始文件。
- `Mode Handler`：
  - `force` -> `wal/tsdb/meta` 子处理器；
  - `replica` -> 副本恢复触发流程；
  - `copy` -> 远端文件拷贝流程。
- `Reporter`：过程进度、repair.log、摘要输出。

### 4.2 数据流（简化）
1. `taosd -r ...` 启动。
2. 解析参数 -> 校验。
3. 构建 `repair session`，定位目标 vnode 列表。
4. 执行 preflight，创建备份目录与状态文件。
5. 按 `mode + file-type` 调度处理器。
6. 持续写入 `repair.log` 和 `repair.state.json`。
7. 输出汇总：成功/失败 vnode、恢复条目、损坏条目、耗时。

## 5. 模式级设计

### 5.1 force 模式
- `wal`：
  - 优先复用 `walCheckAndRepairMeta/Idx`；
  - 增加“修复前备份 + 结构化日志”；
  - 增加可重放性检查结果归档。
- `tsdb`：
  - 枚举 `data/head/sma/stt`；
  - 校验块级完整性；
  - 保留可恢复块、剔除不可恢复块；
  - 重建最小可用结构。
- `meta`：
  - 解析可读元数据；
  - 联合 WAL/TSDB 推导缺失元数据；
  - 对无法推导项打标并告警。

### 5.2 replica 模式
- 目标：触发当前损坏 vnode 从健康副本进行全量同步。
- 设计方向：
  - 将本地损坏副本置为不可读写状态；
  - 通过版本/任期策略触发同步；
  - 复用现有 restore/vgroup 事务动作（需评估社区版路径）。

### 5.3 copy 模式
- 目标：当数据体量大时，用“离线副本文件拷贝”快速恢复。
- 核心步骤：
  - 解析 `--replica-node`；
  - 建立远端连接；
  - 全量拷贝目标 vnode 目录文件；
  - 同步权限与 owner；
  - 完成后一致性校验。

## 6. 安全与一致性设计
- 任何写操作前必须完成备份（`--backup-path=none` 例外时需告警）。
- preflight 失败即停止修复，不进入破坏性步骤。
- 关键步骤写状态检查点，异常退出后可恢复续跑。
- 默认“先保守后激进”：优先保留可确认正确的数据。

## 7. 会话中断恢复机制（开发与运行双层）

### 7.1 开发过程恢复
- 以仓库根目录 `task_plan.md/findings.md/progress.md` 作为持久化工作记忆。
- 每完成 1 个任务立即更新状态与日志。
- 恢复时直接定位 `in_progress` 任务继续。

### 7.2 运行时修复恢复
- 每次修复会生成：
  - `repair.log`：人类可读日志；
  - `repair.state.json`：机器可读状态检查点。
- 下次执行同一任务时可读取状态文件，跳过已完成步骤，继续未完成步骤。

## 8. 测试策略
- 单元测试：
  - 参数解析与校验；
  - 备份路径生成与状态文件读写；
  - mode dispatch 路由。
- 组件测试：
  - WAL 修复样例（损坏 idx、截断 log）。
  - TSDB 块损坏样例。
  - META 元数据缺失样例。
- 系统测试：
  - 单副本 force 场景。
  - 三副本 replica/copy 场景。
  - 故障注入：磁盘不足、文件缺失、副本不可达。

## 9. 风险与缓解
- 风险：TSDB/META 修复复杂度高，首版难以一次做到“全恢复”。
  - 缓解：先交付 WAL MVP，分阶段扩展恢复深度。
- 风险：社区版/企业版恢复能力分叉。
  - 缓解：将 `replica/copy` 路径做能力探测和清晰报错。
- 风险：修复逻辑影响正常启动路径。
  - 缓解：修复逻辑只在 `-r` 显式开启，默认路径零影响。

## 10. 设计确认点
- 是否同意按优先级 `force+wal -> force+tsdb -> force+meta -> replica -> copy` 推进。
- 是否同意首版 `--node-type` 只支持 `vnode`，其他值先返回 `not supported`。
- 是否同意把“会话恢复”作为第一批基础设施（而不是后补）。
