# TDengine 数据修复工具需求调研结论

## 0. 术语澄清（2026-03-03，用户确认）
- 修复对象统一为：`WAL`、`META`、`TSDB`。
- `META` 定义：时序数据的元数据（此前文档中提到的 `TDB` 在本项目中改称 `META`）。
- 计划、设计、实现文档全部按 `META` 术语推进。
- CLI 对外文案使用 `meta`；解析层暂保留 `tdb` 兼容映射，避免历史脚本立即失效。
- 兼容性已验证：`--file-type=meta` 与 `--file-type=tdb` 当前都可被解析并进入同一 `META` 分支。

## 1. 需求关键点（来自 `数据修复工具 - RS.md`）
- 入口保持为 `taosd -r`，不新增独立二进制。
- 命令参数核心：`--node-type --file-type --vnode-id --backup-path --mode --replica-node`。
- 只要求实现 vnode 的 `wal/tsdb/meta` 文件修复。
- 三种模式：
  - `force`：单副本自救。
  - `replica`：多副本触发恢复。
  - `copy`：从副本节点直接拷贝文件。
- 运行要求：备份原始损坏文件、持续进度输出、最终摘要、异常即停止。

## 2. 现有代码事实（按模块）

### 2.1 `taosd -r` 当前行为
- 文件：`source/dnode/mgmt/exe/dmMain.c`
- 事实：`-r` 只设置 `generateNewMeta = true`，没有任何 `--node-type` 等扩展参数。
- 影响：当前实现只触发“元数据重建”分支，不是完整修复工具。

### 2.2 元数据重建能力
- 文件：`source/dnode/vnode/src/meta/metaOpen.c`
- 事实：
  - `generateNewMeta` 在 `metaOpen()` 中触发 `metaGenerateNewMeta()`。
  - 逻辑主要是遍历现有 `uidIdx/tbDb` 重建 meta 目录并切换目录。
- 影响：它依赖现存元数据记录，不等同于“从 WAL/TSDB 反推并修复 META”。

### 2.3 WAL 已有自动修复能力
- 文件：`source/libs/wal/src/walMgmt.c`, `source/libs/wal/src/walMeta.c`
- 事实：
  - `walOpen()` 会自动调用 `walCheckAndRepairMeta()` 和 `walCheckAndRepairIdx()`。
  - 支持截断损坏段、重扫 lastVer、重建 idx、可选损坏目录备份删除（`tsWalDeleteOnCorruption`）。
- 影响：`force+wal` 可以以“编排 + 备份 + 日志增强”为主，复用现有能力。

### 2.4 TSDB 当前以“检测/容错”为主
- 文件：`source/dnode/vnode/src/tsdb/tsdbFS2.c`, `tsdbReaderWriter.c`
- 事实：
  - 有扫描和修补入口（`tsdbFSDoSanAndFix`），发现损坏时标记 `TSDB_FS_STATE_INCOMPLETE`。
  - 读取页有 checksum 校验，不通过报 `TSDB_CODE_FILE_CORRUPTED`。
- 影响：缺少“需求文档定义的块级提取 + 重建文件结构”完整工具链，需要新增。

### 2.5 现有 `restore dnode` 与本需求边界
- 代码：`mndDnode.c`, `mndVgroup.c`
- 文档：`docs/zh/08-operation/05-maintenance.md`
- 事实：
  - `restore dnode` 是“整节点/整 vnode”基于副本恢复，不是文件级修复。
  - 社区版分支里 `mndProcessRestoreDnodeReqImpl()` 为 stub（`#ifndef TD_ENTERPRISE`）。
- 影响：不能直接复用为文件修复工具，但其“副本恢复语义”可参考 `replica` 模式。

## 3. 可复用能力清单
- WAL 修复：`walCheckAndRepairMeta/Idx`。
- vnode 生命周期与批量并发打开/启动：`vmOpenVnodes`, `vmStartVnodes`。
- 启动过程进度上报：`tmsgReportStartup`。
- 配置与全局参数机制：`tglobal.h/c`（适合承载修复运行参数）。

## 4. 主要缺口
- CLI 缺口：未实现需求中的扩展参数。
- 编排缺口：无“按 vnode + file-type + mode”执行框架。
- 状态缺口：无 repair journal/state 持久化，不支持中断续跑。
- 备份缺口：无统一的修复前备份与回滚策略。
- 测试缺口：缺少针对“损坏样本 -> 修复 -> 可启动/可查询”的自动验收流水线。

## 5. 架构建议（结论）
- 采取“增量演进”而非重写：
  - 保留 `taosd -r` 单入口；
  - 新增 repair session 编排层；
  - 模式处理器复用现有 WAL/restore 能力，逐步补齐 TSDB/META。
- 先做可交付 MVP：`force + wal`，再扩展 TSDB、META、replica、copy。

## 6. 待确认项
- `--node-type` 在首版是否仅允许 `vnode`，其余值先报 `not supported`（建议是）。
- `copy` 模式 SSH 实现范围：首版是否允许仅 Linux + ssh/scp 命令依赖。
- `replica` 模式是否允许复用现有 restore 事务动作，还是仅 vnode 局部触发。
- META 反推规则首批覆盖深度（优先保证“可启动 + 查询未损坏数据”）。

## 7. 执行期新增发现（2026-03-03）
- `T1.1` 参数解析模型落在 `source/common` 更合适：
  - 可直接复用 `commonTest` 做 TDD；
  - 降低 `mgmt/exe` 层早期改动复杂度。
- 当前测试运行环境下，直接执行带 ASan 的 `commonTest` 会触发 `LeakSanitizer` 的 ptrace 限制；
  - `ctest` 验证时建议统一加 `ASAN_OPTIONS=detect_leaks=0`。
- 在本仓库中并行执行“build + test”会造成竞态（测试可能使用旧二进制）；
  - 关键验证步骤应保持顺序：`build -> test`。
- `T1.2` 已在 `dmMain.c` 接入 `--node-type/--file-type/--vnode-id`：
  - 同时支持 `--opt value` 与 `--opt=value` 两种格式；
  - 非法值在参数解析阶段即返回 `TSDB_CODE_INVALID_CFG`（运行验证退出码为 `25`）。
- `T1.3` 已接入 `--backup-path/--mode/--replica-node`：
  - 解析能力继续复用 `tRepairParseCliOption()`；
  - `mode` 走枚举解析，`backup-path/replica-node` 走字符串长度校验；
  - 非法值同样在参数解析阶段失败（例如 `--mode bad-mode`）。
- `T1.4` 参数组合校验已落地：
  - repair 参数出现时必须带 `-r`；
  - 必选项：`node-type/file-type/mode`；
  - `node-type` 与 `file-type` 需满足兼容矩阵；
  - `vnode-id` 仅允许且要求在 `node-type=vnode` 场景；
  - `mode=copy` 必须提供 `replica-node`，其他模式禁止提供 `replica-node`。
- `T1.5` 帮助文案已更新，`taosd --help` 已显示 repair 相关选项说明（`-r` 及 6 个长参数）。
- `T1.6` 已由 `RepairOptionParseTest` 覆盖：
  - 包含枚举解析、CLI 参数解析、参数组合校验三层测试；
  - 当前共 10 条用例，`commonTest` 回归通过。
- `T2.1` 已落地 repair 运行时上下文：
  - 新增 `SRepairCtx`（会话标识、启动时间、运行参数快照）；
  - 新增 `tRepairInitCtx()`，在 `dmMain.c` 参数校验后立即初始化上下文；
  - `sessionId` 采用 `repair-<startTimeMs>` 规则，便于后续日志/状态文件命名。
- `T2.2` 已落地 vnode 过滤基础能力：
  - 在 `SRepairCtx` 内把 `vnode-id` 字符串解析为 `int32_t` 数组缓存（去重、非法值拒绝）；
  - 新增 `tRepairShouldRepairVnode()` 作为后续 vnode 遍历过滤入口；
  - 非法 `--vnode-id`（如 `2,a`）现在会在上下文初始化阶段失败并中止执行。
- TDengine 代码约束补充：
  - `strtol/strtoll` 在仓库中被禁止（宏重定义为 forbid）；
  - 数值解析必须使用 `taosStr2Int32/taosStr2Int64` 等封装函数，避免触发编译错误。
- `T2.3` 预检能力已落地在 `source/common`：
  - 新增 `tRepairPrecheck()`，检查项包括：`dataDir` 存在、`backup-path`（若配置）存在、磁盘可用空间阈值、`vnode/<id>/<wal|tsdb|meta>` 目标路径存在性；
  - 磁盘空间检查复用 `taosGetDiskSize()`，当可用空间低于阈值时返回 `TSDB_CODE_NO_ENOUGH_DISKSPACE`；
  - 预检函数目前对 `nodeType=vnode` 做目标路径校验，其他 node type 暂放行（为后续阶段保留扩展空间）。
- `dmMain.c` 已在配置加载完成后、`dmInit()` 前接入预检：
  - 最小可用空间阈值使用 `tsDataSpace.reserved`（若为 0 则不做空间下限）；
  - 失败时立即退出并输出 `failed repair precheck: <reason>`，保持 fail-fast。
- 运行验证注意事项：
  - 在当前 ASan 构建里，命令行使用 `-o /tmp` 会触发 `osDir.c:taosMulModeMkDir` 的已有栈越界问题；
  - 使用 `-o /tmp/taoslog` 可绕开该环境问题并完成本任务的预检路径验证。
- `T2.4` 备份管理器新增目录约定并已接入主流程：
  - 新增 `tRepairPrepareBackupDir()`，对每个目标 vnode 预创建目录；
  - 路径规则：
    - 显式 `--backup-path`：`<backup-path>/<sessionId>/vnode<id>/<fileType>`；
    - 未配置时默认：`<dataDir>/backup/<sessionId>/vnode<id>/<fileType>`；
  - `dmMain.c` 在 precheck 后、`dmInit()` 前对全部目标 vnode 执行目录创建，任一失败直接中止。
- `T2.5` 修复会话追踪文件已落地：
  - 新增 `tRepairPrepareSessionFiles()`、`tRepairAppendSessionLog()`、`tRepairWriteSessionState()`；
  - 会话目录固定为 `<backupBase>/<sessionId>/`，其中写入：
    - `repair.log`（带毫秒时间戳的 append 日志）；
    - `repair.state.json`（包含 `sessionId/startTimeMs/nodeType/fileType/mode/step/status/doneVnodes/totalVnodes/updatedAtMs` 等字段）。
  - `repair.state.json` 使用 `*.tmp` 临时文件 + `rename` 的原子落盘方式，降低中断时状态文件损坏风险。
  - `dmMain.c` 已接入 session 文件初始化、precheck/备份步骤日志写入与 preflight 完成态更新；初始化失败会 fail-fast 返回。
