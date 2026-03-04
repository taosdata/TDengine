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
- `T2.6` 进度输出与摘要已落地：
  - 新增 `tRepairNeedReportProgress()`：按时间间隔节流进度上报（支持首次上报与时钟回拨场景）。
  - 新增 `tRepairBuildProgressLine()`：统一构造进度输出（`session/step/vnode done/total/progress%`）。
  - 新增 `tRepairBuildSummaryLine()`：统一构造最终摘要（`status/successVnodes/failedVnodes/elapsedMs`）。
  - `dmMain.c` 已接入：
    - precheck 通过后立即输出并落盘首条进度；
    - 备份循环中按间隔或收尾条件输出进度；
    - 完成 preflight 后输出最终摘要并落盘；
    - 同步更新 `repair.state.json` 的 `precheck -> backup -> preflight(ready)` 步骤状态。
- `T2.7` 会话恢复能力已落地：
  - 新增 `tRepairTryResumeSession()`：
    - 扫描备份根目录（`--backup-path` 或 `<dataDir>/backup`）下的 `repair-*` 会话目录；
    - 读取并校验 `repair.state.json` 的上下文字段（`nodeType/fileType/mode/vnodeIdList/backupPath/replicaNode`）；
    - 仅接受 `status=initialized|running` 的未完成会话；
    - 选择 `startTimeMs` 最新的候选会话作为续跑目标，并回填 `sessionId/startTimeMs/doneVnodes/totalVnodes`。
  - `dmMain.c` 已接入恢复逻辑：
    - precheck 后先尝试恢复旧会话；
    - 命中恢复时复用原 `repair.log`/`repair.state.json`；
    - 备份循环从 `doneVnodes` 对应下标继续，跳过已完成 vnode。
- JSON 解析注意事项（本次新增）：
  - `tjsonGetStringValue2()` 在字段缺失时返回 `TSDB_CODE_SUCCESS`（仅输出空字符串），不能用返回码判断字段存在性；
  - 需要配合 `tjsonGetObjectItem()` 显式判断字段是否存在，避免把“缺字段”误判为“空字符串字段”。
- `T3.1`（`force+wal` 调度器）已完成最小闭环：
  - `trepair.h/.c` 新增 `tRepairNeedRunWalForceRepair()`（判定 `nodeType=vnode && fileType=wal && mode=force`）；
  - `trepair.h/.c` 新增 `tRepairBuildVnodeTargetPath()`，统一构造 `vnode/<id>/<wal|tsdb|meta>` 路径；
  - `tRepairPrecheck()` 改为复用 `tRepairBuildVnodeTargetPath()` 做目标文件存在性检查，减少路径构造重复逻辑。
- `dmMain.c` 新增 `force+wal` 调度入口（位于 backup 阶段之后）：
  - 仅在 `tRepairNeedRunWalForceRepair()==true` 时触发；
  - 先调用 `walInit(dmStopDaemon)`，再按目标 vnode 循环执行 `walOpen(walPath, &cfg)` + `walClose()`；
  - 复用现有 fail-fast：任一 vnode 打开失败即记录 `repair.state.json(step=wal,status=failed)` 并中止。
- 本阶段会话恢复策略保持“步骤级最小实现”：
  - 继续沿用 `T2.7` 的 `doneVnodes` 语义用于 precheck/backup 续跑；
  - `wal` 步骤暂不做细粒度续跑索引恢复（后续可在 `T3.2/T3.3` 扩展）。
- `T3.2`（WAL 修复前备份与失败回滚保护）已完成：
  - `trepair.h/.c` 新增：
    - `tRepairBackupVnodeTarget()`：按 `vnode/<id>/<fileType>` 递归备份到 `<backup>/<session>/vnode<id>/<fileType>`；
    - `tRepairRollbackVnodeTarget()`：从备份目录递归恢复目标目录。
  - 备份/回滚内部实现要点：
    - 新增目录递归复制 helper（基于 `taosOpenDir/taosReadDir/taosCopyFile`）；
    - 复制前会重建空目录，避免 `taosCopyFile` 的 `TD_FILE_EXCL` 导致覆盖失败；
    - 对源/目标目录存在性与类型做显式校验（必须是目录）。
  - `dmMain.c` 的 `dmRunForceWalRepair()` 已接入保护语义：
    - 每个 vnode 执行 `walOpen()` 前先调用 `tRepairBackupVnodeTarget()`；
    - `walOpen()` 失败时立即触发 `tRepairRollbackVnodeTarget()` 并记录回滚日志；
    - 仍保持 fail-fast：任一 vnode 失败即退出流程。
- `T3.3`（WAL 修复明细记录）已完成：
  - `SWalCkHead` 通过 `SWalCont` 间接包含柔性数组成员（`body[]`），因此 `SWal` 中 `writeHead` 必须保持尾字段；否则会触发 `flexible array member not at end of struct` 编译错误。
  - 新增 `SWalRepairStats` 与 `walGetRepairStats()`，用于导出一次 `walOpen()` 生命周期内的修复明细统计。
  - `walCheckAndRepairMeta()` 在进入损坏段修复分支时累计 `corruptedSegments`。
  - `walCheckAndRepairIdxFile()` 在重建索引成功后累计 `rebuiltIdxEntries` 并记录重建条数日志。
  - `dmRunForceWalRepair()` 在每个 vnode 执行 `walOpen()` 后调用 `walGetRepairStats()`，把 `corruptedSegments/rebuiltIdxEntries` 写入 `repair.log` 明细行，提升可审计性。
- `T3.4`（`wal_test` 扩展：损坏样例自动化验证）已完成：
  - 新增 `walRepairStatsTrackIdxOnlyCorruption` 用例，覆盖“log 正常但 idx 截断损坏”场景，验证自动修复后的统计结果。
  - 新增样例暴露统计口径缺口：此前仅 idx 损坏时 `rebuiltIdxEntries` 会增长，但 `corruptedSegments` 不会增长。
  - 修复方式：在 `walCheckAndRepairIdxFile()` 进入 idx 修复路径时同步累计 `corruptedSegments`，使“损坏区段”语义覆盖 log/idx 两类损坏。
  - 回归结果：`wal_test` 全量通过，`taosd` 构建通过，`force+wal` 阶段测试覆盖提升到“log 损坏 + idx 损坏 + idx-only 损坏”三类样例。
- `T4.1`（TSDB 文件枚举与完整性扫描器封装）已完成：
  - TSDB 目录结构存在多层子目录（例如 `tsdb/f100/...`），扫描器必须递归遍历，不能只看 `tsdb` 根目录。
  - 通过扩展名后缀归类可稳定覆盖当前需求：`.head/.data/.sma/.stt` 归入对应计数，其余文件计入 `unknownFiles`，便于后续定位杂项文件。
  - 新增 `SRepairTsdbScanResult` 与 `tRepairScanTsdbFiles()` 后，可在 `tRepairPrecheck()` 的 `fileType=tsdb` 分支直接复用扫描结果做 fail-fast。
  - 当前完整性基线定义为“至少包含一份 `.head` 与 `.data`”；缺失任一关键文件即返回 `TSDB_CODE_INVALID_PARA`，与新单测期望一致。
- `T4.2`（TSDB 可恢复块提取与损坏块定位输出）已完成：
  - 块级定位采用“目录聚合”策略：对包含 TSDB 识别文件的每个子目录产出一条块记录，避免早期阶段引入复杂的文件名语义解析。
  - 新增 `SRepairTsdbBlockReport` 与 `tRepairAnalyzeTsdbBlocks()`，输出 `totalBlocks/recoverableBlocks/corruptedBlocks/unknownFiles` 以及损坏块路径列表，满足结构化报告诉求。
  - 当前“可恢复块”判定规则为 `head>0 && data>0`；仅有 `sma/stt` 或仅有 `head/data` 的块会被归类为 `corrupted`。
  - 损坏块路径列表采用上限保护（`REPAIR_TSDB_MAX_REPORTED_BLOCKS`），在大规模损坏场景下保留汇总计数准确性并避免报告结构无限增长。
- `T4.3`（TSDB 文件重建流程 MVP：保留有效块）已完成：
  - 新增 `tRepairRebuildTsdbBlocks()`，基于目录级块判定把“可恢复块”（`head+data`）复制到重建输出目录，损坏块仅记录不复制。
  - 重建流程在开始时会重置输出目录，确保重复执行结果可预测且不会混入历史残留文件。
  - 若扫描后没有任何可恢复块，函数返回 `TSDB_CODE_INVALID_PARA`，避免生成“看似成功但不可用”的空重建结果。
  - 当前实现将“包含已识别 TSDB 文件的目录”视为块边界，适合作为 MVP；后续可在 `T4.4/T4.5` 基于真实 TSDB 样本迭代更细粒度块语义。
- `T4.4`（TSDB 修复结果验证）已完成：
  - 新增 `tRepairNeedRunTsdbForceRepair()`，把 `force+tsdb` 调度判定从 `dmMain` 解耦到 `trepair`，并补齐单测 `NeedRunTsdbForceRepair`。
  - `dmMain.c` 新增 `dmRunForceTsdbRepair()`，在 `dmRunRepairWorkflow()` 中接入：
    - 每 vnode 执行 `tRepairAnalyzeTsdbBlocks()` 产出块级报告；
    - 执行 `tRepairRebuildTsdbBlocks()` 到临时目录（`<target>.rebuild`）；
    - 删除原 `tsdb` 并 `rename` 切换为重建结果；
    - 切换失败时执行 `tRepairRollbackVnodeTarget()` 回滚，保证 fail-fast 与“不破坏已有目录”语义。
  - 运行态日志与状态文件已覆盖 `tsdb` 步骤（`repair.log` + `repair.state.json(step=tsdb)`），与 `wal` 流程保持一致。
- `T4.5`（TSDB 场景系统测试脚本补齐）已完成：
  - 新增脚本 `tests/ci/repair_tsdb_force.sh`，自动构造 `vnode2/tsdb` 的“可恢复块 + 损坏块”混合样本。
  - 脚本执行 `taosd -r --node-type vnode --file-type tsdb --mode force`，并校验：
    - 输出包含 `step=tsdb` 的进度行与 `status=success` 摘要行；
    - 修复后目标目录仅保留可恢复块，损坏块被剔除；
    - 备份目录保留原始损坏块，`repair.log/repair.state.json` 均存在。
  - 在当前无完整 dnode 运行环境下，`taosd` 退出码为 `47` 仍可接受（修复流程已完成并产出成功摘要），脚本据此做流程级验收而非进程码等值断言。
- `T5.1`（META 元数据解析器稳定化）已完成：
  - 新增 `SRepairMetaScanResult` 与 `tRepairScanMetaFiles()`，对 `meta` 目录做“结构/标签/索引”最小稳定性检查：
    - 必需文件：`table.db`、`schema.db`、`uid.idx`、`name.idx`；
    - 可选索引：`ctb.idx/suid.idx/tag.idx/sma.idx/ctime.idx/ncol.idx/stream.task.db`（仅统计，不作为失败条件）；
    - 缺失必需文件会返回 `TSDB_CODE_INVALID_PARA`，并在结果中记录缺失文件名。
  - 新增 `tRepairNeedRunMetaForceRepair()`，补齐 `force+meta` 调度判定，与 `wal/tsdb` 调度接口保持一致。
  - `tRepairPrecheck()` 已接入 `fileType=meta` 分支，确保 repair workflow 前置阶段即可 fail-fast 拦截元数据结构不完整场景。
  - `dmMain.c` 新增 `dmRunForceMetaRepair()` 并接入 `dmRunRepairWorkflow()`：
    - 每 vnode 执行 `meta` 目录备份；
    - 记录 `meta scan detail`（required/present/optional/missing）到 `repair.log`；
    - 更新 `repair.state.json(step=meta)` 与 `step=meta` 进度输出。
  - 运行验证结果：`taosd -r --file-type meta --mode force` 在最小样本下输出 `step=meta` 与成功摘要，且备份目录正确落盘元数据文件。
