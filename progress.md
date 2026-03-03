# 数据修复工具开发进度日志

## 当前检查点
- 日期：`2026-03-03`
- 当前完成：`P1` 已完成，`P2` 已完成，`P3` 已完成，`P4` 已完成，`P5` 进行中（`T5.1` 进行中）。
- 下一任务：`T5.1`（META 元数据解析器稳定化：结构/标签/索引）。
- 恢复入口：先读 `task_plan.md`，再读 `findings.md`，最后读本文件。

## 会话日志
| 时间(UTC) | 动作 | 结果 |
|---|---|---|
| 2026-03-03 17:55 | 读取需求文档 `数据修复工具 - RS.md` | 明确三模式、参数与范围边界 |
| 2026-03-03 18:00 | 定位 `taosd -r` 入口 (`dmMain.c`) | 发现当前仅设置 `generateNewMeta` |
| 2026-03-03 18:06 | 阅读 `metaOpen.c` | 发现 `-r` 当前本质是元数据重建 |
| 2026-03-03 18:12 | 阅读 `walMgmt.c/walMeta.c` | 确认 WAL 已有自动修复能力可复用 |
| 2026-03-03 18:18 | 阅读 `tsdbFS2.c/tsdbReaderWriter.c` | 确认 TSDB 现状偏检测/容错，缺少完整修复编排 |
| 2026-03-03 18:24 | 阅读 `mndDnode.c/mndVgroup.c` 与运维文档 | 确认 `restore dnode` 是整节点恢复，不是文件级修复 |
| 2026-03-03 18:33 | 建立规划文件 | 新增 `task_plan.md/findings.md/progress.md` |
| 2026-03-03 18:40 | 输出设计与实施计划文档 | 新增 `docs/plans/*data-repair*.md` |
| 2026-03-03 18:48 | T1.1 Red 阶段 | 在 `commonTests.cpp` 增加 `RepairOptionParseTest` 三组用例，构建失败（缺少 `trepair.h`）符合预期 |
| 2026-03-03 18:58 | T1.1 Green 实现 | 新增 `include/common/trepair.h`、`source/common/src/trepair.c`，实现 node/file/mode 解析 |
| 2026-03-03 19:05 | T1.1 缺陷修复 | 修复 `tRepairParse*` 对 `NULL` 输出指针的崩溃问题（ASan 报告） |
| 2026-03-03 19:08 | T1.1 验证通过 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 19:16 | T1.2 Red 阶段 | 在 `commonTests.cpp` 新增 `ParseCliOption/ParseCliOptionInvalid`，构建失败（缺少 `SRepairCliArgs/tRepairParseCliOption`） |
| 2026-03-03 19:21 | T1.2 Green 实现 | 扩展 `trepair.h/.c` 增加 CLI 选项键值解析；`dmMain.c` 接入 `--node-type/--file-type/--vnode-id`（支持 `--opt val` 与 `--opt=val`） |
| 2026-03-03 19:24 | T1.2 测试回归 | `ASAN_OPTIONS=detect_leaks=0 /Projects/work/TDengine/debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.*` 通过 |
| 2026-03-03 19:24 | T1.2 测试回归 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir /Projects/work/TDengine/debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 19:25 | T1.2 编译验证 | `cmake --build /Projects/work/TDengine/debug -j8 --target taosd` 通过，`dmMain.c` 变更成功编入 `taosd` |
| 2026-03-03 19:26 | T1.2 运行验证 | `ASAN_OPTIONS=detect_leaks=0 taosd -r --node-type vnode --file-type wal --vnode-id 2 --help` 退出码 `0`；非法 `--node-type bad` 退出码 `25` |
| 2026-03-03 19:32 | T1.3 Red 阶段 | 扩展 `ParseCliOption` 测试覆盖 `backup-path/mode/replica-node`，构建失败（`SRepairCliArgs` 缺字段） |
| 2026-03-03 19:38 | T1.3 Green 实现 | 扩展 `SRepairCliArgs` 与 `tRepairParseCliOption()`；`dmMain.c` 新增 `--backup-path/--mode/--replica-node` 解析 |
| 2026-03-03 19:40 | T1.3 测试回归 | `ASAN_OPTIONS=detect_leaks=0 /Projects/work/TDengine/debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.*` 通过 |
| 2026-03-03 19:40 | T1.3 测试回归 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir /Projects/work/TDengine/debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 19:42 | T1.3 编译验证 | `cmake --build /Projects/work/TDengine/debug -j8 --target taosd` 通过 |
| 2026-03-03 19:42 | T1.3 运行验证 | `ASAN_OPTIONS=detect_leaks=0 taosd -r --node-type vnode --file-type wal --vnode-id 2,3 --backup-path /tmp/backup --mode force --replica-node 192.168.1.24:/root/dataDir --help` 退出码 `0`；非法 `--mode bad-mode` 退出码 `25` |
| 2026-03-03 19:49 | T1.4 Red 阶段 | 新增 `ValidateCliArgs*` 规则测试，构建失败（缺少 `tRepairValidateCliArgs`） |
| 2026-03-03 19:54 | T1.4 Green 实现 | `trepair.c` 增加组合校验（必选项、node/file 兼容、vnode-id 规则、copy/replica-node 规则），`dmMain.c` 接入校验并要求 repair 选项必须搭配 `-r` |
| 2026-03-03 19:56 | T1.4 测试回归 | `ASAN_OPTIONS=detect_leaks=0 /Projects/work/TDengine/debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.*` 通过（10/10） |
| 2026-03-03 19:56 | T1.4 测试回归 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir /Projects/work/TDengine/debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 19:58 | T1.4 编译验证 | `cmake --build /Projects/work/TDengine/debug -j8 --target taosd` 通过 |
| 2026-03-03 19:58 | T1.4 运行验证 | `ASAN_OPTIONS=detect_leaks=0 taosd --node-type ...`（无 `-r`）退出码 `25`，提示 `repair options require '-r'`；`mode=copy` 无 `replica-node` 退出码 `25`，提示 `invalid repair option combination`；`mode=force` + 必选项退出码 `0` |
| 2026-03-03 20:03 | T1.5 实现 | 更新 `dmMain.c` 的 `--help` 文案，新增 `-r` 与 `--node-type/--file-type/--vnode-id/--backup-path/--mode/--replica-node` 说明 |
| 2026-03-03 20:04 | T1.5 验证 | `ASAN_OPTIONS=detect_leaks=0 taosd --help | rg ...` 命中全部新增参数说明 |
| 2026-03-03 20:04 | T1.6 完成确认 | `commonTests.cpp` 已覆盖 parser + validator（`RepairOptionParseTest` 共 10 条），`commonTest` 与 `taosd` 构建均通过 |
| 2026-03-03 20:10 | 术语统一修正 | 根据用户澄清将项目术语从 `TDB` 统一为 `META`；代码中保留 `tdb -> META` 兼容解析映射，并同步更新计划/设计/实施文档 |
| 2026-03-03 20:13 | 术语修正回归 | `commonTest` 构建与 `RepairOptionParseTest.*`（10/10）通过，`ctest -R commonTest` 通过 |
| 2026-03-03 20:14 | 术语修正运行验证 | `taosd -r --file-type meta ... --help` 退出码 `0`；兼容 `--file-type tdb ... --help` 退出码 `0`；`taosd --help` 文案仅展示 `meta` |
| 2026-03-03 20:30 | T2.1 Red 阶段 | 在 `commonTests.cpp` 新增 `InitRepairCtxSuccess/InvalidArgs`，构建失败（缺少 `SRepairCtx/tRepairInitCtx`）符合预期 |
| 2026-03-03 20:36 | T2.1 Green 实现 | 扩展 `trepair.h/.c` 新增 `SRepairCtx` 与 `tRepairInitCtx()`；`dmMain.c` 在 repair 参数校验后初始化运行时上下文 |
| 2026-03-03 20:40 | T2.1 验证通过 | `commonTest --gtest_filter=RepairOptionParseTest.InitRepairCtx*` 通过；`ctest -R commonTest` 通过；`cmake --build debug --target taosd` 通过 |
| 2026-03-03 20:46 | T2.2 Red 阶段 | 扩展 `InitRepairCtx` 测试覆盖 vnode 过滤（解析 `vnode-id` 到数组 + 匹配判断），构建失败（缺少 `vnodeIdNum/vnodeIds/tRepairShouldRepairVnode`）符合预期 |
| 2026-03-03 20:52 | T2.2 Green 实现 | `SRepairCtx` 新增 `vnodeIds` 缓存；`tRepairInitCtx()` 增加 `vnode-id` 解析；新增 `tRepairShouldRepairVnode()` 进行目标 vnode 过滤 |
| 2026-03-03 20:57 | T2.2 缺陷修复 | 修复 `strtok_r` 改写原始 `vnodeIdList` 的问题，改为临时缓冲区解析，保留原始字符串 |
| 2026-03-03 20:59 | T2.2 验证通过 | `commonTest --gtest_filter=RepairOptionParseTest.InitRepairCtx*` 通过；`ctest -R commonTest` 通过；`taosd -r ... --vnode-id 2,a --mode force` 退出码 `25` 并提示 `failed to initialize repair context` |
| 2026-03-03 21:24 | T2.3 Red 阶段开始 | 已将 `task_plan.md` 中 `T2.3` 置为 `in_progress`，准备先新增预检单测（路径/磁盘/目标文件）并验证失败 |
| 2026-03-03 21:26 | T2.3 Red 验证 | `cmake --build debug --target commonTest` 失败，报错 `tRepairPrecheck was not declared in this scope`，符合“先测后码”预期 |
| 2026-03-03 21:33 | T2.3 Green 实现 | 在 `trepair.h/.c` 新增 `tRepairPrecheck()`，覆盖数据目录、备份目录、磁盘可用空间、`vnode/<id>/<wal|tsdb|meta>` 目标路径检查；`dmMain.c` 接入启动前预检 |
| 2026-03-03 21:35 | T2.3 单测验证 | `commonTest --gtest_filter=RepairOptionParseTest.Precheck*` 通过（5/5） |
| 2026-03-03 21:35 | T2.3 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 21:43 | T2.3 编译验证 | `cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 21:44 | T2.3 运行验证 | `taosd -o /tmp/taoslog -r ... --backup-path /tmp/td-repair-nonexistent-backup` 退出码 `25`，输出 `failed repair precheck: Invalid parameters`（预检失败路径生效） |
| 2026-03-03 21:35 | T2.4 Red 阶段开始 | 已将 `task_plan.md` 中 `T2.4` 置为 `in_progress`，准备先新增备份目录命名与创建测试并验证失败 |
| 2026-03-03 21:37 | T2.4 Red 验证 | `cmake --build debug --target commonTest` 失败，报错 `tRepairPrepareBackupDir was not declared in this scope`，符合预期 |
| 2026-03-03 21:39 | T2.4 Green 实现 | 在 `trepair.h/.c` 新增 `tRepairPrepareBackupDir()`，输出并创建 `backup/<session>/vnode<id>/<fileType>`；`dmMain.c` 接入启动时按目标 vnode 预创建备份目录 |
| 2026-03-03 21:40 | T2.4 单测验证 | `commonTest --gtest_filter=RepairOptionParseTest.PrepareBackupDir*` 通过（3/3） |
| 2026-03-03 21:40 | T2.4 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 21:41 | T2.4 编译验证 | `cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 21:41 | T2.4 运行验证 | `taosd -o /tmp/taoslog -r ... --backup-path /tmp/td-repair-backup-test` 退出码 `25`，仍可在预检阶段 fail-fast（流程未回归） |
| 2026-03-03 12:47 | T2.5 Red 阶段开始 | 已将 `task_plan.md` 中 `T2.5` 置为 `in_progress`，准备先新增 `repair.log`/`repair.state.json` 的单测并验证失败 |
| 2026-03-03 12:49 | T2.5 Red 验证 | `cmake --build debug --target commonTest` 失败，报错 `tRepairPrepareSessionFiles/tRepairAppendSessionLog/tRepairWriteSessionState was not declared in this scope`，符合预期 |
| 2026-03-03 12:51 | T2.5 Green 实现 | `trepair.h/.c` 新增会话文件准备、日志追加、状态文件写入（JSON 原子落盘）；`dmMain.c` 接入 precheck 后的 session 初始化与状态更新 |
| 2026-03-03 12:52 | T2.5 单测验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.*Session*` 通过（3/3） |
| 2026-03-03 12:52 | T2.5 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 12:53 | T2.5 编译验证 | `cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 12:53 | T2.5 运行验证 | `taosd -o /tmp/taoslog -r ...` 退出码 `25`，仍在 precheck 阶段 fail-fast（未引入启动流程回归） |
| 2026-03-03 13:05 | 汇报规范持久化 | 新增仓库根 `AGENTS.md`，并在 `task_plan.md` 恢复机制追加“每次汇报必须包含进度条”规则 |
| 2026-03-03 13:07 | T2.6 Red 阶段开始 | 已将 `task_plan.md` 中 `T2.6` 置为 `in_progress`，准备先补进度行/摘要行构造与节流函数单测并验证失败 |
| 2026-03-03 13:09 | T2.6 Red 验证 | `cmake --build debug --target commonTest` 失败，报错 `tRepairBuildProgressLine/tRepairBuildSummaryLine/tRepairNeedReportProgress was not declared in this scope`，符合预期 |
| 2026-03-03 13:12 | T2.6 Green 实现 | `trepair.h/.c` 新增进度节流判定、进度行/摘要行构造 API；`dmMain.c` 接入 precheck/backup 阶段进度输出与最终摘要写入 |
| 2026-03-03 13:13 | T2.6 单测验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.*Progress*` 通过（3/3） |
| 2026-03-03 13:13 | T2.6 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过 |
| 2026-03-03 13:14 | T2.6 编译验证 | `cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 13:14 | T2.6 运行验证 | `taosd -o /tmp/taoslog -r ...` 退出码 `25`，仍在 precheck 阶段 fail-fast（启动流程未回归） |
| 2026-03-03 13:27 | T2.7 Red 阶段开始 | 已将 `task_plan.md` 中 `T2.7` 置为 `in_progress`，准备先新增会话恢复/续跑失败用例并验证失败 |
| 2026-03-03 13:29 | T2.7 Red 验证 | `cmake --build debug --target commonTest` 失败，报错 `tRepairTryResumeSession was not declared in this scope`，符合预期 |
| 2026-03-03 13:35 | T2.7 Green 实现 | `trepair.h/.c` 新增 `tRepairTryResumeSession()`（扫描 `repair-*` 会话、校验 `repair.state.json` 并回填续跑进度）；`dmMain.c` 接入恢复入口与 `doneVnodes` 跳过续跑 |
| 2026-03-03 13:36 | T2.7 单测验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.TryResumeSession*` 通过（3/3） |
| 2026-03-03 13:38 | T2.7 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 13:38 | T2.7 运行验证 | `taosd -o /tmp/taoslog -r ... --backup-path /tmp/td-repair-nonexistent-backup-test` 退出码 `25`，仍在 precheck 阶段 fail-fast（启动流程未回归） |
| 2026-03-03 13:39 | T2.7 收尾 | 已将 `task_plan.md` 中 `T2.7` 更新为 `completed`，下一入口切换为 `T3.1` |
| 2026-03-03 13:51 | T3.1 Red 阶段开始 | 已将 `task_plan.md` 中 `T3.1` 置为 `in_progress`，准备先新增 WAL 调度判定与目标路径失败用例并验证失败 |
| 2026-03-03 21:58 | T3.1 Green 实现 | `trepair.h/.c` 新增 `tRepairNeedRunWalForceRepair()/tRepairBuildVnodeTargetPath()`；`dmMain.c` 接入 `force+wal` 调度（`walInit` + 每 vnode `walOpen/walClose`）与状态/日志/进度更新 |
| 2026-03-03 22:01 | T3.1 单测验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.NeedRunWalForceRepair:RepairOptionParseTest.BuildVnodeTargetPath` 通过（2/2） |
| 2026-03-03 22:02 | T3.1 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 22:02 | T3.1 运行验证 | `taosd -o /tmp/taoslog -r --node-type vnode --file-type wal --vnode-id 2 --mode force --backup-path /tmp/td-repair-nonexistent-backup` 退出码 `25`，仍按 precheck fail-fast（未引入回归） |
| 2026-03-03 22:03 | T3.1 收尾 | 已将 `task_plan.md` 中 `T3.1` 更新为 `completed`，下一入口切换为 `T3.2` |
| 2026-03-03 14:28 | T3.2 Red 阶段开始 | 已将 `task_plan.md` 中 `T3.2` 置为 `in_progress`，准备新增 WAL 备份与失败回滚保护单测并先验证失败 |
| 2026-03-03 14:30 | T3.2 Red 验证 | `cmake --build debug -j8 --target commonTest` 失败，报错 `tRepairBackupVnodeTarget/tRepairRollbackVnodeTarget was not declared`，符合先测后码预期 |
| 2026-03-03 14:32 | T3.2 Green 实现 | `trepair.h/.c` 新增 `tRepairBackupVnodeTarget()` 与 `tRepairRollbackVnodeTarget()`（目录递归备份/回滚）；`dmMain.c` 在 `force+wal` 循环接入“先备份、失败回滚”与日志记录 |
| 2026-03-03 14:33 | T3.2 单测验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.BackupAndRollbackVnodeTarget:RepairOptionParseTest.BackupAndRollbackVnodeTargetInvalidArgs` 通过（2/2） |
| 2026-03-03 14:33 | T3.2 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 14:34 | T3.2 运行验证 | `taosd -o /tmp/taoslog -r --node-type vnode --file-type wal --vnode-id 2 --mode force --backup-path /tmp/td-repair-nonexistent-backup` 输出 `failed repair precheck: Invalid parameters`，退出码 `25`，保持 precheck fail-fast |
| 2026-03-03 14:34 | T3.2 收尾 | 已将 `task_plan.md` 中 `T3.2` 更新为 `completed`，下一入口切换为 `T3.3` |
| 2026-03-03 14:47 | T3.3 Red 阶段开始 | 已将 `task_plan.md` 中 `T3.3` 置为 `in_progress`，准备新增 WAL 修复明细统计/输出单测并先验证失败 |
| 2026-03-03 14:50 | T3.3 Red 验证 | `cmake --build debug -j8 --target walTest` 失败，报错 `flexible array member 'SWalCont::body' not at end of 'struct SWal'`，符合“先失败再修复”预期 |
| 2026-03-03 14:53 | T3.3 Green 实现 | 修复 `SWal` 结构体字段顺序（保持 `writeHead` 为末尾字段）；完成 `walGetRepairStats` + WAL 修复统计累计与 `dmMain.c` 的 `repair.log` 明细输出接入 |
| 2026-03-03 14:55 | T3.3 定向验证 | `cmake --build debug -j8 --target walTest` 通过；`./debug/build/bin/walTest --gtest_filter=WalKeepEnv.walGetRepairStatsInvalidArgs:WalKeepEnv.walRepairStatsTrackCorruptedSegmentAndIdxRebuild` 通过（2/2） |
| 2026-03-03 14:56 | T3.3 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R wal_test --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 14:56 | T3.3 收尾 | 已将 `task_plan.md` 中 `T3.3` 更新为 `completed`，下一入口切换为 `T3.4` |
| 2026-03-03 14:58 | T3.4 Red 阶段开始 | 已将 `task_plan.md` 中 `T3.4` 置为 `in_progress`，准备补充“仅 idx 损坏”自动化样例并先验证失败 |
| 2026-03-03 15:00 | T3.4 Red 验证 | 新增 `walRepairStatsTrackIdxOnlyCorruption` 用例失败：`stats.corruptedSegments` 实际为 `0`，未记录“仅 idx 损坏”区段 |
| 2026-03-03 15:00 | T3.4 构建环境处理 | 首次 Red 构建遇到 `ext_pcre2` update 外网失败；通过本地依赖与 stamp 方式消除非业务阻塞后继续测试 |
| 2026-03-03 15:02 | T3.4 Green 实现 | 在 `walCheckAndRepairIdxFile()` 进入 idx 修复路径时累计 `repairStats.corruptedSegments`，统一“损坏区段”统计口径 |
| 2026-03-03 15:03 | T3.4 定向验证 | `walTest` 定向用例通过：`walRepairStatsTrackIdxOnlyCorruption`、`walRepairStatsTrackCorruptedSegmentAndIdxRebuild`（2/2） |
| 2026-03-03 15:04 | T3.4 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R wal_test --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 15:04 | T3.4 收尾 | 已将 `task_plan.md` 中 `T3.4` 更新为 `completed`，下一入口切换为 `T4.1` |
| 2026-03-03 15:05 | T4.1 Red 阶段开始 | 已将 `task_plan.md` 中 `T4.1` 置为 `in_progress`，准备先补 TSDB 扫描器失败用例并验证失败 |
| 2026-03-03 15:06 | T4.1 Red 验证 | `cmake --build debug -j8 --target commonTest` 失败，报错 `SRepairTsdbScanResult/tRepairScanTsdbFiles` 未声明，符合先测后码预期 |
| 2026-03-03 15:09 | T4.1 Green 实现 | `trepair.h/.c` 新增 `SRepairTsdbScanResult` 与 `tRepairScanTsdbFiles()`；实现递归扫描 `.head/.data/.sma/.stt` 统计，并在 `tRepairPrecheck()` 的 `fileType=tsdb` 分支接入完整性校验 |
| 2026-03-03 15:10 | T4.1 定向验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.ScanTsdbFiles*` 通过（3/3） |
| 2026-03-03 15:11 | T4.1 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 15:20 | T4.1 收尾 | 已将 `task_plan.md` 中 `T4.1` 更新为 `completed`，下一入口切换为 `T4.2` |
| 2026-03-03 15:20 | T4.2 Red 阶段开始 | 已将 `task_plan.md` 中 `T4.2` 置为 `in_progress`，准备先补“可恢复块提取/损坏块定位”失败用例并验证失败 |
| 2026-03-03 15:22 | T4.2 Red 验证 | `cmake --build debug -j8 --target commonTest` 失败，报错 `SRepairTsdbBlockReport/tRepairAnalyzeTsdbBlocks` 未声明，符合先测后码预期 |
| 2026-03-03 15:27 | T4.2 Green 实现 | `trepair.h/.c` 新增 `SRepairTsdbBlockReport` 与 `tRepairAnalyzeTsdbBlocks()`；按 TSDB 子目录聚合块级统计（`total/recoverable/corrupted/unknown`）并输出损坏块路径列表 |
| 2026-03-03 15:28 | T4.2 定向验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.AnalyzeTsdbBlocksReport*` 通过（3/3） |
| 2026-03-03 15:29 | T4.2 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 15:33 | T4.2 收尾 | 已将 `task_plan.md` 中 `T4.2` 更新为 `completed`，下一入口切换为 `T4.3` |
| 2026-03-03 15:33 | T4.3 Red 阶段开始 | 已将 `task_plan.md` 中 `T4.3` 置为 `in_progress`，准备先补“保留有效块重建输出目录”失败用例并验证失败 |
| 2026-03-03 15:35 | T4.3 Red 验证 | `cmake --build debug -j8 --target commonTest` 失败，报错 `tRepairRebuildTsdbBlocks` 未声明，符合先测后码预期 |
| 2026-03-03 15:41 | T4.3 Green 实现 | `trepair.h/.c` 新增 `tRepairRebuildTsdbBlocks()`：按目录级块判定保留 `head+data` 可恢复块并重建输出目录；同步输出 `SRepairTsdbBlockReport` 汇总与损坏路径 |
| 2026-03-03 15:43 | T4.3 定向验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.ScanTsdbFiles*:RepairOptionParseTest.AnalyzeTsdbBlocksReport*:RepairOptionParseTest.RebuildTsdbBlocks*` 通过（9/9） |
| 2026-03-03 15:44 | T4.3 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 15:46 | T4.3 收尾 | 已将 `task_plan.md` 中 `T4.3` 更新为 `completed`，下一入口切换为 `T4.4` |
| 2026-03-03 15:46 | T4.4 Red 阶段开始 | 已将 `task_plan.md` 中 `T4.4` 置为 `in_progress`，准备先定义“重建后启动/查询可用”最小验收用例并验证失败 |
| 2026-03-03 15:47 | T4.4 Red 验证 | `cmake --build debug -j8 --target commonTest` 失败，报错 `tRepairNeedRunTsdbForceRepair` 未声明，符合先测后码预期 |
| 2026-03-03 15:50 | T4.4 Green 实现 | `trepair.h/.c` 新增 `tRepairNeedRunTsdbForceRepair()`；`dmMain.c` 新增 `dmRunForceTsdbRepair()` 并接入 `dmRunRepairWorkflow()`（`force+tsdb` 分支），实现 `analyze -> rebuild -> 目录切换` 与失败回滚 |
| 2026-03-03 15:51 | T4.4 定向验证 | `ASAN_OPTIONS=detect_leaks=0 ./debug/build/bin/commonTest --gtest_filter=RepairOptionParseTest.NeedRunWalForceRepair:RepairOptionParseTest.NeedRunTsdbForceRepair` 通过（2/2） |
| 2026-03-03 15:52 | T4.4 回归验证 | `ASAN_OPTIONS=detect_leaks=0 ctest --test-dir debug -R commonTest --output-on-failure` 通过；`cmake --build debug -j8 --target taosd` 通过 |
| 2026-03-03 15:53 | T4.4 运行验证 | `ASAN_OPTIONS=detect_leaks=0 taosd -o /tmp/taoslog -r --node-type vnode --file-type tsdb --vnode-id 2 --mode force --backup-path /tmp/td-repair-nonexistent-backup` 退出码 `25`，输出 `failed repair precheck: Invalid parameters`，保持 precheck fail-fast |
| 2026-03-03 15:54 | T4.4 收尾 | 已将 `task_plan.md` 中 `T4.4` 更新为 `completed`，下一入口切换为 `T4.5`（`in_progress`） |
| 2026-03-03 15:55 | T4.5 Red 阶段开始 | 已将 `task_plan.md` 中 `T4.5` 置为 `in_progress`，准备补 TSDB 场景系统测试脚本并先验证失败 |
| 2026-03-03 15:55 | T4.5 Red 验证 | `bash tests/ci/repair_tsdb_force.sh` 失败（脚本不存在，退出码 `127`），符合先测后码预期 |
| 2026-03-03 15:57 | T4.5 Green 实现 | 新增 `tests/ci/repair_tsdb_force.sh`：构造 `recoverable + corrupted` TSDB 样本，执行 `taosd -r --file-type tsdb --mode force` 并校验 `repair progress/summary`、目标目录重建结果、备份目录与状态文件 |
| 2026-03-03 15:58 | T4.5 定向验证 | `bash tests/ci/repair_tsdb_force.sh` 通过，输出 `tsdb force repair script passed (taosd exit code: 47)` |
| 2026-03-03 15:58 | T4.5 收尾 | 已将 `task_plan.md` 中 `T4.5` 更新为 `completed`，`P4` 标记为 `completed`，下一入口切换为 `T5.1`（`in_progress`） |
| 2026-03-03 15:59 | T5.1 阶段开始 | 已切换到 `force+meta`，准备先勘察 `meta` 解析与现有测试入口，定义首个 Red 用例 |
| 2026-03-03 16:00 | T5.1 上下文勘察 | 已定位 `metaOpen.c:metaGenerateNewMeta()` 与 `dmMain.c` 的 `generateNewMeta` 触发点，确认下一步应先补 `force+meta` 调度判定测试，再决定是否直接复用/包装 `metaGenerateNewMeta` |

## 已落盘文档
- `task_plan.md`
- `findings.md`
- `progress.md`
- `docs/plans/2026-03-03-data-repair-tool-design.md`
- `docs/plans/2026-03-03-data-repair-tool-implementation.md`

## 下一次恢复建议命令
```bash
git status --short
sed -n '1,220p' task_plan.md
sed -n '1,220p' findings.md
sed -n '1,220p' progress.md
sed -n '1,260p' docs/plans/2026-03-03-data-repair-tool-implementation.md
```

## 风险提示
- `copy` 模式需要远端连接能力，可能涉及平台依赖与安全策略。
- `replica` 模式若直接复用现有恢复逻辑，社区版与企业版能力差异需尽早收敛。
- TSDB/META 修复难度显著高于 WAL，建议先交付可运行 MVP（`force+wal`）建立反馈回路。
