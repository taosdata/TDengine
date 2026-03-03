# 数据修复工具开发进度日志

## 当前检查点
- 日期：`2026-03-03`
- 当前完成：`P1` 已完成，`P2` 已完成 `T2.1/T2.2/T2.3`。
- 下一任务：`T2.4`（备份管理器：按 vnode+时间戳目录）。
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
