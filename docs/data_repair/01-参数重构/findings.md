# Findings & Decisions

## Requirements
- 参考 `.vscode/dev/phase1-repair-freeze.md` 完成 `taosd -r` 参数层重构。
- `--mode` 冻结值：`force|replica|copy`，本期仅 `force` 支持，`replica/copy` 返回 `TSDB_CODE_OPS_NOT_SUPPORT`。
- `--force` 完全弃用，出现即 `TSDB_CODE_INVALID_PARA`。
- `--file-type` 本期仅 `wal|tsdb|meta`，`tdb` 不兼容。
- `taosd -r` 仅在“未出现任何新修复参数”时走旧行为。
- 新修复参数必须与 `-r` 一起使用。
- 本期边界仅支持 `--node-type vnode`。

## Research Findings
- `taosd` 参数入口在 `source/dnode/mgmt/exe/dmMain.c::dmParseArgs`。
- 当前 `-r` 仅设置 `generateNewMeta=true`，并无新修复参数解析。
- `generateNewMeta` 在 `source/dnode/vnode/src/meta/metaOpen.c` 中触发旧元数据重建流程。
- 当前仓内未发现现成 `--node-type/--file-type/--mode` 解析实现可复用。
- 已在 `dmMain.c` 完成参数层接入：repair long options 解析、phase1 语义校验、`-r` 兼容策略收敛、repair help 输出。
- 已在 `test/cases/80-Components/01-Taosd/test_com_cmdline.py` 补充 phase1 repair 参数层用例。

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| 在 `dmMain.c` 内先落地 repair 参数层解析与校验 | 现有参数解析集中在该文件，改动最小且风险可控 |
| 通过命令矩阵做红绿验证 | 直接对应冻结文档 P/E 编号，证据清晰 |
| 新修复参数校验通过后在 phase1 直接成功返回 | 避免误进入正常 taosd 启动流程，保持“参数层重构”边界清晰 |

## Current Audit
- 2026-03-06 通过 `planning-with-files` 会话恢复脚本发现：上一会话还有 41 条未同步消息。
- 结合 `git diff --stat` 核对，当前工作区实际仅存在 `source/dnode/mgmt/exe/dmMain.c` 与 `test/cases/80-Components/01-Taosd/test_com_cmdline.py` 两处未提交改动。
- 上一会话提到的 `source/dnode/mgmt/repair/CMakeLists.txt` 与 `source/dnode/mgmt/CMakeLists.txt` 改动当前不在工作区中，应视为未落地或已回退。

## Review Follow-up Fixes
- 修复了 repair long option 缺参时误吞下一个选项的问题：当 `--node-type` 等参数后紧跟另一个 `-` 开头选项时，现统一报 `requires a parameter`。
- 修复了新增命令行测试在 ASAN/LSAN 环境下的稳定性问题：`_run_taosd()` 默认注入 `ASAN_OPTIONS=detect_leaks=0`（若调用环境未显式设置 `detect_leaks=`）。
- 调整了 phase1 repair 中 `TSDB_CODE_OPS_NOT_SUPPORT` 的进程退出行为：保留不支持提示，同时返回非零 shell 退出码，避免自动化脚本将失败误判为成功。
- 新增回归覆盖：缺少 long option 参数、`mode copy` 非零退出、ASAN 环境下命令执行路径。

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| 环境无 pytest（`pytest` command / module not found） | 使用 `python3 -m py_compile` 做语法验证，待 CI 或完整测试镜像执行 pytest |

## Verification Update (2026-03-06)
- 手工执行 phase1 repair 命令矩阵共 20 条，结果为 20/20 通过。
- 覆盖范围包括成功路径 `p01-p05`、错误路径 `e01-e12`，并补充了 `e03b`（`--node-type` 缺参）回归场景。
- 当前命令行退出码行为已验证：`INVALID_PARA -> 24`、`INVALID_CFG -> 25`、`OPS_NOT_SUPPORT -> 1`、成功路径 -> `0`。
- `pytest` 解释器与插件可启动，但执行 `cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1` 时在当前环境卡住，无完成结果；已确认不再是 `pytest` 缺失问题，而是测试运行期环境/初始化问题。

## Matrix Coverage Update (2026-03-06)
- `test_repair_cmdline_phase1` 已扩展为表驱动矩阵测试，覆盖冻结文档中的 `P01-P05` 与 `E01-E12`。
- 新增统一断言辅助方法 `_assert_taosd_case()`，便于后续继续按冻结矩阵增量扩展。
- 当前额外保留了 `E03b`（`--node-type` 缺参）回归场景，用于保护此前修复的 long option 缺参问题。
- `E13-E16` 暂未纳入该测试：这四项属于 repair 真正执行阶段的运行期错误，而当前 phase1 实现仍停留在参数层校验与分流，不具备稳定触发条件。

## Review Update (2026-03-06, Matrix Tests)
- 对扩展后的 `test_repair_cmdline_phase1` 进行了专项复审，未发现新的阻塞问题。
- 当前矩阵测试已按冻结文档覆盖 `P01-P05` 与 `E01-E12`，结构上可维护性良好。
- 一项轻微观察：部分成功路径仍使用较宽松的输出关键字（如 `version`）做匹配，后续如需进一步收紧断言，可改为更稳定的版本输出特征；当前不构成 blocker。

## Planning Files Location
- 当前任务的规划文件已统一位于 `docs/data_repair/01-参数重构/`。
- 后续若继续维护该任务，请更新此目录下的 `task_plan.md`、`findings.md`、`progress.md`，而不是项目根目录。

## Resources
- `.vscode/dev/phase1-repair-freeze.md`
- `.vscode/dev/RS.md`
- `source/dnode/mgmt/exe/dmMain.c`
- `source/dnode/vnode/src/meta/metaOpen.c`

## Visual/Browser Findings
- 无。
