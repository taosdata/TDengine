# Progress: META 强制恢复（Phase2）

### 2026-03-06 Session A
- 启用并遵循 `using-superpowers`、`brainstorming`、`planning-with-files`、`writing-plans` 相关流程。
- 阅读 `.vscode/dev/RS.md`、`.vscode/dev/phase1-repair-freeze.md` 与 `docs/data_repair/01-参数重构/` 留痕。
- 与用户确认：本次仅做 `meta` 的 force repair 实际执行链路，不包含 `wal/tsdb` 联动。
- 补充读取 `source/dnode/vnode/src/meta/metaOpen.c` 与 `source/dnode/mgmt/exe/dmMain.c`，确认现有 `metaGenerateNewMeta()` 与 repair 参数分流位置。
- 初始化本目录下的 `task_plan.md`、`findings.md`、`progress.md` 用于跨会话持续推进。
- 待下一步：继续按 brainstorming 收敛“恢复列表”与 `--backup-path` 的精确定义，然后给出候选设计方案。
- 用户确认采用“方案 1 + 嵌入 meta open 并行链路”的方向，否决集中串行 repair 执行器。
- 用户确认本次不考虑独立 repair context 设计，范围进一步收敛。
- 用户否决将 META force repair 测试继续追加到 `test_com_cmdline.py`；后续计划改为新增独立测试文件。
- 已产出正式设计文档：`docs/plans/2026-03-06-meta-force-repair-design.md`。
- 已产出正式实施计划：`docs/plans/2026-03-06-meta-force-repair-plan.md`。
- 后续实现应按计划逐任务执行，并保持本目录下 planning 文件持续更新。

### 2026-03-06 Session B
- 进入实现阶段，先按 TDD 新建 `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`。
- 先锁定最小行为：`meta force repair` 省略 `--vnode-id` 时应允许并视为全量 vnode。
- 在 `dmMain.c` 中放开上述校验，并验证命令从红转绿。
- 进一步将 `meta force repair` 从 phase1 占位分流中放行，仅保留 `wal/tsdb` 等非本期类型继续占位返回。
- 在 `metaOpen.c` 中新增按 vnode 命中判断、外部备份目录构造和递归目录备份逻辑。
- 通过手工增量编译 `metaOpen.c` / `dmMain.c`、重建 `libvnode.a`、重链 `taosd` 完成了本轮本地验证。
- 待下一步：补充更贴近真实 vnode open/backup 结果的自动化测试，评估是否需要新增日志断言或目录状态断言。
- 新增独立测试文件现可被 `pytest --collect-only` 正常收集到 2 个用例；实际执行仍卡在当前测试框架环境初始化阶段。
- 手工命令矩阵已扩展到 4 条：默认备份、自定义备份、带尾斜杠的自定义备份，以及 `wal` 负控占位行为，结果均符合预期。
- 新增 `test_meta_force_repair_creates_backup_for_real_vnode()`，开始补“真实 vnode open / 备份目录落盘”的自动化覆盖。
- 使用 `pytest --collect-only` 成功收集到 3 个用例；单独执行新测试时在当前环境中超时（90s），尚未拿到稳定 PASS/FAIL 结论。

- 用户明确指出：该测试文件的正确执行方式是在 `test/` 目录下运行 `./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`；此经验已固化到规划留痕中，后续会话默认按该入口验证。
- 已使用用户确认的标准入口重新验证 `test_meta_force_repair.py` 与 `test_com_cmdline.py -k repair_cmdline_phase1`。
- 两者在当前沙箱环境中都能进入 pytest，但都未在短时间内产出最终结果；这表明当前剩余问题偏向测试运行环境差异，而不是单个测试文件入口错误。
- 已完成 `taosd` / `taos` / `libtaos` 来源审计，确认当前环境同时存在仓库构建版与 `/usr/local/taos` 安装版，且 Python `taos` 客户端默认会加载安装版 `libtaos.so`。
- 已执行“统一到构建版”验证：显式注入 `TAOS_BIN_PATH` 与 `LD_LIBRARY_PATH` 后重跑标准脚本入口，但当前沙箱环境中未见明显改善。
- 观察到 `pytest` 进程环境中未稳定看到 `TAOS_BIN_PATH` / `LD_LIBRARY_PATH`，后续需继续排查测试脚本到 pytest 进程的环境传递链路。
- 已修复 `taostest` localhost 命令执行与 ASAN 环境污染问题，并完成沙箱外验证。
- `test_meta_force_repair.py` 全量通过（3 passed）。
- `test_com_cmdline.py -k repair_cmdline_phase1` 回归通过（1 passed, 1 deselected）。

- 已将本轮关于正确测试入口、双版本二进制风险、ASAN/LSAN 环境污染、localhost 启动链路与沙箱权限差异的经验固化到 `findings.md` 的 `Long-Term Memory` 章节，供后续新会话直接复用。
- 已完成针对 code review 的最小修正，并重新跑通 `test_meta_force_repair.py` 与 phase1 参数层回归。
- 已完成“方案 B”边界收敛重构：把 `meta` 专有语义从 `dmMain.c` 挪回 `metaOpen.c`，并保持现有测试通过。
