# Progress Log

## Session: 2026-03-09

### Phase 1: 设计冻结与 handoff 文档
- **Status:** complete
- **Started:** 2026-03-09 UTC
- Actions taken:
  - 基于讨论结果冻结了新的 repair CLI 约定。
  - 明确 `--repair-target` grammar、默认 strategy、重复检测规则和非兼容决策。
  - 写入正式设计文档 `docs/plans/2026-03-09-vnode-repair-target-cli-design.md`。
  - 写入正式实现计划 `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`。
  - 建立 `docs/data_repair/05-repair-target-cli/` 下的 `task_plan.md`、`findings.md`、`progress.md` 作为后续会话接力点。
- Files created/modified:
  - `docs/plans/2026-03-09-vnode-repair-target-cli-design.md` (created)
  - `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md` (created)
  - `docs/data_repair/05-repair-target-cli/task_plan.md` (created)
  - `docs/data_repair/05-repair-target-cli/findings.md` (created)
  - `docs/data_repair/05-repair-target-cli/progress.md` (created)

### Phase 2: 参数层与 accessor 改造
- **Status:** complete
- Actions taken:
  - 将 `dmMain.c` 的 repair 参数模型改成全局 context + `SArray<SDmRepairTarget>`。
  - 新增 `include/common/dmRepair.h`，承载轻量公共 repair 接口，避免 vnode 侧依赖重型 `dmMgmt.h`。
  - `dmMain.c` 实现了 `--repair-target` 解析、默认 strategy 补齐、重复 target 检查和新 help 文案。
  - `vnodeRepair.c`、`dmMgmt.h` 已切到新的 accessor 集合。
  - `test_com_cmdline.py` 已切到新语法并验证通过。
- Files created/modified:
  - `include/common/dmRepair.h` (created)
  - `source/dnode/mgmt/exe/dmMain.c` (modified)
  - `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h` (modified)
  - `source/dnode/vnode/src/vnd/vnodeRepair.c` (modified)
  - `test/cases/80-Components/01-Taosd/test_com_cmdline.py` (modified)

### Phase 3: 运行期接线与初步回归
- **Status:** in_progress
- Actions taken:
  - `metaOpen.c` 已切到基于 target 列表匹配 vnode，并按 CLI strategy 选择 `from_uid` / `from_redo`。
  - `metaForceRepair()` 已补入 `metaBackupCurrentMeta()` 调用。
  - `tsdbFS2.c` 已切到基于 `vnode + fileid` 匹配 target。
  - `test_meta_force_repair.py` 与 `test_tsdb_force_repair.py` 已迁到新语法，并引入 db-specific vnode / fileid 解析。
  - 运行期回归尚未收敛：`meta` backup 用例仍失败；`tsdb` fake-fid 用例仍失败；两条 real-fileset smoke 用例被 skip。
- Files created/modified:
  - `source/dnode/vnode/src/meta/metaOpen.c` (modified)
  - `source/dnode/vnode/src/tsdb/tsdbFS2.c` (modified)
  - `test/cases/80-Components/01-Taosd/test_meta_force_repair.py` (modified)
  - `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py` (modified)

### Phase 3 补记：运行时结构从通用 target array 收敛到按类型聚合
- **Status:** complete
- Actions taken:
  - 将 `include/common/dmRepair.h` 收敛为“叶子配置 + 专用 accessor”，去掉通用 `SDmRepairTarget` 暴露。
  - 将 `dmMain.c` 的运行时持有结构从通用 `SArray` 改为三类聚合索引：
    - `meta`: `vnodeId -> strategy`
    - `wal`: `vnodeId -> enabled`
    - `tsdb`: `vnodeId -> fileId -> strategy`
  - `metaOpen.c` / `tsdbFS2.c` 改为调用专用 accessor，不再循环扫描通用 target array。
  - 重新构建 `taosd` 并回归 `test_com_cmdline.py -k repair_cmdline_repair_target`，均通过。
- Files created/modified:
  - `include/common/dmRepair.h` (modified)
  - `source/dnode/mgmt/exe/dmMain.c` (modified)
  - `source/dnode/vnode/src/meta/metaOpen.c` (modified)
  - `source/dnode/vnode/src/tsdb/tsdbFS2.c` (modified)
  - `source/dnode/vnode/src/vnd/vnodeRepair.c` (modified)

## Verification
| Check | Expected | Actual | Status |
|-------|----------|--------|--------|
| `test -f docs/plans/2026-03-09-vnode-repair-target-cli-design.md` | file exists | file exists | PASS |
| `test -f docs/plans/2026-03-09-vnode-repair-target-cli-plan.md` | file exists | file exists | PASS |
| `test -f docs/data_repair/05-repair-target-cli/task_plan.md` | file exists | file exists | PASS |
| `test -f docs/data_repair/05-repair-target-cli/findings.md` | file exists | file exists | PASS |
| `test -f docs/data_repair/05-repair-target-cli/progress.md` | file exists | file exists | PASS |
| `cmake --build debug --target taosd -j4` | build succeeds | build succeeds | PASS |
| `python3 -m py_compile test/cases/80-Components/01-Taosd/test_com_cmdline.py test/cases/80-Components/01-Taosd/test_meta_force_repair.py test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py` | syntax ok | syntax ok | PASS |
| `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_repair_target -q` | parser cases pass | 1 passed | PASS |
| `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q` | meta runtime updated | 1 passed, 1 failed | FAIL |
| `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k 'dispatches_in_open_fs or enters_real_execution_path' -q` | tsdb entry smoke | 2 skipped | PARTIAL |
| `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k removes_missing_head_data_sma_from_current -q` | fake-fid fileset removal works | 1 failed | FAIL |
| `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_repair_target -q` after data-model refactor | parser cases still pass | 1 passed | PASS |

## Error Log
| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-03-09 UTC | 无 | 0 | 当前阶段仅新增文档 |
| 2026-03-09 UTC | `meta` backup 用例未生成外部 backup 目录 | 1 | 已修正测试 vnode 选取逻辑并补入 `metaBackupCurrentMeta()` 调用，问题仍未闭环 |
| 2026-03-09 UTC | `tsdb` fake-fid 用例未移除目标 core group | 1 | 已确认 parser 新语法生效；需继续排查 fileset 命中/提交链路 |

## Handoff Notes
- 下一次会话开始时，先阅读：
  - `docs/plans/2026-03-09-vnode-repair-target-cli-design.md`
  - `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`
  - `docs/data_repair/05-repair-target-cli/task_plan.md`
  - `docs/data_repair/05-repair-target-cli/findings.md`
- 下一次实现优先从两条失败链路入手：
  - `meta`: 抓 repair 进程 stdout/stderr，确认为什么 external backup 目录仍未生成。
  - `tsdb`: 沿 `tsdbDispatchForceRepair()` 的 target 命中和 `current.json` 提交链路检查 fake-fid case。
- `git status --short` 已确认：本次会话仅新增上述文档；`source/dnode/mgmt/exe/dmMain.c` 仍保持会话开始前已有的未提交修改状态。
- 当前工作区在本次会话开始前已经存在 `source/dnode/mgmt/exe/dmMain.c` 未提交改动；文档编写未触碰该文件。
