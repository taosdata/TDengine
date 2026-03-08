# TSDB Force Repair Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 `taosd -r --node-type vnode --file-type tsdb --mode force` 实现单副本 vnode 的 TSDB 强制修复，确保 vnode 可启动，并按保守模式尽可能保留可确认有效的数据。

**Architecture:** 管理层仅负责 repair 参数校验与执行放行；TSDB repair 逻辑下沉到 `source/dnode/vnode/src/tsdb/`，按“扫描旧文件组 -> 筛选有效块 -> 重建新文件组 -> 更新 current.json”的方式实现，不做 inplace patch。repair 仅备份受影响文件组，且备份必须包含对应的 `current.json` 描述信息。

**Tech Stack:** C, TDengine vnode/tsdb subsystem, existing TSDB FS/file-set readers-writers, pytest E2E under `test/cases/80-Components/01-Taosd/`

---

### Task 1: 固化设计与留痕

**Files:**
- Update: `docs/data_repair/03-TSDB_repair/task_plan.md`
- Update: `docs/data_repair/03-TSDB_repair/findings.md`
- Update: `docs/data_repair/03-TSDB_repair/progress.md`
- Create: `docs/plans/2026-03-07-tsdb-force-repair-design.md`
- Create: `docs/plans/2026-03-07-tsdb-force-repair-plan.md`

**Steps:**
1. 更新第三阶段 `task_plan.md`，将状态从设计中调整到计划已产出。
2. 在 `findings.md` 中补齐用户确认的边界、判定规则、备份约束与推荐实现方向。
3. 在 `progress.md` 中记录设计已获批准、下一步进入实现规划。
4. 写入正式设计文档到 `docs/plans/2026-03-07-tsdb-force-repair-design.md`。
5. 写入正式实施计划到 `docs/plans/2026-03-07-tsdb-force-repair-plan.md`。

**Test/Verify:**
- `test -f docs/plans/2026-03-07-tsdb-force-repair-design.md`
- `test -f docs/plans/2026-03-07-tsdb-force-repair-plan.md`

**Commit:**
- `docs: add tsdb force repair design and plan`

### Task 2: 补最小失败测试，锁定真实执行入口

**Files:**
- Update: `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- Create or Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`
- Check: `source/dnode/mgmt/exe/dmMain.c`

**Steps:**
1. 查看 phase1/phase2 相关 repair 测试写法，复用已有 fixture 与命令执行封装。
2. 为 `tsdb force repair` 增加命令入口用例，先断言当前实现尚未进入真实 repair 或缺少预期行为。
3. 增加最小 E2E 骨架：构造可识别的 TSDB repair 目标 vnode 与断言点。
4. 运行新加用例，确认其在现状下失败且失败原因符合预期。
5. 若需要，补充最小断言，确保不影响 `meta force repair` 既有行为。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k real_entry`

**Commit:**
- `test(repair): add failing coverage for tsdb force repair entry`

### Task 3: 放行 TSDB repair 执行路径

**Files:**
- Update: `source/dnode/mgmt/exe/dmMain.c`
- Check: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- Check: phase2 `meta force repair` 相关入口文件

**Steps:**
1. 对照 `meta force repair` 的分流方式，明确 `tsdb` 文件类型的真实执行放行点。
2. 修改 `dmMain.c`，让 `fileType=tsdb` 不再停留在 phase1 占位返回。
3. 保持 `meta` 行为不变，避免引入回归。
4. 运行参数/分流相关测试，确认 `tsdb` 已走真实执行路径，其他类型边界保持不变。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k real_entry`

**Commit:**
- `feat(repair): enable tsdb force repair execution path`

### Task 4: 设计并落地 TSDB repair 上下文与命中判断

**Files:**
- Create: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Create: `source/dnode/vnode/src/tsdb/tsdbRepair.h`
- Update: `source/dnode/vnode/src/tsdb/CMakeLists.txt` or related build file
- Update: `source/dnode/vnode/src/tsdb/tsdbOpen.c`
- Check: phase2 `metaOpen.c`

**Steps:**
1. 参考 phase2 `meta force repair`，定义 TSDB repair 的命中判断、vnode 过滤、启动内去重状态。
2. 在 `tsdbOpen.c` 中增加 repair 触发点，但保持普通打开路径最小侵入。
3. 提供 TSDB repair 上下文结构，承载当前 vnode、备份路径、日志目标、统计信息与结果视图。
4. 先仅完成“命中 repair 并进入 repair 框架”的最小实现。
5. 运行入口测试，确认能稳定进入 TSDB repair 框架。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k real_entry`

**Commit:**
- `feat(tsdb): add force repair context and dispatch`

### Task 5: 实现受影响文件组扫描与备份清单生成

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Check: `source/dnode/vnode/src/tsdb/tsdbFS2.c`
- Check: file-set / manifest serialization helpers under `source/dnode/vnode/src/tsdb/`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 读取 `current.json` 当前视图，逐个文件组建立 repair 扫描单元。
2. 实现“受影响文件组”初步判定：缺文件、描述不一致、关键结构不可解析。
3. 落地用户确认的缺失联动规则：`stt` 单独摘除；`head/data` 缺失联动摘除 `head/data/sma`。
4. 为受影响文件组生成备份目录，并备份原始文件与对应 manifest 描述信息。
5. 写入文件组级 repair 日志骨架。
6. 增加 E2E：验证仅受影响文件组被备份，且备份包含描述信息。
7. 运行用例，确认现状在块级修复未完成前至少能正确识别和备份。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k backup`

**Commit:**
- `feat(tsdb): back up affected file sets with manifest info`

### Task 6: 实现文件组级决策与最小摘除流程

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Check: `source/dnode/vnode/src/tsdb/tsdbFSet2.h`
- Check: `source/dnode/vnode/src/tsdb/tsdbFS2.c`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 实现文件组三态决策：保留、部分重建、整组摘除。
2. 先支持最小摘除链路：对完全不可恢复文件组从结果视图中移除。
3. 支持 `stt` 缺失/损坏时仅摘除 `stt`。
4. 支持 `head` 或 `data` 缺失时联动移除 `head/data/sma`。
5. 写出新的结果视图与最小 `current.json` 更新逻辑。
6. 跑通“文件组级摘除后 vnode 可启动”的 E2E。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k fileset`

**Commit:**
- `feat(tsdb): support fileset-level drop and stt removal`

### Task 7: 实现 `head/data/sma` 主链路块级扫描与重建

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Create or Update: helper readers/writers under `source/dnode/vnode/src/tsdb/`
- Check: `source/dnode/vnode/src/tsdb/tsdbDataFileRAW.c`
- Check: block reader utilities under `source/dnode/vnode/src/tsdb/`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 从 `head` 枚举候选块，并到 `data` 中做边界、可读性、校验与元数据自洽性验证。
2. 实现块保留规则，构建“有效块列表”。
3. 用有效块写新的 `data` 文件。
4. 基于有效块列表生成新的 `head` 文件。
5. 基于最终保留块重建 `sma`，或在必要时生成可接受的降级结果。
6. 增加坏块场景 E2E：仅坏块被摘除，其余块保留。
7. 运行测试，确认修复后 vnode 可启动且结果稳定。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k block`

**Commit:**
- `feat(tsdb): rebuild head/data/sma from valid blocks`

### Task 8: 收敛 `stt/tomb` 的第一版保守处理

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Check: `source/dnode/vnode/src/tsdb/tsdbReadUtil.c`
- Check: `source/dnode/vnode/src/tsdb/tsdbCommit2.c`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 评估 `stt` 的现有读取能力，优先实现“可确认有效则保留，否则摘除”。
2. 评估 `tomb` 的读取与重建复杂度，按同样保守原则落地第一版。
3. 若某部分无法稳妥保留，则明确写入 repair 日志并在结果中剔除。
4. 增加对应 E2E，验证降级行为与 vnode 可启动性。
5. 控制范围，不在本任务里追求最细粒度 `stt/tomb` 恢复。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k stt_or_tomb`

**Commit:**
- `feat(tsdb): add conservative stt and tomb repair handling`

### Task 9: 完成日志、启动内去重与回归验证

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`
- Check: existing meta repair tests under `test/cases/80-Components/01-Taosd/`

**Steps:**
1. 完善 repair 日志，记录文件组级决策、块级统计和最终结果。
2. 增加“同一 vnode 在同一启动内不重复 repair”的保护。
3. 跑通 TSDB repair 全量用例。
4. 回归 `meta force repair` 与 phase1 参数层测试，确认未破坏既有功能。
5. 更新第三阶段留痕文件，记录最终实现与验证结果。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1`

**Commit:**
- `test(repair): finalize tsdb force repair coverage and regression checks`
