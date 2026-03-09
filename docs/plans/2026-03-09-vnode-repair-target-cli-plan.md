# Vnode Repair Target CLI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 `taosd -r` 的 repair CLI 从旧的 `--file-type/--vnode-id` 单 profile 模型切换为新的 `--repair-target` 多 target 模型，并让 `meta` / `tsdb` 执行侧基于归一化 target 查询命中信息。

**Architecture:** `dmMain.c` 负责解析 `--repair-target`、校验全局约束、补齐默认 strategy，并产出全局 repair context 与 target 数组。执行侧通过 `dmMgmt.h` 暴露的只读访问接口查询匹配的 target；`meta` 与 `tsdb` 不再直接读取单一 `fileType/vnodeId` 字段，也不再自行决定默认 strategy。

**Tech Stack:** C, TDengine dnode/vnode management, existing meta/tsdb repair flows, pytest E2E under `test/cases/80-Components/01-Taosd/`

---

### Task 1: 冻结 CLI 约定并更新 handoff 文档

**Files:**
- Create: `docs/plans/2026-03-09-vnode-repair-target-cli-design.md`
- Create: `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`
- Create: `docs/data_repair/05-repair-target-cli/task_plan.md`
- Create: `docs/data_repair/05-repair-target-cli/findings.md`
- Create: `docs/data_repair/05-repair-target-cli/progress.md`

**Steps:**
1. 将本次讨论确认的 CLI grammar、默认 strategy、冲突规则和非兼容决策写入设计文档。
2. 将实现任务拆成 parser、accessor、meta/tsdb 迁移、测试、验证五段。
3. 在 `docs/data_repair/05-repair-target-cli/` 留下当前阶段、决策摘要和下一步切入点。
4. 明确记录当前工作区已有 `source/dnode/mgmt/exe/dmMain.c` 未提交改动，后续实现前先审阅。

**Test/Verify:**
- `test -f docs/plans/2026-03-09-vnode-repair-target-cli-design.md`
- `test -f docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`

**Commit:**
- `docs: freeze vnode repair-target cli design`

### Task 2: 先补失败的命令行矩阵测试

**Files:**
- Update: `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- Check: `source/dnode/mgmt/exe/dmMain.c`

**Steps:**
1. 把现有成功路径从旧参数改成 `--repair-target` 语法。
2. 新增失败路径：`taosd -r` 裸启动、缺少 `--mode`、`--node-type` 非 `vnode`、缺少 `--repair-target`、非法 file type、非法 key、重复 key、重复 target、非法 strategy。
3. 将旧参数 `--file-type`、`--vnode-id`、`--replica-node` 改成 invalid option 断言。
4. 运行单测，确认在当前实现下先失败，锁定改造目标。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair`

**Commit:**
- `test(repair): add failing coverage for repair-target cli`

### Task 3: 重构 `dmMain.c` 的 repair 数据模型

**Files:**
- Update: `source/dnode/mgmt/exe/dmMain.c`
- Update: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- Update: `source/dnode/vnode/src/vnd/vnodeRepair.c`

**Steps:**
1. 删除旧的单 profile 字段设计，不再依赖 `fileType`、`vnodeId` 这类单值字段。
2. 在 `dmMain.c` 中定义新的全局 repair context 和 target 结构。
3. 为 `meta` / `tsdb` / `wal` 与 strategy 定义清晰枚举，避免执行侧继续使用裸字符串判断。
4. 在 `dmMgmt.h` 和 `vnodeRepair.c` 中同步新增只读 accessor / match API，替换旧的 `dmRepairFileType()`、`dmRepairVnodeId()` 等单值接口。

**Test/Verify:**
- `cmake --build debug --target taosd -j8`

**Commit:**
- `refactor(repair): replace single-profile cli model with target list`

### Task 4: 实现 `--repair-target` 解析、默认值补齐与校验

**Files:**
- Update: `source/dnode/mgmt/exe/dmMain.c`
- Check: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`

**Steps:**
1. 在 argv 单遍扫描中增加 `--repair-target` 入口。
2. 解析 grammar `<file-type>:<key>=<value>[:<key>=<value>]...`，并保留原始字符串用于错误输出。
3. 实现 file-type schema 校验、重复 key 检查、重复 target 检查。
4. 在命令行解析阶段补齐默认 strategy：
   - `meta -> from_uid`
   - `tsdb -> shallow_repair`
5. 落地全局前置条件校验：`-r`、`--mode force`、`--node-type vnode`、至少一个 `--repair-target`。
6. 更新 repair help，彻底移除旧参数帮助项。

**Test/Verify:**
- `cmake --build debug --target taosd -j8`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair`

**Commit:**
- `feat(repair): parse and validate repair-target cli`

### Task 5: 让 `meta` repair 改为查询归一化 target

**Files:**
- Update: `source/dnode/vnode/src/meta/metaOpen.c`
- Check: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- Update: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Steps:**
1. 将 `metaOpen.c` 从单一 `file-type=meta` 判断改为“查询当前 vnode 是否命中 meta target”。
2. 使用命中的归一化 strategy，而不是在 `metaOpen.c` 内部自行推断。
3. 适配测试命令，从旧参数切到 `--repair-target meta:vnode=<id>[:strategy=...]`。
4. 增加 default strategy 覆盖，确认省略 strategy 时使用 `from_uid`。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Commit:**
- `feat(meta): consume normalized repair-target config`

### Task 6: 让 `tsdb` repair 改为查询归一化 target

**Files:**
- Update: `source/dnode/vnode/src/tsdb/tsdbFS2.c`
- Check: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- Update: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 将 `tsdbFS2.c` 从单一 `file-type=tsdb + vnode-id` 判断改为“查询当前 vnode + fileid 是否命中 tsdb target”。
2. 使用命中的归一化 strategy，而不是在 `tsdb` 代码里再做默认值处理。
3. 适配现有 TSDB repair E2E，用 `--repair-target tsdb:vnode=<id>:fileid=<fid>[:strategy=...]` 启动。
4. 为 `shallow_repair` 默认值补一条覆盖，确认未传 strategy 时行为正确。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Commit:**
- `feat(tsdb): consume normalized repair-target config`

### Task 7: 收敛 `wal` target 与当前阶段边界

**Files:**
- Update: `source/dnode/mgmt/exe/dmMain.c`
- Update: `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- Check: WAL repair entry files if available under `source/dnode/vnode/`
- Update: `test/cases/80-Components/01-Taosd/test_com_cmdline.py`

**Steps:**
1. 让 parser 接受 `wal:vnode=<id>`。
2. 明确禁止 `wal` 的 `strategy` 字段，并在 CLI 阶段直接报错。
3. 如果当前 WAL repair 执行入口尚未落地，则至少保证 CLI 层与 target 查询层完整可用，并在 handoff 文档中标明运行期仍待补齐。
4. 补一条 WAL CLI 成功解析用例与一条 `strategy` 非法用例。

**Test/Verify:**
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k wal`

**Commit:**
- `feat(wal): add wal repair-target cli support`

### Task 8: 完成回归验证并更新 handoff 文件

**Files:**
- Update: `docs/data_repair/05-repair-target-cli/task_plan.md`
- Update: `docs/data_repair/05-repair-target-cli/findings.md`
- Update: `docs/data_repair/05-repair-target-cli/progress.md`
- Check: `source/dnode/mgmt/exe/dmMain.c`
- Check: `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- Check: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- Check: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Steps:**
1. 重新构建 `taosd`。
2. 跑通 CLI、meta、tsdb 三组回归。
3. 记录 WAL 运行期现状是否已接入。
4. 在 handoff 文件中写清楚验证结果、未覆盖项和下一步剩余工作。

**Test/Verify:**
- `cmake --build debug --target taosd -j8`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Commit:**
- `test(repair): verify repair-target cli end to end`
