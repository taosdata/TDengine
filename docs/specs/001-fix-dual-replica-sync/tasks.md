# Tasks: 双副本恢复同步阻塞修复

**Input**: Design documents from `/specs/001-fix-dual-replica-sync/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: 本特性在 spec 中明确要求测试覆盖（SC-003/SC-005/SC-006），因此包含测试任务并按“先测后改”执行。

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件且不依赖未完成任务）
- **[Story]**: 用户故事标签（US1/US2/US3）
- 每条任务描述包含明确文件路径

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 建立可执行的开发与测试入口

- [ ] T001 校验构建与测试基线于 `/root/github/taosdata/TDinternal/build`（`cmake --build` 与 `ctest --output-on-failure`）
- [X] T002 创建 sync 单测目录与初始化文件于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/`
- [X] T003 [P] 创建双副本系统测试目录于 `/root/github/taosdata/TDinternal/community/tests/system-test/2-query/dual_replica/`
- [X] T004 [P] 对齐 quickstart 执行入口说明于 `/root/github/taosdata/TDinternal/specs/001-fix-dual-replica-sync/quickstart.md`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 建立所有用户故事共享的判定/配置/诊断基础能力

**⚠️ CRITICAL**: 本阶段完成前，不进入任何用户故事实现

- [X] T005 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c` 增加 follower lag 统一计算辅助逻辑（leaderCommitIndex、matchIndex、lag）
- [X] T006 [P] 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c` 增加 `syncLogLagThreshold` 默认值、读取与非法值回退
- [X] T007 [P] 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c` 增加 `syncCatchupLogIntervalMs` 默认值、读取与非法值回退
- [X] T008 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/inc/syncInt.h` 增加 catchup 观测字段声明（进度日志节流与诊断复用）
- [X] T009 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c` 增加 CHECK_SYNC 诊断输出结构（lag/threshold/snapshotActive）

**Checkpoint**: Foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - 大量写入后副本恢复不阻塞写入 (Priority: P1) 🎯 MVP

**Goal**: follower 未追平时保持 ASSIGNED_LEADER 可写，避免提前回切造成长时间阻塞

**Independent Test**: 停副本->单副本持续写->恢复副本，验证恢复期间写入无 >30 秒停顿，且不会过早 SYNCED

### Tests for User Story 1

- [ ] T010 [P] [US1] 新增单测 Scenario-A（lag 超阈值返回 NOT_SYNCED）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check.py`
- [ ] T011 [P] [US1] 新增单测 Scenario-C（lag 在阈值内返回 SYNCED）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check.py`
- [ ] T012 [P] [US1] 新增单测 Scenario-B（snapshotActive + 混合判定）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check_snapshot_mix.py`
- [ ] T013 [P] [US1] 新增系统测试（恢复期间保持 ASSIGNED_LEADER 可写）于 `/root/github/taosdata/TDinternal/community/tests/system-test/2-query/dual_replica/test_assigned_leader_no_early_switch.py`

### Implementation for User Story 1

- [X] T014 [US1] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c` 的 `syncCheckSynced()`，引入 lag 阈值判定并实现 snapshot 混合规则
- [X] T015 [US1] 修改 `/root/github/taosdata/TDinternal/community/source/dnode/vnode/src/vnd/vnodeSvr.c`，在 CHECK_SYNC 响应中返回新判定分支结果
- [X] T016 [US1] 修改 `/root/github/taosdata/TDinternal/community/source/dnode/mnode/impl/src/mndArbGroup.c`，防止 lag 超阈值时提前清除 `assignedLeader`
- [ ] T017 [US1] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncReplication.c`，确保 follower matchIndex/lag 观测数据实时更新
- [ ] T018 [US1] 执行并通过 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check.py`、`/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check_snapshot_mix.py` 与 `/root/github/taosdata/TDinternal/community/tests/system-test/2-query/dual_replica/test_assigned_leader_no_early_switch.py`

**Checkpoint**: US1 可独立演示与验证（MVP）

---

## Phase 4: User Story 2 - 追数据进度可观测 (Priority: P2)

**Goal**: 输出标准化追赶进度日志与回切日志，便于运维判断恢复状态

**Independent Test**: follower 落后并恢复时，按配置间隔输出 lag 进度，且回切日志包含 term 变化

### Tests for User Story 2

- [ ] T019 [P] [US2] 新增进度日志测试（按 `syncCatchupLogIntervalMs` 输出）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_catchup_log.py`
- [ ] T020 [P] [US2] 新增回切日志测试（状态前后+term 变化）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_transition_log.py`

### Implementation for User Story 2

- [X] T021 [US2] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c`，按 `syncCatchupLogIntervalMs` 周期输出 progress 日志（leaderCommitIndex/matchIndex/lag）
- [ ] T022 [US2] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncPipeline.c`，补充 WAL 追赶阶段的进度上报
- [ ] T023 [US2] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncSnapshot.c`，补充 snapshot 追赶阶段的进度上报与状态日志
- [X] T024 [US2] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c`，补充 ASSIGNED_LEADER -> LEADER 切换日志（state/term/lag）
- [ ] T025 [US2] 执行并通过 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_catchup_log.py` 与 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_transition_log.py`

**Checkpoint**: US2 可独立验证（可观测性闭环）

---

## Phase 5: User Story 3 - 配置追数据完成判定阈值 (Priority: P3)

**Goal**: 提供阈值与日志频率配置能力，并验证边界与默认行为

**Independent Test**: 调整阈值与默认配置，验证判定边界和日志节流行为

### Tests for User Story 3

- [ ] T026 [P] [US3] 新增阈值边界测试（80/100/110）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_lag_threshold_config.py`
- [ ] T027 [P] [US3] 新增默认值回退测试（1000/30000）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_default_config.py`
- [ ] T028 [P] [US3] 新增 SC-003 十次对比测试（>=10 comparisons）于 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check_comparison10.py`

### Implementation for User Story 3

- [X] T029 [US3] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c`，完成 `syncLogLagThreshold` 配置绑定（读取/默认/校验）
- [X] T030 [US3] 修改 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncMain.c`，完成 `syncCatchupLogIntervalMs` 配置绑定（读取/默认/校验）
- [ ] T031 [US3] 执行并通过 `/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_lag_threshold_config.py`、`/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_default_config.py`、`/root/github/taosdata/TDinternal/community/tests/pytest/sync/test_dual_replica_sync_check_comparison10.py`

**Checkpoint**: US3 可独立验证（配置能力闭环）

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: 覆盖跨故事约束（FR-009、SC-006、回归与性能）

- [ ] T032 [P] 在 `/root/github/taosdata/TDinternal/community/source/libs/sync/src/syncSnapshot.c` 明确实现 snapshot 失败自动重试与指数退避上限（3.2s）
- [ ] T033 [P] 新增 snapshot 重试回归测试于 `/root/github/taosdata/TDinternal/community/tests/system-test/2-query/dual_replica/test_snapshot_retry_backoff.py`
- [ ] T034 新增客户端中断预算测试（<=1s）于 `/root/github/taosdata/TDinternal/community/tests/system-test/2-query/dual_replica/test_transition_client_interrupt_budget.py`
- [ ] T035 运行双副本恢复性能回归并记录结果于 `/root/github/taosdata/TDinternal/specs/001-fix-dual-replica-sync/quickstart.md`
- [ ] T036 执行 3 副本与单副本不回归验证于 `/root/github/taosdata/TDinternal/community/tests/system-test/`
- [ ] T037 运行 quickstart 全流程并更新实际命令与判定标准于 `/root/github/taosdata/TDinternal/specs/001-fix-dual-replica-sync/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: 无依赖，可立即开始
- **Phase 2 (Foundational)**: 依赖 Phase 1，且阻塞所有用户故事
- **Phase 3/4/5 (US1/US2/US3)**: 均依赖 Phase 2 完成后可并行启动
- **Phase 6 (Polish)**: 依赖 US1/US2/US3 完成

### User Story Dependencies

- **US1 (P1)**: 仅依赖 Foundational，独立可测
- **US2 (P2)**: 仅依赖 Foundational，独立可测
- **US3 (P3)**: 仅依赖 Foundational，独立可测

### Within Each User Story

- 测试任务先于实现任务
- 核心判定逻辑先于协调层接入
- 实现后执行故事级验证任务

### Parallel Opportunities

- Setup: T003, T004
- Foundational: T006, T007
- US1 Tests: T010, T011, T012, T013
- US2 Tests: T019, T020
- US3 Tests: T026, T027, T028
- Polish: T032, T033

---

## Parallel Example: User Story 1

- 并行执行测试：T010 + T011 + T012 + T013
- 并行执行协调层改动：T015 + T016（不同文件）

## Parallel Example: User Story 2

- 并行执行测试：T019 + T020
- 并行执行进度上报改动：T022 + T023（不同文件）

## Parallel Example: User Story 3

- 并行执行测试：T026 + T027 + T028
- 并行执行配置接入：T029 + T030（同文件，顺序执行；不可并行）

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. 完成 Phase 1 + Phase 2
2. 完成 US1（Phase 3）
3. 验证 SC-001/SC-003/SC-005 的 US1 范围目标
4. 进行阶段性演示

### Incremental Delivery

1. US1: 可用性阻塞修复
2. US2: 可观测性增强
3. US3: 配置弹性与边界验证
4. Polish: FR-009 与 SC-006 及全局回归闭环

### Parallel Team Strategy

1. Engineer A: syncMain/syncReplication 核心判定与配置
2. Engineer B: mnode/vnode 协调逻辑与系统测试
3. Engineer C: 日志与性能/回归验证

---

## Notes

- 所有用户故事任务均采用 `[USx]` 标记并可独立验证
- 所有任务描述均含明确文件路径
- 测试覆盖显式包含 SC-003（10次对比）、SC-005（三场景）、SC-006（<=1s 中断预算）
