# feat/addShowQuery 分支功能说明（排除 merge main/3.0）

## 1. 文档目的

本文档用于完整说明 `feat/addShowQuery` 分支在 `SHOW QUERIES` 方向上的功能演进与最终行为，便于评审、联调和回归测试。

说明范围为：

- 从 `origin/3.0` 分叉点到当前分支 `HEAD` 的提交内容
- 仅包含功能提交与修复提交
- 明确排除 merge main/3.0 相关合并逻辑

## 2. 范围与排除项

### 纳入范围

- 查询阶段追踪（`phase_state` / `phase_start_time`）
- 子任务状态与时间展示（`sub_status`）
- 调度器执行阶段细化（含 `execute:waiting`）
- 客户端、调度器、心跳、mnode 展示链路打通
- 对应测试用例更新

### 排除范围

- merge `main` / `3.0` 带入的公共变更
- 与本功能无直接关系的基线合入逻辑

## 3. 需求背景

原始 `SHOW QUERIES` 信息不足，主要问题包括：

- 阶段字段语义不统一（`current_phase` 命名与时间字段不匹配）
- `sub_status` 仅有粗粒度状态，定位执行瓶颈困难
- 子任务时间展示为时间戳，人工排障成本高
- 发生同相位重复写入时，`phase_start_time` 会被不必要刷新

分支目标是将阶段与子任务状态升级为可观测、可解释、可回放的状态信息。

## 4. 端到端数据流改造（5 层）

### 4.1 Scheduler 层（状态源头）

- 在 `schedulerGetTasksStatus()` 构建子任务精细状态
- 子任务状态由“任务类型 + 运行态”组成，例如：
  - `scan/init`
  - `scan/executing`
  - `scan/partial_succeed`
  - `merge/init`
  - `scan/wait_retry`
  - `scan/waiting_flow`
  - `scan/retry(2)`

关键文件：

- `source/libs/scheduler/src/scheduler.c`
- `source/libs/scheduler/src/schTask.c`
- `source/libs/scheduler/src/schRemote.c`
- `source/libs/scheduler/src/schJob.c`

### 4.2 Client 层（请求态同步）

- `SRequestObj` 增加阶段字段，承载 query 生命周期相位
- 各异步路径在相位变更时才更新阶段时间，避免时间抖动

关键文件：

- `source/client/inc/clientInt.h`
- `source/client/src/clientMain.c`
- `source/client/src/clientImpl.c`
- `source/client/src/clientHb.c`

### 4.3 心跳 / 消息编解码层（跨节点传输）

- `SQueryDesc` 携带 `execPhase`、`phaseStartTime`
- `SQuerySubDesc` 携带 `status`、`startTs`
- 序列化 / 反序列化链路与结构一致

关键文件：

- `include/common/tmsg.h`
- `source/common/src/msg/tmsg.c`

### 4.4 mnode 展示层（SHOW QUERIES 落地）

- `sub_status` 展示格式统一为 `tid:status:startTime`
- `startTime` 输出人类可读时间（`YYYY-MM-DD HH:MM:SS.mmm`）
- 对 `NULL` 子任务描述做保护，降低异常输入风险

关键文件：

- `source/dnode/mnode/impl/src/mndProfile.c`
- `source/common/src/systable.c`

### 4.5 Test 层（行为校验）

- `phase_state` / `phase_start_time` 列存在性校验
- 阶段值集合合法性校验（含 `execute:waiting`）
- `sub_status` 三段结构与精细状态格式校验

关键文件：

- `test/cases/24-Users/test_query_phase_tracking.py`

## 5. 本分支核心行为（最终态）

### 5.1 phase 字段

- `phase_state`：反映当前主阶段/子阶段
- `phase_start_time`：仅在 `phase_state` 发生变化时刷新
- 新增执行子阶段：`execute:waiting`

### 5.2 sub_status 字段

- 展示格式：`tid:status:startTime`
- `status` 为细粒度状态，采用 `type/state` 形式
- `startTime` 为可读时间字符串，未知时为 `-`

示例：

- `10001:scan/executing:2026-03-16 21:30:01.123`
- `10002:merge/init:-`

## 6. 提交脉络（非 merge，按主题归并）

> 说明：以下为相对 `origin/3.0` 的非 merge 提交主题归并，且排除 merge main/3.0 逻辑。

### A. 阶段追踪主功能

- `54645cb7d3` feat: add query phase tracking for SHOW QUERIES
- `63ec536075` fix: complete query phase tracking with proper EQueryExecPhase enum
- `a8e8cebc38` Add query execution phase tracking and update related tests

### B. 子任务状态与时间追踪

- `f98d340dcd` feat: add startTs/endTs timing fields to sub-task in SHOW QUERIES
- `e03c928ce0` feat: improve sub_status display in SHOW QUERIES
- `ea20d0f8a8` feat: add execute:waiting phase for scan-to-merge transition
- `e0a208bdd2` fix: only update phaseStartTime when phase actually changes

### C. 稳定性与可维护性修复

- `c98b454a9b` fix: add NULL check for taosArrayGet and remove unused import
- `bf8b384e29` fix: resolve multiple issues in query phase tracking
- 以及若干 `refactor` / `style` / `update` / `revert` 配套提交

## 7. 变更文件清单（功能相关）

- `include/common/tmsg.h`
- `include/libs/scheduler/scheduler.h`
- `source/client/inc/clientInt.h`
- `source/client/src/clientHb.c`
- `source/client/src/clientImpl.c`
- `source/client/src/clientMain.c`
- `source/common/src/msg/tmsg.c`
- `source/common/src/systable.c`
- `source/dnode/mnode/impl/src/mndProfile.c`
- `source/libs/scheduler/inc/schInt.h`
- `source/libs/scheduler/src/schJob.c`
- `source/libs/scheduler/src/schRemote.c`
- `source/libs/scheduler/src/schTask.c`
- `source/libs/scheduler/src/scheduler.c`
- `test/cases/24-Users/test_query_phase_tracking.py`

## 8. 回归建议

建议至少覆盖以下场景：

- 单查询短链路（parse -> done）
- 超级表多 vnode 查询（scan + merge）
- 高并发查询下的 `phase_start_time` 稳定性
- 子任务重试与流控状态（`wait_retry` / `waiting_flow` / `retry(n)`）
- `SHOW QUERIES` 字段格式兼容性（含空值和异常分支）

## 9. 结论

`feat/addShowQuery` 分支已将 `SHOW QUERIES` 从“粗粒度可见”提升为“阶段与子任务双维度可观测”：

- 阶段追踪更准确（避免同相位刷新时间）
- 子任务状态更细（可识别类型、执行态、重试与流控）
- 展示更可读（时间可读化）
- 端到端链路（scheduler -> client/hb -> mnode -> test）一致

在排除 merge main/3.0 的前提下，该分支的功能改动已形成完整闭环。
