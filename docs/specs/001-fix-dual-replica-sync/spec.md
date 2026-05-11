# Feature Specification: 双副本恢复同步阻塞修复

**Feature Branch**: `001-fix-dual-replica-sync`  
**Created**: 2026-04-10  
**Status**: Draft  
**Input**: User description: "分析项目的代码，理清现有双副本实现。目前双副本实现有个问题，就是当其中一个副本宕掉，vgroup变成单副本运行，这时写入大量数据，然后宕掉的副本起来，vgroup又变回双副本，这时vgroup会长时间无法进入到正常状态，估计是刚起来的副本在追数据。请分析这个问题，并且修复这个问题。"

---

## Background: 现有双副本实现机制

双副本（2-replica）vgroup 使用 **标准 Raft + Arbiter 扩展**机制。由于 2-replica 的 quorum=2，任意副本宕掉后，写入即阻塞。Arbiter（由 mnode 托管）通过将存活副本切换为 `ASSIGNED_LEADER` 状态来解除写入阻塞，使 vgroup 可以在单副本模式下继续运行。

### 关键组件

| 组件 | 位置 | 作用 |
|------|------|------|
| `syncCheckSynced()` | `community/source/libs/sync/src/syncMain.c` | 判断 vgroup 是否已同步 |
| `mndArbCheckSync()` | `community/source/dnode/mnode/impl/src/mndArbGroup.c` | Arbiter 定期检查 isSync |
| `ASSIGNED_LEADER` 状态 | `community/source/libs/sync/src/syncMain.c` | 单副本降级运行模式 |
| `SSyncLogReplMgr` | `community/source/libs/sync/inc/syncInt.h` | 控制 leader → follower 日志复制进度 |
| `snapshotSender` | `community/source/libs/sync/src/syncSnapshot.c` | 快照发送，用于追落太远的副本 |

### 当前故障转移与恢复流程

```
正常状态: R0(LEADER) <──写入+ACK──> R1(FOLLOWER)  isSync=true
R1 宕机:
  - Arbiter 超时检测 → SET_ASSIGNED_LEADER → R0
  - R0: state = ASSIGNED_LEADER, assignedCommitIndex = 当前 commitIndex
  - MND: isSync=false
  - R0 继续接受写入（单副本运行，大量数据写入 WAL）

R1 恢复:
  - R1 上线，以 FOLLOWER 身份连接 R0
  - R0 检测到 R1 重连，启动 SSyncLogReplMgr 追数据
  - 若差距超出 WAL 范围：发送 TSDB snapshot → R1
  - 若在 WAL 范围内：AppendEntries 流水线复制
  - Arbiter 定期（3s）发送 CHECK_SYNC → R0
  - syncCheckSynced() 仅检查 R0 自身 commitIndex >= assignedCommitIndex（始终成立）
  - isSync 过早翻转为 true → Arbiter 发出 "清除 assignedLeader" 信号
  - R0 切换回普通 LEADER 模式（quorum=2）
  - 此时 R1 仍在追数据（snapshot 传输中 / WAL 大量回放中）
  - R0 作为普通 LEADER 尝试提交新写入，但 R1 matchIndex 严重滞后
  - 新写入挂起等待 R1 确认 → vgroup 长时间无法正常写入
```

### 问题根因

`syncCheckSynced()` 的实现只检查 **leader 自身的** `commitIndex >= assignedCommitIndex`，而不检查 **跟随者（恢复中的副本）是否已充分追上**。导致：

1. Arbiter 过早认为双副本已同步（`isSync = true`）
2. 系统提前退出 `ASSIGNED_LEADER` 模式，切换回需要 quorum=2 的普通 `LEADER` 模式
3. R1 仍在追数据期间，R0（普通 LEADER）的每一次写入都需要等待 R1 的 ACK
4. 写入严重阻塞，vgroup 长时间无法进入正常服务状态

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 大量写入后副本恢复不阻塞写入 (Priority: P1)

集群以双副本 vgroup 运行，写入了大量数据（例如数十 GB），副本 R1 宕机（此时 ASSIGNED_LEADER 模式接管）。管理员重启 R1 后，vgroup 应在 R1 完成追数据之前，**保持写入可用**（不因 quorum=2 等待 R1 而阻塞），等 R1 真正追上后再恢复为双副本正常模式。

**Why this priority**: 这是用户直接感受到的核心问题：副本恢复过程中写入被阻塞导致业务中断。

**Independent Test**: 搭建双副本集群，写入 20GB 数据；停止 R1，继续通过 R0（ASSIGNED_LEADER）写入 5GB；重启 R1；观察写入吞吐量——应全程不出现超过 30 秒的写入停顿，直到 R1 完成同步。

**Acceptance Scenarios**:

1. **Given** 双副本 vgroup 已写入大量数据（约 5GB），R1 宕机、R0 切换为 ASSIGNED_LEADER 并继续写入，**When** R1 重新上线，**Then** vgroup 写入吞吐量在 R1 完全追上之前不出现超过 30 秒的停顿，日志中不出现因 quorum 等待而超时的错误。

2. **Given** R1 重新上线且正在进行 snapshot 接收，**When** Arbiter 的 CHECK_SYNC 定时触发，**Then** `syncCheckSynced()` 返回"未同步"状态，Arbiter 不提前清除 `assignedLeader`，R0 继续保持 ASSIGNED_LEADER 模式直到 R1 真正追上。

3. **Given** R1 的 `matchIndex` 已追上 R0 的 `commitIndex`（误差在可配置阈值内），**When** Arbiter 下一次 CHECK_SYNC 触发，**Then** `syncCheckSynced()` 返回"已同步"，系统顺利切换回双副本正常模式，后续写入恢复双副本 quorum 确认。

---

### User Story 2 - 追数据进度可观测 (Priority: P2)

运维人员在 R1 恢复追数据期间，能够通过日志准确了解追数据进度，而不需要靠猜测判断 vgroup 何时恢复正常。

**Why this priority**: 当前追数据期间状态不透明，运维无法判断问题是否卡住还是仍在正常进行。

**Independent Test**: 构造一个副本落后 2GB 的场景；重启副本；观察 taosd 日志中是否每隔合理间隔（≤60 秒）输出"追数据进度"信息，内容包括已追条目数和剩余估算。

**Acceptance Scenarios**:

1. **Given** R1 处于追数据（snapshot 或 WAL 回放）阶段，**When** 追数据进行中，**Then** 日志中每 30 秒输出一次进度信息，至少包含当前 `matchIndex` 与 leader `commitIndex` 的差值。

2. **Given** R1 追数据完成，**When** `matchIndex` 达到阈值，**Then** 日志中输出"副本同步完成，vgroup 切换回双副本正常模式"的明确提示。

---

### User Story 3 - 配置追数据完成判定阈值 (Priority: P3)

集群管理员可以通过配置项调整"追数据完成"的判定阈值（即 follower 落后 leader 多少条目内认为已同步），以适应不同写入速率的生产环境。

**Why this priority**: 不同业务场景写入速率差异极大，硬编码阈值难以满足所有场景。

**Independent Test**: 修改配置项 `syncLogLagThreshold` 为 100，构造 follower 落后 80 条目的场景，验证系统判断为"已同步"；再将落后调整为 110，验证判断为"未同步"。

**Acceptance Scenarios**:

1. **Given** 配置了 `syncLogLagThreshold=N`，**When** follower 的 `matchIndex` 与 leader `commitIndex` 差值 ≤ N，**Then** `syncCheckSynced()` 返回"已同步"。

2. **Given** 未设置该配置项，**When** `syncCheckSynced()` 被调用，**Then** 使用合理的默认值（1000 条）。

---

### Edge Cases

- R1 恢复后 snapshot 传输过程中 R0 继续写入，导致 snapshot 完成时 R1 仍有大量日志差距——应继续走 WAL 追数据而不是循环重发 snapshot。
- R1 在追数据途中再次宕机——系统应能正确重置追数据状态，重启后重新开始追数据。
- 双副本 vgroup 切换回正常 LEADER 模式时，R0 term 正确递增，不影响客户端连接。
- 极端大数据量（100GB+）的追数据场景：snapshot 超时（当前 `SYNC_SNAP_TIMEOUT_MS=180s`）是否需要调整或可配置。
- mnode 在追数据期间发生切主——Arbiter 状态通过 SDB 持久化，不因 mnode 切主而重置 `isSync` 状态。
- 修复逻辑仅在 ASSIGNED_LEADER 模式下生效，不影响 3-replica 及单副本 vgroup 的现有行为。

---

## Clarifications

### Session 2026-04-10

- **Q1: Snapshot 失败恢复策略** → **A**: 自动重试（exponential backoff），与现有 `SSyncLogReplMgr` 重试逻辑保持一致。失败时进行指数退避重试，无需运维介入。

- **Q2: `syncLogLagThreshold` 单位** → **A**: 保持为日志条目数（log entries，默认 1000），不引入时间维度。运维可根据各自环境的写入速率调整此阈值。

- **Q3: Snapshot 传输中的 CHECK_SYNC** → **B**: 混合检查——若 snapshot 正在进行但 follower 的 lag 已在阈值内（通过并行的 WAL 追数据于 snapshot 后追上），则允许返回"已同步"。避免冗长的 snapshot 传输阻塞同步判定。

- **Q4: 切换回 LEADER 时的选举** → **B**: 触发完整 Raft 选举（term 递增），而非直接跳转。保证 Raft 安全性，避免分裂脑。R0 首先递增 term，然后参与选举，重新成为 LEADER。

- **Q5: 追数据进度日志频率** → **C**: 通过配置项 `syncCatchupLogIntervalMs`（默认 30 秒）可调。高吞吐场景可降低日志频率，低吞吐场景可保持密集日志。避免 I/O 瓶颈同时保障可观测性。

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `syncCheckSynced()` 在判断 vgroup 是否已同步时，**MUST** 同时验证正在恢复的 follower 的 `matchIndex` 与 leader 的 `commitIndex` 之差已在"同步完成阈值"范围内，而不仅检查 leader 自身的 `commitIndex >= assignedCommitIndex`。当 follower lag 超过阈值时，必须返回"未同步"。

- **FR-002**: 系统 **MUST** 在 R1 的 `matchIndex` 尚未追上（超出同步完成阈值）时，保持 `ASSIGNED_LEADER` 模式，不提前切换至普通 `LEADER` 模式。即使 leader 的自身条件满足，若 follower 仍在追数据，Arbiter 不得清除 `assignedLeader`。

- **FR-003**: `syncCheckSynced()` 在 snapshot 传输正在进行时（`snapshotSender` 的发送状态为活跃），**原则上** 返回"未同步"；但若同时 follower 的 lag 已在阈值范围内（即 WAL 追数据已追上），则**可特例返回"已同步"**，无需等待 snapshot 发送完毕（混合检查策略）。

- **FR-004**: 系统 **MUST** 在追数据过程中定期向日志输出追数据进度。日志间隔可配置（参见 FR-008），默认 30 秒一次，内容须包含当前 `matchIndex`、leader `commitIndex` 及二者之差（lag）。

- **FR-005**: 同步完成阈值 **MUST** 通过配置项 `syncLogLagThreshold`（默认值 1000 条日志条目）可调。单位为**日志条目数**而非字节或时间，保持实现简单。配置更改在 taosd 重启后生效。

- **FR-006**: R1 完成追数据后，系统 **MUST** 在 Arbiter 下一次 CHECK_SYNC 触发时识别同步完成（via FR-001 返回"已同步"），正确触发从 `ASSIGNED_LEADER` 状态切换回普通 `LEADER` 模式。切换过程中，R0 **MUST** 递增 term，触发 Raft 选举（而非直接跳转为 LEADER），以保证 Raft 安全性。全过程无写入停顿超过 30 秒。

- **FR-007**: 切换回普通 `LEADER` 模式时（从 ASSIGNED_LEADER 切换），日志 **MUST** 输出明确的状态变更记录，包含切换时间、切换前后状态、R1 当时的 `matchIndex`，便于运维追溯。

- **FR-008**: 追数据进度日志间隔 **MUST** 通过配置项 `syncCatchupLogIntervalMs`（默认 30000 ms = 30 秒）可调。高吞吐集群可减少日志量，低吞吐集群可增加密度。

- **FR-009**: Snapshot 失败时（传输中断、接收端崩溃等），系统 **MUST** 自动按指数退避策略重试发送 snapshot（与现有 WAL 复制重试逻辑一致，backoff 最大 3.2s），无需运维干预。

### Key Entities

- **`SSyncNode`**: 单个 vnode 上的同步节点，包含 `state`（当前角色）、`commitIndex`、`assignedCommitIndex`、`restoreFinish`、`arbToken` 字段。
- **`SSyncLogReplMgr`**: 每对 leader-follower 的复制状态机，含 `matchIndex`（已确认复制到的索引）、`restored`（是否追上）、`retryBackoff`（重试退避级别）。
- **`SArbGroup`**: mnode 端 Arbiter 的 vgroup 组状态，含 `isSync`（双副本同步标志）、`assignedLeader`（被指定的 ASSIGNED_LEADER 信息）。
- **`SSyncSnapshotSender`**: Snapshot 传输状态标示，含 `start` 标志位（是否正在传输）、重试计数。

---

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 副本宕机重启后，vgroup 写入中断时间不超过 **30 秒**。具体地，从 R1 重新上线到 vgroup 重新可用（切换回双副本模式或继续保持单副本可写）的全过程，写入延迟不应出现 >30 秒的水位。

- **SC-002**: 在"写入 5GB → R1 宕机 → 通过 R0(ASSIGNED_LEADER) 继续写入 5GB → R1 重启"的标准测试场景中，R1 完成追数据并重新加入双副本前，写入吞吐量 **不低于单副本模式基线的 80%**。不能因为 snapshot 或 WAL 追数据过程而导致吞吐量严重下滑。

- **SC-003**: Arbiter 的 CHECK_SYNC 机制不应误判。具体地，当 R1 的 `matchIndex` 落后 leader `commitIndex` **超过** `syncLogLagThreshold` 时，`syncCheckSynced()` **必须** 返回"未同步"；当落后 **不超过** 阈值时，应返回"已同步"。测试覆盖至少 10 次对比，误判率为 0%。

- **SC-004**: 追数据进度日志按 `syncCatchupLogIntervalMs` 配置输出（默认 30 秒），至少持续至 R1 追数据完成。运维通过日志可清晰看到 lag 的变化趋势。

- **SC-005**: 修复后的 `syncCheckSynced()` 单元测试覆盖三大场景，全部通过：
  - **Scenario A**: follower lag 超出阈值（故意让 follower 滞后），返回"未同步"。
  - **Scenario B**: snapshot 正在传输（`snapshotSender.start == true`），混合检查：若 lag 超阈值则"未同步"，若 lag 在阈值内则"已同步"。
  - **Scenario C**: follower 已追上（lag ≤ 阈值），返回"已同步"。

- **SC-006**: 从 ASSIGNED_LEADER 切换至普通 LEADER 时，term 必须递增，且通过 Raft 选举重新确认。日志记录明确体现"term from X to Y"的递增。客户端连接不应因此中断超过 1 秒。

---

## Assumptions

- 当前 TDengine 版本的双副本实现中，`ASSIGNED_LEADER` 模式下 leader 不会主动等待 follower 追数据，本次修复以此为前提。
- 修复范围集中在 `community/source/libs/sync/` 的 sync 层逻辑，以及 `community/source/dnode/mnode/impl/src/mndArbGroup.c` 的 Arbiter 检查逻辑；不涉及 TSDB 存储层或网络层的改动。
- `syncLogLagThreshold` 的单位为**日志条目数**（log entries），不是字节数，以保持实现简单性和可理解性。
- Snapshot 失败重试采用指数退避策略（与现有 WAL 复制重试逻辑一致），最大退避 3.2 秒，不需要单独配置。
- snapshot 超时阈值 `SYNC_SNAP_TIMEOUT_MS`（当前 180s）设计合理，本次修复不调整该值；若测试发现因数据量极大（100GB+）而触发超时，后续可考虑使其可配置。
- 修复仅在 **ASSIGNED_LEADER 模式（2-replica 单副本运行）** 下启用新逻辑；3-replica 及单副本 vgroup 的现有行为保持不变。
- isSync 标志由 mnode SDB 持久化，mnode 发生切主后状态可正确恢复，本次修复不改变此机制。
- Raft term 在 ASSIGNED_LEADER → LEADER 切换时递增（通过选举完成），且 follower 单调递增 term 无安全隐患（标准 Raft 特性）。
