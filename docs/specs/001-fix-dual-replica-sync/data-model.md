# Phase 1 Data Model: 双副本恢复同步阻塞修复

## Entity: SyncNodeRuntime
- Description: 单个 vnode 上的同步节点运行态。
- Key Fields:
  - `state`: FOLLOWER/CANDIDATE/LEADER/ASSIGNED_LEADER/LEARNER
  - `commitIndex`: 当前已提交日志索引
  - `assignedCommitIndex`: 进入 ASSIGNED_LEADER 时的基线提交点
  - `restoreFinish`: 节点是否完成恢复流程
  - `term`: Raft 任期
- Validation Rules:
  - ASSIGNED_LEADER -> LEADER 回切必须伴随 term 递增并经选举确认。
  - `commitIndex` 单调不减。

## Entity: FollowerCatchupStatus
- Description: leader 侧对单个 follower 的追赶状态视图。
- Key Fields:
  - `matchIndex`: follower 已确认复制到的索引
  - `leaderCommitIndex`: leader 当前提交索引
  - `lag = leaderCommitIndex - matchIndex`
  - `restored`: 是否追上当前恢复目标
  - `snapshotActive`: 是否处于 snapshot 发送阶段
- Validation Rules:
  - `lag <= syncLogLagThreshold` 才可判定 follower 追平。
  - `lag > syncLogLagThreshold` 必须判定未同步。

## Entity: ArbGroupSyncState
- Description: mnode arbiter 对双副本组的同步判定状态。
- Key Fields:
  - `vgId`
  - `isSync`: 当前组是否已同步
  - `assignedLeader`: 被指定的降级 leader 信息
  - `memberTokens`: 双成员 token
- Validation Rules:
  - 仅当 leader 条件与 follower 追平条件同时满足时，`isSync` 才可置 true。
  - token 变化时必须重新验证同步状态。

## Entity: CatchupConfig
- Description: 恢复判定与观测配置。
- Key Fields:
  - `syncLogLagThreshold` (default: 1000, unit: log entries)
  - `syncCatchupLogIntervalMs` (default: 30000)
- Validation Rules:
  - 阈值需为正整数。
  - 日志间隔需在合理范围内（>0）。

## State Transitions
1. Normal Replication:
   - `LEADER/FOLLOWER` + `isSync=true`
2. Degraded Write Mode:
   - peer down -> arbiter assign -> `ASSIGNED_LEADER`
3. Catchup In Progress:
   - follower rejoins -> WAL replication and/or snapshot -> lag decreases
4. Recovered Normal Mode:
   - sync check passes (leader condition + lag threshold) -> clear assignment -> term bump + election -> `LEADER/FOLLOWER`
