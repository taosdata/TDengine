# Internal Contract: Dual-Replica Sync Check and Recovery

## Scope
This contract defines internal behavior between sync runtime (vnode side) and arbiter coordinator (mnode side) for dual-replica recovery.

## Contract A: CHECK_SYNC 判定语义

### Input (logical)
- Leader runtime state: `commitIndex`, `assignedCommitIndex`, `state`
- Follower catchup state: `matchIndex`, `snapshotActive`
- Config: `syncLogLagThreshold`

### Output
- `SYNCED` or `NOT_SYNCED`
- Optional diagnostics: `lag`, `threshold`, `snapshotActive`

### Rules
1. If follower `lag > syncLogLagThreshold` -> output MUST be `NOT_SYNCED`.
2. If follower `lag <= syncLogLagThreshold` and leader base condition is true -> output MAY be `SYNCED`.
3. If snapshot is active and lag still over threshold -> output MUST be `NOT_SYNCED`.
4. Snapshot active alone is not sufficient to block sync if lag already within threshold.

## Contract B: ASSIGNED_LEADER 回切流程

### Preconditions
- CHECK_SYNC returns `SYNCED`
- Arbiter clears assignment intent for current group

### Required Transition
1. Runtime exits `ASSIGNED_LEADER` path.
2. Term is increased.
3. Node participates in normal Raft election.
4. Only elected leader can serve normal quorum-2 commit path.

### Safety Guarantees
- No direct state jump from ASSIGNED_LEADER to steady LEADER without election.
- 3-replica and single-replica behavior remains unchanged.

## Contract C: 可观测性

### Progress Log Contract
- Emit catchup progress every `syncCatchupLogIntervalMs`.
- Each entry includes at least: `leaderCommitIndex`, `followerMatchIndex`, `lag`.

### Transition Log Contract
- On recovery completion, emit transition log with: previous state, next state, term change, observed lag.

## Contract D: 失败恢复

### Snapshot Failure
- Snapshot transfer failures MUST auto-retry with exponential backoff.
- Backoff upper bound aligns with replication retry policy (max 3.2s).
- Manual operator intervention is not required for retry.
