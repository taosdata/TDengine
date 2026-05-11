# Phase 0 Research: 双副本恢复同步阻塞修复

## Decision 1: 同步判定必须包含 follower 追赶进度
- Decision: `syncCheckSynced()` 由“leader 自身判定”升级为“leader 条件 + follower lag 阈值判定”。
- Rationale: 根因是 `isSync` 过早置为 true，导致提前退出 `ASSIGNED_LEADER`，在 follower 未追上时重新受 quorum=2 约束。
- Alternatives considered:
  - 仅延长 Arbiter 检查间隔：无法消除误判，仅延后暴露。
  - 仅按时间窗口判定：不同写速环境不稳定，难以保证一致性。

## Decision 2: snapshot 进行中采用混合判定
- Decision: snapshot 活跃时默认“未同步”，但若 follower lag 已小于等于阈值，则允许判定“已同步”。
- Rationale: 避免纯状态位导致的长尾阻塞；以数据追平事实为准。
- Alternatives considered:
  - snapshot 活跃一律未同步：安全但易导致不必要等待。
  - 无视 snapshot 状态：存在误切换风险。

## Decision 3: ASSIGNED_LEADER 回归 LEADER 必经 Raft 选举
- Decision: 从降级状态回归正常双副本时，要求 term 递增并通过选举确认 LEADER。
- Rationale: 保持 Raft safety，避免状态跳转引入分裂脑风险。
- Alternatives considered:
  - 直接状态跳转为 LEADER：实现简单但破坏一致性语义。

## Decision 4: 阈值与日志频率配置化
- Decision: 新增/使用配置项 `syncLogLagThreshold`（默认1000条）与 `syncCatchupLogIntervalMs`（默认30000ms）。
- Rationale: 兼顾不同写入速率与可观测需求，避免硬编码造成场景失配。
- Alternatives considered:
  - 固定阈值与固定日志间隔：在高吞吐和低吞吐场景都不理想。

## Decision 5: snapshot 失败自动重试并指数退避
- Decision: snapshot 失败采用自动重试，退避上限与当前复制重试策略保持一致（到3.2s）。
- Rationale: 与现有 `SSyncLogReplMgr` 策略一致，减少人工介入。
- Alternatives considered:
  - 人工干预恢复：运维成本高，恢复时间不稳定。
  - 失败后直接报错停止：可用性差。

## Decision 6: 变更边界限制在 sync + mnode/vnode 协调层
- Decision: 仅修改 `community/source/libs/sync/`、`mndArbGroup.c`、`vnodeSvr.c` 相关路径，不触及存储引擎底层语义。
- Rationale: 最小改动面可降低回归风险并加快验证闭环。
- Alternatives considered:
  - 大范围改造复制框架：风险高、周期长、超出本特性目标。
