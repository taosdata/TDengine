# Feature Specification: Prevent Restoring Stepdown Re-Election

**Feature Branch**: `20260411-assigned-stepdown-guard`  
**Created**: 2026-04-10  
**Status**: Draft  
**Input**: User description: "分析项目代码，分析目前双副本的实现，目前的实现有一个点是在一个副本宕机，比如被kill -9 强行杀掉，另外一个副本变成assigned leader，当另外一个节点重新起来后，在assinged leader副本上的syncNodeOnAppendEntriesReply函数中，会判断commitIndex >= ths->assignedCommitIndex这个条件满足后进行stepdown操作，vgroup开始重新选举，这时另外一个副本如果被选举成leader的话，这副本会有可能进入到restoring状态，导致vgroup无法提供服务。所以请修改一下这个判断条件，加入另外一个副本的状态不在restoring的条件，来避免这个问题的发生。"

## User Scenarios & Testing *(mandatory)*

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.
  
  Assign priorities (P1, P2, P3, etc.) to each story, where P1 is the most critical.
  Think of each story as a standalone slice of functionality that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### User Story 1 - Maintain Service During Replica Recovery (Priority: P1)

As an operator, I need a dual-replica vgroup to keep serving requests while one replica recovers, so that transient node failures do not cause avoidable service interruption.

**Why this priority**: This directly protects availability during a common failure-recovery path and prevents user-facing outages.

**Independent Test**: Can be fully tested by forcing one replica down, allowing the peer to continue as assigned leader, then bringing the failed replica back in restoring state and verifying the vgroup continues to serve without entering a new election loop.

**Acceptance Scenarios**:

1. **Given** a dual-replica vgroup where one replica was force-killed and the surviving replica became assigned leader, **When** the failed replica restarts and is still restoring, **Then** the assigned leader MUST NOT step down solely because commit progress reached the assigned threshold.
2. **Given** a dual-replica vgroup where assigned leader has reached required commit progress and the peer replica is healthy (not restoring), **When** normal handoff conditions are met, **Then** stepdown and re-election behavior proceeds as expected.

---

### User Story 2 - Avoid Invalid Leader Selection Window (Priority: P2)

As a cluster maintainer, I need election triggering conditions to respect peer readiness, so that a recovering replica is not promoted before it can safely provide service.

**Why this priority**: Prevents correctness and availability risks caused by leader transition to an unready replica.

**Independent Test**: Can be tested by repeatedly simulating kill-and-recover cycles and verifying no election chooses a restoring replica as leader during the protected window.

**Acceptance Scenarios**:

1. **Given** a recovering peer that has not completed restore, **When** the assigned leader evaluates whether to trigger stepdown, **Then** the decision MUST include peer readiness and block stepdown if the peer is restoring.

---

### User Story 3 - Preserve Existing Behavior Outside Recovery Edge Case (Priority: P3)

As a developer/operator, I need this safeguard to be narrowly scoped, so that normal replication and election flows are unchanged when no replica is restoring.

**Why this priority**: Limits regression risk and keeps operational behavior predictable.

**Independent Test**: Can be tested by running baseline dual-replica scenarios without restoring peers and confirming election timing and service behavior remain consistent with current expectations.

**Acceptance Scenarios**:

1. **Given** both replicas are healthy and synchronized, **When** normal replication messages are processed, **Then** no additional delay or suppression of expected leader transitions is introduced.

---

### Edge Cases

- Peer replica state is temporarily unknown at decision time: system should default to conservative behavior and avoid stepdown until a non-restoring state is confirmed.
- Peer transitions quickly from restoring to ready between checks: system should allow stepdown once readiness is observed, without requiring manual intervention.
- Both replicas experience rapid restart churn: safeguard should prevent repeated avoidable elections that leave the vgroup unavailable.
- Assigned commit progress regresses or stalls after prior advancement: stepdown decision should still require both progress and peer readiness criteria.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: In a dual-replica vgroup, stepdown from assigned leader MUST require commit progress threshold satisfaction and peer replica readiness.
- **FR-002**: Peer replica readiness MUST treat restoring state as not ready for triggering assigned-leader stepdown.
- **FR-003**: When peer replica is restoring, the system MUST keep the assigned leader in place to maintain service continuity.
- **FR-004**: Once peer replica transitions to a non-restoring ready state and other existing stepdown prerequisites are met, normal stepdown/election flow MUST be allowed.
- **FR-005**: The safeguard MUST be limited to this recovery edge case and MUST NOT alter behavior for steady-state healthy dual-replica operation.
- **FR-006**: The system MUST handle temporary absence or delay of peer-state updates conservatively by avoiding stepdown until readiness is confirmed.
- **FR-007**: Regression tests for dual-replica failure-recovery scenarios MUST include coverage for kill-and-restart sequences that previously led to restoring-leader risk.

### Key Entities *(include if feature involves data)*

- **VGroup Replica Pair**: A two-replica service unit with one active assigned leader and one peer replica.
- **Replica Readiness State**: Operational state of each replica (including restoring and ready) used by leader-transition decisions.
- **Stepdown Decision Context**: Combined runtime conditions (commit progress, peer readiness, and existing prerequisites) used to determine whether assigned leader may step down.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In controlled dual-replica kill-and-restart tests, 100% of runs avoid triggering stepdown while the peer replica remains in restoring state.
- **SC-002**: In the same recovery tests, the vgroup remains continuously available for read/write service in at least 99% of test duration.
- **SC-003**: Once the recovering replica reaches non-restoring ready state, expected leader transition behavior completes without manual action in 100% of qualifying runs.
- **SC-004**: No regressions are observed in baseline healthy dual-replica scenarios, measured by unchanged pass rate across existing election/replication test cases.

## Assumptions


- Feature scope is limited to dual-replica behavior in the assigned-leader stepdown decision path.
- Existing definitions of restoring versus ready states are already available and authoritative for readiness decisions.
- Existing election and replication mechanisms remain unchanged except for the new readiness guard in the targeted decision condition.
- Test environment can reproduce force-kill and restart behavior representative of production failure-recovery conditions.
