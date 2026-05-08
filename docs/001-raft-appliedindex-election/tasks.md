# Tasks: Raft Election with Applied Progress Priority

**Input**: Design documents from `/specs/001-raft-appliedindex-election/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/request-vote-applied-index.md, quickstart.md

**Tests**: Add focused Google Test and cluster-regression coverage because the plan explicitly requires validation for serialization, vote-grant decisions, tie handling, and election outcomes.

**Organization**: Tasks are grouped by user story so each behavior change can be implemented and validated independently.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare shared test scaffolding for the extended RequestVote contract

- [X] T001 Update shared RequestVote helper declarations for candidateAppliedIndex in community/source/libs/sync/test/sync_test_lib/inc/syncTest.h
- [X] T002 [P] Extend shared RequestVote debug and JSON serialization for candidateAppliedIndex in community/source/libs/sync/test/sync_test_lib/src/syncMessageDebug.c

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Add the common message-contract and runtime plumbing required by every user story

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 Add candidateAppliedIndex to SyncRequestVote in community/source/libs/sync/inc/syncMessage.h
- [X] T004 Update RequestVote allocation, serialization, and RPC conversion for candidateAppliedIndex in community/source/libs/sync/src/syncMessage.c
- [X] T005 [P] Add syncNodeGetAppliedIndex declaration and implementation in community/source/libs/sync/inc/syncInt.h and community/source/libs/sync/src/syncMain.c
- [X] T006 [P] Extend request-vote logging interfaces for applied-index-aware decision traces in community/source/libs/sync/inc/syncUtil.h and community/source/libs/sync/src/syncUtil.c

**Checkpoint**: The applied-index field is available in the internal vote contract, can be encoded/decoded in tests, and can be queried/logged by sync runtime code.

---

## Phase 3: User Story 1 - Prefer Most Up-to-Date Candidate (Priority: P1) 🎯 MVP

**Goal**: Prefer the eligible candidate with the highest applied progress after existing log-recency checks pass.

**Independent Test**: Trigger elections among otherwise eligible candidates with different applied indexes and verify the highest applied-index candidate wins.

### Tests for User Story 1

- [X] T007 [P] [US1] Extend RequestVote round-trip coverage for candidateAppliedIndex in community/source/libs/sync/test/syncRequestVoteTest.cpp
- [X] T008 [P] [US1] Add unequal-applied-index leader selection coverage in community/source/libs/sync/test/syncElectTest.cpp

### Implementation for User Story 1

- [X] T009 [US1] Populate candidateAppliedIndex in outgoing vote requests in community/source/libs/sync/src/syncElection.c
- [X] T010 [US1] Grant votes to higher-applied eligible candidates after log-recency validation in community/source/libs/sync/src/syncRequestVote.c

**Checkpoint**: User Story 1 is complete when unequal applied-index candidates still satisfy current Raft safety checks and the most up-to-date eligible node wins election.

---

## Phase 4: User Story 2 - Preserve Election Continuity Under Ties (Priority: P2)

**Goal**: Keep election outcomes deterministic and live when eligible candidates have equal applied progress.

**Independent Test**: Re-run equal-applied-index election scenarios and verify a single leader is elected without repeated deadlock.

### Tests for User Story 2

- [X] T011 [P] [US2] Add equal-applied-index vote-decision coverage in community/source/libs/sync/test/syncRequestVoteTest.cpp
- [X] T012 [P] [US2] Add deterministic repeated-tie election coverage in community/source/libs/sync/test/syncVotesGrantedTest.cpp

### Implementation for User Story 2

- [X] T013 [US2] Preserve deterministic tie fallback for equal applied indexes in community/source/libs/sync/src/syncRequestVote.c

**Checkpoint**: User Story 2 is complete when equal applied-index candidates still converge on one leader through the existing deterministic fallback path.

---

## Phase 5: User Story 3 - Prevent Regressive Leader Selection (Priority: P3)

**Goal**: Deprioritize lagging candidates without blocking election progress when applied-index ordering is weak or unavailable.

**Independent Test**: Simulate lagging and missing-applied-index candidates, then confirm stale nodes lose preference when a better candidate exists and elections still complete under degraded inputs.

### Tests for User Story 3

- [X] T014 [P] [US3] Add lower-applied and unavailable-applied RequestVote coverage in community/source/libs/sync/test/syncRequestVoteTest.cpp
- [X] T015 [P] [US3] Add lagging-node preference regression coverage in community/tests/pytest/cluster/syncingTest.py

### Implementation for User Story 3

- [X] T016 [US3] Define and implement fallback handling for lower, equal, and unavailable applied indexes in community/source/libs/sync/src/syncRequestVote.c
- [X] T017 [US3] Emit applied-progress election decision evidence in community/source/libs/sync/src/syncRequestVote.c and community/source/libs/sync/src/syncUtil.c

**Checkpoint**: User Story 3 is complete when lagging candidates no longer win against more up-to-date eligible peers and operators can explain the outcome from logs.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and documentation updates that affect all stories

- [X] T018 [P] Add failover timing baseline and comparison steps in specs/001-raft-appliedindex-election/quickstart.md and community/tests/pytest/cluster/syncingTest.py
- [X] T019 [P] Add election audit verification steps and same-version rollout notes in specs/001-raft-appliedindex-election/quickstart.md and specs/001-raft-appliedindex-election/contracts/request-vote-applied-index.md
- [X] T020 Run the validation commands documented in specs/001-raft-appliedindex-election/quickstart.md against build/community/source/libs/sync/test/syncRequestVoteTest, build/community/source/libs/sync/test/syncVotesGrantedTest, and build/community/source/libs/sync/test/syncElectTest

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies; can start immediately.
- **Foundational (Phase 2)**: Depends on Setup; blocks all user-story work.
- **User Story 1 (Phase 3)**: Depends on Foundational; establishes the MVP behavior change.
- **User Story 2 (Phase 4)**: Depends on Foundational and should land after User Story 1 because it refines the same vote-decision path.
- **User Story 3 (Phase 5)**: Depends on Foundational and should land after User Story 1 because it extends the same applied-index comparison path.
- **Polish (Phase 6)**: Depends on all targeted user stories being complete.

### User Story Dependencies

- **User Story 1 (P1)**: No dependency on other user stories; this is the MVP slice.
- **User Story 2 (P2)**: Uses the same RequestVote comparison path as User Story 1 but remains independently testable through equal-index scenarios.
- **User Story 3 (P3)**: Uses the same RequestVote comparison path as User Story 1 but remains independently testable through lagging and unavailable-index scenarios.

### Within Each User Story

- Tests must fail before the corresponding implementation task is considered complete.
- Message or helper contract updates must land before vote-logic changes that consume them.
- Runtime vote-decision changes must land before observability or cluster-regression verification.

### Recommended Story Completion Order

- **US1 → US2 → US3**

---

## Parallel Opportunities

- `T001` and `T002` can proceed independently because they touch separate test helper files.
- `T005` and `T006` can proceed in parallel after `T003` because runtime applied-index accessors and logging interfaces are independent.
- `T007` and `T008` can proceed in parallel for User Story 1.
- `T011` and `T012` can proceed in parallel for User Story 2.
- `T014` and `T015` can proceed in parallel for User Story 3.

---

## Parallel Example: User Story 1

```bash
# Launch both User Story 1 tests in parallel:
Task: "Extend RequestVote round-trip coverage for candidateAppliedIndex in community/source/libs/sync/test/syncRequestVoteTest.cpp"
Task: "Add unequal-applied-index leader selection coverage in community/source/libs/sync/test/syncElectTest.cpp"
```

## Parallel Example: User Story 2

```bash
# Launch both User Story 2 tests in parallel:
Task: "Add equal-applied-index vote-decision coverage in community/source/libs/sync/test/syncRequestVoteTest.cpp"
Task: "Add deterministic repeated-tie election coverage in community/source/libs/sync/test/syncVotesGrantedTest.cpp"
```

## Parallel Example: User Story 3

```bash
# Launch both User Story 3 regressions in parallel:
Task: "Add lower-applied and unavailable-applied RequestVote coverage in community/source/libs/sync/test/syncRequestVoteTest.cpp"
Task: "Add lagging-node preference regression coverage in community/tests/pytest/cluster/syncingTest.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational.
3. Complete Phase 3: User Story 1.
4. Validate `syncRequestVoteTest` and `syncElectTest` before moving on.

### Incremental Delivery

1. Land the shared RequestVote contract and runtime plumbing.
2. Deliver User Story 1 as the leader-preference MVP.
3. Add deterministic tie handling coverage and safeguards in User Story 2.
4. Add lagging-candidate regression coverage and observability in User Story 3.
5. Finish with failover baseline capture, audit guidance, quickstart validation, and documentation cleanup.

### Parallel Team Strategy

1. One developer handles foundational message-contract work while another prepares shared test scaffolding.
2. After Foundational is complete, tests for each user story can be prepared in parallel before runtime changes land.
3. Cluster regression work in `community/tests/pytest/cluster/syncingTest.py` can proceed alongside runtime observability changes.

---

## Notes

- [P] tasks touch different files and can be worked in parallel.
- All user-story tasks carry `[US1]`, `[US2]`, or `[US3]` for traceability.
- Every task includes exact file paths so an implementation agent can execute it directly.
- Suggested MVP scope: Phase 1 + Phase 2 + Phase 3.