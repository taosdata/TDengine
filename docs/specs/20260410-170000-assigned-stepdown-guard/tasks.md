# Tasks: Prevent Restoring Stepdown Re-Election

**Input**: Design documents from `/specs/20260410-170000-assigned-stepdown-guard/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/, quickstart.md

**Tests**: Test tasks are included because the specification explicitly requires regression coverage for dual-replica kill-and-restart scenarios.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Every task includes an exact file path

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare feature-scoped test/documentation scaffolding and helper utilities.

- [X] T001 Create feature test note header and scenario index in specs/20260410-170000-assigned-stepdown-guard/quickstart.md
- [X] T002 [P] Add feature-specific sync stepdown debug log marker constants in community/source/libs/sync/src/syncUtil.c
- [X] T003 [P] Add a reusable dual-replica recovery test helper skeleton in community/tests/system-test/common/assigned_stepdown_guard.py

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build shared decision primitives required before user-story logic.

**CRITICAL**: No user story work starts before this phase is complete.

- [X] T004 Extend stepdown decision context helpers to expose peer restoring readiness in community/source/libs/sync/inc/syncInt.h
- [X] T005 Implement peer restoring-readiness query helper over log replication managers in community/source/libs/sync/src/syncPipeline.c
- [X] T006 Wire helper declaration/contract for replication readiness access in community/source/libs/sync/inc/syncPipeline.h
- [X] T007 Add conservative unknown-state fallback handling for readiness query in community/source/libs/sync/src/syncPipeline.c
- [X] T008 Add unit-like internal assertions/logs for readiness helper invariants in community/source/libs/sync/src/syncMain.c

**Checkpoint**: Foundation ready; user stories can begin.

---

## Phase 3: User Story 1 - Maintain Service During Replica Recovery (Priority: P1) 🎯 MVP

**Goal**: Prevent assigned leader stepdown while peer replica is restoring.

**Independent Test**: Kill one replica, recover peer in restoring, verify assigned leader does not step down and service stays available.

### Tests for User Story 1

- [X] T009 [P] [US1] Add integration test for kill-and-restart restoring guard in community/tests/system-test/2-query/assigned_stepdown_restoring_guard.py
- [X] T010 [US1] Register US1 test entry in community/tests/system-test/test.py

### Implementation for User Story 1

- [X] T011 [US1] Add restoring-aware gate around assigned leader stepdown in community/source/libs/sync/src/syncAppendEntriesReply.c
- [X] T012 [US1] Emit explicit log when stepdown is blocked by peer restoring state in community/source/libs/sync/src/syncAppendEntriesReply.c
- [X] T013 [US1] Ensure commit-index-only path still commits correctly for non-assigned leader flow in community/source/libs/sync/src/syncAppendEntriesReply.c

**Checkpoint**: US1 is independently functional and testable (MVP).

---

## Phase 4: User Story 2 - Avoid Invalid Leader Selection Window (Priority: P2)

**Goal**: Block re-election trigger until peer readiness is confirmed non-restoring.

**Independent Test**: Repeated failure/recovery loops never elect restoring peer due to assigned-leader stepdown.

### Tests for User Story 2

- [X] T014 [P] [US2] Add loop recovery regression scenario for repeated crash/restart in community/tests/system-test/2-query/assigned_stepdown_recovery_loop.py
- [X] T015 [US2] Add US2 scenario invocation to loop harness config in community/tests/system-test/loop.sh

### Implementation for User Story 2

- [X] T016 [US2] Integrate readiness helper into assigned leader transition checks in community/source/libs/sync/src/syncAppendEntriesReply.c
- [X] T017 [US2] Guard against stale or missing peer state by treating it as not-ready in community/source/libs/sync/src/syncPipeline.c
- [X] T018 [US2] Add trace log for readiness state transitions relevant to election gating in community/source/libs/sync/src/syncMain.c

**Checkpoint**: US1 and US2 both work independently.

---

## Phase 5: User Story 3 - Preserve Existing Behavior Outside Recovery Edge Case (Priority: P3)

**Goal**: Keep healthy steady-state behavior unchanged while applying the restoring guard.

**Independent Test**: Healthy dual-replica baseline scenarios maintain prior transition behavior and pass rates.

### Tests for User Story 3

- [X] T019 [P] [US3] Add healthy dual-replica non-regression scenario in community/tests/system-test/2-query/assigned_stepdown_healthy_baseline.py
- [X] T020 [US3] Add expected-log assertions for no extra suppression in healthy path in community/tests/system-test/2-query/assigned_stepdown_healthy_baseline.py

### Implementation for User Story 3

- [X] T021 [US3] Refine stepdown condition ordering to keep previous leader path behavior unchanged in community/source/libs/sync/src/syncAppendEntriesReply.c
- [X] T022 [US3] Add/adjust comments documenting narrow scope of restoring guard in community/source/libs/sync/src/syncAppendEntriesReply.c

**Checkpoint**: US1, US2, and US3 are independently functional and validated.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, docs alignment, and cleanup.

- [X] T023 [P] Update quickstart verification steps with exact test commands in specs/20260410-170000-assigned-stepdown-guard/quickstart.md
- [X] T024 [P] Document no-new-external-contract confirmation in specs/20260410-170000-assigned-stepdown-guard/contracts/README.md
- [X] T025 Run end-to-end feature checklist validation and update notes in specs/20260410-170000-assigned-stepdown-guard/research.md
- [X] T026 Add availability-duration metric assertions for SC-002 in community/tests/system-test/2-query/assigned_stepdown_restoring_guard.py

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: Starts immediately.
- **Phase 2 (Foundational)**: Depends on Phase 1 completion; blocks all user stories.
- **Phase 3+ (User Stories)**: Depend on Phase 2 completion.
- **Phase 6 (Polish)**: Depends on completion of selected user stories.

### User Story Dependencies

- **US1 (P1)**: Starts after Phase 2; no dependency on US2/US3.
- **US2 (P2)**: Starts after Phase 2; independent from US1 implementation files except shared foundational helpers.
- **US3 (P3)**: Starts after Phase 2; validates non-regression over the same guarded code path.

### Within Each User Story

- Tests are created before implementation and expected to fail before code changes.
- Core logic update precedes log/assertion refinements.
- Story-specific registration/harness updates follow test creation.

---

## Parallel Opportunities

- **Setup**: T002 and T003 can run in parallel after T001.
- **Foundational**: T005 and T006 can run in parallel; T007 follows T005; T008 follows T004-T007.
- **US1**: T009 can run while preparing T011; T012 and T013 follow T011.
- **US2**: T014 and T016 can run in parallel once foundational work is done; T017 follows T016.
- **US3**: T019 and T021 can run in parallel, then T020 and T022.
- **Polish**: T023 and T024 run in parallel; T026 follows test implementation updates; T025 runs last.

---

## Parallel Example: User Story 1

- Parallel task: T009 [US1] in community/tests/system-test/2-query/assigned_stepdown_restoring_guard.py
- Parallel task: T011 [US1] in community/source/libs/sync/src/syncAppendEntriesReply.c

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1 and Phase 2.
2. Complete Phase 3 (US1).
3. Validate US1 independently with scenario A from quickstart.
4. Demo/deploy MVP behavior fix.

### Incremental Delivery

1. Setup + Foundational complete.
2. Deliver US1 and validate.
3. Deliver US2 loop/recovery resilience and validate.
4. Deliver US3 non-regression coverage and validate.
5. Finish Polish phase and close feature.

### Parallel Team Strategy

1. Engineer A: Foundational helpers (T004-T008).
2. Engineer B: US1 test + implementation path (T009-T013).
3. Engineer C: US2/US3 regression scenarios (T014-T022).

---

## Format Validation

- All tasks use required checklist syntax: `- [ ] T### ...`
- All story-phase tasks include story labels: [US1], [US2], [US3]
- [P] used only for parallelizable tasks
- Every task includes a concrete file path
