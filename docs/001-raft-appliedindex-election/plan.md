# Implementation Plan: Raft Election with Applied Progress Priority

**Branch**: `[001-raft-appliedindex-election]` | **Date**: 2026-04-10 | **Spec**: `/root/github/taosdata/TDinternal/specs/001-raft-appliedindex-election/spec.md`
**Input**: Feature specification from `/specs/001-raft-appliedindex-election/spec.md`

**Note**: Planning was bound to `SPECIFY_FEATURE=001-raft-appliedindex-election` because the user explicitly requested no branch creation.

## Summary

Improve TDinternal's Raft leader election so that eligible candidates with higher applied progress are preferred, while preserving existing last-log-term/last-log-index safety checks and election liveness. The implementation will extend the internal `SyncRequestVote` contract to carry the candidate's applied index, incorporate an applied-progress comparison into vote granting after existing log recency validation, and add observability plus unit/integration coverage for unequal-progress, tie, and lagging-node election scenarios.

## Technical Context

**Language/Version**: C for sync runtime, C++ for existing Google Test coverage  
**Primary Dependencies**: TDinternal sync library, internal RPC message layer, WAL/log store, FSM callbacks, Google Test  
**Storage**: Raft store metadata, WAL-backed log store, FSM-reported applied progress  
**Testing**: Existing `community/source/libs/sync/test` Google Test executables plus targeted cluster-style sync tests  
**Target Platform**: Linux server nodes running TDinternal sync/raft services  
**Project Type**: Native distributed systems library  
**Performance Goals**: Keep vote evaluation O(1), add no extra election round-trips, and avoid measurable regression in election completion under current replica counts  
**Constraints**: Preserve Raft log safety, preserve deterministic behavior on ties, keep elections live when applied progress is unavailable/equal, and treat mixed-version wire compatibility as out of scope unless separately planned  
**Scale/Scope**: Per-vgroup replica elections across small quorum-based replica sets with optional learners; change scope is limited to sync election logic, internal message schema, and sync tests

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- Constitution file review result: `.specify/memory/constitution.md` still contains template placeholders and no ratified enforceable principles.
- Gate status before research: PASS by absence of enforceable constitutional constraints.
- Required caution despite missing constitution rules: preserve existing Raft safety invariants, keep plan scoped to sync election paths, and do not assume branch creation is available.
- Post-design re-check: PASS. The design stays within the sync library boundary, preserves current safety gating on log recency, and adds only one internal message-contract change plus corresponding tests.

## Project Structure

### Documentation (this feature)

```text
specs/001-raft-appliedindex-election/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── request-vote-applied-index.md
└── tasks.md
```

### Source Code (repository root)

```text
community/source/libs/sync/
├── inc/
│   ├── syncElection.h
│   ├── syncInt.h
│   ├── syncMessage.h
│   ├── syncRequestVote.h
│   └── syncUtil.h
├── src/
│   ├── syncElection.c
│   ├── syncMain.c
│   ├── syncMessage.c
│   ├── syncRequestVote.c
│   └── syncUtil.c
└── test/
    ├── syncElectTest.cpp
    ├── syncRequestVoteTest.cpp
    ├── syncVotesGrantedTest.cpp
    └── sync_test_lib/

community/tests/
└── pytest/
    └── cluster/
        └── syncingTest.py

community/contrib/test/traft/
├── cluster/
├── join_into_vgroup/
└── single_node/
```

**Structure Decision**: Keep all runtime changes inside `community/source/libs/sync/{inc,src}` and validate behavior primarily through `community/source/libs/sync/test`. Use existing cluster-style test scaffolding under `community/contrib/test/traft` or `community/tests/pytest/cluster` only if unit-level coverage cannot express a required election scenario.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Internal wire contract change | `SyncRequestVote` must carry candidate applied progress so remote voters can compare candidates using data they do not currently receive | Deriving candidate applied progress locally is impossible with current message payloads |
