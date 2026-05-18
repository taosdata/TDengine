# Implementation Plan: Prevent Restoring Stepdown Re-Election

**Branch**: `20260410-170000-assigned-stepdown-guard` | **Date**: 2026-04-10 | **Spec**: /root/github/taosdata/TDinternal/specs/20260410-170000-assigned-stepdown-guard/spec.md
**Input**: Feature specification from `/specs/20260410-170000-assigned-stepdown-guard/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Prevent assigned leader from stepping down in dual-replica recovery while peer replica is still restoring. The change is constrained to stepdown decision gating inside the append-entries-reply path, adding peer readiness (non-restoring) as a required condition together with existing commit-index threshold checks.

## Technical Context

**Language/Version**: C (TDengine core sync module)  
**Primary Dependencies**: community sync library internals (`syncAppendEntriesReply`, `syncPipeline`, sync state machine types)  
**Storage**: N/A (in-memory runtime decision on replicated log progress and peer readiness)  
**Testing**: TDinternal existing sync/vgroup integration paths; system test harness under `community/tests/system-test`  
**Target Platform**: Linux server deployment for TDengine dnode/vnode cluster runtime
**Project Type**: Database engine internal module (replication and leader-election behavior)  
**Performance Goals**: No measurable regression in steady-state replication path; no added network round-trip in decision logic  
**Constraints**: Preserve existing behavior outside restoring edge case; avoid triggering election loops that can drop service availability  
**Scale/Scope**: Dual-replica vgroup recovery path; one targeted decision gate in assigned-leader stepdown condition

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

[Gates determined based on constitution file]

Pre-Phase-0 gate result: PASS (constitution file is still template-only with no enforceable project-specific MUST rules).

Applied gate interpretation:
- No explicit mandatory principle is defined in `.specify/memory/constitution.md`.
- Planning still enforces local quality gates: bounded scope, no unclear requirements, explicit regression coverage requirement.

## Project Structure

### Documentation (this feature)

```text
specs/20260410-170000-assigned-stepdown-guard/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
community/
├── source/
│   └── libs/
│       └── sync/
│           ├── inc/
│           │   ├── syncInt.h
│           │   └── syncPipeline.h
│           └── src/
│               ├── syncAppendEntriesReply.c
│               ├── syncPipeline.c
│               └── syncMain.c
└── tests/
  └── system-test/
    └── (dual-replica recovery scenarios)
```

**Structure Decision**: Use existing TDinternal monorepo engine structure; implement in `community/source/libs/sync/src` with related state definitions in `community/source/libs/sync/inc`, and verify via system-test scenarios under `community/tests/system-test`.

## Post-Design Constitution Check

Post-Phase-1 gate result: PASS.

Validation outcome:
- Design remains internal and narrowly scoped to assigned-leader stepdown gating.
- No new architecture layer, service boundary, or external interface contract introduced.
- Research/design artifacts resolve all clarifications from technical context.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | N/A |
