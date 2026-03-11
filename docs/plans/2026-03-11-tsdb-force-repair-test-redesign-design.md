# TSDB Force Repair Test Redesign Design

**Date:** 2026-03-11

**Scope:** `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

## Goal

Redesign TSDB force-repair tests so the suite validates real repair outcomes instead of mostly checking log text or direct `current.json` edits. The first phase should establish layered coverage with real fileset end-to-end recovery for the main repair paths. The second phase can expand that structure into a fuller on-disk corruption matrix.

## Why The Current Tests Are No Longer Enough

The repair implementation in [`tsdbRepair.c`](/Projects/work/TDengine/source/dnode/vnode/src/tsdb/tsdbRepair.c) now exposes materially different behaviors:

- `drop_invalid_only`
- `head_only_rebuild`
- `full_rebuild`
- independent `stt` drop vs rebuild decisions
- concrete `action` and `reason` reporting in backup logs

The current Python suite still over-relies on:

- synthetic `current.json` injection
- fake filesets that do not exercise the real writer/reader paths
- weak success criteria such as "string exists in output"

Those checks are still useful for a few metadata-transaction cases, but they no longer provide enough confidence for the actual repair behavior.

## Design Principles

1. Real fileset end-to-end tests become the default.
2. Synthetic metadata tests remain only where they are the best way to validate transactional behavior.
3. Every meaningful repair case must verify service recovery, not just file deletion.
4. The helper layer must be designed for a second-phase expansion into a larger corruption matrix.
5. The suite should favor a small number of high-value cases over many weakly asserted cases.

## Two-Phase Strategy

### Phase 1: Layered Redesign

Use a mixed strategy:

- keep a few metadata-oriented tests
- convert the main core and `stt` cases to real fileset end-to-end scenarios
- introduce helpers for fixture preparation, corruption injection, repair execution, restart, and post-repair assertions

This phase is the immediate implementation target.

### Phase 2: Full End-To-End Matrix

Build on the Phase 1 helper layer to reduce or eliminate remaining synthetic cases and expand the matrix across:

- corruption type
- target file kind
- strategy
- expected repair action
- post-repair data availability

Phase 2 is explicitly planned but not required to block Phase 1.

## Test Suite Structure

The test file should be reorganized around repair semantics instead of incremental historical additions.

Recommended grouping:

- helper methods shared by the suite
- metadata and transaction behavior
- core end-to-end repair
- `stt` end-to-end repair
- later: full corruption matrix expansion

The implementation may keep one class for framework compatibility, but helper names and test ordering should clearly reflect these groups.

## Phase 1 Coverage Matrix

### Baseline and Strategy Routing

- healthy fileset repair is a no-op
- default `drop_invalid_only` does not repair size-mismatch core corruption
- `head_only_rebuild` and `full_rebuild` both recover size-mismatch core corruption

### Core Repair

- missing `.head` drops the core group and leaves the database restartable
- missing `.data` drops the core group and leaves the database restartable
- damaged `.head` content triggers rebuild or salvageable drop behavior according to surviving valid blocks
- damaged `.data` content triggers rebuild or drop according to surviving valid blocks

### STT Repair

- missing `.stt` is removed and the database remains queryable
- corrupted `stt` data content causes only the affected `stt` file to be rebuilt or dropped
- tomb or index corruption should be covered when a stable data fixture is available; if not stable enough for Phase 1, the helper API must still reserve the hook for Phase 2

### Audit and Transaction Safety

- real repair writes backup manifest and `repair.log`
- staged `current.c.json` recovery on restart remains covered

## Uniform Success Criteria

Except for explicit metadata-only tests, each repair case should verify four layers:

1. File-level state
   - target files changed as expected
   - `current.json` reflects the repaired state
   - backup manifest and `repair.log` exist when backup is enabled
2. Process-level recovery
   - repair command completes without crashing
   - normal `taosd` startup succeeds afterward
3. SQL read validation
   - `count(*)` succeeds
   - at least one bounded read query succeeds
4. SQL write validation
   - new inserts succeed after repair
   - `flush` succeeds
   - newly inserted rows are queryable

For destructive repair cases the row count is allowed to decrease, but it must still satisfy a deliberate range assertion such as `0 <= repaired_rows <= original_rows`.

## Helper Architecture

### Real Fixture Builders

Provide dedicated builders for:

- core-repair fixtures
- `stt`-repair fixtures
- later: tomb-heavy fixtures

Each builder should return structured context including:

- `dbname`
- `vnode_id`
- `fid`
- baseline row count
- resolved file paths
- backup root when applicable

### Fileset Discovery

Upgrade the current ad hoc path lookups into structured discovery helpers that can resolve a real fileset and report:

- head/data/sma paths
- `stt` file list
- current manifest entry
- file sizes
- whether tomb data appears to exist

### Corruption Injection

Model corruption types explicitly:

- missing file
- size mismatch by truncate or extend
- in-place byte overwrite
- later: `stt` tomb or index region corruption

Each injector should return a structured record describing what changed so failures are diagnosable.

### Repair Lifecycle

Centralize:

- force-repair command execution
- controlled `taosd` stop/start
- readiness checks
- post-repair SQL assertions

This avoids test-local drift in recovery semantics.

## Existing Test Triage

### Keep And Refactor

- dispatch smoke coverage
- backup manifest coverage
- backup log coverage
- staged manifest crash-safe recovery
- one or more core rebuild smoke cases

### Replace With Real End-To-End Cases

- synthetic missing core fileset tests
- synthetic size-mismatch core tests
- synthetic repair-log action/reason tests tied to fake file groups
- synthetic `current.json` only checks for core strategy behavior

## Risks And Constraints

- Real `stt` and tomb fixtures can be timing-sensitive because file materialization is asynchronous.
- Corrupting the wrong real fileset can produce unstable results, so helper selection must only target fully materialized files with manifest-consistent size before injection.
- End-to-end coverage will make the suite slower; that is an accepted tradeoff for this redesign.

## Verification Strategy

Phase 1 completion should verify:

- the updated Python test file is runnable in isolation
- the selected end-to-end cases pass against a real `taosd`
- metadata-only cases still validate backup and crash-safe behavior
- the helper layer is generic enough that Phase 2 can add more corruption modes without another large refactor
