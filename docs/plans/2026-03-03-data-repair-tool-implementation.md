# TDengine Data Repair Tool Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend `taosd -r` into a vnode-level repair tool with resumable execution, covering `force` (WAL first), then TSDB/META, and finally replica/copy modes.
> Terminology note: `META` means time-series metadata (renamed from historical `TDB` wording in this project).

**Architecture:** Keep `taosd` as the single entrypoint. Add a repair options parser and validator in mgmt startup, then pass a repair session context to vnode-side handlers. Reuse existing WAL auto-repair internals while adding preflight, backup, progress reporting, and state checkpointing (`repair.state.json`) for resumability.

**Tech Stack:** C (existing TDengine runtime), CMake, GoogleTest (`source/common/test`, `source/libs/wal/test`, `source/dnode/vnode/test`), existing vnode/wal/meta modules.

---

## Execution Notes
- Single task target duration: `30-60 minutes`.
- Each task ends with: targeted build + targeted test.
- Commit frequency: one commit per completed task.
- Use these base commands from repo root:
```bash
cmake -S . -B debug -DBUILD_TEST=ON
cmake --build debug -j8 --target <target_name>
ctest --test-dir debug -R <test_name> --output-on-failure
```

## Task 1: Repair Option Model

**Files:**
- Create: `include/common/trepair.h`
- Create: `source/common/src/trepair.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add parser-model unit tests in `commonTests.cpp` for:
  - valid enum mapping (`vnode/wal/force`);
  - invalid values return error.

**Step 2: Run test to verify it fails**
- Run:
```bash
cmake --build debug -j8 --target commonTest
ctest --test-dir debug -R commonTest --output-on-failure
```
- Expected: fail due to missing repair model APIs.

**Step 3: Write minimal implementation**
- Add enum parsers in `trepair.h/.c` (common layer first, mgmt wiring in later tasks).

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add include/common/trepair.h source/common/src/trepair.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add repair option enum parsers in common layer"
```

## Task 2: CLI Parse Extension in `dmMain.c`

**Files:**
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Modify: `include/common/trepair.h`
- Modify: `source/common/src/trepair.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add tests for parsing:
  - `taosd -r --node-type vnode --file-type wal --vnode-id 2,3 --mode force`
  - missing required arg combinations should fail.

**Step 2: Run test to verify it fails**
- Build and run `commonTest`.

**Step 3: Write minimal implementation**
- Extend `dmParseArgs()` to parse long options.
- Reuse `tRepairParseCliOption()` to parse option values.
- Keep backward compatibility: plain `-r` still legal.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/exe/dmMain.c source/common/test/commonTests.cpp
git commit -m "feat(repair): parse new taosd -r repair options"
```

## Task 3: Option Validation Rules

**Files:**
- Modify: `source/common/src/trepair.c`
- Modify: `include/common/trepair.h`
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add rule tests:
  - `--mode=copy` requires `--replica-node`.
  - `--node-type=vnode` requires `--vnode-id`.
  - unsupported node/file combinations rejected.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Implement `tRepairValidateCliArgs(...)` with deterministic error codes/messages.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/common/src/trepair.c include/common/trepair.h source/dnode/mgmt/exe/dmMain.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add repair option validation rules"
```

## Task 4: Repair Session + State File Skeleton

**Files:**
- Create: `source/dnode/mgmt/exe/dmRepairSession.h`
- Create: `source/dnode/mgmt/exe/dmRepairSession.c`
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add state serialization tests for `repair.state.json`:
  - write/read roundtrip;
  - missing file should create new state.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Add `SRepairSession` and JSON persistence helpers.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/exe/dmRepairSession.h source/dnode/mgmt/exe/dmRepairSession.c source/dnode/mgmt/exe/dmMain.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add repair session state persistence skeleton"
```

## Task 5: Target Vnode Selection and Preflight

**Files:**
- Modify: `source/dnode/mgmt/mgmt_vnode/src/vmFile.c`
- Modify: `source/dnode/mgmt/mgmt_vnode/src/vmInt.c`
- Create: `source/dnode/mgmt/exe/dmRepairPreflight.c`
- Test: `source/dnode/vnode/test/tqTest.cpp` (or new vnode-specific test file)

**Step 1: Write failing test**
- Add vnode filter test by `vnode-id` list.

**Step 2: Run test to verify it fails**
- Build target:
```bash
cmake --build debug -j8 --target tqTest
ctest --test-dir debug -R tq_test --output-on-failure
```

**Step 3: Write minimal implementation**
- Select only requested vnode IDs.
- Add preflight checks for path existence and free space.

**Step 4: Run test to verify it passes**
- Re-run `tq_test`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/mgmt_vnode/src/vmFile.c source/dnode/mgmt/mgmt_vnode/src/vmInt.c source/dnode/mgmt/exe/dmRepairPreflight.c source/dnode/vnode/test/tqTest.cpp
git commit -m "feat(repair): add target vnode filtering and preflight checks"
```

## Task 6: Backup Manager + Progress Reporter

**Files:**
- Create: `source/dnode/mgmt/exe/dmRepairBackup.c`
- Create: `source/dnode/mgmt/exe/dmRepairReport.c`
- Modify: `source/dnode/mgmt/exe/dmRepairSession.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Test backup directory naming and `repair.log` append behavior.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Backup path builder (`--backup-path` + timestamp).
- Periodic progress line and final summary.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/exe/dmRepairBackup.c source/dnode/mgmt/exe/dmRepairReport.c source/dnode/mgmt/exe/dmRepairSession.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add backup manager and progress reporting"
```

## Task 7: WAL Force-Mode Handler (MVP Core)

**Files:**
- Create: `source/dnode/vnode/src/vnd/vnodeRepairWal.c`
- Modify: `source/dnode/vnode/CMakeLists.txt`
- Modify: `source/dnode/vnode/src/vnd/vnodeOpen.c` (repair dispatch hook)
- Test: `source/libs/wal/test/walMetaTest.cpp`

**Step 1: Write failing test**
- Add WAL corruption scenario expecting repair success and idx rebuild.

**Step 2: Run test to verify it fails**
- Run:
```bash
cmake --build debug -j8 --target walTest
ctest --test-dir debug -R wal_test --output-on-failure
```

**Step 3: Write minimal implementation**
- Hook force-mode WAL handler to call existing `walCheckAndRepair*` in controlled session flow.

**Step 4: Run test to verify it passes**
- Re-run `wal_test`.

**Step 5: Commit**
```bash
git add source/dnode/vnode/src/vnd/vnodeRepairWal.c source/dnode/vnode/CMakeLists.txt source/dnode/vnode/src/vnd/vnodeOpen.c source/libs/wal/test/walMetaTest.cpp
git commit -m "feat(repair): implement force mode WAL repair handler"
```

## Task 8: WAL Recovery Summary + Resume Checkpoint

**Files:**
- Modify: `source/dnode/vnode/src/vnd/vnodeRepairWal.c`
- Modify: `source/dnode/mgmt/exe/dmRepairSession.c`
- Test: `source/libs/wal/test/walMetaTest.cpp`

**Step 1: Write failing test**
- Add test for resumable WAL repair: already-completed file should be skipped.

**Step 2: Run test to verify it fails**
- Run `wal_test`.

**Step 3: Write minimal implementation**
- Persist per-vnode per-step completion status in `repair.state.json`.

**Step 4: Run test to verify it passes**
- Re-run `wal_test`.

**Step 5: Commit**
```bash
git add source/dnode/vnode/src/vnd/vnodeRepairWal.c source/dnode/mgmt/exe/dmRepairSession.c source/libs/wal/test/walMetaTest.cpp
git commit -m "feat(repair): support resumable WAL repair checkpoints"
```

## Task 9: TSDB Force-Mode Scanner (Phase-1)

**Files:**
- Create: `source/dnode/vnode/src/tsdb/tsdbRepairScan.c`
- Modify: `source/dnode/vnode/CMakeLists.txt`
- Test: `source/dnode/vnode/test/tqTest.cpp` (or dedicated new test)

**Step 1: Write failing test**
- Add synthetic TSDB corruption detection case.

**Step 2: Run test to verify it fails**
- Run `tq_test`.

**Step 3: Write minimal implementation**
- Build scanner that reports valid/corrupt blocks without full rewrite.

**Step 4: Run test to verify it passes**
- Re-run `tq_test`.

**Step 5: Commit**
```bash
git add source/dnode/vnode/src/tsdb/tsdbRepairScan.c source/dnode/vnode/CMakeLists.txt source/dnode/vnode/test/tqTest.cpp
git commit -m "feat(repair): add TSDB corruption scanner for force mode"
```

## Task 10: META Force-Mode Metadata Recovery Skeleton

**Files:**
- Create: `source/dnode/vnode/src/meta/metaRepair.c`
- Modify: `source/dnode/vnode/src/meta/metaOpen.c`
- Test: `source/common/test/commonTests.cpp` (metadata reconstruction utility tests)

**Step 1: Write failing test**
- Add minimal metadata inference test from partial inputs.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Add first-pass metadata recovery skeleton and missing-item markers.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/vnode/src/meta/metaRepair.c source/dnode/vnode/src/meta/metaOpen.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add META recovery skeleton with missing metadata markers"
```

## Task 11: Replica Mode Dispatch Stub

**Files:**
- Create: `source/dnode/mgmt/exe/dmRepairReplica.c`
- Modify: `source/dnode/mgmt/exe/dmRepair.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add mode dispatch tests asserting `replica` path selection and preconditions.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Implement dispatch stub + clear `not supported yet` boundary in community path.

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/exe/dmRepairReplica.c source/dnode/mgmt/exe/dmRepair.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add replica mode dispatch stub with preconditions"
```

## Task 12: Copy Mode Dispatch Stub

**Files:**
- Create: `source/dnode/mgmt/exe/dmRepairCopy.c`
- Modify: `source/dnode/mgmt/exe/dmRepair.c`
- Test: `source/common/test/commonTests.cpp`

**Step 1: Write failing test**
- Add mode dispatch tests asserting `copy` requires `--replica-node`.

**Step 2: Run test to verify it fails**
- Run `commonTest`.

**Step 3: Write minimal implementation**
- Add copy mode skeleton (interface + structured error + TODO markers).

**Step 4: Run test to verify it passes**
- Re-run `commonTest`.

**Step 5: Commit**
```bash
git add source/dnode/mgmt/exe/dmRepairCopy.c source/dnode/mgmt/exe/dmRepair.c source/common/test/commonTests.cpp
git commit -m "feat(repair): add copy mode dispatch stub and validation"
```

## Task 13: End-to-End Smoke for `force+wal`

**Files:**
- Create: `tests/system-test/3-enterprise/restore/repair_force_wal.py` (or equivalent existing suite path)
- Modify: `tests/parallel_test/cases.task`

**Step 1: Write failing test**
- Add e2e case with controlled WAL damage and expected successful repair.

**Step 2: Run test to verify it fails**
- Execute selected system test command.

**Step 3: Write minimal implementation**
- Fix integration gaps between CLI parser, session, and WAL handler.

**Step 4: Run test to verify it passes**
- Re-run target case to pass.

**Step 5: Commit**
```bash
git add tests/system-test/3-enterprise/restore/repair_force_wal.py tests/parallel_test/cases.task
git commit -m "test(repair): add e2e smoke for force mode WAL repair"
```

## Task 14: Documentation and Operational Guide

**Files:**
- Modify: `docs/zh/08-operation/05-maintenance.md`
- Modify: `docs/en/08-operation/04-maintenance.md`
- Create: `docs/zh/examples/repair-tool.md`
- Create: `docs/en/examples/repair-tool.md`

**Step 1: Write doc test checklist**
- Define command examples and expected output fragments.

**Step 2: Run doc lint/check if available**
- Run repository doc checks (if configured).

**Step 3: Write minimal implementation**
- Add new command usage, parameter explanation, and caution notes.

**Step 4: Verify examples manually**
- Ensure all examples match implemented CLI behavior.

**Step 5: Commit**
```bash
git add docs/zh/08-operation/05-maintenance.md docs/en/08-operation/04-maintenance.md docs/zh/examples/repair-tool.md docs/en/examples/repair-tool.md
git commit -m "docs(repair): document taosd -r data repair workflows"
```

## Validation Gate (Before Claiming Completion)
- Build and run at least:
```bash
cmake --build debug -j8 --target commonTest walTest tqTest
ctest --test-dir debug -R "commonTest|wal_test|tq_test" --output-on-failure
```
- Run one targeted system test for `force+wal`.
- Confirm `--help` output matches docs examples.
