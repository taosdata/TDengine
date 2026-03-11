# TSDB Force Repair Test Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace weak TSDB force-repair assertions with a layered test suite that proves real repair outcomes on real filesets while preserving a few metadata-oriented checks where synthetic setup is still the best fit.

**Architecture:** Reorganize [`test_tsdb_force_repair.py`](/Projects/work/TDengine/test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py) around reusable helpers for fixture creation, fileset discovery, corruption injection, repair execution, restart, and post-repair validation. Keep a small metadata layer for backup and crash-safe semantics, and shift the main coverage to real core and `stt` end-to-end scenarios that restart `taosd` and verify SQL read/write behavior after repair.

**Tech Stack:** Python, pytest, `new_test_framework`, `taosd` force-repair CLI, on-disk TSDB files, JSON manifest inspection

---

### Task 1: Inventory current coverage and map each case to keep, replace, or remove

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`
- Reference: `source/dnode/vnode/src/tsdb/tsdbRepair.c`
- Reference: `docs/zh/14-reference/01-components/01-taosd.md`

**Step 1: Write the failing test**

Add a small internal mapping comment block or helper constant near the test file top that reflects the new groups:

- metadata
- core e2e
- stt e2e

This is the red gate for the refactor because it makes the intended structure explicit before case movement starts.

**Step 2: Run test to verify it fails**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k nonexistent_group -q`

Expected: no tests selected, confirming no grouped structure exists yet and the suite still needs refactor.

**Step 3: Write minimal implementation**

Add the grouping comment block and mark the legacy synthetic-only cases that will be removed or rewritten.

**Step 4: Run test to verify it passes**

Run: `python3 -m py_compile test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

Expected: no syntax error.

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: outline tsdb force repair suite groups"
```

### Task 2: Build reusable real-fixture and fileset discovery helpers

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Add a focused helper-backed smoke case skeleton that expects a structured fixture dictionary with:

- `dbname`
- `vnode_id`
- `fid`
- `row_count`
- `fileset`

For example:

```python
def test_force_repair_fixture_builder_exposes_real_fileset(self):
    fixture = self._prepare_core_fixture()
    assert fixture["vnode_id"] > 0
    assert fixture["fid"] > 0
    assert os.path.isfile(fixture["fileset"]["head"])
    assert os.path.isfile(fixture["fileset"]["data"])
```

**Step 2: Run test to verify it fails**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py::TestTsdbForceRepair::test_force_repair_fixture_builder_exposes_real_fileset -q`

Expected: FAIL because the helper does not exist or does not return the required structure.

**Step 3: Write minimal implementation**

Introduce helpers to:

- prepare a real core fixture
- prepare a real `stt` fixture
- resolve a materialized fileset with manifest-consistent size
- return structured file information instead of raw paths

**Step 4: Run test to verify it passes**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py::TestTsdbForceRepair::test_force_repair_fixture_builder_exposes_real_fileset -q`

Expected: PASS

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: add tsdb force repair fixture helpers"
```

### Task 3: Add explicit corruption injectors and repair lifecycle helpers

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Add one focused helper test per new injector style, for example:

```python
def test_force_repair_corrupt_size_mismatch_changes_file_size(self):
    fixture = self._prepare_core_fixture()
    target = fixture["fileset"]["head"]
    before = os.path.getsize(target)
    info = self._corrupt_size_mismatch(target, mode="truncate")
    after = os.path.getsize(target)
    assert info["corruption_type"] == "size_mismatch"
    assert after != before
```

**Step 2: Run test to verify it fails**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py::TestTsdbForceRepair::test_force_repair_corrupt_size_mismatch_changes_file_size -q`

Expected: FAIL because the injector helper is missing.

**Step 3: Write minimal implementation**

Add helpers for:

- missing-file corruption
- size-mismatch corruption
- in-place byte overwrite
- running force repair with optional backup root
- restarting `taosd` and waiting until SQL becomes available again

Each corruption helper should return structured metadata for failure diagnostics.

**Step 4: Run test to verify it passes**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "fixture_builder or corrupt_size_mismatch" -q`

Expected: PASS

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: add tsdb repair corruption and lifecycle helpers"
```

### Task 4: Rebuild the metadata and transaction-safety layer

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Keep or add focused metadata cases for:

- healthy dispatch/no-op
- backup manifest and `repair.log`
- staged `current.c.json` recovery on restart

At least one case should assert a richer `repair.log` contract:

```python
assert "fid=" in log_text
assert "action=" in log_text
assert "reason=" in log_text
```

**Step 2: Run test to verify it fails**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "backup or crashsafe or noop" -q`

Expected: FAIL because the legacy cases do not yet enforce the new contract or shared helpers.

**Step 3: Write minimal implementation**

Refactor the retained metadata tests onto the new helpers and remove the old weak log-only assertions.

**Step 4: Run test to verify it passes**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "backup or crashsafe or noop" -q`

Expected: PASS

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: refactor tsdb repair metadata coverage"
```

### Task 5: Add core end-to-end strategy and corruption coverage

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Add real end-to-end cases for:

- healthy fileset no-op
- missing `.head`
- missing `.data`
- default strategy does not repair size mismatch
- `head_only_rebuild` recovers size mismatch
- `full_rebuild` recovers size mismatch
- damaged `.head` content with rebuild-or-drop semantics
- damaged `.data` content with rebuild-or-drop semantics

Use explicit post-repair assertions such as:

```python
self._assert_post_repair_queryable(dbname, min_rows=0, max_rows=baseline_rows)
self._assert_post_repair_insert_and_flush(dbname, inserted_rows=32)
```

**Step 2: Run test to verify it fails**

Run one case first, for example:

`./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py::TestTsdbForceRepair::test_drop_invalid_only_does_not_fix_size_mismatch_core -q`

Expected: FAIL because the new end-to-end contract is not yet implemented.

**Step 3: Write minimal implementation**

Replace the synthetic core-only cases with real fileset versions. Keep range-based row assertions where destructive repair is expected.

**Step 4: Run test to verify it passes**

Run:

`./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "size_mismatch or missing_head or missing_data or rebuild_core" -q`

Expected: PASS

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: add tsdb force repair core e2e coverage"
```

### Task 6: Add STT end-to-end coverage

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Add real end-to-end cases for:

- missing `.stt`
- corrupted `stt` payload data
- optional Phase 1 tomb or index damage when stable enough

At minimum, write the missing `.stt` case first:

```python
def test_missing_stt_is_removed_and_database_remains_writable(self):
    fixture = self._prepare_stt_fixture()
    ...
```

**Step 2: Run test to verify it fails**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py::TestTsdbForceRepair::test_missing_stt_is_removed_and_database_remains_writable -q`

Expected: FAIL

**Step 3: Write minimal implementation**

Implement the real `stt` end-to-end cases and remove legacy `current.json`-only `stt` assertions that no longer add value.

If tomb/index corruption is still too unstable for Phase 1, keep the helper hook and add a documented `pytest.skip(...)` with the stability reason.

**Step 4: Run test to verify it passes**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "stt" -q`

Expected: PASS for implemented Phase 1 cases

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: add tsdb force repair stt e2e coverage"
```

### Task 7: Remove obsolete synthetic-only cases and tighten assertions

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

**Step 1: Write the failing test**

Before removal, run the old synthetic selectors to confirm which names are being retired and which assertions are now redundant.

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -k "size_mismatch or action_for_missing_core or removes_missing_head_data_sma" -q`

Expected: legacy cases still exist and are ready to be replaced.

**Step 2: Run test to verify it fails**

Expected: either PASS with weak assertions or FAIL unpredictably, proving they are not the right long-term coverage layer.

**Step 3: Write minimal implementation**

Delete or rewrite the synthetic-only cases that duplicate the new real end-to-end coverage and standardize helper-backed assertions across the remaining suite.

**Step 4: Run test to verify it passes**

Run: `python3 -m py_compile test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

Expected: no syntax error.

**Step 5: Commit**

```bash
git add test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py
git commit -m "test: remove obsolete tsdb repair synthetic cases"
```

### Task 8: Final verification and Phase 2 handoff

**Files:**
- Modify: `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`
- Reference: `docs/plans/2026-03-11-tsdb-force-repair-test-redesign-design.md`

**Step 1: Run focused verification**

Run: `python3 -m py_compile test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

Expected: PASS

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py -q`

Expected: the redesigned Phase 1 suite passes, with any intentionally deferred Phase 1 tomb/index case clearly skipped and documented.

**Step 2: Run broader verification if time permits**

Run: `./new_test_framework/utils/exec.sh pytest test/cases/80-Components/01-Taosd/test_com_cmdline.py -k repair -q`

Expected: command-line repair contract coverage still passes.

**Step 3: Summarize residual risk**

Document the remaining Phase 2 items:

- fuller tomb/index corruption matrix
- additional repeated-repair durability runs
- migration of any remaining synthetic cases to real filesets
