# META Force Repair Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add real execution for `taosd -r --mode force --file-type meta` by enhancing the existing `metaGenerateNewMeta()` path so each vnode decides during `metaOpen()` whether it should run force repair, with external backup support and crash-safe local directory switching.

**Architecture:** Keep CLI parsing in `source/dnode/mgmt/exe/dmMain.c`, but do not launch a centralized repair executor. Instead, expose the minimal meta-force-repair intent so the existing parallel vnode open path can decide per vnode inside `source/dnode/vnode/src/meta/metaOpen.c`. Reuse the current local `meta` / `meta_tmp` / `meta_bak` switching protocol and add external backup export before the local switch.

**Tech Stack:** C, existing TDengine vnode/meta open pipeline, existing pytest E2E framework under `test/cases/80-Components/01-Taosd/`, CMake build via `./build.sh` or `cmake --build debug`.

---

### Task 1: Freeze file targets and test file location

**Files:**
- Modify: `docs/data_repair/02-META_repair/task_plan.md`
- Modify: `docs/data_repair/02-META_repair/findings.md`
- Create: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Step 1: Write the planning note update**

```markdown
- Confirm implementation files are `source/dnode/mgmt/exe/dmMain.c` and `source/dnode/vnode/src/meta/metaOpen.c`.
- Confirm new test file is `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`.
```

**Step 2: Save the new test filename in planning docs**

Run: `rg -n "test_meta_force_repair.py|metaOpen.c|dmMain.c" docs/data_repair/02-META_repair`
Expected: shows the new test path and implementation file paths in the planning docs.

**Step 3: Commit planning-only update**

```bash
git add docs/data_repair/02-META_repair/task_plan.md docs/data_repair/02-META_repair/findings.md docs/plans/2026-03-06-meta-force-repair-design.md docs/plans/2026-03-06-meta-force-repair-plan.md
git commit -m "docs: add meta force repair design and plan"
```

### Task 2: Write the failing repair-intent tests

**Files:**
- Create: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- Reference: `test/cases/80-Components/01-Taosd/test_com_cmdline.py`

**Step 1: Write the failing test skeleton for target vnode matching**

```python
class TestMetaForceRepair:
    def test_meta_force_repair_targeted_vnodes(self):
        repair_opt = self._parse_repair_opt(
            "-r --node-type vnode --file-type meta --vnode-id 1,3 --mode force"
        )
        assert self._should_repair_vnode(repair_opt, 1) is True
        assert self._should_repair_vnode(repair_opt, 2) is False
        assert self._should_repair_vnode(repair_opt, 3) is True
```

**Step 2: Write the failing test skeleton for all-vnode fallback**

```python
def test_meta_force_repair_all_vnodes_when_no_vnode_id(self):
    repair_opt = self._parse_repair_opt(
        "-r --node-type vnode --file-type meta --mode force"
    )
    assert self._should_repair_all_vnodes(repair_opt) is True
```

**Step 3: Write the failing test skeleton for backup path normalization**

```python
def test_meta_force_repair_default_backup_path(self):
    backup_dir = self._normalize_backup_root(None, "20260306")
    assert backup_dir == "/tmp/taos_backup_20260306"
```

**Step 4: Run the new test file to verify failure**

Run: `cd test && pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q`
Expected: FAIL because helper behavior and/or hooks are not implemented yet.

**Step 5: Commit the red test**

```bash
git add test/cases/80-Components/01-Taosd/test_meta_force_repair.py
git commit -m "test(repair): add failing meta force repair coverage"
```

### Task 3: Expose minimal meta-force-repair intent from CLI parsing

**Files:**
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Test: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Step 1: Add explicit helper predicates for meta force repair**

```c
static bool dmRepairIsMetaForce(void) {
  SDmRepairOption *pOpt = &global.repairOpt;
  return pOpt->withR && global.runRepairFlow && strcmp(pOpt->nodeType, "vnode") == 0 &&
         strcmp(pOpt->fileType, "meta") == 0 && strcmp(pOpt->mode, "force") == 0;
}

static bool dmRepairTargetsAllVnodes(void) {
  SDmRepairOption *pOpt = &global.repairOpt;
  return dmRepairIsMetaForce() && !pOpt->hasVnodeId;
}
```

**Step 2: Add vnode-match helper with comma-list parsing**

```c
static bool dmRepairMatchesVnode(int32_t vgId) {
  char *saveptr = NULL;
  char  vnodeList[PATH_MAX] = {0};
  tstrncpy(vnodeList, global.repairOpt.vnodeId, sizeof(vnodeList));
  for (char *token = strtok_r(vnodeList, ",", &saveptr); token != NULL; token = strtok_r(NULL, ",", &saveptr)) {
    if (atoi(token) == vgId) return true;
  }
  return dmRepairTargetsAllVnodes();
}
```

**Step 3: Add backup root normalization helper**

```c
static void dmRepairBuildBackupRoot(char *buf, int32_t bufLen, const char *dateText) {
  const char *root = global.repairOpt.hasBackupPath ? global.repairOpt.backupPath : "/tmp";
  snprintf(buf, bufLen, "%s%s%s", root, TD_DIRSEP, dateText);
}
```

**Step 4: Export only the minimal accessors needed by meta layer**

```c
bool dmMetaForceRepairEnabled(void);
bool dmMetaForceRepairMatchesVnode(int32_t vgId);
void dmMetaForceRepairBackupRoot(char *buf, int32_t bufLen);
```

**Step 5: Run the targeted test file again**

Run: `cd test && pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q`
Expected: FAIL shifts from missing intent parsing to missing `metaOpen()` repair behavior.

**Step 6: Commit the CLI intent helpers**

```bash
git add source/dnode/mgmt/exe/dmMain.c test/cases/80-Components/01-Taosd/test_meta_force_repair.py
git commit -m "feat(repair): expose meta force repair intent"
```

### Task 4: Gate repair inside `metaOpen()`

**Files:**
- Modify: `source/dnode/vnode/src/meta/metaOpen.c`
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Test: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Step 1: Add a `metaShouldForceRepair()` helper**

```c
static bool metaShouldForceRepair(SVnode *pVnode) {
  return dmMetaForceRepairEnabled() && dmMetaForceRepairMatchesVnode(TD_VID(pVnode));
}
```

**Step 2: Keep legacy rebuild and new repair path mutually exclusive**

```c
if (generateNewMeta) {
  code = metaGenerateNewMeta(ppMeta);
} else if (metaShouldForceRepair(pVnode)) {
  code = metaGenerateNewMeta(ppMeta);
}
```

**Step 3: Add logs that identify target vs non-target vnode behavior**

```c
metaInfo("vgId:%d meta force repair target=%d", TD_VID(pVnode), metaShouldForceRepair(pVnode));
```

**Step 4: Extend the new test file to assert only selected vnode paths trigger repair**

```python
def test_meta_force_repair_only_selected_vnodes(self):
    assert self._repair_selection("1,3", 1) is True
    assert self._repair_selection("1,3", 2) is False
```

**Step 5: Run the new test file**

Run: `cd test && pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q`
Expected: PASS for vnode selection tests, FAIL for backup/copy behavior until next task.

**Step 6: Commit the `metaOpen()` gate**

```bash
git add source/dnode/vnode/src/meta/metaOpen.c test/cases/80-Components/01-Taosd/test_meta_force_repair.py
git commit -m "feat(repair): gate meta force repair in metaOpen"
```

### Task 5: Add external backup directory generation

**Files:**
- Modify: `source/dnode/vnode/src/meta/metaOpen.c`
- Modify: `source/dnode/mgmt/exe/dmMain.c`
- Test: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`

**Step 1: Write the failing backup behavior test**

```python
def test_meta_force_repair_uses_custom_backup_root(self):
    backup_dir = self._build_backup_dir("/var/tmp/repair", "20260306", 12)
    assert backup_dir == "/var/tmp/repair/taos_backup_20260306/vnode12/meta"
```

**Step 2: Add date-normalized external backup root builder**

```c
static int32_t metaBuildRepairBackupDir(SVnode *pVnode, char *buf, int32_t bufLen) {
  char dateBuf[16] = {0};
  taosFormatDateToday(dateBuf, sizeof(dateBuf));
  char rootBuf[PATH_MAX] = {0};
  dmMetaForceRepairBackupRoot(rootBuf, sizeof(rootBuf));
  snprintf(buf, bufLen, "%s%staos_backup_%s%svnode%d%smeta", rootBuf, TD_DIRSEP, dateBuf, TD_DIRSEP,
           TD_VID(pVnode), TD_DIRSEP);
  return TSDB_CODE_SUCCESS;
}
```

**Step 3: Add directory creation and copy/rename backup step before local switch**

```c
static int32_t metaBackupCurrentMeta(SVnode *pVnode) {
  char metaDir[TSDB_FILENAME_LEN] = {0};
  char backupDir[TSDB_FILENAME_LEN] = {0};
  vnodeGetMetaPath(pVnode, VNODE_META_DIR, metaDir);
  code = metaBuildRepairBackupDir(pVnode, backupDir, sizeof(backupDir));
  if (code != 0) return code;
  TAOS_CHECK_RETURN(taosMulMkDir(backupDir));
  return taosCopyDir(metaDir, backupDir);
}
```

**Step 4: Call external backup step in enhanced `metaGenerateNewMeta()` before renaming local `meta`**

```c
if (metaShouldForceRepair(pVnode)) {
  code = metaBackupCurrentMeta(pVnode);
  if (code) return code;
}
```

**Step 5: Run the new test file again**

Run: `cd test && pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q`
Expected: PASS for backup-root selection tests; remaining failures limited to crash-state assertions.

**Step 6: Commit external backup support**

```bash
git add source/dnode/vnode/src/meta/metaOpen.c source/dnode/mgmt/exe/dmMain.c test/cases/80-Components/01-Taosd/test_meta_force_repair.py
git commit -m "feat(repair): back up meta before force repair switch"
```

### Task 6: Verify crash-safe local state handling

**Files:**
- Modify: `source/dnode/vnode/src/meta/metaOpen.c`
- Test: `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- Reference: `docs/plans/2026-03-06-meta-force-repair-design.md`

**Step 1: Write the failing crash-state test**

```python
def test_meta_force_repair_does_not_break_existing_state_machine(self):
    state = self._simulate_meta_dirs(meta=True, meta_tmp=True, meta_bak=False)
    assert self._recovery_action(state) == "drop_tmp"
```

**Step 2: Refactor repair-specific backup code to avoid changing existing three-directory branch semantics**

```c
/* External backup happens before local rename operations.
   Existing metaOpen() state handling branches remain unchanged. */
```

**Step 3: Add explicit regression assertions around the three valid cleanup branches**

```python
assert self._recovery_action((True, True, False)) == "drop_tmp"
assert self._recovery_action((False, True, True)) == "promote_tmp_drop_backup"
assert self._recovery_action((True, False, True)) == "drop_backup"
```

**Step 4: Run the new test file**

Run: `cd test && pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q`
Expected: PASS for crash-state regression coverage.

**Step 5: Commit crash-safety regression protection**

```bash
git add source/dnode/vnode/src/meta/metaOpen.c test/cases/80-Components/01-Taosd/test_meta_force_repair.py
git commit -m "test(repair): protect meta crash recovery branches"
```

### Task 7: Build and run focused verification

**Files:**
- Modify: `docs/data_repair/02-META_repair/progress.md`
- Modify: `docs/data_repair/02-META_repair/findings.md`

**Step 1: Build the taosd target**

Run: `cmake --build debug --target taosd -j8`
Expected: build succeeds.

**Step 2: Run the new focused pytest file**

Run: `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`
Expected: PASS.

**Step 3: Run the phase1 regression file to ensure no parameter-layer regression**

Run: `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1`
Expected: PASS or known environment issue reproduced and documented.

**Step 4: Record exact commands and results in the progress files**

```markdown
- Build command and result
- New pytest command and result
- Phase1 regression command and result
- Any environment limitations or hangs
```

**Step 5: Commit verification notes**

```bash
git add docs/data_repair/02-META_repair/progress.md docs/data_repair/02-META_repair/findings.md
git commit -m "docs: record meta force repair verification"
```


### Environment Memory
- Preferred verification entry: `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`
- Do not assume bare `pytest` reproduces the same environment.
- If tests hang in Codex/sandbox, inspect environment/runtime issues before blaming product code.
- Watch for build-vs-installed binary/library mixing (`debug/build/bin/*` vs `/usr/local/taos/*`).
- For localhost test runtime issues, inspect `test/new_test_framework/taostest/util/remote.py`, `test/new_test_framework/taostest/components/taosd.py`, `sim/dnode1/log/taosdlog.0`, and `sim/asan/dnode1.asan` first.
