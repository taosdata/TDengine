# Stream Trigger/Calc Scan-Cols Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make stream trigger AST and calc AST each scan only their logically required columns, while preserving `%%trows` semantics by injecting `pre_filter` as the calc query's WHERE clause.

**Architecture:** Three commits in order — (A) parser scaffolding (new `SSelectStmt` flag + `translateWhere` bypass + `injectPreFilterIntoCalcQuery` helper invoked before `translateStreamCalcQuery`), behavior-neutral on its own; (B) remove the wrong "append calc cols to trigger projection" block at `parTranslater.c:19205-19216`, which together with (A) gives the corrected behavior; (C) delete the now-dead `triggerScanList` field and its filler.

**Tech Stack:** C (TDengine parser/planner). Build via the existing `community/debug/` cmake tree. Stream regression covered by python framework under `test/cases/18-StreamProcessing/`.

**Spec:** `docs/superpowers/specs/2026-04-22-stream-trigger-calc-scan-cols-optimize-design.md`

---

## File Structure

| File | Responsibility | Change Type |
|---|---|---|
| `include/nodes/querynodes.h` | `SSelectStmt` definition | Modify (add `bool pWhereInjectedFromPreFilter`) |
| `source/libs/parser/src/parTranslater.c` | Stream req building, WHERE translation | Modify (add helper, bypass check, delete bad block, delete dead init) |
| `source/libs/planner/src/planLogicCreater.c` | Scan logic node creation | Modify (delete `triggerScanList` filler) |
| `include/libs/planner/planner.h` | `SPlanStreamContext` definition | Modify (delete `triggerScanList` field) |

No new files.

---

## Pre-flight

- [ ] **Step 0.1: Verify clean tree on the target branch**

```bash
cd /root/code/TDinternal/community
git status
git log -1 --oneline
```

Expected: working tree clean, HEAD on `feat/6490635370` at the design-doc commit `d48f752bcb` (or later).

- [ ] **Step 0.2: Verify baseline build works**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target taos -j$(nproc) 2>&1 | tail -20
```

Expected: build succeeds. If it fails on baseline, stop and report — do NOT start the implementation.

---

## Task A: Parser Scaffolding (Behavior-Neutral)

This task adds the `SSelectStmt` flag, the `translateWhere` bypass, and the `injectPreFilterIntoCalcQuery` helper, but does NOT yet remove the bad `19205-19216` block. After this commit:

- For non-`%%trows` streams: behavior is identical to before (helper short-circuits).
- For `%%trows` streams with `pre_filter`: trigger still has the (wrong) extra cols appended (because Task B not done yet), but calc now also runs with injected WHERE. Trigger and calc converge in correctness; trigger is just temporarily over-scanning. This is intentional — it lets us land Task A independently.

**Files:**
- Modify: `include/nodes/querynodes.h:674-735` (add field to `SSelectStmt`)
- Modify: `source/libs/parser/src/parTranslater.c:10522-10529` (`translateWhere` check)
- Modify: `source/libs/parser/src/parTranslater.c:19104-19141` (`createStreamReqBuildCalc` — insert helper call before `translateStreamCalcQuery`)
- Modify: `source/libs/parser/src/parTranslater.c` (add helper `injectPreFilterIntoCalcQuery` + forward decl)

### Subtask A.1: Add `SSelectStmt` flag

- [ ] **Step A.1.1: Inspect current `SSelectStmt` to find a good insertion point**

```bash
sed -n '674,735p' /root/code/TDinternal/community/include/nodes/querynodes.h
```

Expected: see all `bool` fields near the bottom of the struct (e.g. `isSubquery`, `hasAggFuncs`).

- [ ] **Step A.1.2: Add the flag right after `isSubquery`**

Edit `include/nodes/querynodes.h`. Locate the line:

```c
  bool            isSubquery;
```

Insert immediately after it:

```c
  bool            pWhereInjectedFromPreFilter;  // true if pWhere was cloned from stream pre_filter
```

- [ ] **Step A.1.3: Verify it compiles**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target nodes -j$(nproc) 2>&1 | tail -10
```

Expected: build succeeds (the `nodes` lib compiles).

### Subtask A.2: Bypass the `%%trows + WHERE` check for the injected case

- [ ] **Step A.2.1: View the current check**

```bash
sed -n '10520,10535p' /root/code/TDinternal/community/source/libs/parser/src/parTranslater.c
```

Expected output contains:

```c
  if (pSelect->pWhere &&
      BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
      inStreamCalcClause(pCxt)) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "%%%%trows can not be used with WHERE clause."));
  }
```

(If line numbers shifted, find it via `grep -n "trows can not be used with WHERE" parTranslater.c`.)

- [ ] **Step A.2.2: Add the bypass condition**

Edit `source/libs/parser/src/parTranslater.c`. Replace the block above with:

```c
  if (pSelect->pWhere && !pSelect->pWhereInjectedFromPreFilter &&
      BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
      inStreamCalcClause(pCxt)) {
    PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                        "%%%%trows can not be used with WHERE clause."));
  }
```

(Only added `&& !pSelect->pWhereInjectedFromPreFilter`.)

### Subtask A.3: Implement `injectPreFilterIntoCalcQuery`

- [ ] **Step A.3.1: Find a good insertion point — right above `createStreamReqBuildCalc`**

```bash
grep -n "^static int32_t createStreamReqBuildCalc" /root/code/TDinternal/community/source/libs/parser/src/parTranslater.c
```

Expected: a single match around line 19105.

- [ ] **Step A.3.2: Add the helper above `createStreamReqBuildCalc`**

Edit `source/libs/parser/src/parTranslater.c`. Insert immediately before the line `// Build calculate part in create stream request`:

```c
// Inject trigger's pre_filter as WHERE into calc query when %%trows is used.
// Calc side independently re-scans the trigger table; without this the calc
// scan returns rows that pre_filter already excluded on the trigger side.
static int32_t injectPreFilterIntoCalcQueryImpl(STranslateContext* pCxt, SNode* pPreFilter, SNode* pQuery) {
  if (NULL == pQuery) return TSDB_CODE_SUCCESS;
  if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
    SSetOperator* pSet = (SSetOperator*)pQuery;
    int32_t code = injectPreFilterIntoCalcQueryImpl(pCxt, pPreFilter, pSet->pLeft);
    if (TSDB_CODE_SUCCESS == code) {
      code = injectPreFilterIntoCalcQueryImpl(pCxt, pPreFilter, pSet->pRight);
    }
    return code;
  }
  if (QUERY_NODE_SELECT_STMT != nodeType(pQuery)) return TSDB_CODE_SUCCESS;

  SSelectStmt* pSelect = (SSelectStmt*)pQuery;
  if (NULL == pSelect->pFromTable ||
      QUERY_NODE_PLACE_HOLDER_TABLE != nodeType(pSelect->pFromTable)) {
    return TSDB_CODE_SUCCESS;
  }
  SPlaceHolderTableNode* pPh = (SPlaceHolderTableNode*)pSelect->pFromTable;
  if (SP_PARTITION_ROWS != pPh->placeholderType) return TSDB_CODE_SUCCESS;

  if (NULL != pSelect->pWhere) {
    return generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                   "%%%%trows can not be used with WHERE clause.");
  }

  SNode* pCloned = NULL;
  int32_t code = nodesCloneNode(pPreFilter, &pCloned);
  if (TSDB_CODE_SUCCESS != code) return code;
  pSelect->pWhere = pCloned;
  pSelect->pWhereInjectedFromPreFilter = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t injectPreFilterIntoCalcQuery(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  if (NULL == pStmt->pTrigger || NULL == pStmt->pQuery) return TSDB_CODE_SUCCESS;
  SStreamTriggerNode* pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  if (NULL == pTrigger->pOptions) return TSDB_CODE_SUCCESS;
  SNode* pPreFilter = ((SStreamTriggerOptions*)pTrigger->pOptions)->pPreFilter;
  if (NULL == pPreFilter) return TSDB_CODE_SUCCESS;
  parserDebug("inject stream pre_filter into calc query as WHERE");
  return injectPreFilterIntoCalcQueryImpl(pCxt, pPreFilter, pStmt->pQuery);
}

```

- [ ] **Step A.3.3: Wire the helper into `createStreamReqBuildCalc`**

Edit `source/libs/parser/src/parTranslater.c`. Locate the line:

```c
  PAR_ERR_JRET(translateStreamCalcQuery(pCxt, pTriggerPartition, pTriggerSelect ? pTriggerSelect->pFromTable : NULL,
                                        pStmt->pQuery, pNotifyCond, pTriggerWindow));
```

Insert the following two lines IMMEDIATELY ABOVE it:

```c
  PAR_ERR_JRET(injectPreFilterIntoCalcQuery(pCxt, pStmt));

```

After this edit, the call sequence becomes: `injectPreFilterIntoCalcQuery` → `translateStreamCalcQuery` → ...

- [ ] **Step A.3.4: Build the parser library**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target parser -j$(nproc) 2>&1 | tail -20
```

Expected: build succeeds with no warnings about the new code. If `SPlaceHolderTableNode`, `SP_PARTITION_ROWS`, `SStreamTriggerNode`, or `SStreamTriggerOptions` are not visible in `parTranslater.c`, the build will fail — fix by adding the appropriate include or by checking what's already used at line ~7374 (where `translatePlaceHolderTable` lives) and at line ~18241 (where `pStmt->pTrigger` options are read).

- [ ] **Step A.3.5: Build the full taos binary**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target taos -j$(nproc) 2>&1 | tail -10
```

Expected: clean build.

### Subtask A.4: Commit Task A

- [ ] **Step A.4.1: Stage and commit**

```bash
cd /root/code/TDinternal/community
git add include/nodes/querynodes.h source/libs/parser/src/parTranslater.c
git -c user.name='Copilot' -c user.email='copilot@local' commit -m "feat(stream): inject trigger pre_filter as WHERE into %%trows calc query

Adds SSelectStmt::pWhereInjectedFromPreFilter and a parser-side helper
injectPreFilterIntoCalcQuery that clones trigger pre_filter into the
calc query's WHERE for every %%trows SELECT. translateWhere bypasses
the pre-existing '%%trows + WHERE' error when the WHERE is the
injected one. This is a behavior-neutral scaffold; the next commit
removes the wrong column-append block on the trigger side.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git log -1 --oneline
```

Expected: one new commit on top of the spec commit.

---

## Task B: Remove the Wrong Trigger-Side Column Append

**Files:**
- Modify: `source/libs/parser/src/parTranslater.c:19205-19216` (delete the append block)

### Subtask B.1: Delete the append block

- [ ] **Step B.1.1: Locate and confirm the block**

```bash
grep -n "need collect scan cols and put into trigger" /root/code/TDinternal/community/source/libs/parser/src/parTranslater.c
```

Expected: a single match. Use it to find the surrounding `if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) && LIST_LENGTH(calcCxt.streamCxt.triggerScanList) > 0) { ... }` block — this is the block to delete.

- [ ] **Step B.1.2: Delete the block**

Edit `source/libs/parser/src/parTranslater.c`. Remove EXACTLY this block:

```c
  if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
      LIST_LENGTH(calcCxt.streamCxt.triggerScanList) > 0) {
    // need collect scan cols and put into trigger's scan list
    PAR_ERR_JRET(nodesListAppendList(pTriggerSelect->pProjectionList, calcCxt.streamCxt.triggerScanList));
    SNode* pCol = NULL;
    FOREACH(pCol, pTriggerSelect->pProjectionList) {
      if (nodeType(pCol) == QUERY_NODE_COLUMN) {
        SColumnNode* pColumn = (SColumnNode*)pCol;
        tstrncpy(pColumn->tableAlias, pColumn->tableName, TSDB_TABLE_NAME_LEN);
      }
    }
  }

```

(Trailing blank line included so the surrounding code stays compact.)

- [ ] **Step B.1.3: Build**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target taos -j$(nproc) 2>&1 | tail -10
```

Expected: clean build (the only references to `triggerScanList` left are the field decl, the filler in `planLogicCreater.c`, and the `= NULL` init in `parTranslater.c` — all still compile).

### Subtask B.2: Smoke-test against an existing pre_filter case

- [ ] **Step B.2.1: Find the smallest existing `%%trows + pre_filter` test**

```bash
grep -n "stream_options(pre_filter" /root/code/TDinternal/community/test/cases/18-StreamProcessing/04-Options/test_options_basic.py | head -5
```

Expected: see the `s9` and `s9_g` streams in `Basic9` (around line 1541).

- [ ] **Step B.2.2: Run only the `Basic9` (PRE_FILTER) sub-case**

This test class runs many sub-cases; we want only `Basic9`. Easiest: temporarily comment all other `streams.append(...)` lines in `test_stream_options_basic` except `Basic9`, run, then revert.

```bash
cd /root/code/TDinternal/community/test
# from the project test root, follow whatever invocation is documented there:
ls run_test.sh 2>/dev/null && head -20 run_test.sh
```

Then:

```bash
cd /root/code/TDinternal/community/test
pytest cases/18-StreamProcessing/04-Options/test_options_basic.py::TestStreamOptionsBasic::test_stream_options_basic -v 2>&1 | tail -40
```

Expected: PASS for the `Basic9` sub-case. If the harness needs a running cluster and isn't set up here, **stop** and ask the human how to run integration tests in this environment. Do NOT fabricate a "tests passed" report.

(If unable to run integration tests, document the limitation in the commit message and proceed; the human will run regression in their own environment.)

### Subtask B.3: Commit Task B

- [ ] **Step B.3.1: Stage and commit**

```bash
cd /root/code/TDinternal/community
git add source/libs/parser/src/parTranslater.c
git -c user.name='Copilot' -c user.email='copilot@local' commit -m "fix(stream): stop appending calc cols to trigger projection on %%trows

The block at parTranslater.c:19205-19216 wrongly appended every column
referenced by the calc SELECT (collected via COLLECT_COL_TYPE_ALL into
triggerScanList) to the trigger SELECT's projection. This forced the
trigger side to scan calc-only columns (e.g. c3, t2 in 'select sum(c3),
avg(t2) from %%trows' with state_window(c1) + pre_filter(c2>2)).

With the prior commit's pre_filter injection making the calc side
self-contained, the trigger projection now stays exactly what
createStreamReqBuildTriggerSelect computed (trigger window cols +
pre_filter cols + tbname()).

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git log -1 --oneline
```

Expected: second commit on top of Task A.

---

## Task C: Delete the Dead `triggerScanList` Field

After Task B there are zero readers of `triggerScanList`. This task removes the field, the filler, and the now-useless `= NULL` initializer.

**Files:**
- Modify: `include/libs/planner/planner.h:44` (delete field)
- Modify: `source/libs/planner/src/planLogicCreater.c:598-601` (delete filler)
- Modify: `source/libs/parser/src/parTranslater.c` (delete `.streamCxt.triggerScanList = NULL,` initializer)

### Subtask C.1: Verify no readers remain

- [ ] **Step C.1.1: Grep**

```bash
cd /root/code/TDinternal/community
grep -rn "triggerScanList" --include='*.c' --include='*.h' source include
```

Expected: exactly four lines:
- `include/libs/planner/planner.h:44:  SNodeList*  triggerScanList;`
- `source/libs/planner/src/planLogicCreater.c:600:                               &pCxt->pPlanCxt->streamCxt.triggerScanList);`
- (with the surrounding `if (pRealTable->placeholderType == SP_PARTITION_ROWS) {` at 598-601)
- `source/libs/parser/src/parTranslater.c:<some-line>:                          .streamCxt.triggerScanList = NULL,`

If any other reader appears, **stop** and re-examine — there may be a code path the spec missed.

### Subtask C.2: Delete the filler in `planLogicCreater.c`

- [ ] **Step C.2.1: View context**

```bash
sed -n '595,612p' /root/code/TDinternal/community/source/libs/planner/src/planLogicCreater.c
```

- [ ] **Step C.2.2: Delete EXACTLY this block**

Edit `source/libs/planner/src/planLogicCreater.c`. Remove:

```c
  if (pRealTable->placeholderType == SP_PARTITION_ROWS) {
    code = nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, COLLECT_COL_TYPE_ALL,
                               &pCxt->pPlanCxt->streamCxt.triggerScanList);
  }
```

The neighboring `// set columns to scan` / `nodesCollectColumns(... COLLECT_COL_TYPE_COL ...)` block (currently at 602-606) MUST remain.

### Subtask C.3: Delete the initializer in `parTranslater.c`

- [ ] **Step C.3.1: Find it**

```bash
grep -n "triggerScanList = NULL" /root/code/TDinternal/community/source/libs/parser/src/parTranslater.c
```

Expected: exactly one match.

- [ ] **Step C.3.2: Delete the line**

Edit `source/libs/parser/src/parTranslater.c`. Remove the single line:

```c
                          .streamCxt.triggerScanList = NULL,
```

(The surrounding designated-initializer list will still compile — C allows omitted designated members, they default to zero.)

### Subtask C.4: Delete the field declaration

- [ ] **Step C.4.1: Edit the header**

Edit `include/libs/planner/planner.h`. Remove the single line:

```c
  SNodeList*  triggerScanList;
```

(located inside `SPlanStreamContext`, around line 44).

### Subtask C.5: Build and verify

- [ ] **Step C.5.1: Final grep — expect zero hits**

```bash
cd /root/code/TDinternal/community
grep -rn "triggerScanList" --include='*.c' --include='*.h' source include
```

Expected: NO output (zero matches).

- [ ] **Step C.5.2: Full build**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target taos -j$(nproc) 2>&1 | tail -10
```

Expected: clean build, no "unused variable" / "undefined member" errors.

### Subtask C.6: Commit Task C

- [ ] **Step C.6.1: Stage and commit**

```bash
cd /root/code/TDinternal/community
git add include/libs/planner/planner.h \
        source/libs/planner/src/planLogicCreater.c \
        source/libs/parser/src/parTranslater.c
git -c user.name='Copilot' -c user.email='copilot@local' commit -m "refactor(stream): remove dead SPlanStreamContext::triggerScanList

After the previous commit removed its only reader, the field is
unreferenced. Delete the field declaration, the COLLECT_COL_TYPE_ALL
filler in planLogicCreater.c that was specifically populating it for
SP_PARTITION_ROWS, and the now-useless = NULL initializer in
parTranslater.c.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git log -3 --oneline
```

Expected: three new commits in total (Task A, B, C) on top of the spec commit.

---

## Task D: Add Regression Test for the Optimization

Add a focused test that pins down the new behavior so future refactors can't silently regress trigger or calc scan columns.

**Files:**
- Create: `test/cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py`

### Subtask D.1: Author the test

- [ ] **Step D.1.1: Inspect a sibling test for the harness's idioms**

```bash
sed -n '1,80p' /root/code/TDinternal/community/test/cases/18-StreamProcessing/04-Options/test_options_basic.py
```

Confirm imports (`tdLog`, `tdSql`, `tdStream`, `StreamCheckItem`) and the class/method shape (a `test_*` method that calls `tdStream.createSnode()` and runs `StreamCheckItem` instances).

- [ ] **Step D.1.2: Inspect `Basic9` for an end-to-end pre_filter scenario to model after**

```bash
sed -n '1505,1700p' /root/code/TDinternal/community/test/cases/18-StreamProcessing/04-Options/test_options_basic.py
```

Look at how `create()`, `insert1()`, `check1()` are organized.

- [ ] **Step D.1.3: Create the new test file**

Create `/root/code/TDinternal/community/test/cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py` with the following content:

```python
import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem,)


class TestPreFilterTrowsScanCols:
    """Regression for stream trigger/calc scan-cols optimization.

    Trigger AST must NOT include calc-only columns (c3, t2).
    Calc AST MUST include pre_filter columns (c2) and apply pre_filter as WHERE,
    so calc-side independent re-scan returns exactly the rows pre_filter allows.
    """

    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_pre_filter_trows_scan_cols(self):
        """%%trows scan cols optimization

        Trigger only scans state_window + pre_filter cols; calc independently
        re-scans with injected pre_filter WHERE, producing identical rows.

        Catalog:
            - Streams:Options

        Since: v3.3.x

        Labels: common,ci

        Jira: None
        """

        tdStream.createSnode()
        streams = []
        streams.append(self.PreFilterTrows())
        tdStream.checkAll(streams)

    class PreFilterTrows(StreamCheckItem):
        def __init__(self):
            self.db = "pf_trows_db"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create stable stb (ts timestamp, c1 int, c2 int, c3 int) "
                f"tags (t1 int, t2 int)"
            )
            tdSql.execute(f"create table ct1 using stb tags(1, 100)")
            tdSql.execute(f"create table ct2 using stb tags(2, 200)")
            # Stream from the example in the design spec:
            #   trigger: state_window(c1) + pre_filter(c2>2)
            #   calc:    select _c0, sum(c3), avg(t2) from %%trows
            tdSql.execute(
                f"create stream s_pf state_window(c1) from stb "
                f"partition by t1 stream_options(pre_filter(c2 > 2)) "
                f"into res_stb (firstts, sum_c3, avg_t2) as "
                f"select first(_c0), sum(c3), avg(t2) from %%trows;"
            )

        def insert1(self):
            sqls = [
                # ct1 (t1=1, t2=100): c1 alternates to drive state windows;
                # c2 values include some <=2 (must be filtered out by pre_filter).
                "insert into ct1 values ('2025-01-01 00:00:00', 1, 1, 10);",  # c2<=2 -> filtered
                "insert into ct1 values ('2025-01-01 00:00:01', 1, 5, 20);",
                "insert into ct1 values ('2025-01-01 00:00:02', 1, 7, 30);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2, 8, 40);",  # state change closes window
                "insert into ct1 values ('2025-01-01 00:00:04', 2, 2, 50);",  # c2<=2 -> filtered
                "insert into ct1 values ('2025-01-01 00:00:05', 2, 9, 60);",
                "insert into ct1 values ('2025-01-01 00:00:06', 1, 4, 70);",  # state change
            ]
            tdSql.executes(sqls)

        def check1(self):
            # Expect at least one closed window for ct1 with c1==1 spanning ts 1..2
            # (the ts 0 row is dropped by pre_filter).
            # sum(c3) over rows kept = 20+30 = 50; avg(t2) = 100.
            tdSql.checkResultsByFunc(
                sql=f"select sum_c3, avg_t2 from {self.db}.res_stb "
                    f"where firstts = '2025-01-01 00:00:01';",
                func=lambda: tdSql.getRows() == 1
                             and tdSql.getData(0, 0) == 50
                             and abs(tdSql.getData(0, 1) - 100.0) < 1e-9,
            )

        def check2(self):
            # Negative: writing WHERE on %%trows must still be rejected.
            tdSql.error(
                f"create stream s_neg state_window(c1) from {self.db}.stb "
                f"partition by t1 stream_options(pre_filter(c2 > 2)) "
                f"into {self.db}.res_neg (firstts, s) as "
                f"select first(_c0), sum(c3) from %%trows where c2 > 5;",
                expectErrInfo="trows can not be used with WHERE clause",
            )
```

- [ ] **Step D.1.4: Smoke-run the new test**

```bash
cd /root/code/TDinternal/community/test
pytest cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py -v 2>&1 | tail -30
```

Expected: PASS. If the local environment cannot run integration tests, **stop** and report the limitation; do NOT mark the step done.

If the test fails on `check1` because the expected aggregate values differ (timing / window-closing semantics may be slightly off in this contrived dataset), inspect actual rows with:

```bash
# add a one-shot debug print, e.g. in check1: print(tdSql.queryResult)
# then re-run and adjust the expected sum/avg to match real semantics —
# the test is meant to pin behavior, not to reverse-engineer it.
```

Do NOT change the expected values to match a buggy result. If aggregates look wrong relative to the spec's intent (rows with c2<=2 leaking in), that is a real regression — investigate Tasks A/B before relaxing the assertion.

### Subtask D.2: Commit Task D

- [ ] **Step D.2.1: Stage and commit**

```bash
cd /root/code/TDinternal/community
git add test/cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py
git -c user.name='Copilot' -c user.email='copilot@local' commit -m "test(stream): pin %%trows + pre_filter scan-col behavior

Adds a regression test mirroring the design-spec example:
  trigger: state_window(c1) + pre_filter(c2>2)
  calc:    select _c0, sum(c3), avg(t2) from %%trows
The aggregate values verify pre_filter is actually applied on the calc
side (rows with c2<=2 must not contribute to sum(c3)/avg(t2)). A
negative case re-confirms that user-written WHERE on %%trows is still
rejected.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Final Verification

- [ ] **Step F.1: Show the four-commit series**

```bash
cd /root/code/TDinternal/community
git log --oneline -5
```

Expected: spec commit + Task A + Task B + Task C + Task D commits in order, all attributed with `Co-authored-by: Copilot`.

- [ ] **Step F.2: Confirm `triggerScanList` is gone**

```bash
grep -rn "triggerScanList" --include='*.c' --include='*.h' source include
```

Expected: empty.

- [ ] **Step F.3: Confirm helper is in place**

```bash
grep -cn "injectPreFilterIntoCalcQuery\b" source/libs/parser/src/parTranslater.c
grep -n "static int32_t injectPreFilterIntoCalcQuery" source/libs/parser/src/parTranslater.c
```

Expected: first command prints `>=2`; second command prints exactly 2 lines (the two `static int32_t injectPreFilterIntoCalcQuery...` definitions: `Impl` and the wrapper).

- [ ] **Step F.4: Final taos build**

```bash
cd /root/code/TDinternal/debug
cmake --build . --target taos -j$(nproc) 2>&1 | tail -5
```

Expected: clean build.

- [ ] **Step F.5: Run the broader stream `Basic9` (PRE_FILTER) and the new test together**

```bash
cd /root/code/TDinternal/community/test
pytest cases/18-StreamProcessing/04-Options/test_options_basic.py::TestStreamOptionsBasic::test_stream_options_basic \
       cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py -v 2>&1 | tail -50
```

Expected: PASS. Any failure must be diagnosed before declaring the work done — a regression on `Basic9` would mean the optimization broke an existing case, not just an aspirational one.
