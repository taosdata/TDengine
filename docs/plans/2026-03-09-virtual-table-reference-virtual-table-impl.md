# Virtual Table Reference Virtual Table Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Support virtual tables referencing other virtual tables, enabling recursive resolution of source tables for queries.

**Architecture:** Reuse existing circular reference detection and depth limit framework. Enhance Catalog layer for recursive meta resolution, Planner layer for correct vgid routing, and Executor layer for dynamic query control.

**Tech Stack:** C/C++, TDengine catalog/planner/executor modules

**Design Doc:** `docs/plans/2026-03-09-virtual-table-reference-virtual-table-design.md`

---

## Task 1: Verify Existing Framework

**Goal:** Confirm existing parser-level support is functional.

### Step 1: Read existing circular reference detection

Read: `source/libs/parser/src/parTranslater.c:23917-24043`

Verify these functions exist and understand their logic:
- `detectCircularReference()`
- `checkRefDepth()`
- `getOriginalTablePrecision()`

### Step 2: Read existing checkColRef function

Read: `source/libs/parser/src/parTranslater.c:24046-24134`

Verify it already allows virtual table types:
- `TSDB_VIRTUAL_NORMAL_TABLE`
- `TSDB_VIRTUAL_CHILD_TABLE`
- Virtual super table (`pRefTableMeta->virtualStb && tableType == TSDB_SUPER_TABLE`)

### Step 3: Read existing getOriginalTableVgroupInfo

Read: `source/libs/parser/src/parTranslater.c:6387-6437`

This function already recursively resolves vgroup info for virtual table chains.

---

## Task 2: Catalog Layer - Add Recursive Original Table Meta Fetcher

**Files:**
- Modify: `source/libs/catalog/src/ctgRemote.c`
- Modify: `source/libs/catalog/inc/catalog.h` (if declaration needed)
- Test: Manual verification with test queries

### Step 1: Add helper function declaration

In `source/libs/catalog/inc/catalog.h`, add declaration (if internal function):

```c
/**
 * @brief Recursively get original table meta from virtual table chain
 * @param pCtg Catalog handle
 * @param pDbName Database name
 * @param pTableName Table name (can be virtual)
 * @param ppOriginalMeta Output: original table meta
 * @param pOriginalVgid Output: original table vgid
 * @return TSDB_CODE_SUCCESS on success
 */
int32_t ctgGetOriginalTableMeta(SCatalog* pCtg, const char* pDbName,
                                 const char* pTableName,
                                 STableMeta** ppOriginalMeta,
                                 SVgroupInfo* pOriginalVgid);
```

### Step 2: Implement ctgGetOriginalTableMeta in ctgRemote.c

In `source/libs/catalog/src/ctgRemote.c`, add:

```c
#include "tarray.h"

#define CTG_MAX_RECURSION_DEPTH 10

static int32_t ctgGetOriginalTableMetaImpl(SCatalog* pCtg, const char* pDbName,
                                            const char* pTableName,
                                            STableMeta** ppOriginalMeta,
                                            SVgroupInfo* pOriginalVgid,
                                            SArray* pVisitedTables,
                                            int32_t depth) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pMeta = NULL;
  char        fullName[TSDB_TABLE_FNAME_LEN] = {0};
  
  // 1. Check recursion depth
  if (depth > TSDB_MAX_VTABLE_REF_DEPTH) {
    ctgError("virtual table reference depth exceeded: %d", depth);
    return TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED;
  }
  
  // 2. Build full name
  snprintf(fullName, sizeof(fullName), "%s.%s", pDbName, pTableName);
  
  // 3. Check circular reference
  for (int32_t i = 0; i < taosArrayGetSize(pVisitedTables); i++) {
    char* pVisited = *(char**)taosArrayGet(pVisitedTables, i);
    if (pVisited != NULL && strcmp(pVisited, fullName) == 0) {
      ctgError("circular reference detected: %s", fullName);
      return TSDB_CODE_VTABLE_CIRCULAR_REFERENCE;
    }
  }
  
  // 4. Add current table to visited list
  char* pFullNameCopy = taosStrdup(fullName);
  if (pFullNameCopy == NULL) {
    return terrno;
  }
  if (taosArrayPush(pVisitedTables, &pFullNameCopy) == NULL) {
    taosMemoryFree(pFullNameCopy);
    return terrno;
  }
  
  // 5. Get table meta
  SName name = {0};
  tNameFromString(&name, fullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  code = ctgGetTbMeta(pCtg, NULL, &name, &pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  
  // 6. Check if virtual table
  bool isVirtualTable = (pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE ||
                         pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE ||
                         (pMeta->virtualStb && pMeta->tableType == TSDB_SUPER_TABLE));
  
  if (isVirtualTable && pMeta->numOfColRefs > 0 && pMeta->colRef[0].hasRef) {
    // Recursively get original table
    code = ctgGetOriginalTableMetaImpl(pCxt, 
                                        pMeta->colRef[0].refDbName,
                                        pMeta->colRef[0].refTableName,
                                        ppOriginalMeta, pOriginalVgid,
                                        pVisitedTables, depth + 1);
    taosMemoryFree(pMeta);
    return code;
  }
  
  // 7. Original table found, get vgid
  *ppOriginalMeta = pMeta;
  code = ctgGetVgroupInfo(pCtg, pDbName, pTableName, pOriginalVgid);
  
  return code;
}

int32_t ctgGetOriginalTableMeta(SCatalog* pCtg, const char* pDbName,
                                 const char* pTableName,
                                 STableMeta** ppOriginalMeta,
                                 SVgroupInfo* pOriginalVgid) {
  SArray* pVisitedTables = taosArrayInit(16, sizeof(char*));
  if (pVisitedTables == NULL) {
    return terrno;
  }
  
  int32_t code = ctgGetOriginalTableMetaImpl(pCtg, pDbName, pTableName,
                                              ppOriginalMeta, pOriginalVgid,
                                              pVisitedTables, 1);
  
  // Cleanup visited tables
  for (int32_t i = 0; i < taosArrayGetSize(pVisitedTables); i++) {
    char* p = *(char**)taosArrayGet(pVisitedTables, i);
    taosMemoryFree(p);
  }
  taosArrayDestroy(pVisitedTables);
  
  return code;
}
```

### Step 3: Build and verify compilation

Run:
```bash
cd /home/yihao/3.0/enh/virtualTableRef1
mkdir -p debug && cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make -j4
```

Expected: Build succeeds without errors.

### Step 4: Commit

```bash
git add source/libs/catalog/src/ctgRemote.c source/libs/catalog/inc/catalog.h
git commit -m "feat(catalog): add ctgGetOriginalTableMeta for virtual table chain resolution"
```

---

## Task 3: Planner Layer - Enhance Virtual Table Query Planning

**Files:**
- Modify: `source/libs/planner/src/planLogicCreater.c`
- Test: Manual verification

### Step 1: Locate createVirtualSuperTableLogicNode

Read: `source/libs/planner/src/planLogicCreater.c:1135-1200`

### Step 2: Add virtual table reference resolution

In `createVirtualSuperTableLogicNode`, after getting reference table:

```c
static int32_t createVirtualSuperTableLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                                SVirtualTableNode* pVirtualTable, 
                                                SVirtualScanLogicNode* pVtableScan,
                                                SLogicNode** pLogicNode) {
  int32_t     code = TSDB_CODE_SUCCESS;
  // ... existing code ...
  
  // Get reference table
  SRealTableNode* pRefTable = (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 0);
  
  // NEW: Check if reference table is also virtual
  bool isRefVirtual = (pRefTable->pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE ||
                       pRefTable->pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE ||
                       (pRefTable->pMeta->virtualStb && 
                        pRefTable->pMeta->tableType == TSDB_SUPER_TABLE));
  
  if (isRefVirtual) {
    // Use recursive function to get original table vgroup
    SVgroupInfo originalVgInfo = {0};
    SArray* pVisitedTables = taosArrayInit(16, sizeof(char*), NULL, taosMemoryFree);
    QUERY_CHECK_NULL(pVisitedTables, code, lino, _return, terrno);
    
    code = getOriginalTableVgroupInfo(pCxt, 
                                       pRefTable->table.dbName,
                                       pRefTable->table.tableName,
                                       &originalVgInfo, pVisitedTables);
    taosArrayDestroy(pVisitedTables);
    QUERY_CHECK_CODE(code, lino, _return);
    
    // Use original table's vgroup for scan node
    // ... update pRealTableScan with originalVgInfo ...
  }
  
  // ... rest of existing code ...
}
```

### Step 3: Build and verify

Run:
```bash
cd /home/yihao/3.0/enh/virtualTableRef1/debug
make -j4
```

Expected: Build succeeds.

### Step 4: Commit

```bash
git add source/libs/planner/src/planLogicCreater.c
git commit -m "feat(planner): enhance virtual super table planning for virtual table references"
```

---

## Task 4: Executor Layer - Enhance Dynamic Query Control

**Files:**
- Modify: `source/libs/executor/src/dynqueryctrloperator.c`
- Modify: `source/libs/executor/src/virtualtablescanoperator.c`
- Test: Manual verification

### Step 1: Locate dynQueryCtrlProcessVtableScan

Read: `source/libs/executor/src/dynqueryctrloperator.c:980-1050`

### Step 2: Add helper function for virtual table chain resolution

In `dynqueryctrloperator.c`, add:

```c
/**
 * @brief Get original vgroup for virtual table (handles virtual table chains)
 */
static int32_t getOriginalVgroupForVtable(SExecTaskInfo* pTaskInfo,
                                           const char* pDbName, 
                                           const char* pTableName,
                                           SVgroupInfo* pVgInfo) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STableMeta* pMeta = NULL;
  SArray*     pVisitedTables = NULL;
  
  // Get table meta
  SName name = {0};
  tNameFromString(&name, pDbName, T_NAME_DB);
  strcpy(name.tname, pTableName);
  
  code = catalogGetTableMeta(pTaskInfo->pCatalog, pTaskInfo->pMsg->pCont, &name, &pMeta);
  QUERY_CHECK_CODE(code, lino, _return);
  
  // Check if virtual table
  bool isVirtual = (pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE ||
                    pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE);
  
  if (!isVirtual) {
    // Not virtual, get vgroup directly
    code = catalogGetTableVgroup(pTaskInfo->pCatalog, pTaskInfo->pMsg->pCont, &name, pVgInfo);
    taosMemoryFree(pMeta);
    return code;
  }
  
  // Virtual table: recursively resolve to original table
  pVisitedTables = taosArrayInit(16, sizeof(char*));
  if (pVisitedTables == NULL) {
    taosMemoryFree(pMeta);
    return terrno;
  }
  
  int32_t depth = 1;
  while (isVirtual && depth <= TSDB_MAX_VTABLE_REF_DEPTH) {
    if (pMeta->numOfColRefs <= 0 || !pMeta->colRef[0].hasRef) {
      taosArrayDestroy(pVisitedTables);
      taosMemoryFree(pMeta);
      return TSDB_CODE_VTABLE_NO_SOURCE_TABLE;
    }
    
    // Get reference table
    tNameFromString(&name, pMeta->colRef[0].refDbName, T_NAME_DB);
    strcpy(name.tname, pMeta->colRef[0].refTableName);
    
    taosMemoryFree(pMeta);
    pMeta = NULL;
    
    code = catalogGetTableMeta(pTaskInfo->pCatalog, pTaskInfo->pMsg->pCont, &name, &pMeta);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pVisitedTables);
      return code;
    }
    
    isVirtual = (pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE ||
                 pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE);
    depth++;
  }
  
  if (depth > TSDB_MAX_VTABLE_REF_DEPTH) {
    taosArrayDestroy(pVisitedTables);
    taosMemoryFree(pMeta);
    return TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED;
  }
  
  // Get vgroup of original table
  code = catalogGetTableVgroup(pTaskInfo->pCatalog, pTaskInfo->pMsg->pCont, &name, pVgInfo);
  
  taosArrayDestroy(pVisitedTables);
  taosMemoryFree(pMeta);
  return code;
}
```

### Step 3: Use helper in dynQueryCtrlProcessVtableScan

Modify the existing code to use the new helper:

```c
static int32_t dynQueryCtrlProcessVtableScan(...) {
  // ... existing code ...
  
  for (int32_t i = 0; i < numOfVtables; i++) {
    SVirtualChildTableInfo* pVChild = taosArrayGet(pVtableList, i);
    
    // NEW: Get original vgroup (handles virtual table chains)
    SVgroupInfo originalVgInfo = {0};
    code = getOriginalVgroupForVtable(pTaskInfo, pVChild->dbName, 
                                       pVChild->tableName, &originalVgInfo);
    QUERY_CHECK_CODE(code, lino, _return);
    
    // Create scan param with original vgid
    SVTableScanOperatorParam* pVScan = taosMemoryMalloc(sizeof(SVTableScanOperatorParam));
    QUERY_CHECK_NULL(pVScan, code, lino, _return, terrno);
    
    pVScan->vgid = originalVgInfo.vgId;  // Use original table's vgid
    // ... rest of param setup ...
  }
  
  // ... existing code ...
}
```

### Step 4: Build and verify

Run:
```bash
cd /home/yihao/3.0/enh/virtualTableRef1/debug
make -j4
```

Expected: Build succeeds.

### Step 5: Commit

```bash
git add source/libs/executor/src/dynqueryctrloperator.c
git commit -m "feat(executor): add virtual table chain resolution in dynamic query control"
```

---

## Task 5: Add Error Codes

**Files:**
- Modify: `include/util/taoserror.h`
- Modify: `source/util/src/terror.c`

### Step 1: Add error code definitions

In `include/util/taoserror.h`:

```c
// In appropriate error code section
#define TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED     TAOS_DEF_ERROR_CODE(0, 0x0XXX)  // Fill appropriate module code
#define TSDB_CODE_VTABLE_CIRCULAR_REFERENCE     TAOS_DEF_ERROR_CODE(0, 0x0XXX)
#define TSDB_CODE_VTABLE_NO_SOURCE_TABLE        TAOS_DEF_ERROR_CODE(0, 0x0XXX)
```

### Step 2: Add error messages

In `source/util/src/terror.c`:

```c
// In appropriate section
TMSG(TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED, "Virtual table reference depth exceeded")
TMSG(TSDB_CODE_VTABLE_CIRCULAR_REFERENCE, "Virtual table circular reference detected")
TMSG(TSDB_CODE_VTABLE_NO_SOURCE_TABLE, "Virtual table has no source table")
```

### Step 3: Build and verify

Run:
```bash
cd /home/yihao/3.0/enh/virtualTableRef1/debug
make -j4
```

### Step 4: Commit

```bash
git add include/util/taoserror.h source/util/src/terror.c
git commit -m "feat(error): add virtual table chain error codes"
```

---

## Task 6: Integration Testing

**Files:**
- Create test cases in `tests/system-test/` or manual testing

### Step 1: Create test SQL script

Create file: `tests/vtable_ref_chain_test.sql`

```sql
-- Setup
CREATE DATABASE IF NOT EXISTS test_vtable;
USE test_vtable;

-- Create original table
CREATE TABLE t1 (ts TIMESTAMP, v INT);
INSERT INTO t1 VALUES (now, 1);
INSERT INTO t1 VALUES (now + 1s, 2);

-- Create virtual table referencing original
CREATE VTABLE vt1 (ts TIMESTAMP, v INT) 
  REFERENCE t1 (ts REF t1.ts, v REF t1.v);

-- Create virtual table referencing virtual table (NEW FEATURE)
CREATE VTABLE vt2 (ts TIMESTAMP, v INT) 
  REFERENCE vt1 (ts REF vt1.ts, v REF vt1.v);

-- Test query through chain
SELECT * FROM vt2;

-- Test with deeper chain
CREATE VTABLE vt3 (ts TIMESTAMP, v INT) 
  REFERENCE vt2 (ts REF vt2.ts, v REF vt2.v);

SELECT * FROM vt3;

-- Test error: circular reference (should fail)
-- CREATE VTABLE vt4 (ts TIMESTAMP, v INT) 
--   REFERENCE vt5 (ts REF vt5.ts, v REF vt5.v);
-- CREATE VTABLE vt5 (ts TIMESTAMP, v INT) 
--   REFERENCE vt4 (ts REF vt4.ts, v REF vt4.v);

-- Cleanup
DROP TABLE IF EXISTS vt3;
DROP TABLE IF EXISTS vt2;
DROP TABLE IF EXISTS vt1;
DROP TABLE IF EXISTS t1;
DROP DATABASE IF EXISTS test_vtable;
```

### Step 2: Run test

```bash
cd /home/yihao/3.0/enh/virtualTableRef1
./build/bin/taos -s "source tests/vtable_ref_chain_test.sql"
```

Expected: Queries on vt2 and vt3 should return data from t1.

### Step 3: Verify results

- `SELECT * FROM vt2` should return data
- `SELECT * FROM vt3` should return data
- No circular reference errors for valid chains

---

## Task 7: Final Verification and Documentation

### Step 1: Run full build

```bash
cd /home/yihao/3.0/enh/virtualTableRef1
./build.sh
```

### Step 2: Run relevant unit tests

```bash
cd debug/build/bin
./osTimeTests  # or other relevant tests
```

### Step 3: Update documentation

Update any relevant documentation about virtual table capabilities.

### Step 4: Final commit

```bash
git add .
git commit -m "feat: complete virtual table reference virtual table implementation"
```

---

## Key Files Summary

| File | Purpose |
|------|---------|
| `source/libs/parser/src/parTranslater.c` | Reference checking (already done) |
| `source/libs/catalog/src/ctgRemote.c` | Recursive meta fetching |
| `source/libs/catalog/inc/catalog.h` | Function declarations |
| `source/libs/planner/src/planLogicCreater.c` | Query planning enhancement |
| `source/libs/executor/src/dynqueryctrloperator.c` | Dynamic query control |
| `include/util/taoserror.h` | Error codes |
| `source/util/src/terror.c` | Error messages |

## Rollback Plan

If issues arise:
1. `git revert HEAD~N` to rollback commits
2. Feature is isolated to specific files - no schema changes
3. Existing virtual table functionality unaffected
