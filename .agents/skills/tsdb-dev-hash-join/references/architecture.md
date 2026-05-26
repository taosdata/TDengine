# Hash Join Architecture

## Overview

The TDengine hash join is a classic two-phase hash join implemented as a pull-based operator in the query execution engine.

## Phase 1: Build

Consume all rows from the build table (typically the smaller table). For each row:

1. Serialize equality key columns into a flat byte buffer
2. Insert into `pKeyHash` (a simple open-addressing hash table)
3. Value columns (non-key output columns) are serialized into a page-pool arena in compact binary format
4. Multiple build rows with the same key form a singly-linked list via `SBufRowInfo.next`

```text
Build downstream
  -> getNextBlockFromDownstream (pull blocks)
  -> hJoinLaunchEqualExpr (evaluate TIMETRUNCATE or scalar exprs)
  -> hJoinFilterTimeRange (binary search for valid timestamp range)
  -> hJoinSetKeyColsData (bind column pointers)
  -> for each row in [startIdx, endIdx]:
       hJoinCopyKeyColsDataToBuf (serialize composite key)
       if key has NULL: skip (or emit for FULL join)
       hJoinAddRowToHash:
         hJoinSetValColsData (bind value column pointers)
         tSimpleHashGet (look up existing group)
         hJoinGetValBufFromPages (allocate space in page pool)
         hJoinCopyValColsDataToBuf (serialize value data)
         update linked list head
```

### Full Join Build Phase

Full outer join uses `hFullJoinBuildHash` instead of `hJoinBuildHash`. The key difference:

- Build rows with NULL keys cannot match any probe row but must still appear in the FULL OUTER result
- These rows are emitted immediately during the build phase with NULL-padded probe columns
- Uses `SFGroupData` instead of `SGroupData` to track matched/unmatched status

## Phase 2: Probe

Pull blocks from the probe table one at a time. For each probe row:

1. Serialize key columns and look up in the hash table
2. If found, walk the linked list of matching build rows
3. Reconstruct each result row: probe columns (from current block) + build columns (from page pool)
4. Emit to output block until threshold reached, then return to caller
5. On next call, resume via `pBuildRow` and `rowRemains` flag

```text
hJoinMainProcess (called repeatedly by upstream)
  -> if !keyHashBuilt: run buildFp (build phase)
  -> loop:
       if midRemains: swap midBlk/finBlk, check threshold
       if rowRemains: resume joinFp, return if result ready
       fetch next probe block from downstream
         if NULL: hJoinSetDone, apply finFilter, break
       hJoinPrepareStart:
         hJoinLaunchEqualExpr (scalar exprs on probe block)
         hJoinFilterTimeRange (find valid row range)
         hJoinSetKeyColsData (bind probe key columns)
         set probePhase = PRE (if startIdx > 0) or CUR
         call joinFp
       if threshold reached: apply finFilter, return
```

## Three-Phase Probe State Machine

Used by LEFT, ANTI, and FULL joins when `hasTimeRange` is true.

```text
                    probeStartIdx        probeEndIdx
                         |                    |
  [---PRE phase---]  [---CUR phase---]  [---POST phase---]
  rows before          rows within        rows after
  time window          time window        time window
  -> emit as           -> match against   -> emit as
     non-matching         hash table         non-matching
```

Phase transitions:
- PRE -> CUR: when `probePreIdx` reaches `probeStartIdx`
- CUR -> POST: when all rows in [startIdx, endIdx] are processed
- POST -> done: when `probePostIdx` reaches end of block

## Two-Block Output Strategy (midBlk / finBlk)

When `pPreFilter` exists (non-equi ON conditions):

```text
probe row + matching build rows
  -> write to midBlk
  -> apply pPreFilter to midBlk
  -> if rows survive:
       merge into finBlk (via blockDataMerge or blockDataMergeNRows)
  -> if no rows survive AND all build rows exhausted:
       emit NULL-padded probe row to finBlk
  -> if midBlk overflows finBlk:
       set midRemains = true
       on next call: swap midBlk and finBlk
```

This strategy is necessary because the filter may eliminate all matched rows for a probe row, converting a "match" into a "non-match" that requires NULL padding.

## Output Block Full and Probe Row Resumption

When the output block (`finBlk`) reaches the threshold during probe, the current probe row may not have been fully processed (i.e., there are remaining matching build rows in the linked list). The resumption mechanism works as follows:

```text
Probe row N: hash lookup finds build rows [B1, B2, B3, B4, B5]
  -> append B1, B2, B3 to finBlk
  -> finBlk reaches threshold (hJoinBlkReachThreshold returns true)
  -> save pBuildRow = B4 (next unprocessed build row)
  -> set ctx.rowRemains = true
  -> return finBlk to upstream

Next call to hJoinMainProcess:
  -> detects rowRemains == true
  -> calls joinFp again
  -> joinFp checks pBuildRow != NULL at entry
  -> resumes from B4: append B4, B5 to finBlk
  -> pBuildRow becomes NULL (linked list exhausted)
  -> continues to probe row N+1
```

Key implementation details:
- `ctx.pBuildRow` saves the position in the build-side linked list where processing was interrupted
- `ctx.rowRemains` signals `hJoinMainProcess` to re-enter the join function without fetching a new probe block
- The probe row index is NOT advanced until all matching build rows for that probe row have been processed
- This mechanism is shared by all join types (INNER, LEFT, SEMI, ANTI, FULL) — each `h<Type>JoinDo` function checks `pBuildRow` at entry to handle resumption

## Primary Key Expression Support

The hash join supports expressions on the primary timestamp key column for equality matching. Currently supported:

### TIMETRUNCATE

The only expression currently supported on the primary key. It truncates timestamps to a specified time unit for matching.

Configuration via `SHJoinPrimExprCtx`:
- `truncateUnit`: the truncation granularity (e.g., 1000000000 for 1 second in nanoseconds)
- `timezoneUnit`: timezone offset for timezone-aware truncation; 0 if not needed
- `targetSlotId`: slot in the block where the truncated timestamp result is stored

Computation (inline, no expression evaluation overhead):
- Without timezone: `out[i] = in[i] / unit * unit`
- With timezone: `out[i] = in[i] - (in[i] + timezoneUnit) % unit`

### No expression (direct comparison)

When no primary key expression is specified (`primExpr == NULL`), the primary timestamp column is used directly as the hash key without transformation. The column mapping is handled by `SHJoinColMap` (primCol).

### Other scalar expressions

Additional scalar expressions on non-primary key columns are supported via `SExprSupp` (exprSup), evaluated through `projectApplyFunctions`. These are general-purpose and not limited to specific function types.

## Per-Join-Type Execution Logic

### INNER JOIN (hInnerJoinDo)

Simplest type. For each probe row:
1. Serialize key; skip if NULL
2. Hash lookup; skip if not found
3. Walk linked list, append each build row to finBlk via `hJoinAppendResToBlock`
4. If block fills up, set `rowRemains = true`, save `pBuildRow`, and return

### LEFT/RIGHT OUTER JOIN (hLeftJoinDo)

Uses three-phase state machine. Two sub-paths:

**With pPreFilter**: Matched rows go through midBlk -> filter -> finBlk. If no row passes filter, emit NULL-padded probe row.

**Without pPreFilter**: Matched rows go directly to finBlk. Unmatched probe rows get NULL padding.

RIGHT join is implemented by swapping build/probe sides and using the same LEFT join logic.

### SEMI JOIN (hSemiJoinDo)

Emit each probe row at most once if any matching build row exists.

**With pPreFilter**: Iterate build rows through midBlk, apply filter with `mJoinFilterAndKeepSingleRow` (keeps only first passing row).

**Without pPreFilter**: Uses `grpSingleRow` optimization — each hash group has at most one relevant row.

### ANTI JOIN (hAntiJoinDo)

Emit each probe row if NO matching build row passes the ON condition. Uses three-phase state machine.

**With pPreFilter**: For each probe row with a hash match, iterate all build rows through midBlk using `mJoinFilterAndNoKeepRows`. If any pass, do NOT emit. If none pass, emit the probe row.

**Without pPreFilter**: Emit probe rows with NULL keys or no hash match.

### FULL OUTER JOIN (hFullJoinDo)

Most complex. Must emit:
1. All matched probe-build pairs
2. Unmatched probe rows with NULL build columns
3. Unmatched build rows with NULL probe columns

Uses `SFGroupData` with bitmap to track which build rows were matched during probe. Unmatched build rows (including those with NULL keys) are emitted during the build phase.

## Condition Routing

| Join Type | pFullOnCond | pConditions |
|-----------|-------------|-------------|
| INNER | Merged into pFinFilter | pFinFilter |
| LEFT/RIGHT/FULL | pPreFilter | pFinFilter |

For INNER join, `pFullOnCond` and `pConditions` are merged with AND logic before initializing the filter. For outer joins, they serve different roles: `pPreFilter` determines match/non-match semantics, while `pFinFilter` simply removes rows from the final output.

## Operator Lifecycle

```text
createHashJoinOperatorInfo
  -> allocate SHJoinOperatorInfo
  -> hJoinSetBuildAndProbeTable (determine build/probe assignment)
  -> hJoinInitTableInfo x2 (key cols, value cols, expressions)
  -> hJoinBuildResColsMap (output column mapping)
  -> hJoinInitBufPages (create page pool)
  -> tSimpleHashInit (create hash table, capacity = 1.5x estimated rows)
  -> hJoinHandleConds (set up pre-filter and fin-filter)
  -> hJoinInitResBlocks (allocate finBlk and optionally midBlk)
  -> hJoinSetImplFp (bind joinFp and buildFp)

hJoinMainProcess (next function, called repeatedly)
  -> lazy build on first call
  -> probe loop until done

hJoinSetDone
  -> setOperatorCompleted
  -> tSimpleHashCleanup (release hash table)
  -> blockDataDestroy(midBlk)
  -> release page pool array
  -> finBlk preserved for last output batch

destroyHashJoinOperator (close function)
  -> release all remaining memory
```

## Function Reference

### hashjoinoperator.c — Operator Framework

| Function | Purpose |
|----------|---------|
| `createHashJoinOperatorInfo` | Factory: allocate and initialize the operator |
| `hJoinMainProcess` | Main execution loop (the operator's "next" function) |
| `hJoinSetDone` | Mark operator complete, release mid-execution resources |
| `hJoinBuildHash` | Standard build phase (non-Full joins) |
| `hFullJoinBuildHash` | Full join build phase (interleaves hash build with NMatch emission) |
| `hJoinPrepareStart` | Set up probe context for one probe block |
| `hJoinHandleMidRemains` | Swap midBlk and finBlk on overflow |
| `hJoinCopyMergeMidBlk` | Merge midBlk rows into finBlk |
| `hJoinBlkReachThreshold` | Check if output block is full enough to return |
| `hJoinSetImplFp` | Bind joinFp and buildFp based on join type |
| `hJoinHandleConds` | Route conditions to pPreFilter / pFinFilter |
| `hJoinFilterTimeRange` | Binary search for valid timestamp range in a block |
| `hJoinLaunchEqualExpr` | Evaluate TIMETRUNCATE or other scalar expressions |
| `hJoinAddRowToHash` | Insert one build row into the hash table |
| `hJoinInitTableInfo` | Initialize per-table context (keys, values, expressions) |
| `resetHashJoinOperState` | Reset operator for re-execution (streaming) |

### hashjoin.c — Join Type Implementations

| Function | Purpose |
|----------|---------|
| `hInnerJoinDo` | Execute one probe block for INNER join |
| `hLeftJoinDo` | Execute one probe block for LEFT/RIGHT OUTER join |
| `hSemiJoinDo` | Execute one probe block for SEMI join |
| `hAntiJoinDo` | Execute one probe block for ANTI join |
| `hFullJoinDo` | Execute one probe block for FULL OUTER join |
| `hJoinAppendResToBlock` | Copy matched build rows into output block |
| `hJoinCopyKeyColsDataToBuf` | Serialize key columns for one row |
| `hJoinCopyNMatchRowsToBlock` | Emit non-matching (NULL-padded) rows |
| `hJoinSetKeyColsData` | Bind key column pointers from a block |
| `mJoinFilterAndKeepSingleRow` | Filter midBlk, keep first passing row (Semi) |
| `mJoinFilterAndNoKeepRows` | Filter midBlk, discard all rows (Anti) |

### joinTests.cpp — Test Infrastructure

| Component | Purpose |
|-----------|---------|
| `SJoinTestCtx` | Global test context with mock data and expected results |
| `SJoinTestParam` | Parameters for one test run (join type, conditions, etc.) |
| `createBothBlkRowsData` | Generate random input data for both sides |
| `createDummyHashJoinPhysiNode` | Build complete physical plan AST for testing |
| `innerJoinAppendEqGrpRes` | Compute expected INNER join results |
| `leftJoinAppendEqGrpRes` | Compute expected LEFT join results |
| `semiJoinAppendEqGrpRes` | Compute expected SEMI join results |
| `antiJoinAppendEqGrpRes` | Compute expected ANTI join results |
| `fullJoinAppendEqGrpRes` | Compute expected FULL OUTER join results |
