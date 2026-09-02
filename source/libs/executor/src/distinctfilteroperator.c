/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "executorInt.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tsimplehash.h"
#include "tsort.h"
#include "thash.h"
#include "ttime.h"
#include "tcompare.h"
#include "tglobal.h"

// Spill-to-disk state machine
typedef enum {
  DISTINCT_STATE_HASH = 0,        // Normal hash-based filtering
  DISTINCT_STATE_SPILL_EMIT,      // Emitting deduplicated rows from sorted output
} EDistinctState;

typedef struct SDistinctFilterInfo {
  SSHashObj*   pHashSet;
  int16_t      distinctColSlotId;
  int8_t       colType;
  int32_t      colBytes;
  bool         hasGroup;
  char*        keyBuf;
  int32_t      keyBufSize;
  SExprSupp    scalarSup;
  // interval-aware dedup
  bool         hasInterval;
  int16_t      tsSlotId;
  SInterval    interval;
  // spill-to-disk state
  EDistinctState  state;
  SSortHandle*    pSortHandle;
  char*           prevSortKey;     // previous key from sorted output for streaming dedup (global path)
  int32_t         prevSortKeyLen;
  bool            prevSortKeyValid;
  // partitioned spill emit state (INTERVAL / GROUP BY paths)
  SSHashObj*      pEmitSet;        // dedup set for the current partition (window/group)
  SSDataBlock*    pEmitBlock;      // output block accumulated across emit calls
  int32_t         emitRows;        // rows currently buffered in pEmitBlock
  // Blocks from tsortGetSortedDataBlock are freshly allocated and handed upstream, which does not
  // take ownership. Keep the last one returned and free it on the next call / on destroy.
  SSDataBlock*    pReturnedBlock;
  uint64_t        curGroupId;      // current partition group id
  int64_t         curWindow;       // current partition window start
  bool            curPartValid;    // whether curGroupId/curWindow are initialized
} SDistinctFilterInfo;

// Build composite key into pInfo->keyBuf, return key length
static int32_t buildDistinctKey(SDistinctFilterInfo* pInfo, SColumnInfoData* pDistCol,
                                SColumnInfoData* pTsCol, uint64_t groupId, int32_t rowIdx,
                                bool isVarType) {
  int32_t offset = 0;

  if (pInfo->hasInterval) {
    int64_t ts = *(int64_t*)colDataGetData(pTsCol, rowIdx);
    int64_t windowStart = taosTimeTruncate(ts, &pInfo->interval);
    memcpy(pInfo->keyBuf + offset, &windowStart, sizeof(int64_t));
    offset += sizeof(int64_t);
  }

  if (pInfo->hasGroup) {
    memcpy(pInfo->keyBuf + offset, &groupId, sizeof(uint64_t));
    offset += sizeof(uint64_t);
  }

  bool distNull = colDataIsNull_s(pDistCol, rowIdx);
  pInfo->keyBuf[offset++] = distNull ? 1 : 0;
  if (!distNull) {
    const char* data = colDataGetData(pDistCol, rowIdx);
    int32_t     dataLen = isVarType ? varDataTLen(data) : pInfo->colBytes;
    if (offset + dataLen <= pInfo->keyBufSize) {
      memcpy(pInfo->keyBuf + offset, data, dataLen);
      offset += dataLen;
    }
  }
  return offset;
}

// Fetch callback for sort handle — pulls blocks from our downstream operator
static int32_t distinctSpillFetchBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  *ppBlock = getNextBlockFromDownstream(pOperator, 0);
  return TSDB_CODE_SUCCESS;
}

// Apply scalar expressions if needed (for sort mode)
static void distinctSpillApplyScalar(SSDataBlock* pBlock, void* param) {
  SOperatorInfo*       pOperator = (SOperatorInfo*)param;
  SDistinctFilterInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->scalarSup.pExprInfo != NULL) {
    SExprSupp* pSup = &pInfo->scalarSup;
    int32_t code = projectApplyFunctions(pSup->pExprInfo, pBlock, pBlock, pSup->pCtx, pSup->numOfExprs,
                                         NULL, GET_STM_RTINFO(pTaskInfo), pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }
}

// Initialize sort handle for spill-to-disk mode
static int32_t initSpillSortHandle(SOperatorInfo* pOperator) {
  SDistinctFilterInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  int32_t              code = TSDB_CODE_SUCCESS;

  // Create sort order info.
  //
  // The group id is always the outermost sort key (via tsortSetCompareGroupId
  // below), so rows of the same group cluster together and groups appear in a
  // monotonic order in the sorted stream.
  //
  // Within a group the secondary key depends on the dedup semantics:
  //   * INTERVAL: order by timestamp. taosTimeTruncate() is monotonic in ts, so
  //     a ts-ordered stream is also window-start ordered. This keeps the emitted
  //     rows in ascending ts order, which the upstream Interval operator requires
  //     to form windows, and lets the emit phase reset its per-window dedup set
  //     as the window advances.
  //   * GLOBAL / GROUP: order by the distinct column value so identical values
  //     cluster for dedup.
  bool sortByTs = pInfo->hasInterval;
  SArray* pSortInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
  if (pSortInfo == NULL) return terrno;

  SBlockOrderInfo orderInfo = {
    .nullFirst = true,
    .order = TSDB_ORDER_ASC,
    .slotId = sortByTs ? pInfo->tsSlotId : pInfo->distinctColSlotId,
    .compFn = getKeyComparFunc(sortByTs ? TSDB_DATA_TYPE_TIMESTAMP : pInfo->colType, ORDER_ASC),
    .pColData = NULL,
  };
  if (taosArrayPush(pSortInfo, &orderInfo) == NULL) {
    taosArrayDestroy(pSortInfo);
    return terrno;
  }

  code = tsortCreateSortHandle(pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL,
                               pTaskInfo->id.str, 0, 0,
                               tsPQSortMemThreshold * 1024 * 1024, &pInfo->pSortHandle);
  taosArrayDestroy(pSortInfo);
  if (code != TSDB_CODE_SUCCESS) return code;

  tsortSetFetchRawDataFp(pInfo->pSortHandle, distinctSpillFetchBlock, distinctSpillApplyScalar, pOperator);
  // Only make the group id the outermost sort key for GROUP BY queries. For an
  // INTERVAL (or global) query there is no grouping, and the scan may still tag
  // blocks with non-zero/varying group ids; using it as the primary sort key
  // would break the global timestamp ordering the Interval operator relies on.
  tsortSetCompareGroupId(pInfo->pSortHandle, pInfo->hasGroup);

  SSortSource* pSource = taosMemoryCalloc(1, sizeof(SSortSource));
  if (pSource == NULL) return terrno;
  pSource->param = pOperator;
  pSource->onlyRef = true;

  code = tsortAddSource(pInfo->pSortHandle, pSource);
  if (code != TSDB_CODE_SUCCESS) return code;

  code = tsortOpen(pInfo->pSortHandle);
  return code;
}

// Build key from a sorted tuple using tsort accessor APIs
static int32_t buildDistinctKeyFromTuple(SDistinctFilterInfo* pInfo, STupleHandle* pTupleHandle, bool isVarType) {
  int32_t offset = 0;

  if (pInfo->hasInterval) {
    void* pTsVal = NULL;
    tsortGetValue(pTupleHandle, pInfo->tsSlotId, &pTsVal);
    if (pTsVal != NULL) {
      int64_t ts = *(int64_t*)pTsVal;
      int64_t windowStart = taosTimeTruncate(ts, &pInfo->interval);
      memcpy(pInfo->keyBuf + offset, &windowStart, sizeof(int64_t));
    } else {
      memset(pInfo->keyBuf + offset, 0, sizeof(int64_t));
    }
    offset += sizeof(int64_t);
  }

  if (pInfo->hasGroup) {
    uint64_t groupId = 0, baseGId = 0;
    tsortGetGroupId(pTupleHandle, &groupId, &baseGId);
    memcpy(pInfo->keyBuf + offset, &groupId, sizeof(uint64_t));
    offset += sizeof(uint64_t);
  }

  bool distNull = tsortIsNullVal(pTupleHandle, pInfo->distinctColSlotId);
  pInfo->keyBuf[offset++] = distNull ? 1 : 0;
  if (!distNull) {
    void* pVal = NULL;
    tsortGetValue(pTupleHandle, pInfo->distinctColSlotId, &pVal);
    if (pVal != NULL) {
      int32_t dataLen = isVarType ? varDataTLen(pVal) : pInfo->colBytes;
      if (offset + dataLen <= pInfo->keyBufSize) {
        memcpy(pInfo->keyBuf + offset, pVal, dataLen);
        offset += dataLen;
      }
    }
  }
  return offset;
}

// Update the prev sort key tracker
static int32_t updatePrevSortKey(SDistinctFilterInfo* pInfo, int32_t keyLen) {
  if (keyLen > pInfo->prevSortKeyLen) {
    char* newBuf = taosMemoryRealloc(pInfo->prevSortKey, keyLen);
    if (newBuf == NULL) return terrno;
    pInfo->prevSortKey = newBuf;
  }
  memcpy(pInfo->prevSortKey, pInfo->keyBuf, keyLen);
  pInfo->prevSortKeyLen = keyLen;
  pInfo->prevSortKeyValid = true;
  return TSDB_CODE_SUCCESS;
}

// Copy all columns of a sorted tuple into row rowIdx of pBlock.
static int32_t writeTupleToBlock(SSDataBlock* pBlock, int32_t rowIdx, STupleHandle* pTupleHandle) {
  int32_t code = TSDB_CODE_SUCCESS;
  size_t  numCols = tsortGetColNum(pTupleHandle);
  for (size_t c = 0; c < numCols; c++) {
    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, (int32_t)c);
    if (pDstCol == NULL) continue;

    if (tsortIsNullVal(pTupleHandle, (int32_t)c)) {
      colDataSetNULL(pDstCol, rowIdx);
    } else {
      void* pData = NULL;
      tsortGetValue(pTupleHandle, (int32_t)c, &pData);
      if (pData != NULL) {
        code = colDataSetVal(pDstCol, rowIdx, pData, false);
        if (code != TSDB_CODE_SUCCESS) return code;
      } else {
        colDataSetNULL(pDstCol, rowIdx);
      }
    }
  }
  return code;
}

// Partition-aware emit for INTERVAL / GROUP BY spill.
//
// Unlike the global path, the upstream operators here (Interval, GroupAggregate)
// depend on the structure of the emitted blocks:
//   * GROUP BY: each output block must carry a single group id; the sorted
//     stream is group-id monotonic, so we split a block whenever the group id
//     changes and tag the block with that group id.
//   * INTERVAL: rows must be emitted in ascending ts order (window-monotonic).
//
// Dedup is performed with a per-partition hash set (pEmitSet) that is reset when
// the partition (group id / window start) changes, keeping memory bounded to a
// single partition's distinct values. pHashSet still holds the keys already
// emitted during the pre-spill hash phase, so those are filtered out too.
//
// pEmitBlock is carried across calls: when a block is returned mid-stream, any
// already-consumed tuple that belongs to the next block is written into a fresh
// pEmitBlock so it is not lost.
// Hand a spill-emit block upstream while retaining ownership. tsortGetSortedDataBlock allocates a
// new block each call and consumers do not free it, so the previously returned block is released
// here — it has been fully consumed by the time the operator is called again.
static void distinctSetReturnedBlock(SDistinctFilterInfo* pInfo, SSDataBlock* pBlock, SSDataBlock** ppRes) {
  if (pInfo->pReturnedBlock != NULL && pInfo->pReturnedBlock != pBlock) {
    blockDataDestroy(pInfo->pReturnedBlock);
  }
  pInfo->pReturnedBlock = pBlock;
  *ppRes = pBlock;
}

static int32_t doDistinctFilterSpillEmitPartitioned(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDistinctFilterInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  int32_t              code = TSDB_CODE_SUCCESS;
  bool                 isVarType = IS_VAR_DATA_TYPE(pInfo->colType);
  int32_t              capacity = pOperator->resultInfo.capacity;

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    code = tsortNextTuple(pInfo->pSortHandle, &pTupleHandle);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (pTupleHandle == NULL) {
      // End of sorted stream: flush any buffered rows, else complete.
      if (pInfo->pEmitBlock != NULL && pInfo->emitRows > 0) {
        pInfo->pEmitBlock->info.rows = pInfo->emitRows;
        pInfo->pEmitBlock->info.id.groupId = pInfo->curGroupId;
        pInfo->pEmitBlock->info.dataLoad = 1;
        if (pInfo->hasInterval) blockDataUpdateTsWindow(pInfo->pEmitBlock, pInfo->tsSlotId);
        distinctSetReturnedBlock(pInfo, pInfo->pEmitBlock, ppRes);
        pInfo->pEmitBlock = NULL;
        pInfo->emitRows = 0;
      } else {
        setOperatorCompleted(pOperator);
        *ppRes = NULL;
      }
      return code;
    }

    uint64_t groupId = 0, baseGId = 0;
    tsortGetGroupId(pTupleHandle, &groupId, &baseGId);
    // The scan may tag blocks with non-zero/varying group ids even for a plain
    // INTERVAL (no GROUP BY) query. Only honour the group id when grouping, so
    // the upstream Interval operator sees a single, consistently-tagged stream.
    uint64_t partGroup = pInfo->hasGroup ? groupId : 0;

    int64_t windowStart = 0;
    if (pInfo->hasInterval) {
      void* pTsVal = NULL;
      tsortGetValue(pTupleHandle, pInfo->tsSlotId, &pTsVal);
      if (pTsVal != NULL) {
        windowStart = taosTimeTruncate(*(int64_t*)pTsVal, &pInfo->interval);
      }
    }

    int32_t keyLen = buildDistinctKeyFromTuple(pInfo, pTupleHandle, isVarType);

    bool groupChanged = pInfo->curPartValid && pInfo->hasGroup && (partGroup != pInfo->curGroupId);

    // The group id changed mid-block: finish the current group's block and stash
    // this tuple as the first row of the next block (it belongs to a new group).
    if (groupChanged && pInfo->pEmitBlock != NULL && pInfo->emitRows > 0) {
      pInfo->pEmitBlock->info.rows = pInfo->emitRows;
      pInfo->pEmitBlock->info.id.groupId = pInfo->curGroupId;
      pInfo->pEmitBlock->info.dataLoad = 1;
      if (pInfo->hasInterval) blockDataUpdateTsWindow(pInfo->pEmitBlock, pInfo->tsSlotId);
      distinctSetReturnedBlock(pInfo, pInfo->pEmitBlock, ppRes);
      pInfo->pEmitBlock = NULL;
      pInfo->emitRows = 0;

      tSimpleHashClear(pInfo->pEmitSet);
      pInfo->curGroupId = partGroup;
      pInfo->curWindow = windowStart;
      pInfo->curPartValid = true;

      if (tSimpleHashGet(pInfo->pHashSet, pInfo->keyBuf, keyLen) == NULL) {
        int32_t dummy = 0;
        code = tSimpleHashPut(pInfo->pEmitSet, pInfo->keyBuf, keyLen, &dummy, sizeof(dummy));
        if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
        code = tsortGetSortedDataBlock(pInfo->pSortHandle, &pInfo->pEmitBlock);
        if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
        code = blockDataEnsureCapacity(pInfo->pEmitBlock, capacity);
        if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
        code = writeTupleToBlock(pInfo->pEmitBlock, 0, pTupleHandle);
        if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
        pInfo->emitRows = 1;
      }
      return code;
    }

    // Partition (window or group) advanced: reset the per-partition dedup set.
    if (!pInfo->curPartValid || partGroup != pInfo->curGroupId || windowStart != pInfo->curWindow) {
      tSimpleHashClear(pInfo->pEmitSet);
      pInfo->curGroupId = partGroup;
      pInfo->curWindow = windowStart;
      pInfo->curPartValid = true;
    }

    // Dedup against the current partition and the pre-spill hash phase.
    if (tSimpleHashGet(pInfo->pEmitSet, pInfo->keyBuf, keyLen) != NULL) {
      continue;
    }
    if (tSimpleHashGet(pInfo->pHashSet, pInfo->keyBuf, keyLen) != NULL) {
      continue;
    }
    int32_t dummy = 0;
    code = tSimpleHashPut(pInfo->pEmitSet, pInfo->keyBuf, keyLen, &dummy, sizeof(dummy));
    if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);

    if (pInfo->pEmitBlock == NULL) {
      code = tsortGetSortedDataBlock(pInfo->pSortHandle, &pInfo->pEmitBlock);
      if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
      if (pInfo->pEmitBlock == NULL) {
        setOperatorCompleted(pOperator);
        *ppRes = NULL;
        return code;
      }
      code = blockDataEnsureCapacity(pInfo->pEmitBlock, capacity);
      if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
      pInfo->emitRows = 0;
    }

    code = writeTupleToBlock(pInfo->pEmitBlock, pInfo->emitRows, pTupleHandle);
    if (code != TSDB_CODE_SUCCESS) T_LONG_JMP(pTaskInfo->env, code);
    pInfo->emitRows++;

    if (pInfo->emitRows >= capacity) {
      pInfo->pEmitBlock->info.rows = pInfo->emitRows;
      pInfo->pEmitBlock->info.id.groupId = pInfo->curGroupId;
      pInfo->pEmitBlock->info.dataLoad = 1;
      if (pInfo->hasInterval) blockDataUpdateTsWindow(pInfo->pEmitBlock, pInfo->tsSlotId);
      distinctSetReturnedBlock(pInfo, pInfo->pEmitBlock, ppRes);
      pInfo->pEmitBlock = NULL;
      pInfo->emitRows = 0;
      return code;
    }
  }
}

// Emit deduplicated rows from sorted output
static int32_t doDistinctFilterSpillEmit(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDistinctFilterInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  int32_t              code = TSDB_CODE_SUCCESS;

  SSDataBlock* pBlock = NULL;
  code = tsortGetSortedDataBlock(pInfo->pSortHandle, &pBlock);
  if (code != TSDB_CODE_SUCCESS || pBlock == NULL) {
    setOperatorCompleted(pOperator);
    *ppRes = NULL;
    return code;
  }
  code = blockDataEnsureCapacity(pBlock, pOperator->resultInfo.capacity);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    T_LONG_JMP(pTaskInfo->env, code);
  }

  bool isVarType = IS_VAR_DATA_TYPE(pInfo->colType);
  int32_t rows = 0;

  while (rows < pOperator->resultInfo.capacity) {
    STupleHandle* pTupleHandle = NULL;
    code = tsortNextTuple(pInfo->pSortHandle, &pTupleHandle);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (pTupleHandle == NULL) break;

    int32_t keyLen = buildDistinctKeyFromTuple(pInfo, pTupleHandle, isVarType);

    // Streaming dedup: skip if same as previous key
    if (pInfo->prevSortKeyValid && keyLen == pInfo->prevSortKeyLen &&
        memcmp(pInfo->keyBuf, pInfo->prevSortKey, keyLen) == 0) {
      continue;
    }

    // Check if already emitted during hash phase
    void* exist = tSimpleHashGet(pInfo->pHashSet, pInfo->keyBuf, keyLen);
    if (exist != NULL) {
      code = updatePrevSortKey(pInfo, keyLen);
      if (code != TSDB_CODE_SUCCESS) {
        blockDataDestroy(pBlock);
        T_LONG_JMP(pTaskInfo->env, code);
      }
      continue;
    }

    // New unique key — emit this row by copying all columns
    size_t numCols = tsortGetColNum(pTupleHandle);
    for (size_t c = 0; c < numCols; c++) {
      SColumnInfoData* pSrcCol = NULL;
      tsortGetColumnInfo(pTupleHandle, (int32_t)c, &pSrcCol);
      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, (int32_t)c);
      if (pDstCol == NULL) continue;

      bool isNull = tsortIsNullVal(pTupleHandle, (int32_t)c);
      if (isNull) {
        colDataSetNULL(pDstCol, rows);
      } else {
        void* pData = NULL;
        tsortGetValue(pTupleHandle, (int32_t)c, &pData);
        if (pData != NULL) {
          code = colDataSetVal(pDstCol, rows, pData, false);
          if (code != TSDB_CODE_SUCCESS) {
            blockDataDestroy(pBlock);
            T_LONG_JMP(pTaskInfo->env, code);
          }
        } else {
          colDataSetNULL(pDstCol, rows);
        }
      }
    }
    rows++;

    code = updatePrevSortKey(pInfo, keyLen);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  if (rows == 0) {
    blockDataDestroy(pBlock);
    setOperatorCompleted(pOperator);
    *ppRes = NULL;
    return code;
  }

  pBlock->info.rows = rows;
  pBlock->info.dataLoad = 1;
  distinctSetReturnedBlock(pInfo, pBlock, ppRes);
  return code;
}

static void destroyDistinctFilterOperator(void* param) {
  SDistinctFilterInfo* pInfo = (SDistinctFilterInfo*)param;
  if (pInfo == NULL) return;

  if (pInfo->pHashSet) {
    tSimpleHashCleanup(pInfo->pHashSet);
  }
  if (pInfo->pEmitSet) {
    tSimpleHashCleanup(pInfo->pEmitSet);
  }
  if (pInfo->pReturnedBlock) {
    blockDataDestroy(pInfo->pReturnedBlock);
  }
  // pEmitBlock is always cleared when a block is handed off, so it never aliases pReturnedBlock;
  // the guard keeps that from becoming a double free if that ever changes.
  if (pInfo->pEmitBlock && pInfo->pEmitBlock != pInfo->pReturnedBlock) {
    blockDataDestroy(pInfo->pEmitBlock);
  }
  if (pInfo->pSortHandle) {
    tsortDestroySortHandle(pInfo->pSortHandle);
  }
  taosMemoryFreeClear(pInfo->prevSortKey);
  cleanupExprSupp(&pInfo->scalarSup);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosMemoryFreeClear(pInfo);
}

static int32_t doDistinctFilter(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SDistinctFilterInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  int32_t              code = TSDB_CODE_SUCCESS;

  // Spill emit mode — sorted dedup output
  if (pInfo->state == DISTINCT_STATE_SPILL_EMIT) {
    if (pInfo->hasInterval || pInfo->hasGroup) {
      return doDistinctFilterSpillEmitPartitioned(pOperator, ppRes);
    }
    return doDistinctFilterSpillEmit(pOperator, ppRes);
  }

  // INTERVAL: emission must be single-phase and globally ordered.
  //
  // The global path can safely emit deduplicated rows during the hash phase and
  // then again during the spill phase, because its upstream is a plain aggregate
  // that just counts all distinct rows regardless of order.
  //
  // INTERVAL is different: its upstream operator (Interval) closes a window as
  // soon as a later window is seen. A two-phase emission would present each
  // window twice (once per phase), so windows already closed by the hash-phase
  // output get dropped or merged when the spill phase replays them. To stay
  // correct we route all rows through the sort path from the start and emit a
  // single, globally ordered, window-tagged stream (the sort handle still
  // spills to disk when needed).
  //
  // GROUP BY does NOT use the sort path: the value-sort merges rows from many
  // source blocks into shared output pages, so the per-row group id is lost
  // (tsortGetGroupId only sees the merged block's id). Instead GROUP BY stays in
  // the hash path below and resets its dedup set whenever the group id changes.
  // This is correct because the upstream GroupAggregate requires groups to
  // arrive contiguously, which also bounds the dedup set to one group at a time.
  if (pInfo->hasInterval) {
    code = initSpillSortHandle(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      qError("DistinctFilter: initSpillSortHandle failed code=%s", tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
    pInfo->state = DISTINCT_STATE_SPILL_EMIT;
    return doDistinctFilterSpillEmitPartitioned(pOperator, ppRes);
  }

  while (1) {
    if (pOperator->status == OP_EXEC_DONE) {
      (*ppRes) = NULL;
      return code;
    }

    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      return code;
    }

    if (pBlock->info.rows == 0) {
      continue;
    }

    // GROUP BY: the dedup key already carries the block's group id (see buildDistinctKey), and for
    // a supertable scan that id is derived from the GROUP BY tag values, not the child table
    // (buildGroupIdMapForAllTables). So a single hash set keyed by (groupId, value) dedups every
    // group correctly and must NOT be reset when the group id changes: groups are not guaranteed
    // to arrive contiguously. Two child tables sharing a tag value have the same group id but can
    // live in different vgroups and arrive interleaved; resetting would let their duplicate values
    // through and inflate the count (e.g. count(distinct) returning 5 instead of 3).

    // Evaluate precalc scalar expressions (for expression-based distinct like c_int % 2)
    if (pInfo->scalarSup.pExprInfo != NULL) {
      SExprSupp* pSup = &pInfo->scalarSup;
      code = projectApplyFunctions(pSup->pExprInfo, pBlock, pBlock, pSup->pCtx, pSup->numOfExprs,
                                   NULL, GET_STM_RTINFO(pTaskInfo), pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    SColumnInfoData* pDistCol = taosArrayGet(pBlock->pDataBlock, pInfo->distinctColSlotId);
    if (pDistCol == NULL) {
      code = terrno;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // Get timestamp column if interval-aware dedup
    SColumnInfoData* pTsCol = NULL;
    if (pInfo->hasInterval) {
      pTsCol = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
      if (pTsCol == NULL) {
        code = terrno;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    int32_t numRows = pBlock->info.rows;
    bool*   pKeep = taosMemoryCalloc(numRows, sizeof(bool));
    if (pKeep == NULL) {
      code = terrno;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    int32_t keepCount = 0;
    bool    isVarType = IS_VAR_DATA_TYPE(pInfo->colType);

    for (int32_t i = 0; i < numRows; i++) {
      int32_t keyLen;

      if (pInfo->hasInterval || pInfo->hasGroup) {
        keyLen = buildDistinctKey(pInfo, pDistCol, pTsCol, pBlock->info.id.groupId, i, isVarType);
      } else {
        // Simple key: just the distinct column value (global dedup)
        int64_t nullMarker = 0;
        if (colDataIsNull_s(pDistCol, i)) {
          memcpy(pInfo->keyBuf, &nullMarker, isVarType ? sizeof(nullMarker) : pInfo->colBytes);
          keyLen = isVarType ? sizeof(nullMarker) : pInfo->colBytes;
        } else {
          const char* data = colDataGetData(pDistCol, i);
          keyLen = isVarType ? varDataTLen(data) : pInfo->colBytes;
          if (keyLen > pInfo->keyBufSize) {
            keyLen = pInfo->keyBufSize;
          }
          memcpy(pInfo->keyBuf, data, keyLen);
        }
      }

      void* exist = tSimpleHashGet(pInfo->pHashSet, pInfo->keyBuf, keyLen);
      if (exist == NULL) {
        int32_t dummy = 0;
        code = tSimpleHashPut(pInfo->pHashSet, pInfo->keyBuf, keyLen, &dummy, sizeof(dummy));
        if (code != TSDB_CODE_SUCCESS) {
          taosMemoryFree(pKeep);
          T_LONG_JMP(pTaskInfo->env, code);
        }
        pKeep[i] = true;
        keepCount++;
      }
    }

    // Check memory threshold after processing the full block. GROUP BY never spills: the
    // value-sort cannot preserve per-row group ids, so spilling would corrupt group boundaries.
    // Its dedup set therefore holds every group's distinct values for the whole scan.
    if (pInfo->state == DISTINCT_STATE_HASH && !pInfo->hasGroup) {
      size_t hashMem = tSimpleHashGetMemSize(pInfo->pHashSet);
      if (hashMem > (size_t)tsPQSortMemThreshold * 1024 * 1024) {
        qWarn("DistinctFilter: hash memory %zuMB exceeds threshold %dMB, switching to sort spill",
              hashMem / (1024 * 1024), tsPQSortMemThreshold);

        // Copy current block before sort init (tsortOpen overwrites downstream buffer)
        SSDataBlock* pCopy = NULL;
        if (keepCount > 0) {
          if (keepCount < numRows) {
            code = trimDataBlock(pBlock, numRows, pKeep);
            if (code != TSDB_CODE_SUCCESS) {
              taosMemoryFree(pKeep);
              T_LONG_JMP(pTaskInfo->env, code);
            }
          }
          code = createOneDataBlock(pBlock, true, &pCopy);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFree(pKeep);
            T_LONG_JMP(pTaskInfo->env, code);
          }
        }
        taosMemoryFree(pKeep);

        // Initialize sort handle — BLOCKING: pulls all remaining blocks from downstream
        code = initSpillSortHandle(pOperator);
        if (code != TSDB_CODE_SUCCESS) {
          blockDataDestroy(pCopy);
          qError("DistinctFilter: initSpillSortHandle failed code=%s", tstrerror(code));
          T_LONG_JMP(pTaskInfo->env, code);
        }
        qDebug("DistinctFilter: spill triggered, hashEntries=%d, switching to sort mode",
              tSimpleHashGetSize(pInfo->pHashSet));
        pInfo->state = DISTINCT_STATE_SPILL_EMIT;

        // Return copied block (may be NULL if keepCount==0). pCopy is ours: createOneDataBlock
        // allocated it and the consumer does not take ownership, so retain it for release on the
        // next call / on destroy.
        if (pCopy != NULL) {
          distinctSetReturnedBlock(pInfo, pCopy, ppRes);
        } else {
          (*ppRes) = NULL;
        }
        return code;
      }
    }

    if (keepCount == 0) {
      taosMemoryFree(pKeep);
      continue;
    }

    if (keepCount == numRows) {
      taosMemoryFree(pKeep);
      (*ppRes) = pBlock;
      return code;
    }

    // Trim block to keep only unique rows
    code = trimDataBlock(pBlock, numRows, pKeep);
    taosMemoryFree(pKeep);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    (*ppRes) = pBlock;
    return code;
  }
}

int32_t createDistinctFilterOperatorInfo(SOperatorInfo* downstream, SDistinctFilterPhysiNode* pPhyNode,
                                         SExecTaskInfo* pTaskInfo, SOperatorInfo** ppOptr) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SDistinctFilterInfo* pInfo = taosMemoryCalloc(1, sizeof(SDistinctFilterInfo));
  SOperatorInfo*       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->distinctColSlotId = pPhyNode->distinctColSlotId;
  pInfo->colType = pPhyNode->colType;
  pInfo->colBytes = pPhyNode->colBytes;
  pInfo->hasGroup = (pPhyNode->numGroupCols > 0);
  pInfo->hasInterval = pPhyNode->hasInterval;
  pInfo->state = DISTINCT_STATE_HASH;
  if (pInfo->hasInterval) {
    pInfo->tsSlotId = pPhyNode->tsSlotId;
    pInfo->interval.interval = pPhyNode->interval;
    pInfo->interval.offset = pPhyNode->offset;
    pInfo->interval.sliding = pPhyNode->sliding;
    pInfo->interval.intervalUnit = pPhyNode->intervalUnit;
    pInfo->interval.slidingUnit = pPhyNode->slidingUnit;
    pInfo->interval.precision = pPhyNode->precision;
    pInfo->interval.firstDayOfWeek = pPhyNode->firstDayOfWeek;
    pInfo->interval.timezone = pPhyNode->timezone;
  }

  // Initialize scalar expression support for expression-based distinct
  if (pPhyNode->pExprs != NULL) {
    SExprInfo* pScalarExprInfo = NULL;
    int32_t    numOfScalarExpr = 0;
    code = createExprInfo(pPhyNode->pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    if (code != TSDB_CODE_SUCCESS) goto _error;
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) goto _error;
  }

  // Allocate key buffer for composite keys. Size must accommodate the optional
  // interval window key + group id + null marker prefix plus the full column
  // value, which for VAR data types can reach TSDB_MAX_FIELD_LEN.
  int32_t keyPrefixLen = sizeof(int64_t) /* interval window start */ +
                         sizeof(uint64_t) /* group id */ + 1 /* null marker */;
  pInfo->keyBufSize = pInfo->colBytes + keyPrefixLen;
  if (pInfo->keyBufSize < 4096) {
    pInfo->keyBufSize = 4096;
  }
  pInfo->keyBuf = taosMemoryCalloc(1, pInfo->keyBufSize);
  if (pInfo->keyBuf == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->pHashSet = tSimpleHashInit(1024, MurmurHash3_32);
  if (pInfo->pHashSet == NULL) {
    code = terrno;
    goto _error;
  }

  // Per-partition dedup set used by the INTERVAL / GROUP BY spill emit path.
  if (pInfo->hasInterval || pInfo->hasGroup) {
    pInfo->pEmitSet = tSimpleHashInit(1024, MurmurHash3_32);
    if (pInfo->pEmitSet == NULL) {
      code = terrno;
      goto _error;
    }
  }

  setOperatorInfo(pOperator, "DistinctFilterOperator", QUERY_NODE_PHYSICAL_PLAN_DISTINCT_FILTER, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doDistinctFilter, NULL, destroyDistinctFilterOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *ppOptr = pOperator;
  return code;

_error:
  if (pInfo) destroyDistinctFilterOperator(pInfo);
  if (pOperator) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}
