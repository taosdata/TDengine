/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

// Values of swapColumnData in SSortOperatorInfo and SGroupSortOperatorInfo:
// whether the tuple block's payload buffers may be exchanged with the output
// block's instead of copied. Both operators cache the verdict per handle.
typedef enum ESortColDataSwap {
  SORT_COLDATA_UNKNOWN = 0,  // shape not compared yet
  SORT_COLDATA_SWAP = 1,     // payload buffers can be exchanged
  SORT_COLDATA_COPY = 2,     // payload must be copied
} ESortColDataSwap;

typedef struct SSortOpGroupIdCalc {
  STupleHandle* pSavedTuple;
  SArray*       pSortColsArr;
  char*         keyBuf;
  int32_t       lastKeysLen; // default to be 0
  uint64_t      lastGroupId;
  bool          excludePKCol;
} SSortOpGroupIdCalc;

typedef struct SSortOperatorInfo {
  SOptrBasicInfo      binfo;
  uint32_t            sortBufSize;  // max buffer size for in-memory sort
  SArray*             pSortInfo;
  SSortHandle*        pSortHandle;
  SColMatchInfo       matchInfo;
  int32_t             bufPageSize;
  int64_t             startTs;      // sort start time
  uint64_t            sortElapsed;  // sort elapsed time, time to flush to disk not included.
  SLimitInfo          limitInfo;
  uint64_t            maxTupleLength;
  int64_t             maxRows;
  bool                tsPkSort;
  bool                inputOrderedBySortKeys;
  SSortOpGroupIdCalc* pGroupIdCalc;
  // Tuple block reused across calls so its buffers cycle with the output block
  // instead of being allocated and freed for every output block.
  SSDataBlock*        pInterBlock;
  int8_t              swapColumnData;  // ESortColDataSwap value
} SSortOperatorInfo;

static int32_t doSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static int32_t doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);
static int32_t doGroupSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock);

static void destroySortOperatorInfo(void* param);
static void calcSortOperMaxTupleLength(SSortOperatorInfo* pSortOperInfo, SNodeList* pSortKeys);

// Composite primary keys are stored in (ts, pk) lexicographic order. Only this exact key shape is eligible for
// ordered-suffix validation in PQ sort; mixed directions and ordinary multi-column sorts retain the row-wise path.
// The predicate is shared with the planner's sortInputProvesFullOrder so the two decisions cannot drift.
static bool sortHasTsPkStorageOrder(SNodeList* pSortKeys) {
  return queryNodeSortKeysAreAscPrimTsPk(pSortKeys);
}

// Check whether the given slotId is the output of a Sort scalar pre-calculation expression.
// The planner's rewritePrecalcExprs pushes expression results (e.g. ts+1000) into extra slots
// in the child's output descriptor. This helper identifies such slots.
static bool sortIsExprResultSlot(const SOperatorInfo* pOperator, int32_t slotId) {
  if (pOperator == NULL || pOperator->exprSupp.pExprInfo == NULL || slotId < 0) {
    return false;
  }
  for (int32_t idx = 0; idx < pOperator->exprSupp.numOfExprs; ++idx) {
    if (pOperator->exprSupp.pExprInfo[idx].base.resSchema.slotId == slotId) {
      return true;
    }
  }
  return false;
}

// Find the slot index of the original (non-expression) primary timestamp column in the
// Sort's internal data block.  The internal block may contain both the original ts column
// and an expression-derived ts column (e.g. ts+1000).  We return the first TIMESTAMP
// column whose slot is NOT an expression result.
static int32_t sortFindOrigTsSlot(const SOperatorInfo* pOperator, const SSDataBlock* pBlock) {
  if (pBlock == NULL || pBlock->pDataBlock == NULL) {
    return -1;
  }
  int32_t colCount = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t idx = 0; idx < colCount; ++idx) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, idx);
    if (pCol != NULL && pCol->info.type == TSDB_DATA_TYPE_TIMESTAMP &&
        !sortIsExprResultSlot(pOperator, idx)) {
      return idx;
    }
  }
  return -1;
}

static void destroySortOpGroupIdCalc(SSortOpGroupIdCalc* pCalc);

static int32_t resetSortOperState(SOperatorInfo* pOper) {
  SSortOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  pOper->status = OP_NOT_OPENED;

  resetBasicOperatorState(&pInfo->binfo);
  destroySqlFunctionCtx(pOper->exprSupp.pCtx, pOper->exprSupp.pExprInfo, pOper->exprSupp.numOfExprs);
  taosMemoryFreeClear(pOper->exprSupp.rowEntryInfoOffset);
  pOper->exprSupp.pCtx =
      createSqlFunctionCtx(pOper->exprSupp.pExprInfo, pOper->exprSupp.numOfExprs, &pOper->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;

  // The cached tuple block was created from the old handle's output block, so a
  // new handle may describe a different tuple layout. Drop it so the next fill
  // rebuilds it from the handle actually in use.
  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;
  pInfo->swapColumnData = SORT_COLDATA_UNKNOWN;

  if (pInfo->pGroupIdCalc) {
    pInfo->pGroupIdCalc->lastGroupId = 0;
    pInfo->pGroupIdCalc->lastKeysLen = 0;
  }

  return 0;
}

// todo add limit/offset impl
int32_t createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t code = 0;
  int32_t lino = 0;

  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pTaskInfo = pTaskInfo;
  SDataBlockDescNode* pDescNode = pSortNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  code = createExprInfo(pSortNode->pExprs, NULL, &pOperator->exprSupp.pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  pOperator->exprSupp.numOfExprs = numOfCols;
  int32_t numOfOutputCols = 0;
  code =
      extractColMatchInfo(pSortNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  
  calcSortOperMaxTupleLength(pInfo, pSortNode->pSortKeys);
  pInfo->maxRows = -1;
  if (pSortNode->node.pLimit && ((SLimitNode*)pSortNode->node.pLimit)->limit) {
    SLimitNode* pLimit = (SLimitNode*)pSortNode->node.pLimit;
    if (pLimit->limit->datum.i > 0) {
      pInfo->maxRows = pLimit->limit->datum.i + (pLimit->offset ? pLimit->offset->datum.i : 0);
    }
  }

  pOperator->exprSupp.pCtx =
      createSqlFunctionCtx(pOperator->exprSupp.pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_NULL(pOperator->exprSupp.pCtx, code, lino, _error, terrno);
  initResultSizeInfo(&pOperator->resultInfo, 1024);
  code = filterInitFromNode((SNode*)pSortNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

  pInfo->pSortInfo = createSortInfo(pSortNode->pSortKeys);
  TSDB_CHECK_NULL(pInfo->pSortInfo, code, lino, _error, terrno);
  pInfo->tsPkSort = sortHasTsPkStorageOrder(pSortNode->pSortKeys);
  pInfo->inputOrderedBySortKeys = pSortNode->inputOrderedBySortKeys;

  if (pSortNode->calcGroupId) {
    int32_t keyLen;
    SSortOpGroupIdCalc* pGroupIdCalc = pInfo->pGroupIdCalc = taosMemoryCalloc(1, sizeof(SSortOpGroupIdCalc));
    if (!pGroupIdCalc) {
      code = terrno;
      goto _error;
    }
    SNodeList* pSortColsNodeArr = makeColsNodeArrFromSortKeys(pSortNode->pSortKeys);
    if (!pSortColsNodeArr) code = terrno;
    if (TSDB_CODE_SUCCESS == code) {
      pGroupIdCalc->pSortColsArr = makeColumnArrayFromList(pSortColsNodeArr);
      if (!pGroupIdCalc->pSortColsArr) code = terrno;
      nodesClearList(pSortColsNodeArr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      int32_t numOfSortKeys = taosArrayGetSize(pGroupIdCalc->pSortColsArr);
      int32_t numOfGroupKeys = pSortNode->numOfGroupKeys;
      if (numOfGroupKeys < 0 || numOfGroupKeys > numOfSortKeys) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _error;
      }
      if (numOfGroupKeys == 0) {
        // Plans without an explicit prefix retain the single-PK exclusion.
        numOfGroupKeys = numOfSortKeys - (pSortNode->excludePkCol ? 1 : 0);
      }
      // Only the original partition keys define group identity. Appended time
      // keys still participate in pSortInfo, including both timestamp and PK.
      while (numOfSortKeys > numOfGroupKeys) {
        taosArrayPop(pGroupIdCalc->pSortColsArr);
        --numOfSortKeys;
      }
      code = extractKeysLen(pGroupIdCalc->pSortColsArr, &keyLen);
      QUERY_CHECK_CODE(code, lino, _error);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pGroupIdCalc->lastKeysLen = 0;
      pGroupIdCalc->keyBuf = taosMemoryCalloc(1, keyLen);
      if (!pGroupIdCalc->keyBuf) {
        code = terrno;
      }
    }
  }
  if (code != TSDB_CODE_SUCCESS) goto _error;

  setOptrBasicInfoOrder(&pInfo->binfo, &pSortNode->node);
  initLimitInfo(pSortNode->node.pLimit, pSortNode->node.pSlimit, &pInfo->limitInfo);

  setOperatorInfo(pOperator, "SortOperator", QUERY_NODE_PHYSICAL_PLAN_SORT, true, OP_NOT_OPENED, pInfo, pTaskInfo);


  // lazy evaluation for the following parameter since the input datablock is not known till now.
  //  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;
  //  there are headers, so pageSize = rowSize + header pInfo->sortBufSize  = pInfo->bufPageSize * 16;
  // TODO dynamic set the available sort buffer

  pOperator->fpSet =
      createOperatorFpSet(doOpenSortOperator, doSort, NULL, destroySortOperatorInfo, optrDefaultBufFn, getExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetSortOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroySortOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

int32_t appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  int32_t      code = 0;
  int32_t      rows = pBlock->info.rows;
  const SSDataBlock* pSrcBlock = NULL;
  int32_t            srcRow = 0;
  tsortGetTuplePos(pTupleHandle, &pSrcBlock, &srcRow);
  if (pSrcBlock == NULL) {
    // A tuple handle always owns a source block; without one no value can be read.
    return TSDB_CODE_INVALID_PARA;
  }

  // This is an exported entry point, so it defends itself instead of trusting
  // every caller to have sized the destination first: one row is cheap to add
  // and the call is a no-op when the capacity already covers it.
  if (UNLIKELY(rows >= (int32_t)pBlock->info.capacity)) {
    code = blockDataEnsureCapacity(pBlock, (uint32_t)rows + 1);
    if (code) {
      return code;
    }
  }

  // No path can produce a value from a row outside the source block: both the
  // fast path and the tuple slow path address the block by this index, so an
  // out-of-range row is an error rather than something to hand downstream.
  if (srcRow < 0 || srcRow >= (int32_t)pSrcBlock->info.rows) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  // The fast path addresses the source by row index and column position, so it
  // is only valid while both blocks share the schema.
  bool fastRowOk = taosArrayGetSize(pSrcBlock->pDataBlock) == numOfCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfo == NULL) {
      return terrno;
    }

    // Same fixed-width / no-NULL fast path as the merge output: one memcpy per
    // value instead of the null lookup, tuple value fetch and type dispatch.
    SColumnInfoData* pSrcCol =
        fastRowOk ? (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, i) : NULL;
    if (colDataCanCopyFixed(pColInfo, pSrcCol)) {
      // Invariant for the block handed to the caller: hasNull may stay
      // conservatively set, but every row this path writes has its NULL bit
      // cleared, which is what the source predicate promised.
      if (pColInfo->nullbitmap != NULL) {
        colDataClearNull_f(pColInfo->nullbitmap, rows);
      } else {
        // No bitmap means no row ever carried a NULL bit (setting one needs a
        // bitmap), so a set flag is stale: drop it instead of leaving hasNull
        // out of step with the bitmap the column actually has.
        pColInfo->hasNull = false;
      }
      memcpy(pColInfo->pData + (size_t)rows * pColInfo->info.bytes,
             pSrcCol->pData + (size_t)srcRow * pColInfo->info.bytes, pColInfo->info.bytes);
      continue;
    }

    bool isNull = tsortIsNullVal(pTupleHandle, i);
    if (isNull) {
      colDataSetNULL(pColInfo, pBlock->info.rows);
    } else {
      char* pData = NULL;
      tsortGetValue(pTupleHandle, i, (void**) &pData);

      if (pData != NULL) {
        code = colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
        if (code) {
          return code;
        }
      }
    }
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.scanFlag = pSrcBlock->info.scanFlag;
  pBlock->info.rows += 1;
  return code;
}

// colDataAssign copies the payload of every column into the output block.  When
// the tuple block and the output block agree on the schema of every mapped
// column, the payload can be handed over by exchanging the column buffers
// instead: the tuple block keeps the previous output buffers, and because that
// block is reused across calls a steady-state sort neither copies the payload
// nor allocates new buffers.
static bool sortColumnDataSwappable(const SSDataBlock* pSrcBlock, const SSDataBlock* pDstBlock,
                                    const SArray* pColMatchInfo) {
  int32_t srcCols = (int32_t)taosArrayGetSize(pSrcBlock->pDataBlock);
  int32_t dstCols = (int32_t)taosArrayGetSize(pDstBlock->pDataBlock);
  int32_t numOfCols = (int32_t)taosArrayGetSize(pColMatchInfo);

  // Bitmaps of the slots already used on each side: an O(n) duplicate check
  // instead of rescanning every predecessor for each column. Both sides must
  // fit their stack arrays: srcBytes and dstBytes are sized independently, so
  // checking only one of them can overflow the other array.
  size_t  srcBytes = BitmapLen(TMAX(srcCols, 1));
  size_t  dstBytes = BitmapLen(TMAX(dstCols, 1));
  uint8_t srcUsed[64];
  uint8_t dstUsed[64];
  bool    onStack = srcBytes <= sizeof(srcUsed) && dstBytes <= sizeof(dstUsed);
  uint8_t* pAlloc = onStack ? NULL : taosMemoryCalloc(1, srcBytes + dstBytes);
  if (!onStack && pAlloc == NULL) {
    return false;
  }
  uint8_t* pSrcUsed = onStack ? srcUsed : pAlloc;
  uint8_t* pDstUsed = onStack ? dstUsed : pAlloc + srcBytes;
  if (onStack) {
    memset(srcUsed, 0, sizeof(srcUsed));
    memset(dstUsed, 0, sizeof(dstUsed));
  }

  bool swappable = true;
  for (int32_t i = 0; i < numOfCols; ++i) {
    const SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
    if (pmInfo == NULL || pmInfo->srcSlotId < 0 || pmInfo->srcSlotId >= srcCols || pmInfo->dstSlotId < 0 ||
        pmInfo->dstSlotId >= dstCols) {
      swappable = false;
      break;
    }

    const SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, pmInfo->srcSlotId);
    const SColumnInfoData* pDst = taosArrayGet(pDstBlock->pDataBlock, pmInfo->dstSlotId);
    if (pSrc == NULL || pDst == NULL) {
      swappable = false;
      break;
    }
    // DECIMAL keeps an unscaled integer plus a scale: same type and width with
    // a different scale is a different value, so the buffers cannot be swapped
    // (the same rule colDataCanCopyFixed applies).
    if (pSrc->info.type != pDst->info.type || pSrc->info.bytes != pDst->info.bytes ||
        pSrc->info.scale != pDst->info.scale) {
      swappable = false;
      break;
    }
    // A var column lives in two parts (offset/length/allocLen + payload) and the
    // exchange moves both, so each part must describe the other: a length that
    // exceeds its allocated payload would hand the receiving column a payload
    // it cannot address. Copying is always valid, so fall back to it.
    if (IS_VAR_DATA_TYPE(pSrc->info.type) &&
        (pSrc->varmeta.allocLen < pSrc->varmeta.length || pDst->varmeta.allocLen < pDst->varmeta.length)) {
      swappable = false;
      break;
    }

    if (BMIsNull(pSrcUsed, pmInfo->srcSlotId) || BMIsNull(pDstUsed, pmInfo->dstSlotId)) {
      // one source buffer cannot feed two output columns, and two sources
      // cannot feed one output column
      swappable = false;
      break;
    }
    BMCharPos(pSrcUsed, pmInfo->srcSlotId) |= (uint8_t)(1u << (7u - BitPos(pmInfo->srcSlotId)));
    BMCharPos(pDstUsed, pmInfo->dstSlotId) |= (uint8_t)(1u << (7u - BitPos(pmInfo->dstSlotId)));
  }

  if (!onStack) {
    taosMemoryFree(pAlloc);
  }
  return swappable;
}

// Re-validate a cached exchange verdict against the current pair of blocks.
// The verdict is computed from the handle's first output block; if the two
// layouts ever stop lining up (a rebuilt handle whose output block has another
// shape), a cached SWAP would hand mismatched columns to the output block.
// Demote to COPY, which is always correct and only slower, instead of trusting
// the stale verdict.
static int8_t sortRecheckSwapPlan(int8_t plan, const SSDataBlock* pSrc, const SSDataBlock* pDst,
                                  const SArray* pColMatchInfo) {
  if (plan == SORT_COLDATA_SWAP && !sortColumnDataSwappable(pSrc, pDst, pColMatchInfo)) {
    return SORT_COLDATA_COPY;
  }
  return plan;
}

// Hand the payload of pSrc over to pDst, leaving pSrc with the buffers pDst
// held before.  The observable state matches colDataAssign; only the copy of
// the payload is skipped.
static void sortSwapColumnData(SColumnInfoData* pDst, SColumnInfoData* pSrc) {
  char* pTmpData = pDst->pData;
  pDst->pData = pSrc->pData;
  pSrc->pData = pTmpData;

  if (IS_VAR_DATA_TYPE(pDst->info.type)) {
    // nullbitmap and varmeta are one union, and a var column stores NULL in
    // its offset array (-1 per NULL row), never in the bitmap bits. Swapping
    // varmeta therefore already moves the whole NULL state: swapping
    // nullbitmap as well would alias the offset pointer a second time and
    // leave length/allocLen describing the other block's payload.
    SVarColAttr tmpMeta = pDst->varmeta;
    pDst->varmeta = pSrc->varmeta;
    pSrc->varmeta = tmpMeta;
  } else {
    char* pTmpBitmap = pDst->nullbitmap;
    pDst->nullbitmap = pSrc->nullbitmap;
    pSrc->nullbitmap = pTmpBitmap;
  }

  int16_t slotId = pDst->info.slotId;
  pDst->info = pSrc->info;
  pDst->info.slotId = slotId;

  // The two columns traded their buffers, so they trade the flags describing
  // those buffers as well. Leaving pSrc with its old flag would describe a
  // bitmap/payload it no longer owns when the tuple block is reused.
  bool        tmpHasNull = pDst->hasNull;
  bool        tmpReassigned = pDst->reassigned;
  pDst->hasNull = pSrc->hasNull;
  pDst->reassigned = pSrc->reassigned;
  pSrc->hasNull = tmpHasNull;
  pSrc->reassigned = tmpReassigned;
}

// Reset the per-column state of a reused tuple block. blockDataCleanup() skips
// the columns when info.capacity is still 0 (a block that never got a fill),
// so the flags are cleared explicitly here; capacity is pinned before the block
// is filled again, which makes this the authoritative reset point.
static void sortResetInterBlockColumns(SSDataBlock* p) {
  size_t numOfCols = taosArrayGetSize(p->pDataBlock);
  for (size_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(p->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }
    pCol->hasNull = false;
    pCol->reassigned = false;
  }
}

/**
 * @brief get next tuple with group id attached, here assume that all tuples are sorted by group keys
 * @param [in, out] pBlock the output block, the group id will be saved in it
 * @retval NULL if next group tuple arrived and this new group tuple will be saved in pInfo.pSavedTuple
 */
static int32_t nextTupleWithGroupId(SSortHandle* pHandle, SSortOperatorInfo* pInfo, SSDataBlock* pBlock,
                                    STupleHandle** pTupleHandle) {
  QRY_PARAM_CHECK(pTupleHandle);

  int32_t       code = 0;
  STupleHandle* retTuple = pInfo->pGroupIdCalc->pSavedTuple;
  if (!retTuple) {
    code = tsortNextTuple(pHandle, &retTuple);
    if (code) {
      qError("failed to get next tuple, code:%s", tstrerror(code));
      return code;
    }
  }

  if (retTuple) {
    int32_t newGroup;
    if (pInfo->pGroupIdCalc->pSavedTuple) {
      newGroup = true;
      pInfo->pGroupIdCalc->pSavedTuple = NULL;
    } else {
      newGroup = tsortCompAndBuildKeys(pInfo->pGroupIdCalc->pSortColsArr, pInfo->pGroupIdCalc->keyBuf,
                                       &pInfo->pGroupIdCalc->lastKeysLen, retTuple);
      // A negative return is an error code from buildKeys (a key value wider than the
      // key buffer's declared width); fail the query instead of continuing.
      if (newGroup < 0) {
        return newGroup;
      }
    }

    bool emptyBlock = (pBlock->info.rows == 0);
    if (newGroup) {
      if (!emptyBlock) {
        // new group arrived, and we have already copied some tuples for cur group, save the new group tuple, return
        // NULL. Note that the keyBuf and lastKeysLen has been updated to new value
        pInfo->pGroupIdCalc->pSavedTuple = retTuple;
        retTuple = NULL;
      } else {
        // new group with empty block
        pInfo->pGroupIdCalc->lastGroupId = pBlock->info.id.groupId =
            calcGroupId(pInfo->pGroupIdCalc->keyBuf, pInfo->pGroupIdCalc->lastKeysLen);
      }
    } else {
      if (emptyBlock) {
        // new block but not new group, assign last group id to it
        pBlock->info.id.groupId = pInfo->pGroupIdCalc->lastGroupId;
      } else {
        // not new group and not empty block and ret NOT NULL, just return the tuple
      }
    }
  }

  *pTupleHandle = retTuple;
  return code;
}

static int32_t getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                  const SOperatorInfo* pOperator, SSortOperatorInfo* pInfo, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  blockDataCleanup(pDataBlock);

  int32_t       lino = 0;
  int32_t       code = 0;
  STupleHandle* pTupleHandle = NULL;
  SSDataBlock*  p = pInfo->pInterBlock;

  if (p == NULL) {
    code = tsortGetSortedDataBlock(pHandle, &p);
    if (p == NULL || (code != 0)) {
      return code;
    }
    pInfo->pInterBlock = p;
    pInfo->swapColumnData = sortColumnDataSwappable(p, pDataBlock, pColMatchInfo) ? SORT_COLDATA_SWAP
                                                                                : SORT_COLDATA_COPY;
  } else {
    blockDataCleanup(p);
    // The cached verdict only describes the pair it was computed from. Re-check
    // it so a layout change in either block degrades to COPY instead of
    // exchanging mismatched columns.
    pInfo->swapColumnData = sortRecheckSwapPlan(pInfo->swapColumnData, p, pDataBlock, pColMatchInfo);
  }

  code = blockDataEnsureCapacity(p, capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  // blockDataEnsureCapacity reallocates every column to numOfRows and only then
  // raises info.capacity, so the field read here is the bound the buffers were
  // actually ensured to. Keep it in a local: the exchange guard below must not
  // read back a value this function wrote itself.
  int32_t pEnsuredCap = (int32_t)p->info.capacity;
  // Pin the accounting fields to the capacity both blocks are guaranteed to
  // hold. blockDataEnsureCapacity() set pEnsuredCap >= capacity (or returned
  // an error), and the exchange below is guarded by both ensured capacities,
  // so each block leaves this function owning a buffer sized for at least
  // `capacity` rows. Do not write pEnsuredCap here: it may be larger than the
  // buffer the partner block was ensured to, and appendOneRowToDataBlock()
  // trusts this field as the real buffer bound.
  p->info.capacity = capacity;
  // blockDataCleanup() leaves the column flags alone when it sees capacity 0,
  // so clear them explicitly: the fill below starts from row 0 and must not
  // inherit NULL flags from the previous round.
  sortResetInterBlockColumns(p);

  while (1) {
    if (pInfo->pGroupIdCalc) {
      code = nextTupleWithGroupId(pHandle, pInfo, p, &pTupleHandle);
    } else {
      code = tsortNextTuple(pHandle, &pTupleHandle);
    }

    TSDB_CHECK_CODE(code, lino, _error);
    if (pTupleHandle == NULL) {
      break;
    }

    code = appendOneRowToDataBlock(p, pTupleHandle);
    QUERY_CHECK_CODE(code, lino, _error);

    if (p->info.rows >= capacity) {
      break;
    }
  }

  QUERY_CHECK_CODE(code, lino, _error);

  if (p->info.rows > 0) {
    code = blockDataEnsureCapacity(pDataBlock, capacity);
    QUERY_CHECK_CODE(code, lino, _error);
    int32_t dstEnsuredCap = (int32_t)pDataBlock->info.capacity;
    // Report the common guaranteed bound, not dstEnsuredCap: the buffers the
    // exchange below may bring in from pInterBlock are only guaranteed to hold
    // `capacity` rows, which both ensured capacities cover.
    pDataBlock->info.capacity = capacity;

    bool swapThisCall = (pInfo->swapColumnData == SORT_COLDATA_SWAP);

    // todo extract function to handle this
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
      QUERY_CHECK_NULL(pmInfo, code, lino, _error, terrno);

      int32_t srcSlotId = pmInfo->srcSlotId;

      // Fix: when the planner's setListSlotId resolves Sort pTargets by name, it may
      // mistakenly bind the primary timestamp column to the scalar expression result slot
      // (e.g. slot for ts+1000) instead of the original ts slot, because pushdownDataBlockSlots
      // added the expression slot with the same column name.  This causes the downstream
      // Project operator to apply the expression again (ts+1000 becomes ts+2000).
      // Detect this case and fall back to the original timestamp slot.
      if (pmInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID &&
          pmInfo->dataType.type == TSDB_DATA_TYPE_TIMESTAMP &&
          sortIsExprResultSlot(pOperator, srcSlotId)) {
        int32_t origSlot = sortFindOrigTsSlot(pOperator, p);
        if (origSlot >= 0 && origSlot != srcSlotId) {
          srcSlotId = origSlot;
          // A remapped slot means the exchange plan computed from
          // pmInfo->srcSlotId no longer describes this column set. Copy every
          // column of this block instead, so the tuple block never ends up
          // with a mix of exchanged and assigned buffers. Persist the decision
          // on the operator state as well: the slots stay remapped for every
          // later block, so re-entering the exchange path would compare the
          // wrong columns again.
          swapThisCall = false;
          pInfo->swapColumnData = SORT_COLDATA_COPY;
        }
      }

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, srcSlotId);
      QUERY_CHECK_NULL(pSrc, code, lino, _error, terrno);

      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      QUERY_CHECK_NULL(pDst, code, lino, _error, terrno);

      // The guard uses the capacities captured right after each block was
      // ensured, not the fields written below: both blocks were ensured to
      // `capacity` rows, and the exchange hands buffers of that size to the
      // other block.
      if (swapThisCall && srcSlotId == pmInfo->srcSlotId && pEnsuredCap >= capacity &&
          dstEnsuredCap >= capacity) {
        sortSwapColumnData(pDst, pSrc);
        code = TSDB_CODE_SUCCESS;
      } else {
        code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      }
      QUERY_CHECK_CODE(code, lino, _error);
    }

    pDataBlock->info.dataLoad = 1;
    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    // Propagate both C-group id and baseGId from upstream. blockDataCleanup()
    // zeroes groupId before every fill, and when the group-id calculator is
    // active nextTupleWithGroupId() writes the id of the group being emitted, so
    // the value read here is the current one and not a leftover from an earlier
    // fill. Without a calculator groupId stays 0, which downstream treats as
    // informational for a plain Sort.
    pDataBlock->info.id.groupId = p->info.id.groupId;
    pDataBlock->info.id.baseGId = p->info.id.baseGId;
  }

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;

  _error:
  // A failure can leave the cached block half filled or holding a mix of
  // exchanged and copied columns. Every buffer is still owned by exactly one
  // block (the exchange moves buffers, it never frees), so dropping the block
  // releases only what it owns; the next call then rebuilds it from the
  // handle instead of reusing state no query path validated.
  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;
  pInfo->swapColumnData = SORT_COLDATA_UNKNOWN;
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));

  return code;
}

int32_t loadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  int32_t        code = getNextBlockFromDownstreamImpl(pOperator, 0, false, ppBlock);
  if (code) {
    qError("failed to get next data block from upstream, %s code:%s", __func__, tstrerror(code));
  } else {
    code = blockDataCheck(*ppBlock);
    if (code) {
      qError("failed to check block data, %s code:%s", __func__, tstrerror(code));
    }
  }
  return code;
}

// todo refactor: merged with fetch fp
void applyScalarFunction(SSDataBlock* pBlock, void* param) {
  SOperatorInfo*     pOperator = param;
  SSortOperatorInfo* pSort = pOperator->info;
  if (pOperator->exprSupp.pExprInfo != NULL && pOperator->exprSupp.numOfExprs > 0) {
    int32_t code = projectApplyFunctions(pOperator->exprSupp.pExprInfo, pBlock, pBlock, pOperator->exprSupp.pCtx,
                                         pOperator->exprSupp.numOfExprs, NULL,
                                         GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    // Scalar pre-calculation materializes reserved expression slots in place.
    // Mark them as data-bearing so sort does not treat every key as NULL.
    for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
      int32_t          slotId = pOperator->exprSupp.pExprInfo[i].base.resSchema.slotId;
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      if (pCol != NULL && pBlock->info.rows > 0) {
        pCol->info.noData = false;
      }
    }
  }
}

int32_t doOpenSortOperator(SOperatorInfo* pOperator) {
  SSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSortSource* pSource =NULL;

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    filterSetExecContext(pOperator->exprSupp.pFilterInfo, pTaskInfo, isTaskKilled);
  }

  if (OPTR_IS_OPENED(pOperator)) {
    return code;
  }

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pSortHandle = NULL;
  code =
      tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str, pInfo->maxRows,
                            pInfo->maxTupleLength, tsPQSortMemThreshold * 1024 * 1024, &pInfo->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _end);

  // Both flags are derived from the plan-time sort keys. Slot rewriting can
  // change the runtime column behind a sort slot, so the sort handle re-checks
  // the actual block columns before short-circuiting: tsortPlanOrderStillValid
  // for the plan-proven path and tsortShortCircuitEligible for the threshold
  // path. Trace the decision here so a lost optimization is diagnosable.
  if (pInfo->tsPkSort) {
    tsortSetTsPkSort(pInfo->pSortHandle);
  }
  if (pInfo->inputOrderedBySortKeys) {
    tsortSetInputOrdered(pInfo->pSortHandle);
  }
  qTrace("%s sort flags, tsPkSort:%d inputOrderedBySortKeys:%d", pTaskInfo->id.str, pInfo->tsPkSort,
         pInfo->inputOrderedBySortKeys);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, applyScalarFunction, pOperator);

  pSource = taosMemoryCalloc(1, sizeof(SSortSource));
  QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);

  pSource->param = pOperator;
  pSource->onlyRef = true;

  code = tsortAddSource(pInfo->pSortHandle, pSource);
  QUERY_CHECK_CODE(code, lino, _end);
  pSource = NULL;

  code = tsortOpen(pInfo->pSortHandle);
  QUERY_CHECK_CODE(code, lino, _end);
  pOperator->status = OP_RES_TO_RETURN;
  OPTR_SET_OPENED(pOperator);

_end:
  if (pSource) {
    taosMemoryFree(pSource);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t doSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  // multi-group case not handle here
  SSDataBlock* pBlock = NULL;
  while (1) {
    if (tsortIsClosed(pInfo->pSortHandle)) {
      code = TSDB_CODE_TSC_QUERY_CANCELLED;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    recordOpExecBeforeDownstream(pOperator);
    code = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                pInfo->matchInfo.pList, pOperator, pInfo, &pBlock);
    recordOpExecAfterDownstream(pOperator, pBlock ? pBlock->info.rows : 0);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBlock == NULL) {
      setOperatorCompleted(pOperator);
      return code;
    }

    code = doFilterWithTaskCorrelation(pBlock, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo,
                                       NULL, pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (blockDataGetNumOfRows(pBlock) == 0) {
      continue;
    }

    // there are bugs?
    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
    if (limitReached) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    if (pBlock->info.rows > 0) {
      break;
    }
  }

  *pResBlock = blockDataGetNumOfRows(pBlock) > 0 ? pBlock : NULL;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

void destroySortOperatorInfo(void* param) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  blockDataDestroy(pInfo->binfo.pRes);
  pInfo->binfo.pRes = NULL;

  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);
  destroySortOpGroupIdCalc(pInfo->pGroupIdCalc);
  taosMemoryFreeClear(param);
}

int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));
  if (pInfo == NULL) {
    return terrno;
  }

  SSortOperatorInfo* pOperatorInfo = (SSortOperatorInfo*)pOptr->info;

  *pInfo = tsortGetSortExecInfo(pOperatorInfo->pSortHandle);
  *pOptrExplain = pInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

static void calcSortOperMaxTupleLength(SSortOperatorInfo* pSortOperInfo, SNodeList* pSortKeys) {
  SColMatchInfo* pColItem = &pSortOperInfo->matchInfo;
  size_t         size = taosArrayGetSize(pColItem->pList);
  for (size_t i = 0; i < size; ++i) {
    SColMatchItem* pInfo = taosArrayGet(pColItem->pList, i);
    if (pInfo == NULL) {
      continue;
    }

    pSortOperInfo->maxTupleLength += pInfo->dataType.bytes;
  }

  SNode* pSortKeyNode = NULL;
  FOREACH(pSortKeyNode, pSortKeys) {
    SOrderByExprNode* pOrderExprNode = (SOrderByExprNode*)pSortKeyNode;
    pSortOperInfo->maxTupleLength += ((SColumnNode*)pOrderExprNode->pExpr)->node.resType.bytes;
  }
}

static void destroySortOpGroupIdCalc(SSortOpGroupIdCalc* pCalc) {
  if (pCalc) {
    taosArrayDestroy(pCalc->pSortColsArr);
    taosMemoryFree(pCalc->keyBuf);
    taosMemoryFree(pCalc);
  }
}

//=====================================================================================
// Group Sort Operator
typedef enum EChildOperatorStatus { CHILD_OP_NEW_GROUP, CHILD_OP_SAME_GROUP, CHILD_OP_FINISHED } EChildOperatorStatus;

typedef struct SGroupSortOperatorInfo {
  SOptrBasicInfo       binfo;
  SArray*              pSortInfo;
  SColMatchInfo        matchInfo;
  int64_t              startTs;
  uint64_t             sortElapsed;
  bool                 hasGroupId;
  uint64_t             currGroupId;
  uint64_t             currBaseGId;
  SSDataBlock*         prefetchedSortInput;
  SSortHandle*         pCurrSortHandle;
  EChildOperatorStatus childOpStatus;
  SLimitInfo           limitInfo;
  SSortExecInfo        sortExecInfo;
  // Tuple block reused across calls (see SSortOperatorInfo.pInterBlock).
  SSDataBlock*         pInterBlock;
  int8_t               swapColumnData;  // ESortColDataSwap value
} SGroupSortOperatorInfo;

// Contract of the returned block: info.rows is the number of valid rows and
// info.capacity is the row bound both the output block and the reused tuple
// block are guaranteed to hold (both were ensured to `capacity`). capacity is a
// buffer upper bound, never the row count reported downstream.
int32_t getGroupSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                SGroupSortOperatorInfo* pInfo, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);

  blockDataCleanup(pDataBlock);
  int32_t code = blockDataEnsureCapacity(pDataBlock, capacity);
  if (code) {
    return code;
  }
  int32_t dstEnsuredCap = (int32_t)pDataBlock->info.capacity;

  SSDataBlock* p = pInfo->pInterBlock;
  if (p == NULL) {
    code = tsortGetSortedDataBlock(pHandle, &p);
    if (p == NULL || (code != 0)) {
      return code;
    }
    pInfo->pInterBlock = p;
    pInfo->swapColumnData = sortColumnDataSwappable(p, pDataBlock, pColMatchInfo) ? SORT_COLDATA_SWAP
                                                                                : SORT_COLDATA_COPY;
  } else {
    blockDataCleanup(p);
    // Same stale-verdict guard as the plain Sort path.
    pInfo->swapColumnData = sortRecheckSwapPlan(pInfo->swapColumnData, p, pDataBlock, pColMatchInfo);
  }

  code = blockDataEnsureCapacity(p, capacity);
  if (code) {
    return code;
  }
  int32_t pEnsuredCap = (int32_t)p->info.capacity;
  p->info.capacity = capacity;
  // Same explicit column reset as the plain Sort path above.
  sortResetInterBlockColumns(p);

  bool swapThisCall = (pInfo->swapColumnData == SORT_COLDATA_SWAP);

  while (1) {
    STupleHandle* pTupleHandle = NULL;
    code = tsortNextTuple(pHandle, &pTupleHandle);
    if (pTupleHandle == NULL || code != 0) {
      break;
    }

    code = appendOneRowToDataBlock(p, pTupleHandle);
    if (code) {
      break;
    }

    if (p->info.rows >= capacity) {
      break;
    }
  }

  if (p->info.rows > 0) {
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
      if (pmInfo == NULL) {
        return terrno;
      }

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      if (pSrc == NULL) {
        return terrno;
      }

      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      if (pDst == NULL) {
        return terrno;
      }

      // Same capacity invariant and reasoning as the plain Sort path; the
      // group path has no timestamp-slot remap, so no extra slot guard. The
      // bound comes from the ensure call above, not from the fields pinned
      // afterwards.
      if (swapThisCall && pEnsuredCap >= capacity && dstEnsuredCap >= capacity) {
        sortSwapColumnData(pDst, pSrc);
        code = TSDB_CODE_SUCCESS;
      } else {
        code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      }
      if (code) {
        return code;
      }
    }

    pDataBlock->info.rows = p->info.rows;
    // The column buffers were ensured to `capacity` at entry, so report that
    // bound rather than the row count: reporting rows here would make the next
    // call reallocate every column and fail the buffer-exchange guard.
    pDataBlock->info.capacity = capacity;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    // propagate ids for the current group
    pDataBlock->info.id.groupId = pInfo->currGroupId;
    pDataBlock->info.id.baseGId = pInfo->currBaseGId;
  }

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

typedef struct SGroupSortSourceParam {
  SOperatorInfo*          childOpInfo;
  SGroupSortOperatorInfo* grpSortOpInfo;
} SGroupSortSourceParam;

int32_t fetchNextGroupSortDataBlock(void* param, SSDataBlock** ppBlock) {
  int32_t                 code = 0;
  int32_t                 lino = 0;
  SGroupSortSourceParam*  source = param;
  SGroupSortOperatorInfo* grpSortOpInfo = source->grpSortOpInfo;
  SSDataBlock*            block = NULL;

  QRY_PARAM_CHECK(ppBlock);

  if (grpSortOpInfo->prefetchedSortInput) {
    block = grpSortOpInfo->prefetchedSortInput;
    grpSortOpInfo->prefetchedSortInput = NULL;
    *ppBlock = block;
  } else {
    SOperatorInfo* childOp = source->childOpInfo;
    code = childOp->fpSet.getNextFn(childOp, &block);
    QUERY_CHECK_CODE(code, lino, _end);

    if (block != NULL) {
      code = blockDataCheck(block);
      QUERY_CHECK_CODE(code, lino, _end);
      if (block->info.id.groupId == grpSortOpInfo->currGroupId) {
        grpSortOpInfo->childOpStatus = CHILD_OP_SAME_GROUP;
        *ppBlock = block;
      } else {
        grpSortOpInfo->childOpStatus = CHILD_OP_NEW_GROUP;
        grpSortOpInfo->prefetchedSortInput = block;
      }
    } else {
      grpSortOpInfo->childOpStatus = CHILD_OP_FINISHED;
    }
  }

  return code;
_end:
  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t beginSortGroup(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pCurrSortHandle = NULL;

  // Each group gets a new sort handle, whose output block may describe a
  // different tuple layout than the one the cached block was created from.
  // Drop the cache so it is rebuilt from this handle on first use.
  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;
  pInfo->swapColumnData = SORT_COLDATA_UNKNOWN;

  int32_t code = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str, 0, 0,
                                       0, &pInfo->pCurrSortHandle);
  if (code) {
    return code;
  }

  tsortSetFetchRawDataFp(pInfo->pCurrSortHandle, fetchNextGroupSortDataBlock, applyScalarFunction, pOperator);

  SSortSource*           ps = taosMemoryCalloc(1, sizeof(SSortSource));
  SGroupSortSourceParam* param = taosMemoryCalloc(1, sizeof(SGroupSortSourceParam));
  if (ps == NULL || param == NULL) {
    taosMemoryFree(ps);
    taosMemoryFree(param);
    return terrno;
  }

  param->childOpInfo = pOperator->pDownstream[0];
  param->grpSortOpInfo = pInfo;

  ps->param = param;
  ps->onlyRef = false;
  code = tsortAddSource(pInfo->pCurrSortHandle, ps);
  if (code != 0) {
    return code;
  }

  code = tsortOpen(pInfo->pCurrSortHandle);
  return code;
}

int32_t finishSortGroup(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;

  SSortExecInfo sortExecInfo = tsortGetSortExecInfo(pInfo->pCurrSortHandle);

  pInfo->sortExecInfo.sortMethod = sortExecInfo.sortMethod;
  pInfo->sortExecInfo.sortBuffer = sortExecInfo.sortBuffer;
  pInfo->sortExecInfo.loops += sortExecInfo.loops;
  pInfo->sortExecInfo.readBytes += sortExecInfo.readBytes;
  pInfo->sortExecInfo.writeBytes += sortExecInfo.writeBytes;

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t advanceGroupSort(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;

  code = finishSortGroup(pOperator);
  QUERY_CHECK_CODE(code, lino, _return);

  resetLimitInfoForNextGroup(&pInfo->limitInfo);
  switch (pInfo->childOpStatus) {
    case CHILD_OP_NEW_GROUP:
      pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
      pInfo->currBaseGId = pInfo->prefetchedSortInput->info.id.baseGId;
      code = beginSortGroup(pOperator);
      QUERY_CHECK_CODE(code, lino, _return);
      break;
    case CHILD_OP_FINISHED:
      setOperatorCompleted(pOperator);
      break;
    default:
      break;
  }

_return:
  if (TSDB_CODE_SUCCESS != code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doGroupSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SGroupSortOperatorInfo* pInfo = pOperator->info;
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;

  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    pInfo->prefetchedSortInput = getNextBlockFromDownstream(pOperator, 0);
    if (pInfo->prefetchedSortInput == NULL) {
      setOperatorCompleted(pOperator);
      return code;
    }

    pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
    pInfo->currBaseGId = pInfo->prefetchedSortInput->info.id.baseGId;
    pInfo->childOpStatus = CHILD_OP_NEW_GROUP;
    code = beginSortGroup(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->pCurrSortHandle != NULL) {
    if (tsortIsClosed(pInfo->pCurrSortHandle)) {
      code = TSDB_CODE_TSC_QUERY_CANCELLED;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // beginSortGroup would fetch all child blocks of pInfo->currGroupId;
    if (pInfo->childOpStatus == CHILD_OP_SAME_GROUP) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    recordOpExecBeforeDownstream(pOperator);
    code = getGroupSortedBlockData(pInfo->pCurrSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                     pInfo->matchInfo.pList, pInfo, &pBlock);
    recordOpExecAfterDownstream(pOperator, pBlock ? pBlock->info.rows : 0);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBlock != NULL) {
      // keep both ids aligned with current group
      pBlock->info.id.groupId = pInfo->currGroupId;
      pBlock->info.id.baseGId = pInfo->currBaseGId;
      // baseGId follows upstream; if upstream is empty here, preserve current
      // (no-op if not set). We cannot reconstruct baseGId here; rely on upstream propagation.
      bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      if (limitReached) {
        code = advanceGroupSort(pOperator);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (pBlock->info.rows > 0) {
        *pResBlock = pBlock;
        return code;
      }
    } else {
      if (pInfo->childOpStatus != CHILD_OP_SAME_GROUP) {
        code = advanceGroupSort(pOperator);
        QUERY_CHECK_CODE(code, lino, _end);
        if (pOperator->status == OP_EXEC_DONE) {
          return code;
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

int32_t getGroupSortExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SGroupSortOperatorInfo* pInfo = (SGroupSortOperatorInfo*)pOptr->info;
  *pOptrExplain = &pInfo->sortExecInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

void destroyGroupSortOperatorInfo(void* param) {
  SGroupSortOperatorInfo* pInfo = (SGroupSortOperatorInfo*)param;
  blockDataDestroy(pInfo->binfo.pRes);
  pInfo->binfo.pRes = NULL;

  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  taosMemoryFreeClear(param);
}

static int32_t resetGroupSortOperState(SOperatorInfo* pOper) {
  SGroupSortOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  pOper->status = OP_NOT_OPENED;

  pInfo->currGroupId = 0;
  pInfo->hasGroupId = false;
  pInfo->prefetchedSortInput = NULL;
  pInfo->childOpStatus = CHILD_OP_NEW_GROUP;
  pInfo->sortExecInfo = (SSortExecInfo){0};
  resetLimitInfoForNextGroup(&pInfo->limitInfo);
  
  resetBasicOperatorState(&pInfo->binfo);
  destroySqlFunctionCtx(pOper->exprSupp.pCtx, pOper->exprSupp.pExprInfo, pOper->exprSupp.numOfExprs);
  taosMemoryFreeClear(pOper->exprSupp.rowEntryInfoOffset);
  pOper->exprSupp.pCtx =
      createSqlFunctionCtx(pOper->exprSupp.pExprInfo, pOper->exprSupp.numOfExprs, &pOper->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  // Same reason as resetSortOperState: the next open builds a new handle, so
  // the cached tuple block must not outlive the handle it came from.
  blockDataDestroy(pInfo->pInterBlock);
  pInfo->pInterBlock = NULL;
  pInfo->swapColumnData = SORT_COLDATA_UNKNOWN;

  return 0;
}

int32_t createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode,
                                    SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;

  SGroupSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupSortOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  SExprSupp*          pSup = &pOperator->exprSupp;
  SDataBlockDescNode* pDescNode = pSortPhyNode->node.pOutputDataBlockDesc;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pSortPhyNode->pExprs, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  pSup->pExprInfo = pExprInfo;
  pSup->numOfExprs = numOfCols;

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pOperator->exprSupp.pCtx = createSqlFunctionCtx(pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset,
                                                  &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_NULL(pOperator->exprSupp.pCtx, code, lino, _error, terrno);

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  TSDB_CHECK_CODE(code, lino, _error);

  setOptrBasicInfoOrder(&pInfo->binfo, &pSortPhyNode->node);

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                             &pInfo->matchInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  initLimitInfo(pSortPhyNode->node.pLimit, pSortPhyNode->node.pSlimit, &pInfo->limitInfo);
  setOperatorInfo(pOperator, "GroupSortOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doGroupSort, NULL, destroyGroupSortOperatorInfo,
                                         optrDefaultBufFn, getGroupSortExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetGroupSortOperState);
                                         
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  pTaskInfo->code = code;
  if (pInfo != NULL) {
    destroyGroupSortOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  return code;
}
