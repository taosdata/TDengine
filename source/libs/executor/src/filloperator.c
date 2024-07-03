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

#include "filter.h"
#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "function.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "tfill.h"
#include "operator.h"
#include "querytask.h"

typedef struct STimeRange {
  TSKEY    skey;
  TSKEY    ekey;
  uint64_t groupId;
} STimeRange;

typedef struct SFillOperatorInfo {
  struct SFillInfo* pFillInfo;
  SSDataBlock*      pRes;
  SSDataBlock*      pFinalRes;
  int64_t           totalInputRows;
  void**            p;
  SSDataBlock*      existNewGroupBlock;
  STimeWindow       win;
  SColMatchInfo     matchInfo;
  int32_t           primaryTsCol;
  int32_t           primarySrcSlotId;
  uint64_t          curGroupId;  // current handled group id
  SExprInfo*        pExprInfo;
  int32_t           numOfExpr;
  SExprSupp         noFillExprSupp;
} SFillOperatorInfo;

static void revisedFillStartKey(SFillOperatorInfo* pInfo, SSDataBlock* pBlock, int32_t order);
static void destroyFillOperatorInfo(void* param);
static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);
static void fillResetPrevForNewGroup(SFillInfo* pFillInfo);

static void doHandleRemainBlockForNewGroupImpl(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                               SResultInfo* pResultInfo, int32_t order) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

//  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
//  getTableScanInfo(pOperator, &order, &scanFlag, false);
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  blockDataCleanup(pInfo->pRes);
  doApplyScalarCalculation(pOperator, pInfo->existNewGroupBlock, order, scanFlag);

  revisedFillStartKey(pInfo, pInfo->existNewGroupBlock, order);

  int64_t ts = (order == TSDB_ORDER_ASC)? pInfo->existNewGroupBlock->info.window.ekey:pInfo->existNewGroupBlock->info.window.skey;
  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ts);

  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
  if (pInfo->pFillInfo->type == TSDB_FILL_PREV || pInfo->pFillInfo->type == TSDB_FILL_LINEAR) {
    fillResetPrevForNewGroup(pInfo->pFillInfo);
  }

  int32_t numOfResultRows = pResultInfo->capacity - pResBlock->info.rows;
  taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);

  pInfo->curGroupId = pInfo->existNewGroupBlock->info.id.groupId;
  pInfo->existNewGroupBlock = NULL;
}

static void doHandleRemainBlockFromNewGroup(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                            SResultInfo* pResultInfo, int32_t order) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    int32_t numOfResultRows = pResultInfo->capacity - pInfo->pFinalRes->info.rows;
    taosFillResultDataBlock(pInfo->pFillInfo, pInfo->pFinalRes, numOfResultRows);
    pInfo->pRes->info.id.groupId = pInfo->curGroupId;
    return;
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, order);
  }
}

void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*         pSup = &pOperator->exprSupp;
  setInputDataBlock(pSup, pBlock, order, scanFlag, false);
  projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs, NULL);

  // reset the row value before applying the no-fill functions to the input data block, which is "pBlock" in this case.
  pInfo->pRes->info.rows = 0;
  SExprSupp* pNoFillSupp = &pInfo->noFillExprSupp;
  setInputDataBlock(pNoFillSupp, pBlock, order, scanFlag, false);

  projectApplyFunctions(pNoFillSupp->pExprInfo, pInfo->pRes, pBlock, pNoFillSupp->pCtx, pNoFillSupp->numOfExprs, NULL);
  pInfo->pRes->info.id.groupId = pBlock->info.id.groupId;
}

static void fillResetPrevForNewGroup(SFillInfo* pFillInfo) {
  for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
    if (!pFillInfo->pFillCol[colIdx].notFillCol) {
      SGroupKeys* key = taosArrayGet(pFillInfo->prev.pRowVal, colIdx);
      key->isNull = true;
    }
  }
}

// todo refactor: decide the start key according to the query time range.
static void revisedFillStartKey(SFillOperatorInfo* pInfo, SSDataBlock* pBlock, int32_t order) {
  if (order == TSDB_ORDER_ASC) {
    int64_t skey = pBlock->info.window.skey;
    if (skey < pInfo->pFillInfo->start) {  // the start key may be smaller than the
      ASSERT( taosFillNotStarted(pInfo->pFillInfo));
      taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, skey);
    } else if (pInfo->pFillInfo->start < skey) {
      int64_t t = skey;
      SInterval* pInterval = &pInfo->pFillInfo->interval;

      while(1) {
        int64_t prev = taosTimeAdd(t, -pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
        if (prev <= pInfo->pFillInfo->start) {
          t = prev;
          break;
        }
        t = prev;
      }

      // todo time window chosen problem: t or prev value?
      taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, t);
    }
  } else {
    int64_t ekey = pBlock->info.window.ekey;
    if (ekey > pInfo->pFillInfo->start) {
      ASSERT( taosFillNotStarted(pInfo->pFillInfo));
      taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, ekey);
    } else if (ekey < pInfo->pFillInfo->start) {
      int64_t t = ekey;
      SInterval* pInterval = &pInfo->pFillInfo->interval;
      int64_t    prev = t;
      while(1) {
        int64_t next = taosTimeAdd(t, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
        if (next >= pInfo->pFillInfo->start) {
          prev = t;
          t = next;
          break;
        }
        prev = t;
        t = next;
      }

      // todo time window chosen problem: t or next value?
      if (t > pInfo->pFillInfo->start) t = prev;
      taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, t);
    }
  }
}

static SSDataBlock* doFillImpl(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

  blockDataCleanup(pResBlock);

  int32_t order = pInfo->pFillInfo->order;

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
#if 0
  // the scan order may be different from the output result order for agg interval operator.
  if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL) {
    order = ((SIntervalAggOperatorInfo*) pDownstream->info)->resultTsOrder;
  } else {
    order = pInfo->pFillInfo->order;
  }
#endif

  doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
  if (pResBlock->info.rows > 0) {
    pResBlock->info.id.groupId = pInfo->curGroupId;
    return pResBlock;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0 &&
          (pInfo->pFillInfo->type != TSDB_FILL_NULL_F && pInfo->pFillInfo->type != TSDB_FILL_SET_VALUE_F)) {
        setOperatorCompleted(pOperator);
        return NULL;
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
    } else {
      pResBlock->info.scanFlag = pBlock->info.scanFlag;
      pBlock->info.dataLoad = 1;
      blockDataUpdateTsWindow(pBlock, pInfo->primarySrcSlotId);

      blockDataCleanup(pInfo->pRes);
      blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
      blockDataEnsureCapacity(pInfo->pFinalRes, pBlock->info.rows);
      doApplyScalarCalculation(pOperator, pBlock, order, pBlock->info.scanFlag);

      if (pInfo->curGroupId == 0 || (pInfo->curGroupId == pInfo->pRes->info.id.groupId)) {
        if (pInfo->curGroupId == 0 && taosFillNotStarted(pInfo->pFillInfo)) {
          revisedFillStartKey(pInfo, pBlock, order);
        }

        pInfo->curGroupId = pInfo->pRes->info.id.groupId;  // the first data block
        pInfo->totalInputRows += pInfo->pRes->info.rows;

        int64_t ts = (order == TSDB_ORDER_ASC)? pBlock->info.window.ekey:pBlock->info.window.skey;
        taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ts);
        taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
      } else if (pInfo->curGroupId != pBlock->info.id.groupId) {  // the new group data block
        pInfo->existNewGroupBlock = pBlock;

        // Fill the previous group data block, before handle the data block of new group.
        // Close the fill operation for previous group data block
        taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
        pInfo->pFillInfo->prev.key = 0;
      }
    }

    int32_t numOfResultRows = pOperator->resultInfo.capacity - pResBlock->info.rows;
    taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);

    // current group has no more result to return
    if (pResBlock->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pResBlock->info.rows > pResultInfo->threshold || pBlock == NULL || pInfo->existNewGroupBlock != NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
      if (pResBlock->info.rows < pResultInfo->threshold) {
        assert(!taosFillHasMoreResults(pInfo->pFillInfo));
      }

      doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows >= pOperator->resultInfo.threshold || pBlock == NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(0);
      blockDataCleanup(pResBlock);

      doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows > pResultInfo->threshold) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else {
      return NULL;
    }
  }
}

static SSDataBlock* doFill(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SSDataBlock* fillResult = NULL;
  while (true) {
    fillResult = doFillImpl(pOperator);
    if (fillResult == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }

    doFilter(fillResult, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    if (fillResult->info.rows > 0) {
      break;
    }
  }

  if (fillResult != NULL) {
    pOperator->resultInfo.totalRows += fillResult->info.rows;
  }

  return fillResult;
}

void destroyFillOperatorInfo(void* param) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*)param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  pInfo->pFinalRes = blockDataDestroy(pInfo->pFinalRes);

  cleanupExprSupp(&pInfo->noFillExprSupp);

  taosMemoryFreeClear(pInfo->p);
  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(param);
}

static int32_t initFillInfo(SFillOperatorInfo* pInfo, SExprInfo* pExpr, int32_t numOfCols, SExprInfo* pNotFillExpr,
                            int32_t numOfNotFillCols, SNodeListNode* pValNode, STimeWindow win, int32_t capacity,
                            const char* id, SInterval* pInterval, int32_t fillType, int32_t order) {
  SFillColInfo* pColInfo = createFillColInfo(pExpr, numOfCols, pNotFillExpr, numOfNotFillCols, pValNode);

  int64_t startKey = (order == TSDB_ORDER_ASC) ? win.skey : win.ekey;

//  STimeWindow w = {0};
//  getInitialStartTimeWindow(pInterval, startKey, &w, order == TSDB_ORDER_ASC);
  pInfo->pFillInfo = taosCreateFillInfo(startKey, numOfCols, numOfNotFillCols, capacity, pInterval, fillType, pColInfo,
                                        pInfo->primaryTsCol, order, id);

  if (order == TSDB_ORDER_ASC) {
    pInfo->win.skey = win.skey;
    pInfo->win.ekey = win.ekey;
  } else {
    pInfo->win.skey = win.ekey;
    pInfo->win.ekey = win.skey;
  }
  pInfo->p = taosMemoryCalloc(numOfCols, POINTER_BYTES);

  if (pInfo->pFillInfo == NULL || pInfo->p == NULL) {
    taosMemoryFree(pInfo->pFillInfo);
    taosMemoryFree(pInfo->p);
    return TSDB_CODE_OUT_OF_MEMORY;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static bool isWstartColumnExist(SFillOperatorInfo* pInfo) {
  if (pInfo->noFillExprSupp.numOfExprs == 0) {
    return false;
  }

  for (int32_t i = 0; i < pInfo->noFillExprSupp.numOfExprs; ++i) {
    SExprInfo* exprInfo = pInfo->noFillExprSupp.pExprInfo + i;
    if (exprInfo->pExpr->nodeType == QUERY_NODE_COLUMN && exprInfo->base.numOfParams == 1 &&
        exprInfo->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      return true;
    }
  }
  return false;
}

static int32_t createPrimaryTsExprIfNeeded(SFillOperatorInfo* pInfo, SFillPhysiNode* pPhyFillNode, SExprSupp* pExprSupp,
                                           const char* idStr) {
  bool wstartExist = isWstartColumnExist(pInfo);

  if (wstartExist == false) {
    if (pPhyFillNode->pWStartTs->type != QUERY_NODE_TARGET) {
      qError("pWStartTs of fill physical node is not a target node, %s", idStr);
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    SExprInfo* pExpr = taosMemoryRealloc(pExprSupp->pExprInfo, (pExprSupp->numOfExprs + 1) * sizeof(SExprInfo));
    if (pExpr == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    createExprFromTargetNode(&pExpr[pExprSupp->numOfExprs], (STargetNode*)pPhyFillNode->pWStartTs);
    pExprSupp->numOfExprs += 1;
    pExprSupp->pExprInfo = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode,
                                      SExecTaskInfo* pTaskInfo) {
  SFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SFillOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pRes = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  SExprInfo* pExprInfo = createExprInfo(pPhyFillNode->pFillExprs, NULL, &pInfo->numOfExpr);
  pOperator->exprSupp.pExprInfo = pExprInfo;

  SExprSupp* pNoFillSupp = &pInfo->noFillExprSupp;
  pNoFillSupp->pExprInfo = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &pNoFillSupp->numOfExprs);
  int32_t code = createPrimaryTsExprIfNeeded(pInfo, pPhyFillNode, pNoFillSupp, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initExprSupp(pNoFillSupp, pNoFillSupp->pExprInfo, pNoFillSupp->numOfExprs, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SInterval* pInterval =
      QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == downstream->operatorType
          ? &((SMergeAlignedIntervalAggOperatorInfo*)downstream->info)->intervalAggOperatorInfo->interval
          : &((SIntervalAggOperatorInfo*)downstream->info)->interval;

  int32_t order = (pPhyFillNode->node.inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  int32_t type = convertFillType(pPhyFillNode->mode);

  SResultInfo* pResultInfo = &pOperator->resultInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  code = initExprSupp(&pOperator->exprSupp, pExprInfo, pInfo->numOfExpr, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc, &numOfOutputCols,
                             COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);

  code = initFillInfo(pInfo, pExprInfo, pInfo->numOfExpr, pNoFillSupp->pExprInfo, pNoFillSupp->numOfExprs,
                      (SNodeListNode*)pPhyFillNode->pValues, pPhyFillNode->timeRange, pResultInfo->capacity,
                      pTaskInfo->id.str, pInterval, type, order);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pFinalRes = createOneDataBlock(pInfo->pRes, false);
  blockDataEnsureCapacity(pInfo->pFinalRes, pOperator->resultInfo.capacity);

  code = filterInitFromNode((SNode*)pPhyFillNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "FillOperator", QUERY_NODE_PHYSICAL_PLAN_FILL, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = pInfo->numOfExpr;
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doFill, NULL, destroyFillOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyFillOperatorInfo(pInfo);
  }

  pTaskInfo->code = code;
  taosMemoryFreeClear(pOperator);
  return NULL;
}
