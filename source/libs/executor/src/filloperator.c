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

#include "executorimpl.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "executorInt.h"
#include "function.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "tfill.h"

#define FILL_POS_INVALID 0
#define FILL_POS_START   1
#define FILL_POS_MID     2
#define FILL_POS_END     3

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

static void destroyFillOperatorInfo(void* param);
static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);

static void doHandleRemainBlockForNewGroupImpl(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                               SResultInfo* pResultInfo, SExecTaskInfo* pTaskInfo) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
  getTableScanInfo(pOperator, &order, &scanFlag);

  int64_t ekey = pInfo->existNewGroupBlock->info.window.ekey;
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  blockDataCleanup(pInfo->pRes);
  doApplyScalarCalculation(pOperator, pInfo->existNewGroupBlock, order, scanFlag);

  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ekey);
  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);

  int32_t numOfResultRows = pResultInfo->capacity - pResBlock->info.rows;
  taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);

  pInfo->curGroupId = pInfo->existNewGroupBlock->info.id.groupId;
  pInfo->existNewGroupBlock = NULL;
}

static void doHandleRemainBlockFromNewGroup(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                            SResultInfo* pResultInfo, SExecTaskInfo* pTaskInfo) {
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    int32_t numOfResultRows = pResultInfo->capacity - pInfo->pFinalRes->info.rows;
    taosFillResultDataBlock(pInfo->pFillInfo, pInfo->pFinalRes, numOfResultRows);
    pInfo->pRes->info.id.groupId = pInfo->curGroupId;
    return;
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, pTaskInfo);
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

static SSDataBlock* doFillImpl(SOperatorInfo* pOperator) {
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pFinalRes;

  blockDataCleanup(pResBlock);

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
  getTableScanInfo(pOperator, &order, &scanFlag);

  doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, pTaskInfo);
  if (pResBlock->info.rows > 0) {
    pResBlock->info.id.groupId = pInfo->curGroupId;
    return pResBlock;
  }

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  while (1) {
    SSDataBlock* pBlock = pDownstream->fpSet.getNextFn(pDownstream);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0 && (pInfo->pFillInfo->type != TSDB_FILL_NULL_F && pInfo->pFillInfo->type != TSDB_FILL_SET_VALUE_F)) {
        setOperatorCompleted(pOperator);
        return NULL;
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
    } else {
      pBlock->info.dataLoad = 1;
      blockDataUpdateTsWindow(pBlock, pInfo->primarySrcSlotId);

      blockDataCleanup(pInfo->pRes);
      blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
      blockDataEnsureCapacity(pInfo->pFinalRes, pBlock->info.rows);
      doApplyScalarCalculation(pOperator, pBlock, order, scanFlag);

      if (pInfo->curGroupId == 0 || pInfo->curGroupId == pInfo->pRes->info.id.groupId) {
        pInfo->curGroupId = pInfo->pRes->info.id.groupId;  // the first data block
        pInfo->totalInputRows += pInfo->pRes->info.rows;

        if (order == pInfo->pFillInfo->order) {
          taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, pBlock->info.window.ekey);
        } else {
          taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, pBlock->info.window.skey);
        }
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

      doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, pTaskInfo);
      if (pResBlock->info.rows >= pOperator->resultInfo.threshold || pBlock == NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      assert(pBlock != NULL);

      blockDataCleanup(pResBlock);

      doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, pTaskInfo);
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

  int64_t     startKey = (order == TSDB_ORDER_ASC) ? win.skey : win.ekey;
  STimeWindow w = getAlignQueryTimeWindow(pInterval, pInterval->precision, startKey);
  w = getFirstQualifiedTimeWindow(startKey, &w, pInterval, order);

  pInfo->pFillInfo = taosCreateFillInfo(w.skey, numOfCols, numOfNotFillCols, capacity, pInterval, fillType, pColInfo,
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

  code = initExprSupp(pNoFillSupp, pNoFillSupp->pExprInfo, pNoFillSupp->numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SInterval* pInterval =
      QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == downstream->operatorType
      ? &((SMergeAlignedIntervalAggOperatorInfo*)downstream->info)->intervalAggOperatorInfo->interval
      : &((SIntervalAggOperatorInfo*)downstream->info)->interval;

  int32_t order = (pPhyFillNode->inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  int32_t type = convertFillType(pPhyFillNode->mode);

  SResultInfo* pResultInfo = &pOperator->resultInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  code = initExprSupp(&pOperator->exprSupp, pExprInfo, pInfo->numOfExpr);
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
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doFill, NULL, destroyFillOperatorInfo, optrDefaultBufFn, NULL);

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

TSKEY getNextWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextIntervalWindow(pInterval, &win, TSDB_ORDER_ASC);
  return win.skey;
}

TSKEY getPrevWindowTs(TSKEY ts, SInterval* pInterval) {
  STimeWindow win = {.skey = ts, .ekey = ts};
  getNextIntervalWindow(pInterval, &win, TSDB_ORDER_DESC);
  return win.skey;
}

void setRowCell(SColumnInfoData* pCol, int32_t rowId, const SResultCellData* pCell) {
  colDataAppend(pCol, rowId, pCell->pData, pCell->isNull);
}

SResultCellData* getResultCell(SResultRowData* pRaw, int32_t index) {
  if (!pRaw || !pRaw->pRowVal) {
    return NULL;
  }
  char*            pData = (char*)pRaw->pRowVal;
  SResultCellData* pCell = pRaw->pRowVal;
  for (int32_t i = 0; i < index; i++) {
    pData += (pCell->bytes + sizeof(SResultCellData));
    pCell = (SResultCellData*)pData;
  }
  return pCell;
}

void* destroyFillColumnInfo(SFillColInfo* pFillCol, int32_t start, int32_t end) {
  for (int32_t i = start; i < end; i++) {
    destroyExprInfo(pFillCol[i].pExpr, 1);
    taosVariantDestroy(&pFillCol[i].fillVal);
  }
  taosMemoryFreeClear(pFillCol[start].pExpr);
  taosMemoryFree(pFillCol);
  return NULL;
}

void* destroyStreamFillSupporter(SStreamFillSupporter* pFillSup) {
  pFillSup->pAllColInfo = destroyFillColumnInfo(pFillSup->pAllColInfo, pFillSup->numOfFillCols, pFillSup->numOfAllCols);
  tSimpleHashCleanup(pFillSup->pResMap);
  pFillSup->pResMap = NULL;
  releaseOutputBuf(NULL, NULL, (SResultRow*)pFillSup->cur.pRowVal);
  pFillSup->cur.pRowVal = NULL;
  cleanupExprSupp(&pFillSup->notFillExprSup);

  taosMemoryFree(pFillSup);
  return NULL;
}

void* destroyStreamFillLinearInfo(SStreamFillLinearInfo* pFillLinear) {
  taosArrayDestroy(pFillLinear->pDeltaVal);
  taosArrayDestroy(pFillLinear->pNextDeltaVal);
  taosMemoryFree(pFillLinear);
  return NULL;
}
void* destroyStreamFillInfo(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->type == TSDB_FILL_SET_VALUE || pFillInfo->type == TSDB_FILL_SET_VALUE_F ||
      pFillInfo->type == TSDB_FILL_NULL || pFillInfo->type == TSDB_FILL_NULL_F) {
    taosMemoryFreeClear(pFillInfo->pResRow->pRowVal);
    taosMemoryFreeClear(pFillInfo->pResRow);
  }
  pFillInfo->pLinearInfo = destroyStreamFillLinearInfo(pFillInfo->pLinearInfo);
  taosArrayDestroy(pFillInfo->delRanges);
  taosMemoryFree(pFillInfo);
  return NULL;
}

static void destroyStreamFillOperatorInfo(void* param) {
  SStreamFillOperatorInfo* pInfo = (SStreamFillOperatorInfo*)param;
  pInfo->pFillInfo = destroyStreamFillInfo(pInfo->pFillInfo);
  pInfo->pFillSup = destroyStreamFillSupporter(pInfo->pFillSup);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  pInfo->pSrcBlock = blockDataDestroy(pInfo->pSrcBlock);
  pInfo->pDelRes = blockDataDestroy(pInfo->pDelRes);
  pInfo->matchInfo.pList = taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFree(pInfo);
}

static void resetFillWindow(SResultRowData* pRowData) {
  pRowData->key = INT64_MIN;
  pRowData->pRowVal = NULL;
}

void resetPrevAndNextWindow(SStreamFillSupporter* pFillSup, SStreamState* pState) {
  resetFillWindow(&pFillSup->prev);
  releaseOutputBuf(NULL, NULL, (SResultRow*)pFillSup->cur.pRowVal);
  resetFillWindow(&pFillSup->cur);
  resetFillWindow(&pFillSup->next);
  resetFillWindow(&pFillSup->nextNext);
}

void getCurWindowFromDiscBuf(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, SStreamFillSupporter* pFillSup) {
  SStreamState* pState = pOperator->pTaskInfo->streamInfo.pState;
  resetPrevAndNextWindow(pFillSup, pState);

  SWinKey key = {.ts = ts, .groupId = groupId};
  int32_t curVLen = 0;
  int32_t code = streamStateFillGet(pState, &key, (void**)&pFillSup->cur.pRowVal, &curVLen);
  ASSERT(code == TSDB_CODE_SUCCESS);
  pFillSup->cur.key = key.ts;
}

void getWindowFromDiscBuf(SOperatorInfo* pOperator, TSKEY ts, uint64_t groupId, SStreamFillSupporter* pFillSup) {
  SStreamState* pState = pOperator->pTaskInfo->streamInfo.pState;
  resetPrevAndNextWindow(pFillSup, pState);

  SWinKey key = {.ts = ts, .groupId = groupId};
  void*   curVal = NULL;
  int32_t curVLen = 0;
  int32_t code = streamStateFillGet(pState, &key, (void**)&curVal, &curVLen);
  ASSERT(code == TSDB_CODE_SUCCESS);
  pFillSup->cur.key = key.ts;
  pFillSup->cur.pRowVal = curVal;

  SStreamStateCur* pCur = streamStateFillSeekKeyPrev(pState, &key);
  SWinKey          preKey = {.groupId = groupId};
  void*            preVal = NULL;
  int32_t          preVLen = 0;
  code = streamStateGetGroupKVByCur(pCur, &preKey, (const void**)&preVal, &preVLen);

  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->prev.key = preKey.ts;
    pFillSup->prev.pRowVal = preVal;

    code = streamStateCurNext(pState, pCur);
    ASSERT(code == TSDB_CODE_SUCCESS);

    code = streamStateCurNext(pState, pCur);
    if (code != TSDB_CODE_SUCCESS) {
      streamStateFreeCur(pCur);
      pCur = NULL;
    }
  } else {
    streamStateFreeCur(pCur);
    pCur = streamStateFillSeekKeyNext(pState, &key);
  }

  SWinKey nextKey = {.groupId = groupId};
  void*   nextVal = NULL;
  int32_t nextVLen = 0;
  code = streamStateGetGroupKVByCur(pCur, &nextKey, (const void**)&nextVal, &nextVLen);
  if (code == TSDB_CODE_SUCCESS) {
    pFillSup->next.key = nextKey.ts;
    pFillSup->next.pRowVal = nextVal;
    if (pFillSup->type == TSDB_FILL_PREV || pFillSup->type == TSDB_FILL_NEXT) {
      code = streamStateCurNext(pState, pCur);
      if (code == TSDB_CODE_SUCCESS) {
        SWinKey nextNextKey = {.groupId = groupId};
        void*   nextNextVal = NULL;
        int32_t nextNextVLen = 0;
        code = streamStateGetGroupKVByCur(pCur, &nextNextKey, (const void**)&nextNextVal, &nextNextVLen);
        if (code == TSDB_CODE_SUCCESS) {
          pFillSup->nextNext.key = nextNextKey.ts;
          pFillSup->nextNext.pRowVal = nextNextVal;
        }
      }
    }
  }
  streamStateFreeCur(pCur);
}

static bool hasPrevWindow(SStreamFillSupporter* pFillSup) { return pFillSup->prev.key != INT64_MIN; }
static bool hasNextWindow(SStreamFillSupporter* pFillSup) { return pFillSup->next.key != INT64_MIN; }
static bool hasNextNextWindow(SStreamFillSupporter* pFillSup) {
  return pFillSup->nextNext.key != INT64_MIN;
  return false;
}

static void transBlockToResultRow(const SSDataBlock* pBlock, int32_t rowId, TSKEY ts, SResultRowData* pRowVal) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    SResultCellData* pCell = getResultCell(pRowVal, i);
    if (!colDataIsNull_s(pColData, rowId)) {
      pCell->isNull = false;
      pCell->type = pColData->info.type;
      pCell->bytes = pColData->info.bytes;
      char* val = colDataGetData(pColData, rowId);
      if (IS_VAR_DATA_TYPE(pCell->type)) {
        memcpy(pCell->pData, val, varDataTLen(val));
      } else {
        memcpy(pCell->pData, val, pCell->bytes);
      }
    } else {
      pCell->isNull = true;
    }
  }
  pRowVal->key = ts;
}

static void calcDeltaData(SSDataBlock* pBlock, int32_t rowId, SResultRowData* pRowVal, SArray* pDelta,
                          SFillColInfo* pFillCol, int32_t numOfCol, int32_t winCount, int32_t order) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
      char*            var = colDataGetData(pCol, rowId);
      double           start = 0;
      GET_TYPED_DATA(start, double, pCol->info.type, var);
      SResultCellData* pCell = getResultCell(pRowVal, slotId);
      double           end = 0;
      GET_TYPED_DATA(end, double, pCell->type, pCell->pData);
      double delta = 0;
      if (order == TSDB_ORDER_ASC) {
        delta = (end - start) / winCount;
      } else {
        delta = (start - end) / winCount;
      }
      taosArraySet(pDelta, slotId, &delta);
    }
  }
}

static void calcRowDeltaData(SResultRowData* pStartRow, SResultRowData* pEndRow, SArray* pDelta, SFillColInfo* pFillCol,
                             int32_t numOfCol, int32_t winCount) {
  for (int32_t i = 0; i < numOfCol; i++) {
    if (!pFillCol[i].notFillCol) {
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol + i);
      SResultCellData* pSCell = getResultCell(pStartRow, slotId);
      double           start = 0.0;
      GET_TYPED_DATA(start, double, pSCell->type, pSCell->pData);
      SResultCellData* pECell = getResultCell(pEndRow, slotId);
      double           end = 0.0;
      GET_TYPED_DATA(end, double, pECell->type, pECell->pData);
      double delta = (end - start) / winCount;
      taosArraySet(pDelta, slotId, &delta);
    }
  }
}

static void setFillInfoStart(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->start = ts;
}

static void setFillInfoEnd(TSKEY ts, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  ts = taosTimeAdd(ts, pInterval->sliding * -1, pInterval->slidingUnit, pInterval->precision);
  pFillInfo->end = ts;
}

static void setFillKeyInfo(TSKEY start, TSKEY end, SInterval* pInterval, SStreamFillInfo* pFillInfo) {
  setFillInfoStart(start, pInterval, pFillInfo);
  pFillInfo->current = pFillInfo->start;
  setFillInfoEnd(end, pInterval, pFillInfo);
}

void setDeleteFillValueInfo(TSKEY start, TSKEY end, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo) {
  if (!hasPrevWindow(pFillSup) || !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    return;
  }

  TSKEY realStart = taosTimeAdd(pFillSup->prev.key, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                pFillSup->interval.precision);

  pFillInfo->needFill = true;
  pFillInfo->start = realStart;
  pFillInfo->current = pFillInfo->start;
  pFillInfo->end = end;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F:
      break;
    case TSDB_FILL_PREV:
      pFillInfo->pResRow = &pFillSup->prev;
      break;
    case TSDB_FILL_NEXT:
      pFillInfo->pResRow = &pFillSup->next;
      break;
    case TSDB_FILL_LINEAR: {
      setFillKeyInfo(pFillSup->prev.key, pFillSup->next.key, &pFillSup->interval, pFillInfo);
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
      int32_t numOfWins = taosTimeCountInterval(pFillSup->prev.key, pFillSup->next.key, pFillSup->interval.sliding,
                                                pFillSup->interval.slidingUnit, pFillSup->interval.precision);
      calcRowDeltaData(&pFillSup->prev, &pFillSup->next, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                       pFillSup->numOfAllCols, numOfWins);
      pFillInfo->pResRow = &pFillSup->prev;
      pFillInfo->pLinearInfo->winIndex = 0;
    } break;
    default:
      ASSERT(0);
      break;
  }
}

void copyNotFillExpData(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo) {
  for (int32_t i = pFillSup->numOfFillCols; i < pFillSup->numOfAllCols; ++i) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
    SResultCellData* pCell = getResultCell(pFillInfo->pResRow, slotId);
    SResultCellData* pCurCell = getResultCell(&pFillSup->cur, slotId);
    pCell->isNull = pCurCell->isNull;
    if (!pCurCell->isNull) {
      memcpy(pCell->pData, pCurCell->pData, pCell->bytes);
    }
  }
}

void setFillValueInfo(SSDataBlock* pBlock, TSKEY ts, int32_t rowId, SStreamFillSupporter* pFillSup,
                      SStreamFillInfo* pFillInfo) {
  pFillInfo->preRowKey = pFillSup->cur.key;
  if (!hasPrevWindow(pFillSup) && !hasNextWindow(pFillSup)) {
    pFillInfo->needFill = false;
    pFillInfo->pos = FILL_POS_START;
    return;
  }
  TSKEY prevWKey = INT64_MIN;
  TSKEY nextWKey = INT64_MIN;
  if (hasPrevWindow(pFillSup)) {
    prevWKey = pFillSup->prev.key;
  }
  if (hasNextWindow(pFillSup)) {
    nextWKey = pFillSup->next.key;
  }

  pFillInfo->needFill = true;
  pFillInfo->pos = FILL_POS_INVALID;
  switch (pFillInfo->type) {
    case TSDB_FILL_NULL:
    case TSDB_FILL_NULL_F:
    case TSDB_FILL_SET_VALUE:
    case TSDB_FILL_SET_VALUE_F: {
      if (pFillSup->prev.key == pFillInfo->preRowKey) {
        resetFillWindow(&pFillSup->prev);
      }
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        if (pFillSup->next.key == pFillInfo->nextRowKey) {
          pFillInfo->preRowKey = INT64_MIN;
          setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
          pFillInfo->pos = FILL_POS_END;
        } else {
          pFillInfo->needFill = false;
          pFillInfo->pos = FILL_POS_START;
        }
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
      } else {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      copyNotFillExpData(pFillSup, pFillInfo);
    } break;
    case TSDB_FILL_PREV: {
      if (hasNextWindow(pFillSup) && ((pFillSup->next.key != pFillInfo->nextRowKey) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && hasNextNextWindow(pFillSup)) ||
                                      (pFillSup->next.key == pFillInfo->nextRowKey && !hasPrevWindow(pFillSup)))) {
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        pFillSup->prev.key = pFillSup->cur.key;
        pFillSup->prev.pRowVal = pFillSup->cur.pRowVal;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->preRowKey = INT64_MIN;
      }
      pFillInfo->pResRow = &pFillSup->prev;
    } break;
    case TSDB_FILL_NEXT: {
      if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillSup->next.key = pFillSup->cur.key;
        pFillSup->next.pRowVal = pFillSup->cur.pRowVal;
        pFillInfo->preRowKey = INT64_MIN;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
      }
      pFillInfo->pResRow = &pFillSup->next;
    } break;
    case TSDB_FILL_LINEAR: {
      pFillInfo->pLinearInfo->winIndex = 0;
      if (hasPrevWindow(pFillSup) && hasNextWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_MID;
        pFillInfo->pLinearInfo->nextEnd = nextWKey;
        int32_t numOfWins = taosTimeCountInterval(prevWKey, ts, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->prev, &pFillSup->cur, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->prev;

        numOfWins = taosTimeCountInterval(ts, nextWKey, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                          pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->cur, &pFillSup->next, pFillInfo->pLinearInfo->pNextDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pLinearInfo->hasNext = true;
      } else if (hasPrevWindow(pFillSup)) {
        setFillKeyInfo(prevWKey, ts, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_END;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        int32_t numOfWins = taosTimeCountInterval(prevWKey, ts, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->prev, &pFillSup->cur, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->prev;
        pFillInfo->pLinearInfo->hasNext = false;
      } else {
        ASSERT(hasNextWindow(pFillSup));
        setFillKeyInfo(ts, nextWKey, &pFillSup->interval, pFillInfo);
        pFillInfo->pos = FILL_POS_START;
        pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
        int32_t numOfWins = taosTimeCountInterval(ts, nextWKey, pFillSup->interval.sliding,
                                                  pFillSup->interval.slidingUnit, pFillSup->interval.precision);
        calcRowDeltaData(&pFillSup->cur, &pFillSup->next, pFillInfo->pLinearInfo->pDeltaVal, pFillSup->pAllColInfo,
                         pFillSup->numOfAllCols, numOfWins);
        pFillInfo->pResRow = &pFillSup->cur;
        pFillInfo->pLinearInfo->hasNext = false;
      }
    } break;
    default:
      ASSERT(0);
      break;
  }
  ASSERT(pFillInfo->pos != FILL_POS_INVALID);
}

static bool checkResult(SStreamFillSupporter* pFillSup, TSKEY ts, uint64_t groupId) {
  SWinKey key = {.groupId = groupId, .ts = ts};
  if (tSimpleHashGet(pFillSup->pResMap, &key, sizeof(SWinKey)) != NULL) {
    return false;
  }
  tSimpleHashPut(pFillSup->pResMap, &key, sizeof(SWinKey), NULL, 0);
  return true;
}

static void buildFillResult(SResultRowData* pResRow, SStreamFillSupporter* pFillSup, TSKEY ts, SSDataBlock* pBlock) {
  uint64_t groupId = pBlock->info.id.groupId;
  if (pFillSup->hasDelete && !checkResult(pFillSup, ts, groupId)) {
    return;
  }
  for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
    SFillColInfo*    pFillCol = pFillSup->pAllColInfo + i;
    int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
    SFillInfo        tmpInfo = {
        .currentKey = ts,
        .order = TSDB_ORDER_ASC,
        .interval = pFillSup->interval,
    };
    bool filled = fillIfWindowPseudoColumn(&tmpInfo, pFillCol, pColData, pBlock->info.rows);
    if (!filled) {
      SResultCellData* pCell = getResultCell(pResRow, slotId);
      setRowCell(pColData, pBlock->info.rows, pCell);
    }
  }
  pBlock->info.rows++;
}

static bool hasRemainCalc(SStreamFillInfo* pFillInfo) {
  if (pFillInfo->current != INT64_MIN && pFillInfo->current <= pFillInfo->end) {
    return true;
  }
  return false;
}

static void doStreamFillNormal(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    buildFillResult(pFillInfo->pResRow, pFillSup, pFillInfo->current, pBlock);
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
  }
}

static void doStreamFillLinear(SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock) {
  while (hasRemainCalc(pFillInfo) && pBlock->info.rows < pBlock->info.capacity) {
    uint64_t groupId = pBlock->info.id.groupId;
    SWinKey  key = {.groupId = groupId, .ts = pFillInfo->current};
    if (pFillSup->hasDelete && !checkResult(pFillSup, pFillInfo->current, groupId)) {
      pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                       pFillSup->interval.precision);
      pFillInfo->pLinearInfo->winIndex++;
      continue;
    }
    pFillInfo->pLinearInfo->winIndex++;
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SFillColInfo* pFillCol = pFillSup->pAllColInfo + i;
      SFillInfo     tmp = {
          .currentKey = pFillInfo->current,
          .order = TSDB_ORDER_ASC,
          .interval = pFillSup->interval,
      };

      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, slotId);
      int16_t          type = pColData->info.type;
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, slotId);
      int32_t          index = pBlock->info.rows;
      if (pFillCol->notFillCol) {
        bool filled = fillIfWindowPseudoColumn(&tmp, pFillCol, pColData, index);
        if (!filled) {
          setRowCell(pColData, index, pCell);
        }
      } else {
        if (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_BOOL || pCell->isNull) {
          colDataAppendNULL(pColData, index);
          continue;
        }
        double* pDelta = taosArrayGet(pFillInfo->pLinearInfo->pDeltaVal, slotId);
        double  vCell = 0;
        GET_TYPED_DATA(vCell, double, pCell->type, pCell->pData);
        vCell += (*pDelta) * pFillInfo->pLinearInfo->winIndex;
        int64_t result = 0;
        SET_TYPED_DATA(&result, pCell->type, vCell);
        colDataAppend(pColData, index, (const char*)&result, false);
      }
    }
    pFillInfo->current = taosTimeAdd(pFillInfo->current, pFillSup->interval.sliding, pFillSup->interval.slidingUnit,
                                     pFillSup->interval.precision);
    pBlock->info.rows++;
  }
}

static void keepResultInDiscBuf(SOperatorInfo* pOperator, uint64_t groupId, SResultRowData* pRow, int32_t len) {
  SWinKey key = {.groupId = groupId, .ts = pRow->key};
  int32_t code = streamStateFillPut(pOperator->pTaskInfo->streamInfo.pState, &key, pRow->pRowVal, len);
  ASSERT(code == TSDB_CODE_SUCCESS);
}

static void doStreamFillRange(SStreamFillInfo* pFillInfo, SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  if (pFillInfo->needFill == false) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
    return;
  }

  if (pFillInfo->pos == FILL_POS_START) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
  }
  if (pFillInfo->type != TSDB_FILL_LINEAR) {
    doStreamFillNormal(pFillSup, pFillInfo, pRes);
  } else {
    doStreamFillLinear(pFillSup, pFillInfo, pRes);

    if (pFillInfo->pos == FILL_POS_MID) {
      buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
    }

    if (pFillInfo->current > pFillInfo->end && pFillInfo->pLinearInfo->hasNext) {
      pFillInfo->pLinearInfo->hasNext = false;
      pFillInfo->pLinearInfo->winIndex = 0;
      taosArrayClear(pFillInfo->pLinearInfo->pDeltaVal);
      taosArrayAddAll(pFillInfo->pLinearInfo->pDeltaVal, pFillInfo->pLinearInfo->pNextDeltaVal);
      pFillInfo->pResRow = &pFillSup->cur;
      setFillKeyInfo(pFillSup->cur.key, pFillInfo->pLinearInfo->nextEnd, &pFillSup->interval, pFillInfo);
      doStreamFillLinear(pFillSup, pFillInfo, pRes);
    }
  }
  if (pFillInfo->pos == FILL_POS_END) {
    buildFillResult(&pFillSup->cur, pFillSup, pFillSup->cur.key, pRes);
  }
}

void keepBlockRowInDiscBuf(SOperatorInfo* pOperator, SStreamFillInfo* pFillInfo, SSDataBlock* pBlock, TSKEY* tsCol,
                           int32_t rowId, uint64_t groupId, int32_t rowSize) {
  TSKEY ts = tsCol[rowId];
  pFillInfo->nextRowKey = ts;
  SResultRowData tmpNextRow = {.key = ts};
  tmpNextRow.pRowVal = taosMemoryCalloc(1, rowSize);
  transBlockToResultRow(pBlock, rowId, ts, &tmpNextRow);
  keepResultInDiscBuf(pOperator, groupId, &tmpNextRow, rowSize);
  taosMemoryFreeClear(tmpNextRow.pRowVal);
}

static void doFillResults(SOperatorInfo* pOperator, SStreamFillSupporter* pFillSup, SStreamFillInfo* pFillInfo,
                          SSDataBlock* pBlock, TSKEY* tsCol, int32_t rowId, SSDataBlock* pRes) {
  uint64_t groupId = pBlock->info.id.groupId;
  getWindowFromDiscBuf(pOperator, tsCol[rowId], groupId, pFillSup);
  if (pFillSup->prev.key == pFillInfo->preRowKey) {
    resetFillWindow(&pFillSup->prev);
  }
  setFillValueInfo(pBlock, tsCol[rowId], rowId, pFillSup, pFillInfo);
  doStreamFillRange(pFillInfo, pFillSup, pRes);
}

static void doStreamFillImpl(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcBlock;
  uint64_t                 groupId = pBlock->info.id.groupId;
  SSDataBlock*             pRes = pInfo->pRes;
  pRes->info.id.groupId = groupId;
  if (hasRemainCalc(pFillInfo)) {
    doStreamFillRange(pFillInfo, pFillSup, pRes);
  }

  SColumnInfoData* pTsCol = taosArrayGet(pInfo->pSrcBlock->pDataBlock, pInfo->primaryTsCol);
  TSKEY*           tsCol = (TSKEY*)pTsCol->pData;

  if (pInfo->srcRowIndex == 0) {
    keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    pInfo->srcRowIndex++;
  }

  while (pInfo->srcRowIndex < pBlock->info.rows) {
    TSKEY ts = tsCol[pInfo->srcRowIndex];
    keepBlockRowInDiscBuf(pOperator, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex, groupId, pFillSup->rowSize);
    doFillResults(pOperator, pFillSup, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex - 1, pRes);
    if (pInfo->pRes->info.rows == pInfo->pRes->info.capacity) {
      blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
      return;
    }
    pInfo->srcRowIndex++;
  }
  doFillResults(pOperator, pFillSup, pFillInfo, pBlock, tsCol, pInfo->srcRowIndex - 1, pRes);
  blockDataUpdateTsWindow(pRes, pInfo->primaryTsCol);
  blockDataCleanup(pInfo->pSrcBlock);
}

static void buildDeleteRange(SOperatorInfo* pOp, TSKEY start, TSKEY end, uint64_t groupId, SSDataBlock* delRes) {
  SStreamState* pState = pOp->pTaskInfo->streamInfo.pState;

  SSDataBlock*     pBlock = delRes;
  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pTbNameCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
  colDataAppend(pStartCol, pBlock->info.rows, (const char*)&start, false);
  colDataAppend(pEndCol, pBlock->info.rows, (const char*)&end, false);
  colDataAppendNULL(pUidCol, pBlock->info.rows);
  colDataAppend(pGroupCol, pBlock->info.rows, (const char*)&groupId, false);
  colDataAppendNULL(pCalStartCol, pBlock->info.rows);
  colDataAppendNULL(pCalEndCol, pBlock->info.rows);

  SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);

  void* tbname = NULL;
  streamStateGetParName(pOp->pTaskInfo->streamInfo.pState, groupId, &tbname);
  if (tbname == NULL) {
    colDataAppendNULL(pTableCol, pBlock->info.rows);
  } else {
    char parTbName[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN];
    STR_WITH_MAXSIZE_TO_VARSTR(parTbName, tbname, sizeof(parTbName));
    colDataAppend(pTableCol, pBlock->info.rows, (const char*)parTbName, false);
    tdbFree(tbname);
  }

  pBlock->info.rows++;
}

static void buildDeleteResult(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                              SSDataBlock* delRes) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  if (hasPrevWindow(pFillSup)) {
    TSKEY start = getNextWindowTs(pFillSup->prev.key, &pFillSup->interval);
    buildDeleteRange(pOperator, start, endTs, groupId, delRes);
  } else if (hasNextWindow(pFillSup)) {
    TSKEY end = getPrevWindowTs(pFillSup->next.key, &pFillSup->interval);
    buildDeleteRange(pOperator, startTs, end, groupId, delRes);
  } else {
    buildDeleteRange(pOperator, startTs, endTs, groupId, delRes);
  }
}

static void doDeleteFillResultImpl(SOperatorInfo* pOperator, TSKEY startTs, TSKEY endTs, uint64_t groupId) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  getWindowFromDiscBuf(pOperator, startTs, groupId, pInfo->pFillSup);
  setDeleteFillValueInfo(startTs, endTs, pInfo->pFillSup, pInfo->pFillInfo);
  SWinKey key = {.ts = startTs, .groupId = groupId};
  if (!pInfo->pFillInfo->needFill) {
    streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &key);
    buildDeleteResult(pOperator, startTs, endTs, groupId, pInfo->pDelRes);
  } else {
    STimeRange tw = {
        .skey = startTs,
        .ekey = endTs,
        .groupId = groupId,
    };
    taosArrayPush(pInfo->pFillInfo->delRanges, &tw);
    while (key.ts <= endTs) {
      key.ts = taosTimeAdd(key.ts, pInfo->pFillSup->interval.sliding, pInfo->pFillSup->interval.slidingUnit,
                           pInfo->pFillSup->interval.precision);
      tSimpleHashPut(pInfo->pFillSup->pResMap, &key, sizeof(SWinKey), NULL, 0);
    }
  }
}

static void doDeleteFillFinalize(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  int32_t                  size = taosArrayGetSize(pFillInfo->delRanges);
  tSimpleHashClear(pInfo->pFillSup->pResMap);
  for (; pFillInfo->delIndex < size; pFillInfo->delIndex++) {
    STimeRange* range = taosArrayGet(pFillInfo->delRanges, pFillInfo->delIndex);
    if (pInfo->pRes->info.id.groupId != 0 && pInfo->pRes->info.id.groupId != range->groupId) {
      return;
    }
    getWindowFromDiscBuf(pOperator, range->skey, range->groupId, pInfo->pFillSup);
    setDeleteFillValueInfo(range->skey, range->ekey, pInfo->pFillSup, pInfo->pFillInfo);
    if (pInfo->pFillInfo->needFill) {
      doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      pInfo->pRes->info.id.groupId = range->groupId;
    }
    SWinKey key = {.ts = range->skey, .groupId = range->groupId};
    streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &key);
  }
}

static void doDeleteFillResult(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SStreamFillSupporter*    pFillSup = pInfo->pFillSup;
  SStreamFillInfo*         pFillInfo = pInfo->pFillInfo;
  SSDataBlock*             pBlock = pInfo->pSrcDelBlock;
  SSDataBlock*             pRes = pInfo->pRes;
  SSDataBlock*             pDelRes = pInfo->pDelRes;

  SColumnInfoData* pStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           tsStarts = (TSKEY*)pStartCol->pData;
  SColumnInfoData* pGroupCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        groupIds = (uint64_t*)pGroupCol->pData;
  while (pInfo->srcDelRowIndex < pBlock->info.rows) {
    TSKEY            ts = tsStarts[pInfo->srcDelRowIndex];
    TSKEY            endTs = ts;
    uint64_t         groupId = groupIds[pInfo->srcDelRowIndex];
    SWinKey          key = {.ts = ts, .groupId = groupId};
    SStreamStateCur* pCur = streamStateGetAndCheckCur(pOperator->pTaskInfo->streamInfo.pState, &key);
    if (!pCur) {
      pInfo->srcDelRowIndex++;
      continue;
    }

    SWinKey nextKey = {.groupId = groupId, .ts = ts};
    while (pInfo->srcDelRowIndex < pBlock->info.rows) {
      void*    nextVal = NULL;
      int32_t  nextLen = 0;
      TSKEY    delTs = tsStarts[pInfo->srcDelRowIndex];
      uint64_t delGroupId = groupIds[pInfo->srcDelRowIndex];
      int32_t  code = TSDB_CODE_SUCCESS;
      if (groupId != delGroupId) {
        break;
      }
      if (delTs > nextKey.ts) {
        break;
      }

      SWinKey delKey = {.groupId = delGroupId, .ts = delTs};
      if (delTs == nextKey.ts) {
        code = streamStateCurNext(pOperator->pTaskInfo->streamInfo.pState, pCur);
        if (code == TSDB_CODE_SUCCESS) {
          code = streamStateGetGroupKVByCur(pCur, &nextKey, (const void**)&nextVal, &nextLen);
        }
        // ts will be deleted later
        if (delTs != ts) {
          streamStateFillDel(pOperator->pTaskInfo->streamInfo.pState, &delKey);
          streamStateFreeCur(pCur);
          pCur = streamStateGetAndCheckCur(pOperator->pTaskInfo->streamInfo.pState, &nextKey);
        }
        endTs = TMAX(delTs, nextKey.ts - 1);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
      pInfo->srcDelRowIndex++;
    }
    streamStateFreeCur(pCur);
    doDeleteFillResultImpl(pOperator, ts, endTs, groupId);
  }
  pFillInfo->current = pFillInfo->end + 1;
}

static void resetStreamFillInfo(SStreamFillOperatorInfo* pInfo) {
  tSimpleHashClear(pInfo->pFillSup->pResMap);
  pInfo->pFillSup->hasDelete = false;
  taosArrayClear(pInfo->pFillInfo->delRanges);
  pInfo->pFillInfo->delIndex = 0;
}

static void doApplyStreamScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pSrcBlock, SSDataBlock* pDstBlock) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;

  blockDataCleanup(pDstBlock);
  blockDataEnsureCapacity(pDstBlock, pSrcBlock->info.rows);
  setInputDataBlock(pSup, pSrcBlock, TSDB_ORDER_ASC, MAIN_SCAN, false);
  projectApplyFunctions(pSup->pExprInfo, pDstBlock, pSrcBlock, pSup->pCtx, pSup->numOfExprs, NULL);

  pDstBlock->info.rows = 0;
  pSup = &pInfo->pFillSup->notFillExprSup;
  setInputDataBlock(pSup, pSrcBlock, TSDB_ORDER_ASC, MAIN_SCAN, false);
  projectApplyFunctions(pSup->pExprInfo, pDstBlock, pSrcBlock, pSup->pCtx, pSup->numOfExprs, NULL);
  pDstBlock->info.id.groupId = pSrcBlock->info.id.groupId;

  blockDataUpdateTsWindow(pDstBlock, pInfo->primaryTsCol);
}

static SSDataBlock* doStreamFill(SOperatorInfo* pOperator) {
  SStreamFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  blockDataCleanup(pInfo->pRes);
  if (pOperator->status == OP_RES_TO_RETURN) {
    if (hasRemainCalc(pInfo->pFillInfo)) {
      doStreamFillRange(pInfo->pFillInfo, pInfo->pFillSup, pInfo->pRes);
      if (pInfo->pRes->info.rows > 0) {
        return pInfo->pRes;
      }
    }
    doDeleteFillFinalize(pOperator);
    if (pInfo->pRes->info.rows > 0) {
      printDataBlock(pInfo->pRes, "stream fill");
      return pInfo->pRes;
    }
    setOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    return NULL;
  }

  SSDataBlock*   fillResult = NULL;
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  while (1) {
    if (pInfo->srcRowIndex >= pInfo->pSrcBlock->info.rows) {
      // If there are delete datablocks, we receive  them first.
      SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
      if (pBlock == NULL) {
        pOperator->status = OP_RES_TO_RETURN;
        pInfo->pFillInfo->preRowKey = INT64_MIN;
        if (pInfo->pRes->info.rows > 0) {
          printDataBlock(pInfo->pRes, "stream fill");
          return pInfo->pRes;
        }
        break;
      }
      printDataBlock(pBlock, "stream fill recv");

      switch (pBlock->info.type) {
        case STREAM_RETRIEVE:
          return pBlock;
        case STREAM_DELETE_RESULT: {
          pInfo->pSrcDelBlock = pBlock;
          pInfo->srcDelRowIndex = 0;
          blockDataCleanup(pInfo->pDelRes);
          pInfo->pFillSup->hasDelete = true;
          doDeleteFillResult(pOperator);
          if (pInfo->pDelRes->info.rows > 0) {
            printDataBlock(pInfo->pDelRes, "stream fill delete");
            return pInfo->pDelRes;
          }
          continue;
        } break;
        case STREAM_NORMAL:
        case STREAM_INVALID: {
          doApplyStreamScalarCalculation(pOperator, pBlock, pInfo->pSrcBlock);
          memcpy(pInfo->pSrcBlock->info.parTbName, pBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
          pInfo->srcRowIndex = 0;
        } break;
        default:
          ASSERT(0);
          break;
      }
    }

    doStreamFillImpl(pOperator);
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    memcpy(pInfo->pRes->info.parTbName, pInfo->pSrcBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
    pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
    if (pInfo->pRes->info.rows > 0) {
      break;
    }
  }
  if (pOperator->status == OP_RES_TO_RETURN) {
    doDeleteFillFinalize(pOperator);
  }

  if (pInfo->pRes->info.rows == 0) {
    setOperatorCompleted(pOperator);
    resetStreamFillInfo(pInfo);
    return NULL;
  }

  pOperator->resultInfo.totalRows += pInfo->pRes->info.rows;
  printDataBlock(pInfo->pRes, "stream fill");
  return pInfo->pRes;
}

static int32_t initResultBuf(SStreamFillSupporter* pFillSup) {
  pFillSup->rowSize = sizeof(SResultCellData) * pFillSup->numOfAllCols;
  for (int i = 0; i < pFillSup->numOfAllCols; i++) {
    SFillColInfo* pCol = &pFillSup->pAllColInfo[i];
    SResSchema*   pSchema = &pCol->pExpr->base.resSchema;
    pFillSup->rowSize += pSchema->bytes;
  }
  pFillSup->next.key = INT64_MIN;
  pFillSup->nextNext.key = INT64_MIN;
  pFillSup->prev.key = INT64_MIN;
  pFillSup->cur.key = INT64_MIN;
  pFillSup->next.pRowVal = NULL;
  pFillSup->nextNext.pRowVal = NULL;
  pFillSup->prev.pRowVal = NULL;
  pFillSup->cur.pRowVal = NULL;

  return TSDB_CODE_SUCCESS;
}

static SStreamFillSupporter* initStreamFillSup(SStreamFillPhysiNode* pPhyFillNode, SInterval* pInterval,
                                               SExprInfo* pFillExprInfo, int32_t numOfFillCols) {
  SStreamFillSupporter* pFillSup = taosMemoryCalloc(1, sizeof(SStreamFillSupporter));
  if (!pFillSup) {
    return NULL;
  }
  pFillSup->numOfFillCols = numOfFillCols;
  int32_t    numOfNotFillCols = 0;
  SExprInfo* noFillExprInfo = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &numOfNotFillCols);
  pFillSup->pAllColInfo = createFillColInfo(pFillExprInfo, pFillSup->numOfFillCols, noFillExprInfo, numOfNotFillCols,
                                            (const SNodeListNode*)(pPhyFillNode->pValues));
  pFillSup->type = convertFillType(pPhyFillNode->mode);
  pFillSup->numOfAllCols = pFillSup->numOfFillCols + numOfNotFillCols;
  pFillSup->interval = *pInterval;

  int32_t code = initResultBuf(pFillSup);
  if (code != TSDB_CODE_SUCCESS) {
    destroyStreamFillSupporter(pFillSup);
    return NULL;
  }

  SExprInfo* noFillExpr = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &numOfNotFillCols);
  code = initExprSupp(&pFillSup->notFillExprSup, noFillExpr, numOfNotFillCols);
  if (code != TSDB_CODE_SUCCESS) {
    destroyStreamFillSupporter(pFillSup);
    return NULL;
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pFillSup->pResMap = tSimpleHashInit(16, hashFn);
  pFillSup->hasDelete = false;
  return pFillSup;
}

SStreamFillInfo* initStreamFillInfo(SStreamFillSupporter* pFillSup, SSDataBlock* pRes) {
  SStreamFillInfo* pFillInfo = taosMemoryCalloc(1, sizeof(SStreamFillInfo));
  pFillInfo->start = INT64_MIN;
  pFillInfo->current = INT64_MIN;
  pFillInfo->end = INT64_MIN;
  pFillInfo->preRowKey = INT64_MIN;
  pFillInfo->needFill = false;
  pFillInfo->pLinearInfo = taosMemoryCalloc(1, sizeof(SStreamFillLinearInfo));
  pFillInfo->pLinearInfo->hasNext = false;
  pFillInfo->pLinearInfo->nextEnd = INT64_MIN;
  pFillInfo->pLinearInfo->pDeltaVal = NULL;
  pFillInfo->pLinearInfo->pNextDeltaVal = NULL;
  if (pFillSup->type == TSDB_FILL_LINEAR) {
    pFillInfo->pLinearInfo->pDeltaVal = taosArrayInit(pFillSup->numOfAllCols, sizeof(double));
    pFillInfo->pLinearInfo->pNextDeltaVal = taosArrayInit(pFillSup->numOfAllCols, sizeof(double));
    for (int32_t i = 0; i < pFillSup->numOfAllCols; i++) {
      double value = 0.0;
      taosArrayPush(pFillInfo->pLinearInfo->pDeltaVal, &value);
      taosArrayPush(pFillInfo->pLinearInfo->pNextDeltaVal, &value);
    }
  }
  pFillInfo->pLinearInfo->winIndex = 0;

  pFillInfo->pResRow = NULL;
  if (pFillSup->type == TSDB_FILL_SET_VALUE || pFillSup->type == TSDB_FILL_SET_VALUE_F 
   || pFillSup->type == TSDB_FILL_NULL || pFillSup->type == TSDB_FILL_NULL_F) {
    pFillInfo->pResRow = taosMemoryCalloc(1, sizeof(SResultRowData));
    pFillInfo->pResRow->key = INT64_MIN;
    pFillInfo->pResRow->pRowVal = taosMemoryCalloc(1, pFillSup->rowSize);
    for (int32_t i = 0; i < pFillSup->numOfAllCols; ++i) {
      SColumnInfoData* pColData = taosArrayGet(pRes->pDataBlock, i);
      SResultCellData* pCell = getResultCell(pFillInfo->pResRow, i);
      pCell->bytes = pColData->info.bytes;
      pCell->type = pColData->info.type;
    }
  }

  pFillInfo->type = pFillSup->type;
  pFillInfo->delRanges = taosArrayInit(16, sizeof(STimeRange));
  pFillInfo->delIndex = 0;
  return pFillInfo;
}

SOperatorInfo* createStreamFillOperatorInfo(SOperatorInfo* downstream, SStreamFillPhysiNode* pPhyFillNode,
                                            SExecTaskInfo* pTaskInfo) {
  SStreamFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamFillOperatorInfo));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SInterval* pInterval = &((SStreamIntervalOperatorInfo*)downstream->info)->interval;
  int32_t    numOfFillCols = 0;
  SExprInfo* pFillExprInfo = createExprInfo(pPhyFillNode->pFillExprs, NULL, &numOfFillCols);
  pInfo->pFillSup = initStreamFillSup(pPhyFillNode, pInterval, pFillExprInfo, numOfFillCols);
  if (!pInfo->pFillSup) {
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pRes = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  pInfo->pSrcBlock = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  blockDataEnsureCapacity(pInfo->pSrcBlock, pOperator->resultInfo.capacity);

  pInfo->pFillInfo = initStreamFillInfo(pInfo->pFillSup, pInfo->pRes);
  if (!pInfo->pFillInfo) {
    goto _error;
  }

  if (pInfo->pFillInfo->type == TSDB_FILL_SET_VALUE || pInfo->pFillInfo->type == TSDB_FILL_SET_VALUE_F) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      SVariant*        pVar = &(pFillCol->fillVal);
      if (pCell->type == TSDB_DATA_TYPE_FLOAT) {
        float v = 0;
        GET_TYPED_DATA(v, float, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (pCell->type == TSDB_DATA_TYPE_DOUBLE) {
        double v = 0;
        GET_TYPED_DATA(v, double, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else if (IS_SIGNED_NUMERIC_TYPE(pCell->type)) {
        int64_t v = 0;
        GET_TYPED_DATA(v, int64_t, pVar->nType, &pVar->i);
        SET_TYPED_DATA(pCell->pData, pCell->type, v);
      } else {
        pCell->isNull = true;
      }
    }
  } else if (pInfo->pFillInfo->type == TSDB_FILL_NULL || pInfo->pFillInfo->type == TSDB_FILL_NULL_F) {
    for (int32_t i = 0; i < pInfo->pFillSup->numOfAllCols; ++i) {
      SFillColInfo*    pFillCol = pInfo->pFillSup->pAllColInfo + i;
      int32_t          slotId = GET_DEST_SLOT_ID(pFillCol);
      SResultCellData* pCell = getResultCell(pInfo->pFillInfo->pResRow, slotId);
      pCell->isNull = true;
    }
  }

  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);
  blockDataEnsureCapacity(pInfo->pDelRes, pOperator->resultInfo.capacity);

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  int32_t code = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc,
                                     &numOfOutputCols, COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pPhyFillNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initExprSupp(&pOperator->exprSupp, pFillExprInfo, numOfFillCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->srcRowIndex = 0;
  setOperatorInfo(pOperator, "StreamFillOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamFill, NULL, destroyStreamFillOperatorInfo, optrDefaultBufFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

  _error:
  destroyStreamFillOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}
