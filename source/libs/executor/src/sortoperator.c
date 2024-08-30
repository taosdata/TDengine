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
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

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
  SSortOpGroupIdCalc* pGroupIdCalc;
} SSortOperatorInfo;

static int32_t      doSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock);
static SSDataBlock* doSort1(SOperatorInfo* pOperator);
static int32_t      doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t      getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);
static SSDataBlock* doGroupSort1(SOperatorInfo* pOperator);
static int32_t doGroupSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock);

static void destroySortOperatorInfo(void* param);
static void calcSortOperMaxTupleLength(SSortOperatorInfo* pSortOperInfo, SNodeList* pSortKeys);

static void destroySortOpGroupIdCalc(SSortOpGroupIdCalc* pCalc);

// todo add limit/offset impl
int32_t createSortOperatorInfo(SOperatorInfo* downstream, SSortPhysiNode* pSortNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t code = 0;
  int32_t lino = 0;

  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

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
  if (pSortNode->node.pLimit) {
    SLimitNode* pLimit = (SLimitNode*)pSortNode->node.pLimit;
    if (pLimit->limit > 0) pInfo->maxRows = pLimit->limit + pLimit->offset;
  }

  pOperator->exprSupp.pCtx =
      createSqlFunctionCtx(pOperator->exprSupp.pExprInfo, numOfCols, &pOperator->exprSupp.rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_NULL(pOperator->exprSupp.pCtx, code, lino, _error, terrno);
  initResultSizeInfo(&pOperator->resultInfo, 1024);
  code = filterInitFromNode((SNode*)pSortNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);

  pInfo->pSortInfo = createSortInfo(pSortNode->pSortKeys);
  TSDB_CHECK_NULL(pInfo->pSortInfo, code, lino, _error, terrno);

  if (pSortNode->calcGroupId) {
    int32_t keyLen;
    SSortOpGroupIdCalc* pGroupIdCalc = pInfo->pGroupIdCalc = taosMemoryCalloc(1, sizeof(SSortOpGroupIdCalc));
    if (!pGroupIdCalc) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    SNodeList* pSortColsNodeArr = makeColsNodeArrFromSortKeys(pSortNode->pSortKeys);
    if (!pSortColsNodeArr) code = TSDB_CODE_OUT_OF_MEMORY;
    if (TSDB_CODE_SUCCESS == code) {
      pGroupIdCalc->pSortColsArr = makeColumnArrayFromList(pSortColsNodeArr);
      if (!pGroupIdCalc->pSortColsArr) code = TSDB_CODE_OUT_OF_MEMORY;
      nodesClearList(pSortColsNodeArr);
    }
    if (TSDB_CODE_SUCCESS == code) {
      // PK ts col should always at last, see partColOptCreateSort
      if (pSortNode->excludePkCol) taosArrayPop(pGroupIdCalc->pSortColsArr);
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

  pInfo->binfo.inputTsOrder = pSortNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pSortNode->node.outputTsOrder;
  initLimitInfo(pSortNode->node.pLimit, pSortNode->node.pSlimit, &pInfo->limitInfo);

  setOperatorInfo(pOperator, "SortOperator", QUERY_NODE_PHYSICAL_PLAN_SORT, true, OP_NOT_OPENED, pInfo, pTaskInfo);


  // lazy evaluation for the following parameter since the input datablock is not known till now.
  //  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;
  //  there are headers, so pageSize = rowSize + header pInfo->sortBufSize  = pInfo->bufPageSize * 16;
  // TODO dynamic set the available sort buffer

  pOperator->fpSet =
      createOperatorFpSet(doOpenSortOperator, doSort1, NULL, destroySortOperatorInfo, optrDefaultBufFn, getExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroySortOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    if (pOperator->pDownstream == NULL && downstream != NULL) {
      destroyOperator(downstream);
    }
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

int32_t appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  int32_t code = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfo == NULL) {
      return terrno;
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

  SDataBlockInfo info = {0};
  tsortGetBlockInfo(pTupleHandle, &info);

  pBlock->info.scanFlag = info.scanFlag;
  pBlock->info.rows += 1;
  return code;
}

/**
 * @brief get next tuple with group id attached, here assume that all tuples are sorted by group keys
 * @param [in, out] pBlock the output block, the group id will be saved in it
 * @retval NULL if next group tuple arrived and this new group tuple will be saved in pInfo.pSavedTuple
 * @retval NULL if no more tuples
 */
static STupleHandle* nextTupleWithGroupId(SSortHandle* pHandle, SSortOperatorInfo* pInfo, SSDataBlock* pBlock) {
  int32_t code = 0;
  STupleHandle* retTuple = pInfo->pGroupIdCalc->pSavedTuple;
  if (!retTuple) {
    code = tsortNextTuple(pHandle, &retTuple);
    if (code) {
      return NULL;
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
    }
    bool emptyBlock = pBlock->info.rows == 0;
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

  return retTuple;
}

static int32_t getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                SSortOperatorInfo* pInfo, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);
  blockDataCleanup(pDataBlock);
  int32_t lino = 0;
  int32_t code = 0;

  SSDataBlock* p = NULL;
  code = tsortGetSortedDataBlock(pHandle, &p);
  if (p == NULL || (code != 0)) {
    return code;
  }

  code = blockDataEnsureCapacity(p, capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  STupleHandle* pTupleHandle;
  while (1) {
    if (pInfo->pGroupIdCalc) {
      pTupleHandle = nextTupleWithGroupId(pHandle, pInfo, p);
    } else {
      code = tsortNextTuple(pHandle, &pTupleHandle);
    }

    if (pTupleHandle == NULL || code != 0) {
      lino = __LINE__;
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

    // todo extract function to handle this
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, i);
      QUERY_CHECK_NULL(pmInfo, code, lino, _error, terrno);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      QUERY_CHECK_NULL(pSrc, code, lino, _error, terrno);

      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->dstSlotId);
      QUERY_CHECK_NULL(pDst, code, lino, _error, terrno);

      code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      QUERY_CHECK_CODE(code, lino, _error);
    }

    pDataBlock->info.dataLoad = 1;
    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
    pDataBlock->info.id.groupId = p->info.id.groupId;
  }

  blockDataDestroy(p);
  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;

  _error:
  qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));

  blockDataDestroy(p);
  return code;
}

int32_t loadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  *ppBlock = pOperator->fpSet.getNextFn(pOperator);
  return TSDB_CODE_SUCCESS;
}

// todo refactor: merged with fetch fp
void applyScalarFunction(SSDataBlock* pBlock, void* param) {
  SOperatorInfo*     pOperator = param;
  SSortOperatorInfo* pSort = pOperator->info;
  if (pOperator->exprSupp.pExprInfo != NULL && pOperator->exprSupp.numOfExprs > 0) {
    int32_t code = projectApplyFunctions(pOperator->exprSupp.pExprInfo, pBlock, pBlock, pOperator->exprSupp.pCtx,
                                         pOperator->exprSupp.numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }
  }
}

int32_t doOpenSortOperator(SOperatorInfo* pOperator) {
  SSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();
  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pSortHandle = NULL;
  int32_t code = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str,
                                             pInfo->maxRows, pInfo->maxTupleLength, tsPQSortMemThreshold * 1024 * 1024, &pInfo->pSortHandle);
  if (code) {
    return code;
  }

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, applyScalarFunction, pOperator);

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  if (ps == NULL) {
    return terrno;
  }

  ps->param = pOperator->pDownstream[0];
  ps->onlyRef = true;

  code = tsortAddSource(pInfo->pSortHandle, ps);
  if (code) {
    taosMemoryFree(ps);
    return code;
  }

  code = tsortOpen(pInfo->pSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return code;
}

SSDataBlock* doSort1(SOperatorInfo* pOperator) {
  SSDataBlock* pBlock = NULL;
  pOperator->pTaskInfo->code = doSort(pOperator, &pBlock);
  return pBlock;
}

int32_t doSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);
  if (pOperator->status == OP_EXEC_DONE) {
    return 0;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  // multi-group case not handle here
  SSDataBlock* pBlock = NULL;
  while (1) {
    if (tsortIsClosed(pInfo->pSortHandle)) {
      code = TSDB_CODE_TSC_QUERY_CANCELLED;
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    code = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                pInfo->matchInfo.pList, pInfo, &pBlock);
    if (pBlock == NULL || code != 0) {
      setOperatorCompleted(pOperator);
      return code;
    }

    code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    if (code) {
      break;
    }

    if (blockDataGetNumOfRows(pBlock) == 0) {
      continue;
    }

    // there are bugs?
    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
    if (limitReached) {
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }

    pOperator->resultInfo.totalRows += pBlock->info.rows;
    if (pBlock->info.rows > 0) {
      break;
    }
  }

  *pResBlock = blockDataGetNumOfRows(pBlock) > 0 ? pBlock : NULL;
  return code;
}

void destroySortOperatorInfo(void* param) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  blockDataDestroy(pInfo->binfo.pRes);
  pInfo->binfo.pRes = NULL;

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

  size = LIST_LENGTH(pSortKeys);
  for (size_t i = 0; i < size; ++i) {
    SOrderByExprNode* pOrderExprNode = (SOrderByExprNode*)nodesListGetNode(pSortKeys, i);
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
  SSDataBlock*         prefetchedSortInput;
  SSortHandle*         pCurrSortHandle;
  EChildOperatorStatus childOpStatus;
  SSortExecInfo        sortExecInfo;
} SGroupSortOperatorInfo;

int32_t getGroupSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo,
                                SGroupSortOperatorInfo* pInfo, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);

  blockDataCleanup(pDataBlock);
  int32_t code = blockDataEnsureCapacity(pDataBlock, capacity);
  if (code) {
    return code;
  }

  SSDataBlock* p = NULL;
  code = tsortGetSortedDataBlock(pHandle, &p);
  if (p == NULL || (code != 0)) {
    return code;
  }

  code = blockDataEnsureCapacity(p, capacity);
  if (code) {
    return code;
  }

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

      code = colDataAssign(pDst, pSrc, p->info.rows, &pDataBlock->info);
      if (code) {
        return code;
      }
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.capacity = p->info.rows;
    pDataBlock->info.scanFlag = p->info.scanFlag;
  }

  blockDataDestroy(p);
  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

typedef struct SGroupSortSourceParam {
  SOperatorInfo*          childOpInfo;
  SGroupSortOperatorInfo* grpSortOpInfo;
} SGroupSortSourceParam;

int32_t fetchNextGroupSortDataBlock(void* param, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  
  SGroupSortSourceParam*  source = param;
  SGroupSortOperatorInfo* grpSortOpInfo = source->grpSortOpInfo;
  if (grpSortOpInfo->prefetchedSortInput) {
    SSDataBlock* block = grpSortOpInfo->prefetchedSortInput;
    grpSortOpInfo->prefetchedSortInput = NULL;
    *ppBlock = block;
  } else {
    SOperatorInfo* childOp = source->childOpInfo;
    SSDataBlock*   block = childOp->fpSet.getNextFn(childOp);
    if (block != NULL) {
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

  return TSDB_CODE_SUCCESS;
}

int32_t beginSortGroup(SOperatorInfo* pOperator) {
  SGroupSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pCurrSortHandle = NULL;

  int32_t code = tsortCreateSortHandle(pInfo->pSortInfo, SORT_SINGLESOURCE_SORT, -1, -1, NULL, pTaskInfo->id.str, 0, 0,
                                       0, &pInfo->pCurrSortHandle);
  if (code) {
    return code;
  }

  tsortSetFetchRawDataFp(pInfo->pCurrSortHandle, fetchNextGroupSortDataBlock, applyScalarFunction, pOperator);

  SSortSource*           ps = taosMemoryCalloc(1, sizeof(SSortSource));
  SGroupSortSourceParam* param = taosMemoryCalloc(1, sizeof(SGroupSortSourceParam));
  if (ps == NULL || param == NULL) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  param->childOpInfo = pOperator->pDownstream[0];
  param->grpSortOpInfo = pInfo;
  ps->param = param;
  ps->onlyRef = false;
  code = tsortAddSource(pInfo->pCurrSortHandle, ps);
  if (code) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  code = tsortOpen(pInfo->pCurrSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

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

SSDataBlock* doGroupSort1(SOperatorInfo* pOperator) {
  SSDataBlock* pBlock = NULL;
  pOperator->pTaskInfo->code = doGroupSort(pOperator, &pBlock);
  return pBlock;
}

int32_t doGroupSort(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  QRY_OPTR_CHECK(pResBlock);
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SGroupSortOperatorInfo* pInfo = pOperator->info;

  if (pOperator->status == OP_EXEC_DONE) {
    return 0;
  }

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    pInfo->prefetchedSortInput = getNextBlockFromDownstream(pOperator, 0);
    if (pInfo->prefetchedSortInput == NULL) {
      setOperatorCompleted(pOperator);
      return code;
    }

    pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
    pInfo->childOpStatus = CHILD_OP_NEW_GROUP;
    code = beginSortGroup(pOperator);
    if (code) {
      return code;
    }
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->pCurrSortHandle != NULL) {
    if (tsortIsClosed(pInfo->pCurrSortHandle)) {
      code = TSDB_CODE_TSC_QUERY_CANCELLED;
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    // beginSortGroup would fetch all child blocks of pInfo->currGroupId;
    if (pInfo->childOpStatus == CHILD_OP_SAME_GROUP) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      pOperator->pTaskInfo->code = code;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    code = getGroupSortedBlockData(pInfo->pCurrSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity,
                                     pInfo->matchInfo.pList, pInfo, &pBlock);
    if (pBlock != NULL && (code == 0)) {
      pBlock->info.id.groupId = pInfo->currGroupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      *pResBlock = pBlock;
      return code;
    } else {
      if (pInfo->childOpStatus == CHILD_OP_NEW_GROUP) {
        (void) finishSortGroup(pOperator);
        pInfo->currGroupId = pInfo->prefetchedSortInput->info.id.groupId;
        code = beginSortGroup(pOperator);
      } else if (pInfo->childOpStatus == CHILD_OP_FINISHED) {
        (void) finishSortGroup(pOperator);
        setOperatorCompleted(pOperator);
        return code;
      }
    }
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

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);

  tsortDestroySortHandle(pInfo->pCurrSortHandle);
  pInfo->pCurrSortHandle = NULL;

  taosMemoryFreeClear(param);
}

int32_t createGroupSortOperatorInfo(SOperatorInfo* downstream, SGroupSortPhysiNode* pSortPhyNode,
                                    SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;

  SGroupSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupSortOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

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

  pInfo->binfo.inputTsOrder = pSortPhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pSortPhyNode->node.outputTsOrder;

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pSortPhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                             &pInfo->matchInfo);
  TSDB_CHECK_CODE(code, lino, _error);

  pInfo->pSortInfo = createSortInfo(pSortPhyNode->pSortKeys);
  setOperatorInfo(pOperator, "GroupSortOperator", QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doGroupSort1, NULL, destroyGroupSortOperatorInfo,
                                         optrDefaultBufFn, getGroupSortExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return code;

_error:
  pTaskInfo->code = code;
  if (pInfo != NULL) {
    destroyGroupSortOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    if (pOperator->pDownstream == NULL && downstream != NULL) {
      destroyOperator(downstream);
    }
    destroyOperator(pOperator);
  }
  return code;
}
