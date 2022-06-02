#include "tdatablock.h"
#include "executorimpl.h"

static SSDataBlock* doSort(SOperatorInfo* pOperator);
static int32_t doOpenSortOperator(SOperatorInfo* pOperator);
static int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len);

static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput);

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSDataBlock* pResBlock, SArray* pSortInfo, SExprInfo* pExprInfo, int32_t numOfCols,
                                      SArray* pColMatchColInfo, SExecTaskInfo* pTaskInfo) {
  SSortOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  int32_t            rowSize = pResBlock->info.rowSize;

  if (pInfo == NULL || pOperator == NULL || rowSize > 100 * 1024 * 1024) {
    goto _error;
  }

  pOperator->pExpr    = pExprInfo;
  pOperator->numOfExprs = numOfCols;
  pInfo->binfo.pCtx   = createSqlFunctionCtx(pExprInfo, numOfCols, &pInfo->binfo.rowCellInfoOffset);
  pInfo->binfo.pRes   = pResBlock;

  initResultSizeInfo(pOperator, 1024);

  pInfo->pSortInfo    = pSortInfo;
  pInfo->pColMatchInfo= pColMatchColInfo;
  pOperator->name     = "SortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blocking = true;
  pOperator->status   = OP_NOT_OPENED;
  pOperator->info     = pInfo;

  // lazy evaluation for the following parameter since the input datablock is not known till now.
//  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;  // there are headers, so pageSize = rowSize + header
//  pInfo->sortBufSize  = pInfo->bufPageSize * 16;  // TODO dynamic set the available sort buffer

  pOperator->pTaskInfo = pTaskInfo;
  pOperator->fpSet =
      createOperatorFpSet(doOpenSortOperator, doSort, NULL, NULL, destroyOrderOperatorInfo, NULL, NULL, getExplainExecInfo);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

void appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    bool isNull = tsortIsNullVal(pTupleHandle, i);
    if (isNull) {
      colDataAppendNULL(pColInfo, pBlock->info.rows);
    } else {
      char* pData = tsortGetValue(pTupleHandle, i);
      colDataAppend(pColInfo, pBlock->info.rows, pData, false);
    }
  }

  pBlock->info.rows += 1;
}

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity, SArray* pColMatchInfo) {
  blockDataCleanup(pDataBlock);
  ASSERT(taosArrayGetSize(pColMatchInfo) == pDataBlock->info.numOfCols);

  SSDataBlock* p = tsortGetSortedDataBlock(pHandle);
  if (p == NULL) {
    return NULL;
  }

  blockDataEnsureCapacity(p, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(p, pTupleHandle);
    if (p->info.rows >= capacity) {
      return pDataBlock;
    }
  }

  if (p->info.rows > 0) {
    int32_t numOfCols = taosArrayGetSize(pColMatchInfo);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColMatchInfo* pmInfo = taosArrayGet(pColMatchInfo, i);
      ASSERT(pmInfo->matchType == COL_MATCH_FROM_SLOT_ID);

      SColumnInfoData* pSrc = taosArrayGet(p->pDataBlock, pmInfo->srcSlotId);
      SColumnInfoData* pDst = taosArrayGet(pDataBlock->pDataBlock, pmInfo->targetSlotId);
      colDataAssign(pDst, pSrc, p->info.rows);
    }

    pDataBlock->info.rows = p->info.rows;
    pDataBlock->info.capacity = p->info.rows;
  }

  blockDataDestroy(p);
  return (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
}

SSDataBlock* loadNextDataBlock(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  return pOperator->fpSet.getNextFn(pOperator);
}

// todo refactor: merged with fetch fp
void applyScalarFunction(SSDataBlock* pBlock, void* param) {
  SOperatorInfo* pOperator = param;
  SSortOperatorInfo* pSort = pOperator->info;
  if (pOperator->pExpr != NULL) {
    int32_t code = projectApplyFunctions(pOperator->pExpr, pBlock, pBlock, pSort->binfo.pCtx, pOperator->numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pOperator->pTaskInfo->env, code);
    }
  }
}

int32_t doOpenSortOperator(SOperatorInfo* pOperator) {
  SSortOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();

  //  pInfo->binfo.pRes is not equalled to the input datablock.
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, pInfo->pColMatchInfo, SORT_SINGLESOURCE_SORT,
                                             -1, -1, NULL, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, applyScalarFunction, pOperator);

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  ps->param = pOperator->pDownstream[0];
  tsortAddSource(pInfo->pSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pSortHandle);
  taosMemoryFreeClear(ps);

  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs)/1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doSort(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, code);
  }

  SSDataBlock* pBlock = getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity, pInfo->pColMatchInfo);

  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  } else {
    doSetOperatorCompleted(pOperator);
  }
  return pBlock;
}

void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->pColMatchInfo);
}

int32_t getExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
  SSortExecInfo* pInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SSortOperatorInfo *pOperatorInfo = (SSortOperatorInfo*)pOptr->info;

  *pInfo = tsortGetSortExecInfo(pOperatorInfo->pSortHandle);
  *pOptrExplain = pInfo;
  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}
