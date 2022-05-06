#include "tdatablock.h"
#include "executorimpl.h"

static SSDataBlock* doSort(SOperatorInfo* pOperator);
static void destroyOrderOperatorInfo(void* param, int32_t numOfOutput);

SOperatorInfo* createSortOperatorInfo(SOperatorInfo* downstream, SSDataBlock* pResBlock, SArray* pSortInfo, SExprInfo* pExprInfo, int32_t numOfCols,
                                      SArray* pIndexMap, SExecTaskInfo* pTaskInfo) {
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
  pInfo->bufPageSize  = rowSize < 1024 ? 1024 * 2 : rowSize * 2;  // there are headers, so pageSize = rowSize + header

  pInfo->sortBufSize  = pInfo->bufPageSize * 16;  // TODO dynamic set the available sort buffer
  pInfo->pSortInfo    = pSortInfo;
  pInfo->inputSlotMap = pIndexMap;
  pOperator->name     = "SortOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  pOperator->blocking = true;
  pOperator->status   = OP_NOT_OPENED;
  pOperator->info     = pInfo;

  pOperator->pTaskInfo = pTaskInfo;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doSort, NULL, NULL, destroyOrderOperatorInfo, NULL, NULL, NULL);

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

// TODO merge aggregate super table
void appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle) {
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    bool isNull = tsortIsNullVal(pTupleHandle, i);
    if (isNull) {
      colDataAppend(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      char* pData = tsortGetValue(pTupleHandle, i);
      colDataAppend(pColInfo, pBlock->info.rows, pData, false);
    }
  }

  pBlock->info.rows += 1;
}

SSDataBlock* getSortedBlockData(SSortHandle* pHandle, SSDataBlock* pDataBlock, int32_t capacity) {
  blockDataCleanup(pDataBlock);
  blockDataEnsureCapacity(pDataBlock, capacity);

  blockDataEnsureCapacity(pDataBlock, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(pDataBlock, pTupleHandle);
    if (pDataBlock->info.rows >= capacity) {
      return pDataBlock;
    }
  }

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
    projectApplyFunctions(pOperator->pExpr, pBlock, pBlock, pSort->binfo.pCtx, pOperator->numOfExprs, NULL);
  }
}

SSDataBlock* doSort(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSortOperatorInfo* pInfo = pOperator->info;

  if (pOperator->status == OP_RES_TO_RETURN) {
    return getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  }

  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, pInfo->inputSlotMap, SORT_SINGLESOURCE_SORT,
                                             pInfo->bufPageSize, numOfBufPage, pInfo->binfo.pRes, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, applyScalarFunction, pOperator);


  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  ps->param = pOperator->pDownstream[0];
  tsortAddSource(pInfo->pSortHandle, ps);

  int32_t code = tsortOpen(pInfo->pSortHandle);
  taosMemoryFreeClear(ps);
  if (code != TSDB_CODE_SUCCESS) {
    longjmp(pTaskInfo->env, terrno);
  }

  pOperator->status = OP_RES_TO_RETURN;
  return getSortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pOperator->resultInfo.capacity);
}

void destroyOrderOperatorInfo(void* param, int32_t numOfOutput) {
  SSortOperatorInfo* pInfo = (SSortOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);

  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->inputSlotMap);
}
