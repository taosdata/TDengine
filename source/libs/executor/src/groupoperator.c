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

#include "os.h"
#include "function.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorimpl.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

static void destroyGroupbyOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroy(pInfo->pGroupColVals);
}

static int32_t initGroupOptrInfo(SGroupbyOperatorInfo* pInfo, SArray* pGroupColList) {
  pInfo->pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if (pInfo->pGroupColVals == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = taosArrayGet(pGroupColList, i);
    pInfo->groupKeyLen += pCol->bytes;

    struct SGroupKeys key = {0};
    key.bytes = pCol->bytes;
    key.type = pCol->type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pCol->bytes);
    if (key.pData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush(pInfo->pGroupColVals, &key);
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  pInfo->keyBuf = taosMemoryCalloc(1, pInfo->groupKeyLen + nullFlagSize);

  if (pInfo->keyBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static bool groupKeyCompare(SGroupbyOperatorInfo* pInfo, SSDataBlock* pBlock, int32_t rowIndex,
                            int32_t numOfGroupCols) {
  SColumnDataAgg* pColAgg = NULL;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = taosArrayGet(pInfo->pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);
    if (pBlock->pBlockAgg != NULL) {
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    bool isNull = colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg);

    SGroupKeys* pkey = taosArrayGet(pInfo->pGroupColVals, i);
    if (pkey->isNull && isNull) {
      continue;
    }

    if (isNull || pkey->isNull) {
      return false;
    }

    char* val = colDataGetData(pColInfoData, rowIndex);

    if (IS_VAR_DATA_TYPE(pkey->type)) {
      int32_t len = varDataLen(val);
      if (len == varDataLen(pkey->pData) && memcmp(varDataVal(pkey->pData), varDataVal(val), len) == 0) {
        continue;
      } else {
        return false;
      }
    } else {
      if (memcmp(pkey->pData, val, pkey->bytes) != 0) {
        return false;
      }
    }
  }

  return true;
}

static void recordNewGroupKeys(SGroupbyOperatorInfo* pInfo, SSDataBlock* pBlock, int32_t rowIndex, int32_t numOfGroupCols) {
  SColumnDataAgg* pColAgg = NULL;

  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = taosArrayGet(pInfo->pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    if (pBlock->pBlockAgg != NULL) {
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    SGroupKeys* pkey = taosArrayGet(pInfo->pGroupColVals, i);
    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      pkey->isNull = true;
    } else {
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, varDataTLen(val));
      } else {
        memcpy(pkey->pData, val, pkey->bytes);
      }
    }
  }
}

static int32_t buildGroupKeys(void* pKey, const SArray* pGroupColVals) {
  ASSERT(pKey != NULL);
  size_t numOfGroupCols = taosArrayGetSize(pGroupColVals);

  char* isNull = (char*)pKey;
  char* pStart = (char*)pKey + sizeof(int8_t) * numOfGroupCols;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
    if (pkey->isNull) {
      isNull[i] = 1;
      continue;
    }

    isNull[i] = 0;
    if (IS_VAR_DATA_TYPE(pkey->type)) {
      varDataCopy(pStart, pkey->pData);
      pStart += varDataTLen(pkey->pData);
      ASSERT(varDataTLen(pkey->pData) <= pkey->bytes);
    } else {
      memcpy(pStart, pkey->pData, pkey->bytes);
      pStart += pkey->bytes;
    }
  }

  return (int32_t) (pStart - (char*)pKey);
}

// assign the group keys or user input constant values if required
static void doAssignGroupKeys(SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t totalRows, int32_t rowIndex) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (pCtx[i].functionId == -1) {
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[i]);

      SColumnInfoData* pColInfoData = pCtx[i].input.pData[0];
      if (!colDataIsNull(pColInfoData, totalRows, rowIndex, NULL)) {
        char* dest = GET_ROWCELL_INTERBUF(pEntryInfo);
        char* data = colDataGetData(pColInfoData, rowIndex);

        memcpy(dest, data, pColInfoData->info.bytes);
      } else { // it is a NULL value
        pEntryInfo->isNullRes = 1;
      }

      pEntryInfo->numOfRes = 1;
    }
  }
}

static void doHashGroupbyAgg(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;

  SqlFunctionCtx* pCtx = pInfo->binfo.pCtx;
  int32_t         numOfGroupCols = taosArrayGetSize(pInfo->pGroupCols);
  //  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
  // qError("QInfo:0x%"PRIx64" group by not supported on double/float columns, abort", GET_TASKID(pRuntimeEnv));
  //    return;
  //  }

  int32_t     len = 0;
  STimeWindow w = TSWINDOW_INITIALIZER;

  int32_t num = 0;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    // Compare with the previous row of this column, and do not set the output buffer again if they are identical.
    if (!pInfo->isInit) {
      recordNewGroupKeys(pInfo, pBlock, j, numOfGroupCols);
      pInfo->isInit = true;
      num++;
      continue;
    }

    bool equal = groupKeyCompare(pInfo, pBlock, j, numOfGroupCols);
    if (equal) {
      num++;
      continue;
    }

    // The first row of a new block does not belongs to the previous existed group
    if (!equal && j == 0) {
      num++;
      recordNewGroupKeys(pInfo, pBlock, j, numOfGroupCols);
      continue;
    }

    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf_rv(&(pInfo->binfo), pOperator->numOfOutput, pInfo->keyBuf, TSDB_DATA_TYPE_VARCHAR, len, 0, pInfo->aggSup.pResultBuf, pTaskInfo, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = j - num;
    doApplyFunctions(pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->numOfOutput, TSDB_ORDER_ASC);

    // assign the group keys or user input constant values if required
    doAssignGroupKeys(pCtx, pOperator->numOfOutput, pBlock->info.rows, rowIndex);
    recordNewGroupKeys(pInfo, pBlock, j, numOfGroupCols);
    num = 1;
  }

  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret =
        setGroupResultOutputBuf_rv(&(pInfo->binfo), pOperator->numOfOutput, pInfo->keyBuf, TSDB_DATA_TYPE_VARCHAR, len,
                                   0, pInfo->aggSup.pResultBuf, pTaskInfo, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    doApplyFunctions(pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->numOfOutput, TSDB_ORDER_ASC);
    doAssignGroupKeys(pCtx, pOperator->numOfOutput, pBlock->info.rows, rowIndex);
  }
}

static SSDataBlock* hashGroupbyAggregate(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    toSDatablock(&pInfo->groupResInfo, pInfo->aggSup.pResultBuf, pRes, pInfo->binfo.capacity, pInfo->binfo.rowCellInfoOffset);
    if (pRes->info.rows == 0 || !hasRemainDataInCurrentGroup(&pInfo->groupResInfo)) {
      pOperator->status = OP_EXEC_DONE;
    }
    return pRes;
  }

  int32_t        order = TSDB_ORDER_ASC;
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    publishOperatorProfEvent(downstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    SSDataBlock* pBlock = downstream->getNextFn(downstream, newgroup);
    publishOperatorProfEvent(downstream, QUERY_PROF_AFTER_OPERATOR_EXEC);
    if (pBlock == NULL) {
      break;
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, order);
    //    setTagValue(pOperator, pRuntimeEnv->current->pTable, pInfo->binfo.pCtx, pOperator->numOfOutput);
    doHashGroupbyAgg(pOperator, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->binfo.resultRowInfo);

  finalizeMultiTupleQueryResult(pInfo->binfo.pCtx, pOperator->numOfOutput, pInfo->aggSup.pResultBuf,
                                &pInfo->binfo.resultRowInfo, pInfo->binfo.rowCellInfoOffset);
  //  if (!stableQuery) { // finalize include the update of result rows
  //    finalizeQueryResult(pInfo->binfo.pCtx, pOperator->numOfOutput);
  //  } else {
  //    updateNumOfRowsInResultRows(pInfo->binfo.pCtx, pOperator->numOfOutput, &pInfo->binfo.resultRowInfo,
  //    pInfo->binfo.rowCellInfoOffset);
  //  }

  blockDataEnsureCapacity(pRes, pInfo->binfo.capacity);
  initGroupResInfo(&pInfo->groupResInfo, &pInfo->binfo.resultRowInfo);

  while(1) {
    toSDatablock(&pInfo->groupResInfo, pInfo->aggSup.pResultBuf, pRes, pInfo->binfo.capacity, pInfo->binfo.rowCellInfoOffset);
    doFilter(pInfo->pCondition, pRes);

    bool hasRemain = hasRemainDataInCurrentGroup(&pInfo->groupResInfo);
    if (!hasRemain) {
      pOperator->status = OP_EXEC_DONE;
      break;
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

  return pInfo->binfo.pRes;
}

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SArray* pGroupColList, SNode* pCondition, SExecTaskInfo* pTaskInfo,
                                       const STableGroupInfo* pTableGroupInfo) {
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pGroupCols = pGroupColList;
  pInfo->pCondition = pCondition;
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, 4096, pResultBlock, pTaskInfo->id.str);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  int32_t code = initGroupOptrInfo(pInfo, pGroupColList);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name         = "GroupbyAggOperator";
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  //  pOperator->operatorType = OP_Groupby;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->_openFn      = operatorDummyOpenFn;
  pOperator->getNextFn    = hashGroupbyAggregate;
  pOperator->closeFn      = destroyGroupbyOperatorInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock,
                                           SArray* pGroupColList, SNode* pCondition, SExecTaskInfo* pTaskInfo, const STableGroupInfo* pTableGroupInfo) {
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pGroupCols = pGroupColList;
  pInfo->pCondition = pCondition;
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, 4096, pResultBlock, pTaskInfo->id.str);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  int32_t code = initGroupOptrInfo(pInfo, pGroupColList);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name         = "PartitionByOperator";
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PARTITION;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->_openFn      = operatorDummyOpenFn;
  pOperator->getNextFn    = hashGroupbyAggregate;
  pOperator->closeFn      = destroyGroupbyOperatorInfo;

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}