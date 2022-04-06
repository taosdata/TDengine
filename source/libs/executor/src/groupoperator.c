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

static int32_t buildGroupValKey(void* pKey, int32_t* length, SArray* pGroupColVals) {
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

  *length = (pStart - (char*)pKey);
  return 0;
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

    /*int32_t ret = */ buildGroupValKey(pInfo->keyBuf, &len, pInfo->pGroupColVals);
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
    /*int32_t ret = */ buildGroupValKey(pInfo->keyBuf, &len, pInfo->pGroupColVals);
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

#define MULTI_KEY_DELIM "-"

static void destroyDistinctOperatorInfo(void* param, int32_t numOfOutput) {
  SDistinctOperatorInfo* pInfo = (SDistinctOperatorInfo*)param;
  taosHashCleanup(pInfo->pSet);
  taosMemoryFreeClear(pInfo->buf);
  taosArrayDestroy(pInfo->pDistinctDataInfo);
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

static void buildMultiDistinctKey(SDistinctOperatorInfo* pInfo, SSDataBlock* pBlock, int32_t rowId) {
  char* p = pInfo->buf;
//  memset(p, 0, pInfo->totalBytes);

  for (int i = 0; i < taosArrayGetSize(pInfo->pDistinctDataInfo); i++) {
    SDistinctDataInfo* pDistDataInfo = (SDistinctDataInfo*)taosArrayGet(pInfo->pDistinctDataInfo, i);
    SColumnInfoData*   pColDataInfo = taosArrayGet(pBlock->pDataBlock, pDistDataInfo->index);

    char* val = ((char*)pColDataInfo->pData) + pColDataInfo->info.bytes * rowId;
    if (isNull(val, pDistDataInfo->type)) {
      p += pDistDataInfo->bytes;
      continue;
    }
    if (IS_VAR_DATA_TYPE(pDistDataInfo->type)) {
      memcpy(p, varDataVal(val), varDataLen(val));
      p += varDataLen(val);
    } else {
      memcpy(p, val, pDistDataInfo->bytes);
      p += pDistDataInfo->bytes;
    }
    memcpy(p, MULTI_KEY_DELIM, strlen(MULTI_KEY_DELIM));
    p += strlen(MULTI_KEY_DELIM);
  }
}

static bool initMultiDistinctInfo(SDistinctOperatorInfo* pInfo, SOperatorInfo* pOperator) {
  for (int i = 0; i < pOperator->numOfOutput; i++) {
    //    pInfo->totalBytes += pOperator->pExpr[i].base.colBytes;
  }
#if 0
  for (int i = 0; i < pOperator->numOfOutput; i++) {
    int numOfCols = (int)(taosArrayGetSize(pBlock->pDataBlock));
    assert(i < numOfCols);
    
    for (int j = 0; j < numOfCols; j++) {
      SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, j);
      if (pColDataInfo->info.colId == pOperator->pExpr[i].base.resSchema.colId) {
        SDistinctDataInfo item = {.index = j, .type = pColDataInfo->info.type, .bytes = pColDataInfo->info.bytes};
        taosArrayInsert(pInfo->pDistinctDataInfo, i, &item);
      }
    }
  }
#endif

//  pInfo->totalBytes += (int32_t)strlen(MULTI_KEY_DELIM) * (pOperator->numOfOutput);
//  pInfo->buf = taosMemoryCalloc(1, pInfo->totalBytes);
  return taosArrayGetSize(pInfo->pDistinctDataInfo) == pOperator->numOfOutput ? true : false;
}

static SSDataBlock* hashDistinct(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SDistinctOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*           pRes = pInfo->pRes;

  pRes->info.rows = 0;
  SSDataBlock* pBlock = NULL;

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
  while (1) {
    publishOperatorProfEvent(pDownstream, QUERY_PROF_BEFORE_OPERATOR_EXEC);
    pBlock = pDownstream->getNextFn(pDownstream, newgroup);
    publishOperatorProfEvent(pDownstream, QUERY_PROF_AFTER_OPERATOR_EXEC);

    if (pBlock == NULL) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    // ensure result output buf
    if (pRes->info.rows + pBlock->info.rows > pInfo->resInfo.capacity) {
      int32_t newSize = pRes->info.rows + pBlock->info.rows;
      for (int i = 0; i < taosArrayGetSize(pRes->pDataBlock); i++) {
        SColumnInfoData*   pResultColInfoData = taosArrayGet(pRes->pDataBlock, i);
        SDistinctDataInfo* pDistDataInfo = taosArrayGet(pInfo->pDistinctDataInfo, i);

//        char* tmp = taosMemoryRealloc(pResultColInfoData->pData, newSize * pDistDataInfo->bytes);
//        if (tmp == NULL) {
//          return NULL;
//        } else {
//          pResultColInfoData->pData = tmp;
//        }
      }
      pInfo->resInfo.capacity = newSize;
    }

    for (int32_t i = 0; i < pBlock->info.rows; i++) {
      buildMultiDistinctKey(pInfo, pBlock, i);
      if (taosHashGet(pInfo->pSet, pInfo->buf, 0) == NULL) {
        taosHashPut(pInfo->pSet, pInfo->buf, 0, NULL, 0);

        for (int j = 0; j < taosArrayGetSize(pRes->pDataBlock); j++) {
          SDistinctDataInfo* pDistDataInfo = taosArrayGet(pInfo->pDistinctDataInfo, j);  // distinct meta info
          SColumnInfoData*   pColInfoData = taosArrayGet(pBlock->pDataBlock, pDistDataInfo->index);  // src
          SColumnInfoData*   pResultColInfoData = taosArrayGet(pRes->pDataBlock, j);                 // dist

          char* val = ((char*)pColInfoData->pData) + pDistDataInfo->bytes * i;
          char* start = pResultColInfoData->pData + pDistDataInfo->bytes * pInfo->pRes->info.rows;
          memcpy(start, val, pDistDataInfo->bytes);
        }

        pRes->info.rows += 1;
      }
    }

    if (pRes->info.rows >= pInfo->resInfo.threshold) {
      break;
    }
  }

  return (pInfo->pRes->info.rows > 0) ? pInfo->pRes : NULL;
}

SOperatorInfo* createDistinctOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExpr, int32_t numOfCols, SSDataBlock* pResBlock, SExecTaskInfo* pTaskInfo) {
  SDistinctOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SDistinctOperatorInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pOperator->resultInfo.capacity = 4096;  // todo extract function.

//  pInfo->totalBytes        = 0;
  pInfo->buf               = NULL;

  pInfo->pDistinctDataInfo = taosArrayInit(numOfCols, sizeof(SDistinctDataInfo));
  initMultiDistinctInfo(pInfo, pOperator);
  
  pInfo->pSet = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  pOperator->name         = "DistinctOperator";
  pOperator->blockingOptr = true;
  pOperator->status       = OP_NOT_OPENED;
//  pOperator->operatorType = DISTINCT;
  pOperator->pExpr        = pExpr;
  pOperator->numOfOutput  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->getNextFn    = hashDistinct;
  pOperator->closeFn      = destroyDistinctOperatorInfo;

  int32_t code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}
