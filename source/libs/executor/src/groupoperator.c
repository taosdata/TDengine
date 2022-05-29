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

static int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity);
static void* getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len);
static uint64_t calcGroupId(char* pData, int32_t len);

static void destroyGroupOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroy(pInfo->pGroupColVals);
}

static int32_t initGroupOptrInfo(SArray** pGroupColVals, int32_t* keyLen, char** keyBuf, const SArray* pGroupColList) {
  *pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if ((*pGroupColVals) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = taosArrayGet(pGroupColList, i);
    (*keyLen) += pCol->bytes; // actual data + null_flag

    SGroupKeys key = {0};
    key.bytes  = pCol->bytes;
    key.type   = pCol->type;
    key.isNull = false;
    key.pData  = taosMemoryCalloc(1, pCol->bytes);
    if (key.pData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush((*pGroupColVals), &key);
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  (*keyLen) += nullFlagSize;

  (*keyBuf) = taosMemoryCalloc(1, (*keyLen));
  if ((*keyBuf) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static bool groupKeyCompare(SArray* pGroupCols, SArray* pGroupColVals, SSDataBlock* pBlock, int32_t rowIndex, int32_t numOfGroupCols) {
  SColumnDataAgg* pColAgg = NULL;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);
    if (pBlock->pBlockAgg != NULL) {
      pColAgg = pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    bool isNull = colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg);

    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
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

static void recordNewGroupKeys(SArray* pGroupCols, SArray* pGroupColVals, SSDataBlock* pBlock, int32_t rowIndex, int32_t numOfGroupCols) {
  SColumnDataAgg* pColAgg = NULL;

  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    if (pBlock->pBlockAgg != NULL) {
      pColAgg = pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      pkey->isNull = true;
    } else {
      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      if (IS_VAR_DATA_TYPE(pkey->type)) {
        memcpy(pkey->pData, val, varDataTLen(val));
        ASSERT(varDataTLen(val) <= pkey->bytes);
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
    if (pCtx[i].functionId == -1) {       // select count(*),key from t group by key.
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[i]);

      SColumnInfoData* pColInfoData = pCtx[i].input.pData[0];
      // todo OPT all/all not NULL
      if (!colDataIsNull(pColInfoData, totalRows, rowIndex, NULL)) {
        char* dest = GET_ROWCELL_INTERBUF(pEntryInfo);
        char* data = colDataGetData(pColInfoData, rowIndex);

        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          varDataCopy(dest, data);
        } else {
          memcpy(dest, data, pColInfoData->info.bytes);
        }
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
      recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
      pInfo->isInit = true;
      num++;
      continue;
    }

    bool equal = groupKeyCompare(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
    if (equal) {
      num++;
      continue;
    }

    // The first row of a new block does not belongs to the previous existed group
    if (j == 0) {
      num++;
      recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
      continue;
    }

    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf(&(pInfo->binfo), pOperator->numOfExprs, pInfo->keyBuf, TSDB_DATA_TYPE_VARCHAR, len, 0, pInfo->aggSup.pResultBuf, pTaskInfo, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = j - num;
    doApplyFunctions(pTaskInfo, pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->numOfExprs, TSDB_ORDER_ASC);

    // assign the group keys or user input constant values if required
    doAssignGroupKeys(pCtx, pOperator->numOfExprs, pBlock->info.rows, rowIndex);
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
    num = 1;
  }

  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret =
        setGroupResultOutputBuf(&(pInfo->binfo), pOperator->numOfExprs, pInfo->keyBuf, TSDB_DATA_TYPE_VARCHAR, len,
                                   0, pInfo->aggSup.pResultBuf, pTaskInfo, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    doApplyFunctions(pTaskInfo, pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->numOfExprs, TSDB_ORDER_ASC);
    doAssignGroupKeys(pCtx, pOperator->numOfExprs, pBlock->info.rows, rowIndex);
  }
}

static SSDataBlock* hashGroupbyAggregate(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

    size_t rows = pRes->info.rows;
    if (rows == 0 || !hashRemainDataInGroupInfo(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
    }

    pOperator->resultInfo.totalRows += rows;
    return (pRes->info.rows == 0)? NULL:pRes;
  }

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;

  int64_t st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    int32_t code = getTableScanInfo(pOperator, &order, &scanFlag);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, code);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(pOperator, pInfo->binfo.pCtx, pBlock, order, scanFlag, true);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->pScalarExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->pScalarExprInfo, pBlock, pBlock, pInfo->pScalarFuncCtx, pInfo->numOfScalarExpr, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doHashGroupbyAgg(pOperator, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;
  closeAllResultRows(&pInfo->binfo.resultRowInfo);
  //  if (!stableQuery) { // finalize include the update of result rows
  //    finalizeQueryResult(pInfo->binfo.pCtx, pOperator->numOfExprs);
  //  } else {
  //    updateNumOfRowsInResultRows(pInfo->binfo.pCtx, pOperator->numOfExprs, &pInfo->binfo.resultRowInfo,
  //    pInfo->binfo.rowCellInfoOffset);
  //  }

  blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, 0);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  while(1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pInfo->pCondition, pRes, NULL);

    bool hasRemain = hashRemainDataInGroupInfo(&pInfo->groupResInfo);
    if (!hasRemain) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

  size_t rows = pRes->info.rows;
  pOperator->resultInfo.totalRows += rows;

  return (rows == 0)? NULL:pRes;
}

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SArray* pGroupColList,
    SNode* pCondition, SExprInfo* pScalarExprInfo, int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo) {
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pGroupCols      = pGroupColList;
  pInfo->pCondition      = pCondition;

  pInfo->pScalarExprInfo = pScalarExprInfo;
  pInfo->numOfScalarExpr = numOfScalarExpr;
  pInfo->pScalarFuncCtx  = createSqlFunctionCtx(pScalarExprInfo, numOfScalarExpr, &pInfo->rowCellInfoOffset);

  int32_t code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pGroupColList);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultSizeInfo(pOperator, 4096);
  initAggInfo(&pInfo->binfo, &pInfo->aggSup, pExprInfo, numOfCols, pResultBlock, pInfo->groupKeyLen, pTaskInfo->id.str);
  initResultRowInfo(&pInfo->binfo.resultRowInfo, 8);

  pOperator->name         = "GroupbyAggOperator";
  pOperator->blocking     = true;
  pOperator->status       = OP_NOT_OPENED;
  // pOperator->operatorType = OP_Groupby;
  pOperator->pExpr        = pExprInfo;
  pOperator->numOfExprs  = numOfCols;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, hashGroupbyAggregate, NULL, NULL, destroyGroupOperatorInfo, aggEncodeResultRow, aggDecodeResultRow, NULL);
  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void doHashPartition(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
//  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SPartitionOperatorInfo* pInfo = pOperator->info;

  int32_t numOfGroupCols = taosArrayGetSize(pInfo->pGroupCols);
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
    int32_t len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);

    SDataGroupInfo* pGInfo = NULL;
    void *pPage = getCurrentDataGroupInfo(pInfo, &pGInfo, len);

    pGInfo->numOfRows += 1;
    if (pGInfo->groupId == 0) {
      pGInfo->groupId = calcGroupId(pInfo->keyBuf, len);
    }

    // number of rows
    int32_t* rows = (int32_t*) pPage;

    // group id

    size_t numOfCols = pOperator->numOfExprs;
    for(int32_t i = 0; i < numOfCols; ++i) {
      SExprInfo* pExpr = &pOperator->pExpr[i];
      int32_t slotId = pExpr->base.pParam[0].pCol->slotId;

      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);

      int32_t bytes = pColInfoData->info.bytes;
      int32_t startOffset = pInfo->columnOffset[i];

      int32_t* columnLen  = NULL;
      int32_t  contentLen = 0;

      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        int32_t* offset = (int32_t*)((char*)pPage + startOffset);
        columnLen       = (int32_t*) ((char*)pPage + startOffset + sizeof(int32_t) * pInfo->rowCapacity);
        char*    data   = (char*)((char*) columnLen + sizeof(int32_t));

        if (colDataIsNull_s(pColInfoData, j)) {
          offset[(*rows)] = -1;
          contentLen = 0;
        } else {
          offset[*rows] = (*columnLen);
          char* src = colDataGetData(pColInfoData, j);
          memcpy(data + (*columnLen), src, varDataTLen(src));
          int32_t v = (data + (*columnLen) + varDataTLen(src) - (char*)pPage);
          ASSERT(v > 0);

          contentLen = varDataTLen(src);
        }
      } else {
        char* bitmap = (char*)pPage + startOffset;
        columnLen    = (int32_t*) ((char*)pPage + startOffset + BitmapLen(pInfo->rowCapacity));
        char* data   = (char*) columnLen + sizeof(int32_t);

        bool isNull = colDataIsNull_f(pColInfoData->nullbitmap, j);
        if (isNull) {
          colDataSetNull_f(bitmap, (*rows));
        } else {
          memcpy(data + (*columnLen), colDataGetData(pColInfoData, j), bytes);
        }
        contentLen = bytes;
      }

      (*columnLen) += contentLen;
      ASSERT(*columnLen >= 0);
    }

    (*rows) += 1;

    setBufPageDirty(pPage, true);
    releaseBufPage(pInfo->pBuf, pPage);
  }
}

void* getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len) {
  SDataGroupInfo* p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);

  void* pPage = NULL;
  if (p == NULL) { // it is a new group
    SDataGroupInfo gi = {0};
    gi.pPageList = taosArrayInit(100, sizeof(int32_t));
    taosHashPut(pInfo->pGroupSet, pInfo->keyBuf, len, &gi, sizeof(SDataGroupInfo));

    p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);

    int32_t pageId = 0;
    pPage = getNewBufPage(pInfo->pBuf, 0, &pageId);
    taosArrayPush(p->pPageList, &pageId);

    *(int32_t *) pPage = 0;
  } else {
    int32_t* curId = taosArrayGetLast(p->pPageList);
    pPage = getBufPage(pInfo->pBuf, *curId);

    int32_t *rows = (int32_t*) pPage;
    if (*rows >= pInfo->rowCapacity) {
      // release buffer
      releaseBufPage(pInfo->pBuf, pPage);

      // add a new page for current group
      int32_t pageId = 0;
      pPage = getNewBufPage(pInfo->pBuf, 0, &pageId);
      taosArrayPush(p->pPageList, &pageId);
      memset(pPage, 0, getBufPageSize(pInfo->pBuf));
    }
  }

  *pGroupInfo = p;
  return pPage;
}

uint64_t calcGroupId(char* pData, int32_t len) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pData, len);
  tMD5Final(&context);

  // NOTE: only extract the initial 8 bytes of the final MD5 digest
  uint64_t id = 0;
  memcpy(&id, context.digest, sizeof(uint64_t));
  return id;
}

int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity) {
  size_t numOfCols = pBlock->info.numOfCols;
  int32_t* offset = taosMemoryCalloc(pBlock->info.numOfCols, sizeof(int32_t));

  offset[0] = sizeof(int32_t) + sizeof(uint64_t);  // the number of rows in current page, ref to SSDataBlock paged serialization format

  for(int32_t i = 0; i < numOfCols - 1; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    int32_t bytes = pColInfoData->info.bytes;
    int32_t payloadLen = bytes * rowCapacity;
    
    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      // offset segment + content length + payload
      offset[i + 1] = rowCapacity * sizeof(int32_t) + sizeof(int32_t) + payloadLen + offset[i];
    } else {
      // bitmap + content length + payload
      offset[i + 1] = BitmapLen(rowCapacity) + sizeof(int32_t) + payloadLen + offset[i];
    }
  }

  return offset;
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;

  SDataGroupInfo* pGroupInfo = pInfo->pGroupIter;
  if (pInfo->pGroupIter == NULL || pInfo->pageIndex >= taosArrayGetSize(pGroupInfo->pPageList)) {
    // try next group data
    pInfo->pGroupIter = taosHashIterate(pInfo->pGroupSet, pInfo->pGroupIter);
    if (pInfo->pGroupIter == NULL) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    pGroupInfo = pInfo->pGroupIter;
    pInfo->pageIndex = 0;
  }

  int32_t* pageId = taosArrayGet(pGroupInfo->pPageList, pInfo->pageIndex);
  void* page = getBufPage(pInfo->pBuf, *pageId);

  blockDataFromBuf1(pInfo->binfo.pRes, page, pInfo->rowCapacity);

  pInfo->pageIndex += 1;
  releaseBufPage(pInfo->pBuf, page);

  blockDataUpdateTsWindow(pInfo->binfo.pRes, 0);
  pInfo->binfo.pRes->info.groupId = pGroupInfo->groupId;

  pOperator->resultInfo.totalRows += pInfo->binfo.pRes->info.rows;
  return pInfo->binfo.pRes;
}

static SSDataBlock* hashPartition(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    blockDataCleanup(pRes);
    return buildPartitionResult(pOperator);
  }

  int64_t st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    doHashPartition(pOperator, pBlock);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  pOperator->status = OP_RES_TO_RETURN;
  blockDataEnsureCapacity(pRes, 4096);
  return buildPartitionResult(pOperator);
}

static void destroyPartitionOperatorInfo(void* param, int32_t numOfOutput) {
  SPartitionOperatorInfo* pInfo = (SPartitionOperatorInfo*)param;
  doDestroyBasicInfo(&pInfo->binfo, numOfOutput);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroy(pInfo->pGroupColVals);
  taosMemoryFree(pInfo->keyBuf);
  taosMemoryFree(pInfo->columnOffset);
}

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols, SSDataBlock* pResultBlock, SArray* pGroupColList,
                                           SExecTaskInfo* pTaskInfo) {
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pGroupCols = pGroupColList;

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    goto _error;
  }

  uint32_t defaultPgsz  = 0;
  uint32_t defaultBufsz = 0;
  getBufferPgSize(pResultBlock->info.rowSize, &defaultPgsz, &defaultBufsz);

  int32_t code = createDiskbasedBuf(&pInfo->pBuf, defaultPgsz, defaultBufsz, pTaskInfo->id.str, TD_TMP_DIR_PATH);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->rowCapacity = blockDataGetCapacityInRow(pResultBlock, getBufPageSize(pInfo->pBuf));
  pInfo->columnOffset = setupColumnOffset(pResultBlock, pInfo->rowCapacity);
  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pGroupColList);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name         = "PartitionOperator";
  pOperator->blocking     = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PARTITION;
  pInfo->binfo.pRes       = pResultBlock;
  pOperator->numOfExprs   = numOfCols;
  pOperator->pExpr        = pExprInfo;
  pOperator->info         = pInfo;
  pOperator->pTaskInfo    = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, hashPartition, NULL, NULL, destroyPartitionOperatorInfo,
                                         NULL, NULL, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

  _error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}