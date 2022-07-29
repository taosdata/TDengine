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
#include "executorInt.h"

static void*    getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len);
static int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity);
static int32_t  setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t bytes,
                                        uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup);

static void destroyGroupOperatorInfo(void* param, int32_t numOfOutput) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroy(pInfo->pGroupColVals);
  cleanupExprSupp(&pInfo->scalarSup);
  
  taosMemoryFreeClear(param);
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

    if (pkey->type == TSDB_DATA_TYPE_JSON) {
      int32_t dataLen = getJsonValueLen(val);

      if (memcmp(pkey->pData, val, dataLen) == 0){
        continue;
      } else {
        return false;
      }
    } else if (IS_VAR_DATA_TYPE(pkey->type)) {
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

static void recordNewGroupKeys(SArray* pGroupCols, SArray* pGroupColVals, SSDataBlock* pBlock, int32_t rowIndex) {
  SColumnDataAgg* pColAgg = NULL;

  size_t numOfGroupCols = taosArrayGetSize(pGroupCols);

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
      if (pkey->type == TSDB_DATA_TYPE_JSON) {
        if(tTagIsJson(val)){
          terrno = TSDB_CODE_QRY_JSON_IN_GROUP_ERROR;
          return;
        }
        int32_t dataLen = getJsonValueLen(val);
        memcpy(pkey->pData, val, dataLen);
      } else if (IS_VAR_DATA_TYPE(pkey->type)) {
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
    if (pkey->type == TSDB_DATA_TYPE_JSON) {
      int32_t dataLen = getJsonValueLen(pkey->pData);
      memcpy(pStart, (pkey->pData), dataLen);
      pStart += dataLen;
    } else if (IS_VAR_DATA_TYPE(pkey->type)) {
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

        if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
          int32_t dataLen = getJsonValueLen(data);
          memcpy(dest, data, dataLen);
        } else if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
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

  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t         numOfGroupCols = taosArrayGetSize(pInfo->pGroupCols);
  //  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
  // qError("QInfo:0x%"PRIx64" group by not supported on double/float columns, abort", GET_TASKID(pRuntimeEnv));
  //    return;
  //  }

  int32_t     len = 0;
  STimeWindow w = TSWINDOW_INITIALIZER;

  terrno = TSDB_CODE_SUCCESS;
  int32_t num = 0;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    // Compare with the previous row of this column, and do not set the output buffer again if they are identical.
    if (!pInfo->isInit) {
      recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
      if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
        longjmp(pTaskInfo->env, terrno);
      }
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
      recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
      if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
        longjmp(pTaskInfo->env, terrno);
      }
      continue;
    }

    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf, len, pBlock->info.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = j - num;
    doApplyFunctions(pTaskInfo, pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->exprSupp.numOfExprs, TSDB_ORDER_ASC);

    // assign the group keys or user input constant values if required
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    num = 1;
  }

  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret =
        setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf, len,
                                pBlock->info.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {
      longjmp(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    doApplyFunctions(pTaskInfo, pCtx, &w, NULL, rowIndex, num, NULL, pBlock->info.rows, pOperator->exprSupp.numOfExprs, TSDB_ORDER_ASC);
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
  }
}

static SSDataBlock* buildGroupResultDataBlock(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;

  SSDataBlock* pRes = pInfo->binfo.pRes;
  while(1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pInfo->pCondition, pRes, NULL);

    if (!hasRemainResults(&pInfo->groupResInfo)) {
      doSetOperatorCompleted(pOperator);
      break;
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

  pOperator->resultInfo.totalRows += pRes->info.rows;
  return (pRes->info.rows == 0)? NULL:pRes;
}

static SSDataBlock* hashGroupbyAggregate(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    return buildGroupResultDataBlock(pOperator);
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
    setInputDataBlock(pOperator, pOperator->exprSupp.pCtx, pBlock, order, scanFlag, true);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx, pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, pTaskInfo->code);
      }
    }

    doHashGroupbyAgg(pOperator, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;

#if 0
  if(pOperator->fpSet.encodeResultRow){
    char *result = NULL;
    int32_t length = 0;
    pOperator->fpSet.encodeResultRow(pOperator, &result, &length);
    SAggSupporter* pSup = &pInfo->aggSup;
    taosHashClear(pSup->pResultRowHashTable);
    pInfo->binfo.resultRowInfo.size = 0;
    pOperator->fpSet.decodeResultRow(pOperator, result);
    if(result){
      taosMemoryFree(result);
    }
  }
#endif
  blockDataEnsureCapacity(pRes, pOperator->resultInfo.capacity);
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, 0);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return buildGroupResultDataBlock(pOperator);
}

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SExprInfo* pExprInfo, int32_t numOfCols,
                                       SSDataBlock* pResultBlock, SArray* pGroupColList, SNode* pCondition,
                                       SExprInfo* pScalarExprInfo, int32_t numOfScalarExpr, SExecTaskInfo* pTaskInfo) {
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pGroupCols      = pGroupColList;
  pInfo->pCondition      = pCondition;

  int32_t code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pGroupColList);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, numOfCols, pInfo->groupKeyLen, pTaskInfo->id.str);
  initBasicInfo(&pInfo->binfo, pResultBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);

  pOperator->name         = "GroupbyAggOperator";
  pOperator->blocking     = true;
  pOperator->status       = OP_NOT_OPENED;
  // pOperator->operatorType = OP_Groupby;
  pOperator->exprSupp.pExprInfo    = pExprInfo;
  pOperator->exprSupp.numOfExprs   = numOfCols;
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

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    int32_t len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);

    SDataGroupInfo* pGroupInfo = NULL;
    void *pPage = getCurrentDataGroupInfo(pInfo, &pGroupInfo, len);

    pGroupInfo->numOfRows += 1;

    // group id
    if (pGroupInfo->groupId == 0) {
      pGroupInfo->groupId = calcGroupId(pInfo->keyBuf, len);
    }

    // number of rows
    int32_t* rows = (int32_t*) pPage;

    size_t numOfCols = pOperator->exprSupp.numOfExprs;
    for(int32_t i = 0; i < numOfCols; ++i) {
      SExprInfo* pExpr = &pOperator->exprSupp.pExprInfo[i];
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
        } else if(pColInfoData->info.type == TSDB_DATA_TYPE_JSON){
          offset[*rows] = (*columnLen);
          char* src = colDataGetData(pColInfoData, j);
          int32_t dataLen = getJsonValueLen(src);

          memcpy(data + (*columnLen), src, dataLen);
          int32_t v = (data + (*columnLen) + dataLen - (char*)pPage);
          ASSERT(v > 0);

          contentLen = dataLen;
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
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t* offset = taosMemoryCalloc(numOfCols, sizeof(int32_t));

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

static void clearPartitionOperator(SPartitionOperatorInfo* pInfo) {
  void *ite = NULL;
  while( (ite = taosHashIterate(pInfo->pGroupSet, ite)) != NULL ) {
    taosArrayDestroy( ((SDataGroupInfo *)ite)->pPageList);
  }
  taosArrayClear(pInfo->sortedGroupArray);
  clearDiskbasedBuf(pInfo->pBuf);
}

static int compareDataGroupInfo(const void* group1, const void* group2) {
  const SDataGroupInfo* pGroupInfo1 = group1;
  const SDataGroupInfo* pGroupInfo2 = group2;

  if (pGroupInfo1->groupId == pGroupInfo2->groupId) {
    ASSERT(0);
    return 0;
  }

  return (pGroupInfo1->groupId < pGroupInfo2->groupId)? -1:1;
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;

  SDataGroupInfo* pGroupInfo = (pInfo->groupIndex != -1) ? taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex) : NULL;
  if (pInfo->groupIndex == -1 || pInfo->pageIndex >= taosArrayGetSize(pGroupInfo->pPageList)) {
    // try next group data
    ++pInfo->groupIndex;
    if (pInfo->groupIndex >= taosArrayGetSize(pInfo->sortedGroupArray)) {
      doSetOperatorCompleted(pOperator);
      clearPartitionOperator(pInfo);
      return NULL;
    }

    pGroupInfo = taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex);
    pInfo->pageIndex = 0;
  }

  int32_t* pageId = taosArrayGet(pGroupInfo->pPageList, pInfo->pageIndex);
  void* page = getBufPage(pInfo->pBuf, *pageId);

  blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->rowCapacity);
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

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SPartitionOperatorInfo* pInfo = pOperator->info;
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

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx, pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, pTaskInfo->code);
      }
    }

    terrno = TSDB_CODE_SUCCESS;
    doHashPartition(pOperator, pBlock);
    if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
      longjmp(pTaskInfo->env, terrno);
    }
  }

  SArray* groupArray = taosArrayInit(taosHashGetSize(pInfo->pGroupSet), sizeof(SDataGroupInfo));
  void* pGroupIter = NULL;
  pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
  while (pGroupIter != NULL) {
    SDataGroupInfo* pGroupInfo = pGroupIter;
    taosArrayPush(groupArray, pGroupInfo);
    pGroupIter = taosHashIterate(pInfo->pGroupSet, pGroupIter);
  }

  taosArraySort(groupArray, compareDataGroupInfo);
  pInfo->sortedGroupArray = groupArray;
  pInfo->groupIndex = -1;
  taosHashClear(pInfo->pGroupSet);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  pOperator->status = OP_RES_TO_RETURN;
  blockDataEnsureCapacity(pRes, 4096);
  return buildPartitionResult(pOperator);
}

static void destroyPartitionOperatorInfo(void* param, int32_t numOfOutput) {
  SPartitionOperatorInfo* pInfo = (SPartitionOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->pGroupCols);

  for(int i = 0; i < taosArrayGetSize(pInfo->pGroupColVals); i++){
    SGroupKeys key = *(SGroupKeys*)taosArrayGet(pInfo->pGroupColVals, i);
    taosMemoryFree(key.pData);
  }

  taosArrayDestroy(pInfo->pGroupColVals);
  taosMemoryFree(pInfo->keyBuf);
  taosArrayDestroy(pInfo->sortedGroupArray);
  taosHashCleanup(pInfo->pGroupSet);
  taosMemoryFree(pInfo->columnOffset);

  cleanupExprSupp(&pInfo->scalarSup);
  
  taosMemoryFreeClear(param);
}

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode, SExecTaskInfo* pTaskInfo) {
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SSDataBlock* pResBlock = createResDataBlock(pPartNode->node.pOutputDataBlockDesc);

  int32_t numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pPartNode->pTargets, NULL, &numOfCols);

  pInfo->pGroupCols = extractPartitionColInfo(pPartNode->pPartitionKeys);

  if (pPartNode->pExprs != NULL) {
    int32_t num = 0;
    SExprInfo* pExprInfo1 = createExprInfo(pPartNode->pExprs, NULL, &num);
    int32_t code = initExprSupp(&pInfo->scalarSup, pExprInfo1, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    goto _error;
  }

  uint32_t defaultPgsz  = 0;
  uint32_t defaultBufsz = 0;
  getBufferPgSize(pResBlock->info.rowSize, &defaultPgsz, &defaultBufsz);

  int32_t code = createDiskbasedBuf(&pInfo->pBuf, defaultPgsz, defaultBufsz, pTaskInfo->id.str, TD_TMP_DIR_PATH);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->rowCapacity = blockDataGetCapacityInRow(pResBlock, getBufPageSize(pInfo->pBuf));
  pInfo->columnOffset = setupColumnOffset(pResBlock, pInfo->rowCapacity);
  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name         = "PartitionOperator";
  pOperator->blocking     = true;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_PARTITION;
  pInfo->binfo.pRes       = pResBlock;
  pOperator->exprSupp.numOfExprs   = numOfCols;
  pOperator->exprSupp.pExprInfo    = pExprInfo;
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

int32_t setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData, int16_t bytes,
                                uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  SResultRow* pResultRow =
      doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo, false, pAggSup);
  assert(pResultRow != NULL);

  setResultRowInitCtx(pResultRow, pCtx, numOfCols, pOperator->exprSupp.rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}
