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
#include "function.h"
#include "os.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorInt.h"
#include "executorimpl.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

static void*    getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len);
static int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity);
static int32_t  setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                        int16_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup);
static SArray*  extractColumnInfo(SNodeList* pNodeList);

static void freeGroupKey(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

static void destroyGroupOperatorInfo(void* param) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }

  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroyEx(pInfo->pGroupColVals, freeGroupKey);
  cleanupExprSupp(&pInfo->scalarSup);

  cleanupGroupResInfo(&pInfo->groupResInfo);
  cleanupAggSup(&pInfo->aggSup);
  taosMemoryFreeClear(param);
}

static int32_t initGroupOptrInfo(SArray** pGroupColVals, int32_t* keyLen, char** keyBuf, const SArray* pGroupColList) {
  *pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if ((*pGroupColVals) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(pGroupColList, i);
    (*keyLen) += pCol->bytes;  // actual data + null_flag

    SGroupKeys key = {0};
    key.bytes = pCol->bytes;
    key.type = pCol->type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pCol->bytes);
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

static bool groupKeyCompare(SArray* pGroupCols, SArray* pGroupColVals, SSDataBlock* pBlock, int32_t rowIndex,
                            int32_t numOfGroupCols) {
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

      if (memcmp(pkey->pData, val, dataLen) == 0) {
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
        if (tTagIsJson(val)) {
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

  return (int32_t)(pStart - (char*)pKey);
}

// assign the group keys or user input constant values if required
static void doAssignGroupKeys(SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t totalRows, int32_t rowIndex) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (pCtx[i].functionId == -1) {  // select count(*),key from t group by key.
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
      } else {  // it is a NULL value
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
        T_LONG_JMP(pTaskInfo->env, terrno);
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
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      continue;
    }

    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf,
                                          len, pBlock->info.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = j - num;
    doApplyFunctions(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows, pOperator->exprSupp.numOfExprs);

    // assign the group keys or user input constant values if required
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    num = 1;
  }

  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf,
                                          len, pBlock->info.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_APP_ERROR);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    doApplyFunctions(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows, pOperator->exprSupp.numOfExprs);
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
  }
}

static SSDataBlock* buildGroupResultDataBlock(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;

  SSDataBlock* pRes = pInfo->binfo.pRes;
  while (1) {
    doBuildResultDatablock(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);
    doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);

    if (!hasRemainResults(&pInfo->groupResInfo)) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

  pOperator->resultInfo.totalRows += pRes->info.rows;
  return (pRes->info.rows == 0) ? NULL : pRes;
}

static SSDataBlock* hashGroupbyAggregate(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SGroupbyOperatorInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_RES_TO_RETURN) {
    return buildGroupResultDataBlock(pOperator);
  }

  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    int32_t code = getTableScanInfo(pOperator, &order, &scanFlag);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(&pOperator->exprSupp, pBlock, order, scanFlag, true);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
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
  initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, 0);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return buildGroupResultDataBlock(pOperator);
}

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo) {
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SSDataBlock* pResBlock = createResDataBlock(pAggNode->node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;
  if (pAggNode->pExprs != NULL) {
    pScalarExprInfo = createExprInfo(pAggNode->pExprs, NULL, &numOfScalarExpr);
  }

  pInfo->pGroupCols = extractColumnInfo(pAggNode->pGroupKeys);
  int32_t code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t    num = 0;
  SExprInfo* pExprInfo = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &num);
  code = initAggInfo(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, pInfo->groupKeyLen, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "GroupbyAggOperator", 0, true, OP_NOT_OPENED, pInfo, pTaskInfo);

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, hashGroupbyAggregate, NULL, destroyGroupOperatorInfo, NULL);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  if (pInfo != NULL) {
    destroyGroupOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void doHashPartition(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SPartitionOperatorInfo* pInfo = pOperator->info;

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    int32_t len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);

    SDataGroupInfo* pGroupInfo = NULL;
    void*           pPage = getCurrentDataGroupInfo(pInfo, &pGroupInfo, len);

    pGroupInfo->numOfRows += 1;

    // group id
    if (pGroupInfo->groupId == 0) {
      pGroupInfo->groupId = calcGroupId(pInfo->keyBuf, len);
    }

    // number of rows
    int32_t* rows = (int32_t*)pPage;

    size_t numOfCols = pOperator->exprSupp.numOfExprs;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SExprInfo* pExpr = &pOperator->exprSupp.pExprInfo[i];
      int32_t    slotId = pExpr->base.pParam[0].pCol->slotId;

      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);

      int32_t bytes = pColInfoData->info.bytes;
      int32_t startOffset = pInfo->columnOffset[i];

      int32_t* columnLen = NULL;
      int32_t  contentLen = 0;

      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        int32_t* offset = (int32_t*)((char*)pPage + startOffset);
        columnLen = (int32_t*)((char*)pPage + startOffset + sizeof(int32_t) * pInfo->rowCapacity);
        char* data = (char*)((char*)columnLen + sizeof(int32_t));

        if (colDataIsNull_s(pColInfoData, j)) {
          offset[(*rows)] = -1;
          contentLen = 0;
        } else if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
          offset[*rows] = (*columnLen);
          char*   src = colDataGetData(pColInfoData, j);
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
        columnLen = (int32_t*)((char*)pPage + startOffset + BitmapLen(pInfo->rowCapacity));
        char* data = (char*)columnLen + sizeof(int32_t);

        bool isNull = colDataIsNull_f(pColInfoData->nullbitmap, j);
        if (isNull) {
          colDataSetNull_f(bitmap, (*rows));
        } else {
          memcpy(data + (*columnLen), colDataGetData(pColInfoData, j), bytes);
          ASSERT((data + (*columnLen) + bytes - (char*)pPage) <= getBufPageSize(pInfo->pBuf));
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
  if (p == NULL) {  // it is a new group
    SDataGroupInfo gi = {0};
    gi.pPageList = taosArrayInit(100, sizeof(int32_t));
    taosHashPut(pInfo->pGroupSet, pInfo->keyBuf, len, &gi, sizeof(SDataGroupInfo));

    p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);

    int32_t pageId = 0;
    pPage = getNewBufPage(pInfo->pBuf, &pageId);
    taosArrayPush(p->pPageList, &pageId);

    *(int32_t*)pPage = 0;
  } else {
    int32_t* curId = taosArrayGetLast(p->pPageList);
    pPage = getBufPage(pInfo->pBuf, *curId);

    int32_t* rows = (int32_t*)pPage;
    if (*rows >= pInfo->rowCapacity) {
      // release buffer
      releaseBufPage(pInfo->pBuf, pPage);

      // add a new page for current group
      int32_t pageId = 0;
      pPage = getNewBufPage(pInfo->pBuf, &pageId);
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
  size_t   numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t* offset = taosMemoryCalloc(numOfCols, sizeof(int32_t));

  offset[0] = sizeof(int32_t) +
              sizeof(uint64_t);  // the number of rows in current page, ref to SSDataBlock paged serialization format

  for (int32_t i = 0; i < numOfCols - 1; ++i) {
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
  int32_t size = taosArrayGetSize(pInfo->sortedGroupArray);
  for (int32_t i = 0; i < size; i++) {
    SDataGroupInfo* pGp = taosArrayGet(pInfo->sortedGroupArray, i);
    taosArrayDestroy(pGp->pPageList);
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

  return (pGroupInfo1->groupId < pGroupInfo2->groupId) ? -1 : 1;
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;

  SDataGroupInfo* pGroupInfo =
      (pInfo->groupIndex != -1) ? taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex) : NULL;
  if (pInfo->groupIndex == -1 || pInfo->pageIndex >= taosArrayGetSize(pGroupInfo->pPageList)) {
    // try next group data
    ++pInfo->groupIndex;
    if (pInfo->groupIndex >= taosArrayGetSize(pInfo->sortedGroupArray)) {
      setOperatorCompleted(pOperator);
      clearPartitionOperator(pInfo);
      return NULL;
    }

    pGroupInfo = taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex);
    pInfo->pageIndex = 0;
  }

  int32_t* pageId = taosArrayGet(pGroupInfo->pPageList, pInfo->pageIndex);
  void*    page = getBufPage(pInfo->pBuf, *pageId);

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
  SSDataBlock*            pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    blockDataCleanup(pRes);
    return buildPartitionResult(pOperator);
  }

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      break;
    }

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
    }

    terrno = TSDB_CODE_SUCCESS;
    doHashPartition(pOperator, pBlock);
    if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
  }

  SArray* groupArray = taosArrayInit(taosHashGetSize(pInfo->pGroupSet), sizeof(SDataGroupInfo));

  void* pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
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

static void destroyPartitionOperatorInfo(void* param) {
  SPartitionOperatorInfo* pInfo = (SPartitionOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->pGroupCols);

  for (int i = 0; i < taosArrayGetSize(pInfo->pGroupColVals); i++) {
    SGroupKeys key = *(SGroupKeys*)taosArrayGet(pInfo->pGroupColVals, i);
    taosMemoryFree(key.pData);
  }

  taosArrayDestroy(pInfo->pGroupColVals);
  taosMemoryFree(pInfo->keyBuf);
  taosArrayDestroy(pInfo->sortedGroupArray);

  void* pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
  while (pGroupIter != NULL) {
    SDataGroupInfo* pGroupInfo = pGroupIter;
    taosArrayDestroy(pGroupInfo->pPageList);
    pGroupIter = taosHashIterate(pInfo->pGroupSet, pGroupIter);
  }

  taosHashCleanup(pInfo->pGroupSet);
  taosMemoryFree(pInfo->columnOffset);

  cleanupExprSupp(&pInfo->scalarSup);
  destroyDiskbasedBuf(pInfo->pBuf);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode,
                                           SExecTaskInfo* pTaskInfo) {
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pPartNode->pTargets, NULL, &numOfCols);
  pInfo->pGroupCols = extractPartitionColInfo(pPartNode->pPartitionKeys);

  if (pPartNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pExprInfo1 = createExprInfo(pPartNode->pExprs, NULL, &num);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pExprInfo1, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    goto _error;
  }

  uint32_t defaultPgsz = 0;
  uint32_t defaultBufsz = 0;

  pInfo->binfo.pRes = createResDataBlock(pPartNode->node.pOutputDataBlockDesc);
  getBufferPgSize(pInfo->binfo.pRes->info.rowSize, &defaultPgsz, &defaultBufsz);

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_AVAIL_DISK;
    pTaskInfo->code = terrno;
    qError("Create partition operator info failed since %s", terrstr(terrno));
    goto _error;
  }

  int32_t code = createDiskbasedBuf(&pInfo->pBuf, defaultPgsz, defaultBufsz, pTaskInfo->id.str, tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->rowCapacity = blockDataGetCapacityInRow(pInfo->binfo.pRes, getBufPageSize(pInfo->pBuf));
  pInfo->columnOffset = setupColumnOffset(pInfo->binfo.pRes, pInfo->rowCapacity);
  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "PartitionOperator", QUERY_NODE_PHYSICAL_PLAN_PARTITION, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, hashPartition, NULL, destroyPartitionOperatorInfo, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  if (pInfo != NULL) {
    destroyPartitionOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  return NULL;
}

int32_t setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                int16_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  SResultRow* pResultRow =
      doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo, false, pAggSup);
  assert(pResultRow != NULL);

  setResultRowInitCtx(pResultRow, pCtx, numOfCols, pOperator->exprSupp.rowEntryInfoOffset);
  return TSDB_CODE_SUCCESS;
}

uint64_t calGroupIdByData(SPartitionBySupporter* pParSup, SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t rowId) {
  if (pExprSup->pExprInfo != NULL) {
    int32_t code =
        projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      qError("calaculate group id error, code:%d", code);
    }
  }
  recordNewGroupKeys(pParSup->pGroupCols, pParSup->pGroupColVals, pBlock, rowId);
  int32_t  len = buildGroupKeys(pParSup->keyBuf, pParSup->pGroupColVals);
  uint64_t groupId = calcGroupId(pParSup->keyBuf, len);
  return groupId;
}

static bool hasRemainPartion(SStreamPartitionOperatorInfo* pInfo) { return pInfo->parIte != NULL; }

static SSDataBlock* buildStreamPartitionResult(SOperatorInfo* pOperator) {
  SStreamPartitionOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*                  pDest = pInfo->binfo.pRes;
  ASSERT(hasRemainPartion(pInfo));
  SPartitionDataInfo* pParInfo = (SPartitionDataInfo*)pInfo->parIte;
  blockDataCleanup(pDest);
  int32_t      rows = taosArrayGetSize(pParInfo->rowIds);
  SSDataBlock* pSrc = pInfo->pInputDataBlock;
  for (int32_t i = 0; i < rows; i++) {
    int32_t rowIndex = *(int32_t*)taosArrayGet(pParInfo->rowIds, i);
    for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; j++) {
      int32_t          slotId = pOperator->exprSupp.pExprInfo[j].base.pParam[0].pCol->slotId;
      SColumnInfoData* pSrcCol = taosArrayGet(pSrc->pDataBlock, slotId);
      SColumnInfoData* pDestCol = taosArrayGet(pDest->pDataBlock, j);
      bool             isNull = colDataIsNull(pSrcCol, pSrc->info.rows, rowIndex, NULL);
      char*            pSrcData = colDataGetData(pSrcCol, rowIndex);
      colDataAppend(pDestCol, pDest->info.rows, pSrcData, isNull);
    }
    pDest->info.rows++;
    if (pInfo->tbnameCalSup.numOfExprs > 0 && i == 0) {
      SSDataBlock* pTmpBlock = blockCopyOneRow(pSrc, rowIndex);
      SSDataBlock* pResBlock = createDataBlock();
      pResBlock->info.rowSize = TSDB_TABLE_NAME_LEN;
      SColumnInfoData data = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, TSDB_TABLE_NAME_LEN, 0);
      taosArrayPush(pResBlock->pDataBlock, &data);
      blockDataEnsureCapacity(pResBlock, 1);
      projectApplyFunctions(pInfo->tbnameCalSup.pExprInfo, pResBlock, pTmpBlock, pInfo->tbnameCalSup.pCtx, 1, NULL);
      ASSERT(pResBlock->info.rows == 1);
      ASSERT(taosArrayGetSize(pResBlock->pDataBlock) == 1);
      SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, 0);
      ASSERT(pCol->info.type == TSDB_DATA_TYPE_VARCHAR);
      void* pData = colDataGetVarData(pCol, 0);
      // TODO check tbname validity
      if (pData != (void*)-1) {
        memset(pDest->info.parTbName, 0, TSDB_TABLE_NAME_LEN);
        int32_t len = TMIN(varDataLen(pData), TSDB_TABLE_NAME_LEN - 1);
        memcpy(pDest->info.parTbName, varDataVal(pData), len);
        /*pDest->info.parTbName[len + 1] = 0;*/
      } else {
        pDest->info.parTbName[0] = 0;
      }
      /*printf("\n\n set name %s\n\n", pDest->info.parTbName);*/
      blockDataDestroy(pTmpBlock);
      blockDataDestroy(pResBlock);
    }
  }
  taosArrayDestroy(pParInfo->rowIds);
  pParInfo->rowIds = NULL;
  blockDataUpdateTsWindow(pDest, pInfo->tsColIndex);
  pDest->info.groupId = pParInfo->groupId;
  pOperator->resultInfo.totalRows += pDest->info.rows;
  pInfo->parIte = taosHashIterate(pInfo->pPartitions, pInfo->parIte);
  ASSERT(pDest->info.rows > 0);
  printDataBlock(pDest, "stream partitionby");
  return pDest;
}

static void doStreamHashPartitionImpl(SStreamPartitionOperatorInfo* pInfo, SSDataBlock* pBlock) {
  pInfo->pInputDataBlock = pBlock;
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    recordNewGroupKeys(pInfo->partitionSup.pGroupCols, pInfo->partitionSup.pGroupColVals, pBlock, i);
    int32_t             keyLen = buildGroupKeys(pInfo->partitionSup.keyBuf, pInfo->partitionSup.pGroupColVals);
    SPartitionDataInfo* pParData =
        (SPartitionDataInfo*)taosHashGet(pInfo->pPartitions, pInfo->partitionSup.keyBuf, keyLen);
    if (pParData) {
      taosArrayPush(pParData->rowIds, &i);
    } else {
      SPartitionDataInfo newParData = {0};
      newParData.groupId = calcGroupId(pInfo->partitionSup.keyBuf, keyLen);
      newParData.rowIds = taosArrayInit(64, sizeof(int32_t));
      taosArrayPush(newParData.rowIds, &i);
      taosHashPut(pInfo->pPartitions, pInfo->partitionSup.keyBuf, keyLen, &newParData, sizeof(SPartitionDataInfo));
    }
  }
}

static SSDataBlock* doStreamHashPartition(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*                pTaskInfo = pOperator->pTaskInfo;
  SStreamPartitionOperatorInfo* pInfo = pOperator->info;
  if (hasRemainPartion(pInfo)) {
    return buildStreamPartitionResult(pOperator);
  }

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  {
    pInfo->pInputDataBlock = NULL;
    SSDataBlock* pBlock = downstream->fpSet.getNextFn(downstream);
    if (pBlock == NULL) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    printDataBlock(pBlock, "stream partitionby recv");
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_PULL_DATA:
      case STREAM_INVALID:
        pInfo->binfo.pRes->info.type = pBlock->info.type;
        break;
      case STREAM_DELETE_DATA: {
        copyDataBlock(pInfo->pDelRes, pBlock);
        pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
        printDataBlock(pInfo->pDelRes, "stream partitionby delete");
        return pInfo->pDelRes;
      } break;
      default:
        return pBlock;
    }

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      pTaskInfo->code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                              pInfo->scalarSup.numOfExprs, NULL);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        longjmp(pTaskInfo->env, pTaskInfo->code);
      }
    }
    taosHashClear(pInfo->pPartitions);
    doStreamHashPartitionImpl(pInfo, pBlock);
  }
  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  pInfo->parIte = taosHashIterate(pInfo->pPartitions, NULL);
  return buildStreamPartitionResult(pOperator);
}

static void destroyStreamPartitionOperatorInfo(void* param) {
  SStreamPartitionOperatorInfo* pInfo = (SStreamPartitionOperatorInfo*)param;
  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->partitionSup.pGroupCols);

  for (int i = 0; i < taosArrayGetSize(pInfo->partitionSup.pGroupColVals); i++) {
    SGroupKeys key = *(SGroupKeys*)taosArrayGet(pInfo->partitionSup.pGroupColVals, i);
    taosMemoryFree(key.pData);
  }
  taosArrayDestroy(pInfo->partitionSup.pGroupColVals);

  taosMemoryFree(pInfo->partitionSup.keyBuf);
  cleanupExprSupp(&pInfo->scalarSup);
  cleanupExprSupp(&pInfo->tbnameCalSup);
  cleanupExprSupp(&pInfo->tagCalSup);
  blockDataDestroy(pInfo->pDelRes);
  taosHashCleanup(pInfo->pPartitions);
  taosMemoryFreeClear(param);
}

void initParDownStream(SOperatorInfo* downstream, SPartitionBySupporter* pParSup, SExprSupp* pExpr) {
  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    return;
  }
  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->partitionSup = *pParSup;
  pScanInfo->pPartScalarSup = pExpr;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo = updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, 0);
  }
}

SOperatorInfo* createStreamPartitionOperatorInfo(SOperatorInfo* downstream, SStreamPartitionPhysiNode* pPartNode,
                                                 SExecTaskInfo* pTaskInfo) {
  SStreamPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamPartitionOperatorInfo));
  SOperatorInfo*                pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  pInfo->partitionSup.pGroupCols = extractPartitionColInfo(pPartNode->part.pPartitionKeys);

  if (pPartNode->part.pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pCalExprInfo = createExprInfo(pPartNode->part.pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pCalExprInfo, num);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (pPartNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    if (pSubTableExpr == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    createExprFromOneNode(pSubTableExpr, pPartNode->pSubtable, 0);
    code = initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (pPartNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExprInfo(pPartNode->pTags, NULL, &numOfTags);
    if (pTagExpr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    if (initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
  }

  int32_t keyLen = 0;
  code = initGroupOptrInfo(&pInfo->partitionSup.pGroupColVals, &keyLen, &pInfo->partitionSup.keyBuf,
                           pInfo->partitionSup.pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pInfo->partitionSup.needCalc = true;

  pInfo->binfo.pRes = createResDataBlock(pPartNode->part.node.pOutputDataBlockDesc);
  if (pInfo->binfo.pRes == NULL) {
    goto _error;
  }

  blockDataEnsureCapacity(pInfo->binfo.pRes, 4096);

  pInfo->parIte = NULL;
  pInfo->pInputDataBlock = NULL;

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pPartitions = taosHashInit(1024, hashFn, false, HASH_NO_LOCK);
  pInfo->tsColIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pPartNode->part.pTargets, NULL, &numOfCols);

  setOperatorInfo(pOperator, "StreamPartitionOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doStreamHashPartition, NULL, destroyStreamPartitionOperatorInfo, NULL);

  initParDownStream(downstream, &pInfo->partitionSup, &pInfo->scalarSup);
  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  destroyStreamPartitionOperatorInfo(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

SArray* extractColumnInfo(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);

    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColumn c = extractColumnFromColumnNode(pColNode);
      taosArrayPush(pList, &c);
    } else if (nodeType(pNode->pExpr) == QUERY_NODE_VALUE) {
      SValueNode* pValNode = (SValueNode*)pNode->pExpr;
      SColumn     c = {0};
      c.slotId = pNode->slotId;
      c.colId = pNode->slotId;
      c.type = pValNode->node.type;
      c.bytes = pValNode->node.resType.bytes;
      c.scale = pValNode->node.resType.scale;
      c.precision = pValNode->node.resType.precision;

      taosArrayPush(pList, &c);
    }
  }

  return pList;
}
