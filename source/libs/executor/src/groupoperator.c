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
#include "operator.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

typedef struct SGroupbyOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pGroupCols;     // group by columns, SArray<SColumn>
  SArray*        pGroupColVals;  // current group column values, SArray<SGroupKeys>
  bool           isInit;         // denote if current val is initialized or not
  char*          keyBuf;         // group by keys for hash
  int32_t        groupKeyLen;    // total group by column width
  SGroupResInfo  groupResInfo;
  SExprSupp      scalarSup;
} SGroupbyOperatorInfo;

// The sort in partition may be needed later.
typedef struct SPartitionOperatorInfo {
  SOptrBasicInfo binfo;
  SArray*        pGroupCols;
  SArray*        pGroupColVals;  // current group column values, SArray<SGroupKeys>
  char*          keyBuf;         // group by keys for hash
  int32_t        groupKeyLen;    // total group by column width
  SHashObj*      pGroupSet;      // quick locate the window object for each result

  SDiskbasedBuf* pBuf;              // query result buffer based on blocked-wised disk file
  int32_t        rowCapacity;       // maximum number of rows for each buffer page
  int32_t*       columnOffset;      // start position for each column data
  SArray*        sortedGroupArray;  // SDataGroupInfo sorted by group id
  int32_t        groupIndex;        // group index
  int32_t        pageIndex;         // page index of current group
  SExprSupp      scalarSup;

  int32_t remainRows;
  int32_t orderedRows;
  SArray* pOrderInfoArr;
} SPartitionOperatorInfo;

static void*    getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len);
static int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity);
static int32_t  setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                        int32_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup);
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
    SColumn*         pCol = (SColumn*) taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    // valid range check. todo: return error code.
    if (pCol->slotId > taosArrayGetSize(pBlock->pDataBlock)) {
      continue;
    }

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

  int32_t len = 0;
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
                                          len, pBlock->info.id.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {  // null data, too many state code
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
    }

    int32_t rowIndex = j - num;
    applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                    pOperator->exprSupp.numOfExprs);

    // assign the group keys or user input constant values if required
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    num = 1;
  }

  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    int32_t ret = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf,
                                          len, pBlock->info.id.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_APP_ERROR);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                    pOperator->exprSupp.numOfExprs);
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

bool hasRemainResultByHash(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSHashObj* pHashmap = pInfo->aggSup.pResultRowHashTable;
  return pInfo->groupResInfo.index < tSimpleHashGetSize(pHashmap);
}

void doBuildResultDatablockByHash(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                                  SDiskbasedBuf* pBuf) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSHashObj*            pHashmap = pInfo->aggSup.pResultRowHashTable;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;

  SSDataBlock* pBlock = pInfo->binfo.pRes;

  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResultByHash(pOperator)) {
    return;
  }

  pBlock->info.id.groupId = 0;
  if (!pInfo->binfo.mergeResultBlock) {
    doCopyToSDataBlockByHash(pTaskInfo, pBlock, &pOperator->exprSupp, pInfo->aggSup.pResultBuf, &pInfo->groupResInfo,
                             pHashmap, pOperator->resultInfo.threshold, false);
  } else {
    while (hasRemainResultByHash(pOperator)) {
      doCopyToSDataBlockByHash(pTaskInfo, pBlock, &pOperator->exprSupp, pInfo->aggSup.pResultBuf, &pInfo->groupResInfo,
                               pHashmap, pOperator->resultInfo.threshold, true);
      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }
      pBlock->info.id.groupId = 0;
    }

    // clear the group id info in SSDataBlock, since the client does not need it
    pBlock->info.id.groupId = 0;
  }
}

static SSDataBlock* buildGroupResultDataBlockByHash(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock* pRes = pInfo->binfo.pRes;

  // after filter, if result block turn to null, get next from whole set
  while (1) {
    doBuildResultDatablockByHash(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

    doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL);
    if (!hasRemainResultByHash(pOperator)) {
      setOperatorCompleted(pOperator);
      // clean hash after completed
      tSimpleHashCleanup(pInfo->aggSup.pResultRowHashTable);
      pInfo->aggSup.pResultRowHashTable = NULL;
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
    return buildGroupResultDataBlockByHash(pOperator);
  }
  SGroupResInfo* pGroupResInfo = &pInfo->groupResInfo;
  
  int32_t order = pInfo->binfo.inputTsOrder;
  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;

    // the pDataBlock are always the same one, no need to call this again
    setInputDataBlock(&pOperator->exprSupp, pBlock, order, pBlock->info.scanFlag, true);

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

  // initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, 0);
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }
  if (pGroupResInfo->pBuf) {
    taosMemoryFree(pGroupResInfo->pBuf);
    pGroupResInfo->pBuf = NULL;
  }
  pGroupResInfo->index = 0;
  pGroupResInfo->iter = 0;
  pGroupResInfo->dataPos = NULL;

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  return buildGroupResultDataBlockByHash(pOperator);
}

SOperatorInfo* createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAggNode->node.pOutputDataBlockDesc);
  initBasicInfo(&pInfo->binfo, pResBlock);

  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;
  if (pAggNode->pExprs != NULL) {
    pScalarExprInfo = createExprInfo(pAggNode->pExprs, NULL, &numOfScalarExpr);
  }

  pInfo->pGroupCols = extractColumnInfo(pAggNode->pGroupKeys);
  code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
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
  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, pInfo->groupKeyLen, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "GroupbyAggOperator", 0, true, OP_NOT_OPENED, pInfo, pTaskInfo);

  pInfo->binfo.mergeResultBlock = pAggNode->mergeDataBlock;
  pInfo->binfo.inputTsOrder = pAggNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pAggNode->node.outputTsOrder;

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hashGroupbyAggregate, NULL, destroyGroupOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  return pOperator;

_error:
  pTaskInfo->code = code;
  if (pInfo != NULL) {
    destroyGroupOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void doHashPartition(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    int32_t len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);

    SDataGroupInfo* pGroupInfo = NULL;
    void*           pPage = getCurrentDataGroupInfo(pInfo, &pGroupInfo, len);
    if (pPage == NULL) {
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

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
    if (pPage == NULL) {
      return pPage;
    }

    taosArrayPush(p->pPageList, &pageId);
    *(int32_t*)pPage = 0;
  } else {
    int32_t* curId = taosArrayGetLast(p->pPageList);
    pPage = getBufPage(pInfo->pBuf, *curId);
    if (pPage == NULL) {
      qError("failed to get buffer, code:%s", tstrerror(terrno));
      return pPage;
    }

    int32_t* rows = (int32_t*)pPage;
    if (*rows >= pInfo->rowCapacity) {
      // release buffer
      releaseBufPage(pInfo->pBuf, pPage);

      // add a new page for current group
      int32_t pageId = 0;
      pPage = getNewBufPage(pInfo->pBuf, &pageId);
      if (pPage == NULL) {
        qError("failed to get new buffer, code:%s", tstrerror(terrno));
        return NULL;
      }

      taosArrayPush(p->pPageList, &pageId);
      memset(pPage, 0, getBufPageSize(pInfo->pBuf));
    }
  }

  *pGroupInfo = p;
  return pPage;
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
    return 0;
  }

  return (pGroupInfo1->groupId < pGroupInfo2->groupId) ? -1 : 1;
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->remainRows == 0) {
    blockDataCleanup(pInfo->binfo.pRes);
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
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->rowCapacity);
    blockDataFromBuf1(pInfo->binfo.pRes, page, pInfo->rowCapacity);

    pInfo->pageIndex += 1;
    releaseBufPage(pInfo->pBuf, page);
    pInfo->binfo.pRes->info.id.groupId = pGroupInfo->groupId;
    pInfo->binfo.pRes->info.dataLoad = 1;
    pInfo->orderedRows = 0;
  }

  if (pInfo->pOrderInfoArr) {
    pInfo->binfo.pRes->info.rows += pInfo->remainRows;
    blockDataTrimFirstRows(pInfo->binfo.pRes, pInfo->orderedRows);
    pInfo->orderedRows = blockDataGetSortedRows(pInfo->binfo.pRes, pInfo->pOrderInfoArr);
    pInfo->remainRows = pInfo->binfo.pRes->info.rows - pInfo->orderedRows;
    pInfo->binfo.pRes->info.rows = pInfo->orderedRows;
  }

  blockDataUpdateTsWindow(pInfo->binfo.pRes, 0);

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
    return buildPartitionResult(pOperator);
  }

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
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

  int32_t size = taosArrayGetSize(pInfo->sortedGroupArray);
  for (int32_t i = 0; i < size; i++) {
    SDataGroupInfo* pGp = taosArrayGet(pInfo->sortedGroupArray, i);
    taosArrayDestroy(pGp->pPageList);
  }
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
  taosArrayDestroy(pInfo->pOrderInfoArr);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode,
                                           SExecTaskInfo* pTaskInfo) {
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    pTaskInfo->code = terrno;
    goto _error;
  }

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pPartNode->pTargets, NULL, &numOfCols);
  pInfo->pGroupCols = makeColumnArrayFromList(pPartNode->pPartitionKeys);

  if (pPartNode->needBlockOutputTsOrder) {
    SBlockOrderInfo order = {.order = ORDER_ASC, .pColData = NULL, .nullFirst = false, .slotId = pPartNode->tsSlotId};
    pInfo->pOrderInfoArr = taosArrayInit(1, sizeof(SBlockOrderInfo));
    if (!pInfo->pOrderInfoArr) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pTaskInfo->code = terrno;
      goto _error;
    }
    taosArrayPush(pInfo->pOrderInfoArr, &order);
  }

  if (pPartNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pExprInfo1 = createExprInfo(pPartNode->pExprs, NULL, &num);
    int32_t    code = initExprSupp(&pInfo->scalarSup, pExprInfo1, num, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      pTaskInfo->code = terrno;
      goto _error;
    }
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    pTaskInfo->code = terrno;
    goto _error;
  }

  uint32_t defaultPgsz = 0;
  uint32_t defaultBufsz = 0;

  pInfo->binfo.pRes = createDataBlockFromDescNode(pPartNode->node.pOutputDataBlockDesc);
  int32_t code = getBufferPgSize(pInfo->binfo.pRes->info.rowSize, &defaultPgsz, &defaultBufsz);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    pTaskInfo->code = code;
    goto _error;
  }

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    pTaskInfo->code = terrno;
    qError("Create partition operator info failed since %s, tempDir:%s", terrstr(), tsTempDir);
    goto _error;
  }

  code = createDiskbasedBuf(&pInfo->pBuf, defaultPgsz, defaultBufsz, pTaskInfo->id.str, tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    pTaskInfo->code = code;
    goto _error;
  }

  pInfo->rowCapacity = blockDataGetCapacityInRow(pInfo->binfo.pRes, getBufPageSize(pInfo->pBuf),
                                                 blockDataGetSerialMetaSize(taosArrayGetSize(pInfo->binfo.pRes->pDataBlock)));
  pInfo->columnOffset = setupColumnOffset(pInfo->binfo.pRes, pInfo->rowCapacity);
  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    pTaskInfo->code = code;
    goto _error;
  }

  setOperatorInfo(pOperator, "PartitionOperator", QUERY_NODE_PHYSICAL_PLAN_PARTITION, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;

  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, hashPartition, NULL, destroyPartitionOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    pTaskInfo->code = code;
    goto _error;
  }

  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyPartitionOperatorInfo(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  return NULL;
}

int32_t setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                int32_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  SResultRow* pResultRow =
      doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo, false, pAggSup, false);

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
static bool hasRemainTbName(SStreamPartitionOperatorInfo* pInfo) { return pInfo->pTbNameIte != NULL; }

static SSDataBlock* buildStreamPartitionResult(SOperatorInfo* pOperator) {
  SStorageAPI*   pAPI = &pOperator->pTaskInfo->storageAPI;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

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
      char*            pSrcData = NULL;
      if (!isNull) pSrcData = colDataGetData(pSrcCol, rowIndex);
      colDataSetVal(pDestCol, pDest->info.rows, pSrcData, isNull);
    }
    pDest->info.rows++;
  }
  pDest->info.parTbName[0] = 0;
  if (pInfo->tbnameCalSup.numOfExprs > 0) {
    void* tbname = NULL;
    if (pAPI->stateStore.streamStateGetParName(pOperator->pTaskInfo->streamInfo.pState, pParInfo->groupId, &tbname) == 0) {
      memcpy(pDest->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
      pAPI->stateStore.streamStateFreeVal(tbname);
    }
  }
  taosArrayDestroy(pParInfo->rowIds);
  pParInfo->rowIds = NULL;
  pDest->info.dataLoad = 1;

  blockDataUpdateTsWindow(pDest, pInfo->tsColIndex);
  pDest->info.id.groupId = pParInfo->groupId;
  pOperator->resultInfo.totalRows += pDest->info.rows;
  pInfo->parIte = taosHashIterate(pInfo->pPartitions, pInfo->parIte);
  ASSERT(pDest->info.rows > 0);
  printDataBlock(pDest, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
  return pDest;
}

void appendCreateTableRow(void* pState, SExprSupp* pTableSup, SExprSupp* pTagSup, uint64_t groupId,
                          SSDataBlock* pSrcBlock, int32_t rowId, SSDataBlock* pDestBlock, SStateStore* pAPI) {
  void* pValue = NULL;
  if (pAPI->streamStateGetParName(pState, groupId, &pValue) != 0) {
    SSDataBlock* pTmpBlock = blockCopyOneRow(pSrcBlock, rowId);
    memset(pTmpBlock->info.parTbName, 0, TSDB_TABLE_NAME_LEN);
    pTmpBlock->info.id.groupId = groupId;
    char* tbName = pSrcBlock->info.parTbName;
    if (pTableSup->numOfExprs > 0) {
      projectApplyFunctions(pTableSup->pExprInfo, pDestBlock, pTmpBlock, pTableSup->pCtx, pTableSup->numOfExprs, NULL);
      SColumnInfoData* pTbCol = taosArrayGet(pDestBlock->pDataBlock, UD_TABLE_NAME_COLUMN_INDEX);
      memset(tbName, 0, TSDB_TABLE_NAME_LEN);
      int32_t len = 0;
      if (colDataIsNull_s(pTbCol, pDestBlock->info.rows - 1)) {
        len = 1;
        tbName[0] = 0;
      } else {
        void* pData = colDataGetData(pTbCol, pDestBlock->info.rows - 1);
        len = TMIN(varDataLen(pData), TSDB_TABLE_NAME_LEN - 1);
        memcpy(tbName, varDataVal(pData), len);
        pAPI->streamStatePutParName(pState, groupId, tbName);
      }
      memcpy(pTmpBlock->info.parTbName, tbName, len);
      pDestBlock->info.rows--;
    } else {
      void* pTbNameCol = taosArrayGet(pDestBlock->pDataBlock, UD_TABLE_NAME_COLUMN_INDEX);
      colDataSetNULL(pTbNameCol, pDestBlock->info.rows);
      tbName[0] = 0;
    }

    if (pTagSup->numOfExprs > 0) {
      projectApplyFunctions(pTagSup->pExprInfo, pDestBlock, pTmpBlock, pTagSup->pCtx, pTagSup->numOfExprs, NULL);
      pDestBlock->info.rows--;
    } else {
      memcpy(pDestBlock->info.parTbName, pTmpBlock->info.parTbName, TSDB_TABLE_NAME_LEN);
    }

    void* pGpIdCol = taosArrayGet(pDestBlock->pDataBlock, UD_GROUPID_COLUMN_INDEX);
    colDataSetVal(pGpIdCol, pDestBlock->info.rows, (const char*)&groupId, false);
    pDestBlock->info.rows++;
    blockDataDestroy(pTmpBlock);
  } else {
    memcpy(pSrcBlock->info.parTbName, pValue, TSDB_TABLE_NAME_LEN);
  }
  pAPI->streamStateFreeVal(pValue);
}

static SSDataBlock* buildStreamCreateTableResult(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTask = pOperator->pTaskInfo;

  SStreamPartitionOperatorInfo* pInfo = pOperator->info;
  if ((pInfo->tbnameCalSup.numOfExprs == 0 && pInfo->tagCalSup.numOfExprs == 0) ||
      taosHashGetSize(pInfo->pPartitions) == 0) {
    return NULL;
  }
  blockDataCleanup(pInfo->pCreateTbRes);
  blockDataEnsureCapacity(pInfo->pCreateTbRes, taosHashGetSize(pInfo->pPartitions));
  SSDataBlock* pSrc = pInfo->pInputDataBlock;

  if (pInfo->pTbNameIte != NULL) {
    SPartitionDataInfo* pParInfo = (SPartitionDataInfo*)pInfo->pTbNameIte;
    int32_t             rowId = *(int32_t*)taosArrayGet(pParInfo->rowIds, 0);
    appendCreateTableRow(pTask->streamInfo.pState, &pInfo->tbnameCalSup, &pInfo->tagCalSup,
                         pParInfo->groupId, pSrc, rowId, pInfo->pCreateTbRes, &pTask->storageAPI.stateStore);
    pInfo->pTbNameIte = taosHashIterate(pInfo->pPartitions, pInfo->pTbNameIte);
  }
  return pInfo->pCreateTbRes->info.rows > 0 ? pInfo->pCreateTbRes : NULL;
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
  SSDataBlock*                  pCtRes = NULL;

  if (hasRemainTbName(pInfo)) {
    pCtRes = buildStreamCreateTableResult(pOperator);
    if (pCtRes != NULL) {
      return pCtRes;
    }
  }

  if (hasRemainPartion(pInfo)) {
    return buildStreamPartitionResult(pOperator);
  }

  int64_t        st = taosGetTimestampUs();
  SOperatorInfo* downstream = pOperator->pDownstream[0];
  {
    pInfo->pInputDataBlock = NULL;
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "recv", GET_TASKID(pTaskInfo));
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_PULL_DATA:
      case STREAM_INVALID:
        pInfo->binfo.pRes->info.type = pBlock->info.type;
        break;
      case STREAM_DELETE_DATA: {
        copyDataBlock(pInfo->pDelRes, pBlock);
        pInfo->pDelRes->info.type = STREAM_DELETE_RESULT;
        printDataBlock(pInfo->pDelRes, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
        return pInfo->pDelRes;
      } break;
      case STREAM_CREATE_CHILD_TABLE:
      case STREAM_RETRIEVE:
      case STREAM_CHECKPOINT: 
      case STREAM_GET_ALL: {
        return pBlock;
      }
      default:
        ASSERTS(0, "invalid SSDataBlock type");
    }

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                            pInfo->scalarSup.numOfExprs, NULL);
    }
    taosHashClear(pInfo->pPartitions);
    doStreamHashPartitionImpl(pInfo, pBlock);
  }
  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  pInfo->parIte = taosHashIterate(pInfo->pPartitions, NULL);
  pInfo->pTbNameIte = taosHashIterate(pInfo->pPartitions, NULL);
  pCtRes = buildStreamCreateTableResult(pOperator);
  if (pCtRes != NULL) {
    return pCtRes;
  }
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
  blockDataDestroy(pInfo->pCreateTbRes);
  taosMemoryFreeClear(param);
}

void initParDownStream(SOperatorInfo* downstream, SPartitionBySupporter* pParSup, SExprSupp* pExpr) {
  SStorageAPI* pAPI = &downstream->pTaskInfo->storageAPI;

  if (downstream->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    return;
  }

  SStreamScanInfo* pScanInfo = downstream->info;
  pScanInfo->partitionSup = *pParSup;
  pScanInfo->pPartScalarSup = pExpr;
  if (!pScanInfo->pUpdateInfo) {
    pScanInfo->pUpdateInfo = pAPI->stateStore.updateInfoInit(60000, TSDB_TIME_PRECISION_MILLI, 0, pScanInfo->igCheckUpdate);
  }
}

SSDataBlock* buildCreateTableBlock(SExprSupp* tbName, SExprSupp* tag) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = false;
  pBlock->info.id.groupId = 0;
  pBlock->info.rows = 0;
  pBlock->info.type = STREAM_CREATE_CHILD_TABLE;
  pBlock->info.watermark = INT64_MIN;

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  if (tbName->numOfExprs > 0) {
    infoData.info.bytes = tbName->pExprInfo->base.resSchema.bytes;
  } else {
    infoData.info.bytes = 1;
  }
  pBlock->info.rowSize += infoData.info.bytes;
  // sub table name
  taosArrayPush(pBlock->pDataBlock, &infoData);

  SColumnInfoData gpIdData = {0};
  gpIdData.info.type = TSDB_DATA_TYPE_UBIGINT;
  gpIdData.info.bytes = 8;
  pBlock->info.rowSize += gpIdData.info.bytes;
  // group id
  taosArrayPush(pBlock->pDataBlock, &gpIdData);

  for (int32_t i = 0; i < tag->numOfExprs; i++) {
    SColumnInfoData tagCol = {0};
    tagCol.info.type = tag->pExprInfo[i].base.resSchema.type;
    tagCol.info.bytes = tag->pExprInfo[i].base.resSchema.bytes;
    tagCol.info.precision = tag->pExprInfo[i].base.resSchema.precision;
    // tag info
    taosArrayPush(pBlock->pDataBlock, &tagCol);
    pBlock->info.rowSize += tagCol.info.bytes;
  }

  return pBlock;
}

void freePartItem(void* ptr) {
  SPartitionDataInfo* pPart = (SPartitionDataInfo*)ptr;
  taosArrayDestroy(pPart->rowIds);
}

SOperatorInfo* createStreamPartitionOperatorInfo(SOperatorInfo* downstream, SStreamPartitionPhysiNode* pPartNode,
                                                 SExecTaskInfo* pTaskInfo) {
  int32_t                       code = TSDB_CODE_SUCCESS;
  SStreamPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamPartitionOperatorInfo));
  SOperatorInfo*                pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->partitionSup.pGroupCols = makeColumnArrayFromList(pPartNode->part.pPartitionKeys);

  if (pPartNode->part.pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pCalExprInfo = createExprInfo(pPartNode->part.pExprs, NULL, &num);
    code = initExprSupp(&pInfo->scalarSup, pCalExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->tbnameCalSup.numOfExprs = 0;
  if (pPartNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    if (pSubTableExpr == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    createExprFromOneNode(pSubTableExpr, pPartNode->pSubtable, 0);
    code = initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1, &pTaskInfo->storageAPI.functionStore);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  pInfo->tagCalSup.numOfExprs = 0;
  if (pPartNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExpr(pPartNode->pTags, &numOfTags);
    if (pTagExpr == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    if (initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags, &pTaskInfo->storageAPI.functionStore) != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
  }

  if (pInfo->tbnameCalSup.numOfExprs != 0 || pInfo->tagCalSup.numOfExprs != 0) {
    pInfo->pCreateTbRes = buildCreateTableBlock(&pInfo->tbnameCalSup, &pInfo->tagCalSup);
  } else {
    pInfo->pCreateTbRes = NULL;
  }

  int32_t keyLen = 0;
  code = initGroupOptrInfo(&pInfo->partitionSup.pGroupColVals, &keyLen, &pInfo->partitionSup.keyBuf,
                           pInfo->partitionSup.pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pInfo->partitionSup.needCalc = true;

  pInfo->binfo.pRes = createDataBlockFromDescNode(pPartNode->part.node.pOutputDataBlockDesc);
  if (pInfo->binfo.pRes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  blockDataEnsureCapacity(pInfo->binfo.pRes, 4096);

  pInfo->parIte = NULL;
  pInfo->pTbNameIte = NULL;
  pInfo->pInputDataBlock = NULL;

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pPartitions = taosHashInit(1024, hashFn, false, HASH_NO_LOCK);
  taosHashSetFreeFp(pInfo->pPartitions, freePartItem);
  pInfo->tsColIndex = 0;
  pInfo->pDelRes = createSpecialDataBlock(STREAM_DELETE_RESULT);

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pPartNode->part.pTargets, NULL, &numOfCols);

  setOperatorInfo(pOperator, "StreamPartitionOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doStreamHashPartition, NULL,
                                         destroyStreamPartitionOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamOpReleaseState, streamOpReloadState);

  initParDownStream(downstream, &pInfo->partitionSup, &pInfo->scalarSup);
  code = appendDownstream(pOperator, &downstream, 1);
  return pOperator;

_error:
  pTaskInfo->code = code;
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
