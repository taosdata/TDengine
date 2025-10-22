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
#include "query.h"
#include "tname.h"
#include "tutil.h"

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
  SOperatorInfo* pOperator;
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
static int32_t  extractColumnInfo(SNodeList* pNodeList, SArray** pArrayRes);

static void freeGroupKey(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

static void destroyGroupOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;

  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroyEx(pInfo->pGroupColVals, freeGroupKey);
  cleanupExprSupp(&pInfo->scalarSup);

  if (pInfo->pOperator != NULL) {
    cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                      false);
    pInfo->pOperator = NULL;
  }

  cleanupGroupResInfo(&pInfo->groupResInfo);
  cleanupAggSup(&pInfo->aggSup);
  taosMemoryFreeClear(param);
}

static int32_t initGroupOptrInfo(SArray** pGroupColVals, int32_t* keyLen, char** keyBuf, const SArray* pGroupColList) {
  *pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if ((*pGroupColVals) == NULL) {
    return terrno;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(pGroupColList, i);
    if (!pCol) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    (*keyLen) += pCol->bytes;  // actual data + null_flag

    SGroupKeys key = {0};
    key.bytes = pCol->bytes;
    key.type = pCol->type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pCol->bytes);
    if (key.pData == NULL) {
      return terrno;
    }

    void* tmp = taosArrayPush((*pGroupColVals), &key);
    if (!tmp) {
      return terrno;
    }
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  (*keyLen) += nullFlagSize;

  (*keyBuf) = taosMemoryCalloc(1, (*keyLen));
  if ((*keyBuf) == NULL) {
    return terrno;
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
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
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
      if (IS_STR_DATA_BLOB(pkey->type)) {
        int32_t len = blobDataLen(val);
        if (len == blobDataLen(pkey->pData) && memcmp(blobDataVal(pkey->pData), blobDataVal(val), len) == 0) {
          continue;
        } else {
          return false;
        }
      } else {
        int32_t len = varDataLen(val);
        if (len == varDataLen(pkey->pData) && memcmp(varDataVal(pkey->pData), varDataVal(val), len) == 0) {
          continue;
        } else {
          return false;
        }
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
    SColumn*         pCol = (SColumn*)taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    // valid range check. todo: return error code.
    if (pCol->slotId > taosArrayGetSize(pBlock->pDataBlock)) {
      continue;
    }

    if (pBlock->pBlockAgg != NULL) {
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
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
        if (IS_STR_DATA_BLOB(pkey->type)) {
          memcpy(pkey->pData, val, blobDataTLen(val));
        } else {
          memcpy(pkey->pData, val, varDataTLen(val));
        }
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
      if (IS_STR_DATA_BLOB(pkey->type)) {
        blobDataCopy(pStart, pkey->pData);
        pStart += blobDataTLen(pkey->pData);
      } else {
        varDataCopy(pStart, pkey->pData);
        pStart += varDataTLen(pkey->pData);
      }
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
          if (IS_STR_DATA_BLOB(pColInfoData->info.type)) {
            blobDataCopy(dest, data);
          } else {
            varDataCopy(dest, data);
          }
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
  //  qError("QInfo:0x%" PRIx64 ", group by not supported on double/float columns, abort", GET_TASKID(pRuntimeEnv));
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
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    int32_t rowIndex = j - num;
    ret = applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                          pOperator->exprSupp.numOfExprs);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }

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
      T_LONG_JMP(pTaskInfo->env, ret);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    ret = applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                          pOperator->exprSupp.numOfExprs);
    if (ret != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, ret);
    }
    doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
  }
}

bool hasRemainResultByHash(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSHashObj*            pHashmap = pInfo->aggSup.pResultRowHashTable;
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
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*          pRes = pInfo->binfo.pRes;

  // after filter, if result block turn to null, get next from whole set
  while (1) {
    doBuildResultDatablockByHash(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

    code = doFilter(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pRes->info.rows == 0) ? NULL : pRes;
}

static int32_t hashGroupbyAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SGroupResInfo*        pGroupResInfo = &pInfo->groupResInfo;
  int32_t               order = pInfo->binfo.inputTsOrder;
  int64_t               st = taosGetTimestampUs();

  QRY_PARAM_CHECK(ppRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    (*ppRes) = buildGroupResultDataBlockByHash(pOperator);
    return code;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;

    // the pDataBlock are always the same one, no need to call this again
    code = setInputDataBlock(&pOperator->exprSupp, pBlock, order, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    (*ppRes) = buildGroupResultDataBlockByHash(pOperator);
  }

  return code;
}

static int32_t resetGroupOperState(SOperatorInfo* pOper) {
  SGroupbyOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SAggPhysiNode* pPhynode = (SAggPhysiNode*)pOper->pPhyNode;
  resetBasicOperatorState(&pInfo->binfo);
  pOper->status = OP_NOT_OPENED;

  cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
    false);

  cleanupGroupResInfo(&pInfo->groupResInfo);

  qInfo("[group key] len use:%d", pInfo->groupKeyLen);
  int32_t code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->pAggFuncs, pPhynode->pGroupKeys,
    pInfo->groupKeyLen + POINTER_BYTES, pTaskInfo->id.str, pTaskInfo->streamInfo.pState,
    &pTaskInfo->storageAPI.functionStore);

  if (code == 0){
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
      &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->isInit = false;

  return code;
}

int32_t createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo,
                                SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = (SNode*)pAggNode;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = false;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAggNode->node.pOutputDataBlockDesc);
  if (pResBlock == NULL) {
    code = terrno;
    goto _error;
  }
  initBasicInfo(&pInfo->binfo, pResBlock);

  pInfo->pGroupCols = NULL;
  code = extractColumnInfo(pAggNode->pGroupKeys, &pInfo->pGroupCols);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;
  if (pAggNode->pExprs != NULL) {
    code = createExprInfo(pAggNode->pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;

  code = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, pInfo->groupKeyLen, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "GroupbyAggOperator", 0, true, OP_NOT_OPENED, pInfo, pTaskInfo);

  pInfo->binfo.mergeResultBlock = pAggNode->mergeDataBlock;
  pInfo->binfo.inputTsOrder = pAggNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pAggNode->node.outputTsOrder;

  pInfo->pOperator = pOperator;

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hashGroupbyAggregateNext, NULL, destroyGroupOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetGroupOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyGroupOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

SSDataBlock* createBlockDataNotLoaded(const SOperatorInfo* pOperator, SSDataBlock* pDataBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pDataBlock == NULL) {
    return NULL;
  }

  SSDataBlock* pDstBlock = NULL;
  code = createDataBlock(&pDstBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  pDstBlock->info = pDataBlock->info;
  pDstBlock->info.id.blockId = pOperator->resultDataBlockId;
  pDstBlock->info.capacity = 0;
  pDstBlock->info.rowSize = 0;

  size_t numOfCols = pOperator->exprSupp.numOfExprs;
  if (pDataBlock->pBlockAgg) {
    pDstBlock->pBlockAgg = taosMemoryCalloc(numOfCols, sizeof(SColumnDataAgg));
    if (pDstBlock->pBlockAgg == NULL) {
      blockDataDestroy(pDstBlock);
      return NULL;
    }
    for (int i = 0; i < numOfCols; ++i) {
      pDstBlock->pBlockAgg[i].colId = -1;
    }
  }

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SExprInfo*       pExpr = &pOperator->exprSupp.pExprInfo[i];
    int32_t          slotId = pExpr->base.pParam[0].pCol->slotId;
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, slotId);
    SColumnInfoData  colInfo = {.hasNull = true, .info = pSrc->info};
    code = blockDataAppendColInfo(pDstBlock, &colInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pDst = taosArrayGet(pDstBlock->pDataBlock, i);
    if (pDataBlock->pBlockAgg && pDataBlock->pBlockAgg[slotId].colId != -1) {
      pDstBlock->pBlockAgg[i] = pDataBlock->pBlockAgg[slotId];
    } else {
      code = doEnsureCapacity(pDst, &pDstBlock->info, pDataBlock->info.rows, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = colDataAssign(pDst, pSrc, pDataBlock->info.rows, &pDataBlock->info);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pDstBlock);
    return NULL;
  }
  return pDstBlock;
}

static void doHashPartition(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
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

    if (pBlock->info.dataLoad) {
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
            QUERY_CHECK_CONDITION((v > 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

            contentLen = dataLen;
          } else {
            if (IS_STR_DATA_BLOB(pColInfoData->info.type)) {
              offset[*rows] = (*columnLen);
              char* src = colDataGetData(pColInfoData, j);
              memcpy(data + (*columnLen), src, blobDataTLen(src));
              int32_t v = (data + (*columnLen) + blobDataTLen(src) - (char*)pPage);
              QUERY_CHECK_CONDITION((v > 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

              contentLen = blobDataTLen(src);
            } else {
              offset[*rows] = (*columnLen);
              char* src = colDataGetData(pColInfoData, j);
              memcpy(data + (*columnLen), src, varDataTLen(src));
              int32_t v = (data + (*columnLen) + varDataTLen(src) - (char*)pPage);
              QUERY_CHECK_CONDITION((v > 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

              contentLen = varDataTLen(src);
            }
          }
        } else {
          char* bitmap = (char*)pPage + startOffset;
          columnLen = (int32_t*)((char*)pPage + startOffset + BitmapLen(pInfo->rowCapacity));
          char* data = (char*)columnLen + sizeof(int32_t);

          bool isNull = colDataIsNull_f(pColInfoData, j);
          if (isNull) {
            colDataSetNull_f(bitmap, (*rows));
          } else {
            memcpy(data + (*columnLen), colDataGetData(pColInfoData, j), bytes);
            QUERY_CHECK_CONDITION(((data + (*columnLen) + bytes - (char*)pPage) <= getBufPageSize(pInfo->pBuf)), code,
                                  lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
          }
          contentLen = bytes;
        }

        (*columnLen) += contentLen;
      }

      (*rows) += 1;

      setBufPageDirty(pPage, true);
      releaseBufPage(pInfo->pBuf, pPage);
    } else {
      SSDataBlock* dataNotLoadBlock = createBlockDataNotLoaded(pOperator, pBlock);
      if (dataNotLoadBlock == NULL) {
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      if (pGroupInfo->blockForNotLoaded == NULL) {
        pGroupInfo->blockForNotLoaded = taosArrayInit(0, sizeof(SSDataBlock*));
        QUERY_CHECK_NULL(pGroupInfo->blockForNotLoaded, code, lino, _end, terrno);
        pGroupInfo->offsetForNotLoaded = 0;
      }
      dataNotLoadBlock->info.id.groupId = pGroupInfo->groupId;
      dataNotLoadBlock->info.dataLoad = 0;
      void* tmp = taosArrayPush(pGroupInfo->blockForNotLoaded, &dataNotLoadBlock);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

void* getCurrentDataGroupInfo(const SPartitionOperatorInfo* pInfo, SDataGroupInfo** pGroupInfo, int32_t len) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SDataGroupInfo* p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);

  void* pPage = NULL;
  if (p == NULL) {  // it is a new group
    SDataGroupInfo gi = {0};
    gi.pPageList = taosArrayInit(100, sizeof(int32_t));
    QUERY_CHECK_NULL(gi.pPageList, code, lino, _end, terrno);

    code = taosHashPut(pInfo->pGroupSet, pInfo->keyBuf, len, &gi, sizeof(SDataGroupInfo));
    if (code == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);

    p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);

    int32_t pageId = 0;
    pPage = getNewBufPage(pInfo->pBuf, &pageId);
    if (pPage == NULL) {
      return pPage;
    }

    void* tmp = taosArrayPush(p->pPageList, &pageId);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

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

      void* tmp = taosArrayPush(p->pPageList, &pageId);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

      memset(pPage, 0, getBufPageSize(pInfo->pBuf));
    }
  }

  *pGroupInfo = p;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return NULL;
  }

  return pPage;
}

int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity) {
  size_t   numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t* offset = taosMemoryCalloc(numOfCols, sizeof(int32_t));
  if (!offset) {
    return NULL;
  }

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
    if (pGp && pGp->blockForNotLoaded) {
      for (int32_t i = 0; i < pGp->blockForNotLoaded->size; i++) {
        SSDataBlock** pBlock = taosArrayGet(pGp->blockForNotLoaded, i);
        if (pBlock) blockDataDestroy(*pBlock);
      }
      taosArrayClear(pGp->blockForNotLoaded);
      pGp->offsetForNotLoaded = 0;
    }
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

static SSDataBlock* buildPartitionResultForNotLoadBlock(SDataGroupInfo* pGroupInfo) {
  if (pGroupInfo->blockForNotLoaded && pGroupInfo->offsetForNotLoaded < pGroupInfo->blockForNotLoaded->size) {
    SSDataBlock** pBlock = taosArrayGet(pGroupInfo->blockForNotLoaded, pGroupInfo->offsetForNotLoaded);
    if (!pBlock) {
      return NULL;
    }
    pGroupInfo->offsetForNotLoaded++;
    return *pBlock;
  }
  return NULL;
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->remainRows == 0) {
    blockDataCleanup(pInfo->binfo.pRes);
    SDataGroupInfo* pGroupInfo =
        (pInfo->groupIndex != -1) ? taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex) : NULL;
    if (pInfo->groupIndex == -1 || pInfo->pageIndex >= taosArrayGetSize(pGroupInfo->pPageList)) {
      if (pGroupInfo != NULL) {
        SSDataBlock* ret = buildPartitionResultForNotLoadBlock(pGroupInfo);
        if (ret != NULL) return ret;
      }
      // try next group data
      if (pInfo->groupIndex + 1 >= taosArrayGetSize(pInfo->sortedGroupArray)) {
        setOperatorCompleted(pOperator);
        clearPartitionOperator(pInfo);
        return NULL;
      }
      ++pInfo->groupIndex;

      pGroupInfo = taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex);
      if (pGroupInfo == NULL) {
        qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      pInfo->pageIndex = 0;
    }

    int32_t* pageId = taosArrayGet(pGroupInfo->pPageList, pInfo->pageIndex);
    if (pageId == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
    void* page = getBufPage(pInfo->pBuf, *pageId);
    if (page == NULL) {
      qError("failed to get buffer, code:%s, %s", tstrerror(terrno), GET_TASKID(pTaskInfo));
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
    if (*(int32_t*)page == 0) {
      releaseBufPage(pInfo->pBuf, page);
      SSDataBlock* ret = buildPartitionResultForNotLoadBlock(pGroupInfo);
      if (ret != NULL) return ret;
      if (pInfo->groupIndex + 1 < taosArrayGetSize(pInfo->sortedGroupArray)) {
        pInfo->groupIndex++;
        pInfo->pageIndex = 0;
      } else {
        setOperatorCompleted(pOperator);
        clearPartitionOperator(pInfo);
        return NULL;
      }
      return buildPartitionResult(pOperator);
    }

    code = blockDataEnsureCapacity(pInfo->binfo.pRes, pInfo->rowCapacity);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataFromBuf1(pInfo->binfo.pRes, page, pInfo->rowCapacity);
    QUERY_CHECK_CODE(code, lino, _end);

    pInfo->pageIndex += 1;
    releaseBufPage(pInfo->pBuf, page);
    pInfo->binfo.pRes->info.id.groupId = pGroupInfo->groupId;
    pInfo->binfo.pRes->info.dataLoad = 1;
    pInfo->orderedRows = 0;
  } else if (pInfo->pOrderInfoArr == NULL) {
    qError("Exception, remainRows not zero, but pOrderInfoArr is NULL");
  }

  if (pInfo->pOrderInfoArr) {
    pInfo->binfo.pRes->info.rows += pInfo->remainRows;
    code = blockDataTrimFirstRows(pInfo->binfo.pRes, pInfo->orderedRows);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->orderedRows = blockDataGetSortedRows(pInfo->binfo.pRes, pInfo->pOrderInfoArr);
    pInfo->remainRows = pInfo->binfo.pRes->info.rows - pInfo->orderedRows;
    pInfo->binfo.pRes->info.rows = pInfo->orderedRows;
  }

  code = blockDataUpdateTsWindow(pInfo->binfo.pRes, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  pOperator->resultInfo.totalRows += pInfo->binfo.pRes->info.rows;
  return pInfo->binfo.pRes;
}

static int32_t hashPartitionNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*            pRes = pInfo->binfo.pRes;

  if (pOperator->status == OP_RES_TO_RETURN) {
    (*ppRes) = buildPartitionResult(pOperator);
    return code;
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
      code =
          projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    terrno = TSDB_CODE_SUCCESS;
    doHashPartition(pOperator, pBlock);
    if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SArray* groupArray = taosArrayInit(taosHashGetSize(pInfo->pGroupSet), sizeof(SDataGroupInfo));
  QUERY_CHECK_NULL(groupArray, code, lino, _end, terrno);

  void* pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
  while (pGroupIter != NULL) {
    SDataGroupInfo* pGroupInfo = pGroupIter;
    void*           tmp = taosArrayPush(groupArray, pGroupInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    pGroupIter = taosHashIterate(pInfo->pGroupSet, pGroupIter);
  }

  taosArraySort(groupArray, compareDataGroupInfo);
  pInfo->sortedGroupArray = groupArray;
  pInfo->groupIndex = -1;
  taosHashClear(pInfo->pGroupSet);

  pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;

  pOperator->status = OP_RES_TO_RETURN;
  code = blockDataEnsureCapacity(pRes, 4096);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = buildPartitionResult(pOperator);
  return code;
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
    if (pGp) {
      taosArrayDestroy(pGp->pPageList);
    }
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

static int32_t resetPartitionOperState(SOperatorInfo* pOper) {
  SPartitionOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SPartitionPhysiNode* pPhynode = (SPartitionPhysiNode*)pOper->pPhyNode;
  resetBasicOperatorState(&pInfo->binfo);

  int32_t code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
    &pTaskInfo->storageAPI.functionStore);

  clearPartitionOperator(pInfo);

  void* pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
  while (pGroupIter != NULL) {
    SDataGroupInfo* pGroupInfo = pGroupIter;
    taosArrayDestroy(pGroupInfo->pPageList);
    pGroupIter = taosHashIterate(pInfo->pGroupSet, pGroupIter);
  }
  taosHashClear(pInfo->pGroupSet);

  int32_t size = taosArrayGetSize(pInfo->sortedGroupArray);
  for (int32_t i = 0; i < size; i++) {
    SDataGroupInfo* pGp = taosArrayGet(pInfo->sortedGroupArray, i);
    if (pGp) {
      taosArrayDestroy(pGp->pPageList);
    }
  }
  taosArrayDestroy(pInfo->sortedGroupArray);
  pInfo->sortedGroupArray = NULL;

  pInfo->groupIndex = 0;
  pInfo->pageIndex = 0;
  pInfo->remainRows = 0;
  pInfo->orderedRows = 0;
  return 0;
}

int32_t createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode,
                                           SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pPartNode;
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPartNode->pTargets, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;

  pInfo->pGroupCols = makeColumnArrayFromList(pPartNode->pPartitionKeys);

  if (pPartNode->needBlockOutputTsOrder) {
    SBlockOrderInfo order = {.order = ORDER_ASC, .pColData = NULL, .nullFirst = false, .slotId = pPartNode->tsSlotId};
    pInfo->pOrderInfoArr = taosArrayInit(1, sizeof(SBlockOrderInfo));
    if (!pInfo->pOrderInfoArr) {
      pTaskInfo->code = terrno;
      goto _error;
    }

    void* tmp = taosArrayPush(pInfo->pOrderInfoArr, &order);
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
  }

  if (pPartNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pExprInfo1 = NULL;
    code = createExprInfo(pPartNode->pExprs, NULL, &pExprInfo1, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pExprInfo1, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    goto _error;
  }

  uint32_t defaultPgsz = 0;
  int64_t  defaultBufsz = 0;

  pInfo->binfo.pRes = createDataBlockFromDescNode(pPartNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);
  code = getBufferPgSize(pInfo->binfo.pRes->info.rowSize, &defaultPgsz, &defaultBufsz);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    qError("Create partition operator info failed since %s, tempDir:%s", terrstr(), tsTempDir);
    goto _error;
  }

  code = createDiskbasedBuf(&pInfo->pBuf, defaultPgsz, defaultBufsz, pTaskInfo->id.str, tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->rowCapacity =
      blockDataGetCapacityInRow(pInfo->binfo.pRes, getBufPageSize(pInfo->pBuf),
                                blockDataGetSerialMetaSize(taosArrayGetSize(pInfo->binfo.pRes->pDataBlock)));
  if (pInfo->rowCapacity < 0) {
    code = terrno;
    goto _error;
  }

  pInfo->columnOffset = setupColumnOffset(pInfo->binfo.pRes, pInfo->rowCapacity);
  QUERY_CHECK_NULL(pInfo->columnOffset, code, lino, _error, terrno);

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "PartitionOperator", QUERY_NODE_PHYSICAL_PLAN_PARTITION, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hashPartitionNext, NULL, destroyPartitionOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetPartitionOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyPartitionOperatorInfo(pInfo);
  }
  pTaskInfo->code = code;
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  TAOS_RETURN(code);
}

int32_t setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                int32_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  SResultRow* pResultRow = doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo,
                                                  false, pAggSup, false);
  if (pResultRow == NULL || pTaskInfo->code != 0) {
    return pTaskInfo->code;
  }

  return setResultRowInitCtx(pResultRow, pCtx, numOfCols, pOperator->exprSupp.rowEntryInfoOffset);
}

SSDataBlock* buildCreateTableBlock(SExprSupp* tbName, SExprSupp* tag) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (!pBlock) {
    return NULL;
  }
  pBlock->info.hasVarCol = false;
  pBlock->info.id.groupId = 0;
  pBlock->info.rows = 0;
  pBlock->info.type = STREAM_CREATE_CHILD_TABLE;
  pBlock->info.watermark = INT64_MIN;

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
  QUERY_CHECK_NULL(pBlock->pDataBlock, code, lino, _end, terrno);
  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  if (tbName->numOfExprs > 0) {
    infoData.info.bytes = tbName->pExprInfo->base.resSchema.bytes;
  } else {
    infoData.info.bytes = 1;
  }
  pBlock->info.rowSize += infoData.info.bytes;
  // sub table name
  void* tmp = taosArrayPush(pBlock->pDataBlock, &infoData);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

  SColumnInfoData gpIdData = {0};
  gpIdData.info.type = TSDB_DATA_TYPE_UBIGINT;
  gpIdData.info.bytes = 8;
  pBlock->info.rowSize += gpIdData.info.bytes;
  // group id
  tmp = taosArrayPush(pBlock->pDataBlock, &gpIdData);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

  for (int32_t i = 0; i < tag->numOfExprs; i++) {
    SColumnInfoData tagCol = {0};
    tagCol.info.type = tag->pExprInfo[i].base.resSchema.type;
    tagCol.info.bytes = tag->pExprInfo[i].base.resSchema.bytes;
    tagCol.info.precision = tag->pExprInfo[i].base.resSchema.precision;
    // tag info
    tmp = taosArrayPush(pBlock->pDataBlock, &tagCol);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    pBlock->info.rowSize += tagCol.info.bytes;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pBlock);
    return NULL;
  }
  return pBlock;
}

void freePartItem(void* ptr) {
  SPartitionDataInfo* pPart = (SPartitionDataInfo*)ptr;
  taosArrayDestroy(pPart->rowIds);
}

int32_t extractColumnInfo(SNodeList* pNodeList, SArray** pArrayRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    code = terrno;
    (*pArrayRes) = NULL;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);

    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColumn c = extractColumnFromColumnNode(pColNode);
      void*   tmp = taosArrayPush(pList, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    } else if (nodeType(pNode->pExpr) == QUERY_NODE_VALUE) {
      SValueNode* pValNode = (SValueNode*)pNode->pExpr;
      SColumn     c = {0};
      c.slotId = pNode->slotId;
      c.colId = pNode->slotId;
      c.type = pValNode->node.type;
      c.bytes = pValNode->node.resType.bytes;
      c.scale = pValNode->node.resType.scale;
      c.precision = pValNode->node.resType.precision;

      void* tmp = taosArrayPush(pList, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  (*pArrayRes) = pList;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
