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
#include "index.h"
#include "function.h"
#include "functionMgt.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"

#include "executil.h"
#include "executorimpl.h"
#include "tcompression.h"

void initResultRowInfo(SResultRowInfo *pResultRowInfo) {
  pResultRowInfo->size       = 0;
  pResultRowInfo->cur.pageId = -1;
}

void cleanupResultRowInfo(SResultRowInfo *pResultRowInfo) {
  if (pResultRowInfo == NULL) {
    return;
  }

  for(int32_t i = 0; i < pResultRowInfo->size; ++i) {
//    if (pResultRowInfo->pResult[i]) {
//      taosMemoryFreeClear(pResultRowInfo->pResult[i]->key);
//    }
  }
}

void closeAllResultRows(SResultRowInfo *pResultRowInfo) {
// do nothing
}

bool isResultRowClosed(SResultRow* pRow) {
  return (pRow->closed == true);
}

void closeResultRow(SResultRow* pResultRow) {
  pResultRow->closed = true;
}

// TODO refactor: use macro
SResultRowEntryInfo* getResultEntryInfo(const SResultRow* pRow, int32_t index, const int32_t* offset) {
  assert(index >= 0 && offset != NULL);
  return (SResultRowEntryInfo*)((char*) pRow->pEntryInfo + offset[index]);
}

size_t getResultRowSize(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t rowSize = (numOfOutput * sizeof(SResultRowEntryInfo)) + sizeof(SResultRow);

  for(int32_t i = 0; i < numOfOutput; ++i) {
    rowSize += pCtx[i].resDataInfo.interBufSize;
  }

  return rowSize;
}

void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);

  taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->pRows     = NULL;
  pGroupResInfo->index     = 0;
}

static int32_t resultrowComparAsc(const void* p1, const void* p2) {
  SResKeyPos* pp1 = *(SResKeyPos**) p1;
  SResKeyPos* pp2 = *(SResKeyPos**) p2;

  if (pp1->groupId == pp2->groupId) {
    int64_t pts1 = *(int64_t*) pp1->key;
    int64_t pts2 = *(int64_t*) pp2->key;

    if (pts1 == pts2) {
      return 0;
    } else {
      return pts1 < pts2? -1:1;
    }
  } else {
    return pp1->groupId < pp2->groupId? -1:1;
  }
}

static int32_t resultrowComparDesc(const void* p1, const void* p2) {
  return resultrowComparAsc(p2, p1);
}

void initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SHashObj* pHashmap, int32_t order) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  // extract the result rows information from the hash map
  void* pData = NULL;
  pGroupResInfo->pRows = taosArrayInit(10, POINTER_BYTES);

  size_t keyLen = 0;
  while((pData = taosHashIterate(pHashmap, pData)) != NULL) {
    void* key = taosHashGetKey(pData, &keyLen);

    SResKeyPos* p = taosMemoryMalloc(keyLen + sizeof(SResultRowPosition));

    p->groupId = *(uint64_t*) key;
    p->pos = *(SResultRowPosition*) pData;
    memcpy(p->key, (char*)key + sizeof(uint64_t), keyLen - sizeof(uint64_t));

    taosArrayPush(pGroupResInfo->pRows, &p);
  }

  if (order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC) {
    __compar_fn_t fn = (order == TSDB_ORDER_ASC)? resultrowComparAsc:resultrowComparDesc;
    qsort(pGroupResInfo->pRows->pData, taosArrayGetSize(pGroupResInfo->pRows), POINTER_BYTES, fn);
  }

  pGroupResInfo->index = 0;
  assert(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroyP(pGroupResInfo->pRows, taosMemoryFree);
  }

  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
  ASSERT(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

bool hasDataInGroupInfo(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->pRows == NULL) {
    return false;
  }

  return pGroupResInfo->index < taosArrayGetSize(pGroupResInfo->pRows);
}

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);
  if (pGroupResInfo->pRows == 0) {
    return 0;
  }

  return (int32_t) taosArrayGetSize(pGroupResInfo->pRows);
}

SArray* createSortInfo(SNodeList* pNodeList) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SBlockOrderInfo));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pList;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SOrderByExprNode* pSortKey = (SOrderByExprNode*)nodesListGetNode(pNodeList, i);
    SBlockOrderInfo   bi = {0};
    bi.order = (pSortKey->order == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    bi.nullFirst = (pSortKey->nullOrder == NULL_ORDER_FIRST);

    SColumnNode* pColNode = (SColumnNode*)pSortKey->pExpr;
    bi.slotId = pColNode->slotId;
    taosArrayPush(pList, &bi);
  }

  return pList;
}

SSDataBlock* createResDataBlock(SDataBlockDescNode* pNode) {
  int32_t numOfCols = LIST_LENGTH(pNode->pSlots);

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  pBlock->info.blockId = pNode->dataBlockId;
  pBlock->info.rowSize = pNode->totalRowSize;  // todo ??
  pBlock->info.type = STREAM_INVALID;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData idata = {{0}};
    SSlotDescNode*  pDescNode = (SSlotDescNode*)nodesListGetNode(pNode->pSlots, i);
    //    if (!pDescNode->output) {  // todo disable it temporarily
    //      continue;
    //    }

    idata.info.type = pDescNode->dataType.type;
    idata.info.bytes = pDescNode->dataType.bytes;
    idata.info.scale = pDescNode->dataType.scale;
    idata.info.slotId = pDescNode->slotId;
    idata.info.precision = pDescNode->dataType.precision;

    if (IS_VAR_DATA_TYPE(idata.info.type)) {
      pBlock->info.hasVarCol = true;
    }

    taosArrayPush(pBlock->pDataBlock, &idata);
  }

  pBlock->info.numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  return pBlock;
}

int32_t getTableList(void* metaHandle, SScanPhysiNode* pScanNode, STableListInfo* pListInfo, SNode* pTagCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  pListInfo->pTableList = taosArrayInit(8, sizeof(STableKeyInfo));

  uint64_t tableUid = pScanNode->uid;

  if (pScanNode->tableType == TSDB_SUPER_TABLE) {
    if (pTagCond) {
      SIndexMetaArg metaArg = {
          .metaEx = metaHandle, .idx = tsdbGetIdx(metaHandle), .ivtIdx = tsdbGetIvtIdx(metaHandle), .suid = tableUid};

      SArray* res = taosArrayInit(8, sizeof(uint64_t));
      code = doFilterTag(pTagCond, &metaArg, res);
      if (code == TSDB_CODE_INDEX_REBUILDING) {  // todo
        // doFilter();
      } else if (code != TSDB_CODE_SUCCESS) {
        qError("failed  to  get tableIds, reason: %s, suid: %" PRIu64 "", tstrerror(code), tableUid);
        taosArrayDestroy(res);
        terrno = code;
        return code;
      } else {
        qDebug("sucess to  get tableIds, size: %d, suid: %" PRIu64 "", (int)taosArrayGetSize(res), tableUid);
      }

      for (int i = 0; i < taosArrayGetSize(res); i++) {
        STableKeyInfo info = {.lastKey = TSKEY_INITIAL_VAL, .uid = *(uint64_t*)taosArrayGet(res, i)};
        taosArrayPush(pListInfo->pTableList, &info);
      }
      taosArrayDestroy(res);
    } else {
      code = tsdbGetAllTableList(metaHandle, tableUid, pListInfo->pTableList);
    }
  } else {  // Create one table group.
    STableKeyInfo info = {.lastKey = 0, .uid = tableUid};
    taosArrayPush(pListInfo->pTableList, &info);
  }

  return code;
}

SArray* extractPartitionColInfo(SNodeList* pNodeList) {
  if(!pNodeList) {
    return NULL;
  }

  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pNodeList, i);

    // todo extract method
    SColumn c = {0};
    c.slotId = pColNode->slotId;
    c.colId = pColNode->colId;
    c.type = pColNode->node.resType.type;
    c.bytes = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    taosArrayPush(pList, &c);
  }

  return pList;
}


SArray* extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            int32_t type) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColMatchInfo));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

    SColMatchInfo c = {0};
    c.output = true;
    c.colId = pColNode->colId;
    c.srcSlotId = pColNode->slotId;
    c.matchType = type;
    c.targetSlotId = pNode->slotId;
    taosArrayPush(pList, &c);
  }

  *numOfOutputCols = 0;
  int32_t num = LIST_LENGTH(pOutputNodeList->pSlots);
  for (int32_t i = 0; i < num; ++i) {
    SSlotDescNode* pNode = (SSlotDescNode*)nodesListGetNode(pOutputNodeList->pSlots, i);

    // todo: add reserve flag check
    // it is a column reserved for the arithmetic expression calculation
    if (pNode->slotId >= numOfCols) {
      (*numOfOutputCols) += 1;
      continue;
    }

    SColMatchInfo* info = taosArrayGet(pList, pNode->slotId);
    if (pNode->output) {
      (*numOfOutputCols) += 1;
    } else {
      info->output = false;
    }
  }

  return pList;
}

static SResSchema createResSchema(int32_t type, int32_t bytes, int32_t slotId, int32_t scale, int32_t precision,
                                  const char* name) {
  SResSchema s = {0};
  s.scale = scale;
  s.type = type;
  s.bytes = bytes;
  s.slotId = slotId;
  s.precision = precision;
  strncpy(s.name, name, tListLen(s.name));

  return s;
}

static SColumn* createColumn(int32_t blockId, int32_t slotId, int32_t colId, SDataType* pType) {
  SColumn* pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (pCol == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCol->slotId = slotId;
  pCol->colId = colId;
  pCol->bytes = pType->bytes;
  pCol->type = pType->type;
  pCol->scale = pType->scale;
  pCol->precision = pType->precision;
  pCol->dataBlockId = blockId;

  return pCol;
}

SExprInfo* createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, int32_t* numOfExprs) {
  int32_t numOfFuncs = LIST_LENGTH(pNodeList);
  int32_t numOfGroupKeys = 0;
  if (pGroupKeys != NULL) {
    numOfGroupKeys = LIST_LENGTH(pGroupKeys);
  }

  *numOfExprs = numOfFuncs + numOfGroupKeys;
  SExprInfo* pExprs = taosMemoryCalloc(*numOfExprs, sizeof(SExprInfo));

  for (int32_t i = 0; i < (*numOfExprs); ++i) {
    STargetNode* pTargetNode = NULL;
    if (i < numOfFuncs) {
      pTargetNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    } else {
      pTargetNode = (STargetNode*)nodesListGetNode(pGroupKeys, i - numOfFuncs);
    }

    SExprInfo* pExp = &pExprs[i];

    pExp->pExpr = taosMemoryCalloc(1, sizeof(tExprNode));
    pExp->pExpr->_function.num = 1;
    pExp->pExpr->_function.functionId = -1;

    int32_t type = nodeType(pTargetNode->pExpr);
    // it is a project query, or group by column
    if (type == QUERY_NODE_COLUMN) {
      pExp->pExpr->nodeType = QUERY_NODE_COLUMN;
      SColumnNode* pColNode = (SColumnNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pColNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pColNode->colName);
      pExp->base.pParam[0].pCol = createColumn(pColNode->dataBlockId, pColNode->slotId, pColNode->colId, pType);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
    } else if (type == QUERY_NODE_VALUE) {
      pExp->pExpr->nodeType = QUERY_NODE_VALUE;
      SValueNode* pValNode = (SValueNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pValNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pValNode->node.aliasName);
      pExp->base.pParam[0].type = FUNC_PARAM_TYPE_VALUE;
      nodesValueNodeToVariant(pValNode, &pExp->base.pParam[0].param);
    } else if (type == QUERY_NODE_FUNCTION) {
      pExp->pExpr->nodeType = QUERY_NODE_FUNCTION;
      SFunctionNode* pFuncNode = (SFunctionNode*)pTargetNode->pExpr;

      SDataType* pType = &pFuncNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pFuncNode->node.aliasName);

      pExp->pExpr->_function.functionId = pFuncNode->funcId;
      pExp->pExpr->_function.pFunctNode = pFuncNode;

      strncpy(pExp->pExpr->_function.functionName, pFuncNode->functionName,
              tListLen(pExp->pExpr->_function.functionName));
#if 1
      // todo refactor: add the parameter for tbname function
      if (strcmp(pExp->pExpr->_function.functionName, "tbname") == 0) {
        pFuncNode->pParameterList = nodesMakeList();
        ASSERT(LIST_LENGTH(pFuncNode->pParameterList) == 0);
        SValueNode* res = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
        if (NULL == res) {  // todo handle error
        } else {
          res->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
          nodesListAppend(pFuncNode->pParameterList, (SNode*)res);
        }
      }
#endif

      int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);

      pExp->base.pParam = taosMemoryCalloc(numOfParam, sizeof(SFunctParam));
      pExp->base.numOfParams = numOfParam;

      for (int32_t j = 0; j < numOfParam; ++j) {
        SNode* p1 = nodesListGetNode(pFuncNode->pParameterList, j);
        if (p1->type == QUERY_NODE_COLUMN) {
          SColumnNode* pcn = (SColumnNode*)p1;

          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_COLUMN;
          pExp->base.pParam[j].pCol = createColumn(pcn->dataBlockId, pcn->slotId, pcn->colId, &pcn->node.resType);
        } else if (p1->type == QUERY_NODE_VALUE) {
          SValueNode* pvn = (SValueNode*)p1;
          pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
          nodesValueNodeToVariant(pvn, &pExp->base.pParam[j].param);
        }
      }
    } else if (type == QUERY_NODE_OPERATOR) {
      pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
      SOperatorNode* pNode = (SOperatorNode*)pTargetNode->pExpr;

      pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
      pExp->base.numOfParams = 1;

      SDataType* pType = &pNode->node.resType;
      pExp->base.resSchema = createResSchema(pType->type, pType->bytes, pTargetNode->slotId, pType->scale,
                                             pType->precision, pNode->node.aliasName);
      pExp->pExpr->_optrRoot.pRootNode = pTargetNode->pExpr;
    } else {
      ASSERT(0);
    }
  }

  return pExprs;
}

// set the output buffer for the selectivity + tag query
static int32_t setSelectValueColumnInfo(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t num = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pValCtx = taosMemoryCalloc(numOfOutput, POINTER_BYTES);
  if (pValCtx == NULL) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (strcmp(pCtx[i].pExpr->pExpr->_function.functionName, "_select_value") == 0) {
      pValCtx[num++] = &pCtx[i];
    } else if (fmIsSelectFunc(pCtx[i].functionId)) {
      p = &pCtx[i];
    }
  }

  if (p != NULL) {
    p->subsidiaries.pCtx = pValCtx;
    p->subsidiaries.num = num;
  } else {
    taosMemoryFreeClear(pValCtx);
  }

  return TSDB_CODE_SUCCESS;
}

SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowEntryInfoOffset) {
  SqlFunctionCtx* pFuncCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowEntryInfoOffset = taosMemoryCalloc(numOfOutput, sizeof(int32_t));
  if (*rowEntryInfoOffset == 0) {
    taosMemoryFreeClear(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = &pExprInfo[i];

    SExprBasicInfo* pFunct = &pExpr->base;
    SqlFunctionCtx* pCtx = &pFuncCtx[i];

    pCtx->functionId = -1;
    pCtx->curBufPage = -1;
    pCtx->pExpr = pExpr;

    if (pExpr->pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SFuncExecEnv env = {0};
      pCtx->functionId = pExpr->pExpr->_function.pFunctNode->funcId;

      if (fmIsAggFunc(pCtx->functionId) || fmIsIndefiniteRowsFunc(pCtx->functionId)) {
        bool isUdaf = fmIsUserDefinedFunc(pCtx->functionId);
        if (!isUdaf) {
          fmGetFuncExecFuncs(pCtx->functionId, &pCtx->fpSet);
        } else {
          char* udfName = pExpr->pExpr->_function.pFunctNode->functionName;
          strncpy(pCtx->udfName, udfName, strlen(udfName));
          fmGetUdafExecFuncs(pCtx->functionId, &pCtx->fpSet);
        }
        pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
      } else {
        fmGetScalarFuncExecFuncs(pCtx->functionId, &pCtx->sfp);
        if (pCtx->sfp.getEnv != NULL) {
          pCtx->sfp.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
        }
      }
      pCtx->resDataInfo.interBufSize = env.calcMemSize;
    } else if (pExpr->pExpr->nodeType == QUERY_NODE_COLUMN || pExpr->pExpr->nodeType == QUERY_NODE_OPERATOR ||
               pExpr->pExpr->nodeType == QUERY_NODE_VALUE) {
      // for simple column, the result buffer needs to hold at least one element.
      pCtx->resDataInfo.interBufSize = pFunct->resSchema.bytes;
    }

    pCtx->input.numOfInputCols = pFunct->numOfParams;
    pCtx->input.pData = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);
    pCtx->input.pColumnDataAgg = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);

    pCtx->pTsOutput = NULL;
    pCtx->resDataInfo.bytes = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type = pFunct->resSchema.type;
    pCtx->order = TSDB_ORDER_ASC;
    pCtx->start.key = INT64_MIN;
    pCtx->end.key = INT64_MIN;
    pCtx->numOfParams = pExpr->base.numOfParams;
    pCtx->increase = false;

    pCtx->param = pFunct->pParam;
  }

  for (int32_t i = 1; i < numOfOutput; ++i) {
    (*rowEntryInfoOffset)[i] =
        (int32_t)((*rowEntryInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) + pFuncCtx[i - 1].resDataInfo.interBufSize);
  }

  setSelectValueColumnInfo(pFuncCtx, numOfOutput);
  return pFuncCtx;
}

// NOTE: sources columns are more than the destination SSDatablock columns.
void relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols) {
  size_t numOfSrcCols = taosArrayGetSize(pCols);

  int32_t i = 0, j = 0;
  while (i < numOfSrcCols && j < taosArrayGetSize(pColMatchInfo)) {
    SColumnInfoData* p = taosArrayGet(pCols, i);
    SColMatchInfo*   pmInfo = taosArrayGet(pColMatchInfo, j);
    if (!pmInfo->output) {
      j++;
      continue;
    }

    if (p->info.colId == pmInfo->colId) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, pmInfo->targetSlotId);
      colDataAssign(pDst, p, pBlock->info.rows);
      i++;
      j++;
    } else if (p->info.colId < pmInfo->colId) {
      i++;
    } else {
      ASSERT(0);
    }
  }
}

SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode) {
  SInterval interval = {
      .interval = pTableScanNode->interval,
      .sliding = pTableScanNode->sliding,
      .intervalUnit = pTableScanNode->intervalUnit,
      .slidingUnit = pTableScanNode->slidingUnit,
      .offset = pTableScanNode->offset,
  };

  return interval;
}

SColumn extractColumnFromColumnNode(SColumnNode* pColNode) {
  SColumn c = {0};
  c.slotId = pColNode->slotId;
  c.colId = pColNode->colId;
  c.type = pColNode->node.resType.type;
  c.bytes = pColNode->node.resType.bytes;
  c.scale = pColNode->node.resType.scale;
  c.precision = pColNode->node.resType.precision;
  return c;
}

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode) {
  pCond->loadExternalRows = false;

  pCond->order = pTableScanNode->scanSeq[0] > 0 ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pCond->numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);
  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  if (pCond->colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return terrno;
  }

  // pCond->twindow = pTableScanNode->scanRange;
  // TODO: get it from stable scan node
  pCond->numOfTWindows = 1;
  pCond->twindows = taosMemoryCalloc(pCond->numOfTWindows, sizeof(STimeWindow));
  pCond->twindows[0] = pTableScanNode->scanRange;
  pCond->suid = pTableScanNode->scan.suid;

#if 1
  // todo work around a problem, remove it later
  for (int32_t i = 0; i < pCond->numOfTWindows; ++i) {
    if ((pCond->order == TSDB_ORDER_ASC && pCond->twindows[i].skey > pCond->twindows[i].ekey) ||
        (pCond->order == TSDB_ORDER_DESC && pCond->twindows[i].skey < pCond->twindows[i].ekey)) {
      TSWAP(pCond->twindows[i].skey, pCond->twindows[i].ekey);
    }
  }
#endif

  for (int32_t i = 0; i < pCond->numOfTWindows; ++i) {
    if ((pCond->order == TSDB_ORDER_ASC && pCond->twindows[i].skey > pCond->twindows[i].ekey) ||
        (pCond->order == TSDB_ORDER_DESC && pCond->twindows[i].skey < pCond->twindows[i].ekey)) {
      TSWAP(pCond->twindows[i].skey, pCond->twindows[i].ekey);
    }
  }
  taosqsort(pCond->twindows, pCond->numOfTWindows, sizeof(STimeWindow), pCond, compareTimeWindow);

  pCond->type = BLOCK_LOAD_OFFSET_SEQ_ORDER;
  //  pCond->type = pTableScanNode->scanFlag;

  int32_t j = 0;
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pTableScanNode->scan.pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
    if (pColNode->colType == COLUMN_TYPE_TAG) {
      continue;
    }

    pCond->colList[j].type = pColNode->node.resType.type;
    pCond->colList[j].bytes = pColNode->node.resType.bytes;
    pCond->colList[j].colId = pColNode->colId;
    j += 1;
  }

  pCond->numOfCols = j;
  return TSDB_CODE_SUCCESS;
}

void cleanupQueryTableDataCond(SQueryTableDataCond* pCond) {
  taosMemoryFree(pCond->twindows);
  taosMemoryFree(pCond->colList);
}