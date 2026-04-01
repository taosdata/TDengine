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

#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tcompare.h"
#include "virtualtablescan.h"

typedef struct SVirtualTableScanInfo {
  STableScanBase base;
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  int32_t        bufPageSize;
  uint64_t       sortBufSize;         // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;  // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SSHashObj*     dataSlotMap;
  SSHashObj*     refSlotMap;     // key: slotId, value: SArray<int32_t>*
  SArray*        refSlotGroups;  // SArray<SArray<int32_t>>
  int32_t        tsSlotId;
  int64_t        tagBlockId;
  int32_t        tagDownStreamId;
  bool           scanAllCols;
  bool           useOrgTsCol;
  SArray*        pSortCtxList;
  SArray*        pTagRefSourceBlockIds;
  SSHashObj*     pTagRefDownstreamMap;
  SArray*        pSavedTagRefBlocks;
  SNodeList*     pTargets;
  SNodeList*     pScanPseudoCols;
  SNodeList*     pRefTagCols;
  tb_uid_t       vtableUid;  // virtual table uid, used to identify the vtable scan operator
  char           vtableName[TSDB_TABLE_NAME_LEN];
} SVirtualTableScanInfo;

typedef struct SVirtualScanMergeOperatorInfo {
  SOptrBasicInfo        binfo;
  EMergeType            type;
  SVirtualTableScanInfo virtualScanInfo;
  bool                  ignoreGroupId;
  uint64_t              groupId;
  STupleHandle*         pSavedTuple;
  SSDataBlock*          pSavedTagBlock;
} SVirtualScanMergeOperatorInfo;

typedef struct SLoadNextCtx {
  SOperatorInfo*  pOperator;
  SOperatorParam* pOperatorGetParam;
  int64_t         blockId;
  STimeWindow     window;
  SSDataBlock*    pIntermediateBlock;
} SLoadNextCtx;

typedef struct {
  int32_t downstreamId;
  int64_t blockId;
} STagRefDownstreamInfo;

typedef struct {
  int64_t      blockId;
  SSDataBlock* pBlock;
} STagRefSavedBlock;

static bool isTagRefTargetColumn(const SColumnNode* pColNode) {
  return pColNode != NULL && pColNode->colType == COLUMN_TYPE_TAG && '\0' == pColNode->tableAlias[0];
}

// Classify downstream exchange: tag-ref sources (from pRefTagCols) are NOT sortable,
// everything else is sortable data.  This mirrors main-branch logic (tag vs data)
// extended with one extra check for tag-ref exchanges.
static bool isSortableDataBlockId(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL) {
    return true;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pRefTagCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->dataBlockId == blockId) {
      return false;
    }
  }

  return true;
}

static STagRefSavedBlock* findSavedTagRefBlock(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pSavedTagRefBlocks == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSavedTagRefBlocks); ++i) {
    STagRefSavedBlock* pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, i);
    if (pSaved != NULL && pSaved->blockId == blockId) {
      return pSaved;
    }
  }

  return NULL;
}

static const STagVal* findResolvedTagVal(const SOperatorInfo* pOperator, col_id_t colId) {
  if (pOperator == NULL || pOperator->pOperatorGetParam == NULL) {
    return NULL;
  }

  SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
  if (pParam == NULL || pParam->pResolvedTags == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pParam->pResolvedTags); ++i) {
    const STagVal* pTagVal = taosArrayGet(pParam->pResolvedTags, i);
    if (pTagVal != NULL && pTagVal->cid == colId) {
      return pTagVal;
    }
  }

  return NULL;
}

static int32_t setTagValueToColumn(SColumnInfoData* pDstCol, const STagVal* pTagVal, int32_t rows) {
  if (pTagVal == NULL) {
    colDataSetNNULL(pDstCol, 0, rows);
    return TSDB_CODE_SUCCESS;
  }

  char* data = NULL;
  if (pDstCol->info.type == TSDB_DATA_TYPE_JSON) {
    data = (char*)pTagVal->pData;
  } else if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    data = tTagValToData(pTagVal, false);
  } else {
    data = (char*)&pTagVal->i64;
  }

  bool    isNullVal = (data == NULL) || (pDstCol->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
  int32_t code = TSDB_CODE_SUCCESS;
  if (isNullVal) {
    colDataSetNNULL(pDstCol, 0, rows);
  } else {
    for (int32_t i = 0; i < rows; ++i) {
      code = colDataSetVal(pDstCol, i, data, false);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
  }

  if (pDstCol->info.type != TSDB_DATA_TYPE_JSON && IS_VAR_DATA_TYPE(pTagVal->type) && data != NULL) {
    taosMemoryFree(data);
  }

  return code;
}

static int32_t setTagColumnValue(SColumnInfoData* pDstCol, SColumnInfoData* pSrcCol, const STagVal* pResolvedTagVal,
                                 int32_t rows) {
  int32_t code = TSDB_CODE_SUCCESS;

  colInfoDataCleanup(pDstCol, rows);
  if (pSrcCol == NULL || colDataIsNull_s(pSrcCol, 0) || IS_JSON_NULL(pSrcCol->info.type, colDataGetData(pSrcCol, 0))) {
    if (pResolvedTagVal != NULL) {
      return setTagValueToColumn(pDstCol, pResolvedTagVal, rows);
    }

    colDataSetNNULL(pDstCol, 0, rows);
    return TSDB_CODE_SUCCESS;
  }

  char* data = colDataGetData(pSrcCol, 0);
  for (int32_t i = 0; i < rows; ++i) {
    code = colDataSetVal(pDstCol, i, data, false);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  return code;
}

static bool isRefTagSourceBlockId(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pRefTagCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pColNode = (SColumnNode*)pNode;
    if (pColNode->dataBlockId == blockId) {
      return true;
    }
  }

  return false;
}

int32_t virtualScanloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SLoadNextCtx*  pCtx = (SLoadNextCtx*)param;
  SOperatorInfo* pOperator = pCtx->pOperator;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        line = 0;
  SSDataBlock*   pRes = NULL;

  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }

  VTS_ERR_JRET(pOperator->fpSet.getNextFn(pOperator, &pRes));
  VTS_ERR_JRET(blockDataCheck(pRes));
  if (pRes) {
    VTS_ERR_JRET(createOneDataBlock(pRes, true, &pCtx->pIntermediateBlock));
    SColumnInfoData* p = taosArrayGet(pCtx->pIntermediateBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(p, code, line, _return, terrno);
    if (p->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
      p->hasNull = false;
    }
    *ppBlock = pCtx->pIntermediateBlock;
  } else {
    *ppBlock = NULL;
  }

  return code;
_return:
  qError("failed to load data block from downstream, %s code:%s, line:%d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Read first/last timestamp from one data block.
 *
 * @param pBlock Input data block.
 * @param startTs Output start timestamp.
 * @param endTs Output end timestamp.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t getTimeWindowOfBlock(SSDataBlock* pBlock, int64_t* startTs, int64_t* endTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);

  SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, 0);
  QUERY_CHECK_NULL(pColData, code, lino, _return, terrno)

  if (pColData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    *startTs = INT64_MIN;
    *endTs = INT64_MAX;
    return code;
  }

  pColData->hasNull = false;
  GET_TYPED_DATA(*startTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, 0), 0);
  GET_TYPED_DATA(*endTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, pBlock->info.rows - 1), 0);

  return code;
_return:
  qError("failed to get time window of block, %s code:%s, line:%d", __func__, tstrerror(code), lino);
  return code;
}

int32_t virtualScanloadNextDataBlockFromParam(void* param, SSDataBlock** ppBlock) {
  SLoadNextCtx*           pCtx = (SLoadNextCtx*)param;
  SOperatorInfo*          pOperator = pCtx->pOperator;
  SOperatorParam*         pOperatorGetParam = pCtx->pOperatorGetParam;
  int32_t                 code = TSDB_CODE_SUCCESS;
  SSDataBlock*            pRes = NULL;
  SExchangeOperatorParam* pParam = (SExchangeOperatorParam*)pOperatorGetParam->value;

  pParam->basic.window = pCtx->window;
  pOperator->status = OP_NOT_OPENED;
  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }

  // Same source param is reused by sort callbacks; downstream must not free it.
  pOperatorGetParam->reUse = true;
  VTS_ERR_JRET(pOperator->fpSet.getNextExtFn(pOperator, pOperatorGetParam, &pRes));

  pParam->basic.isNewParam = false;
  if ((pRes)) {
    qDebug("%s load from downstream, blockId:%" PRId64, __func__, pCtx->blockId);
    (pRes)->info.id.blockId = pCtx->blockId;
    VTS_ERR_JRET(getTimeWindowOfBlock(pRes, &pCtx->window.skey, &pCtx->window.ekey));
    VTS_ERR_JRET(createOneDataBlock(pRes, true, &pCtx->pIntermediateBlock));
    *ppBlock = pCtx->pIntermediateBlock;
  } else {
    pCtx->window.ekey = INT64_MAX;
    *ppBlock = NULL;
  }

  return code;
_return:
  qError("failed to load data block from downstream, %s code:%s", __func__, tstrerror(code));
  return code;
}

int32_t makeTSMergeKey(SNodeList** pMergeKeys, col_id_t tsSlotId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SNodeList*        pNodeList = NULL;
  SColumnNode*      pColumnNode = NULL;
  SOrderByExprNode* pOrderByExprNode = NULL;

  VTS_ERR_JRET(nodesMakeList(&pNodeList));

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumnNode));
  pColumnNode->slotId = tsSlotId;

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExprNode));
  pOrderByExprNode->pExpr = (SNode*)pColumnNode;
  pOrderByExprNode->order = ORDER_ASC;
  pOrderByExprNode->nullOrder = NULL_ORDER_FIRST;

  VTS_ERR_JRET(nodesListAppend(pNodeList, (SNode*)pOrderByExprNode));

  *pMergeKeys = pNodeList;
  return code;
_return:
  nodesDestroyNode((SNode*)pColumnNode);
  nodesDestroyNode((SNode*)pOrderByExprNode);
  nodesDestroyList(pNodeList);
  return code;
}

static void cleanupSavedTagRefBlocks(SVirtualTableScanInfo* pInfo);

void cleanUpVirtualScanInfo(SVirtualTableScanInfo* pVirtualScanInfo) {
  if (pVirtualScanInfo->pSortInfo) {
    taosArrayDestroy(pVirtualScanInfo->pSortInfo);
    pVirtualScanInfo->pSortInfo = NULL;
  }
  if (pVirtualScanInfo->pSortHandle) {
    tsortDestroySortHandle(pVirtualScanInfo->pSortHandle);
    pVirtualScanInfo->pSortHandle = NULL;
  }
  if (pVirtualScanInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pVirtualScanInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pVirtualScanInfo->pSortCtxList);
    pVirtualScanInfo->pSortCtxList = NULL;
  }
  pVirtualScanInfo->vtableName[0] = '\0';

  taosArrayDestroy(pVirtualScanInfo->pTagRefSourceBlockIds);
  pVirtualScanInfo->pTagRefSourceBlockIds = NULL;
  cleanupSavedTagRefBlocks(pVirtualScanInfo);
  tSimpleHashCleanup(pVirtualScanInfo->pTagRefDownstreamMap);
  pVirtualScanInfo->pTagRefDownstreamMap = NULL;
}

static void cleanupRefSlotGroups(SVirtualTableScanInfo* pInfo) {
  if (pInfo->refSlotMap) {
    tSimpleHashCleanup(pInfo->refSlotMap);
    pInfo->refSlotMap = NULL;
  }
  if (pInfo->refSlotGroups) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->refSlotGroups); i++) {
      SArray* slots = *(SArray**)taosArrayGet(pInfo->refSlotGroups, i);
      taosArrayDestroy(slots);
    }
    taosArrayDestroy(pInfo->refSlotGroups);
    pInfo->refSlotGroups = NULL;
  }
}

static void cleanupSavedTagRefBlocks(SVirtualTableScanInfo* pInfo) {
  if (pInfo->pSavedTagRefBlocks == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSavedTagRefBlocks); ++i) {
    STagRefSavedBlock* pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, i);
    if (pSaved != NULL) {
      blockDataDestroy(pSaved->pBlock);
    }
  }

  taosArrayDestroy(pInfo->pSavedTagRefBlocks);
  pInfo->pSavedTagRefBlocks = NULL;
}

static int32_t buildRefSlotGroupsFromParam(SVirtualTableScanInfo* pInfo, SArray* pRefColGroups) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pRefColGroups == NULL || taosArrayGetSize(pRefColGroups) == 0) {
    return code;
  }

  cleanupRefSlotGroups(pInfo);

  pInfo->refSlotGroups = taosArrayInit(taosArrayGetSize(pRefColGroups), POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->refSlotGroups, code, lino, _return, terrno)
  pInfo->refSlotMap = tSimpleHashInit(taosArrayGetSize(pRefColGroups), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pInfo->refSlotMap, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pRefColGroups); i++) {
    SRefColIdGroup* pGroup = (SRefColIdGroup*)taosArrayGet(pRefColGroups, i);
    QUERY_CHECK_NULL(pGroup, code, lino, _return, terrno)
    if (pGroup->pSlotIdList == NULL || taosArrayGetSize(pGroup->pSlotIdList) <= 1) {
      continue;
    }

    SArray* pSlots = taosArrayInit(taosArrayGetSize(pGroup->pSlotIdList), sizeof(int32_t));
    QUERY_CHECK_NULL(pSlots, code, lino, _return, terrno)
    for (int32_t j = 0; j < taosArrayGetSize(pGroup->pSlotIdList); j++) {
      int32_t* pSlotId = taosArrayGet(pGroup->pSlotIdList, j);
      if (pSlotId == NULL || *pSlotId < 0) {
        continue;
      }
      int32_t slotId = *pSlotId;
      QUERY_CHECK_NULL(taosArrayPush(pSlots, &slotId), code, lino, _return, terrno)
    }

    if (taosArrayGetSize(pSlots) > 1) {
      taosArraySort(pSlots, compareInt32Val);
      QUERY_CHECK_NULL(taosArrayPush(pInfo->refSlotGroups, &pSlots), code, lino, _return, terrno)
      for (int32_t j = 0; j < taosArrayGetSize(pSlots); j++) {
        int32_t slotId = *(int32_t*)taosArrayGet(pSlots, j);
        VTS_ERR_JRET(tSimpleHashPut(pInfo->refSlotMap, &slotId, sizeof(slotId), &pSlots, POINTER_BYTES));
      }
    } else {
      taosArrayDestroy(pSlots);
    }
  }

  return code;

_return:
  cleanupRefSlotGroups(pInfo);
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createSortHandleFromParam(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SVTableScanOperatorParam*      pParam = (SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  pVirtualScanInfo->sortBufSize = pVirtualScanInfo->bufPageSize * (taosArrayGetSize((pParam)->pOpParamArray) + 1);
  int32_t numOfBufPage = (int32_t)((uint64_t)pVirtualScanInfo->sortBufSize / (uint64_t)pVirtualScanInfo->bufPageSize);
  SNodeList*    pMergeKeys = NULL;
  SSortSource*  ps = NULL;
  int32_t       scanOpIndex = 0;
  SLoadNextCtx* pCtx = NULL;

  cleanUpVirtualScanInfo(pVirtualScanInfo);
  VTS_ERR_JRET(buildRefSlotGroupsFromParam(pVirtualScanInfo, pParam->pRefColGroups));
  VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
  pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  NODES_DESTORY_LIST(pMergeKeys);

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE,
                                     pVirtualScanInfo->bufPageSize, numOfBufPage, pVirtualScanInfo->pInputBlock,
                                     pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlockFromParam, NULL, NULL);

  // Find the tag scan downstream by matching resultDataBlockId (most reliable).
  // Don't trust pParam->tagDownStreamId blindly — it may be stale when
  // TagRefSource exchanges shift the downstream indices.
  pVirtualScanInfo->tagDownStreamId = -1;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      pVirtualScanInfo->tagDownStreamId = i;
      pInfo->pSavedTagBlock = NULL;
      break;
    }
  }
  if (pVirtualScanInfo->tagDownStreamId == -1 &&
      pParam->tagDownStreamId >= 0 &&
      pParam->tagDownStreamId < pOperator->numOfDownstream) {
    pVirtualScanInfo->tagDownStreamId = pParam->tagDownStreamId;
    pInfo->pSavedTagBlock = NULL;
  }

  // Find the data scan downstream (first non-tag-scan, non-TagRefSource downstream)
  scanOpIndex = -1;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    if (i == pVirtualScanInfo->tagDownStreamId) continue;
    if (isRefTagSourceBlockId(pVirtualScanInfo, pOperator->pDownstream[i]->resultDataBlockId)) continue;
    scanOpIndex = i;
    break;
  }
  if (scanOpIndex < 0) {
    scanOpIndex = 0;
  }

  pOperator->pDownstream[scanOpIndex]->status = OP_NOT_OPENED;
  pVirtualScanInfo->pSortCtxList = taosArrayInit(taosArrayGetSize((pParam)->pOpParamArray), POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno)
  for (size_t i = 0; i < taosArrayGetSize((pParam)->pOpParamArray); i++) {
    SOperatorParam* pOpParam = *(SOperatorParam**)taosArrayGet((pParam)->pOpParamArray, i);

    pCtx = taosMemoryMalloc(sizeof(SLoadNextCtx));
    QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno)
    pCtx->blockId = (int64_t)i;
    pCtx->pOperator = pOperator->pDownstream[scanOpIndex];
    pCtx->pOperatorGetParam = pOpParam;
    pCtx->window = pParam->window;
    pCtx->pIntermediateBlock = NULL;

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(ps, code, lino, _return, terrno)

    ps->param = pCtx;
    ps->onlyRef = true;

    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
    pCtx = NULL;
    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    ps = NULL;
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

_return:
  if (code != 0) {
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
    if (ps) {
      taosMemoryFree(ps);
    }
    if (pCtx) {
      taosMemoryFree(pCtx);
    }
    NODES_DESTORY_LIST(pMergeKeys);
  }
  return code;
}

int32_t createSortHandle(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  int32_t      numOfBufPage = (int32_t)(pVirtualScanInfo->sortBufSize / pVirtualScanInfo->bufPageSize);
  SSortSource* ps = NULL;
  SLoadNextCtx* pCtx = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE,
                                     pVirtualScanInfo->bufPageSize, numOfBufPage, pVirtualScanInfo->pInputBlock,
                                     pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlock, NULL, NULL);

  pVirtualScanInfo->pSortCtxList = taosArrayInit(pOperator->numOfDownstream, POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno)

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      VTS_ERR_JRET(pDownstream->fpSet._openFn(pDownstream));
    } else {
      VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM);
    }

    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      // tag block do not need sort
      pVirtualScanInfo->tagDownStreamId = i;
      continue;
    }

    if (!isSortableDataBlockId(pVirtualScanInfo, pDownstream->resultDataBlockId)) {
      if (pVirtualScanInfo->tagDownStreamId == -1 && pVirtualScanInfo->pScanPseudoCols != NULL &&
          !isRefTagSourceBlockId(pVirtualScanInfo, pDownstream->resultDataBlockId)) {
        pVirtualScanInfo->tagDownStreamId = i;
        continue;
      }

      if (pVirtualScanInfo->pTagRefSourceBlockIds == NULL) {
        pVirtualScanInfo->pTagRefSourceBlockIds =
            taosArrayInit(pOperator->numOfDownstream, sizeof(int64_t));
        TSDB_CHECK_NULL(pVirtualScanInfo->pTagRefSourceBlockIds, code, lino, _return, terrno)
      }

      if (pVirtualScanInfo->pTagRefDownstreamMap == NULL) {
        pVirtualScanInfo->pTagRefDownstreamMap =
            tSimpleHashInit(pOperator->numOfDownstream, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        TSDB_CHECK_NULL(pVirtualScanInfo->pTagRefDownstreamMap, code, lino, _return, terrno)
      }

      QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pTagRefSourceBlockIds, &pDownstream->resultDataBlockId), code, lino,
                       _return, terrno)
      STagRefDownstreamInfo downstreamInfo = {.downstreamId = i, .blockId = pDownstream->resultDataBlockId};
      VTS_ERR_JRET(tSimpleHashPut(pVirtualScanInfo->pTagRefDownstreamMap, &downstreamInfo.blockId,
                                  sizeof(downstreamInfo.blockId), &downstreamInfo, sizeof(downstreamInfo)));
      continue;
    }

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    TSDB_CHECK_NULL(ps, code, lino, _return, terrno)

    pCtx = taosMemoryCalloc(1, sizeof(SLoadNextCtx));
    TSDB_CHECK_NULL(pCtx, code, lino, _return, terrno)
    pCtx->pOperator = pDownstream;
    pCtx->pOperatorGetParam = NULL;
    pCtx->blockId = i;
    pCtx->window = (STimeWindow){0};
    pCtx->pIntermediateBlock = NULL;

    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
    ps->param = pCtx;
    ps->onlyRef = true;

    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    pCtx = NULL;
    ps = NULL;
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

_return:
  if (code != 0) {
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  if (pCtx != NULL) {
    taosMemoryFree(pCtx);
  }
  if (ps != NULL) {
    taosMemoryFree(ps);
  }
  return code;
}

int32_t openVirtualTableScanOperatorImpl(SOperatorInfo* pOperator) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pOperator->numOfDownstream == 0) {
    return code;
  }

  if (pOperator->pOperatorGetParam) {
    // The param is referenced by sort source contexts and may still be used by
    // async fetch callbacks when this operator is being torn down.
    pOperator->pOperatorGetParam->reUse = true;
  }

  if (pOperator->pOperatorGetParam) {
    VTS_ERR_JRET(createSortHandleFromParam(pOperator));
  } else {
    VTS_ERR_JRET(createSortHandle(pOperator));
  }

  return code;

_return:
  qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  return code;
}

int32_t openVirtualTableScanOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  code = openVirtualTableScanOperatorImpl(pOperator);

  pOperator->status = OP_RES_TO_RETURN;

  VTS_ERR_RET(code);

  OPTR_SET_OPENED(pOperator);
  return code;
}

static int32_t doGetVtableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                          SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = INT64_MIN;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    int32_t blockId = (int32_t)tsortGetBlockId(pTupleHandle);
    int32_t colNum = pInfo->virtualScanInfo.scanAllCols ? 1 : (int32_t)tsortGetColNum(pTupleHandle);
    for (int32_t i = 0; i < colNum; i++) {
      char* pData = NULL;
      tsortGetValue(pTupleHandle, i, (void**)&pData);
      if (pData != NULL) {
        if (i == 0) {
          if (lastTs != *(int64_t*)pData) {
            if (rowNums >= capacity - 1) {
              pInfo->pSavedTuple = pTupleHandle;
              goto _return;
            }
            rowNums++;
            if (pInfo->virtualScanInfo.tsSlotId != -1) {
              VTS_ERR_JRET(
                  colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
            }
            lastTs = *(int64_t*)pData;
          }
          if (pInfo->virtualScanInfo.useOrgTsCol && rowNums >= 0) {
            int64_t slotKey = blockId << 16 | i;
            void*   slotId = tSimpleHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
            if (slotId) {
              VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, *(int32_t*)slotId), rowNums, pData, false));
            }
          }
          continue;
        }
        if (tsortIsNullVal(pTupleHandle, i)) {
          continue;
        }
        int64_t slotKey = blockId << 16 | i;
        void*   slotId = tSimpleHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
        if (slotId == NULL) {
          qError("failed to get slotId from dataSlotMap, blockId:%d, slotId:%d", blockId, i);
          VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
        }
        VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, *(int32_t*)slotId), rowNums, pData, false));
      }
    }
  }
_return:
  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = MAIN_SCAN;
  return code;
}

static int32_t doGetVStableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                           SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = INT64_MIN;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    int32_t tsIndex = 0;
    size_t  colNum = tsortGetColNum(pTupleHandle);

    char* pData = NULL;
    // first, set ts slot's data
    // then, set other slots' data
    tsortGetValue(pTupleHandle, tsIndex, (void**)&pData);

    if (pData != NULL) {
      if (lastTs != *(int64_t*)pData) {
        if (rowNums >= capacity - 1) {
          pInfo->pSavedTuple = pTupleHandle;
          goto _return;
        }
        rowNums++;
        if (pInfo->virtualScanInfo.tsSlotId != -1) {
          VTS_ERR_JRET(
              colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
        }
        lastTs = *(int64_t*)pData;
      }
    }
    if (pInfo->virtualScanInfo.scanAllCols) {
      continue;
    }

    for (int32_t i = 0; i < colNum; i++) {
      if (tsortIsNullVal(pTupleHandle, i)) {
        continue;
      }

      SColumnInfoData* pColInfo = NULL;
      tsortGetColumnInfo(pTupleHandle, i, &pColInfo);
      tsortGetValue(pTupleHandle, i, (void**)&pData);

      if (pData != NULL) {
        if (pInfo->virtualScanInfo.refSlotMap) {
          SArray** ppSlots =
              tSimpleHashGet(pInfo->virtualScanInfo.refSlotMap, (int32_t*)&pColInfo->info.slotId, sizeof(int32_t));
          if (ppSlots && taosArrayGetSize(*ppSlots) > 1) {
            for (int32_t k = 0; k < taosArrayGetSize(*ppSlots); k++) {
              int32_t slotId = *(int32_t*)taosArrayGet(*ppSlots, k);
              VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, slotId), rowNums, pData, false));
            }
            continue;
          }
        }
        if (pColInfo->info.slotId != -1) {
          VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, pColInfo->info.slotId), rowNums, pData, false));
        }
      }
    }
  }
_return:

  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = MAIN_SCAN;
  return code;
}

int32_t doVirtualTableMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = 0;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SSortHandle*                   pHandle = pVirtualScanInfo->pSortHandle;
  SSDataBlock*                   pDataBlock = pInfo->binfo.pRes;
  int32_t                        capacity = pOperator->resultInfo.capacity;

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pDataBlock);

  if (pHandle == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (pVirtualScanInfo->pIntermediateBlock == NULL) {
    VTS_ERR_RET(tsortGetSortedDataBlock(pHandle, &pVirtualScanInfo->pIntermediateBlock));
    if (pVirtualScanInfo->pIntermediateBlock == NULL) {
      return TSDB_CODE_SUCCESS;
    }

    VTS_ERR_RET(blockDataEnsureCapacity(pVirtualScanInfo->pIntermediateBlock, capacity));
  } else {
    blockDataCleanup(pVirtualScanInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pVirtualScanInfo->pIntermediateBlock;
  if (pOperator->pOperatorGetParam) {
    VTS_ERR_RET(doGetVStableMergedBlockData(pInfo, pHandle, capacity, p));
  } else {
    VTS_ERR_RET(doGetVtableMergedBlockData(pInfo, pHandle, capacity, p));
  }

  VTS_ERR_RET(copyDataBlock(pDataBlock, p));
  qDebug("%s %s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64, GET_TASKID(pTaskInfo), __func__,
         pDataBlock->info.id.groupId, pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

static bool isRefTagOutputSlot(const SVirtualTableScanInfo* pInfo, int32_t dstSlotId) {
  if (pInfo == NULL || pInfo->pTargets == NULL || pInfo->pRefTagCols == NULL) {
    return false;
  }

  int32_t slotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    if (slotId == dstSlotId) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        return false;
      }

      SColumnNode* pCol = (SColumnNode*)pNode;
      if (pCol->colType != COLUMN_TYPE_TAG) {
        return false;
      }

      SNode* pRefNode = NULL;
      FOREACH(pRefNode, pInfo->pRefTagCols) {
        if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
          continue;
        }

        SColumnNode* pRefCol = (SColumnNode*)pRefNode;
        if (0 == strcmp(pRefCol->colName, pCol->colName)) {
          return true;
        }
      }
      return false;
    }

    ++slotId;
  }

  return false;
}

static SColumnNode* findRefTagOutputColumn(const SVirtualTableScanInfo* pInfo, const SColumnNode* pTargetCol) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL || pTargetCol == NULL) {
    return NULL;
  }

  SNode* pRefNode = NULL;
  FOREACH(pRefNode, pInfo->pRefTagCols) {
    if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pRefCol = (SColumnNode*)pRefNode;
    if (0 == strcmp(pRefCol->colName, pTargetCol->colName)) {
      return pRefCol;
    }
  }

  return NULL;
}

static void normalizeRefTagTargets(SVirtualTableScanInfo* pInfo) {
  if (pInfo == NULL || pInfo->pTargets == NULL || pInfo->pRefTagCols == NULL) {
    return;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pTargetCol = (SColumnNode*)pNode;
    SColumnNode* pRefCol = findRefTagOutputColumn(pInfo, pTargetCol);
    if (pRefCol == NULL || pTargetCol->hasRef) {
      continue;
    }

    pTargetCol->hasRef = true;
    pTargetCol->colType = pRefCol->colType;
    pTargetCol->dataBlockId = pRefCol->dataBlockId;
    pTargetCol->slotId = pRefCol->slotId;
    pTargetCol->colId = pRefCol->colId;
    tstrncpy(pTargetCol->refDbName, pRefCol->refDbName, sizeof(pTargetCol->refDbName));
    tstrncpy(pTargetCol->refTableName, pRefCol->refTableName, sizeof(pTargetCol->refTableName));
    tstrncpy(pTargetCol->refColName, pRefCol->refColName, sizeof(pTargetCol->refColName));
  }
}

int32_t vtableAddTagPseudoColumnData(const SOperatorInfo* pOperator, const SExprInfo* pExpr, int32_t numOfExpr,
                                     SSDataBlock* tagBlock, SSDataBlock* pBlock, int32_t rows,
                                     const SVirtualTableScanInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t backupRows;
  const char* tbName = NULL;
  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator != NULL && pOperator->pOperatorGetParam != NULL) {
    SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
    if (pParam != NULL && pParam->tbName[0] != '\0') {
      tbName = pParam->tbName;
    }
  }
  if (tbName == NULL && pInfo != NULL && pInfo->vtableName[0] != '\0') {
    tbName = pInfo->vtableName;
  }

  backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    int32_t functionId = pExpr1->pExpr->_function.functionId;
    if (!fmIsScanPseudoColumnFunc(functionId)) {
      continue;
    }

    if (isRefTagOutputSlot(pInfo, dstSlotId)) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    TSDB_CHECK_NULL(pColInfoData, code, lino, _return, terrno)
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    int32_t functionType = pExpr1->pExpr->_function.functionType;
    if (fmIsScanPseudoColumnFunc(functionId) && functionType == FUNCTION_TYPE_TBNAME && tbName != NULL) {
      char tbNameBuf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(tbNameBuf, tbName);
      code = colDataSetNItems(pColInfoData, 0, tbNameBuf, pBlock->info.rows, 1, true);
      QUERY_CHECK_CODE(code, lino, _return);
    } else {
      colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;

_return:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetLocalTagPseudoColumnData(SOperatorInfo* pOperator, const SVirtualTableScanInfo* pInfo,
                                             SSDataBlock* pTagBlock, SSDataBlock* pBlock, int32_t rows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfTagCols = pTagBlock == NULL ? 0 : taosArrayGetSize(pTagBlock->pDataBlock);

  if (pInfo == NULL || pInfo->pScanPseudoCols == NULL) {
    return code;
  }

  int32_t srcSlotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pInfo->pScanPseudoCols) {
    SNode*  pExpr = pNode;
    int16_t dstSlotId = srcSlotId;
    if (nodeType(pNode) == QUERY_NODE_TARGET) {
      STargetNode* pTarget = (STargetNode*)pNode;
      pExpr = pTarget->pExpr;
      dstSlotId = pTarget->slotId;
    }

    if (pExpr != NULL && nodeType(pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pExpr;
      if (pColNode->colType == COLUMN_TYPE_TAG && !pColNode->hasRef) {
        SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
        SColumnInfoData* pSrcCol = NULL;
        const STagVal*   pResolvedTagVal = findResolvedTagVal(pOperator, pColNode->colId);
        TSDB_CHECK_NULL(pDstCol, code, lino, _return, terrno)

        if (pTagBlock != NULL && srcSlotId >= 0 && srcSlotId < numOfTagCols) {
          pSrcCol = taosArrayGet(pTagBlock->pDataBlock, srcSlotId);
        }

        code = setTagColumnValue(pDstCol, pSrcCol, pResolvedTagVal, rows);
        QUERY_CHECK_CODE(code, lino, _return);
      }
    }

    ++srcSlotId;
  }

_return:
  return code;
}

static int32_t doSetTagColumnData(SOperatorInfo* pOperator, SVirtualTableScanInfo* pInfo, SSDataBlock* pTagBlock,
                                  SSDataBlock* pBlock, int32_t rows) {
  int32_t         code = 0;
  int32_t         lino = 0;
  STableScanBase* pTableScanInfo = &pInfo->base;
  SExprSupp*      pSup = &pTableScanInfo->pseudoSup;
  int64_t         backupRows = pBlock->info.rows;

  pBlock->info.rows = rows;
  VTS_ERR_RET(doSetLocalTagPseudoColumnData(pOperator, pInfo, pTagBlock, pBlock, pBlock->info.rows));
  if (pInfo->pTargets != NULL) {
    int32_t dstSlotId = 0;
    SNode*  pNode = NULL;
    FOREACH(pNode, pInfo->pTargets) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        ++dstSlotId;
        continue;
      }

      SColumnNode* pColNode = (SColumnNode*)pNode;
      SColumnNode* pMatchedRefCol = findRefTagOutputColumn(pInfo, pColNode);
      bool         isRefTagOutput = (pMatchedRefCol != NULL);
      if (pColNode->colType != COLUMN_TYPE_TAG || !isRefTagOutput) {
        ++dstSlotId;
        continue;
      }

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      SColumnInfoData* pSrcCol = NULL;
      const STagVal*   pResolvedTagVal = findResolvedTagVal(pOperator, pColNode->colId);

      if (pResolvedTagVal == NULL && pInfo->pRefTagCols != NULL) {
        SNode*  pRefNode = NULL;
        int32_t refIdx = 0;
        FOREACH(pRefNode, pInfo->pRefTagCols) {
          if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
            ++refIdx;
            continue;
          }

          SColumnNode* pRefCol = (SColumnNode*)pRefNode;
          if (0 != strcmp(pRefCol->colName, pColNode->colName)) {
            ++refIdx;
            continue;
          }

          STagRefSavedBlock* pSaved = findSavedTagRefBlock(pInfo, pRefCol->dataBlockId);
          if (pSaved == NULL && pInfo->pSavedTagRefBlocks != NULL && refIdx < taosArrayGetSize(pInfo->pSavedTagRefBlocks)) {
            pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, refIdx);
          }
          if (pSaved != NULL && pSaved->pBlock != NULL) {
            pSrcCol = taosArrayGet(pSaved->pBlock->pDataBlock, pRefCol->slotId);
            if (pSrcCol != NULL) {
              break;
            }
          }
          ++refIdx;
        }
      }
      TSDB_CHECK_NULL(pDstCol, code, lino, _return, terrno)
      code = setTagColumnValue(pDstCol, pSrcCol, pResolvedTagVal, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _return);

      ++dstSlotId;

    }
  }

  if (pSup->numOfExprs > 0) {
    VTS_ERR_RET(vtableAddTagPseudoColumnData(pOperator, pSup->pExprInfo, pSup->numOfExprs, pTagBlock, pBlock, rows, pInfo));
  }

_return:
  pBlock->info.rows = backupRows;
  return code;
}

static bool isSingleRowTagBlockAllNull(SSDataBlock* pTagBlock) {
  if (pTagBlock == NULL || pTagBlock->info.rows != 1) {
    return false;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pTagBlock->pDataBlock); ++i) {
    SColumnInfoData* pCol = taosArrayGet(pTagBlock->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }
    if (!colDataIsNull_s(pCol, 0)) {
      char* data = colDataGetData(pCol, 0);
      if (!IS_JSON_NULL(pCol->info.type, data)) {
        return false;
      }
    }
  }
  return true;
}

int32_t virtualTableGetNext(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    *pResBlock = NULL;
    return code;
  }

  if (taosArrayGetSize(pInfo->binfo.pRes->pDataBlock) == 0) {
    // scan no cols
    *pResBlock = NULL;
    return code;
  }

  VTS_ERR_JRET(pOperator->fpSet._openFn(pOperator));

  if (pVirtualScanInfo->pTagRefSourceBlockIds != NULL && pVirtualScanInfo->pSavedTagRefBlocks == NULL) {
    pVirtualScanInfo->pSavedTagRefBlocks =
        taosArrayInit(taosArrayGetSize(pVirtualScanInfo->pTagRefSourceBlockIds), sizeof(STagRefSavedBlock));
    QUERY_CHECK_NULL(pVirtualScanInfo->pSavedTagRefBlocks, code, lino, _return, terrno)

    for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pTagRefSourceBlockIds); ++i) {
      int64_t* pBlockId = taosArrayGet(pVirtualScanInfo->pTagRefSourceBlockIds, i);
      QUERY_CHECK_NULL(pBlockId, code, lino, _return, terrno)
      STagRefDownstreamInfo* pDownstreamInfo =
          tSimpleHashGet(pVirtualScanInfo->pTagRefDownstreamMap, pBlockId, sizeof(*pBlockId));
      QUERY_CHECK_NULL(pDownstreamInfo, code, lino, _return, TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR)

      SSDataBlock*   pTagRefBlock = NULL;
      SSDataBlock*   pSavedBlock = NULL;
      SOperatorInfo* pTagRefOp = pOperator->pDownstream[pDownstreamInfo->downstreamId];
      VTS_ERR_JRET(pTagRefOp->fpSet.getNextFn(pTagRefOp, &pTagRefBlock));
      QUERY_CHECK_NULL(pTagRefBlock, code, lino, _return, TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR)
      if (pTagRefBlock->info.rows != 1) {
        VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
      }
      VTS_ERR_JRET(createOneDataBlock(pTagRefBlock, true, &pSavedBlock));

      STagRefSavedBlock savedBlock = {.blockId = *pBlockId, .pBlock = pSavedBlock};
      QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSavedTagRefBlocks, &savedBlock), code, lino, _return, terrno)
    }
  }

  // Dynamic VSTB keeps the original table-scan path unchanged; when metadata is projected,
  // fetch the real 1-row tag block from the extra tag-scan child and use it only for fill.
  SVTableScanOperatorParam* pDynParam =
      pOperator->pOperatorGetParam ? (SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value : NULL;
  bool needTagBlock = (pVirtualScanInfo->tagBlockId != -1) || (pDynParam != NULL && pDynParam->pTagScanOp != NULL) ||
                      (pDynParam == NULL && pVirtualScanInfo->tagDownStreamId != -1 &&
                       pVirtualScanInfo->pScanPseudoCols != NULL && LIST_LENGTH(pVirtualScanInfo->pScanPseudoCols) > 0);
  if (needTagBlock && pVirtualScanInfo->tagDownStreamId != -1 && !pInfo->pSavedTagBlock) {
      SSDataBlock*   pTagBlock = NULL;
      SOperatorInfo* pTagScanOp = pOperator->pDownstream[pVirtualScanInfo->tagDownStreamId];
      if (pDynParam != NULL) {
        SOperatorParam* pTagOp = pDynParam->pTagScanOp;
        if (pTagOp != NULL) {
          VTS_ERR_JRET(pTagScanOp->fpSet.getNextExtFn(pTagScanOp, pTagOp, &pTagBlock));
        }
      } else {
        VTS_ERR_JRET(pTagScanOp->fpSet.getNextFn(pTagScanOp, &pTagBlock));
      }

      if (pTagBlock == NULL) {
        // dynamic vtable scan may skip dedicated tag-scan param; fallback tag list will be applied later.
      } else if (pTagBlock->info.rows != 1) {
      VTS_ERR_JRET(TSDB_CODE_FAILED);
    } else {
      VTS_ERR_JRET(createOneDataBlock(pTagBlock, true, &pInfo->pSavedTagBlock));
    }
  }

  while (1) {
    VTS_ERR_JRET(doVirtualTableMerge(pOperator, pResBlock));
    if (*pResBlock == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }

    if (pOperator->pOperatorGetParam) {
      uint64_t uid = ((SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value)->uid;
      (*pResBlock)->info.id.uid = uid;
      qTrace("vtable scan uid:%" PRIu64, (*pResBlock)->info.id.uid);
    } else {
      (*pResBlock)->info.id.uid = pInfo->virtualScanInfo.vtableUid;
      qTrace("vtable scan vtb uid:%" PRIu64, (*pResBlock)->info.id.uid);
    }

    // Dynamic vtable scans may need local metadata fill (resolved tags/tbname) even without a saved tag block.
    bool tagAllNull = isSingleRowTagBlockAllNull(pInfo->pSavedTagBlock);
    bool needMetadataFill =
        (pInfo->pSavedTagBlock != NULL && !tagAllNull) ||
        pVirtualScanInfo->pSavedTagRefBlocks != NULL ||
        pOperator->pOperatorGetParam != NULL;
    if (needMetadataFill) {
      VTS_ERR_JRET(doSetTagColumnData(pOperator, pVirtualScanInfo, pInfo->pSavedTagBlock, (*pResBlock),
                                      (*pResBlock)->info.rows));
    }
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      const char* filterTbName = NULL;
      if (pOperator->pOperatorGetParam != NULL) {
        SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
        if (pParam != NULL && pParam->tbName[0] != '\0') {
          filterTbName = pParam->tbName;
        }
      }
      if (filterTbName == NULL && pVirtualScanInfo->vtableName[0] != '\0') {
        filterTbName = pVirtualScanInfo->vtableName;
      }
      filterInfoSetTableCtx(pOperator->exprSupp.pFilterInfo, (void*)filterTbName);
    }
    VTS_ERR_JRET(doFilter(*pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL));
    if ((*pResBlock)->info.rows > 0) {
      break;
    }
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  pTaskInfo->code = code;
  T_LONG_JMP(pTaskInfo->env, code);
}

static void destroyTableScanBase(STableScanBase* pBase, TsdReader* pAPI) {
  cleanupQueryTableDataCond(&pBase->cond);

  if (pAPI->tsdReaderClose) {
    pAPI->tsdReaderClose(pBase->dataReader);
  }
  pBase->dataReader = NULL;

  if (pBase->matchInfo.pList != NULL) {
    taosArrayDestroy(pBase->matchInfo.pList);
  }

  taosLRUCacheCleanup(pBase->metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pBase->pseudoSup);
}

void destroyVirtualTableScanOperatorInfo(void* param) {
  if (!param) {
    return;
  }
  SVirtualScanMergeOperatorInfo* pOperatorInfo = (SVirtualScanMergeOperatorInfo*)param;
  SVirtualTableScanInfo*         pInfo = &pOperatorInfo->virtualScanInfo;
  blockDataDestroy(pOperatorInfo->binfo.pRes);
  pOperatorInfo->binfo.pRes = NULL;

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  taosArrayDestroy(pInfo->pSortInfo);
  pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = NULL;
  destroyTableScanBase(&pInfo->base, &pInfo->base.readerAPI);

  tSimpleHashCleanup(pInfo->dataSlotMap);
  cleanupRefSlotGroups(pInfo);
  cleanupSavedTagRefBlocks(pInfo);
  tSimpleHashCleanup(pInfo->pTagRefDownstreamMap);
  pInfo->pTagRefDownstreamMap = NULL;
  taosArrayDestroy(pInfo->pTagRefSourceBlockIds);
  pInfo->pTagRefSourceBlockIds = NULL;

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }
  taosMemoryFreeClear(param);
}

int32_t extractColMap(SNodeList* pNodeList, SSHashObj** pSlotMap, int32_t* tsSlotId, int64_t* tagBlockId,
                      bool* useOriginTs) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (numOfCols == 0) {
    return code;
  }

  *tsSlotId = -1;
  *tagBlockId = -1;
  *useOriginTs = false;
  *pSlotMap = tSimpleHashInit(numOfCols, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  TSDB_CHECK_NULL(*pSlotMap, code, lino, _return, terrno);

  int32_t i = 0;
  SNode*  node = NULL;
  FOREACH(node, pNodeList) {
    SColumnNode* pColNode = (SColumnNode*)node;
    TSDB_CHECK_NULL(pColNode, code, lino, _return, terrno)

    if (pColNode->isPrimTs || pColNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      *tsSlotId = i;
    } else if (pColNode->colType != COLUMN_TYPE_COLUMN && !pColNode->hasRef) {
      // Non-data, non-ref column (e.g., tbname pseudo column or local tag).
      // colType may be COLUMN_TYPE_TAG, COLUMN_TYPE_TBNAME, or 0 (unset for pseudo cols).
      *tagBlockId = pColNode->dataBlockId;
    } else if (pColNode->hasRef) {
      int64_t slotKey = pColNode->dataBlockId << 16 | pColNode->slotId;
      if (pColNode->slotId == 0) {
        *useOriginTs = true;
      }
      VTS_ERR_JRET(tSimpleHashPut(*pSlotMap, &slotKey, sizeof(slotKey), &i, sizeof(i)));
    }
    ++i;
  }

  return code;
_return:
  tSimpleHashCleanup(*pSlotMap);
  *pSlotMap = NULL;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t resetVirtualTableMergeOperState(SOperatorInfo* pOper) {
  int32_t                        code = 0, lino = 0;
  SVirtualScanMergeOperatorInfo* pMergeInfo = pOper->info;
  SVirtualScanPhysiNode*         pPhynode = (SVirtualScanPhysiNode*)pOper->pPhyNode;
  SVirtualTableScanInfo*         pInfo = &pMergeInfo->virtualScanInfo;

  pOper->status = OP_NOT_OPENED;
  resetBasicOperatorState(&pMergeInfo->binfo);

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  // taosArrayDestroy(pInfo->pSortInfo);
  // pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = createDataBlockFromDescNode(((SPhysiNode*)pPhynode)->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInfo->pInputBlock, code, lino, _exit, terrno)

  pInfo->tagDownStreamId = -1;

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      blockDataDestroy(pCtx->pIntermediateBlock);
      taosMemoryFree(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }

  pMergeInfo->pSavedTuple = NULL;
  pMergeInfo->pSavedTagBlock = NULL;
  cleanupSavedTagRefBlocks(pInfo);
  tSimpleHashCleanup(pInfo->pTagRefDownstreamMap);
  pInfo->pTagRefDownstreamMap = NULL;
  taosArrayDestroy(pInfo->pTagRefSourceBlockIds);
  pInfo->pTagRefSourceBlockIds = NULL;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t createVirtualTableMergeOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                            SVirtualScanPhysiNode* pVirtualScanPhyNode, SExecTaskInfo* pTaskInfo,
                                            SReadHandle* pHandle, SOperatorInfo** pOptrInfo) {
  SPhysiNode*                    pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
  int32_t                        lino = 0;
  int32_t                        code = TSDB_CODE_SUCCESS;
  SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*            pDescNode = pPhyNode->pOutputDataBlockDesc;
  SNodeList*                     pMergeKeys = NULL;

  QUERY_CHECK_NULL(pInfo, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pOperator, code, lino, _return, terrno)
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pVirtualScanPhyNode;

  pInfo->binfo.inputTsOrder = pVirtualScanPhyNode->scan.node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pVirtualScanPhyNode->scan.node.outputTsOrder;

  SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;
  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _return, terrno)

  SSDataBlock* pInputBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInputBlock, code, lino, _return, terrno)
  pVirtualScanInfo->pInputBlock = pInputBlock;
  pVirtualScanInfo->tagDownStreamId = -1;
  pVirtualScanInfo->vtableUid = (tb_uid_t)pVirtualScanPhyNode->scan.uid;
  tstrncpy(pVirtualScanInfo->vtableName, pVirtualScanPhyNode->scan.tableName.tname,
           sizeof(pVirtualScanInfo->vtableName));
  pVirtualScanInfo->pTargets = pVirtualScanPhyNode->pTargets;
  pVirtualScanInfo->pScanPseudoCols = pVirtualScanPhyNode->scan.pScanPseudoCols;
  pVirtualScanInfo->pRefTagCols = pVirtualScanPhyNode->pRefTagCols;
  normalizeRefTagTargets(pVirtualScanInfo);
  if (pHandle != NULL) {
    pVirtualScanInfo->base.readHandle = *pHandle;
  }
  if (pVirtualScanPhyNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pVirtualScanInfo->base.pseudoSup;
    pSup->pExprInfo = NULL;
    VTS_ERR_JRET(createExprInfo(pVirtualScanPhyNode->scan.pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs));

    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                      &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_NULL(pSup->pCtx, code, lino, _return, terrno)
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  TSDB_CHECK_CODE(blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity), lino, _return);

  size_t  numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;

  pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pVirtualScanInfo->sortBufSize = (uint64_t)pVirtualScanInfo->bufPageSize *
                                  (uint64_t)(numOfDownstream + 1);  // one additional is reserved for merged result.
  VTS_ERR_JRET(extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId,
                             &pVirtualScanInfo->tagBlockId, &pVirtualScanInfo->useOrgTsCol));

  if (!pVirtualScanPhyNode->scan.node.dynamicOp) {
    VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, pVirtualScanInfo->tsSlotId >= 0 ? pVirtualScanInfo->tsSlotId : 0));
    pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys);
    TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  } else {
    pTaskInfo->dynamicTask = true;
  }

  pVirtualScanInfo->scanAllCols = pVirtualScanPhyNode->scanAllCols;

  VTS_ERR_JRET(filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo,
                                  0, pTaskInfo->pStreamRuntimeInfo));

  if (tsMetaEntryCache) {
    pVirtualScanInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(tsMetaEntryCacheSize, -1, .5);
    QUERY_CHECK_NULL(pVirtualScanInfo->base.metaCache.pTableMetaEntryCache, code, lino, _return, terrno);
  }

  setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(openVirtualTableScanOperator, virtualTableGetNext, NULL, destroyVirtualTableScanOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetVirtualTableMergeOperState);

  if (NULL != pDownstream) {
    VTS_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));
  } else {
    pVirtualScanInfo->tagDownStreamId = -1;
  }

  nodesDestroyList(pMergeKeys);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) {
    destroyVirtualTableScanOperatorInfo(pInfo);
  }
  nodesDestroyList(pMergeKeys);
  pTaskInfo->code = code;
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  return code;
}
