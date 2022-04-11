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

#include "planInt.h"

#include "functionMgt.h"

typedef struct SSlotIdInfo {
  int16_t slotId;
  bool set;
} SSlotIdInfo;

typedef struct SSlotIndex {
  int16_t dataBlockId;
  SArray* pSlotIdsInfo; // duplicate name slot
} SSlotIndex;

typedef struct SPhysiPlanContext {
  SPlanContext* pPlanCxt;
  int32_t errCode;
  int16_t nextDataBlockId;
  SArray* pLocationHelper;
  SArray* pExecNodeList;
} SPhysiPlanContext;

static int32_t getSlotKey(SNode* pNode, const char* pStmtName, char* pKey) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (NULL != pStmtName) {
      return sprintf(pKey, "%s.%s", pStmtName, pCol->node.aliasName);
    }
    if ('\0' == pCol->tableAlias[0]) {
      return sprintf(pKey, "%s", pCol->colName);
    }
    return sprintf(pKey, "%s.%s", pCol->tableAlias, pCol->colName);
  }

  if (NULL != pStmtName) {
    return sprintf(pKey, "%s.%s", pStmtName, ((SExprNode*)pNode)->aliasName);
  }
  return sprintf(pKey, "%s", ((SExprNode*)pNode)->aliasName);
}

static SNode* createSlotDesc(SPhysiPlanContext* pCxt, const SNode* pNode, int16_t slotId, bool output) {
  SSlotDescNode* pSlot = (SSlotDescNode*)nodesMakeNode(QUERY_NODE_SLOT_DESC);
  if (NULL == pSlot) {
    return NULL;
  }
  pSlot->slotId = slotId;
  pSlot->dataType = ((SExprNode*)pNode)->resType;
  pSlot->reserve = false;
  pSlot->output = output;
  return (SNode*)pSlot;
}

static int32_t createTarget(SNode* pNode, int16_t dataBlockId, int16_t slotId, SNode** pOutput) {
  STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
  if (NULL == pTarget) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTarget->dataBlockId = dataBlockId;
  pTarget->slotId = slotId;
  pTarget->pExpr = pNode;

  *pOutput = (SNode*)pTarget;
  return TSDB_CODE_SUCCESS;
}

static int32_t putSlotToHashImpl(int16_t dataBlockId, int16_t slotId, const char* pName, int32_t len, SHashObj* pHash) {
  SSlotIndex* pIndex = taosHashGet(pHash, pName, len);
  if (NULL != pIndex) {
    SSlotIdInfo info = { .slotId = slotId, .set = false };
    taosArrayPush(pIndex->pSlotIdsInfo, &info);
    return TSDB_CODE_SUCCESS;
  }

  SSlotIndex index = { .dataBlockId = dataBlockId, .pSlotIdsInfo = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SSlotIdInfo)) };
  if (NULL == index.pSlotIdsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SSlotIdInfo info = { .slotId = slotId, .set = false };
  taosArrayPush(index.pSlotIdsInfo, &info);
  return taosHashPut(pHash, pName, len, &index, sizeof(SSlotIndex));
}

static int32_t putSlotToHash(int16_t dataBlockId, int16_t slotId, SNode* pNode, SHashObj* pHash) {
  char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = getSlotKey(pNode, NULL, name);
  return putSlotToHashImpl(dataBlockId, slotId, name, len, pHash);
}

static int32_t createDataBlockDescHash(SPhysiPlanContext* pCxt, int32_t capacity, int16_t dataBlockId, SHashObj** pDescHash) {
  SHashObj* pHash = taosHashInit(capacity, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (NULL == taosArrayInsert(pCxt->pLocationHelper, dataBlockId, &pHash)) {
    taosHashCleanup(pHash);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pDescHash = pHash;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc, SHashObj* pHash) {
  pDataBlockDesc->pSlots = nodesMakeList();
  if (NULL == pDataBlockDesc->pSlots) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int16_t slotId = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    code = nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, pNode, slotId, true));
    if (TSDB_CODE_SUCCESS == code) {
      code = putSlotToHash(pDataBlockDesc->dataBlockId, slotId, pNode, pHash);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pDataBlockDesc->totalRowSize += ((SExprNode*)pNode)->resType.bytes;
      pDataBlockDesc->outputRowSize += ((SExprNode*)pNode)->resType.bytes;
      ++slotId;
    } else {
      break;
    }
  }
  return code;
}

static int32_t createDataBlockDesc(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode** pDataBlockDesc) {
  SDataBlockDescNode* pDesc = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC);
  if (NULL == pDesc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pDesc->dataBlockId = pCxt->nextDataBlockId++;

  SHashObj* pHash = NULL;
  int32_t code = createDataBlockDescHash(pCxt, LIST_LENGTH(pList), pDesc->dataBlockId, &pHash);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildDataBlockSlots(pCxt, pList, pDesc, pHash);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pDataBlockDesc = pDesc;
  } else {
    nodesDestroyNode(pDesc);
  }

  return code;
}

static int16_t getUnsetSlotId(const SArray* pSlotIdsInfo) {
  int32_t size = taosArrayGetSize(pSlotIdsInfo);
  for (int32_t i = 0; i < size; ++i) {
    SSlotIdInfo* pInfo = taosArrayGet(pSlotIdsInfo, i);
    if (!pInfo->set) {
      pInfo->set = true;
      return pInfo->slotId;
    }
  }
  return ((SSlotIdInfo*)taosArrayGet(pSlotIdsInfo, 0))->slotId;
}

static int32_t addDataBlockSlotsImpl(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc, const char* pStmtName, bool output) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  int16_t nextSlotId = taosHashGetSize(pHash), slotId = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode* pExpr = QUERY_NODE_ORDER_BY_EXPR == nodeType(pNode) ? ((SOrderByExprNode*)pNode)->pExpr : pNode;
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN] = {0};
    int32_t len = getSlotKey(pExpr, pStmtName, name);
    SSlotIndex* pIndex = taosHashGet(pHash, name, len);
    if (NULL == pIndex) {
      code = nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, pExpr, nextSlotId, output));
      if (TSDB_CODE_SUCCESS == code) {
        code = putSlotToHashImpl(pDataBlockDesc->dataBlockId, nextSlotId, name, len, pHash);
      }
      pDataBlockDesc->totalRowSize += ((SExprNode*)pExpr)->resType.bytes;
      if (output) {
        pDataBlockDesc->outputRowSize += ((SExprNode*)pExpr)->resType.bytes;
      }
      slotId = nextSlotId;
      ++nextSlotId;
    } else {
      slotId = getUnsetSlotId(pIndex->pSlotIdsInfo);
    }

    if (TSDB_CODE_SUCCESS == code) {
      SNode* pTarget = NULL;
      code = createTarget(pNode, pDataBlockDesc->dataBlockId, slotId, &pTarget);
      if (TSDB_CODE_SUCCESS == code) {
        REPLACE_NODE(pTarget);
      }
    }
  
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t addDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, false);
}

static int32_t addDataBlockSlot(SPhysiPlanContext* pCxt, SNode** pNode, SDataBlockDescNode* pDataBlockDesc) {
  if (NULL == pNode || NULL == *pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pList = NULL;
  int32_t code = nodesListMakeAppend(&pList, *pNode);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pList, pDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pNode = nodesListGetNode(pList, 0);
  }
  nodesClearList(pList);
  return code;
}

static int32_t addDataBlockSlotsForProject(SPhysiPlanContext* pCxt, const char* pStmtName, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, pStmtName, true);
}

static int32_t pushdownDataBlockSlots(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  return addDataBlockSlotsImpl(pCxt, pList, pDataBlockDesc, NULL, true);
}

typedef struct SSetSlotIdCxt {
  int32_t errCode;
  SHashObj* pLeftHash;
  SHashObj* pRightHash;
} SSetSlotIdCxt;

static EDealRes doSetSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SSetSlotIdCxt* pCxt = (SSetSlotIdCxt*)pContext;
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
    int32_t len = getSlotKey(pNode, NULL, name);
    SSlotIndex* pIndex = taosHashGet(pCxt->pLeftHash, name, len);
    if (NULL == pIndex) {
      pIndex = taosHashGet(pCxt->pRightHash, name, len);
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    if (NULL == pIndex) {
      pCxt->errCode = TSDB_CODE_PLAN_INTERNAL_ERROR;
      return DEAL_RES_ERROR;
    }
    ((SColumnNode*)pNode)->dataBlockId = pIndex->dataBlockId;
    ((SColumnNode*)pNode)->slotId = ((SSlotIdInfo*)taosArrayGet(pIndex->pSlotIdsInfo, 0))->slotId;
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t setNodeSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNode* pNode, SNode** pOutput) {
  SNode* pRes = nodesCloneNode(pNode);
  if (NULL == pRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSetSlotIdCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
    .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId))
  };
  nodesWalkExpr(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(pRes);
    return cxt.errCode;
  }

  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t setListSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, const SNodeList* pList, SNodeList** pOutput) {
  SNodeList* pRes = nodesCloneList(pList);
  if (NULL == pRes) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSetSlotIdCxt cxt = {
    .errCode = TSDB_CODE_SUCCESS,
    .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
    .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId))
  };
  nodesWalkExprs(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return cxt.errCode;
  }
  *pOutput = pRes;
  return TSDB_CODE_SUCCESS;
}

static uint8_t getPrecision(SNodeList* pChildren) {
  if (1 == LIST_LENGTH(pChildren)) {
    return (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc)->precision;
  } else if (2 == LIST_LENGTH(pChildren)) {
    uint8_t lp = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc)->precision;
    uint8_t rp = (((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc)->precision;
    return (lp > rp ? rp : lp);
  }
  return 0;
}

static SPhysiNode* makePhysiNode(SPhysiPlanContext* pCxt, uint8_t precision, SLogicNode* pLogicNode, ENodeType type) {
  SPhysiNode* pPhysiNode = (SPhysiNode*)nodesMakeNode(type);
  if (NULL == pPhysiNode) {
    return NULL;
  }

  int32_t code = createDataBlockDesc(pCxt, pLogicNode->pTargets, &pPhysiNode->pOutputDataBlockDesc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pPhysiNode);
    return NULL;
  }
  pPhysiNode->pOutputDataBlockDesc->precision = precision;
  return pPhysiNode;
}

static int32_t setConditionsSlotId(SPhysiPlanContext* pCxt, const SLogicNode* pLogicNode, SPhysiNode* pPhysiNode) {
  if (NULL != pLogicNode->pConditions) {
    return setNodeSlotId(pCxt, pPhysiNode->pOutputDataBlockDesc->dataBlockId, -1, pLogicNode->pConditions, &pPhysiNode->pConditions);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t colIdCompare(const void* pLeft, const void* pRight) {
  SColumnNode* pLeftCol = *(SColumnNode**)pLeft;
  SColumnNode* pRightCol = *(SColumnNode**)pRight;
  return pLeftCol->colId > pRightCol->colId ? 1 : -1;
}

static int32_t sortScanCols(SNodeList* pScanCols) {
  SArray* pArray = taosArrayInit(LIST_LENGTH(pScanCols), POINTER_BYTES);
  if (NULL == pArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pCol = NULL;
  FOREACH(pCol, pScanCols) {
    taosArrayPush(pArray, &pCol);
  }
  taosArraySort(pArray, colIdCompare);

  int32_t index = 0;
  FOREACH(pCol, pScanCols) {
    REPLACE_NODE(taosArrayGetP(pArray, index++));
  }
  taosArrayDestroy(pArray);

  return TSDB_CODE_SUCCESS;
}

static int32_t createScanCols(SPhysiPlanContext* pCxt, SScanPhysiNode* pScanPhysiNode, SNodeList* pScanCols) {
  pScanPhysiNode->pScanCols = nodesCloneList(pScanCols);
  if (NULL == pScanPhysiNode->pScanCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return sortScanCols(pScanPhysiNode->pScanCols);
}

static int32_t createScanPhysiNodeFinalize(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode, SScanPhysiNode* pScanPhysiNode, SPhysiNode** pPhyNode) {
  int32_t code = createScanCols(pCxt, pScanPhysiNode, pScanLogicNode->pScanCols);
  if (TSDB_CODE_SUCCESS == code) {
    // Data block describe also needs to be set without scanning column, such as SELECT COUNT(*) FROM t
    code = addDataBlockSlots(pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pScanPhysiNode->uid = pScanLogicNode->pMeta->uid;
    pScanPhysiNode->tableType = pScanLogicNode->pMeta->tableType;
    pScanPhysiNode->order = TSDB_ORDER_ASC;
    pScanPhysiNode->count = 1;
    pScanPhysiNode->reverse = 0;
    memcpy(&pScanPhysiNode->tableName, &pScanLogicNode->tableName, sizeof(SName));
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pScanPhysiNode;
  } else {
    nodesDestroyNode(pScanPhysiNode);
  }

  return code;
}

static void vgroupInfoToNodeAddr(const SVgroupInfo* vg, SQueryNodeAddr* pNodeAddr) {
  pNodeAddr->nodeId = vg->vgId;
  pNodeAddr->epSet  = vg->epSet;
}

static int32_t createTagScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  STagScanPhysiNode* pTagScan = (STagScanPhysiNode*)makePhysiNode(pCxt, pScanLogicNode->pMeta->tableInfo.precision, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN);
  if (NULL == pTagScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return createScanPhysiNodeFinalize(pCxt, pScanLogicNode, (SScanPhysiNode*)pTagScan, pPhyNode);
}

static int32_t createTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  STableScanPhysiNode* pTableScan = (STableScanPhysiNode*)makePhysiNode(pCxt, pScanLogicNode->pMeta->tableInfo.precision, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  if (NULL == pTableScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTableScan->scanFlag = pScanLogicNode->scanFlag;
  pTableScan->scanRange = pScanLogicNode->scanRange;
  pTableScan->ratio = pScanLogicNode->ratio;
  vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
  pSubplan->execNodeStat.tableNum = pScanLogicNode->pVgroupList->vgroups[0].numOfTable;
  tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);

  return createScanPhysiNodeFinalize(pCxt, pScanLogicNode, (SScanPhysiNode*)pTableScan, pPhyNode);
}

static int32_t createSystemTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  SSystemTableScanPhysiNode* pScan = (SSystemTableScanPhysiNode*)makePhysiNode(pCxt, pScanLogicNode->pMeta->tableInfo.precision, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN);
  if (NULL == pScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pScan->showRewrite = pScanLogicNode->showRewrite;
  pScan->accountId = pCxt->pPlanCxt->acctId;
  if (0 == strcmp(pScanLogicNode->tableName.tname, TSDB_INS_TABLE_USER_TABLES)) {
    vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
    taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
  } else {
    SQueryNodeAddr addr = { .nodeId = MNODE_HANDLE, .epSet = pCxt->pPlanCxt->mgmtEpSet };
    taosArrayPush(pCxt->pExecNodeList, &addr);
  }
  pScan->mgmtEpSet = pCxt->pPlanCxt->mgmtEpSet;
  tNameGetFullDbName(&pScanLogicNode->tableName, pSubplan->dbFName);

  return createScanPhysiNodeFinalize(pCxt, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
}

static int32_t createStreamScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  SStreamScanPhysiNode* pScan = (SStreamScanPhysiNode*)makePhysiNode(pCxt, pScanLogicNode->pMeta->tableInfo.precision, (SLogicNode*)pScanLogicNode, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  if (NULL == pScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return createScanPhysiNodeFinalize(pCxt, pScanLogicNode, (SScanPhysiNode*)pScan, pPhyNode);
}

static int32_t createScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode, SPhysiNode** pPhyNode) {
  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      return createTagScanPhysiNode(pCxt, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_TABLE:
      return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_SYSTEM_TABLE:
      return createSystemTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    case SCAN_TYPE_STREAM:
      return createStreamScanPhysiNode(pCxt, pSubplan, pScanLogicNode, pPhyNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static int32_t createColFromDataBlockDesc(SDataBlockDescNode* pDesc, SNodeList* pCols) {
  SNode* pNode;
  FOREACH(pNode, pDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pCol->node.resType = pSlot->dataType;
    pCol->dataBlockId = pDesc->dataBlockId;
    pCol->slotId = pSlot->slotId;
    pCol->colId = -1;
    int32_t code = nodesListStrictAppend(pCols, pCol);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t createJoinOutputCols(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc, SDataBlockDescNode* pRightDesc, SNodeList** pList) {
  SNodeList* pCols = nodesMakeList();
  if (NULL == pCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = createColFromDataBlockDesc(pLeftDesc, pCols);
  if (TSDB_CODE_SUCCESS == code) {
    code = createColFromDataBlockDesc(pRightDesc, pCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pList = pCols;
  } else {
    nodesDestroyList(pCols);
  }

  return code;
}

static int32_t createJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode, SPhysiNode** pPhyNode) {
  SJoinPhysiNode* pJoin = (SJoinPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pJoinLogicNode, QUERY_NODE_PHYSICAL_PLAN_JOIN);
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SDataBlockDescNode* pLeftDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SDataBlockDescNode* pRightDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc;

  int32_t code = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pOnConditions, &pJoin->pOnConditions);
  if (TSDB_CODE_SUCCESS == code) {
    code = createJoinOutputCols(pCxt, pLeftDesc, pRightDesc, &pJoin->pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pJoin;
  } else {
    nodesDestroyNode(pJoin);
  }

  return code;
}

typedef struct SRewritePrecalcExprsCxt {
  int32_t errCode;
  int32_t planNodeId;
  int32_t rewriteId;
  SNodeList* pPrecalcExprs;
} SRewritePrecalcExprsCxt;

static EDealRes collectAndRewrite(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SNode* pExpr = nodesCloneNode(*pNode);
  if (NULL == pExpr) {
    return DEAL_RES_ERROR;
  }
  if (nodesListAppend(pCxt->pPrecalcExprs, pExpr)) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SExprNode* pRewrittenExpr = (SExprNode*)pExpr;
  pCol->node.resType = pRewrittenExpr->resType;
  if ('\0' != pRewrittenExpr->aliasName[0]) {
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  } else {
    snprintf(pRewrittenExpr->aliasName, sizeof(pRewrittenExpr->aliasName), "#expr_%d_%d", pCxt->planNodeId, pCxt->rewriteId);
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  }
  nodesDestroyNode(*pNode);
  *pNode = (SNode*)pCol;
  return DEAL_RES_IGNORE_CHILD;
}

static EDealRes doRewritePrecalcExprs(SNode** pNode, void* pContext) {
  SRewritePrecalcExprsCxt* pCxt = (SRewritePrecalcExprsCxt*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION: {
      return collectAndRewrite(pContext, pNode);
    }
    case QUERY_NODE_FUNCTION: {
      if (fmIsScalarFunc(((SFunctionNode*)(*pNode))->funcId)) {
        return collectAndRewrite(pContext, pNode);
      }
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewritePrecalcExprs(SPhysiPlanContext* pCxt, SNodeList* pList, SNodeList** pPrecalcExprs, SNodeList** pRewrittenList) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == *pPrecalcExprs) {
    *pPrecalcExprs = nodesMakeList();
    if (NULL == *pPrecalcExprs) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (NULL == *pRewrittenList) {
    *pRewrittenList = nodesMakeList();
    if (NULL == *pRewrittenList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode* pNew = NULL;
    if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
      pNew = nodesCloneNode(nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0));
    } else {
      pNew = nodesCloneNode(pNode);
    }
    if (NULL == pNew) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (TSDB_CODE_SUCCESS != nodesListAppend(*pRewrittenList, pNew)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  SRewritePrecalcExprsCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pPrecalcExprs = *pPrecalcExprs };
  nodesRewriteExprs(*pRewrittenList, doRewritePrecalcExprs, &cxt);
  if (0 == LIST_LENGTH(cxt.pPrecalcExprs)) {
    nodesDestroyList(cxt.pPrecalcExprs);
    *pPrecalcExprs = NULL;
  }
  return cxt.errCode;
}

static int32_t rewritePrecalcExpr(SPhysiPlanContext* pCxt, SNode* pNode, SNodeList** pPrecalcExprs, SNode** pRewritten) {
  if (NULL == pNode) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pList = NULL;
  int32_t code = nodesListMakeAppend(&pList, pNode);
  SNodeList* pRewrittenList = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pList, pPrecalcExprs, &pRewrittenList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pRewritten = nodesListGetNode(pRewrittenList, 0);
  }
  nodesClearList(pList);
  nodesClearList(pRewrittenList);
  return code;
}

static int32_t createAggPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SAggLogicNode* pAggLogicNode, SPhysiNode** pPhyNode) {
  SAggPhysiNode* pAgg = (SAggPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pAggLogicNode, QUERY_NODE_PHYSICAL_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pGroupKeys = NULL;
  SNodeList* pAggFuncs = NULL;
  int32_t code = rewritePrecalcExprs(pCxt, pAggLogicNode->pGroupKeys, &pPrecalcExprs, &pGroupKeys);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewritePrecalcExprs(pCxt, pAggLogicNode->pAggFuncs, &pPrecalcExprs, &pAggFuncs);
  }

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pAgg->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = pushdownDataBlockSlots(pCxt, pAgg->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pGroupKeys) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pGroupKeys, &pAgg->pGroupKeys);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pAgg->pGroupKeys, pAgg->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pAggFuncs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pAggFuncs, &pAgg->pAggFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pAgg->pAggFuncs, pAgg->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pAggLogicNode, (SPhysiNode*)pAgg);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pAgg;
  } else {
    nodesDestroyNode(pAgg);
  }

  nodesDestroyList(pPrecalcExprs);
  nodesDestroyList(pGroupKeys);
  nodesDestroyList(pAggFuncs);

  return code;
}

static int32_t createProjectPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SProjectLogicNode* pProjectLogicNode, SPhysiNode** pPhyNode) {
  SProjectPhysiNode* pProject = (SProjectPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pProjectLogicNode, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pProject->limit = pProjectLogicNode->limit;
  pProject->offset = pProjectLogicNode->offset;
  pProject->slimit = pProjectLogicNode->slimit;
  pProject->soffset = pProjectLogicNode->soffset;

  int32_t code = setListSlotId(pCxt, ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc->dataBlockId, -1, pProjectLogicNode->pProjections, &pProject->pProjections);
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlotsForProject(pCxt, pProjectLogicNode->stmtName, pProject->pProjections, pProject->node.pOutputDataBlockDesc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setConditionsSlotId(pCxt, (const SLogicNode*)pProjectLogicNode, (SPhysiNode*)pProject);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pProject;
  } else {
    nodesDestroyNode(pProject);
  }

  return code;
}

static int32_t doCreateExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode, SPhysiNode** pPhyNode) {
  SExchangePhysiNode* pExchange = (SExchangePhysiNode*)makePhysiNode(pCxt, pExchangeLogicNode->precision, (SLogicNode*)pExchangeLogicNode, QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pExchange->srcGroupId = pExchangeLogicNode->srcGroupId;
  *pPhyNode = (SPhysiNode*)pExchange;

  return TSDB_CODE_SUCCESS;
}
static int32_t createStreamScanPhysiNodeByExchange(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode, SPhysiNode** pPhyNode) {
  SStreamScanPhysiNode* pScan = (SStreamScanPhysiNode*)makePhysiNode(pCxt, pExchangeLogicNode->precision, (SLogicNode*)pExchangeLogicNode, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  if (NULL == pScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  int32_t code = TSDB_CODE_SUCCESS;

  pScan->pScanCols = nodesCloneList(pExchangeLogicNode->node.pTargets);
  if (NULL == pScan->pScanCols) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = sortScanCols(pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = sortScanCols(pScan->pScanCols);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = addDataBlockSlots(pCxt, pScan->pScanCols, pScan->node.pOutputDataBlockDesc);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pScan;
  } else {
    nodesDestroyNode(pScan);
  }

  return code;
}

static int32_t createExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode, SPhysiNode** pPhyNode) {
  if (pCxt->pPlanCxt->streamQuery) {
    return createStreamScanPhysiNodeByExchange(pCxt, pExchangeLogicNode, pPhyNode);
  } else {
    return doCreateExchangePhysiNode(pCxt, pExchangeLogicNode, pPhyNode);
  }
}

static int32_t createWindowPhysiNodeFinalize(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWinodwPhysiNode* pWindow, SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  int32_t code = rewritePrecalcExprs(pCxt, pWindowLogicNode->pFuncs, &pPrecalcExprs, &pFuncs);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pWindow->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pWindow->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pWindowLogicNode->pTspk, &pWindow->pTspk);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pFuncs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs, &pWindow->pFuncs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pWindow->pFuncs, pWindow->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pWindow;
  } else {
    nodesDestroyNode(pWindow);
  }

  return code;
}

static int32_t createIntervalPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SIntervalPhysiNode* pInterval = (SIntervalPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pWindowLogicNode, QUERY_NODE_PHYSICAL_PLAN_INTERVAL);
  if (NULL == pInterval) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pInterval->interval = pWindowLogicNode->interval;
  pInterval->offset = pWindowLogicNode->offset;
  pInterval->sliding = pWindowLogicNode->sliding;
  pInterval->intervalUnit = pWindowLogicNode->intervalUnit;
  pInterval->slidingUnit = pWindowLogicNode->slidingUnit;
  pInterval->precision = ((SColumnNode*)pWindowLogicNode->pTspk)->node.resType.precision;

  pInterval->pFill = nodesCloneNode(pWindowLogicNode->pFill);
  if (NULL != pWindowLogicNode->pFill && NULL == pInterval->pFill) {
    nodesDestroyNode(pInterval);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return createWindowPhysiNodeFinalize(pCxt, pChildren, &pInterval->window, pWindowLogicNode, pPhyNode);
}

static int32_t createSessionWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SSessionWinodwPhysiNode* pSession = (SSessionWinodwPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pWindowLogicNode, QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW);
  if (NULL == pSession) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSession->gap = pWindowLogicNode->sessionGap;

  return createWindowPhysiNodeFinalize(pCxt, pChildren, &pSession->window, pWindowLogicNode, pPhyNode);
}

static int32_t createStateWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  SStateWinodwPhysiNode* pState = (SStateWinodwPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pWindowLogicNode, QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW);
  if (NULL == pState) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNode* pStateKey = NULL;
  int32_t code = rewritePrecalcExpr(pCxt, pWindowLogicNode->pStateExpr, &pPrecalcExprs, &pStateKey);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pState->window.pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pState->window.pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setNodeSlotId(pCxt, pChildTupe->dataBlockId, -1, pStateKey, &pState->pStateKey);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlot(pCxt, &pState->pStateKey, pState->window.node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pState);
    return code;
  }

  return createWindowPhysiNodeFinalize(pCxt, pChildren, &pState->window, pWindowLogicNode, pPhyNode);
}

static int32_t createWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode, SPhysiNode** pPhyNode) {
  switch (pWindowLogicNode->winType) {
    case WINDOW_TYPE_INTERVAL:
      return createIntervalPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_SESSION:
      return createSessionWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    case WINDOW_TYPE_STATE:
      return createStateWindowPhysiNode(pCxt, pChildren, pWindowLogicNode, pPhyNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static int32_t createSortPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SSortLogicNode* pSortLogicNode, SPhysiNode** pPhyNode) {
  SSortPhysiNode* pSort = (SSortPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pSortLogicNode, QUERY_NODE_PHYSICAL_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pSortKeys = NULL;
  int32_t code = rewritePrecalcExprs(pCxt, pSortLogicNode->pSortKeys, &pPrecalcExprs, &pSortKeys);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pSort->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pSort->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pSortKeys, &pSort->pSortKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pSortLogicNode->node.pTargets, &pSort->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pSort->pTargets, pSort->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pSort;
  } else {
    nodesDestroyNode(pSort);
  }

  return code;
}

static int32_t createPartitionPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SPartitionLogicNode* pPartLogicNode, SPhysiNode** pPhyNode) {
  SPartitionPhysiNode* pPart = (SPartitionPhysiNode*)makePhysiNode(pCxt, getPrecision(pChildren), (SLogicNode*)pPartLogicNode, QUERY_NODE_PHYSICAL_PLAN_PARTITION);
  if (NULL == pPart) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pPartitionKeys = NULL;
  int32_t code = rewritePrecalcExprs(pCxt, pPartLogicNode->pPartitionKeys, &pPrecalcExprs, &pPartitionKeys);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (TSDB_CODE_SUCCESS == code && NULL != pPrecalcExprs) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs, &pPart->pExprs);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pPart->pExprs, pChildTupe);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartitionKeys, &pPart->pPartitionKeys);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPartLogicNode->node.pTargets, &pPart->pTargets);
    if (TSDB_CODE_SUCCESS == code) {
      code = addDataBlockSlots(pCxt, pPart->pTargets, pPart->node.pOutputDataBlockDesc);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhyNode = (SPhysiNode*)pPart;
  } else {
    nodesDestroyNode(pPart);
  }

  return code;
}

static int32_t doCreatePhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubplan* pSubplan, SNodeList* pChildren, SPhysiNode** pPhyNode) {
  switch (nodeType(pLogicNode)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      return createScanPhysiNode(pCxt, pSubplan, (SScanLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      return createJoinPhysiNode(pCxt, pChildren, (SJoinLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_AGG:
      return createAggPhysiNode(pCxt, pChildren, (SAggLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      return createProjectPhysiNode(pCxt, pChildren, (SProjectLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      return createExchangePhysiNode(pCxt, (SExchangeLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      return createWindowPhysiNode(pCxt, pChildren, (SWindowLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_SORT:
      return createSortPhysiNode(pCxt, pChildren, (SSortLogicNode*)pLogicNode, pPhyNode);
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      return createPartitionPhysiNode(pCxt, pChildren, (SPartitionLogicNode*)pLogicNode, pPhyNode);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createPhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubplan* pSubplan, SPhysiNode** pPhyNode) {
  SNodeList* pChildren = nodesMakeList();
  if (NULL == pChildren) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pLogicChild;
  FOREACH(pLogicChild, pLogicNode->pChildren) {
    SPhysiNode* pChild = NULL;
    code = createPhysiNode(pCxt, (SLogicNode*)pLogicChild, pSubplan, &pChild);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pChildren, pChild);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = doCreatePhysiNode(pCxt, pLogicNode, pSubplan, pChildren, pPhyNode);
  }

  if (TSDB_CODE_SUCCESS == code) {
    (*pPhyNode)->pChildren = pChildren;
    SNode* pChild;
    FOREACH(pChild, (*pPhyNode)->pChildren) {
      ((SPhysiNode*)pChild)->pParent = (*pPhyNode);
    }
  } else {
    nodesDestroyList(pChildren);
  }

  return code;
}

static int32_t createDataInserter(SPhysiPlanContext* pCxt, SVgDataBlocks* pBlocks, SDataSinkNode** pSink) {
  SDataInserterNode* pInserter = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT);
  if (NULL == pInserter) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pInserter->numOfTables = pBlocks->numOfTables;
  pInserter->size = pBlocks->size;
  TSWAP(pInserter->pData, pBlocks->pData, char*);

  *pSink = (SDataSinkNode*)pInserter;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDataDispatcher(SPhysiPlanContext* pCxt, const SPhysiNode* pRoot, SDataSinkNode** pSink) {
  SDataDispatcherNode* pDispatcher = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH);
  if (NULL == pDispatcher) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pDispatcher->sink.pInputDataBlockDesc = nodesCloneNode(pRoot->pOutputDataBlockDesc);
  if (NULL == pDispatcher->sink.pInputDataBlockDesc) {
    nodesDestroyNode(pDispatcher);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pSink = (SDataSinkNode*)pDispatcher;
  return TSDB_CODE_SUCCESS;
}

static SSubplan* makeSubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan) {
  SSubplan* pSubplan = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id = pLogicSubplan->id;
  pSubplan->subplanType = pLogicSubplan->subplanType;
  pSubplan->level = pLogicSubplan->level;
  return pSubplan;
}

static int32_t createPhysiSubplan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan** pPhysiSubplan) {
  SSubplan* pSubplan = makeSubplan(pCxt, pLogicSubplan);
  if (NULL == pSubplan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType) {
    SVnodeModifLogicNode* pModif = (SVnodeModifLogicNode*)pLogicSubplan->pNode;
    pSubplan->msgType = pModif->msgType;
    pSubplan->execNode.epSet = pModif->pVgDataBlocks->vg.epSet;
    taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
    code = createDataInserter(pCxt, pModif->pVgDataBlocks, &pSubplan->pDataSink);
  } else {
    pSubplan->msgType = TDMT_VND_QUERY;
    code = createPhysiNode(pCxt, pLogicSubplan->pNode, pSubplan, &pSubplan->pNode);
    if (TSDB_CODE_SUCCESS == code && !pCxt->pPlanCxt->streamQuery && !pCxt->pPlanCxt->topicQuery) {
      code = createDataDispatcher(pCxt, pSubplan->pNode, &pSubplan->pDataSink);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhysiSubplan = pSubplan;
  } else {
    nodesDestroyNode(pSubplan);
  }
  
  return code;
}

static SQueryPlan* makeQueryPhysiPlan(SPhysiPlanContext* pCxt) {
  SQueryPlan* pPlan = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);
  if (NULL == pPlan) {
    return NULL;
  }
  pPlan->pSubplans = nodesMakeList();
  if (NULL == pPlan->pSubplans) {
    nodesDestroyNode(pPlan);
    return NULL;
  }
  pPlan->queryId = pCxt->pPlanCxt->queryId;
  return pPlan;
}

static int32_t pushSubplan(SPhysiPlanContext* pCxt, SNodeptr pSubplan, int32_t level, SNodeList* pSubplans) {
  SNodeListNode* pGroup;
  if (level >= LIST_LENGTH(pSubplans)) {
    pGroup = nodesMakeNode(QUERY_NODE_NODE_LIST);
    if (NULL == pGroup) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pSubplans, pGroup)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pGroup = nodesListGetNode(pSubplans, level);
  }
  if (NULL == pGroup->pNodeList) {
    pGroup->pNodeList = nodesMakeList();
    if (NULL == pGroup->pNodeList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListStrictAppend(pGroup->pNodeList, pSubplan);
}

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SSubplan* pParent, SQueryPlan* pQueryPlan) {
  SSubplan* pSubplan = NULL;
  int32_t code = createPhysiSubplan(pCxt, pLogicSubplan, &pSubplan);

  if (TSDB_CODE_SUCCESS == code) {
    code = pushSubplan(pCxt, pSubplan, pLogicSubplan->level, pQueryPlan->pSubplans);
    ++(pQueryPlan->numOfSubplans);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pParent) {
    code = nodesListMakeAppend(&pParent->pChildren, pSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeAppend(&pSubplan->pParents, pParent);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild = NULL;
    FOREACH(pChild, pLogicSubplan->pChildren) {
      code = buildPhysiPlan(pCxt, (SLogicSubplan*)pChild, pSubplan, pQueryPlan);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pSubplan);
  }

  return code;
}

static int32_t doCreatePhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPhysiPlan) {
  SQueryPlan* pPlan = makeQueryPhysiPlan(pCxt);
  if (NULL == pPlan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pSubplan = NULL;
  FOREACH(pSubplan, pLogicPlan->pTopSubplans) {
    code = buildPhysiPlan(pCxt, (SLogicSubplan*)pSubplan, NULL, pPlan);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pPhysiPlan = pPlan;
  } else {
    nodesDestroyNode(pPlan);
  }

  return code;
}

static void destoryLocationHash(void* p) {
  SHashObj* pHash = *(SHashObj**)p;
  SSlotIndex* pIndex = taosHashIterate(pHash, NULL);
  while (NULL != pIndex) {
    taosArrayDestroy(pIndex->pSlotIdsInfo);
    pIndex = taosHashIterate(pHash, pIndex);
  }
  taosHashCleanup(pHash);
}

static void destoryPhysiPlanContext(SPhysiPlanContext* pCxt) {
  taosArrayDestroyEx(pCxt->pLocationHelper, destoryLocationHash);
}

static void setExplainInfo(SPlanContext* pCxt, SQueryPlan* pPlan) {
  if (QUERY_NODE_EXPLAIN_STMT == nodeType(pCxt->pAstRoot)) {
    SExplainStmt* pStmt = (SExplainStmt*)pCxt->pAstRoot;
    pPlan->explainInfo.mode = pStmt->analyze ? EXPLAIN_MODE_ANALYZE : EXPLAIN_MODE_STATIC;
    pPlan->explainInfo.verbose = pStmt->pOptions->verbose;
    pPlan->explainInfo.ratio = pStmt->pOptions->ratio;
  } else {
    pPlan->explainInfo.mode = EXPLAIN_MODE_DISABLE;
  }
}

int32_t createPhysiPlan(SPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan, SArray* pExecNodeList) {
  SPhysiPlanContext cxt = {
    .pPlanCxt = pCxt,
    .errCode = TSDB_CODE_SUCCESS,
    .nextDataBlockId = 0,
    .pLocationHelper = taosArrayInit(32, POINTER_BYTES),
    .pExecNodeList = pExecNodeList
  };
  if (NULL == cxt.pLocationHelper) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = doCreatePhysiPlan(&cxt, pLogicPlan, pPlan);
  if (TSDB_CODE_SUCCESS == code) {
    setExplainInfo(pCxt, *pPlan);
  }

  destoryPhysiPlanContext(&cxt);
  return code;
}
