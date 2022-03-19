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

typedef struct SSlotIndex {
  int16_t dataBlockId;
  int16_t slotId;
} SSlotIndex;

typedef struct SPhysiPlanContext {
  SPlanContext* pPlanCxt;
  int32_t errCode;
  int16_t nextDataBlockId;
  SArray* pLocationHelper;
  SArray* pExecNodeList;
  int32_t subplanId;
} SPhysiPlanContext;

static int32_t getSlotKey(SNode* pNode, char* pKey) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if ('\0' == pCol->tableAlias[0]) {
      return sprintf(pKey, "%s", pCol->colName);
    }
    return sprintf(pKey, "%s.%s", pCol->tableAlias, pCol->colName);
  }
  return sprintf(pKey, "%s", ((SExprNode*)pNode)->aliasName);
}

static SNode* createSlotDesc(SPhysiPlanContext* pCxt, const SNode* pNode, int16_t slotId) {
  SSlotDescNode* pSlot = (SSlotDescNode*)nodesMakeNode(QUERY_NODE_SLOT_DESC);
  CHECK_ALLOC(pSlot, NULL);
  pSlot->slotId = slotId;
  pSlot->dataType = ((SExprNode*)pNode)->resType;
  pSlot->reserve = false;
  pSlot->output = true;
  return (SNode*)pSlot;
}

static SNode* createTarget(SNode* pNode, int16_t dataBlockId, int16_t slotId) {
  STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
  if (NULL == pTarget) {
    return NULL;
  }
  pTarget->dataBlockId = dataBlockId;
  pTarget->slotId = slotId;
  pTarget->pExpr = pNode;
  return (SNode*)pTarget;
}

static int32_t addDataBlockDesc(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  SHashObj* pHash = NULL;
  if (NULL == pDataBlockDesc->pSlots) {
    pDataBlockDesc->pSlots = nodesMakeList();
    CHECK_ALLOC(pDataBlockDesc->pSlots, TSDB_CODE_OUT_OF_MEMORY);

    pHash = taosHashInit(LIST_LENGTH(pList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    CHECK_ALLOC(pHash, TSDB_CODE_OUT_OF_MEMORY);
    if (NULL == taosArrayInsert(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId, &pHash)) {
      taosHashCleanup(pHash);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  }
  
  SNode* pNode = NULL;
  int16_t slotId = taosHashGetSize(pHash);
  FOREACH(pNode, pList) {
    CHECK_CODE_EXT(nodesListStrictAppend(pDataBlockDesc->pSlots, createSlotDesc(pCxt, pNode, slotId)));

    SSlotIndex index = { .dataBlockId = pDataBlockDesc->dataBlockId, .slotId = slotId };
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
    int32_t len = getSlotKey(pNode, name);
    CHECK_CODE(taosHashPut(pHash, name, len, &index, sizeof(SSlotIndex)), TSDB_CODE_OUT_OF_MEMORY);

    SNode* pTarget = createTarget(pNode, pDataBlockDesc->dataBlockId, slotId);
    CHECK_ALLOC(pTarget, TSDB_CODE_OUT_OF_MEMORY);
    REPLACE_NODE(pTarget);

    pDataBlockDesc->resultRowSize += ((SExprNode*)pNode)->resType.bytes;
    ++slotId;
  }
  return TSDB_CODE_SUCCESS;
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
    int32_t len = getSlotKey(pNode, name);
    SSlotIndex* pIndex = taosHashGet(pCxt->pLeftHash, name, len);
    if (NULL == pIndex) {
      pIndex = taosHashGet(pCxt->pRightHash, name, len);
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    CHECK_ALLOC(pIndex, DEAL_RES_ERROR);
    ((SColumnNode*)pNode)->dataBlockId = pIndex->dataBlockId;
    ((SColumnNode*)pNode)->slotId = pIndex->slotId;
    CHECK_ALLOC(pNode, DEAL_RES_ERROR);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static SNode* setNodeSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNode* pNode) {
  SNode* pRes = nodesCloneNode(pNode);
  CHECK_ALLOC(pRes, NULL);
  SSetSlotIdCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)) };
  nodesWalkNode(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(pRes);
    return NULL;
  }
  return pRes;
}

static SNodeList* setListSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNodeList* pList) {
  SNodeList* pRes = nodesCloneList(pList);
  CHECK_ALLOC(pRes, NULL);
  SSetSlotIdCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)) };
  nodesWalkList(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return NULL;
  }
  return pRes;
}

static SPhysiNode* makePhysiNode(SPhysiPlanContext* pCxt, ENodeType type) {
  SPhysiNode* pPhysiNode = (SPhysiNode*)nodesMakeNode(type);
  CHECK_ALLOC(pPhysiNode, NULL);
  pPhysiNode->pOutputDataBlockDesc = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC);
  if (NULL == pPhysiNode->pOutputDataBlockDesc) {
    nodesDestroyNode(pPhysiNode);
    return NULL;
  }
  pPhysiNode->pOutputDataBlockDesc->dataBlockId = pCxt->nextDataBlockId++;
  pPhysiNode->pOutputDataBlockDesc->type = QUERY_NODE_DATABLOCK_DESC;
  return pPhysiNode;
}

static int32_t setConditionsSlotId(SPhysiPlanContext* pCxt, const SLogicNode* pLogicNode, SPhysiNode* pPhysiNode) {
  if (NULL != pLogicNode->pConditions) {
    pPhysiNode->pConditions = setNodeSlotId(pCxt, pPhysiNode->pOutputDataBlockDesc->dataBlockId, -1, pLogicNode->pConditions);
    CHECK_ALLOC(pPhysiNode->pConditions, TSDB_CODE_OUT_OF_MEMORY);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setSlotOutput(SPhysiPlanContext* pCxt, SNodeList* pTargets, SDataBlockDescNode* pDataBlockDesc) {
  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  SNode* pNode;
  FOREACH(pNode, pTargets) {
    int32_t len = getSlotKey(pNode, name);
    SSlotIndex* pIndex = taosHashGet(pHash, name, len);
    // pIndex is definitely not NULL, otherwise it is a bug
    CHECK_ALLOC(pIndex, TSDB_CODE_FAILED);
    ((SSlotDescNode*)nodesListGetNode(pDataBlockDesc->pSlots, pIndex->slotId))->output = true;
  }
  
  return TSDB_CODE_SUCCESS;
}

static SNodeptr createPrimaryKeyCol(SPhysiPlanContext* pCxt, uint64_t tableId) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_ALLOC(pCol, NULL);
  pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
  pCol->tableId = tableId;
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pCol->colType = COLUMN_TYPE_COLUMN;
  strcpy(pCol->colName, "#primarykey");
  return pCol;
}

static int32_t createScanCols(SPhysiPlanContext* pCxt, SScanPhysiNode* pScanPhysiNode, SNodeList* pScanCols) {
  pScanPhysiNode->pScanCols = nodesMakeList();
  CHECK_ALLOC(pScanPhysiNode->pScanCols, TSDB_CODE_OUT_OF_MEMORY);
  CHECK_CODE_EXT(nodesListStrictAppend(pScanPhysiNode->pScanCols, createPrimaryKeyCol(pCxt, pScanPhysiNode->uid)));

  SNode* pNode;
  FOREACH(pNode, pScanCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) {
      SColumnNode* pCol = nodesListGetNode(pScanPhysiNode->pScanCols, 0);
      strcpy(pCol->tableAlias, ((SColumnNode*)pNode)->tableAlias);
      strcpy(pCol->colName, ((SColumnNode*)pNode)->colName);
      continue;
    }
    CHECK_CODE_EXT(nodesListStrictAppend(pScanPhysiNode->pScanCols, nodesCloneNode(pNode)));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t initScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode, SScanPhysiNode* pScanPhysiNode) {
  CHECK_CODE(createScanCols(pCxt, pScanPhysiNode, pScanLogicNode->pScanCols), TSDB_CODE_OUT_OF_MEMORY);

  // Data block describe also needs to be set without scanning column, such as SELECT COUNT(*) FROM t
  CHECK_CODE(addDataBlockDesc(pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setSlotOutput(pCxt, pScanLogicNode->node.pTargets, pScanPhysiNode->node.pOutputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  pScanPhysiNode->uid = pScanLogicNode->pMeta->uid;
  pScanPhysiNode->tableType = pScanLogicNode->pMeta->tableType;
  pScanPhysiNode->order = TSDB_ORDER_ASC;
  pScanPhysiNode->count = 1;
  pScanPhysiNode->reverse = 0;
  memcpy(&pScanPhysiNode->tableName, &pScanLogicNode->tableName, sizeof(SName));

  return TSDB_CODE_SUCCESS;
}

static void vgroupInfoToNodeAddr(const SVgroupInfo* vg, SQueryNodeAddr* pNodeAddr) {
  pNodeAddr->nodeId = vg->vgId;
  pNodeAddr->epSet  = vg->epSet;
}

static SPhysiNode* createTagScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode) {
  STagScanPhysiNode* pTagScan = (STagScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN);
  CHECK_ALLOC(pTagScan, NULL);
  CHECK_CODE(initScanPhysiNode(pCxt, pScanLogicNode, (SScanPhysiNode*)pTagScan), (SPhysiNode*)pTagScan);
  return (SPhysiNode*)pTagScan;
}

static SPhysiNode* createTableScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode) {
  STableScanPhysiNode* pTableScan = (STableScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  CHECK_ALLOC(pTableScan, NULL);
  CHECK_CODE(initScanPhysiNode(pCxt, pScanLogicNode, (SScanPhysiNode*)pTableScan), (SPhysiNode*)pTableScan);
  pTableScan->scanFlag = pScanLogicNode->scanFlag;
  pTableScan->scanRange = pScanLogicNode->scanRange;
  vgroupInfoToNodeAddr(pScanLogicNode->pVgroupList->vgroups, &pSubplan->execNode);
  taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
  return (SPhysiNode*)pTableScan;
}

static SPhysiNode* createStreamScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode) {
  SStreamScanPhysiNode* pTableScan = (SStreamScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
  CHECK_ALLOC(pTableScan, NULL);
  CHECK_CODE(initScanPhysiNode(pCxt, pScanLogicNode, (SScanPhysiNode*)pTableScan), (SPhysiNode*)pTableScan);
  return (SPhysiNode*)pTableScan;
}

static SPhysiNode* createScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode) {
  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      return createTagScanPhysiNode(pCxt, pScanLogicNode);
    case SCAN_TYPE_TABLE:
      return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode);
    case SCAN_TYPE_TOPIC:
    case SCAN_TYPE_STREAM:
      return createStreamScanPhysiNode(pCxt, pSubplan, pScanLogicNode);
    default:
      break;
  }
  return NULL;
}

static SNodeList* createJoinOutputCols(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc, SDataBlockDescNode* pRightDesc) {
  SNodeList* pCols = nodesMakeList();
  CHECK_ALLOC(pCols, NULL);
  SNode* pNode;
  FOREACH(pNode, pLeftDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      goto error;
    }
    pCol->node.resType = pSlot->dataType;
    pCol->dataBlockId = pLeftDesc->dataBlockId;
    pCol->slotId = pSlot->slotId;
    pCol->colId = -1;
    if (TSDB_CODE_SUCCESS != nodesListAppend(pCols, (SNode*)pCol)) {
      goto error;
    }
  }
  FOREACH(pNode, pRightDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      goto error;
    }
    pCol->node.resType = pSlot->dataType;
    pCol->dataBlockId = pRightDesc->dataBlockId;
    pCol->slotId = pSlot->slotId;
    pCol->colId = -1;
    if (TSDB_CODE_SUCCESS != nodesListAppend(pCols, (SNode*)pCol)) {
      goto error;
    }
  }
  return pCols;
error:
  nodesDestroyList(pCols);
  return NULL;
}

static SPhysiNode* createJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode) {
  SJoinPhysiNode* pJoin = (SJoinPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_JOIN);
  CHECK_ALLOC(pJoin, NULL);

  SDataBlockDescNode* pLeftDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc;
  SDataBlockDescNode* pRightDesc = ((SPhysiNode*)nodesListGetNode(pChildren, 1))->pOutputDataBlockDesc;
  pJoin->pOnConditions = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pOnConditions);
  CHECK_ALLOC(pJoin->pOnConditions, (SPhysiNode*)pJoin);

  pJoin->pTargets = createJoinOutputCols(pCxt, pLeftDesc, pRightDesc);
  CHECK_ALLOC(pJoin->pTargets, (SPhysiNode*)pJoin);
  CHECK_CODE(addDataBlockDesc(pCxt, pJoin->pTargets, pJoin->node.pOutputDataBlockDesc), (SPhysiNode*)pJoin);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin), (SPhysiNode*)pJoin);

  CHECK_CODE(setSlotOutput(pCxt, pJoinLogicNode->node.pTargets, pJoin->node.pOutputDataBlockDesc), (SPhysiNode*)pJoin);

  return (SPhysiNode*)pJoin;
}

typedef struct SRewritePrecalcExprsCxt {
  int32_t errCode;
  int32_t planNodeId;
  int32_t rewriteId;
  SNodeList* pPrecalcExprs;
} SRewritePrecalcExprsCxt;

static EDealRes collectAndRewrite(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SNode* pExpr = nodesCloneNode(*pNode);
  CHECK_ALLOC(pExpr, DEAL_RES_ERROR);
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
      if (!fmIsAggFunc(((SFunctionNode*)(*pNode))->funcId)) {
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
    CHECK_ALLOC(*pPrecalcExprs, TSDB_CODE_OUT_OF_MEMORY);
  }
  if (NULL == *pRewrittenList) {
    *pRewrittenList = nodesMakeList();
    CHECK_ALLOC(*pRewrittenList, TSDB_CODE_OUT_OF_MEMORY);
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode* pNew = NULL;
    if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
      pNew = nodesCloneNode(nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0));
    } else {
      pNew = nodesCloneNode(pNode);
    }
    CHECK_ALLOC(pNew, TSDB_CODE_OUT_OF_MEMORY);
    CHECK_CODE(nodesListAppend(*pRewrittenList, pNew), TSDB_CODE_OUT_OF_MEMORY);
  }
  SRewritePrecalcExprsCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pPrecalcExprs = *pPrecalcExprs };
  nodesRewriteList(*pRewrittenList, doRewritePrecalcExprs, &cxt);
  if (0 == LIST_LENGTH(cxt.pPrecalcExprs)) {
    nodesDestroyList(cxt.pPrecalcExprs);
    *pPrecalcExprs = NULL;
  }
  return cxt.errCode;
}

static SPhysiNode* createAggPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SAggLogicNode* pAggLogicNode) {
  SAggPhysiNode* pAgg = (SAggPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_AGG);
  CHECK_ALLOC(pAgg, NULL);

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pGroupKeys = NULL;
  SNodeList* pAggFuncs = NULL;
  CHECK_CODE(rewritePrecalcExprs(pCxt, pAggLogicNode->pGroupKeys, &pPrecalcExprs, &pGroupKeys), (SPhysiNode*)pAgg);
  CHECK_CODE(rewritePrecalcExprs(pCxt, pAggLogicNode->pAggFuncs, &pPrecalcExprs, &pAggFuncs), (SPhysiNode*)pAgg);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (NULL != pPrecalcExprs) {
    pAgg->pExprs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs);
    CHECK_ALLOC(pAgg->pExprs, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pExprs, pChildTupe), (SPhysiNode*)pAgg);
  }

  if (NULL != pGroupKeys) {
    pAgg->pGroupKeys = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pGroupKeys);
    CHECK_ALLOC(pAgg->pGroupKeys, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pGroupKeys, pAgg->node.pOutputDataBlockDesc), (SPhysiNode*)pAgg);
  }

  if (NULL != pAggFuncs) {
    pAgg->pAggFuncs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pAggFuncs);
    CHECK_ALLOC(pAgg->pAggFuncs, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pAggFuncs, pAgg->node.pOutputDataBlockDesc), (SPhysiNode*)pAgg);
  }

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pAggLogicNode, (SPhysiNode*)pAgg), (SPhysiNode*)pAgg);

  CHECK_CODE(setSlotOutput(pCxt, pAggLogicNode->node.pTargets, pAgg->node.pOutputDataBlockDesc), (SPhysiNode*)pAgg);

  return (SPhysiNode*)pAgg;
}

static SPhysiNode* createProjectPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SProjectLogicNode* pProjectLogicNode) {
  SProjectPhysiNode* pProject = (SProjectPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  CHECK_ALLOC(pProject, NULL);

  pProject->pProjections = setListSlotId(pCxt, ((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc->dataBlockId, -1, pProjectLogicNode->pProjections);
  CHECK_ALLOC(pProject->pProjections, (SPhysiNode*)pProject);
  CHECK_CODE(addDataBlockDesc(pCxt, pProject->pProjections, pProject->node.pOutputDataBlockDesc), (SPhysiNode*)pProject);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pProjectLogicNode, (SPhysiNode*)pProject), (SPhysiNode*)pProject);

  return (SPhysiNode*)pProject;
}

static SPhysiNode* createExchangePhysiNode(SPhysiPlanContext* pCxt, SExchangeLogicNode* pExchangeLogicNode) {
  if (pCxt->pPlanCxt->streamQuery) {
    SStreamScanPhysiNode* pScan = (SStreamScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN);
    CHECK_ALLOC(pScan, NULL);
    pScan->pScanCols = nodesCloneList(pExchangeLogicNode->node.pTargets);
    CHECK_ALLOC(pScan->pScanCols, (SPhysiNode*)pScan);
    CHECK_CODE(addDataBlockDesc(pCxt, pExchangeLogicNode->node.pTargets, pScan->node.pOutputDataBlockDesc), (SPhysiNode*)pScan);
    return (SPhysiNode*)pScan;
  } else {
    SExchangePhysiNode* pExchange = (SExchangePhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
    CHECK_ALLOC(pExchange, NULL);
    CHECK_CODE(addDataBlockDesc(pCxt, pExchangeLogicNode->node.pTargets, pExchange->node.pOutputDataBlockDesc), (SPhysiNode*)pExchange);
    pExchange->srcGroupId = pExchangeLogicNode->srcGroupId;
    return (SPhysiNode*)pExchange;
  }
}

static SPhysiNode* createIntervalPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode) {
  SIntervalPhysiNode* pInterval = (SIntervalPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_INTERVAL);
  CHECK_ALLOC(pInterval, NULL);

  pInterval->interval = pWindowLogicNode->interval;
  pInterval->offset = pWindowLogicNode->offset;
  pInterval->sliding = pWindowLogicNode->sliding;
  pInterval->intervalUnit = pWindowLogicNode->intervalUnit;
  pInterval->slidingUnit = pWindowLogicNode->slidingUnit;

  pInterval->pFill = nodesCloneNode(pWindowLogicNode->pFill);

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pFuncs = NULL;
  CHECK_CODE(rewritePrecalcExprs(pCxt, pWindowLogicNode->pFuncs, &pPrecalcExprs, &pFuncs), (SPhysiNode*)pInterval);

  SDataBlockDescNode* pChildTupe = (((SPhysiNode*)nodesListGetNode(pChildren, 0))->pOutputDataBlockDesc);
  // push down expression to pOutputDataBlockDesc of child node
  if (NULL != pPrecalcExprs) {
    pInterval->pExprs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs);
    CHECK_ALLOC(pInterval->pExprs, (SPhysiNode*)pInterval);
    CHECK_CODE(addDataBlockDesc(pCxt, pInterval->pExprs, pChildTupe), (SPhysiNode*)pInterval);
  }

  if (NULL != pFuncs) {
    pInterval->pFuncs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pFuncs);
    CHECK_ALLOC(pInterval->pFuncs, (SPhysiNode*)pInterval);
    CHECK_CODE(addDataBlockDesc(pCxt, pInterval->pFuncs, pInterval->node.pOutputDataBlockDesc), (SPhysiNode*)pInterval);
  }

  CHECK_CODE(setSlotOutput(pCxt, pWindowLogicNode->node.pTargets, pInterval->node.pOutputDataBlockDesc), (SPhysiNode*)pInterval);

  return (SPhysiNode*)pInterval;
}

static SPhysiNode* createWindowPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SWindowLogicNode* pWindowLogicNode) {
  switch (pWindowLogicNode->winType) {
    case WINDOW_TYPE_INTERVAL:
      return createIntervalPhysiNode(pCxt, pChildren, pWindowLogicNode);
    case WINDOW_TYPE_SESSION:
    case WINDOW_TYPE_STATE:
      break;
    default:
      break;
  }
  return NULL;
}

static SPhysiNode* createPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SLogicNode* pLogicPlan) {
  SNodeList* pChildren = nodesMakeList();
  CHECK_ALLOC(pChildren, NULL);

  SNode* pLogicChild;
  FOREACH(pLogicChild, pLogicPlan->pChildren) {
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pChildren, createPhysiNode(pCxt, pSubplan, (SLogicNode*)pLogicChild))) {
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
      nodesDestroyList(pChildren);
      return NULL;
    }
  }

  SPhysiNode* pPhyNode = NULL;
  switch (nodeType(pLogicPlan)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      pPhyNode = createScanPhysiNode(pCxt, pSubplan, (SScanLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      pPhyNode = createJoinPhysiNode(pCxt, pChildren, (SJoinLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      pPhyNode = createAggPhysiNode(pCxt, pChildren, (SAggLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      pPhyNode = createProjectPhysiNode(pCxt, pChildren, (SProjectLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      pPhyNode = createExchangePhysiNode(pCxt, (SExchangeLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      pPhyNode = createWindowPhysiNode(pCxt, pChildren, (SWindowLogicNode*)pLogicPlan);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    nodesDestroyNode(pPhyNode);
    return NULL;
  }

  pPhyNode->pChildren = pChildren;
  SNode* pChild;
  FOREACH(pChild, pPhyNode->pChildren) {
    ((SPhysiNode*)pChild)->pParent = pPhyNode;
  }

  return pPhyNode;
}

static SDataSinkNode* createDataInserter(SPhysiPlanContext* pCxt, SVgDataBlocks* pBlocks) {
  SDataInserterNode* pInserter = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INSERT);
  CHECK_ALLOC(pInserter, NULL);
  pInserter->numOfTables = pBlocks->numOfTables;
  pInserter->size = pBlocks->size;
  TSWAP(pInserter->pData, pBlocks->pData, char*);
  return (SDataSinkNode*)pInserter;
}

static SDataSinkNode* createDataDispatcher(SPhysiPlanContext* pCxt, const SPhysiNode* pRoot) {
  SDataDispatcherNode* pDispatcher = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_DISPATCH);
  CHECK_ALLOC(pDispatcher, NULL);
  pDispatcher->sink.pInputDataBlockDesc = nodesCloneNode(pRoot->pOutputDataBlockDesc);
  CHECK_ALLOC(pDispatcher->sink.pInputDataBlockDesc, (SDataSinkNode*)pDispatcher);
  return (SDataSinkNode*)pDispatcher;
}

static SSubplan* makeSubplan(SPhysiPlanContext* pCxt, SSubLogicPlan* pLogicSubplan) {
  SSubplan* pSubplan = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  CHECK_ALLOC(pSubplan, NULL);
  pSubplan->id = pLogicSubplan->id;
  pSubplan->subplanType = pLogicSubplan->subplanType;
  pSubplan->level = pLogicSubplan->level;
  return pSubplan;
}

static SSubplan* createPhysiSubplan(SPhysiPlanContext* pCxt, SSubLogicPlan* pLogicSubplan) {
  SSubplan* pSubplan = makeSubplan(pCxt, pLogicSubplan);
  CHECK_ALLOC(pSubplan, NULL);
  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType) {
    SVnodeModifLogicNode* pModif = (SVnodeModifLogicNode*)pLogicSubplan->pNode;
    pSubplan->pDataSink = createDataInserter(pCxt, pModif->pVgDataBlocks);
    pSubplan->msgType = pModif->msgType;
    pSubplan->execNode.epSet = pModif->pVgDataBlocks->vg.epSet;
    taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
  } else {
    pSubplan->pNode = createPhysiNode(pCxt, pSubplan, pLogicSubplan->pNode);
    if (!pCxt->pPlanCxt->streamQuery && !pCxt->pPlanCxt->topicQuery) {
      pSubplan->pDataSink = createDataDispatcher(pCxt, pSubplan->pNode);
    }
    pSubplan->msgType = TDMT_VND_QUERY;
  }
  return pSubplan;
}

static void doSetLogicNodeParent(SLogicNode* pNode, SLogicNode* pParent) {
  pNode->pParent = pParent;
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    doSetLogicNodeParent((SLogicNode*)pChild, pNode);
  }
}

static void setLogicNodeParent(SLogicNode* pNode) {
  doSetLogicNodeParent(pNode, NULL);
}

static int32_t splitLogicPlan(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubLogicPlan** pSubLogicPlan) {
  *pSubLogicPlan = (SSubLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  CHECK_ALLOC(*pSubLogicPlan, TSDB_CODE_OUT_OF_MEMORY);
  (*pSubLogicPlan)->pNode = nodesCloneNode(pLogicNode);
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIF == nodeType(pLogicNode)) {
    (*pSubLogicPlan)->subplanType = SUBPLAN_TYPE_MODIFY;
    TSWAP(((SVnodeModifLogicNode*)pLogicNode)->pDataBlocks, ((SVnodeModifLogicNode*)(*pSubLogicPlan)->pNode)->pDataBlocks, SArray*);
  } else {
    (*pSubLogicPlan)->subplanType = SUBPLAN_TYPE_SCAN;
  }
  (*pSubLogicPlan)->id.queryId = pCxt->pPlanCxt->queryId;
  setLogicNodeParent((*pSubLogicPlan)->pNode);
  return applySplitRule(*pSubLogicPlan);
}

static int32_t pushSubplan(SPhysiPlanContext* pCxt, SNodeptr pSubplan, int32_t level, SNodeList* pSubplans) {
  SNodeListNode* pGroup;
  if (level >= LIST_LENGTH(pSubplans)) {
    pGroup = nodesMakeNode(QUERY_NODE_NODE_LIST);
    CHECK_ALLOC(pGroup, TSDB_CODE_OUT_OF_MEMORY);
    CHECK_CODE(nodesListStrictAppend(pSubplans, pGroup), TSDB_CODE_OUT_OF_MEMORY);
  } else {
    pGroup = nodesListGetNode(pSubplans, level);
  }
  if (NULL == pGroup->pNodeList) {
    pGroup->pNodeList = nodesMakeList();
    CHECK_ALLOC(pGroup->pNodeList, TSDB_CODE_OUT_OF_MEMORY);
  }
  CHECK_CODE(nodesListStrictAppend(pGroup->pNodeList, pSubplan), TSDB_CODE_OUT_OF_MEMORY);
  return TSDB_CODE_SUCCESS;
}

static SSubLogicPlan* singleCloneSubLogicPlan(SPhysiPlanContext* pCxt, SSubLogicPlan* pSrc, int32_t level) {
  SSubLogicPlan* pDst = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  CHECK_ALLOC(pDst, NULL);
  pDst->pNode = nodesCloneNode(pSrc->pNode);
  if (NULL == pDst->pNode) {
    nodesDestroyNode(pDst);
    return NULL;
  }
  pDst->subplanType = pSrc->subplanType;
  pDst->level = level;
  pDst->id.queryId = pSrc->id.queryId;
  pDst->id.groupId = pSrc->id.groupId;
  pDst->id.subplanId = pCxt->subplanId++;
  return pDst;
}

static int32_t scaleOutForModify(SPhysiPlanContext* pCxt, SSubLogicPlan* pSubplan, int32_t level, SNodeList* pGroup) {
  SVnodeModifLogicNode* pNode = (SVnodeModifLogicNode*)pSubplan->pNode;
  size_t numOfVgroups = taosArrayGetSize(pNode->pDataBlocks);
  for (int32_t i = 0; i < numOfVgroups; ++i) {
    SSubLogicPlan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    CHECK_ALLOC(pNewSubplan, TSDB_CODE_OUT_OF_MEMORY);
    SVgDataBlocks* blocks = (SVgDataBlocks*)taosArrayGetP(pNode->pDataBlocks, i);
    ((SVnodeModifLogicNode*)pNewSubplan->pNode)->pVgDataBlocks = blocks;
    CHECK_CODE_EXT(nodesListAppend(pGroup, pNewSubplan));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t scaleOutForMerge(SPhysiPlanContext* pCxt, SSubLogicPlan* pSubplan, int32_t level, SNodeList* pGroup) {
  return nodesListStrictAppend(pGroup, singleCloneSubLogicPlan(pCxt, pSubplan, level));
}

static int32_t doSetScanVgroup(SPhysiPlanContext* pCxt, SLogicNode* pNode, const SVgroupInfo* pVgroup, bool* pFound) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    pScan->pVgroupList = calloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
    CHECK_ALLOC(pScan->pVgroupList, TSDB_CODE_OUT_OF_MEMORY);
    memcpy(pScan->pVgroupList->vgroups, pVgroup, sizeof(SVgroupInfo));
    *pFound = true;
    return TSDB_CODE_SUCCESS;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    int32_t code = doSetScanVgroup(pCxt, (SLogicNode*)pChild, pVgroup, pFound);
    if (TSDB_CODE_SUCCESS != code || *pFound) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setScanVgroup(SPhysiPlanContext* pCxt, SLogicNode* pNode, const SVgroupInfo* pVgroup) {
  bool found = false;
  return doSetScanVgroup(pCxt, pNode, pVgroup, &found);
}

static int32_t scaleOutForScan(SPhysiPlanContext* pCxt, SSubLogicPlan* pSubplan, int32_t level, SNodeList* pGroup) {
  if (pSubplan->pVgroupList) {
    for (int32_t i = 0; i < pSubplan->pVgroupList->numOfVgroups; ++i) {
      SSubLogicPlan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
      CHECK_ALLOC(pNewSubplan, TSDB_CODE_OUT_OF_MEMORY);
      CHECK_CODE_EXT(setScanVgroup(pCxt, pNewSubplan->pNode, pSubplan->pVgroupList->vgroups + i));
      CHECK_CODE_EXT(nodesListAppend(pGroup, pNewSubplan));
    }
    return TSDB_CODE_SUCCESS;
  } else {
    return scaleOutForMerge(pCxt, pSubplan, level, pGroup);
  }
}

static int32_t appendWithMakeList(SNodeList** pList, SNodeptr pNode) {
  if (NULL == *pList) {
    *pList = nodesMakeList();
    if (NULL == *pList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return nodesListAppend(*pList, pNode);
}

static int32_t pushHierarchicalPlan(SPhysiPlanContext* pCxt, SNodeList* pParentsGroup, SNodeList* pCurrentGroup) {
  bool topLevel = (0 == LIST_LENGTH(pParentsGroup));
  SNode* pChild = NULL;
  FOREACH(pChild, pCurrentGroup) {
    if (topLevel) {
      CHECK_CODE_EXT(nodesListAppend(pParentsGroup, pChild));
    } else {
      SNode* pParent = NULL;
      FOREACH(pParent, pParentsGroup) {
        CHECK_CODE_EXT(appendWithMakeList(&(((SSubLogicPlan*)pParent)->pChildren), pChild));
        CHECK_CODE_EXT(appendWithMakeList(&(((SSubLogicPlan*)pChild)->pParents), pParent));
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t doScaleOut(SPhysiPlanContext* pCxt, SSubLogicPlan* pSubplan, int32_t* pLevel, SNodeList* pParentsGroup) {
  SNodeList* pCurrentGroup = nodesMakeList();
  CHECK_ALLOC(pCurrentGroup, TSDB_CODE_OUT_OF_MEMORY);
  int32_t code = TSDB_CODE_SUCCESS;
  switch (pSubplan->subplanType) {
    case SUBPLAN_TYPE_MERGE:
      code = scaleOutForMerge(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_SCAN:
      code = scaleOutForScan(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_MODIFY:
      code = scaleOutForModify(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  CHECK_CODE_EXT(pushHierarchicalPlan(pCxt, pParentsGroup, pCurrentGroup));
  ++(*pLevel);
  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    CHECK_CODE_EXT(doScaleOut(pCxt, (SSubLogicPlan*)pChild, pLevel, pCurrentGroup));
  }

  return TSDB_CODE_SUCCESS;
}

static SQueryLogicPlan* makeQueryLogicPlan(SPhysiPlanContext* pCxt) {
  SQueryLogicPlan* pLogicPlan = (SQueryLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN);
  CHECK_ALLOC(pLogicPlan, NULL);
  pLogicPlan->pTopSubplans = nodesMakeList();
  if (NULL == pLogicPlan->pTopSubplans) {
    nodesDestroyNode(pLogicPlan);
    return NULL;
  }
  return pLogicPlan;
}

static int32_t scaleOutLogicPlan(SPhysiPlanContext* pCxt, SSubLogicPlan* pRootSubLogicPlan, SQueryLogicPlan** pLogicPlan) {
  *pLogicPlan = makeQueryLogicPlan(pCxt);
  CHECK_ALLOC(*pLogicPlan, TSDB_CODE_OUT_OF_MEMORY);
  return doScaleOut(pCxt, pRootSubLogicPlan, &((*pLogicPlan)->totalLevel), (*pLogicPlan)->pTopSubplans);
}

static SQueryPlan* makeQueryPhysiPlan(SPhysiPlanContext* pCxt) {
  SQueryPlan* pPlan = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);
  CHECK_ALLOC(pPlan, NULL);
  pPlan->pSubplans = nodesMakeList();
  if (NULL == pPlan->pSubplans) {
    nodesDestroyNode(pPlan);
    return NULL;
  }
  pPlan->queryId = pCxt->pPlanCxt->queryId;
  return pPlan;
}

static int32_t doBuildPhysiPlan(SPhysiPlanContext* pCxt, SSubLogicPlan* pLogicSubplan, SSubplan* pParent, SQueryPlan* pQueryPlan) {
  SSubplan* pSubplan = createPhysiSubplan(pCxt, pLogicSubplan);
  CHECK_ALLOC(pSubplan, DEAL_RES_ERROR);
  CHECK_CODE_EXT(pushSubplan(pCxt, pSubplan, pLogicSubplan->level, pQueryPlan->pSubplans));
  ++(pQueryPlan->numOfSubplans);
  if (NULL != pParent) {
    CHECK_CODE_EXT(appendWithMakeList(&pParent->pChildren, pSubplan));
    CHECK_CODE_EXT(appendWithMakeList(&pSubplan->pParents, pParent));
  }

  SNode* pChild = NULL;
  FOREACH(pChild, pLogicSubplan->pChildren) {
    CHECK_CODE_EXT(doBuildPhysiPlan(pCxt, (SSubLogicPlan*)pChild, pSubplan, pQueryPlan));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan) {
  *pPlan =  makeQueryPhysiPlan(pCxt);
  CHECK_ALLOC(*pPlan, TSDB_CODE_OUT_OF_MEMORY);
  SNode* pSubplan = NULL;
  FOREACH(pSubplan, pLogicPlan->pTopSubplans) {
    CHECK_CODE_EXT(doBuildPhysiPlan(pCxt, (SSubLogicPlan*)pSubplan, NULL, *pPlan));
  }
  return TSDB_CODE_SUCCESS;
}

int32_t createPhysiPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SQueryPlan** pPlan, SArray* pExecNodeList) {
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
  SQueryLogicPlan* pLogicPlan = NULL;
  SSubLogicPlan* pSubLogicPlan = NULL;
  int32_t code = splitLogicPlan(&cxt, pLogicNode, &pSubLogicPlan);
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(&cxt, pSubLogicPlan, &pLogicPlan);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildPhysiPlan(&cxt, pLogicPlan, pPlan);
  }
  nodesDestroyNode(pSubLogicPlan);
  nodesDestroyNode(pLogicPlan);
  return code;
}
