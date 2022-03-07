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

#include "plannerInt.h"

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
  pSlot->output = false;
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
    SNode* pSlot = createSlotDesc(pCxt, pNode, slotId);
    CHECK_ALLOC(pSlot, TSDB_CODE_OUT_OF_MEMORY);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pDataBlockDesc->pSlots, (SNode*)pSlot)) {
      nodesDestroyNode(pSlot);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SSlotIndex index = { .dataBlockId = pDataBlockDesc->dataBlockId, .slotId = slotId };
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
    int32_t len = getSlotKey(pNode, name);
    CHECK_CODE(taosHashPut(pHash, name, len, &index, sizeof(SSlotIndex)), TSDB_CODE_OUT_OF_MEMORY);

    SNode* pTarget = createTarget(pNode, pDataBlockDesc->dataBlockId, slotId);
    CHECK_ALLOC(pTarget, TSDB_CODE_OUT_OF_MEMORY);
    REPLACE_NODE(pTarget);

    pDataBlockDesc->resultRowSize += ((SSlotDescNode*)pSlot)->dataType.bytes;
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
  return pCol;
}

static int32_t addPrimaryKeyCol(SPhysiPlanContext* pCxt, SScanPhysiNode* pScanPhysiNode) {
  if (NULL == pScanPhysiNode->pScanCols) {
    pScanPhysiNode->pScanCols = nodesMakeList();
    CHECK_ALLOC(pScanPhysiNode->pScanCols, TSDB_CODE_OUT_OF_MEMORY);
    CHECK_CODE_EXT(nodesListStrictAppend(pScanPhysiNode->pScanCols, createPrimaryKeyCol(pCxt, pScanPhysiNode->uid)));
    return TSDB_CODE_SUCCESS;
  }
  SNode* pNode;
  FOREACH(pNode, pScanPhysiNode->pScanCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) {
      return TSDB_CODE_SUCCESS;
    }
  }
  CHECK_CODE_EXT(nodesListStrictAppend(pScanPhysiNode->pScanCols, createPrimaryKeyCol(pCxt, pScanPhysiNode->uid)));
  return TSDB_CODE_SUCCESS;
}

static int32_t initScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode, SScanPhysiNode* pScanPhysiNode) {
  if (NULL != pScanLogicNode->pScanCols) {
    pScanPhysiNode->pScanCols = nodesCloneList(pScanLogicNode->pScanCols);
    CHECK_ALLOC(pScanPhysiNode->pScanCols, TSDB_CODE_OUT_OF_MEMORY);
  }
  CHECK_CODE(addPrimaryKeyCol(pCxt, pScanPhysiNode), TSDB_CODE_OUT_OF_MEMORY);

  // Data block describe also needs to be set without scanning column, such as SELECT COUNT(*) FROM t
  CHECK_CODE(addDataBlockDesc(pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setSlotOutput(pCxt, pScanLogicNode->node.pTargets, pScanPhysiNode->node.pOutputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  pScanPhysiNode->uid = pScanLogicNode->pMeta->uid;
  pScanPhysiNode->tableType = pScanLogicNode->pMeta->tableType;
  pScanPhysiNode->order = TSDB_ORDER_ASC;
  pScanPhysiNode->count = 1;
  pScanPhysiNode->reverse = 0;

  return TSDB_CODE_SUCCESS;
}

static void vgroupInfoToNodeAddr(const SVgroupInfo* vg, SQueryNodeAddr* pNodeAddr) {
  pNodeAddr->nodeId = vg->vgId;
  pNodeAddr->epset  = vg->epset;
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

static SPhysiNode* createScanPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SScanLogicNode* pScanLogicNode) {
  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      return createTagScanPhysiNode(pCxt, pScanLogicNode);
    case SCAN_TYPE_TABLE:
      return createTableScanPhysiNode(pCxt, pSubplan, pScanLogicNode);
    case SCAN_TYPE_STABLE:
    case SCAN_TYPE_STREAM:
      break;
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

static SPhysiNode* createPhysiNode(SPhysiPlanContext* pCxt, SSubplan* pSubplan, SLogicNode* pLogicPlan) {
  SNodeList* pChildren = nodesMakeList();
  CHECK_ALLOC(pChildren, NULL);

  SNode* pLogicChild;
  FOREACH(pLogicChild, pLogicPlan->pChildren) {
    SNode* pChildPhyNode = (SNode*)createPhysiNode(pCxt, pSubplan, (SLogicNode*)pLogicChild);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pChildren, pChildPhyNode)) {
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
    default:
      break;
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

static SSubplan* createPhysiSubplan(SPhysiPlanContext* pCxt, SSubLogicPlan* pLogicSubplan) {
  SSubplan* pSubplan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  CHECK_ALLOC(pSubplan, NULL);
  if (SUBPLAN_TYPE_MODIFY == pLogicSubplan->subplanType) {
    SVnodeModifLogicNode* pModif = (SVnodeModifLogicNode*)pLogicSubplan->pNode;
    pSubplan->pDataSink = createDataInserter(pCxt, pModif->pVgDataBlocks);
    pSubplan->msgType = pModif->msgType;
    pSubplan->execNode.epset = pModif->pVgDataBlocks->vg.epset;
    taosArrayPush(pCxt->pExecNodeList, &pSubplan->execNode);
  } else {
    pSubplan->pNode = createPhysiNode(pCxt, pSubplan, pLogicSubplan->pNode);
    pSubplan->pDataSink = createDataDispatcher(pCxt, pSubplan->pNode);
    pSubplan->msgType = TDMT_VND_QUERY;
  }
  pSubplan->subplanType = pLogicSubplan->subplanType;
  return pSubplan;
}

static int32_t splitLogicPlan(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SSubLogicPlan** pSubLogicPlan) {
  *pSubLogicPlan = (SSubLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  CHECK_ALLOC(*pSubLogicPlan, TSDB_CODE_OUT_OF_MEMORY);
  // todo pSubplan->pNode = nodesCloneNode(pLogicNode);
  (*pSubLogicPlan)->pNode = pLogicNode;
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIF == nodeType(pLogicNode)) {
    (*pSubLogicPlan)->subplanType = SUBPLAN_TYPE_MODIFY;
  }
  // todo split
  return TSDB_CODE_SUCCESS;
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
}

SSubLogicPlan* singleCloneSubLogicPlan(SPhysiPlanContext* pCxt, SSubLogicPlan* pSrc, int32_t level) {
  SSubLogicPlan* pDst = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  CHECK_ALLOC(pDst, NULL);
  // todo pDst->pNode = nodesCloneNode(pSrc->pNode);
  pDst->pNode = pSrc->pNode;
  if (NULL == pDst->pNode) {
    nodesDestroyNode(pDst);
    return NULL;
  }
  pDst->subplanType = pSrc->subplanType;
  pDst->level = level;
  return pDst;
}

static int32_t doScaleOut(SPhysiPlanContext* pCxt, SSubLogicPlan* pSubplan, int32_t level, SQueryLogicPlan* pLogicPlan) {
  if (SUBPLAN_TYPE_MODIFY == pSubplan->subplanType) {
    SVnodeModifLogicNode* pNode = (SVnodeModifLogicNode*)pSubplan->pNode;
    size_t numOfVgroups = taosArrayGetSize(pNode->pDataBlocks);
    for (int32_t i = 0; i < numOfVgroups; ++i) {
      SSubLogicPlan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
      CHECK_ALLOC(pNewSubplan, TSDB_CODE_OUT_OF_MEMORY);
      SVgDataBlocks* blocks = (SVgDataBlocks*)taosArrayGetP(pNode->pDataBlocks, i);
      ((SVnodeModifLogicNode*)pNewSubplan->pNode)->pVgDataBlocks = blocks;
      CHECK_CODE_EXT(pushSubplan(pCxt, pNewSubplan, level, pLogicPlan->pSubplans));
    }
  } else {
    SSubLogicPlan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    CHECK_ALLOC(pNewSubplan, TSDB_CODE_OUT_OF_MEMORY);
    CHECK_CODE_EXT(pushSubplan(pCxt, pNewSubplan, level, pLogicPlan->pSubplans));
  }

  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    CHECK_CODE_EXT(doScaleOut(pCxt, (SSubLogicPlan*)pChild, level + 1, pLogicPlan));
  }

  return TSDB_CODE_SUCCESS;
}

static SQueryLogicPlan* makeQueryLogicPlan(SPhysiPlanContext* pCxt) {
  SQueryLogicPlan* pLogicPlan = (SQueryLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN);
  CHECK_ALLOC(pLogicPlan, NULL);
  pLogicPlan->pSubplans = nodesMakeList();
  if (NULL == pLogicPlan->pSubplans) {
    nodesDestroyNode(pLogicPlan);
    return NULL;
  }
  return pLogicPlan;
}

static int32_t scaleOutLogicPlan(SPhysiPlanContext* pCxt, SSubLogicPlan* pRootSubLogicPlan, SQueryLogicPlan** pLogicPlan) {
  *pLogicPlan = makeQueryLogicPlan(pCxt);
  CHECK_ALLOC(*pLogicPlan, TSDB_CODE_OUT_OF_MEMORY);
  return doScaleOut(pCxt, pRootSubLogicPlan, 0, *pLogicPlan);
}

typedef struct SBuildPhysiSubplanCxt {
  int32_t errCode;
  SQueryPlan* pQueryPlan;
  SPhysiPlanContext* pPhyCxt;
} SBuildPhysiSubplanCxt;

static EDealRes doBuildPhysiSubplan(SNode* pNode, void* pContext) {
  SBuildPhysiSubplanCxt* pCxt = (SBuildPhysiSubplanCxt*)pContext;
  if (QUERY_NODE_LOGIC_SUBPLAN == nodeType(pNode)) {
    SSubplan* pSubplan = createPhysiSubplan(pCxt->pPhyCxt, (SSubLogicPlan*)pNode);
    CHECK_ALLOC(pSubplan, DEAL_RES_ERROR);
    CHECK_CODE(pushSubplan(pCxt->pPhyCxt, pSubplan, ((SSubLogicPlan*)pNode)->level, pCxt->pQueryPlan->pSubplans), DEAL_RES_ERROR);
    ++(pCxt->pQueryPlan->numOfSubplans);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
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

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan) {
  SBuildPhysiSubplanCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pQueryPlan = makeQueryPhysiPlan(pCxt), .pPhyCxt = pCxt };
  CHECK_ALLOC(cxt.pQueryPlan, TSDB_CODE_OUT_OF_MEMORY);
  nodesWalkList(pLogicPlan->pSubplans, doBuildPhysiSubplan, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(cxt.pQueryPlan);
    return cxt.errCode;
  }
  *pPlan = cxt.pQueryPlan;
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
  SQueryLogicPlan* pLogicPlan;
  SSubLogicPlan* pSubLogicPlan;
  int32_t code = splitLogicPlan(&cxt, pLogicNode, &pSubLogicPlan);
  if (TSDB_CODE_SUCCESS == code) {
    code = scaleOutLogicPlan(&cxt, pSubLogicPlan, &pLogicPlan);
  }
  // todo maping
  if (TSDB_CODE_SUCCESS == code) {
    code = buildPhysiPlan(&cxt, pLogicPlan, pPlan);
  }
  nodesDestroyNode((SNode*)pLogicPlan);
  return code;
}
