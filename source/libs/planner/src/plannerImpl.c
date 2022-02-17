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

#include "plannerImpl.h"
#include "functionMgt.h"

static SLogicNode* createQueryLogicNode(SNode* pStmt);

typedef struct SCollectColumnsCxt {
  SNodeList* pCols;
  SHashObj* pColIdHash;
} SCollectColumnsCxt;

static EDealRes doCollectColumns(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
    int16_t colId = ((SColumnNode*)pNode)->colId;
    if (colId > 0) {
      if (NULL == taosHashGet(pCxt->pColIdHash, &colId, sizeof(colId))) {
        taosHashPut(pCxt->pColIdHash, &colId, sizeof(colId), NULL, 0);
        nodesListAppend(pCxt->pCols, pNode);
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static SNodeList* collectColumns(SSelectStmt* pSelect) {
  SCollectColumnsCxt cxt = { .pCols = nodesMakeList(), .pColIdHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK) };
  if (NULL == cxt.pCols || NULL == cxt.pColIdHash) {
    return NULL;
  }
  nodesWalkNode(pSelect->pFromTable, doCollectColumns, &cxt);
  nodesWalkNode(pSelect->pWhere, doCollectColumns, &cxt);
  nodesWalkList(pSelect->pPartitionByList, doCollectColumns, &cxt);
  nodesWalkNode(pSelect->pWindow, doCollectColumns, &cxt);
  nodesWalkList(pSelect->pGroupByList, doCollectColumns, &cxt);
  nodesWalkNode(pSelect->pHaving, doCollectColumns, &cxt);
  nodesWalkList(pSelect->pProjectionList, doCollectColumns, &cxt);
  nodesWalkList(pSelect->pOrderByList, doCollectColumns, &cxt);
  taosHashCleanup(cxt.pColIdHash);
  return cxt.pCols;
}

typedef struct SCollectAggFuncsCxt {
  SNodeList* pAggFuncs;
} SCollectAggFuncsCxt;

static EDealRes doCollectAggFuncs(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId)) {
    SCollectAggFuncsCxt* pCxt = (SCollectAggFuncsCxt*)pContext;
    nodesListAppend(pCxt->pAggFuncs, pNode);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static SNodeList* collectAggFuncs(SSelectStmt* pSelect) {
  SCollectAggFuncsCxt cxt = { .pAggFuncs = nodesMakeList() };
  if (NULL == cxt.pAggFuncs) {
    return NULL;
  }
  nodesWalkNode(pSelect->pHaving, doCollectAggFuncs, &cxt);
  nodesWalkList(pSelect->pProjectionList, doCollectAggFuncs, &cxt);
  if (!pSelect->isDistinct) {
    nodesWalkList(pSelect->pOrderByList, doCollectAggFuncs, &cxt);
  }
  return cxt.pAggFuncs;
}

typedef struct SRewriteExprCxt {
  SNodeList* pTargets;
} SRewriteExprCxt;

static EDealRes doRewriteExpr(SNode** pNode, void* pContext) {
  SRewriteExprCxt* pCxt = (SRewriteExprCxt*)pContext;
  SNode* pTarget;
  int32_t index = 0;
  FOREACH(pTarget, pCxt->pTargets) {
    if (nodesEqualNode(pTarget, *pNode)) {
      nodesDestroyNode(*pNode);
      *pNode = nodesMakeNode(QUERY_NODE_COLUMN_REF);
      ((SColumnRef*)*pNode)->slotId = index;
      return DEAL_RES_IGNORE_CHILD;
    }
    ++index;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteExpr(SNodeList* pTargets, SSelectStmt* pSelect) {
  SRewriteExprCxt cxt = { .pTargets = pTargets };
  nodesRewriteNode(&(pSelect->pFromTable), doRewriteExpr, &cxt);
  nodesRewriteNode(&(pSelect->pWhere), doRewriteExpr, &cxt);
  nodesRewriteList(pSelect->pPartitionByList, doRewriteExpr, &cxt);
  nodesRewriteNode(&(pSelect->pWindow), doRewriteExpr, &cxt);
  nodesRewriteList(pSelect->pGroupByList, doRewriteExpr, &cxt);
  nodesRewriteNode(&(pSelect->pHaving), doRewriteExpr, &cxt);
  nodesRewriteList(pSelect->pProjectionList, doRewriteExpr, &cxt);
  nodesRewriteList(pSelect->pOrderByList, doRewriteExpr, &cxt);
  return TSDB_CODE_SUCCESS;
}

static SLogicNode* pushLogicNode(SLogicNode* pRoot, SLogicNode* pNode) {
  if (NULL == pRoot) {
    return pNode;
  }
  if (NULL == pNode) {
    return pRoot;
  }
  pRoot->pParent = pNode;
  if (NULL == pNode->pChildren) {
    pNode->pChildren = nodesMakeList();
  }
  nodesListAppend(pNode->pChildren, (SNode*)pRoot);
  return pNode;
}

static SNodeList* createScanTargets(SNodeList* pCols) {
  
}

static SLogicNode* createScanLogicNode(SSelectStmt* pSelect, SRealTableNode* pRealTable) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  SNodeList* pCols = collectColumns(pSelect);
  pScan->pScanCols = nodesCloneList(pCols);
  // 
  rewriteExpr(pScan->pScanCols, pSelect);
  pScan->node.pTargets = createScanTargets(pCols);
  pScan->pMeta = pRealTable->pMeta;
  return (SLogicNode*)pScan;
}

static SLogicNode* createSubqueryLogicNode(SSelectStmt* pSelect, STempTableNode* pTable) {
  return createQueryLogicNode(pTable->pSubquery);
}

static SLogicNode* createLogicNodeByTable(SSelectStmt* pSelect, SNode* pTable) {
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      return createScanLogicNode(pSelect, (SRealTableNode*)pTable);
    case QUERY_NODE_TEMP_TABLE:
      return createSubqueryLogicNode(pSelect, (STempTableNode*)pTable);
    case QUERY_NODE_JOIN_TABLE:
    default:
      break;
  }
  return NULL;
}

static SLogicNode* createFilterLogicNode(SNode* pWhere) {
  if (NULL == pWhere) {
    return NULL;
  }
  SFilterLogicNode* pFilter = (SFilterLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILTER);
  pFilter->node.pConditions = nodesCloneNode(pWhere);
  return (SLogicNode*)pFilter;
}

static SLogicNode* createAggLogicNode(SSelectStmt* pSelect, SNodeList* pGroupByList, SNode* pHaving) {
  SNodeList* pAggFuncs = collectAggFuncs(pSelect);
  if (NULL == pAggFuncs && NULL == pGroupByList) {
    return NULL;
  }
  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  pAgg->pGroupKeys = nodesCloneList(pGroupByList);
  pAgg->pAggFuncs = nodesCloneList(pAggFuncs);
  pAgg->node.pConditions = nodesCloneNode(pHaving);
  return (SLogicNode*)pAgg;
}

static SLogicNode* createSelectLogicNode(SSelectStmt* pSelect) {
  SLogicNode* pRoot = createLogicNodeByTable(pSelect, pSelect->pFromTable);
  pRoot = pushLogicNode(pRoot, createFilterLogicNode(pSelect->pWhere));
  pRoot = pushLogicNode(pRoot, createAggLogicNode(pSelect, pSelect->pGroupByList, pSelect->pHaving));
  // pRoot = pushLogicNode(pRoot, createProjectLogicNode(pSelect, pSelect->pProjectionList));
  return pRoot;
}

static SLogicNode* createQueryLogicNode(SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return createSelectLogicNode((SSelectStmt*)pStmt);    
    default:
      break;
  }
}

int32_t createLogicPlan(SNode* pNode, SLogicNode** pLogicNode) {
  *pLogicNode = createQueryLogicNode(pNode);
  return TSDB_CODE_SUCCESS;
}
