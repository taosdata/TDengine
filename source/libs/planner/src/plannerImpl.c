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

#define CHECK_ALLOC(p, res) \
  do { \
    if (NULL == p) { \
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY; \
      return res; \
    } \
  } while (0)

#define CHECK_CODE(exec, res) \
  do { \
    int32_t code = exec; \
    if (TSDB_CODE_SUCCESS != code) { \
      pCxt->errCode = code; \
      return res; \
    } \
  } while (0)

typedef struct SPlanContext {
  int32_t errCode;
  int32_t planNodeId;
  SNodeList* pResource;
} SPlanContext;

typedef struct SRewriteExprCxt {
  int32_t errCode;
  int32_t planNodeId;
  SNodeList* pTargets;
} SRewriteExprCxt;

static EDealRes doRewriteExpr(SNode** pNode, void* pContext) {
  SRewriteExprCxt* pCxt = (SRewriteExprCxt*)pContext;
  SNode* pTarget;
  int32_t index = 0;
  FOREACH(pTarget, pCxt->pTargets) {
    if (nodesEqualNode(pTarget, *pNode)) {
      SColumnRefNode* pCol = (SColumnRefNode*)nodesMakeNode(QUERY_NODE_COLUMN_REF);
      if (NULL == pCol) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      pCol->tupleId = pCxt->planNodeId;
      pCol->slotId = index;
      nodesDestroyNode(*pNode);
      *pNode = (SNode*)pCol;
      return DEAL_RES_IGNORE_CHILD;
    }
    ++index;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteExpr(int32_t planNodeId, SNodeList* pTargets, SSelectStmt* pSelect, ESqlClause clause) {
  SRewriteExprCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .planNodeId = planNodeId, .pTargets = pTargets };
  nodesRewriteSelectStmt(pSelect, clause, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static SLogicNode* pushLogicNode(SPlanContext* pCxt, SLogicNode* pRoot, SLogicNode* pNode) {
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    goto error;
  }

  if (NULL == pRoot) {
    return pNode;
  }

  if (NULL == pNode) {
    return pRoot;
  }

  if (NULL == pNode->pChildren) {
    pNode->pChildren = nodesMakeList();
    if (NULL == pNode->pChildren) {
      goto error;
    }
  }
  if (TSDB_CODE_SUCCESS != nodesListAppend(pNode->pChildren, (SNode*)pRoot)) {
    goto error;
  }
  pRoot->pParent = pNode;
  return pNode;
error:
  nodesDestroyNode((SNode*)pNode);
  return pRoot;
}

static SNodeList* createScanTargets(int32_t planNodeId, int32_t numOfScanCols) {
  SNodeList* pTargets = nodesMakeList();
  if (NULL == pTargets) {
    return NULL;
  }
  for (int32_t i = 0; i < numOfScanCols; ++i) {
    SColumnRefNode* pCol = (SColumnRefNode*)nodesMakeNode(QUERY_NODE_COLUMN_REF);
    if (NULL == pCol || TSDB_CODE_SUCCESS != nodesListAppend(pTargets, (SNode*)pCol)) {
      nodesDestroyList(pTargets);
      return NULL;
    }
    pCol->tupleId = planNodeId;
    pCol->slotId = i;
  }
  return pTargets;
}

static SLogicNode* createScanLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  CHECK_ALLOC(pScan, NULL);
  pScan->node.id = pCxt->planNodeId++;

  pScan->pMeta = pRealTable->pMeta;

  // set columns to scan
  SNodeList* pCols = NULL;
  CHECK_CODE(nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pScan->pMeta->uid, true, &pCols), (SLogicNode*)pScan);
  pScan->pScanCols = nodesCloneList(pCols);
  CHECK_ALLOC(pScan->pScanCols, (SLogicNode*)pScan);

  // pScanCols of SScanLogicNode is equivalent to pTargets of other logic nodes
  CHECK_CODE(rewriteExpr(pScan->node.id, pScan->pScanCols, pSelect, SQL_CLAUSE_FROM), (SLogicNode*)pScan);

  // set output
  pScan->node.pTargets = createScanTargets(pScan->node.id, LIST_LENGTH(pScan->pScanCols));
  CHECK_ALLOC(pScan->node.pTargets, (SLogicNode*)pScan);

  return (SLogicNode*)pScan;
}

static SLogicNode* createQueryLogicNode(SPlanContext* pCxt, SNode* pStmt);

static SLogicNode* createSubqueryLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable) {
  return createQueryLogicNode(pCxt, pTable->pSubquery);
}

static SLogicNode* createLogicNodeByTable(SPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable) {
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      return createScanLogicNode(pCxt, pSelect, (SRealTableNode*)pTable);
    case QUERY_NODE_TEMP_TABLE:
      return createSubqueryLogicNode(pCxt, pSelect, (STempTableNode*)pTable);
    case QUERY_NODE_JOIN_TABLE:
    default:
      break;
  }
  return NULL;
}

static SLogicNode* createFilterLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, SNode* pWhere) {
  if (NULL == pWhere) {
    return NULL;
  }

  SFilterLogicNode* pFilter = (SFilterLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILTER);
  CHECK_ALLOC(pFilter, NULL);
  pFilter->node.id = pCxt->planNodeId++;

  // set filter conditions
  pFilter->node.pConditions = nodesCloneNode(pWhere);
  CHECK_ALLOC(pFilter->node.pConditions, (SLogicNode*)pFilter);

  // set the output and rewrite the expression in subsequent clauses with the output
  SNodeList* pCols = NULL;
  CHECK_CODE(nodesCollectColumns(pSelect, SQL_CLAUSE_WHERE, 0, false, &pCols), (SLogicNode*)pFilter);
  pFilter->node.pTargets = nodesCloneList(pCols);
  CHECK_ALLOC(pFilter->node.pTargets, (SLogicNode*)pFilter);
  CHECK_CODE(rewriteExpr(pFilter->node.id, pFilter->node.pTargets, pSelect, SQL_CLAUSE_WHERE), (SLogicNode*)pFilter);

  return (SLogicNode*)pFilter;
}

static SLogicNode* createAggLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, SNodeList* pGroupByList, SNode* pHaving) {
  SNodeList* pAggFuncs = NULL;
  CHECK_CODE(nodesCollectFuncs(pSelect, fmIsAggFunc, &pAggFuncs), NULL);
  if (NULL == pAggFuncs && NULL == pGroupByList) {
    return NULL;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  CHECK_ALLOC(pAgg, NULL);
  pAgg->node.id = pCxt->planNodeId++;

  // set grouyp keys, agg funcs and having conditions
  pAgg->pGroupKeys = nodesCloneList(pGroupByList);
  CHECK_ALLOC(pAgg->pGroupKeys, (SLogicNode*)pAgg);
  pAgg->pAggFuncs = nodesCloneList(pAggFuncs);
  CHECK_ALLOC(pAgg->pAggFuncs, (SLogicNode*)pAgg);
  pAgg->node.pConditions = nodesCloneNode(pHaving);
  CHECK_ALLOC(pAgg->node.pConditions, (SLogicNode*)pAgg);

  // set the output and rewrite the expression in subsequent clauses with the output
  SNodeList* pCols = NULL;
  CHECK_CODE(nodesCollectColumns(pSelect, SQL_CLAUSE_HAVING, 0, false, &pCols), (SLogicNode*)pAgg);
  pAgg->node.pTargets = nodesCloneList(pCols);
  CHECK_ALLOC(pAgg->node.pTargets, (SLogicNode*)pAgg);
  CHECK_CODE(rewriteExpr(pAgg->node.id, pAgg->node.pTargets, pSelect, SQL_CLAUSE_HAVING), (SLogicNode*)pAgg);

  return (SLogicNode*)pAgg;
}

static SLogicNode* createSelectLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect) {
  SLogicNode* pRoot = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createFilterLogicNode(pCxt, pSelect, pSelect->pWhere));
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createAggLogicNode(pCxt, pSelect, pSelect->pGroupByList, pSelect->pHaving));
  }
  // pRoot = pushLogicNode(pCxt, pRoot, createProjectLogicNode(pSelect, pSelect->pProjectionList));
  return pRoot;
}

static SLogicNode* createQueryLogicNode(SPlanContext* pCxt, SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return createSelectLogicNode(pCxt, (SSelectStmt*)pStmt);    
    default:
      break;
  }
}

int32_t createLogicPlan(SNode* pNode, SLogicNode** pLogicNode) {
  SPlanContext cxt = { .errCode = TSDB_CODE_SUCCESS, .planNodeId = 0 };
  SLogicNode* pRoot = createQueryLogicNode(&cxt, pNode);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode((SNode*)pRoot);
    return cxt.errCode;
  }
  *pLogicNode = pRoot;
  return TSDB_CODE_SUCCESS;
}
