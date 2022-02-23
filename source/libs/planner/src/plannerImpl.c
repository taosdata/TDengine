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
    if (NULL == (p)) { \
      printf("%s : %d\n", __FUNCTION__, __LINE__); \
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY; \
      return (res); \
    } \
  } while (0)

#define CHECK_CODE(exec, res) \
  do { \
    int32_t code = (exec); \
    if (TSDB_CODE_SUCCESS != code) { \
      printf("%s : %d\n", __FUNCTION__, __LINE__); \
      pCxt->errCode = code; \
      return (res); \
    } \
  } while (0)

typedef struct SPlanContext {
  int32_t errCode;
  int32_t planNodeId;
  SNodeList* pResource;
} SPlanContext;

static SLogicNode* createQueryLogicNode(SPlanContext* pCxt, SNode* pStmt);
static SLogicNode* createLogicNodeByTable(SPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable);

typedef struct SRewriteExprCxt {
  int32_t errCode;
  SNodeList* pExprs;
} SRewriteExprCxt;

static EDealRes doRewriteExpr(SNode** pNode, void* pContext) {
  switch (nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SRewriteExprCxt* pCxt = (SRewriteExprCxt*)pContext;
      SNode* pExpr;
      int32_t index = 0;
      FOREACH(pExpr, pCxt->pExprs) {
        if (nodesEqualNode(pExpr, *pNode)) {
          SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
          if (NULL == pCol) {
            pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
            return DEAL_RES_ERROR;
          }
          SExprNode* pToBeRewrittenExpr = (SExprNode*)(*pNode);
          pCol->node.resType = pToBeRewrittenExpr->resType;
          strcpy(pCol->node.aliasName, pToBeRewrittenExpr->aliasName);
          strcpy(pCol->colName, ((SExprNode*)pExpr)->aliasName);
          nodesDestroyNode(*pNode);
          *pNode = (SNode*)pCol;
          return DEAL_RES_IGNORE_CHILD;
        }
        ++index;
      }
      break;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

typedef struct SNameExprCxt {
  int32_t planNodeId;
  int32_t rewriteId;
} SNameExprCxt;

static EDealRes doNameExpr(SNode* pNode, void* pContext) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SNameExprCxt* pCxt = (SNameExprCxt*)pContext;
      sprintf(((SExprNode*)pNode)->aliasName, "#expr_%d_%d", pCxt->planNodeId, pCxt->rewriteId++);
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t rewriteExpr(int32_t planNodeId, int32_t rewriteId, SNodeList* pExprs, SSelectStmt* pSelect, ESqlClause clause) {
  SNameExprCxt nameCxt = { .planNodeId = planNodeId, .rewriteId = rewriteId };
  nodesWalkList(pExprs, doNameExpr, &nameCxt);
  SRewriteExprCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs };
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

static SLogicNode* createScanLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  CHECK_ALLOC(pScan, NULL);
  pScan->node.id = pCxt->planNodeId++;

  pScan->pMeta = pRealTable->pMeta;

  // set columns to scan
  SNodeList* pCols = NULL;
  CHECK_CODE(nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, &pCols), (SLogicNode*)pScan);
  if (NULL != pCols) {
    pScan->pScanCols = nodesCloneList(pCols);
    CHECK_ALLOC(pScan->pScanCols, (SLogicNode*)pScan);
  }

  // set output
  if (NULL != pCols) {
    pScan->node.pTargets = nodesCloneList(pCols);
    CHECK_ALLOC(pScan->node.pTargets, (SLogicNode*)pScan);
  }

  return (SLogicNode*)pScan;
}

static SLogicNode* createSubqueryLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable) {
  SLogicNode* pRoot = createQueryLogicNode(pCxt, pTable->pSubquery);
  CHECK_ALLOC(pRoot, NULL);
  SNode* pNode;
  FOREACH(pNode, pRoot->pTargets) {
    strcpy(((SColumnNode*)pNode)->tableAlias, pTable->table.tableAlias);
  }
  return pRoot;
}

static SLogicNode* createJoinLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect, SJoinTableNode* pJoinTable) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN);
  CHECK_ALLOC(pJoin, NULL);
  pJoin->node.id = pCxt->planNodeId++;

  pJoin->joinType = pJoinTable->joinType;

  // set left and right node
  pJoin->node.pChildren = nodesMakeList();
  CHECK_ALLOC(pJoin->node.pChildren, (SLogicNode*)pJoin);
  SLogicNode* pLeft = createLogicNodeByTable(pCxt, pSelect, pJoinTable->pLeft);
  CHECK_ALLOC(pLeft, (SLogicNode*)pJoin);
  CHECK_CODE(nodesListAppend(pJoin->node.pChildren, (SNode*)pLeft), (SLogicNode*)pJoin);
  SLogicNode* pRight = createLogicNodeByTable(pCxt, pSelect, pJoinTable->pRight);
  CHECK_ALLOC(pRight, (SLogicNode*)pJoin);
  CHECK_CODE(nodesListAppend(pJoin->node.pChildren, (SNode*)pRight), (SLogicNode*)pJoin);

  // set on conditions
  if (NULL != pJoinTable->pOnCond) {
    pJoin->pOnConditions = nodesCloneNode(pJoinTable->pOnCond);
    CHECK_ALLOC(pJoin->pOnConditions, (SLogicNode*)pJoin);
  }

  // set the output
  pJoin->node.pTargets = nodesCloneList(pLeft->pTargets);
  CHECK_ALLOC(pJoin->node.pTargets, (SLogicNode*)pJoin);
  SNodeList* pTargets = nodesCloneList(pRight->pTargets);
  CHECK_ALLOC(pTargets, (SLogicNode*)pJoin);
  nodesListAppendList(pJoin->node.pTargets, pTargets);

  return (SLogicNode*)pJoin;
}

static SLogicNode* createLogicNodeByTable(SPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable) {
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      return createScanLogicNode(pCxt, pSelect, (SRealTableNode*)pTable);
    case QUERY_NODE_TEMP_TABLE:
      return createSubqueryLogicNode(pCxt, pSelect, (STempTableNode*)pTable);
    case QUERY_NODE_JOIN_TABLE:
      return createJoinLogicNode(pCxt, pSelect, (SJoinTableNode*)pTable);
    default:
      break;
  }
  return NULL;
}

static SLogicNode* createWhereFilterLogicNode(SPlanContext* pCxt, SLogicNode* pChild, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWhere) {
    return NULL;
  }

  SFilterLogicNode* pFilter = (SFilterLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILTER);
  CHECK_ALLOC(pFilter, NULL);
  pFilter->node.id = pCxt->planNodeId++;

  // set filter conditions
  pFilter->node.pConditions = nodesCloneNode(pSelect->pWhere);
  CHECK_ALLOC(pFilter->node.pConditions, (SLogicNode*)pFilter);

  // set the output
  pFilter->node.pTargets = nodesCloneList(pChild->pTargets);
  CHECK_ALLOC(pFilter->node.pTargets, (SLogicNode*)pFilter);

  return (SLogicNode*)pFilter;
}

typedef struct SCreateColumnCxt {
  int32_t errCode;
  SNodeList* pList;
} SCreateColumnCxt;

static EDealRes doCreateColumn(SNode* pNode, void* pContext) {
  SCreateColumnCxt* pCxt = (SCreateColumnCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SNode* pCol = nodesCloneNode(pNode);
      if (NULL == pCol || TSDB_CODE_SUCCESS != nodesListAppend(pCxt->pList, pCol)) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      return DEAL_RES_IGNORE_CHILD;
    }
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SExprNode* pExpr = (SExprNode*)pNode;
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      pCol->node.resType = pExpr->resType;
      strcpy(pCol->colName, pExpr->aliasName);
      if (TSDB_CODE_SUCCESS != nodesListAppend(pCxt->pList, (SNode*)pCol)) {
        pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
        return DEAL_RES_ERROR;
      }
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static SNodeList* createColumnByRewriteExps(SPlanContext* pCxt, SNodeList* pExprs) {
  SCreateColumnCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pList = nodesMakeList() };
  if (NULL == cxt.pList) {
    return NULL;
  }
  nodesWalkList(pExprs, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return NULL;
  }
  return cxt.pList;
}

static SLogicNode* createAggLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect) {
  SNodeList* pAggFuncs = NULL;
  CHECK_CODE(nodesCollectFuncs(pSelect, fmIsAggFunc, &pAggFuncs), NULL);
  if (NULL == pAggFuncs && NULL == pSelect->pGroupByList) {
    return NULL;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  CHECK_ALLOC(pAgg, NULL);
  pAgg->node.id = pCxt->planNodeId++;

  // set grouyp keys, agg funcs and having conditions
  if (NULL != pSelect->pGroupByList) {
    pAgg->pGroupKeys = nodesCloneList(pSelect->pGroupByList);
    CHECK_ALLOC(pAgg->pGroupKeys, (SLogicNode*)pAgg);
  }
  if (NULL != pAggFuncs) {
    pAgg->pAggFuncs = nodesCloneList(pAggFuncs);
    CHECK_ALLOC(pAgg->pAggFuncs, (SLogicNode*)pAgg);
  }

  // rewrite the expression in subsequent clauses
  CHECK_CODE(rewriteExpr(pAgg->node.id, 1, pAgg->pGroupKeys, pSelect, SQL_CLAUSE_GROUP_BY), (SLogicNode*)pAgg);
  CHECK_CODE(rewriteExpr(pAgg->node.id, 1 + LIST_LENGTH(pAgg->pGroupKeys), pAgg->pAggFuncs, pSelect, SQL_CLAUSE_GROUP_BY), (SLogicNode*)pAgg);

  if (NULL != pSelect->pHaving) {
    pAgg->node.pConditions = nodesCloneNode(pSelect->pHaving);
    CHECK_ALLOC(pAgg->node.pConditions, (SLogicNode*)pAgg);
  }

  // set the output
  pAgg->node.pTargets = nodesMakeList();
  CHECK_ALLOC(pAgg->node.pTargets, (SLogicNode*)pAgg);
  if (NULL != pAgg->pGroupKeys) {
    SNodeList* pTargets = createColumnByRewriteExps(pCxt, pAgg->pGroupKeys);
    CHECK_ALLOC(pAgg->node.pTargets, (SLogicNode*)pAgg);
    nodesListAppendList(pAgg->node.pTargets, pTargets);
  }
  if (NULL != pAgg->pAggFuncs) {
    SNodeList* pTargets = createColumnByRewriteExps(pCxt, pAgg->pAggFuncs);
    CHECK_ALLOC(pTargets, (SLogicNode*)pAgg);
    nodesListAppendList(pAgg->node.pTargets, pTargets);
  }
  
  return (SLogicNode*)pAgg;
}

static SNodeList* createColumnByProjections(SPlanContext* pCxt, SNodeList* pExprs) {
  SNodeList* pList = nodesMakeList();
  CHECK_ALLOC(pList, NULL);
  SNode* pNode;
  FOREACH(pNode, pExprs) {
    SExprNode* pExpr = (SExprNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      goto error;
    }
    pCol->node.resType = pExpr->resType;
    strcpy(pCol->colName, pExpr->aliasName);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pList, (SNode*)pCol)) {
      goto error;
    }
  }
  return pList;
error:
  nodesDestroyList(pList);
  return NULL;
}

static SLogicNode* createProjectLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  CHECK_ALLOC(pProject, NULL);
  pProject->node.id = pCxt->planNodeId++;

  pProject->pProjections = nodesCloneList(pSelect->pProjectionList);

  pProject->node.pTargets = createColumnByProjections(pCxt,pSelect->pProjectionList);
  CHECK_ALLOC(pProject->node.pTargets, (SLogicNode*)pProject);

  return (SLogicNode*)pProject;
}

static SLogicNode* createSelectLogicNode(SPlanContext* pCxt, SSelectStmt* pSelect) {
  SLogicNode* pRoot = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createWhereFilterLogicNode(pCxt, pRoot, pSelect));
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createAggLogicNode(pCxt, pSelect));
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createProjectLogicNode(pCxt, pSelect));
  }
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
  SPlanContext cxt = { .errCode = TSDB_CODE_SUCCESS, .planNodeId = 1 };
  SLogicNode* pRoot = createQueryLogicNode(&cxt, pNode);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode((SNode*)pRoot);
    return cxt.errCode;
  }
  *pLogicNode = pRoot;
  return TSDB_CODE_SUCCESS;
}
