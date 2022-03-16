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

typedef struct SLogicPlanContext {
  int32_t errCode;
  int32_t planNodeId;
  int32_t acctId;
} SLogicPlanContext;

static SLogicNode* createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt);
static SLogicNode* createLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable);

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
        if (QUERY_NODE_GROUPING_SET == nodeType(pExpr)) {
          pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
        }
        if (nodesEqualNode(pExpr, *pNode)) {
          SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
          CHECK_ALLOC(pCol, DEAL_RES_ERROR);
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

static SLogicNode* pushLogicNode(SLogicPlanContext* pCxt, SLogicNode* pRoot, SLogicNode* pNode) {
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

static SLogicNode* createScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  CHECK_ALLOC(pScan, NULL);
  pScan->node.id = pCxt->planNodeId++;

  TSWAP(pScan->pMeta, pRealTable->pMeta, STableMeta*);
  TSWAP(pScan->pVgroupList, pRealTable->pVgroupList, SVgroupsInfo*);

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

  pScan->scanType = SCAN_TYPE_TABLE;
  pScan->scanFlag = MAIN_SCAN;
  pScan->scanRange = TSWINDOW_INITIALIZER;  
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->acctId;
  strcpy(pScan->tableName.dbname, pRealTable->table.dbName);
  strcpy(pScan->tableName.tname, pRealTable->table.tableName);

  return (SLogicNode*)pScan;
}

static SLogicNode* createSubqueryLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable) {
  SLogicNode* pRoot = createQueryLogicNode(pCxt, pTable->pSubquery);
  CHECK_ALLOC(pRoot, NULL);
  SNode* pNode;
  FOREACH(pNode, pRoot->pTargets) {
    strcpy(((SColumnNode*)pNode)->tableAlias, pTable->table.tableAlias);
  }
  return pRoot;
}

static SLogicNode* createJoinLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SJoinTableNode* pJoinTable) {
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

static SLogicNode* createLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable) {
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

typedef struct SCreateColumnCxt {
  int32_t errCode;
  SNodeList* pList;
} SCreateColumnCxt;

static EDealRes doCreateColumn(SNode* pNode, void* pContext) {
  SCreateColumnCxt* pCxt = (SCreateColumnCxt*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SNode* pCol = nodesCloneNode(pNode);
      CHECK_ALLOC(pCol, DEAL_RES_ERROR);
      CHECK_CODE(nodesListAppend(pCxt->pList, pCol), DEAL_RES_ERROR);
      return DEAL_RES_IGNORE_CHILD;
    }
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SExprNode* pExpr = (SExprNode*)pNode;
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      CHECK_ALLOC(pCol, DEAL_RES_ERROR);
      pCol->node.resType = pExpr->resType;
      strcpy(pCol->colName, pExpr->aliasName);
      CHECK_CODE(nodesListAppend(pCxt->pList, (SNode*)pCol), DEAL_RES_ERROR);
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static SNodeList* createColumnByRewriteExps(SLogicPlanContext* pCxt, SNodeList* pExprs) {
  SCreateColumnCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pList = nodesMakeList() };
  CHECK_ALLOC(cxt.pList, NULL);

  nodesWalkList(pExprs, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return NULL;
  }
  return cxt.pList;
}

static SLogicNode* createAggLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
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

static SLogicNode* createWindowLogicNodeByInterval(SLogicPlanContext* pCxt, SIntervalWindowNode* pInterval, SSelectStmt* pSelect) {
  SWindowLogicNode* pWindow = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  CHECK_ALLOC(pWindow, NULL);
  pWindow->node.id = pCxt->planNodeId++;

  pWindow->winType = WINDOW_TYPE_INTERVAL;
  SValueNode* pIntervalNode = (SValueNode*)((SRawExprNode*)(pInterval->pInterval))->pNode;

  pWindow->interval = pIntervalNode->datum.i;
  pWindow->offset = (NULL != pInterval->pOffset ? ((SValueNode*)pInterval->pOffset)->datum.i : 0);
  pWindow->sliding = (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->datum.i : pWindow->interval);

  if (NULL != pInterval->pFill) {
    pWindow->pFill = nodesCloneNode(pInterval->pFill);
    CHECK_ALLOC(pWindow->pFill, (SLogicNode*)pWindow);
  }

  SNodeList* pFuncs = NULL;
  CHECK_CODE(nodesCollectFuncs(pSelect, fmIsAggFunc, &pFuncs), NULL);
  if (NULL != pFuncs) {
    pWindow->pFuncs = nodesCloneList(pFuncs);
    CHECK_ALLOC(pWindow->pFuncs, (SLogicNode*)pWindow);
  }

  CHECK_CODE(rewriteExpr(pWindow->node.id, 1, pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW), (SLogicNode*)pWindow);

  pWindow->node.pTargets = createColumnByRewriteExps(pCxt, pWindow->pFuncs);
  CHECK_ALLOC(pWindow->node.pTargets, (SLogicNode*)pWindow);

  return (SLogicNode*)pWindow;
}

static SLogicNode* createWindowLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow) {
    return NULL;
  }

  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_INTERVAL_WINDOW:
      return createWindowLogicNodeByInterval(pCxt, (SIntervalWindowNode*)pSelect->pWindow, pSelect);    
    default:
      break;
  }

  return NULL;
}

static SNodeList* createColumnByProjections(SLogicPlanContext* pCxt, SNodeList* pExprs) {
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

static SLogicNode* createProjectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  CHECK_ALLOC(pProject, NULL);
  pProject->node.id = pCxt->planNodeId++;

  pProject->pProjections = nodesCloneList(pSelect->pProjectionList);

  pProject->node.pTargets = createColumnByProjections(pCxt,pSelect->pProjectionList);
  CHECK_ALLOC(pProject->node.pTargets, (SLogicNode*)pProject);

  return (SLogicNode*)pProject;
}

static SLogicNode* createSelectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  SLogicNode* pRoot = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == pCxt->errCode && NULL != pSelect->pWhere) {
    pRoot->pConditions = nodesCloneNode(pSelect->pWhere);
    CHECK_ALLOC(pRoot->pConditions, pRoot);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createWindowLogicNode(pCxt, pSelect));
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createAggLogicNode(pCxt, pSelect));
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pRoot = pushLogicNode(pCxt, pRoot, createProjectLogicNode(pCxt, pSelect));
  }
  return pRoot;
}

static int32_t getMsgType(ENodeType sqlType) {
  return (QUERY_NODE_CREATE_TABLE_STMT == sqlType || QUERY_NODE_CREATE_MULTI_TABLE_STMT == sqlType) ? TDMT_VND_CREATE_TABLE : TDMT_VND_SUBMIT;
}

static SLogicNode* createVnodeModifLogicNode(SLogicPlanContext* pCxt, SVnodeModifOpStmt* pStmt) {
  SVnodeModifLogicNode* pModif = (SVnodeModifLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIF);
  CHECK_ALLOC(pModif, NULL);
  pModif->pDataBlocks = pStmt->pDataBlocks;
  pModif->msgType = getMsgType(pStmt->sqlNodeType);
  return (SLogicNode*)pModif;
}

static SLogicNode* createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return createSelectLogicNode(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_VNODE_MODIF_STMT:
      return createVnodeModifLogicNode(pCxt, (SVnodeModifOpStmt*)pStmt);
    default:
      break;
  }
  return NULL; // to avoid compiler error
}

int32_t createLogicPlan(SPlanContext* pCxt, SLogicNode** pLogicNode) {
  SLogicPlanContext cxt = { .errCode = TSDB_CODE_SUCCESS, .planNodeId = 1, .acctId = pCxt->acctId };
  SLogicNode* pRoot = createQueryLogicNode(&cxt, pCxt->pAstRoot);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode((SNode*)pRoot);
    return cxt.errCode;
  }
  *pLogicNode = pRoot;
  return TSDB_CODE_SUCCESS;
}
