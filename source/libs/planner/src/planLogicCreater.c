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
  SPlanContext* pPlanCxt;
} SLogicPlanContext;

typedef int32_t (*FCreateLogicNode)(SLogicPlanContext*, SSelectStmt*, SLogicNode**);

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable, SLogicNode** pLogicNode);
static int32_t createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt, SLogicNode** pLogicNode);

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
  int32_t rewriteId;
} SNameExprCxt;

static EDealRes doNameExpr(SNode* pNode, void* pContext) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SNameExprCxt* pCxt = (SNameExprCxt*)pContext;
      sprintf(((SExprNode*)pNode)->aliasName, "#expr_%d", pCxt->rewriteId++);
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t rewriteExpr(SNodeList* pExprs, SSelectStmt* pSelect, ESqlClause clause) {
  static int32_t rewriteId = 1;
  SNameExprCxt nameCxt = { .rewriteId = rewriteId };
  nodesWalkList(pExprs, doNameExpr, &nameCxt);
  SRewriteExprCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs };
  nodesRewriteSelectStmt(pSelect, clause, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t pushLogicNode(SLogicPlanContext* pCxt, SLogicNode** pOldRoot, SLogicNode* pNewRoot) {
  if (NULL == pNewRoot->pChildren) {
    pNewRoot->pChildren = nodesMakeList();
    if (NULL == pNewRoot->pChildren) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS != nodesListAppend(pNewRoot->pChildren, (SNode*)*pOldRoot)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pOldRoot)->pParent = pNewRoot;
  *pOldRoot = pNewRoot;

  return TSDB_CODE_SUCCESS;
}

static int32_t createChildLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, FCreateLogicNode func, SLogicNode** pRoot) {
  SLogicNode* pNode = NULL;
  int32_t code = func(pCxt, pSelect, &pNode);
  if (TSDB_CODE_SUCCESS == code && NULL != pNode) {
    code = pushLogicNode(pCxt, pRoot, pNode);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
}

static int32_t createScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable, SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  if (NULL == pScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pScan->pMeta, pRealTable->pMeta, STableMeta*);
  TSWAP(pScan->pVgroupList, pRealTable->pVgroupList, SVgroupsInfo*);
  pScan->scanType = pCxt->pPlanCxt->topicQuery ? SCAN_TYPE_TOPIC : SCAN_TYPE_TABLE;
  pScan->scanFlag = MAIN_SCAN;
  pScan->scanRange = TSWINDOW_INITIALIZER;  
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->pPlanCxt->acctId;
  strcpy(pScan->tableName.dbname, pRealTable->table.dbName);
  strcpy(pScan->tableName.tname, pRealTable->table.tableName);

  // set columns to scan
  SNodeList* pCols = NULL;
  int32_t code = nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, &pCols);
  if (TSDB_CODE_SUCCESS == code && NULL != pCols) {
    pScan->pScanCols = nodesCloneList(pCols);
    if (NULL == pScan) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set output
  if (TSDB_CODE_SUCCESS == code && NULL != pCols) {
    pScan->node.pTargets = nodesCloneList(pCols);
    if (NULL == pScan) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pScan;
  } else {
    nodesDestroyNode(pScan);
  }

  return code;
}

static int32_t createSubqueryLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable, SLogicNode** pLogicNode) {
  int32_t code = createQueryLogicNode(pCxt, pTable->pSubquery, pLogicNode);
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNode;
    FOREACH(pNode, (*pLogicNode)->pTargets) {
      strcpy(((SColumnNode*)pNode)->tableAlias, pTable->table.tableAlias);
    }
  }
  return code;
}

static int32_t createJoinLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SJoinTableNode* pJoinTable, SLogicNode** pLogicNode) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN);
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->joinType = pJoinTable->joinType;

  int32_t code = TSDB_CODE_SUCCESS;

  // set left and right node
  pJoin->node.pChildren = nodesMakeList();
  if (NULL == pJoin->node.pChildren) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  SLogicNode* pLeft = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = doCreateLogicNodeByTable(pCxt, pSelect, pJoinTable->pLeft, &pLeft);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->node.pChildren, (SNode*)pLeft);
    }
  }

  SLogicNode* pRight = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = doCreateLogicNodeByTable(pCxt, pSelect, pJoinTable->pRight, &pRight);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pJoin->node.pChildren, (SNode*)pRight);
    }
  }

  // set on conditions
  if (TSDB_CODE_SUCCESS == code && NULL != pJoinTable->pOnCond) {
    pJoin->pOnConditions = nodesCloneNode(pJoinTable->pOnCond);
    if (NULL == pJoin->pOnConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    pJoin->node.pTargets = nodesCloneList(pLeft->pTargets);
    if (NULL == pJoin->pOnConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppendList(pJoin->node.pTargets, nodesCloneList(pRight->pTargets));
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pJoin;
  } else {
    nodesDestroyNode(pJoin);
  }

  return code;
}

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable, SLogicNode** pLogicNode) {
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      return createScanLogicNode(pCxt, pSelect, (SRealTableNode*)pTable, pLogicNode);
    case QUERY_NODE_TEMP_TABLE:
      return createSubqueryLogicNode(pCxt, pSelect, (STempTableNode*)pTable, pLogicNode);
    case QUERY_NODE_JOIN_TABLE:
      return createJoinLogicNode(pCxt, pSelect, (SJoinTableNode*)pTable, pLogicNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static int32_t createLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable, SLogicNode** pLogicNode) {
  SLogicNode* pNode = NULL;
  int32_t code = doCreateLogicNodeByTable(pCxt, pSelect, pTable, &pNode);
  if (TSDB_CODE_SUCCESS == code) {
    pNode->pConditions = nodesCloneNode(pSelect->pWhere);
    if (NULL != pSelect->pWhere && NULL == pNode->pConditions) {
      nodesDestroyNode(pNode);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    *pLogicNode = pNode;
  }
  return code;
}

static SColumnNode* createColumnByExpr(SExprNode* pExpr) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = pExpr->resType;
  strcpy(pCol->colName, pExpr->aliasName);
  return pCol;
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

static int32_t createColumnByRewriteExps(SLogicPlanContext* pCxt, SNodeList* pExprs, SNodeList** pList) {
  SCreateColumnCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pList = (NULL == *pList ? nodesMakeList() : *pList) };
  if (NULL == cxt.pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  nodesWalkList(pExprs, doCreateColumn, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pList);
    return cxt.errCode;
  }
  if (NULL == *pList) {
    *pList = cxt.pList;
  }
  return cxt.errCode;
}

static int32_t createAggLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SNodeList* pAggFuncs = NULL;
  int32_t code = nodesCollectFuncs(pSelect, fmIsAggFunc, &pAggFuncs);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (NULL == pAggFuncs && NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // set grouyp keys, agg funcs and having conditions
  if (NULL != pSelect->pGroupByList) {
    pAgg->pGroupKeys = nodesCloneList(pSelect->pGroupByList);
    if (NULL == pAgg->pGroupKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }  
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pAggFuncs) {
    pAgg->pAggFuncs = nodesCloneList(pAggFuncs);
    if (NULL == pAgg->pAggFuncs) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_GROUP_BY);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pAggFuncs, pSelect, SQL_CLAUSE_GROUP_BY);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pAgg->node.pConditions = nodesCloneNode(pSelect->pHaving);
    if (NULL == pAgg->node.pConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pGroupKeys) {
    code = createColumnByRewriteExps(pCxt, pAgg->pGroupKeys, &pAgg->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = createColumnByRewriteExps(pCxt, pAgg->pAggFuncs, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode(pAgg);
  }

  return code;
}

static int32_t createWindowLogicNodeByInterval(SLogicPlanContext* pCxt, SIntervalWindowNode* pInterval, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pWindow->winType = WINDOW_TYPE_INTERVAL;
  pWindow->interval = ((SValueNode*)pInterval->pInterval)->datum.i;
  pWindow->intervalUnit = ((SValueNode*)pInterval->pInterval)->unit;
  pWindow->offset = (NULL != pInterval->pOffset ? ((SValueNode*)pInterval->pOffset)->datum.i : 0);
  pWindow->sliding = (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->datum.i : pWindow->interval);
  pWindow->slidingUnit = (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->unit : pWindow->intervalUnit);

  int32_t code = TSDB_CODE_SUCCESS;

  if (NULL != pInterval->pFill) {
    pWindow->pFill = nodesCloneNode(pInterval->pFill);
    if (NULL == pWindow->pFill) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, fmIsAggFunc, &pWindow->pFuncs);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExps(pCxt, pWindow->pFuncs, &pWindow->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pWindow;
  } else {
    nodesDestroyNode(pWindow);
  }

  return code;
}

static int32_t createWindowLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_INTERVAL_WINDOW:
      return createWindowLogicNodeByInterval(pCxt, (SIntervalWindowNode*)pSelect->pWindow, pSelect, pLogicNode);    
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createColumnByProjections(SLogicPlanContext* pCxt, SNodeList* pExprs, SNodeList** pCols) {
  SNodeList* pList = nodesMakeList();
  if (NULL == pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pNode;
  FOREACH(pNode, pExprs) {    
    if (TSDB_CODE_SUCCESS != nodesListAppend(pList, createColumnByExpr((SExprNode*)pNode))) {
      nodesDestroyList(pList);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  *pCols = pList;
  return TSDB_CODE_SUCCESS;
}

static int32_t createProjectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pProject->pProjections = nodesCloneList(pSelect->pProjectionList);
  if (NULL == pProject->pProjections) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByProjections(pCxt,pSelect->pProjectionList, &pProject->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pProject;
  } else {
    nodesDestroyNode(pProject);
  }

  return code;
}

static int32_t createSelectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SLogicNode* pRoot = NULL;
  int32_t code = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = createChildLogicNode(pCxt, pSelect, createWindowLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createChildLogicNode(pCxt, pSelect, createAggLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createChildLogicNode(pCxt, pSelect, createProjectLogicNode, &pRoot);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = pRoot;
  } else {
    nodesDestroyNode(pRoot);
  }

  return code;
}

static int32_t getMsgType(ENodeType sqlType) {
  return (QUERY_NODE_CREATE_TABLE_STMT == sqlType || QUERY_NODE_CREATE_MULTI_TABLE_STMT == sqlType) ? TDMT_VND_CREATE_TABLE : TDMT_VND_SUBMIT;
}

static int32_t createVnodeModifLogicNode(SLogicPlanContext* pCxt, SVnodeModifOpStmt* pStmt, SLogicNode** pLogicNode) {
  SVnodeModifLogicNode* pModif = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIF);
  if (NULL == pModif) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pModif->pDataBlocks = pStmt->pDataBlocks;
  pModif->msgType = getMsgType(pStmt->sqlNodeType);
  *pLogicNode = (SLogicNode*)pModif;
  return TSDB_CODE_SUCCESS;
}

static int32_t createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt, SLogicNode** pLogicNode) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return createSelectLogicNode(pCxt, (SSelectStmt*)pStmt, pLogicNode);
    case QUERY_NODE_VNODE_MODIF_STMT:
      return createVnodeModifLogicNode(pCxt, (SVnodeModifOpStmt*)pStmt, pLogicNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

int32_t createLogicPlan(SPlanContext* pCxt, SLogicNode** pLogicNode) {
  SLogicPlanContext cxt = { .pPlanCxt = pCxt };
  int32_t code = createQueryLogicNode(&cxt, pCxt->pAstRoot, pLogicNode);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  return TSDB_CODE_SUCCESS;
}
