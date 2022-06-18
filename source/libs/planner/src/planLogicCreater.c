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

typedef int32_t (*FCreateLogicNode)(SLogicPlanContext*, void*, SLogicNode**);
typedef int32_t (*FCreateSelectLogicNode)(SLogicPlanContext*, SSelectStmt*, SLogicNode**);
typedef int32_t (*FCreateSetOpLogicNode)(SLogicPlanContext*, SSetOperator*, SLogicNode**);
typedef int32_t (*FCreateDeleteLogicNode)(SLogicPlanContext*, SDeleteStmt*, SLogicNode**);

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                        SLogicNode** pLogicNode);
static int32_t createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt, SLogicNode** pLogicNode);

typedef struct SRewriteExprCxt {
  int32_t    errCode;
  SNodeList* pExprs;
} SRewriteExprCxt;

static EDealRes doRewriteExpr(SNode** pNode, void* pContext) {
  switch (nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      SRewriteExprCxt* pCxt = (SRewriteExprCxt*)pContext;
      SNode*           pExpr;
      int32_t          index = 0;
      FOREACH(pExpr, pCxt->pExprs) {
        if (QUERY_NODE_GROUPING_SET == nodeType(pExpr)) {
          pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
        }
        if (nodesEqualNode(pExpr, *pNode)) {
          SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
          if (NULL == pCol) {
            return DEAL_RES_ERROR;
          }
          SExprNode* pToBeRewrittenExpr = (SExprNode*)(*pNode);
          pCol->node.resType = pToBeRewrittenExpr->resType;
          strcpy(pCol->node.aliasName, pToBeRewrittenExpr->aliasName);
          strcpy(pCol->colName, ((SExprNode*)pExpr)->aliasName);
          if (QUERY_NODE_FUNCTION == nodeType(pExpr) && FUNCTION_TYPE_WSTARTTS == ((SFunctionNode*)pExpr)->funcType) {
            pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
          }
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

static EDealRes doNameExpr(SNode* pNode, void* pContext) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION: {
      if ('\0' == ((SExprNode*)pNode)->aliasName[0]) {
        sprintf(((SExprNode*)pNode)->aliasName, "#expr_%p", pNode);
      }
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t rewriteExprForSelect(SNodeList* pExprs, SSelectStmt* pSelect, ESqlClause clause) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs};
  nodesRewriteSelectStmt(pSelect, clause, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t rewriteExpr(SNodeList* pExprs, SNode** pTarget) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs};
  nodesRewriteExpr(pTarget, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t rewriteExprs(SNodeList* pExprs, SNodeList* pTarget) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs};
  nodesRewriteExprs(pTarget, doRewriteExpr, &cxt);
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

static int32_t createRootLogicNode(SLogicPlanContext* pCxt, void* pStmt, uint8_t precision, FCreateLogicNode func,
                                   SLogicNode** pRoot) {
  SLogicNode* pNode = NULL;
  int32_t     code = func(pCxt, pStmt, &pNode);
  if (TSDB_CODE_SUCCESS == code && NULL != pNode) {
    pNode->precision = precision;
    code = pushLogicNode(pCxt, pRoot, pNode);
  }
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pNode);
  }
  return code;
}

static int32_t createSelectRootLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, FCreateSelectLogicNode func,
                                         SLogicNode** pRoot) {
  return createRootLogicNode(pCxt, pSelect, pSelect->precision, (FCreateLogicNode)func, pRoot);
}

static EScanType getScanType(SLogicPlanContext* pCxt, SNodeList* pScanPseudoCols, SNodeList* pScanCols,
                             int8_t tableType) {
  if (pCxt->pPlanCxt->topicQuery || pCxt->pPlanCxt->streamQuery) {
    return SCAN_TYPE_STREAM;
  }

  if (NULL == pScanCols) {
    // select count(*) from t
    return NULL == pScanPseudoCols ? SCAN_TYPE_TABLE : SCAN_TYPE_TAG;
  }

  if (TSDB_SYSTEM_TABLE == tableType) {
    return SCAN_TYPE_SYSTEM_TABLE;
  }

  SNode* pCol = NULL;
  FOREACH(pCol, pScanCols) {
    if (COLUMN_TYPE_COLUMN == ((SColumnNode*)pCol)->colType) {
      return SCAN_TYPE_TABLE;
    }
  }

  return SCAN_TYPE_TAG;
}

static SNode* createPrimaryKeyCol(uint64_t tableId) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pCol->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
  pCol->tableId = tableId;
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pCol->colType = COLUMN_TYPE_COLUMN;
  strcpy(pCol->colName, "#primarykey");
  return (SNode*)pCol;
}

static int32_t addPrimaryKeyCol(uint64_t tableId, SNodeList** pCols) {
  if (NULL == *pCols) {
    *pCols = nodesMakeList();
    if (NULL == *pCols) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  bool   found = false;
  SNode* pCol = NULL;
  FOREACH(pCol, *pCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      found = true;
      break;
    }
  }

  if (!found) {
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(*pCols, createPrimaryKeyCol(tableId))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t makeScanLogicNode(SLogicPlanContext* pCxt, SRealTableNode* pRealTable, bool hasRepeatScanFuncs,
                                 SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = (SScanLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN);
  if (NULL == pScan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TSWAP(pScan->pVgroupList, pRealTable->pVgroupList);
  TSWAP(pScan->pSmaIndexes, pRealTable->pSmaIndexes);
  pScan->tableId = pRealTable->pMeta->uid;
  pScan->stableId = pRealTable->pMeta->suid;
  pScan->tableType = pRealTable->pMeta->tableType;
  pScan->scanSeq[0] = hasRepeatScanFuncs ? 2 : 1;
  pScan->scanSeq[1] = 0;
  pScan->scanRange = TSWINDOW_INITIALIZER;
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->pPlanCxt->acctId;
  strcpy(pScan->tableName.dbname, pRealTable->table.dbName);
  strcpy(pScan->tableName.tname, pRealTable->table.tableName);
  pScan->showRewrite = pCxt->pPlanCxt->showRewrite;
  pScan->ratio = pRealTable->ratio;
  pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;

  *pLogicNode = (SLogicNode*)pScan;

  return TSDB_CODE_SUCCESS;
}

static int32_t createScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable,
                                   SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = makeScanLogicNode(pCxt, pRealTable, pSelect->hasRepeatScanFuncs, (SLogicNode**)&pScan);

  // set columns to scan
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, COLLECT_COL_TYPE_COL,
                               &pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                               &pScan->pScanPseudoCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_FROM, fmIsScanPseudoColumnFunc, &pScan->pScanPseudoCols);
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pScan->pScanPseudoCols, pSelect, SQL_CLAUSE_FROM);
  }

  pScan->scanType = getScanType(pCxt, pScan->pScanPseudoCols, pScan->pScanCols, pScan->tableType);

  if (TSDB_CODE_SUCCESS == code) {
    code = addPrimaryKeyCol(pScan->tableId, &pScan->pScanCols);
  }

  // set output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pScan->pScanPseudoCols, &pScan->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pScan;
  } else {
    nodesDestroyNode((SNode*)pScan);
  }

  return code;
}

static int32_t createSubqueryLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable,
                                       SLogicNode** pLogicNode) {
  return createQueryLogicNode(pCxt, pTable->pSubquery, pLogicNode);
}

static int32_t createJoinLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SJoinTableNode* pJoinTable,
                                   SLogicNode** pLogicNode) {
  SJoinLogicNode* pJoin = (SJoinLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN);
  if (NULL == pJoin) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pJoin->joinType = pJoinTable->joinType;
  pJoin->isSingleTableJoin = pJoinTable->table.singleTable;

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
    if (NULL == pJoin->node.pTargets) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppendList(pJoin->node.pTargets, nodesCloneList(pRight->pTargets));
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                        SLogicNode** pLogicNode) {
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

static int32_t createLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                      SLogicNode** pLogicNode) {
  SLogicNode* pNode = NULL;
  int32_t     code = doCreateLogicNodeByTable(pCxt, pSelect, pTable, &pNode);
  if (TSDB_CODE_SUCCESS == code) {
    pNode->pConditions = nodesCloneNode(pSelect->pWhere);
    if (NULL != pSelect->pWhere && NULL == pNode->pConditions) {
      nodesDestroyNode((SNode*)pNode);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pNode->precision = pSelect->precision;
    *pLogicNode = pNode;
  }
  return code;
}

static SColumnNode* createColumnByExpr(const char* pStmtName, SExprNode* pExpr) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = pExpr->resType;
  strcpy(pCol->colName, pExpr->aliasName);
  if (NULL != pStmtName) {
    strcpy(pCol->tableAlias, pStmtName);
  }
  return pCol;
}

static int32_t createAggLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasAggFuncs && NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  // set grouyp keys, agg funcs and having conditions
  if (NULL != pSelect->pGroupByList) {
    pAgg->pGroupKeys = nodesCloneList(pSelect->pGroupByList);
    if (NULL == pAgg->pGroupKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_GROUP_BY);
  }

  if (TSDB_CODE_SUCCESS == code && pSelect->hasAggFuncs) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, fmIsAggFunc, &pAgg->pAggFuncs);
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pAgg->pAggFuncs, pSelect, SQL_CLAUSE_GROUP_BY);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pAgg->node.pConditions = nodesCloneNode(pSelect->pHaving);
    if (NULL == pAgg->node.pConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pGroupKeys) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }

  return code;
}

static int32_t createIndefRowsFuncLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  // top/bottom are both an aggregate function and a indefinite rows function
  if (!pSelect->hasIndefiniteRowsFunc || pSelect->hasAggFuncs || NULL != pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  SIndefRowsFuncLogicNode* pIdfRowsFunc =
      (SIndefRowsFuncLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC);
  if (NULL == pIdfRowsFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, fmIsVectorFunc, &pIdfRowsFunc->pVectorFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pIdfRowsFunc->pVectorFuncs, pSelect, SQL_CLAUSE_SELECT);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pIdfRowsFunc->pVectorFuncs, &pIdfRowsFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pIdfRowsFunc;
  } else {
    nodesDestroyNode((SNode*)pIdfRowsFunc);
  }

  return code;
}

static int32_t createWindowLogicNodeFinalize(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SWindowLogicNode* pWindow,
                                             SLogicNode** pLogicNode) {
  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_WINDOW, fmIsWindowClauseFunc, &pWindow->pFuncs);

  if (pCxt->pPlanCxt->streamQuery) {
    pWindow->triggerType = pCxt->pPlanCxt->triggerType;
    pWindow->watermark = pCxt->pPlanCxt->watermark;
  }

  if (pCxt->pPlanCxt->rSmaQuery) {
    /*pWindow->filesFactor = pCxt->pPlanCxt->filesFactor;*/
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets);
  }

  pSelect->hasAggFuncs = false;

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pWindow;
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  return code;
}

static int32_t createWindowLogicNodeByState(SLogicPlanContext* pCxt, SStateWindowNode* pState, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pWindow->winType = WINDOW_TYPE_STATE;
  pWindow->pStateExpr = nodesCloneNode(pState->pExpr);

  pWindow->pTspk = nodesCloneNode(pState->pCol);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeBySession(SLogicPlanContext* pCxt, SSessionWindowNode* pSession,
                                              SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pWindow->winType = WINDOW_TYPE_SESSION;
  pWindow->sessionGap = ((SValueNode*)pSession->pGap)->datum.i;
  pWindow->windowAlgo = pCxt->pPlanCxt->streamQuery ? SESSION_ALGO_STREAM_SINGLE : SESSION_ALGO_MERGE;

  pWindow->pTspk = nodesCloneNode((SNode*)pSession->pCol);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByInterval(SLogicPlanContext* pCxt, SIntervalWindowNode* pInterval,
                                               SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pWindow->winType = WINDOW_TYPE_INTERVAL;
  pWindow->interval = ((SValueNode*)pInterval->pInterval)->datum.i;
  pWindow->intervalUnit = ((SValueNode*)pInterval->pInterval)->unit;
  pWindow->offset = (NULL != pInterval->pOffset ? ((SValueNode*)pInterval->pOffset)->datum.i : 0);
  pWindow->sliding = (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->datum.i : pWindow->interval);
  pWindow->slidingUnit =
      (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->unit : pWindow->intervalUnit);
  pWindow->windowAlgo = pCxt->pPlanCxt->streamQuery ? INTERVAL_ALGO_STREAM_SINGLE : INTERVAL_ALGO_HASH;

  pWindow->pTspk = nodesCloneNode(pInterval->pCol);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_STATE_WINDOW:
      return createWindowLogicNodeByState(pCxt, (SStateWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    case QUERY_NODE_SESSION_WINDOW:
      return createWindowLogicNodeBySession(pCxt, (SSessionWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    case QUERY_NODE_INTERVAL_WINDOW:
      return createWindowLogicNodeByInterval(pCxt, (SIntervalWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t createFillLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pWindow || QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow) ||
      NULL == ((SIntervalWindowNode*)pSelect->pWindow)->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  SFillNode* pFillNode = (SFillNode*)(((SIntervalWindowNode*)pSelect->pWindow)->pFill);

  SFillLogicNode* pFill = (SFillLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILL);
  if (NULL == pFill) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = nodesCollectColumns(pSelect, SQL_CLAUSE_WINDOW, NULL, COLLECT_COL_TYPE_ALL, &pFill->node.pTargets);

  pFill->mode = pFillNode->mode;
  pFill->timeRange = pFillNode->timeRange;
  pFill->pValues = nodesCloneNode(pFillNode->pValues);
  pFill->pWStartTs = nodesCloneNode(pFillNode->pWStartTs);
  if ((NULL != pFillNode->pValues && NULL == pFill->pValues) || NULL == pFill->pWStartTs) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pFill;
  } else {
    nodesDestroyNode((SNode*)pFill);
  }

  return code;
}

static int32_t createSortLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pOrderByList) {
    return TSDB_CODE_SUCCESS;
  }

  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = nodesCollectColumns(pSelect, SQL_CLAUSE_ORDER_BY, NULL, COLLECT_COL_TYPE_ALL, &pSort->node.pTargets);

  if (TSDB_CODE_SUCCESS == code) {
    pSort->pSortKeys = nodesCloneList(pSelect->pOrderByList);
    if (NULL == pSort->pSortKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pSort;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }

  return code;
}

static int32_t createColumnByProjections(SLogicPlanContext* pCxt, const char* pStmtName, SNodeList* pExprs,
                                         SNodeList** pCols) {
  SNodeList* pList = nodesMakeList();
  if (NULL == pList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode* pNode;
  FOREACH(pNode, pExprs) {
    if (TSDB_CODE_SUCCESS != nodesListAppend(pList, (SNode*)createColumnByExpr(pStmtName, (SExprNode*)pNode))) {
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

  if (NULL != pSelect->pLimit) {
    pProject->limit = pSelect->pLimit->limit;
    pProject->offset = pSelect->pLimit->offset;
  } else {
    pProject->limit = -1;
    pProject->offset = -1;
  }

  if (NULL != pSelect->pSlimit) {
    pProject->slimit = ((SLimitNode*)pSelect->pSlimit)->limit;
    pProject->soffset = ((SLimitNode*)pSelect->pSlimit)->offset;
  } else {
    pProject->slimit = -1;
    pProject->soffset = -1;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pProject->pProjections = nodesCloneList(pSelect->pProjectionList);
  if (NULL == pProject->pProjections) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pProject->stmtName, pSelect->stmtName);

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByProjections(pCxt, pSelect->stmtName, pSelect->pProjectionList, &pProject->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pProject;
  } else {
    nodesDestroyNode((SNode*)pProject);
  }

  return code;
}

static int32_t createPartitionLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pPartitionByList) {
    return TSDB_CODE_SUCCESS;
  }

  SPartitionLogicNode* pPartition = (SPartitionLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION);
  if (NULL == pPartition) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code =
      nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, COLLECT_COL_TYPE_ALL, &pPartition->node.pTargets);

  if (TSDB_CODE_SUCCESS == code) {
    pPartition->pPartitionKeys = nodesCloneList(pSelect->pPartitionByList);
    if (NULL == pPartition->pPartitionKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pPartition;
  } else {
    nodesDestroyNode((SNode*)pPartition);
  }

  return code;
}

static int32_t createDistinctLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->isDistinct) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  // set grouyp keys, agg funcs and having conditions
  pAgg->pGroupKeys = nodesCloneList(pSelect->pProjectionList);
  if (NULL == pAgg->pGroupKeys) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprForSelect(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_DISTINCT);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }

  return code;
}

static int32_t createSelectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SLogicNode* pRoot = NULL;
  int32_t     code = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createPartitionLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createWindowLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createFillLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createAggLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createIndefRowsFuncLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createDistinctLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createSortLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createProjectLogicNode, &pRoot);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = pRoot;
  } else {
    nodesDestroyNode((SNode*)pRoot);
  }

  return code;
}

static int32_t createSetOpRootLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator, FCreateSetOpLogicNode func,
                                        SLogicNode** pRoot) {
  return createRootLogicNode(pCxt, pSetOperator, pSetOperator->precision, (FCreateLogicNode)func, pRoot);
}

static int32_t createSetOpSortLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator, SLogicNode** pLogicNode) {
  if (NULL == pSetOperator->pOrderByList) {
    return TSDB_CODE_SUCCESS;
  }

  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pSort->node.pTargets = nodesCloneList(pSetOperator->pProjectionList);
  if (NULL == pSort->node.pTargets) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    pSort->pSortKeys = nodesCloneList(pSetOperator->pOrderByList);
    if (NULL == pSort->pSortKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pSort;
  } else {
    nodesDestroyNode((SNode*)pSort);
  }

  return code;
}

static int32_t createSetOpProjectLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator,
                                           SLogicNode** pLogicNode) {
  SProjectLogicNode* pProject = (SProjectLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT);
  if (NULL == pProject) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (NULL != pSetOperator->pLimit) {
    pProject->limit = ((SLimitNode*)pSetOperator->pLimit)->limit;
    pProject->offset = ((SLimitNode*)pSetOperator->pLimit)->offset;
  } else {
    pProject->limit = -1;
    pProject->offset = -1;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pProject->pProjections = nodesCloneList(pSetOperator->pProjectionList);
  if (NULL == pProject->pProjections) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByProjections(pCxt, pSetOperator->stmtName, pSetOperator->pProjectionList,
                                     &pProject->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pProject;
  } else {
    nodesDestroyNode((SNode*)pProject);
  }

  return code;
}

static int32_t createSetOpAggLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator, SLogicNode** pLogicNode) {
  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  pAgg->pGroupKeys = nodesCloneList(pSetOperator->pProjectionList);
  if (NULL == pAgg->pGroupKeys) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprs(pAgg->pGroupKeys, pSetOperator->pOrderByList);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }

  return code;
}

static int32_t createSetOpLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator, SLogicNode** pLogicNode) {
  SLogicNode* pSetOp = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  switch (pSetOperator->opType) {
    case SET_OP_TYPE_UNION_ALL:
      code = createSetOpProjectLogicNode(pCxt, pSetOperator, &pSetOp);
      break;
    case SET_OP_TYPE_UNION:
      code = createSetOpAggLogicNode(pCxt, pSetOperator, &pSetOp);
      break;
    default:
      code = TSDB_CODE_FAILED;
      break;
  }

  SLogicNode* pLeft = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = createQueryLogicNode(pCxt, pSetOperator->pLeft, &pLeft);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pSetOp->pChildren, (SNode*)pLeft);
  }
  SLogicNode* pRight = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = createQueryLogicNode(pCxt, pSetOperator->pRight, &pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pSetOp->pChildren, (SNode*)pRight);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pSetOp;
  } else {
    nodesDestroyNode((SNode*)pSetOp);
  }

  return code;
}

static int32_t createSetOperatorLogicNode(SLogicPlanContext* pCxt, SSetOperator* pSetOperator,
                                          SLogicNode** pLogicNode) {
  SLogicNode* pRoot = NULL;
  int32_t     code = createSetOpLogicNode(pCxt, pSetOperator, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = createSetOpRootLogicNode(pCxt, pSetOperator, createSetOpSortLogicNode, &pRoot);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = pRoot;
  } else {
    nodesDestroyNode((SNode*)pRoot);
  }

  return code;
}

static int32_t getMsgType(ENodeType sqlType) {
  switch (sqlType) {
    case QUERY_NODE_CREATE_TABLE_STMT:
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      return TDMT_VND_CREATE_TABLE;
    case QUERY_NODE_DROP_TABLE_STMT:
      return TDMT_VND_DROP_TABLE;
    case QUERY_NODE_ALTER_TABLE_STMT:
      return TDMT_VND_ALTER_TABLE;
    default:
      break;
  }
  return TDMT_VND_SUBMIT;
}

static int32_t createVnodeModifLogicNode(SLogicPlanContext* pCxt, SVnodeModifOpStmt* pStmt, SLogicNode** pLogicNode) {
  SVnodeModifyLogicNode* pModif = (SVnodeModifyLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY);
  if (NULL == pModif) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pModif->modifyType = MODIFY_TABLE_TYPE_INSERT;
  TSWAP(pModif->pDataBlocks, pStmt->pDataBlocks);
  pModif->msgType = getMsgType(pStmt->sqlNodeType);
  *pLogicNode = (SLogicNode*)pModif;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDeleteRootLogicNode(SLogicPlanContext* pCxt, SDeleteStmt* pDelete, FCreateDeleteLogicNode func,
                                         SLogicNode** pRoot) {
  return createRootLogicNode(pCxt, pDelete, pDelete->precision, (FCreateLogicNode)func, pRoot);
}

static int32_t createDeleteScanLogicNode(SLogicPlanContext* pCxt, SDeleteStmt* pDelete, SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = makeScanLogicNode(pCxt, (SRealTableNode*)pDelete->pFromTable, false, (SLogicNode**)&pScan);

  // set columns to scan
  if (TSDB_CODE_SUCCESS == code) {
    pScan->scanType = SCAN_TYPE_TABLE;
    pScan->pScanCols = nodesCloneList(((SFunctionNode*)pDelete->pCountFunc)->pParameterList);
    if (NULL == pScan->pScanCols) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pDelete->pTagIndexCond) {
    pScan->pTagCond = nodesCloneNode(pDelete->pTagIndexCond);
    if (NULL == pScan->pTagCond) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pScan;
  } else {
    nodesDestroyNode((SNode*)pScan);
  }

  return code;
}

static int32_t createDeleteAggLogicNode(SLogicPlanContext* pCxt, SDeleteStmt* pDelete, SLogicNode** pLogicNode) {
  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, nodesCloneNode(pDelete->pCountFunc));
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pAggFuncs, &pDelete->pCountFunc);
  }
  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pAgg;
  } else {
    nodesDestroyNode((SNode*)pAgg);
  }

  return code;
}

static int32_t createVnodeModifLogicNodeByDelete(SLogicPlanContext* pCxt, SDeleteStmt* pDelete,
                                                 SLogicNode** pLogicNode) {
  SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY);
  if (NULL == pModify) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SRealTableNode* pRealTable = (SRealTableNode*)pDelete->pFromTable;

  pModify->modifyType = MODIFY_TABLE_TYPE_DELETE;
  pModify->tableId = pRealTable->pMeta->uid;
  pModify->tableType = pRealTable->pMeta->tableType;
  snprintf(pModify->tableFName, sizeof(pModify->tableFName), "%d.%s.%s", pCxt->pPlanCxt->acctId,
           pRealTable->table.dbName, pRealTable->table.tableName);
  pModify->deleteTimeRange = pDelete->timeRange;
  pModify->pAffectedRows = nodesCloneNode(pDelete->pCountFunc);
  if (NULL == pModify->pAffectedRows) {
    nodesDestroyNode((SNode*)pModify);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pLogicNode = (SLogicNode*)pModify;
  return TSDB_CODE_SUCCESS;
}

static int32_t createDeleteLogicNode(SLogicPlanContext* pCxt, SDeleteStmt* pDelete, SLogicNode** pLogicNode) {
  SLogicNode* pRoot = NULL;
  int32_t     code = createDeleteScanLogicNode(pCxt, pDelete, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = createDeleteRootLogicNode(pCxt, pDelete, createDeleteAggLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createDeleteRootLogicNode(pCxt, pDelete, createVnodeModifLogicNodeByDelete, &pRoot);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = pRoot;
  } else {
    nodesDestroyNode((SNode*)pRoot);
  }

  return code;
}

static int32_t createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt, SLogicNode** pLogicNode) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SELECT_STMT:
      return createSelectLogicNode(pCxt, (SSelectStmt*)pStmt, pLogicNode);
    case QUERY_NODE_VNODE_MODIF_STMT:
      return createVnodeModifLogicNode(pCxt, (SVnodeModifOpStmt*)pStmt, pLogicNode);
    case QUERY_NODE_EXPLAIN_STMT:
      return createQueryLogicNode(pCxt, ((SExplainStmt*)pStmt)->pQuery, pLogicNode);
    case QUERY_NODE_SET_OPERATOR:
      return createSetOperatorLogicNode(pCxt, (SSetOperator*)pStmt, pLogicNode);
    case QUERY_NODE_DELETE_STMT:
      return createDeleteLogicNode(pCxt, (SDeleteStmt*)pStmt, pLogicNode);
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

static void doSetLogicNodeParent(SLogicNode* pNode, SLogicNode* pParent) {
  pNode->pParent = pParent;
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { doSetLogicNodeParent((SLogicNode*)pChild, pNode); }
}

static void setLogicNodeParent(SLogicNode* pNode) { doSetLogicNodeParent(pNode, NULL); }

int32_t createLogicPlan(SPlanContext* pCxt, SLogicSubplan** pLogicSubplan) {
  SLogicPlanContext cxt = {.pPlanCxt = pCxt};

  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = 1;
  pSubplan->id.subplanId = 1;

  int32_t code = createQueryLogicNode(&cxt, pCxt->pAstRoot, &pSubplan->pNode);
  if (TSDB_CODE_SUCCESS == code) {
    setLogicNodeParent(pSubplan->pNode);
    if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY == nodeType(pSubplan->pNode)) {
      pSubplan->subplanType = SUBPLAN_TYPE_MODIFY;
    } else {
      pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode((SNode*)pSubplan);
  }

  return code;
}
