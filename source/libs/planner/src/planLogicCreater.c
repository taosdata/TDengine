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
#include "filter.h"
#include "functionMgt.h"
#include "tglobal.h"

typedef struct SLogicPlanContext {
  SPlanContext* pPlanCxt;
  SLogicNode*   pCurrRoot;
  bool          hasScan;
} SLogicPlanContext;

typedef int32_t (*FCreateLogicNode)(SLogicPlanContext*, void*, SLogicNode**);
typedef int32_t (*FCreateSelectLogicNode)(SLogicPlanContext*, SSelectStmt*, SLogicNode**);
typedef int32_t (*FCreateSetOpLogicNode)(SLogicPlanContext*, SSetOperator*, SLogicNode**);
typedef int32_t (*FCreateDeleteLogicNode)(SLogicPlanContext*, SDeleteStmt*, SLogicNode**);
typedef int32_t (*FCreateInsertLogicNode)(SLogicPlanContext*, SInsertStmt*, SLogicNode**);

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                        SLogicNode** pLogicNode);
static int32_t createQueryLogicNode(SLogicPlanContext* pCxt, SNode* pStmt, SLogicNode** pLogicNode);

typedef struct SRewriteExprCxt {
  int32_t    errCode;
  SNodeList* pExprs;
  bool*      pOutputs;
  bool       isPartitionBy;
} SRewriteExprCxt;

static void setColumnInfo(SFunctionNode* pFunc, SColumnNode* pCol, bool isPartitionBy) {
  switch (pFunc->funcType) {
    case FUNCTION_TYPE_TBNAME:
      pCol->colType = COLUMN_TYPE_TBNAME;
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal) {
        snprintf(pCol->tableName, sizeof(pCol->tableName), "%s", pVal->literal);
        snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", pVal->literal);
      }
      break;
    case FUNCTION_TYPE_WSTART:
      if (!isPartitionBy) {
        pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      }
      pCol->colType = COLUMN_TYPE_WINDOW_START;
      break;
    case FUNCTION_TYPE_WEND:
      if (!isPartitionBy) {
        pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      }
      pCol->colType = COLUMN_TYPE_WINDOW_END;
      break;
    case FUNCTION_TYPE_WDURATION:
      pCol->colType = COLUMN_TYPE_WINDOW_DURATION;
      break;
    case FUNCTION_TYPE_GROUP_KEY:
      pCol->colType = COLUMN_TYPE_GROUP_KEY;
      break;
    default:
      break;
  }
}

static EDealRes doRewriteExpr(SNode** pNode, void* pContext) {
  SRewriteExprCxt* pCxt = (SRewriteExprCxt*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_COLUMN: {
      if (NULL != pCxt->pOutputs) {
        SNode*  pExpr;
        int32_t index = 0;
        FOREACH(pExpr, pCxt->pExprs) {
          if (QUERY_NODE_GROUPING_SET == nodeType(pExpr)) {
            pExpr = nodesListGetNode(((SGroupingSetNode*)pExpr)->pParameterList, 0);
          }
          if (nodesEqualNode(pExpr, *pNode)) {
            pCxt->pOutputs[index] = true;
            break;
          }
          index++;
        }
      }
      break;
    }
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_CASE_WHEN: {
      SNode*  pExpr;
      int32_t index = 0;
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
          strcpy(pCol->node.userAlias, ((SExprNode*)pExpr)->userAlias);
          strcpy(pCol->colName, ((SExprNode*)pExpr)->aliasName);
          if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
            setColumnInfo((SFunctionNode*)pExpr, pCol, pCxt->isPartitionBy);
          }
          nodesDestroyNode(*pNode);
          *pNode = (SNode*)pCol;
          if (NULL != pCxt->pOutputs) {
            pCxt->pOutputs[index] = true;
          }
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

static int32_t rewriteExprForSelect(SNode* pExpr, SSelectStmt* pSelect, ESqlClause clause) {
  nodesWalkExpr(pExpr, doNameExpr, NULL);
  bool isPartitionBy = (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? true : false;
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = NULL, .pOutputs = NULL, .isPartitionBy = isPartitionBy};
  cxt.errCode = nodesListMakeAppend(&cxt.pExprs, pExpr);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    nodesRewriteSelectStmt(pSelect, clause, doRewriteExpr, &cxt);
    nodesClearList(cxt.pExprs);
  }
  return cxt.errCode;
}

static int32_t cloneRewriteExprs(SNodeList* pExprs, bool* pOutputs, SNodeList** pRewriteExpr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t index = 0;
  SNode*  pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    if (pOutputs[index]) {
      code = nodesListMakeStrictAppend(pRewriteExpr, nodesCloneNode(pExpr));
      if (TSDB_CODE_SUCCESS != code) {
        NODES_DESTORY_LIST(*pRewriteExpr);
        break;
      }
    }
    index++;
  }
  return code;
}

static int32_t rewriteExprsForSelect(SNodeList* pExprs, SSelectStmt* pSelect, ESqlClause clause,
                                     SNodeList** pRewriteExprs) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  bool isPartitionBy = (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? true : false;
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs, .pOutputs = NULL, .isPartitionBy = isPartitionBy};
  if (NULL != pRewriteExprs) {
    cxt.pOutputs = taosMemoryCalloc(LIST_LENGTH(pExprs), sizeof(bool));
    if (NULL == cxt.pOutputs) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  nodesRewriteSelectStmt(pSelect, clause, doRewriteExpr, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode && NULL != pRewriteExprs) {
    cxt.errCode = cloneRewriteExprs(pExprs, cxt.pOutputs, pRewriteExprs);
  }
  taosMemoryFree(cxt.pOutputs);
  return cxt.errCode;
}

static int32_t rewriteExpr(SNodeList* pExprs, SNode** pTarget) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs, .pOutputs = NULL, .isPartitionBy = false};
  nodesRewriteExpr(pTarget, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t rewriteExprs(SNodeList* pExprs, SNodeList* pTarget) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs, .pOutputs = NULL, .isPartitionBy = false};
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
    pCxt->pCurrRoot = pNode;
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
                             int8_t tableType, bool tagScan) {
  if (pCxt->pPlanCxt->topicQuery || pCxt->pPlanCxt->streamQuery) {
    return SCAN_TYPE_STREAM;
  }

  if (TSDB_SYSTEM_TABLE == tableType) {
    return SCAN_TYPE_SYSTEM_TABLE;
  }

  if (tagScan && 0 == LIST_LENGTH(pScanCols) && 0 != LIST_LENGTH(pScanPseudoCols)) {
    return SCAN_TYPE_TAG;
  }

  if (NULL == pScanCols) {
    if (NULL == pScanPseudoCols) {
      return (!tagScan) ? SCAN_TYPE_TABLE : SCAN_TYPE_TAG;
    }
    return FUNCTION_TYPE_BLOCK_DIST_INFO == ((SFunctionNode*)nodesListGetNode(pScanPseudoCols, 0))->funcType
               ? SCAN_TYPE_BLOCK_INFO
               : SCAN_TYPE_TABLE;
  }

  return SCAN_TYPE_TABLE;
}

static SNode* createFirstCol(uint64_t tableId, const SSchema* pSchema) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType.type = pSchema->type;
  pCol->node.resType.bytes = pSchema->bytes;
  pCol->tableId = tableId;
  pCol->colId = pSchema->colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  strcpy(pCol->colName, pSchema->name);
  return (SNode*)pCol;
}

static int32_t addPrimaryKeyCol(uint64_t tableId, const SSchema* pSchema, SNodeList** pCols) {
  bool   found = false;
  SNode* pCol = NULL;
  FOREACH(pCol, *pCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      found = true;
      break;
    }
  }

  if (!found) {
    return nodesListMakeStrictAppend(pCols, createFirstCol(tableId, pSchema));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addSystableFirstCol(uint64_t tableId, const SSchema* pSchema, SNodeList** pCols) {
  if (LIST_LENGTH(*pCols) > 0) {
    return TSDB_CODE_SUCCESS;
  }
  return nodesListMakeStrictAppend(pCols, createFirstCol(tableId, pSchema));
}

static int32_t addDefaultScanCol(const STableMeta* pMeta, SNodeList** pCols) {
  if (TSDB_SYSTEM_TABLE == pMeta->tableType) {
    return addSystableFirstCol(pMeta->uid, pMeta->schema, pCols);
  }
  return addPrimaryKeyCol(pMeta->uid, pMeta->schema, pCols);
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
  pScan->cacheLastMode = pRealTable->cacheLastMode;

  *pLogicNode = (SLogicNode*)pScan;

  return TSDB_CODE_SUCCESS;
}

static bool needScanDefaultCol(EScanType scanType) { return SCAN_TYPE_TABLE_COUNT != scanType; }

static EDealRes tagScanNodeHasTbnameFunc(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && FUNCTION_TYPE_TBNAME == ((SFunctionNode*)pNode)->funcType ||
        (QUERY_NODE_COLUMN == nodeType(pNode) && COLUMN_TYPE_TBNAME == ((SColumnNode*)pNode)->colType)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool tagScanNodeListHasTbname(SNodeList* pCols) {
  bool hasTbname = false;
  nodesWalkExprs(pCols, tagScanNodeHasTbnameFunc, &hasTbname);
  return hasTbname;
}

static bool tagScanNodeHasTbname(SNode* pKeys) {
  bool hasTbname = false;
  nodesWalkExpr(pKeys, tagScanNodeHasTbnameFunc, &hasTbname);
  return hasTbname;
}

static int32_t tagScanSetExecutionMode(SScanLogicNode* pScan) {
  pScan->onlyMetaCtbIdx = false;

  if (pScan->tableType == TSDB_CHILD_TABLE) {
    pScan->onlyMetaCtbIdx = false;
    return TSDB_CODE_SUCCESS;
  }

  if (tagScanNodeListHasTbname(pScan->pScanPseudoCols)) {
    pScan->onlyMetaCtbIdx = false;
    return TSDB_CODE_SUCCESS;
  }

  if (pScan->node.pConditions == NULL) {
    pScan->onlyMetaCtbIdx = true;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pCond = nodesCloneNode(pScan->node.pConditions);
  SNode* pTagCond = NULL;
  SNode* pTagIndexCond = NULL;
  filterPartitionCond(&pCond, NULL, &pTagIndexCond, &pTagCond, NULL);
  if (pTagIndexCond || tagScanNodeHasTbname(pTagCond)) {
    pScan->onlyMetaCtbIdx = false;
  } else {
    pScan->onlyMetaCtbIdx = true;
  }
  nodesDestroyNode(pCond);
  nodesDestroyNode(pTagIndexCond);
  nodesDestroyNode(pTagCond);
  return TSDB_CODE_SUCCESS;
}

static int32_t createScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable,
                                   SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = makeScanLogicNode(pCxt, pRealTable, pSelect->hasRepeatScanFuncs, (SLogicNode**)&pScan);

  pScan->node.groupAction = GROUP_ACTION_NONE;
  pScan->node.resultDataOrder = DATA_ORDER_LEVEL_IN_BLOCK;
  if (pCxt->pPlanCxt->streamQuery) {
    pScan->triggerType = pCxt->pPlanCxt->triggerType;
    pScan->watermark = pCxt->pPlanCxt->watermark;
    pScan->deleteMark = pCxt->pPlanCxt->deleteMark;
    pScan->igExpired = pCxt->pPlanCxt->igExpired;
    pScan->igCheckUpdate = pCxt->pPlanCxt->igCheckUpdate;
  }

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
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, fmIsScanPseudoColumnFunc, &pScan->pScanPseudoCols);
  }

  pScan->scanType = getScanType(pCxt, pScan->pScanPseudoCols, pScan->pScanCols, pScan->tableType, pSelect->tagScan);

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    SNodeList* pNewScanPseudoCols = NULL;
    code = rewriteExprsForSelect(pScan->pScanPseudoCols, pSelect, SQL_CLAUSE_FROM, NULL);
/*
    if (TSDB_CODE_SUCCESS == code && NULL != pScan->pScanPseudoCols) {
      code = createColumnByRewriteExprs(pScan->pScanPseudoCols, &pNewScanPseudoCols);
      if (TSDB_CODE_SUCCESS == code) {
        nodesDestroyList(pScan->pScanPseudoCols);
        pScan->pScanPseudoCols = pNewScanPseudoCols;
      }
    }
*/
  }

  if (NULL != pScan->pScanCols) {
    pScan->hasNormalCols = true;
  }

  if (TSDB_CODE_SUCCESS == code && needScanDefaultCol(pScan->scanType)) {
    code = addDefaultScanCol(pRealTable->pMeta, &pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pTags && NULL == pSelect->pPartitionByList) {
    pScan->pTags = nodesCloneList(pSelect->pTags);
    if (NULL == pScan->pTags) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pSubtable && NULL == pSelect->pPartitionByList) {
    pScan->pSubtable = nodesCloneNode(pSelect->pSubtable);
    if (NULL == pScan->pSubtable) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pScan->pScanCols, &pScan->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pScan->pScanPseudoCols, &pScan->node.pTargets);
  }

  if (pScan->scanType == SCAN_TYPE_TAG) {
    code = tagScanSetExecutionMode(pScan);
  }

  bool isCountByTag = false;
  if (pSelect->hasCountFunc && NULL == pSelect->pWindow) {
    if (pSelect->pGroupByList) {
      isCountByTag = !keysHasCol(pSelect->pGroupByList);
    } else if (pSelect->pPartitionByList) {
      isCountByTag = !keysHasCol(pSelect->pPartitionByList);
    }
  }
  pScan->isCountByTag = isCountByTag;

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pScan;
  } else {
    nodesDestroyNode((SNode*)pScan);
  }
  pScan->paraTablesSort = getparaTablesSortOptHint(pSelect->pHint);
  pCxt->hasScan = true;

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
  pJoin->joinAlgo = JOIN_ALGO_UNKNOWN;
  pJoin->isSingleTableJoin = pJoinTable->table.singleTable;
  pJoin->hasSubQuery = pJoinTable->hasSubQuery;
  pJoin->node.inputTsOrder = ORDER_ASC;
  pJoin->node.groupAction = GROUP_ACTION_CLEAR;
  pJoin->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pJoin->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->isLowLevelJoin = pJoinTable->isLowLevelJoin;

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
    if (TSDB_CODE_SUCCESS != code) {
      pLeft = NULL;
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
    pJoin->pOtherOnCond = nodesCloneNode(pJoinTable->pOnCond);
    if (NULL == pJoin->pOtherOnCond) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    SNodeList* pColList = NULL;
    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pLeft) && !pJoin->isLowLevelJoin) {
      code = nodesCollectColumns(pSelect, SQL_CLAUSE_WHERE, ((SRealTableNode*)pJoinTable->pLeft)->table.tableAlias, COLLECT_COL_TYPE_ALL, &pColList);
    } else {
      pJoin->node.pTargets = nodesCloneList(pLeft->pTargets);
      if (NULL == pJoin->node.pTargets) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pColList) {
      code = createColumnByRewriteExprs(pColList, &pJoin->node.pTargets);
    }
    nodesDestroyList(pColList);
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNodeList* pColList = NULL;
    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pRight) && !pJoin->isLowLevelJoin) {
      code = nodesCollectColumns(pSelect, SQL_CLAUSE_WHERE, ((SRealTableNode*)pJoinTable->pRight)->table.tableAlias, COLLECT_COL_TYPE_ALL, &pColList);
    } else {
      if (pJoin->node.pTargets) {
        nodesListStrictAppendList(pJoin->node.pTargets, nodesCloneList(pRight->pTargets));
      } else {
        pJoin->node.pTargets = nodesCloneList(pRight->pTargets);
        if (NULL == pJoin->node.pTargets) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
    if (TSDB_CODE_SUCCESS == code && NULL != pColList) {
      code = createColumnByRewriteExprs(pColList, &pJoin->node.pTargets);
    }
    nodesDestroyList(pColList);
  }

  if (NULL == pJoin->node.pTargets && NULL != pLeft) {
    pJoin->node.pTargets = nodesCloneList(pLeft->pTargets);
    if (NULL == pJoin->node.pTargets) {
      code = TSDB_CODE_OUT_OF_MEMORY;
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
    pCxt->pCurrRoot = pNode;
  }
  return code;
}

static SColumnNode* createColumnByExpr(const char* pStmtName, SExprNode* pExpr) {
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = pExpr->resType;
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", pExpr->aliasName);
  if (NULL != pStmtName) {
    snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", pStmtName);
  }
  return pCol;
}

static SNode* createGroupingSetNode(SNode* pExpr) {
  SGroupingSetNode* pGroupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  if (NULL == pGroupingSet) {
    return NULL;
  }
  pGroupingSet->groupingSetType = GP_TYPE_NORMAL;
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pGroupingSet->pParameterList, nodesCloneNode(pExpr))) {
    nodesDestroyNode((SNode*)pGroupingSet);
    return NULL;
  }
  return (SNode*)pGroupingSet;
}

static EGroupAction getDistinctGroupAction(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  return (pCxt->pPlanCxt->streamQuery || NULL != pSelect->pLimit || NULL != pSelect->pSlimit) ? GROUP_ACTION_KEEP
                                                                                              : GROUP_ACTION_NONE;
}

static EGroupAction getGroupAction(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  return ((pCxt->pPlanCxt->streamQuery || NULL != pSelect->pLimit || NULL != pSelect->pSlimit) && !pSelect->isDistinct) ? GROUP_ACTION_KEEP
                                                                                              : GROUP_ACTION_NONE;
}

static EDataOrderLevel getRequireDataOrder(bool needTimeline, SSelectStmt* pSelect) {
  return needTimeline ? (NULL != pSelect->pPartitionByList ? DATA_ORDER_LEVEL_IN_GROUP : DATA_ORDER_LEVEL_GLOBAL)
                      : DATA_ORDER_LEVEL_NONE;
}

static int32_t createAggLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasAggFuncs && NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = (SAggLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG);
  if (NULL == pAgg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pAgg->hasLastRow = pSelect->hasLastRowFunc;
  pAgg->hasLast = pSelect->hasLastFunc;
  pAgg->hasTimeLineFunc = pSelect->hasTimeLineFunc;
  pAgg->hasGroupKeyOptimized = false;
  pAgg->onlyHasKeepOrderFunc = pSelect->onlyHasKeepOrderFunc;
  pAgg->node.groupAction = getGroupAction(pCxt, pSelect);
  pAgg->node.requireDataOrder = getRequireDataOrder(pAgg->hasTimeLineFunc, pSelect);
  pAgg->node.resultDataOrder = pAgg->onlyHasKeepOrderFunc ? pAgg->node.requireDataOrder : DATA_ORDER_LEVEL_NONE;

  int32_t code = TSDB_CODE_SUCCESS;

  // set grouyp keys, agg funcs and having conditions
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, NULL, fmIsAggFunc, &pAgg->pAggFuncs);
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = rewriteExprsForSelect(pAgg->pAggFuncs, pSelect, SQL_CLAUSE_GROUP_BY, NULL);
  }

  if (NULL != pSelect->pGroupByList) {
    pAgg->pGroupKeys = nodesCloneList(pSelect->pGroupByList);
    if (NULL == pAgg->pGroupKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // rewrite the expression in subsequent clauses
  SNodeList* pOutputGroupKeys = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_GROUP_BY, &pOutputGroupKeys);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pAgg->node.pConditions = nodesCloneNode(pSelect->pHaving);
    if (NULL == pAgg->node.pConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = createColumnByRewriteExprs(pAgg->pAggFuncs, &pAgg->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pOutputGroupKeys) {
      code = createColumnByRewriteExprs(pOutputGroupKeys, &pAgg->node.pTargets);
    } else if (NULL == pAgg->node.pTargets && NULL != pAgg->pGroupKeys) {
      code = createColumnByRewriteExprs(pAgg->pGroupKeys, &pAgg->node.pTargets);
    }
  }
  nodesDestroyList(pOutputGroupKeys);

  pAgg->isGroupTb = pAgg->pGroupKeys ? keysHasTbname(pAgg->pGroupKeys) : 0;
  pAgg->isPartTb = pSelect->pPartitionByList ? keysHasTbname(pSelect->pPartitionByList) : 0;
  pAgg->hasGroup = pAgg->pGroupKeys || pSelect->pPartitionByList;
  
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

  pIdfRowsFunc->isTailFunc = pSelect->hasTailFunc;
  pIdfRowsFunc->isUniqueFunc = pSelect->hasUniqueFunc;
  pIdfRowsFunc->isTimeLineFunc = pSelect->hasTimeLineFunc;
  pIdfRowsFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pIdfRowsFunc->node.requireDataOrder = getRequireDataOrder(pIdfRowsFunc->isTimeLineFunc, pSelect);
  pIdfRowsFunc->node.resultDataOrder = pIdfRowsFunc->node.requireDataOrder;

  // indefinite rows functions and _select_values functions
  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, fmIsVectorFunc, &pIdfRowsFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pIdfRowsFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pIdfRowsFunc->pFuncs, &pIdfRowsFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pIdfRowsFunc;
  } else {
    nodesDestroyNode((SNode*)pIdfRowsFunc);
  }

  return code;
}

static bool isInterpFunc(int32_t funcId) {
  return fmIsInterpFunc(funcId) || fmIsInterpPseudoColumnFunc(funcId) || fmIsGroupKeyFunc(funcId);
}

static int32_t createInterpFuncLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasInterpFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SInterpFuncLogicNode* pInterpFunc = (SInterpFuncLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INTERP_FUNC);
  if (NULL == pInterpFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pInterpFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pInterpFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pInterpFunc->node.resultDataOrder = pInterpFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isInterpFunc, &pInterpFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pInterpFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pFill) {
    SFillNode* pFill = (SFillNode*)pSelect->pFill;
    pInterpFunc->timeRange = pFill->timeRange;
    pInterpFunc->fillMode = pFill->mode;
    pInterpFunc->pTimeSeries = nodesCloneNode(pFill->pWStartTs);
    pInterpFunc->pFillValues = nodesCloneNode(pFill->pValues);
    if (NULL == pInterpFunc->pTimeSeries || (NULL != pFill->pValues && NULL == pInterpFunc->pFillValues)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pEvery) {
    pInterpFunc->interval = ((SValueNode*)pSelect->pEvery)->datum.i;
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pInterpFunc->pFuncs, &pInterpFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pInterpFunc;
  } else {
    nodesDestroyNode((SNode*)pInterpFunc);
  }

  return code;
}

static int32_t createWindowLogicNodeFinalize(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SWindowLogicNode* pWindow,
                                             SLogicNode** pLogicNode) {
  if (pCxt->pPlanCxt->streamQuery) {
    pWindow->triggerType = pCxt->pPlanCxt->triggerType;
    pWindow->watermark = pCxt->pPlanCxt->watermark;
    pWindow->deleteMark = pCxt->pPlanCxt->deleteMark;
    pWindow->igExpired = pCxt->pPlanCxt->igExpired;
    pWindow->igCheckUpdate = pCxt->pPlanCxt->igCheckUpdate;
  }
  pWindow->node.inputTsOrder = ORDER_ASC;
  pWindow->node.outputTsOrder = ORDER_ASC;

  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_WINDOW, NULL, fmIsWindowClauseFunc, &pWindow->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pWindow->node.pConditions = nodesCloneNode(pSelect->pHaving);
    if (NULL == pWindow->node.pConditions) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
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
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_IN_BLOCK : getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_GLOBAL : pWindow->node.requireDataOrder;
  pWindow->pStateExpr = nodesCloneNode(pState->pExpr);
  pWindow->pTspk = nodesCloneNode(pState->pCol);
  if (NULL == pWindow->pStateExpr || NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  // rewrite the expression in subsequent clauses
  int32_t code = rewriteExprForSelect(pWindow->pStateExpr, pSelect, SQL_CLAUSE_WINDOW);
  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
  }

  return code;
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
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_IN_BLOCK : getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_GLOBAL : pWindow->node.requireDataOrder;

  pWindow->pTspk = nodesCloneNode((SNode*)pSession->pCol);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pWindow->pTsEnd = nodesCloneNode((SNode*)pSession->pCol);

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
  pWindow->node.groupAction = (NULL != pInterval->pFill ? GROUP_ACTION_KEEP : getGroupAction(pCxt, pSelect));
  pWindow->node.requireDataOrder =
      pCxt->pPlanCxt->streamQuery
          ? DATA_ORDER_LEVEL_IN_BLOCK
          : (pSelect->hasTimeLineFunc ? getRequireDataOrder(true, pSelect) : DATA_ORDER_LEVEL_IN_BLOCK);
  pWindow->node.resultDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_GLOBAL : getRequireDataOrder(true, pSelect);
  pWindow->pTspk = nodesCloneNode(pInterval->pCol);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pWindow->isPartTb = pSelect->pPartitionByList ? keysHasTbname(pSelect->pPartitionByList) : 0;

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByEvent(SLogicPlanContext* pCxt, SEventWindowNode* pEvent, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pWindow->winType = WINDOW_TYPE_EVENT;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_IN_BLOCK : getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_GLOBAL : pWindow->node.requireDataOrder;
  pWindow->pStartCond = nodesCloneNode(pEvent->pStartCond);
  pWindow->pEndCond = nodesCloneNode(pEvent->pEndCond);
  pWindow->pTspk = nodesCloneNode(pEvent->pCol);
  if (NULL == pWindow->pStartCond || NULL == pWindow->pEndCond || NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByCount(SLogicPlanContext* pCxt, SCountWindowNode* pCount, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = (SWindowLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW);
  if (NULL == pWindow) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (!pCxt->pPlanCxt->streamQuery && tsDisableCount) {
    return TSDB_CODE_FAILED;
  }

  pWindow->winType = WINDOW_TYPE_COUNT;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_IN_BLOCK : getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder =
      pCxt->pPlanCxt->streamQuery ? DATA_ORDER_LEVEL_GLOBAL : pWindow->node.requireDataOrder;
  pWindow->windowCount = pCount->windowCount;
  pWindow->windowSliding = pCount->windowSliding;
  pWindow->pTspk = nodesCloneNode(pCount->pCol);
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
    case QUERY_NODE_EVENT_WINDOW:
      return createWindowLogicNodeByEvent(pCxt, (SEventWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    case QUERY_NODE_COUNT_WINDOW:
      return createWindowLogicNodeByCount(pCxt, (SCountWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static EDealRes needFillValueImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (COLUMN_TYPE_WINDOW_START != pCol->colType && COLUMN_TYPE_WINDOW_END != pCol->colType &&
        COLUMN_TYPE_WINDOW_DURATION != pCol->colType && COLUMN_TYPE_GROUP_KEY != pCol->colType) {
      *(bool*)pContext = true;
      return DEAL_RES_END;
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool needFillValue(SNode* pNode) {
  bool hasFillCol = false;
  nodesWalkExpr(pNode, needFillValueImpl, &hasFillCol);
  return hasFillCol;
}

static int32_t partFillExprs(SSelectStmt* pSelect, SNodeList** pFillExprs, SNodeList** pNotFillExprs) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pProject = NULL;
  FOREACH(pProject, pSelect->pProjectionList) {
    if (needFillValue(pProject)) {
      code = nodesListMakeStrictAppend(pFillExprs, nodesCloneNode(pProject));
    } else if (QUERY_NODE_VALUE != nodeType(pProject)) {
      code = nodesListMakeStrictAppend(pNotFillExprs, nodesCloneNode(pProject));
    }
    if (TSDB_CODE_SUCCESS != code) {
      NODES_DESTORY_LIST(*pFillExprs);
      NODES_DESTORY_LIST(*pNotFillExprs);
      break;
    }
  }
  if (!pSelect->isDistinct) {
    SNode* pOrderExpr = NULL;
    FOREACH(pOrderExpr, pSelect->pOrderByList) {
      SNode* pExpr = ((SOrderByExprNode*)pOrderExpr)->pExpr;
      if (needFillValue(pExpr)) {
        code = nodesListMakeStrictAppend(pFillExprs, nodesCloneNode(pExpr));
      } else if (QUERY_NODE_VALUE != nodeType(pExpr)) {
        code = nodesListMakeStrictAppend(pNotFillExprs, nodesCloneNode(pExpr));
      }
      if (TSDB_CODE_SUCCESS != code) {
        NODES_DESTORY_LIST(*pFillExprs);
        NODES_DESTORY_LIST(*pNotFillExprs);
        break;
      }
    }
  }
  return code;
}

static int32_t createFillLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pWindow || QUERY_NODE_INTERVAL_WINDOW != nodeType(pSelect->pWindow) ||
      NULL == ((SIntervalWindowNode*)pSelect->pWindow)->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  SFillNode* pFillNode = (SFillNode*)(((SIntervalWindowNode*)pSelect->pWindow)->pFill);
  if (FILL_MODE_NONE == pFillNode->mode) {
    return TSDB_CODE_SUCCESS;
  }

  SFillLogicNode* pFill = (SFillLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILL);
  if (NULL == pFill) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pFill->node.groupAction = getGroupAction(pCxt, pSelect);
  pFill->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pFill->node.resultDataOrder = pFill->node.requireDataOrder;
  pFill->node.inputTsOrder = 0;

  int32_t code = partFillExprs(pSelect, &pFill->pFillExprs, &pFill->pNotFillExprs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pFill->pFillExprs, pSelect, SQL_CLAUSE_FILL, NULL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pFill->pNotFillExprs, pSelect, SQL_CLAUSE_FILL, NULL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pFill->pFillExprs, &pFill->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pFill->pNotFillExprs, &pFill->node.pTargets);
  }

  pFill->mode = pFillNode->mode;
  pFill->timeRange = pFillNode->timeRange;
  pFill->pValues = nodesCloneNode(pFillNode->pValues);
  pFill->pWStartTs = nodesCloneNode(pFillNode->pWStartTs);
  if ((NULL != pFillNode->pValues && NULL == pFill->pValues) || NULL == pFill->pWStartTs) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

  if (TSDB_CODE_SUCCESS == code && 0 == LIST_LENGTH(pFill->node.pTargets)) {
    code = createColumnByRewriteExpr(pFill->pWStartTs, &pFill->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pFill;
  } else {
    nodesDestroyNode((SNode*)pFill);
  }

  return code;
}

static bool isPrimaryKeySort(SNodeList* pOrderByList) {
  SNode* pExpr = ((SOrderByExprNode*)nodesListGetNode(pOrderByList, 0))->pExpr;
  if (QUERY_NODE_COLUMN != nodeType(pExpr)) {
    return false;
  }
  return PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId;
}

static int32_t createSortLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pOrderByList) {
    return TSDB_CODE_SUCCESS;
  }

  SSortLogicNode* pSort = (SSortLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT);
  if (NULL == pSort) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSort->groupSort = pSelect->groupSort;
  pSort->node.groupAction = pSort->groupSort ? GROUP_ACTION_KEEP : GROUP_ACTION_CLEAR;
  pSort->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pSort->node.resultDataOrder = isPrimaryKeySort(pSelect->pOrderByList)
                                    ? (pSort->groupSort ? DATA_ORDER_LEVEL_IN_GROUP : DATA_ORDER_LEVEL_GLOBAL)
                                    : DATA_ORDER_LEVEL_NONE;
  int32_t code = nodesCollectColumns(pSelect, SQL_CLAUSE_ORDER_BY, NULL, COLLECT_COL_TYPE_ALL, &pSort->node.pTargets);
  if (TSDB_CODE_SUCCESS == code && NULL == pSort->node.pTargets) {
    code = nodesListMakeStrictAppend(&pSort->node.pTargets,
                                     nodesCloneNode(nodesListGetNode(pCxt->pCurrRoot->pTargets, 0)));
  }

  if (TSDB_CODE_SUCCESS == code) {
    pSort->pSortKeys = nodesCloneList(pSelect->pOrderByList);
    if (NULL == pSort->pSortKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
    SNode*            pNode = NULL;
    SOrderByExprNode* firstSortKey = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);
    if (isPrimaryKeySort(pSelect->pOrderByList)) pSort->node.outputTsOrder = firstSortKey->order;
    if (firstSortKey->pExpr->type == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)firstSortKey->pExpr;
      int16_t      projIdx = 1;
      FOREACH(pNode, pSelect->pProjectionList) {
        SExprNode* pExpr = (SExprNode*)pNode;
        if (0 == strcmp(pCol->node.aliasName, pExpr->aliasName)) {
          pCol->projIdx = projIdx; break;
        }
        projIdx++;
      }
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

  TSWAP(pProject->node.pLimit, pSelect->pLimit);
  TSWAP(pProject->node.pSlimit, pSelect->pSlimit);
  pProject->ignoreGroupId = pSelect->isSubquery ? true : (NULL == pSelect->pPartitionByList);
  pProject->node.groupAction =
      (!pSelect->isSubquery && pCxt->pPlanCxt->streamQuery) ? GROUP_ACTION_KEEP : GROUP_ACTION_CLEAR;
  pProject->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pProject->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

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

  pPartition->node.groupAction = GROUP_ACTION_SET;
  pPartition->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pPartition->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  int32_t code =
      nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, COLLECT_COL_TYPE_ALL, &pPartition->node.pTargets);
  if (TSDB_CODE_SUCCESS == code && NULL == pPartition->node.pTargets) {
    code = nodesListMakeStrictAppend(&pPartition->node.pTargets,
                                     nodesCloneNode(nodesListGetNode(pCxt->pCurrRoot->pTargets, 0)));
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, NULL, fmIsAggFunc, &pPartition->pAggFuncs);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pPartition->pPartitionKeys = nodesCloneList(pSelect->pPartitionByList);
    if (NULL == pPartition->pPartitionKeys) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (keysHasCol(pPartition->pPartitionKeys) && pSelect->pWindow &&
      nodeType(pSelect->pWindow) == QUERY_NODE_INTERVAL_WINDOW) {
    pPartition->needBlockOutputTsOrder = true;
    SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pSelect->pWindow;
    SColumnNode* pTsCol = (SColumnNode*)pInterval->pCol;
    pPartition->pkTsColId = pTsCol->colId;
    pPartition->pkTsColTbId = pTsCol->tableId;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pTags) {
    pPartition->pTags = nodesCloneList(pSelect->pTags);
    if (NULL == pPartition->pTags) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pSubtable) {
    pPartition->pSubtable = nodesCloneNode(pSelect->pSubtable);
    if (NULL == pPartition->pSubtable) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving && !pSelect->hasAggFuncs && NULL == pSelect->pGroupByList &&
      NULL == pSelect->pWindow) {
    pPartition->node.pConditions = nodesCloneNode(pSelect->pHaving);
    if (NULL == pPartition->node.pConditions) {
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

  pAgg->node.groupAction = GROUP_ACTION_CLEAR;//getDistinctGroupAction(pCxt, pSelect);
  pAgg->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pAgg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  int32_t code = TSDB_CODE_SUCCESS;
  // set grouyp keys, agg funcs and having conditions
  SNodeList* pGroupKeys = NULL;
  SNode*     pProjection = NULL;
  FOREACH(pProjection, pSelect->pProjectionList) {
    code = nodesListMakeStrictAppend(&pGroupKeys, createGroupingSetNode(pProjection));
    if (TSDB_CODE_SUCCESS != code) {
      nodesDestroyList(pGroupKeys);
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pAgg->pGroupKeys = pGroupKeys;
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_DISTINCT, NULL);
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

static int32_t createSelectWithoutFromLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                                SLogicNode** pLogicNode) {
  return createProjectLogicNode(pCxt, pSelect, pLogicNode);
}

static int32_t createSelectFromLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
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
    code = createSelectRootLogicNode(pCxt, pSelect, createInterpFuncLogicNode, &pRoot);
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

static int32_t createSelectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pSelect->pFromTable) {
    code = createSelectWithoutFromLogicNode(pCxt, pSelect, pLogicNode);
  } else {
    code = createSelectFromLogicNode(pCxt, pSelect, pLogicNode);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != *pLogicNode) {
    (*pLogicNode)->stmtRoot = true;
    TSWAP((*pLogicNode)->pHint, pSelect->pHint);
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

  TSWAP(pSort->node.pLimit, pSetOperator->pLimit);

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

  if (NULL == pSetOperator->pOrderByList) {
    TSWAP(pProject->node.pLimit, pSetOperator->pLimit);
  }
  pProject->ignoreGroupId = true;

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

  if (NULL == pSetOperator->pOrderByList) {
    TSWAP(pAgg->node.pSlimit, pSetOperator->pLimit);
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
    pSetOp->precision = pSetOperator->precision;
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
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      return TDMT_VND_CREATE_TABLE;
    case QUERY_NODE_DROP_TABLE_STMT:
      return TDMT_VND_DROP_TABLE;
    case QUERY_NODE_ALTER_TABLE_STMT:
      return TDMT_VND_ALTER_TABLE;
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      return TDMT_VND_COMMIT;
    default:
      break;
  }
  return TDMT_VND_SUBMIT;
}

static int32_t createVnodeModifLogicNode(SLogicPlanContext* pCxt, SVnodeModifyOpStmt* pStmt, SLogicNode** pLogicNode) {
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
  int32_t          code = makeScanLogicNode(pCxt, (SRealTableNode*)pDelete->pFromTable, false, (SLogicNode**)&pScan);

  // set columns to scan
  if (TSDB_CODE_SUCCESS == code) {
    pScan->scanType = SCAN_TYPE_TABLE;
    pScan->scanRange = pDelete->timeRange;
    pScan->pScanCols = nodesCloneList(((SFunctionNode*)pDelete->pCountFunc)->pParameterList);
    if (NULL == pScan->pScanCols) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pDelete->pTagCond) {
    pScan->pTagCond = nodesCloneNode(pDelete->pTagCond);
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
    code = nodesListStrictAppend(pAgg->pAggFuncs, nodesCloneNode(pDelete->pFirstFunc));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListStrictAppend(pAgg->pAggFuncs, nodesCloneNode(pDelete->pLastFunc));
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pAggFuncs, &pDelete->pCountFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pAggFuncs, &pDelete->pFirstFunc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExpr(pAgg->pAggFuncs, &pDelete->pLastFunc);
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
  snprintf(pModify->tableName, sizeof(pModify->tableName), "%s", pRealTable->table.tableName);
  strcpy(pModify->tsColName, pRealTable->pMeta->schema->name);
  pModify->deleteTimeRange = pDelete->timeRange;
  pModify->pAffectedRows = nodesCloneNode(pDelete->pCountFunc);
  pModify->pStartTs = nodesCloneNode(pDelete->pFirstFunc);
  pModify->pEndTs = nodesCloneNode(pDelete->pLastFunc);
  if (NULL == pModify->pAffectedRows || NULL == pModify->pStartTs || NULL == pModify->pEndTs) {
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

static int32_t creatInsertRootLogicNode(SLogicPlanContext* pCxt, SInsertStmt* pInsert, FCreateInsertLogicNode func,
                                        SLogicNode** pRoot) {
  return createRootLogicNode(pCxt, pInsert, pInsert->precision, (FCreateLogicNode)func, pRoot);
}

static int32_t createVnodeModifLogicNodeByInsert(SLogicPlanContext* pCxt, SInsertStmt* pInsert,
                                                 SLogicNode** pLogicNode) {
  SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY);
  if (NULL == pModify) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SRealTableNode* pRealTable = (SRealTableNode*)pInsert->pTable;

  pModify->modifyType = MODIFY_TABLE_TYPE_INSERT;
  pModify->tableId = pRealTable->pMeta->uid;
  pModify->stableId = pRealTable->pMeta->suid;
  pModify->tableType = pRealTable->pMeta->tableType;
  snprintf(pModify->tableName, sizeof(pModify->tableName), "%s", pRealTable->table.tableName);
  TSWAP(pModify->pVgroupList, pRealTable->pVgroupList);
  pModify->pInsertCols = nodesCloneList(pInsert->pCols);
  if (NULL == pModify->pInsertCols) {
    nodesDestroyNode((SNode*)pModify);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pLogicNode = (SLogicNode*)pModify;
  return TSDB_CODE_SUCCESS;
}

static int32_t createInsertLogicNode(SLogicPlanContext* pCxt, SInsertStmt* pInsert, SLogicNode** pLogicNode) {
  SLogicNode* pRoot = NULL;
  int32_t     code = createQueryLogicNode(pCxt, pInsert->pQuery, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = creatInsertRootLogicNode(pCxt, pInsert, createVnodeModifLogicNodeByInsert, &pRoot);
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
    case QUERY_NODE_VNODE_MODIFY_STMT:
      return createVnodeModifLogicNode(pCxt, (SVnodeModifyOpStmt*)pStmt, pLogicNode);
    case QUERY_NODE_EXPLAIN_STMT:
      return createQueryLogicNode(pCxt, ((SExplainStmt*)pStmt)->pQuery, pLogicNode);
    case QUERY_NODE_SET_OPERATOR:
      return createSetOperatorLogicNode(pCxt, (SSetOperator*)pStmt, pLogicNode);
    case QUERY_NODE_DELETE_STMT:
      return createDeleteLogicNode(pCxt, (SDeleteStmt*)pStmt, pLogicNode);
    case QUERY_NODE_INSERT_STMT:
      return createInsertLogicNode(pCxt, (SInsertStmt*)pStmt, pLogicNode);
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

static void setLogicSubplanType(bool hasScan, SLogicSubplan* pSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY != nodeType(pSubplan->pNode)) {
    pSubplan->subplanType = hasScan ? SUBPLAN_TYPE_SCAN : SUBPLAN_TYPE_MERGE;
  } else {
    SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)pSubplan->pNode;
    pSubplan->subplanType = (MODIFY_TABLE_TYPE_INSERT == pModify->modifyType && NULL != pModify->node.pChildren)
                                ? SUBPLAN_TYPE_SCAN
                                : SUBPLAN_TYPE_MODIFY;
  }
}

int32_t createLogicPlan(SPlanContext* pCxt, SLogicSubplan** pLogicSubplan) {
  SLogicPlanContext cxt = {.pPlanCxt = pCxt, .pCurrRoot = NULL, .hasScan = false};

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
    setLogicSubplanType(cxt.hasScan, pSubplan);
    code = adjustLogicNodeDataRequirement(pSubplan->pNode, DATA_ORDER_LEVEL_NONE);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode((SNode*)pSubplan);
  }

  return code;
}
