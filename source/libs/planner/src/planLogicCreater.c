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

#include "filter.h"
#include "functionMgt.h"
#include "parser.h"
#include "planInt.h"
#include "planner.h"
#include "plannodes.h"
#include "querynodes.h"
#include "systable.h"
#include "tglobal.h"

#include <stdarg.h>

// primary key column always the second column if exists
#define PRIMARY_COLUMN_SLOT 1

typedef struct SLogicPlanContext {
  SPlanContext* pPlanCxt;
  SLogicNode*   pCurrRoot;
  SSHashObj*    pChildTables;
  bool          containsOuterJoin;
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
  SNode*     pOrderByFirstExpr;  // May be invalid after rewriting, use getOrderByFirstExpr() instead
  SSelectStmt* pSelect;  // Used to get the current first order by expr after rewriting
} SRewriteExprCxt;

// Helper function to safely get the first order by expression after rewriting
static SNode* getOrderByFirstExpr(SRewriteExprCxt* pCxt) {
  if (pCxt->pSelect && pCxt->pSelect->pOrderByList && pCxt->pSelect->pOrderByList->length > 0) {
    SOrderByExprNode* pOrderByExpr = (SOrderByExprNode*)nodesListGetNode(pCxt->pSelect->pOrderByList, 0);
    if (pOrderByExpr && pOrderByExpr->pExpr) {
      return pOrderByExpr->pExpr;
    }
  }
  return NULL;
}

static void setColumnInfo(SFunctionNode* pFunc, SColumnNode* pCol, bool isPartitionBy, SRewriteExprCxt* pCxt) {
  // Get the current first order by expression (may have been rewritten)
  SNode* pOrderByFirstExpr = getOrderByFirstExpr(pCxt);
  
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
      pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pCol->colType = COLUMN_TYPE_WINDOW_START;
      break;
    case FUNCTION_TYPE_WEND:
      pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pCol->colType = COLUMN_TYPE_WINDOW_END;
      break;
    case FUNCTION_TYPE_WDURATION:
      pCol->colType = COLUMN_TYPE_WINDOW_DURATION;
      break;
    case FUNCTION_TYPE_GROUP_KEY:
      pCol->colType = COLUMN_TYPE_GROUP_KEY;
      break;
    case FUNCTION_TYPE_IS_WINDOW_FILLED:
      pCol->colType = COLUMN_TYPE_IS_WINDOW_FILLED;
      break;
    case FUNCTION_TYPE_TPREV_TS:
    case FUNCTION_TYPE_TCURRENT_TS:
    case FUNCTION_TYPE_TNEXT_TS:
    case FUNCTION_TYPE_TWSTART:
    case FUNCTION_TYPE_TWEND:
    case FUNCTION_TYPE_TPREV_LOCALTIME:
    case FUNCTION_TYPE_TNEXT_LOCALTIME:
    case FUNCTION_TYPE_TLOCALTIME:
    case FUNCTION_TYPE_TIDLESTART:
    case FUNCTION_TYPE_TIDLEEND:
      pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pCol->isPrimTs = true;
      break;
    default:
      break;
  }
  if (fmIsKeepOrderFunc(pFunc) && isPrimaryKeyImpl((SNode*)pFunc)) {
    if (!isPartitionBy || (pOrderByFirstExpr && ((SExprNode*)pOrderByFirstExpr)->projIdx == pCol->node.projIdx &&
                           isPrimaryKeyImpl(pOrderByFirstExpr))) {
      pCol->isPrimTs = true;
    }
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
          SColumnNode* pCol = NULL;
          pCxt->errCode = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
          if (NULL == pCol) {
            return DEAL_RES_ERROR;
          }
          SExprNode* pToBeRewrittenExpr = (SExprNode*)(*pNode);
          pCol->node.resType = pToBeRewrittenExpr->resType;
          tstrncpy(pCol->node.aliasName, pToBeRewrittenExpr->aliasName, TSDB_COL_NAME_LEN);
          tstrncpy(pCol->node.userAlias, ((SExprNode*)pExpr)->userAlias, TSDB_COL_NAME_LEN);
          tstrncpy(pCol->colName, ((SExprNode*)pExpr)->aliasName, TSDB_COL_NAME_LEN);
          pCol->node.projIdx = ((SExprNode*)(*pNode))->projIdx;
          pCol->node.relatedTo = ((SExprNode*)(*pNode))->relatedTo;
          if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
            setColumnInfo((SFunctionNode*)pExpr, pCol, pCxt->isPartitionBy, pCxt);
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
        rewriteExprAliasName((SExprNode*)pNode, (int64_t)pNode);
      }
      return DEAL_RES_IGNORE_CHILD;
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t generatePlanErrMsg(SLogicPlanContext* pCxt, int32_t errCode, const char* pFormat, ...) {
  if (pCxt && pCxt->pPlanCxt && pCxt->pPlanCxt->pMsg && pCxt->pPlanCxt->msgLen > 0 && pFormat) {
    va_list args;
    va_start(args, pFormat);
    (void)vsnprintf(pCxt->pPlanCxt->pMsg, pCxt->pPlanCxt->msgLen, pFormat, args);
    va_end(args);
  }
  return errCode;
}

static int32_t rewriteExprForSelect(SNode* pExpr, SSelectStmt* pSelect, ESqlClause clause) {
  nodesWalkExpr(pExpr, doNameExpr, NULL);
  bool isPartitionBy = (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? true : false;
  SNode*          pOrderByFirstExpr = (pSelect->pOrderByList && pSelect->pOrderByList->length > 0)
                                          ? ((SOrderByExprNode*)nodesListGetNode(pSelect->pOrderByList, 0))->pExpr
                                          : NULL;
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                         .pExprs = NULL,
                         .pOutputs = NULL,
                         .isPartitionBy = isPartitionBy,
                         .pOrderByFirstExpr = pOrderByFirstExpr,
                         .pSelect = pSelect};
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
      SNode* pNew = NULL;
      code = nodesCloneNode(pExpr, &pNew);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeStrictAppend(pRewriteExpr, pNew);
      }
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
  SNode*     pOrderByFirstExpr =
      (pSelect->pOrderByList && pSelect->pOrderByList->length > 0)
          ? ((SOrderByExprNode*)nodesListGetNode(pSelect->pOrderByList, 0))->pExpr : NULL;

  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                         .pExprs = pExprs,
                         .pOutputs = NULL,
                         .isPartitionBy = isPartitionBy,
                         .pOrderByFirstExpr = pOrderByFirstExpr,
                         .pSelect = pSelect};
  if (NULL != pRewriteExprs) {
    cxt.pOutputs = taosMemoryCalloc(LIST_LENGTH(pExprs), sizeof(bool));
    if (NULL == cxt.pOutputs) {
      return terrno;
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
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                         .pExprs = pExprs,
                         .pOutputs = NULL,
                         .isPartitionBy = false,
                         .pOrderByFirstExpr = NULL,
                         .pSelect = NULL};
  nodesRewriteExpr(pTarget, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t rewriteExprs(SNodeList* pExprs, SNodeList* pTarget) {
  nodesWalkExprs(pExprs, doNameExpr, NULL);
  SRewriteExprCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                         .pExprs = pExprs,
                         .pOutputs = NULL,
                         .isPartitionBy = false,
                         .pOrderByFirstExpr = NULL,
                         .pSelect = NULL};
  nodesRewriteExprs(pTarget, doRewriteExpr, &cxt);
  return cxt.errCode;
}

static int32_t pushLogicNode(SLogicPlanContext* pCxt, SLogicNode** pOldRoot, SLogicNode* pNewRoot) {
  if (NULL == pNewRoot->pChildren) {
    int32_t code = nodesMakeList(&pNewRoot->pChildren);
    if (NULL == pNewRoot->pChildren) {
      return code;
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
  if (pCxt->pPlanCxt->topicQuery) {
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

static bool hasPkInTable(const STableMeta* pTableMeta) {
  return pTableMeta->tableInfo.numOfColumns >= 2 && pTableMeta->schema[1].flags & COL_IS_KEY;
}

static SNode* createFirstCol(SRealTableNode* pTable, const SSchema* pSchema) {
  SColumnNode* pCol = NULL;
  terrno = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType.type = pSchema->type;
  pCol->node.resType.bytes = pSchema->bytes;
  pCol->tableId = pTable->pMeta->uid;
  pCol->colId = pSchema->colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  pCol->isPk = pSchema->flags & COL_IS_KEY;
  pCol->tableHasPk = hasPkInTable(pTable->pMeta);
  pCol->numOfPKs = pTable->pMeta->tableInfo.numOfPKs;
  tstrncpy(pCol->colName, pSchema->name, TSDB_COL_NAME_LEN);
  return (SNode*)pCol;
}

static SNode* createInsColsScanCol(SRealTableNode* pTable, const SSchema* pSchema) {
  SColumnNode* pCol = NULL;
  terrno = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType.type = pSchema->type;
  pCol->node.resType.bytes = pSchema->bytes;
  pCol->tableId = pTable->pMeta->uid;
  pCol->colId = pSchema->colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->colName, pSchema->name, TSDB_COL_NAME_LEN);
  return (SNode*)pCol;
}

static SNode* createVtbFirstCol(SVirtualTableNode* pTable, const SSchema* pSchema) {
  SColumnNode* pCol = NULL;
  terrno = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType.type = pSchema->type;
  pCol->node.resType.bytes = pSchema->bytes;
  pCol->tableId = pTable->pMeta->uid;
  pCol->colId = pSchema->colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
  pCol->isPk = pSchema->flags & COL_IS_KEY;
  pCol->tableHasPk = false;
  pCol->numOfPKs = 0;
  pCol->isPrimTs = true;
  tstrncpy(pCol->colName, pSchema->name, TSDB_COL_NAME_LEN);
  return (SNode*)pCol;
}

static int32_t addVtbPrimaryTsCol(SVirtualTableNode* pTable, SNodeList** pCols) {
  bool     found = false;
  SNode*   pCol = NULL;
  SSchema* pSchema = &pTable->pMeta->schema[0];
  FOREACH(pCol, *pCols) {
    if (pSchema->colId == ((SColumnNode*)pCol)->colId) {
      found = true;
      break;
    }
  }

  if (!found) {
    return nodesListMakeStrictAppend(pCols, createVtbFirstCol(pTable, pSchema));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addInsColumnScanCol(SRealTableNode* pTable, SNodeList** pCols) {
  for (int32_t i = 1; i <= 9; i++) {
    PLAN_ERR_RET(nodesListMakeStrictAppend(pCols, createInsColsScanCol(pTable, &pTable->pMeta->schema[i])));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addPrimaryTsCol(SRealTableNode* pTable, SNodeList** pCols) {
  bool   found = false;
  SNode* pCol = NULL;
  FOREACH(pCol, *pCols) {
    if (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pCol)->colId) {
      found = true;
      break;
    }
  }

  if (!found) {
    return nodesListMakeStrictAppend(pCols, createFirstCol(pTable, pTable->pMeta->schema));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addSystableFirstCol(SRealTableNode* pTable, SNodeList** pCols) {
  if (LIST_LENGTH(*pCols) > 0) {
    return TSDB_CODE_SUCCESS;
  }
  return nodesListMakeStrictAppend(pCols, createFirstCol(pTable, pTable->pMeta->schema));
}

static int32_t addPrimaryKeyCol(SRealTableNode* pTable, SNodeList** pCols) {
  bool     found = false;
  SNode*   pCol = NULL;
  SSchema* pSchema = &pTable->pMeta->schema[PRIMARY_COLUMN_SLOT];
  FOREACH(pCol, *pCols) {
    if (pSchema->colId == ((SColumnNode*)pCol)->colId) {
      found = true;
      break;
    }
  }

  if (!found) {
    return nodesListMakeStrictAppend(pCols, createFirstCol(pTable, pSchema));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addDefaultScanCol(SRealTableNode* pTable, SNodeList** pCols) {
  if (TSDB_SYSTEM_TABLE == pTable->pMeta->tableType) {
    return addSystableFirstCol(pTable, pCols);
  }
  int32_t code = addPrimaryTsCol(pTable, pCols);
  if (code == TSDB_CODE_SUCCESS && hasPkInTable(pTable->pMeta)) {
    code = addPrimaryKeyCol(pTable, pCols);
  }
  return code;
}

static int32_t makeScanLogicNode(SLogicPlanContext* pCxt, SRealTableNode* pRealTable, bool hasRepeatScanFuncs,
                                 SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan);
  if (NULL == pScan) {
    return code;
  }

  TSWAP(pScan->pVgroupList, pRealTable->pVgroupList);
  TSWAP(pScan->pSmaIndexes, pRealTable->pSmaIndexes);
  TSWAP(pScan->pTsmas, pRealTable->pTsmas);
  TSWAP(pScan->pTsmaTargetTbVgInfo, pRealTable->tsmaTargetTbVgInfo);
  TSWAP(pScan->pTsmaTargetTbInfo, pRealTable->tsmaTargetTbInfo);
  pScan->tableId = pRealTable->pMeta->uid;
  pScan->stableId = pRealTable->pMeta->suid;
  pScan->tableType = pRealTable->pMeta->tableType;
  pScan->scanSeq[0] = hasRepeatScanFuncs ? 2 : 1;
  pScan->scanSeq[1] = 0;
  TAOS_SET_OBJ_ALIGNED(&pScan->scanRange, TSWINDOW_INITIALIZER);
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->pPlanCxt->acctId;
  tstrncpy(pScan->tableName.dbname, pRealTable->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pScan->tableName.tname, pRealTable->table.tableName, TSDB_TABLE_NAME_LEN);
  pScan->showRewrite = pCxt->pPlanCxt->showRewrite;
  pScan->ratio = pRealTable->ratio;
  pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
  pScan->cacheLastMode = pRealTable->cacheLastMode;

  *pLogicNode = (SLogicNode*)pScan;

  return TSDB_CODE_SUCCESS;
}

static bool needScanDefaultCol(EScanType scanType) { return SCAN_TYPE_TABLE_COUNT != scanType; }

static int32_t updateScanNoPseudoRefAfterGrp(SSelectStmt* pSelect, SScanLogicNode* pScan, SRealTableNode* pRealTable) {
  if (NULL == pScan->pScanPseudoCols || pScan->pScanPseudoCols->length <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pList = NULL;
  int32_t code = 0;
  if (NULL == pSelect->pPartitionByList || pSelect->pPartitionByList->length <= 0) {
    if (NULL == pSelect->pGroupByList || pSelect->pGroupByList->length <= 0) {
      return TSDB_CODE_SUCCESS;
    }

    code = nodesCollectColumns(pSelect, SQL_CLAUSE_GROUP_BY, pRealTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                               &pList);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, pRealTable->table.tableAlias, fmIsScanPseudoColumnFunc,
                               &pList);
    }
    if (TSDB_CODE_SUCCESS == code && (NULL == pList || pList->length <= 0)) {
      pScan->noPseudoRefAfterGrp = true;
    }    
    goto _return;    
  }

  code = nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, pRealTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                             &pList);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_PARTITION_BY, pRealTable->table.tableAlias, fmIsScanPseudoColumnFunc,
                             &pList);
  }
  
  if (TSDB_CODE_SUCCESS == code && (NULL == pList || pList->length <= 0)) {
    pScan->noPseudoRefAfterGrp = true;
  }    

_return:

  nodesDestroyList(pList);
  return code;
}

bool hasExternalWindowDerivedFromSubquery(SSelectStmt* pSelect);

static int32_t createScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable,
                                   SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = makeScanLogicNode(pCxt, pRealTable, pSelect->hasRepeatScanFuncs, (SLogicNode**)&pScan);

  pScan->placeholderType = pRealTable->placeholderType;
  pScan->phTbnameScan = (pRealTable->pMeta->tableType == TSDB_SUPER_TABLE && pRealTable->placeholderType == SP_PARTITION_TBNAME);
  pScan->node.groupAction = GROUP_ACTION_NONE;
  pScan->node.resultDataOrder = (pRealTable->pMeta->tableType == TSDB_SUPER_TABLE) ? DATA_ORDER_LEVEL_IN_BLOCK : DATA_ORDER_LEVEL_GLOBAL;

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pSelect->pTimeRange, (SNode**)&pScan->pTimeRange);
  }

  if (pRealTable->placeholderType == SP_PARTITION_ROWS) {
    code = nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, COLLECT_COL_TYPE_ALL,
                               &pCxt->pPlanCxt->streamCxt.triggerScanList);
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
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_FROM, pRealTable->table.tableAlias, fmIsScanPseudoColumnFunc,
                             &pScan->pScanPseudoCols);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = updateScanNoPseudoRefAfterGrp(pSelect, pScan, pRealTable);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pScan->scanType = getScanType(pCxt, pScan->pScanPseudoCols, pScan->pScanCols, pScan->tableType, pSelect->tagScan);
    if (pScan->scanType == SCAN_TYPE_SYSTEM_TABLE) {
      SET_SYS_SCAN_FLAG(pCxt->pPlanCxt->sysScanFlag);

      if (strcmp(tNameGetTableName(&pScan->tableName), TSDB_INS_TABLE_DATABASES) == 0 ||
          strcmp(tNameGetTableName(&pScan->tableName), TSDB_INS_TABLE_LICENCES) == 0 ||
          strcmp(tNameGetTableName(&pScan->tableName), TSDB_PERFS_TABLE_QUERIES) == 0) {
        SET_HSYS_SCAN_FLAG(pCxt->pPlanCxt->sysScanFlag);
      }
    }

    pCxt->pPlanCxt->hasScan = true;
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code) {
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
    code = addDefaultScanCol(pRealTable, &pScan->pScanCols);
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
    if (pScan->tableType == TSDB_CHILD_TABLE) {
      isCountByTag = true;
    }
  }
  pScan->isCountByTag = isCountByTag;

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pScan;
    pScan->paraTablesSort = getParaTablesSortOptHint(pSelect->pHint);
    pScan->smallDataTsSort = getSmallDataTsSortOptHint(pSelect->pHint);
    // pCxt->hasScan = true;
  } else {
    nodesDestroyNode((SNode*)pScan);
  }

  pScan->virtualStableScan = false;
  return code;
}

static int32_t createRefScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SRealTableNode* pRealTable,
                                      SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = TSDB_CODE_SUCCESS;

  PLAN_ERR_RET(makeScanLogicNode(pCxt, pRealTable, pSelect->hasRepeatScanFuncs, (SLogicNode**)&pScan));
  pScan->node.groupAction = GROUP_ACTION_NONE;
  pScan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;

  PLAN_ERR_RET(addDefaultScanCol(pRealTable, &pScan->pScanCols));

  pScan->scanType = getScanType(pCxt, pScan->pScanPseudoCols, pScan->pScanCols, pScan->tableType, pSelect->tagScan);

  PLAN_ERR_RET(nodesCloneNode(pSelect->pTimeRange, (SNode**)&pScan->pTimeRange));

  SNode *pTsCol = nodesListGetNode(pScan->pScanCols, 0);
  ((SColumnNode*)pTsCol)->hasDep = true;
  tstrncpy(((SColumnNode*)pTsCol)->dbName, pRealTable->table.dbName, TSDB_DB_NAME_LEN);
  *pLogicNode = (SLogicNode*)pScan;
  // pCxt->hasScan = true;

  return code;
}

static int32_t createSubqueryLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, STempTableNode* pTable,
                                       SLogicNode** pLogicNode) {
  return createQueryLogicNode(pCxt, pTable->pSubquery, pLogicNode);
}

int32_t collectJoinResColumns(SSelectStmt* pSelect, SJoinLogicNode* pJoin, SNodeList** pCols) {
  SSHashObj* pTables = NULL;
  int32_t    code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 0), &pTables);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  code = collectTableAliasFromNodes(nodesListGetNode(pJoin->node.pChildren, 1), &pTables);
  if (TSDB_CODE_SUCCESS != code) {
    tSimpleHashCleanup(pTables);
    return code;
  } else {
    code = nodesCollectColumnsExt(pSelect, SQL_CLAUSE_WHERE, pTables, COLLECT_COL_TYPE_ALL, pCols);
  }

  tSimpleHashCleanup(pTables);

  return code;
}

static int32_t createJoinLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SJoinTableNode* pJoinTable,
                                   SLogicNode** pLogicNode) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SJoinLogicNode* pJoin = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_JOIN, (SNode**)&pJoin);
  if (NULL == pJoin) {
    return code;
  }

  pJoin->joinType = pJoinTable->joinType;
  pJoin->subType = pJoinTable->subType;
  pJoin->joinAlgo = JOIN_ALGO_UNKNOWN;
  pJoin->isSingleTableJoin = pJoinTable->table.singleTable;
  pJoin->hasSubQuery = pJoinTable->hasSubQuery;
  pJoin->node.inputTsOrder = ORDER_ASC;
  pJoin->node.groupAction = GROUP_ACTION_CLEAR;
  pJoin->hashJoinHint = getHashJoinOptHint(pSelect->pHint);
  pJoin->batchScanHint = getBatchScanOptionFromHint(pSelect->pHint);
  pJoin->node.requireDataOrder = (pJoin->hashJoinHint || pJoinTable->leftNoOrderedSubQuery || pJoinTable->rightNoOrderedSubQuery) ? DATA_ORDER_LEVEL_NONE : DATA_ORDER_LEVEL_GLOBAL;
  pJoin->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
  pJoin->isLowLevelJoin = pJoinTable->isLowLevelJoin;
  pJoin->leftNoOrderedSubQuery = pJoinTable->leftNoOrderedSubQuery;
  pJoin->rightNoOrderedSubQuery = pJoinTable->rightNoOrderedSubQuery;
  
  code = nodesCloneNode(pJoinTable->pWindowOffset, &pJoin->pWindowOffset);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pJoinTable->pJLimit, &pJoin->pJLimit);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pJoinTable->addPrimCond, &pJoin->addPrimEqCond);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMakeList(&pJoin->node.pChildren);
  }
  pJoin->seqWinGroup =
      (JOIN_STYPE_WIN == pJoinTable->subType) && (pSelect->hasAggFuncs || pSelect->hasIndefiniteRowsFunc);

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
    code = nodesCloneNode(pJoinTable->pOnCond, &pJoin->pFullOnCond);
  }

#if 0
  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    SNodeList* pColList = NULL;
//    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pLeft) && !pJoin->isLowLevelJoin) {
    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pLeft)) {
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
//    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pRight) && !pJoin->isLowLevelJoin) {
    if (QUERY_NODE_REAL_TABLE == nodeType(pJoinTable->pRight)) {
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

#else
  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    SNodeList* pColList = NULL;
    code = collectJoinResColumns(pSelect, pJoin, &pColList);
    if (TSDB_CODE_SUCCESS == code && NULL != pColList) {
      code = createColumnByRewriteExprs(pColList, &pJoin->node.pTargets);
    }
    nodesDestroyList(pColList);
  }

  if (TSDB_CODE_SUCCESS == code) {
    rewriteTargetsWithResId(pJoin->node.pTargets);
  }

  if (NULL == pJoin->node.pTargets && NULL != pLeft) {
    code = nodesCloneList(pLeft->pTargets, &pJoin->node.pTargets);
  }

#endif

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pJoin;
  } else {
    nodesDestroyNode((SNode*)pJoin);
  }

  return code;
}

static void buildRefTableKey(char* buf, size_t bufSize, const char* dbName, const char* tableName) {
  size_t dbLen = strnlen(dbName, TSDB_DB_NAME_LEN);
  size_t tbLen = strnlen(tableName, TSDB_TABLE_NAME_LEN);
  if (dbLen + 1 + tbLen >= bufSize) {
    buf[0] = '\0';
    return;
  }
  if (snprintf(buf, bufSize, "%.*s.%.*s", (int)dbLen, dbName, (int)tbLen, tableName) >= (int)bufSize) {
    buf[0] = '\0';
  }
}

static int32_t findRefTableNode(SHashObj* pRefTableMap, const char* dbName, const char* tableName,
                                SNode** pRefTable) {
  char tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
  buildRefTableKey(tableNameKey, sizeof(tableNameKey), dbName, tableName);
  if (tableNameKey[0] == '\0') {
    return TSDB_CODE_NOT_FOUND;
  }
  SNode** ppRefTable = (SNode**)taosHashGet(pRefTableMap, tableNameKey, strlen(tableNameKey));
  if (NULL == ppRefTable) {
    return TSDB_CODE_NOT_FOUND;
  }
  return nodesCloneNode(*ppRefTable, pRefTable);
}

static int32_t findRefColId(SNode *pRefTable, const char *colName, col_id_t *colId, int32_t *colIdx) {
  SRealTableNode *pRealTable = (SRealTableNode*)pRefTable;
  int32_t totalCols = pRealTable->pMeta->tableInfo.numOfColumns + pRealTable->pMeta->tableInfo.numOfTags;
  for (int32_t i = 0; i < totalCols; ++i) {
    if (0 == strcasecmp(pRealTable->pMeta->schema[i].name, colName)) {
      *colId = pRealTable->pMeta->schema[i].colId;
      *colIdx = i;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_NOT_FOUND;
}

static int32_t scanAddCol(SLogicNode* pLogicNode, SColRef* colRef, STableNode* pVirtualTableNode, const SSchema* pSchema,
                          col_id_t colId, const SSchema* pRefSchema, bool isTagInRefTable) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SColumnNode    *pRefTableScanCol = NULL;
  SScanLogicNode *pLogicScan = (SScanLogicNode*)pLogicNode;
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pRefTableScanCol));
  if (colRef) {
    tstrncpy(pRefTableScanCol->tableAlias, colRef->refTableName, sizeof(pRefTableScanCol->tableAlias));
    tstrncpy(pRefTableScanCol->dbName, colRef->refDbName, sizeof(pRefTableScanCol->dbName));
    tstrncpy(pRefTableScanCol->tableName, colRef->refTableName, sizeof(pRefTableScanCol->tableName));
    tstrncpy(pRefTableScanCol->colName, colRef->refColName, sizeof(pRefTableScanCol->colName));
  } else {
    tstrncpy(pRefTableScanCol->tableAlias, pVirtualTableNode->tableAlias, sizeof(pRefTableScanCol->tableAlias));
    tstrncpy(pRefTableScanCol->dbName, pVirtualTableNode->dbName, sizeof(pRefTableScanCol->dbName));
    tstrncpy(pRefTableScanCol->tableName, pVirtualTableNode->tableName, sizeof(pRefTableScanCol->tableName));
    tstrncpy(pRefTableScanCol->colName, pSchema->name, sizeof(pRefTableScanCol->colName));
  }

  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    SNode *pTsCol = nodesListGetNode(pLogicScan->pScanCols, 0);
    tstrncpy(((SColumnNode*)pTsCol)->dbName, pRefTableScanCol->dbName, sizeof(((SColumnNode*)pTsCol)->dbName));
    ((SColumnNode*)pTsCol)->hasRef = false;
    ((SColumnNode*)pTsCol)->hasDep = true;
    goto _return;
  }

  SNodeList* pTargetList = isTagInRefTable ? pLogicScan->pScanPseudoCols : pLogicScan->pScanCols;

  // eliminate duplicate scan cols.
  SNode *pCol = NULL;
  FOREACH(pCol, pTargetList) {
    if (0 == strncmp(((SColumnNode*)pCol)->colName, pRefTableScanCol->colName, TSDB_COL_NAME_LEN) &&
        0 == strncmp(((SColumnNode*)pCol)->tableName, pRefTableScanCol->tableName, TSDB_TABLE_NAME_LEN) &&
        0 == strncmp(((SColumnNode*)pCol)->dbName, pRefTableScanCol->dbName, TSDB_DB_NAME_LEN)) {
      nodesDestroyNode((SNode*)pRefTableScanCol);
      return TSDB_CODE_SUCCESS;
    }
  }
  pRefTableScanCol->colId = colId;
  pRefTableScanCol->tableId = pLogicScan->tableId;
  pRefTableScanCol->tableType = pLogicScan->tableType;
  pRefTableScanCol->node.resType.type = pSchema->type;
  if (pRefSchema && IS_VAR_DATA_TYPE(pSchema->type)) {
    pRefTableScanCol->node.resType.bytes = TMAX(pSchema->bytes, pRefSchema->bytes);
    planDebug("scanAddCol: col %s, vtb bytes=%d, ref bytes=%d, final bytes=%d",
              pRefTableScanCol->colName, pSchema->bytes, pRefSchema->bytes, pRefTableScanCol->node.resType.bytes);
  } else {
    pRefTableScanCol->node.resType.bytes = pSchema->bytes;
  }
  pRefTableScanCol->isPk = false;
  pRefTableScanCol->tableHasPk = false;
  pRefTableScanCol->numOfPKs = 0;
  pRefTableScanCol->hasRef = false;
  pRefTableScanCol->hasDep = true;

  if (isTagInRefTable) {
    pRefTableScanCol->colType = COLUMN_TYPE_TAG;
    PLAN_ERR_JRET(nodesListMakeAppend(&pLogicScan->pScanPseudoCols, (SNode*)pRefTableScanCol));
  } else {
    pRefTableScanCol->colType = COLUMN_TYPE_COLUMN;
    PLAN_ERR_JRET(nodesListMakeAppend(&pLogicScan->pScanCols, (SNode*)pRefTableScanCol));
  }
  return code;
_return:
  nodesDestroyNode((SNode*)pRefTableScanCol);
  return code;
}

static int32_t checkColRefType(const SSchema* vtbSchema, const SSchemaExt* vtbSchemaExt, const SSchema* refSchema,
                               const SSchemaExt* refSchemaExt) {
  SDataType vtbType = {0};
  SDataType refType = {0};
  schemaToRefDataType(vtbSchema, NULL != vtbSchemaExt ? vtbSchemaExt->typeMod : 0, &vtbType);
  schemaToRefDataType(refSchema, NULL != refSchemaExt ? refSchemaExt->typeMod : 0, &refType);

  if (vtbType.type != refType.type) {
    qError("virtual table column:%s type mismatch, virtual table column type:%d, bytes:%d, "
        "ref table column:%s, type:%d, bytes:%d",
        vtbSchema->name, vtbType.type, vtbType.bytes, refSchema->name, refType.type, refType.bytes);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
  }
  if (!IS_VAR_DATA_TYPE(vtbType.type) && vtbType.bytes != refType.bytes) {
    qError("virtual table column:%s bytes mismatch, virtual table column type:%d, bytes:%d, "
        "ref table column:%s, type:%d, bytes:%d",
        vtbSchema->name, vtbType.type, vtbType.bytes, refSchema->name, refType.type, refType.bytes);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
  }
  if (IS_DECIMAL_TYPE(vtbType.type) &&
      (vtbType.precision != refType.precision || vtbType.scale != refType.scale)) {
    qError("virtual table column:%s decimal type mismatch, virtual table column type:%d, precision:%u, scale:%u, "
           "ref table column:%s, type:%d, precision:%u, scale:%u",
           vtbSchema->name, vtbType.type, vtbType.precision, vtbType.scale, refSchema->name, refType.type,
           refType.precision, refType.scale);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addSubScanNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SVirtualTableNode* pVirtualTable,
                              int32_t colRefIndex, int32_t schemaIndex, SHashObj* refTablesMap,
                              SHashObj* refTableNodeMap) {
  int32_t     code = TSDB_CODE_SUCCESS;
  col_id_t    colId = 0;
  int32_t     colIdx = 0;
  SColRef    *pColRef = &pVirtualTable->pMeta->colRef[colRefIndex];
  SNode      *pRefTable = NULL;
  SLogicNode *pRefScan = NULL;
  bool        put = false;

  PLAN_ERR_JRET(findRefTableNode(refTableNodeMap, pColRef->refDbName, pColRef->refTableName, &pRefTable));
  PLAN_ERR_JRET(findRefColId(pRefTable, pColRef->refColName, &colId, &colIdx));

  char tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
  TSlice tableNameBuf = {0};
  sliceInit(&tableNameBuf, tableNameKey, sizeof(tableNameKey));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, pColRef->refDbName, strlen(pColRef->refDbName)));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, ".", 1));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, pColRef->refTableName, strlen(pColRef->refTableName)));

  const SSchema* pRefColSchema = &((SRealTableNode*)pRefTable)->pMeta->schema[colIdx];
  bool isTagInRefTable = (colIdx >= ((SRealTableNode*)pRefTable)->pMeta->tableInfo.numOfColumns);
  const SSchemaExt* pRefSchemaExt =
      (((SRealTableNode*)pRefTable)->pMeta->schemaExt && colIdx < ((SRealTableNode*)pRefTable)->pMeta->tableInfo.numOfColumns)
          ? ((SRealTableNode*)pRefTable)->pMeta->schemaExt + colIdx
          : NULL;
  const SSchemaExt* pVtbSchemaExt =
      (pVirtualTable->pMeta->schemaExt && schemaIndex < pVirtualTable->pMeta->tableInfo.numOfColumns)
          ? pVirtualTable->pMeta->schemaExt + schemaIndex
          : NULL;
  SLogicNode **ppRefScan = (SLogicNode **)taosHashGet(refTablesMap, &tableNameKey, strlen(tableNameKey));
  if (NULL == ppRefScan) {
    PLAN_ERR_JRET(createRefScanLogicNode(pCxt, pSelect, (SRealTableNode*)pRefTable, &pRefScan));
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], pVtbSchemaExt, pRefColSchema,
                                  pRefSchemaExt));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId, pRefColSchema, isTagInRefTable));
    PLAN_ERR_JRET(taosHashPut(refTablesMap, &tableNameKey, strlen(tableNameKey), &pRefScan, POINTER_BYTES));
    put = true;
  } else {
    pRefScan = *ppRefScan;
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], pVtbSchemaExt, pRefColSchema, pRefSchemaExt));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId, pRefColSchema, isTagInRefTable));
  }

  nodesDestroyNode((SNode*)pRefTable);
  return code;
_return:
  nodesDestroyNode((SNode*)pRefTable);
  if (!put) {
    nodesDestroyNode((SNode*)pRefScan);
  }
  return code;
}

static int32_t addSubScanNodeByRef(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SVirtualTableNode* pVirtualTable,
                                   SColRef* pColRef, int32_t schemaIndex, SHashObj* refTablesMap,
                                   SHashObj* refTableNodeMap) {
  int32_t     code = TSDB_CODE_SUCCESS;
  col_id_t    colId = 0;
  int32_t     colIdx = 0;
  SNode*      pRefTable = NULL;
  SLogicNode* pRefScan = NULL;
  bool        put = false;

  PLAN_ERR_JRET(findRefTableNode(refTableNodeMap, pColRef->refDbName, pColRef->refTableName, &pRefTable));
  PLAN_ERR_JRET(findRefColId(pRefTable, pColRef->refColName, &colId, &colIdx));

  char   tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
  TSlice tableNameBuf = {0};
  sliceInit(&tableNameBuf, tableNameKey, sizeof(tableNameKey));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, pColRef->refDbName, strlen(pColRef->refDbName)));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, ".", 1));
  PLAN_ERR_JRET(sliceAppend(&tableNameBuf, pColRef->refTableName, strlen(pColRef->refTableName)));

  SLogicNode** ppRefScan = (SLogicNode**)taosHashGet(refTablesMap, &tableNameKey, strlen(tableNameKey));
  const SSchema* pRefColSchema = &((SRealTableNode*)pRefTable)->pMeta->schema[colIdx];

  const SSchemaExt* pRefSchemaExt =
      (((SRealTableNode*)pRefTable)->pMeta->schemaExt && colIdx < ((SRealTableNode*)pRefTable)->pMeta->tableInfo.numOfColumns)
          ? ((SRealTableNode*)pRefTable)->pMeta->schemaExt + colIdx
          : NULL;
  const SSchemaExt* pVtbSchemaExt =
      (pVirtualTable->pMeta->schemaExt && schemaIndex < pVirtualTable->pMeta->tableInfo.numOfColumns)
          ? pVirtualTable->pMeta->schemaExt + schemaIndex
          : NULL;
  bool isTagInRefTable = (colIdx >= ((SRealTableNode*)pRefTable)->pMeta->tableInfo.numOfColumns);
  if (NULL == ppRefScan) {
    PLAN_ERR_JRET(createRefScanLogicNode(pCxt, pSelect, (SRealTableNode*)pRefTable, &pRefScan));
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], pVtbSchemaExt, pRefColSchema, pRefSchemaExt));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId,
                             pRefColSchema, isTagInRefTable));
    PLAN_ERR_JRET(taosHashPut(refTablesMap, &tableNameKey, strlen(tableNameKey), &pRefScan, POINTER_BYTES));
    put = true;
  } else {
    pRefScan = *ppRefScan;
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], pVtbSchemaExt, pRefColSchema, pRefSchemaExt));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId,
                             pRefColSchema, isTagInRefTable));
  }

  nodesDestroyNode((SNode*)pRefTable);
  return code;
_return:
  nodesDestroyNode((SNode*)pRefTable);
  if (!put) {
    nodesDestroyNode((SNode*)pRefScan);
  }
  return code;
}

static int32_t makeVirtualScanLogicNode(SLogicPlanContext* pCxt, SVirtualTableNode* pVirtualTable,
                                        SVirtualScanLogicNode* pScan) {
  TSWAP(pScan->pVgroupList, pVirtualTable->pVgroupList);
  pScan->tableId = pVirtualTable->pMeta->uid;
  pScan->stableId = pVirtualTable->pMeta->suid;
  pScan->tableType = pVirtualTable->pMeta->tableType;
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->pPlanCxt->acctId;
  tstrncpy(pScan->tableName.dbname, pVirtualTable->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pScan->tableName.tname, pVirtualTable->table.tableName, TSDB_TABLE_NAME_LEN);
  return TSDB_CODE_SUCCESS;
}

static void destroyScanLogicNode(void* data) {
  if (data == NULL) {
    return;
  }
  SScanLogicNode* pNode = *(SScanLogicNode **)data;
  nodesDestroyNode((SNode*)pNode);
}

static int32_t findColRefIndex(SColRef* pColRef, SVirtualTableNode* pVirtualTable, col_id_t colId) {
  for (int32_t i = 0; i < pVirtualTable->pMeta->numOfColRefs; i++) {
    if (pColRef[i].hasRef && pColRef[i].id == colId) {
      return i;
    }
  }
  return -1;
}

static int32_t findTagRefIndex(SColRef* pTagRef, SVirtualTableNode* pVirtualTable, col_id_t colId) {
  for (int32_t i = 0; i < pVirtualTable->pMeta->numOfTagRefs; i++) {
    if (pTagRef[i].hasRef && pTagRef[i].id == colId) {
      return i;
    }
  }
  return -1;
}

static int32_t findSchemaIndex(const SSchema* pSchema, int32_t numOfColumns, col_id_t colId);

static int32_t findTagRefIndexByName(SColRef* pTagRef, SVirtualTableNode* pVirtualTable, const char* colName) {
  if (colName == NULL || colName[0] == '\0') {
    return -1;
  }

  int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
  for (int32_t i = 0; i < pVirtualTable->pMeta->numOfTagRefs; ++i) {
    if (!pTagRef[i].hasRef) {
      continue;
    }
    if ((pTagRef[i].colName[0] != '\0' && 0 == strcasecmp(pTagRef[i].colName, colName)) ||
        (pTagRef[i].refColName[0] != '\0' && 0 == strcasecmp(pTagRef[i].refColName, colName))) {
      return i;
    }
    int32_t schemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pTagRef[i].id);
    if (schemaIndex < 0) {
      continue;
    }
    if (0 == strcasecmp(pVirtualTable->pMeta->schema[schemaIndex].name, colName)) {
      return i;
    }
  }
  return -1;
}

static int32_t findColRefIndexByName(SColRef* pColRef, SVirtualTableNode* pVirtualTable, const char* colName) {
  if (colName == NULL || colName[0] == '\0') {
    return -1;
  }

  for (int32_t i = 0; i < pVirtualTable->pMeta->numOfColRefs; ++i) {
    if (!pColRef[i].hasRef) {
      continue;
    }
    if ((pColRef[i].colName[0] != '\0' && 0 == strcasecmp(pColRef[i].colName, colName)) ||
        (pColRef[i].refColName[0] != '\0' && 0 == strcasecmp(pColRef[i].refColName, colName))) {
      return i;
    }
  }

  return -1;
}

typedef struct {
  SVirtualTableNode* pVirtualTable;
} SSyncVTableColRefCxt;

static bool isSameVirtualTableCol(SVirtualTableNode* pVirtualTable, SColumnNode* pCol) {
  if (NULL == pVirtualTable || NULL == pCol) {
    return false;
  }

  if (pCol->tableId == pVirtualTable->pMeta->uid) {
    return true;
  }

  return 0 == strcmp(pCol->dbName, pVirtualTable->table.dbName) &&
         0 == strcmp(pCol->tableName, pVirtualTable->table.tableName);
}

static EDealRes syncVirtualTableColRefImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  SSyncVTableColRefCxt* pCxt = (SSyncVTableColRefCxt*)pContext;
  SColumnNode*          pCol = (SColumnNode*)pNode;
  SColRef*              pRef = NULL;

  if (!isSameVirtualTableCol(pCxt->pVirtualTable, pCol)) {
    return DEAL_RES_CONTINUE;
  }

  int32_t colRefIndex = findColRefIndex(pCxt->pVirtualTable->pMeta->colRef, pCxt->pVirtualTable, pCol->colId);
  if (colRefIndex < 0) {
    colRefIndex = findColRefIndexByName(pCxt->pVirtualTable->pMeta->colRef, pCxt->pVirtualTable, pCol->colName);
  }
  if (colRefIndex >= 0) {
    pRef = &pCxt->pVirtualTable->pMeta->colRef[colRefIndex];
  } else {
    int32_t tagRefIndex = findTagRefIndex(pCxt->pVirtualTable->pMeta->tagRef, pCxt->pVirtualTable, pCol->colId);
    if (tagRefIndex < 0) {
      tagRefIndex = findTagRefIndexByName(pCxt->pVirtualTable->pMeta->tagRef, pCxt->pVirtualTable, pCol->colName);
    }
    if (tagRefIndex >= 0) {
      pRef = &pCxt->pVirtualTable->pMeta->tagRef[tagRefIndex];
    }
  }

  if (NULL != pRef && pRef->hasRef) {
    pCol->hasRef = true;
    tstrncpy(pCol->refDbName, pRef->refDbName, sizeof(pCol->refDbName));
    tstrncpy(pCol->refTableName, pRef->refTableName, sizeof(pCol->refTableName));
    tstrncpy(pCol->refColName, pRef->refColName, sizeof(pCol->refColName));
  } else {
    pCol->hasRef = false;
    pCol->refDbName[0] = '\0';
    pCol->refTableName[0] = '\0';
    pCol->refColName[0] = '\0';
  }

  return DEAL_RES_CONTINUE;
}

static int32_t syncVirtualTableColRefs(SNodeList* pExprs, SVirtualTableNode* pVirtualTable) {
  if (NULL == pExprs) {
    return TSDB_CODE_SUCCESS;
  }

  SSyncVTableColRefCxt cxt = {.pVirtualTable = pVirtualTable};
  nodesWalkExprs(pExprs, syncVirtualTableColRefImpl, &cxt);
  return TSDB_CODE_SUCCESS;
}

static int32_t syncVirtualTablePrimaryTsRef(SVirtualTableNode* pVirtualTable) {
  if (NULL == pVirtualTable || NULL == pVirtualTable->pMeta || NULL == pVirtualTable->pMeta->schema ||
      NULL == pVirtualTable->pMeta->colRef || pVirtualTable->pMeta->numOfColRefs <= 0 ||
      LIST_LENGTH(pVirtualTable->refTables) != 1) {
    return TSDB_CODE_SUCCESS;
  }

  SRealTableNode* pRefTable = (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 0);
  if (NULL == pRefTable || NULL == pRefTable->pMeta || NULL == pRefTable->pMeta->schema) {
    return TSDB_CODE_SUCCESS;
  }

  SColRef* pTsRef = &pVirtualTable->pMeta->colRef[0];
  pTsRef->hasRef = true;
  pTsRef->id = pVirtualTable->pMeta->schema[0].colId;
  tstrncpy(pTsRef->refDbName, pRefTable->table.dbName, sizeof(pTsRef->refDbName));
  tstrncpy(pTsRef->refTableName, pRefTable->table.tableName, sizeof(pTsRef->refTableName));
  tstrncpy(pTsRef->refColName, pRefTable->pMeta->schema[0].name, sizeof(pTsRef->refColName));
  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteFlatVirtualChildScanCols(SScanLogicNode* pRefScan, SVirtualTableNode* pVirtualTable) {
  if (NULL == pRefScan || NULL == pVirtualTable || NULL == pVirtualTable->pMeta) {
    return TSDB_CODE_INVALID_PARA;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pRefScan->pScanCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    SColumnNode*   pCol = (SColumnNode*)pNode;
    const SSchema* pVtbSchema = NULL;
    char           refDbName[TSDB_DB_NAME_LEN] = {0};
    char           refTableName[TSDB_TABLE_NAME_LEN] = {0};
    char           refColName[TSDB_COL_NAME_LEN] = {0};

    tstrncpy(refDbName, pCol->dbName, sizeof(refDbName));
    tstrncpy(refTableName, pCol->tableName, sizeof(refTableName));
    tstrncpy(refColName, pCol->colName, sizeof(refColName));

    if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pVtbSchema = &pVirtualTable->pMeta->schema[0];
    } else {
      for (int32_t i = 0; i < pVirtualTable->pMeta->numOfColRefs; ++i) {
        SColRef* pColRef = &pVirtualTable->pMeta->colRef[i];
        if (!pColRef->hasRef) {
          continue;
        }
        if (0 != strcmp(pColRef->refDbName, pCol->dbName) || 0 != strcmp(pColRef->refTableName, pCol->tableName) ||
            0 != strcmp(pColRef->refColName, pCol->colName)) {
          continue;
        }

        int32_t schemaIndex =
            findSchemaIndex(pVirtualTable->pMeta->schema, pVirtualTable->pMeta->tableInfo.numOfColumns, pColRef->id);
        if (schemaIndex < 0) {
          return TSDB_CODE_NOT_FOUND;
        }
        pVtbSchema = &pVirtualTable->pMeta->schema[schemaIndex];
        break;
      }
    }

    if (NULL == pVtbSchema) {
      return TSDB_CODE_NOT_FOUND;
    }

    tstrncpy(pCol->tableAlias, pVirtualTable->table.tableAlias, sizeof(pCol->tableAlias));
    tstrncpy(pCol->dbName, pVirtualTable->table.dbName, sizeof(pCol->dbName));
    tstrncpy(pCol->tableName, pVirtualTable->table.tableName, sizeof(pCol->tableName));
    tstrncpy(pCol->colName, pVtbSchema->name, sizeof(pCol->colName));
    pCol->tableId = pVirtualTable->pMeta->uid;
    pCol->tableType = pVirtualTable->pMeta->tableType;
    pCol->node.resType.type = pVtbSchema->type;
    pCol->node.resType.bytes = pVtbSchema->bytes;
    pCol->hasRef = true;
    pCol->hasDep = false;
    pCol->isPrimTs = (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID);
    tstrncpy(pCol->refDbName, refDbName, sizeof(pCol->refDbName));
    tstrncpy(pCol->refTableName, refTableName, sizeof(pCol->refTableName));
    tstrncpy(pCol->refColName, refColName, sizeof(pCol->refColName));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t findSchemaIndex(const SSchema* pSchema, int32_t numOfColumns, col_id_t colId) {
  for (int32_t i = 0; i < numOfColumns; i++) {
    if (pSchema[i].colId == colId) {
      return i;
    }
  }
  return -1;
}

static int32_t eliminateDupScanCols(SNodeList* pScanCols) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SNode*    pCols = NULL;
  SHashObj* colsMap = taosHashInit(LIST_LENGTH(pScanCols), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == colsMap) {
    return terrno;
  }

  FOREACH(pCols, pScanCols) {
    SColumnNode* pCol = (SColumnNode*)pCols;
    if (!pCol->hasRef) {
      continue;
    }
    char         key[TSDB_COL_FNAME_EX_LEN] = {0};
    TSlice       keyBuf = {0};

    sliceInit(&keyBuf, key, sizeof(key));
    PLAN_ERR_JRET(sliceAppend(&keyBuf, pCol->refDbName, strlen(pCol->refDbName)));
    PLAN_ERR_JRET(sliceAppend(&keyBuf, ".", 1));
    PLAN_ERR_JRET(sliceAppend(&keyBuf, pCol->refTableName, strlen(pCol->refTableName)));
    PLAN_ERR_JRET(sliceAppend(&keyBuf, ".", 1));
    PLAN_ERR_JRET(sliceAppend(&keyBuf, pCol->refColName, strlen(pCol->refColName)));

    if (NULL != taosHashGet(colsMap, key, strlen(key))) {
      ERASE_NODE(pScanCols);
    } else {
      PLAN_ERR_JRET(taosHashPut(colsMap, key, strlen(key), NULL, 0));
    }
  }

_return:
  taosHashCleanup(colsMap);
  return code;
}

static int32_t eliminateDupVirtualTagCols(SNodeList* pCols) {
  if (pCols == NULL || LIST_LENGTH(pCols) <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* colsMap =
      taosHashInit(LIST_LENGTH(pCols), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (colsMap == NULL) {
    return terrno;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    char         key[TSDB_COL_FNAME_EX_LEN] = {0};

    if (pCol->colId > 0) {
      snprintf(key, sizeof(key), "id:%d", pCol->colId);
    } else if (pCol->hasRef) {
      snprintf(key, sizeof(key), "ref:%s.%s.%s", pCol->refDbName, pCol->refTableName, pCol->refColName);
    } else if ('\0' != pCol->tableAlias[0]) {
      snprintf(key, sizeof(key), "alias:%s.%s", pCol->tableAlias, pCol->colName);
    } else {
      snprintf(key, sizeof(key), "name:%s", pCol->colName);
    }

    if (NULL != taosHashGet(colsMap, key, strlen(key))) {
      ERASE_NODE(pCols);
    } else {
      PLAN_ERR_JRET(taosHashPut(colsMap, key, strlen(key), NULL, 0));
    }
  }

_return:
  taosHashCleanup(colsMap);
  return code;
}

static int32_t eliminateDupTagRefSources(SNodeList* pSources) {
  if (pSources == NULL || LIST_LENGTH(pSources) <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t   code = TSDB_CODE_SUCCESS;
  SHashObj* sourceMap =
      taosHashInit(LIST_LENGTH(pSources), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (sourceMap == NULL) {
    return terrno;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pSources) {
    if (nodeType(pNode) != QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE) {
      continue;
    }

    STagRefSourceLogicNode* pSource = (STagRefSourceLogicNode*)pNode;
    STagRefColumn*          pRefCol =
        (STagRefColumn*)(pSource->pRefCols ? nodesListGetNode(pSource->pRefCols, 0) : NULL);
    char key[TSDB_COL_FNAME_EX_LEN] = {0};
    snprintf(key, sizeof(key), "%s.%s.%s", pSource->sourceTableName.dbname, pSource->sourceTableName.tname,
             pRefCol != NULL ? pRefCol->sourceColName : "");

    if (NULL != taosHashGet(sourceMap, key, strlen(key))) {
      ERASE_NODE(pSources);
    } else {
      PLAN_ERR_JRET(taosHashPut(sourceMap, key, strlen(key), NULL, 0));
    }
  }

_return:
  taosHashCleanup(sourceMap);
  return code;
}

static int32_t cloneVgroups(SVgroupsInfo **pDst, SVgroupsInfo* pSrc) {
  if (pSrc == NULL) {
    *pDst = NULL;
    return TSDB_CODE_SUCCESS;
  }
  int32_t len = VGROUPS_INFO_SIZE(pSrc);
  *pDst = taosMemoryMalloc(len);
  if (NULL == *pDst) {
    return terrno;
  }
  memcpy(*pDst, pSrc, len);
  return TSDB_CODE_SUCCESS;
}

static int32_t appendCondTagPseudoCols(SNode* pCond, SNodeList** ppPseudoCols) {
  if (pCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pCondCols = NULL;
  int32_t    code = nodesCollectColumnsFromNode(pCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  if (TSDB_CODE_SUCCESS != code || pCondCols == NULL) {
    nodesDestroyList(pCondCols);
    return code;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pCondCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN || ((SColumnNode*)pNode)->colType != COLUMN_TYPE_TAG) {
      continue;
    }

    SNode* pClone = NULL;
    code = nodesCloneNode(pNode, &pClone);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(ppPseudoCols, pClone);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  nodesDestroyList(pCondCols);
  return code;
}

// ====================================================================
// Tag Reference Filter Condition Extraction
// ====================================================================

/**
 * Context structure for checking if a tag column is used in WHERE clause or projection.
 * pTargetNode may be SColumnNode (QUERY_NODE_COLUMN) or STagRefColumn (QUERY_NODE_TAG_REF_COLUMN).
 */
typedef struct SCheckTagInFilterCtx {
  SNode* pTargetNode;
  bool   found;
} SCheckTagInFilterCtx;

/**
 * Walker function implementation for checking if a tag column is in WHERE clause or projection
 */
static EDealRes checkTagInFilterImpl(SNode* pNode, void* pContext) {
  SCheckTagInFilterCtx* pCtx = (SCheckTagInFilterCtx*)pContext;
  if (pCtx->found) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pWhereCol = (SColumnNode*)pNode;
    if (QUERY_NODE_COLUMN == nodeType(pCtx->pTargetNode)) {
      SColumnNode* pTarget = (SColumnNode*)pCtx->pTargetNode;
      if (pWhereCol->colId == pTarget->colId &&
          strcmp(pWhereCol->tableName, pTarget->tableName) == 0 &&
          strcmp(pWhereCol->dbName, pTarget->dbName) == 0) {
        pCtx->found = true;
      }
    } else if (QUERY_NODE_TAG_REF_COLUMN == nodeType(pCtx->pTargetNode)) {
      STagRefColumn* pRefCol = (STagRefColumn*)pCtx->pTargetNode;
      if (pWhereCol->colId == pRefCol->colId &&
          strcmp(pWhereCol->colName, pRefCol->colName) == 0) {
        pCtx->found = true;
      }
    }
  }

  return DEAL_RES_CONTINUE;
}

/**
 * Check if a tag column is used in WHERE clause
 */
static bool isTagUsedInFilter(SNode* pWhere, SColumnNode* pCol) {
  if (pWhere == NULL || pCol == NULL) {
    return false;
  }

  SCheckTagInFilterCtx ctx = {.pTargetNode = (SNode*)pCol, .found = false};
  nodesWalkExpr(pWhere, checkTagInFilterImpl, &ctx);
  return ctx.found;
}

/**
 * Check if a tag column is used in projection (SELECT clause)
 */
static bool isTagUsedInProjection(SNodeList* pTargets, SColumnNode* pCol) {
  if (pTargets == NULL || pCol == NULL) {
    return false;
  }

  SCheckTagInFilterCtx ctx = {.pTargetNode = (SNode*)pCol, .found = false};
  nodesWalkExprs(pTargets, checkTagInFilterImpl, &ctx);
  return ctx.found;
}

static bool isTagUsedInAnyClause(SSelectStmt* pSelect, SColumnNode* pCol, bool* pUsedInFilter, bool* pUsedInNonFilter) {
  bool usedInFilter = isTagUsedInFilter(pSelect != NULL ? pSelect->pWhere : NULL, pCol);
  bool usedInNonFilter = false;

  if (pSelect != NULL && pCol != NULL) {
    usedInNonFilter = isTagUsedInProjection(pSelect->pProjectionList, pCol) ||
                      isTagUsedInProjection(pSelect->pPartitionByList, pCol) ||
                      isTagUsedInProjection(pSelect->pGroupByList, pCol) ||
                      isTagUsedInProjection(pSelect->pOrderByList, pCol);

    if (!usedInNonFilter && pSelect->pHaving != NULL) {
      SCheckTagInFilterCtx ctx = {.pTargetNode = (SNode*)pCol, .found = false};
      nodesWalkExpr(pSelect->pHaving, checkTagInFilterImpl, &ctx);
      usedInNonFilter = ctx.found;
    }
  }

  if (pUsedInFilter != NULL) {
    *pUsedInFilter = usedInFilter;
  }
  if (pUsedInNonFilter != NULL) {
    *pUsedInNonFilter = usedInNonFilter;
  }

  return usedInFilter || usedInNonFilter;
}

static int32_t keepOnlyLocalTagPseudoCols(SVirtualTableNode* pVirtualTable, SVirtualScanLogicNode* pVtableScan) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pVtableScan->pScanPseudoCols == NULL) {
    return code;
  }

  SNodeList* pRemainPseudoCols = NULL;
  PLAN_ERR_RET(nodesMakeList(&pRemainPseudoCols));

  SNode* pNode = NULL;
  FOREACH(pNode, pVtableScan->pScanPseudoCols) {
    bool keepInPseudoCols = true;

    if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t      tagRefIndex = findTagRefIndex(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colId);
      if (tagRefIndex == -1) {
        tagRefIndex = findTagRefIndexByName(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colName);
      }
      if (tagRefIndex == -1) {
        int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
        int32_t tagSchemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pCol->colId);
        if (tagSchemaIndex >= pVirtualTable->pMeta->tableInfo.numOfColumns) {
          int32_t tagPos = tagSchemaIndex - pVirtualTable->pMeta->tableInfo.numOfColumns;
          if (tagPos >= 0 && tagPos < pVirtualTable->pMeta->numOfTagRefs &&
              pVirtualTable->pMeta->tagRef[tagPos].hasRef) {
            tagRefIndex = tagPos;
          }
        }
      }

      if (tagRefIndex != -1 && pVirtualTable->pMeta->tagRef[tagRefIndex].hasRef) {
        keepInPseudoCols = false;
      }
    }

    if (keepInPseudoCols) {
      SNode* pClone = NULL;
      PLAN_ERR_RET(nodesCloneNode(pNode, &pClone));
      PLAN_ERR_RET(nodesListMakeStrictAppend(&pRemainPseudoCols, pClone));
    }
  }

  nodesDestroyList(pVtableScan->pScanPseudoCols);
  if (LIST_LENGTH(pRemainPseudoCols) == 0) {
    nodesDestroyList(pRemainPseudoCols);
    pVtableScan->pScanPseudoCols = NULL;
  } else {
    pVtableScan->pScanPseudoCols = pRemainPseudoCols;
  }

  return code;
}

/**
 * Forward declaration
 */
static bool isNodeContainRefColumn(SNode* pNode, SNodeList* pRefCols);

/**
 * Context for extracting filter conditions related to reference columns
 */
typedef struct SExtractFilterCondCtx {
  SNodeList*  pRefCols;        // List of reference columns (tagRef or colRef)
  SNodeList*  pMatchedConds;   // Conditions that match reference columns
  bool        hasNonRefCol;    // Whether condition has non-reference columns
} SExtractFilterCondCtx;

static bool isNodeContainForeignRefColumn(SNode* pNode, SNodeList* pRefCols);

/**
 * Walker function to extract filter conditions for reference columns
 */
static EDealRes extractFilterCondImpl(SNode* pNode, void* pContext) {
  SExtractFilterCondCtx* pCtx = (SExtractFilterCondCtx*)pContext;

  ENodeType nType = nodeType(pNode);

  // Handle logic conditions (AND/OR)
  if (QUERY_NODE_LOGIC_CONDITION == nType) {
    SLogicConditionNode* pLogicCond = (SLogicConditionNode*)pNode;

    if (LOGIC_COND_TYPE_AND == pLogicCond->condType) {
      nodesWalkExprs(pLogicCond->pParameterList, extractFilterCondImpl, pContext);
    } else if (LOGIC_COND_TYPE_OR == pLogicCond->condType) {
      bool hasRefColInOr = false;
      bool hasForeignInOr = false;
      SNode* pChild = NULL;
      FOREACH(pChild, pLogicCond->pParameterList) {
        if (isNodeContainRefColumn(pChild, pCtx->pRefCols)) {
          hasRefColInOr = true;
        }
        if (isNodeContainForeignRefColumn(pChild, pCtx->pRefCols)) {
          hasForeignInOr = true;
        }
      }
      if (hasRefColInOr && !hasForeignInOr) {
        SNode* pClone = NULL;
        if (TSDB_CODE_SUCCESS == nodesCloneNode(pNode, &pClone)) {
          if (nodesListAppend(pCtx->pMatchedConds, pClone) != TSDB_CODE_SUCCESS) {
            nodesDestroyNode(pClone);
          }
        }
      }
    }
    return DEAL_RES_IGNORE_CHILD;
  }

  // Handle operator nodes
  if (QUERY_NODE_OPERATOR == nType) {
    if (isNodeContainRefColumn(pNode, pCtx->pRefCols) &&
        !isNodeContainForeignRefColumn(pNode, pCtx->pRefCols)) {
      SNode* pClone = NULL;
      if (TSDB_CODE_SUCCESS == nodesCloneNode(pNode, &pClone)) {
        if (nodesListAppend(pCtx->pMatchedConds, pClone) != TSDB_CODE_SUCCESS) {
          nodesDestroyNode(pClone);
        }
      }
    }
  }

  return DEAL_RES_CONTINUE;
}

/**
 * Check if a node contains any of the reference columns
 */
static bool isNodeContainRefColumn(SNode* pNode, SNodeList* pRefCols) {
  if (NULL == pRefCols || LIST_LENGTH(pRefCols) == 0) {
    return false;
  }

  SNode* pRefCol = NULL;
  FOREACH(pRefCol, pRefCols) {
    SCheckTagInFilterCtx ctx = {.pTargetNode = pRefCol, .found = false};
    nodesWalkExpr(pNode, checkTagInFilterImpl, &ctx);
    if (ctx.found) {
      return true;
    }
  }
  return false;
}

typedef struct SCheckForeignTagCtx {
  SNodeList* pRefCols;   // columns belonging to this TagRefSource
  bool       foundForeign;
} SCheckForeignTagCtx;

static EDealRes checkForeignTagImpl(SNode* pNode, void* pContext) {
  SCheckForeignTagCtx* pCtx = (SCheckForeignTagCtx*)pContext;
  if (pCtx->foundForeign) {
    return DEAL_RES_CONTINUE;
  }
  if (QUERY_NODE_COLUMN != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }
  SColumnNode* pCol = (SColumnNode*)pNode;
  if (pCol->colType != COLUMN_TYPE_TAG || !pCol->hasRef) {
    return DEAL_RES_CONTINUE;
  }
  // Check whether this tag-ref column matches any column in pRefCols
  bool matched = false;
  SNode* pRefNode = NULL;
  FOREACH(pRefNode, pCtx->pRefCols) {
    if (QUERY_NODE_COLUMN == nodeType(pRefNode)) {
      SColumnNode* pRef = (SColumnNode*)pRefNode;
      if (pCol->colId == pRef->colId &&
          strcmp(pCol->tableName, pRef->tableName) == 0 &&
          strcmp(pCol->dbName, pRef->dbName) == 0) {
        matched = true;
        break;
      }
    } else if (QUERY_NODE_TAG_REF_COLUMN == nodeType(pRefNode)) {
      STagRefColumn* pRef = (STagRefColumn*)pRefNode;
      if (pCol->colId == pRef->colId &&
          strcmp(pCol->colName, pRef->colName) == 0) {
        matched = true;
        break;
      }
    }
  }
  if (!matched) {
    pCtx->foundForeign = true;
  }
  return DEAL_RES_CONTINUE;
}

/**
 * Check if a node contains tag-ref columns that do NOT belong to pRefCols.
 * Used to prevent pushing cross-TagRefSource conditions (e.g. r1 != r2)
 * down to an individual TagRefSource node.
 */
static bool isNodeContainForeignRefColumn(SNode* pNode, SNodeList* pRefCols) {
  SCheckForeignTagCtx ctx = {.pRefCols = pRefCols, .foundForeign = false};
  nodesWalkExpr(pNode, checkForeignTagImpl, &ctx);
  return ctx.foundForeign;
}

/**
 * Build combined filter condition from a list of conditions
 */
static int32_t buildCombinedFilterCondition(SNodeList* pCondList, SNode** ppCombined) {
  if (NULL == pCondList || LIST_LENGTH(pCondList) == 0) {
    *ppCombined = NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pCondList) == 1) {
    return nodesCloneNode(nodesListGetNode(pCondList, 0), ppCombined);
  }

  SLogicConditionNode* pLogicCond = NULL;
  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond));
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  PLAN_ERR_RET(nodesCloneList(pCondList, &pLogicCond->pParameterList));

  *ppCombined = (SNode*)pLogicCond;
  return TSDB_CODE_SUCCESS;
}

/**
 * Extract filter condition for a single tag reference
 * This function extracts filter conditions from WHERE clause that involve
 * a specific tag reference. The extracted conditions are set on the RefSource
 * node so that it can filter the source table before returning results.
 */
static int32_t extractFilterConditionForSingleRef(SNode* pWhere, SColumnNode* pCol, SNode** ppFilterCond) {
  if (NULL == pWhere || NULL == pCol) {
    *ppFilterCond = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SNodeList* pMatchedConds = NULL;
  PLAN_ERR_RET(nodesMakeList(&pMatchedConds));

  SExtractFilterCondCtx ctx = {.pRefCols = NULL, .pMatchedConds = pMatchedConds, .hasNonRefCol = false};

  SNodeList* pSingleColList = NULL;
  PLAN_ERR_RET(nodesMakeList(&pSingleColList));
  PLAN_ERR_RET(nodesListStrictAppend(pSingleColList, (SNode*)pCol));
  ctx.pRefCols = pSingleColList;

  nodesWalkExpr(pWhere, extractFilterCondImpl, &ctx);

  int32_t code = buildCombinedFilterCondition(ctx.pMatchedConds, ppFilterCond);

  nodesDestroyList(ctx.pMatchedConds);
  nodesDestroyList(pSingleColList);
  PLAN_ERR_RET(code);

  if (*ppFilterCond != NULL) {
    planDebug("Extracted filter condition for tagRef %s.%s.%s",
              pCol->dbName, pCol->tableName, pCol->colName);
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * Extract filter conditions for reference columns from WHERE clause
 */
static int32_t extractFilterConditionForRefs(SNode* pWhere, SNodeList* pRefCols, SNode** ppFilterCond) {
  if (NULL == pWhere || NULL == pRefCols || LIST_LENGTH(pRefCols) == 0) {
    *ppFilterCond = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SExtractFilterCondCtx ctx = {0};
  PLAN_ERR_RET(nodesMakeList(&ctx.pMatchedConds));
  ctx.pRefCols = pRefCols;

  nodesWalkExpr(pWhere, extractFilterCondImpl, &ctx);

  int32_t code = buildCombinedFilterCondition(ctx.pMatchedConds, ppFilterCond);

  nodesDestroyList(ctx.pMatchedConds);
  PLAN_ERR_RET(code);

  planDebug("Extracted filter condition for %d reference columns", LIST_LENGTH(pRefCols));

  return TSDB_CODE_SUCCESS;
}

// ====================================================================
// Tag Classification System
// ====================================================================

/**
 * Tag classification result structure
 */
typedef struct STagClassifyResult {
  SNodeList* pLocalTags;     // Local tags (no reference) - use TagScan
  SNodeList* pRefTagCols;    // Referenced tag columns - need source table scan
  SHashObj*  pRefSourceMap;  // Reference source map (grouped by source table)
} STagClassifyResult;

/**
 * Initialize tag classification result
 */
static int32_t initTagClassifyResult(STagClassifyResult* pResult) {
  memset(pResult, 0, sizeof(STagClassifyResult));
  PLAN_ERR_RET(nodesMakeList(&pResult->pLocalTags));
  PLAN_ERR_RET(nodesMakeList(&pResult->pRefTagCols));
  pResult->pRefSourceMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                         true, HASH_NO_LOCK);
  if (NULL == pResult->pRefSourceMap) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * Free tag classification result
 */
static void freeTagClassifyResult(STagClassifyResult* pResult) {
  if (NULL == pResult) {
    return;
  }
  if (pResult->pRefSourceMap) {
    void* pIter = NULL;
    while ((pIter = taosHashIterate(pResult->pRefSourceMap, pIter)) != NULL) {
      SNodeList** ppRefCols = (SNodeList**)pIter;
      if (ppRefCols != NULL && *ppRefCols != NULL) {
        nodesDestroyList(*ppRefCols);
      }
    }
  }
  nodesDestroyList(pResult->pLocalTags);
  nodesDestroyList(pResult->pRefTagCols);
  if (pResult->pRefSourceMap) {
    taosHashCleanup(pResult->pRefSourceMap);
  }
  memset(pResult, 0, sizeof(STagClassifyResult));
}

/**
 * Classify tag columns into local tags and referenced tags
 */
static int32_t classifyTagColumns(SVirtualTableNode* pVirtualTable,
                                   SVirtualScanLogicNode* pVtableScan,
                                   STagClassifyResult* pResult) {
  if (!pVtableScan->pScanPseudoCols) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pVtableScan->pScanPseudoCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      PLAN_ERR_RET(nodesListStrictAppend(pResult->pLocalTags, pNode));
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;

    int32_t tagRefIndex = findTagRefIndex(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colId);
    if (tagRefIndex == -1) {
      tagRefIndex = findTagRefIndexByName(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colName);
    }

    if (tagRefIndex == -1) {
      int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns +
                          pVirtualTable->pMeta->tableInfo.numOfTags;
      int32_t tagSchemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pCol->colId);
      if (tagSchemaIndex >= pVirtualTable->pMeta->tableInfo.numOfColumns) {
        int32_t tagPos = tagSchemaIndex - pVirtualTable->pMeta->tableInfo.numOfColumns;
        if (tagPos >= 0 && tagPos < pVirtualTable->pMeta->numOfTagRefs &&
            pVirtualTable->pMeta->tagRef[tagPos].hasRef) {
          tagRefIndex = tagPos;
        }
      }
    }

    if (tagRefIndex >= 0 && pVirtualTable->pMeta->tagRef[tagRefIndex].hasRef) {
      SColRef* pTagRef = &pVirtualTable->pMeta->tagRef[tagRefIndex];

      char refKey[TSDB_TABLE_FNAME_LEN] = {0};
      buildRefTableKey(refKey, sizeof(refKey), pTagRef->refDbName, pTagRef->refTableName);
      if (refKey[0] == '\0') {
        planError("Invalid ref table key for db=%s table=%s",
                  pTagRef->refDbName, pTagRef->refTableName);
        PLAN_ERR_RET(TSDB_CODE_INVALID_PARA);
      }

      SNodeList** ppRefCols = taosHashGet(pResult->pRefSourceMap, refKey, strlen(refKey));
      if (ppRefCols == NULL) {
        SNodeList* pNewList = NULL;
        PLAN_ERR_RET(nodesMakeList(&pNewList));
        // Store the list pointer itself, not its address
        if (taosHashPut(pResult->pRefSourceMap, refKey, strlen(refKey),
                        &pNewList, sizeof(pNewList)) != 0) {
          nodesDestroyList(pNewList);
          PLAN_ERR_RET(terrno);
        }
        // Get the pointer from hash table to ensure we have the correct address
        ppRefCols = taosHashGet(pResult->pRefSourceMap, refKey, strlen(refKey));
      }

      SNode* pClone = NULL;
      PLAN_ERR_RET(nodesCloneNode(pNode, &pClone));
      PLAN_ERR_RET(nodesListStrictAppend(pResult->pRefTagCols, pClone));

      // Clone again for the per-source list to avoid double free
      SNode* pClone2 = NULL;
      PLAN_ERR_RET(nodesCloneNode(pNode, &pClone2));
      PLAN_ERR_RET(nodesListStrictAppend(*ppRefCols, pClone2));

      planDebug("Classified tag %s as REF (from %s.%s.%s)",
                pCol->colName, pTagRef->refDbName, pTagRef->refTableName, pTagRef->refColName);
    } else {
      PLAN_ERR_RET(nodesListStrictAppend(pResult->pLocalTags, pNode));
      planDebug("Classified tag %s as LOCAL", pCol->colName);
    }
  }

  return TSDB_CODE_SUCCESS;
}

// ====================================================================
// RefSource Node Creation and Filter Condition Setting
// ====================================================================

/**
 * Build virtual scan tree with RefSource nodes as children of pVtableScan
 *
 * This function moves TagRefSource nodes from pTagRefSources list to pVtableScan->pChildren.
 * The caller should have already processed pTagRefSources for building pTagRefCols and
 * pTagFilterCond before calling this function.
 *
 * IMPORTANT: This function clears pTagRefSources list after moving nodes to avoid
 * double-free issues. The nodes are now owned by pVtableScan->pChildren.
 *
 * Structure after this function:
 *   pVtableScan->pChildren
 *     ├── TagRefSource nodes (moved from pTagRefSources)
 *     ├── pTagScan (if exists)
 *     └── pRealTableScan (to be added later)
 */
static int32_t buildVirtualScanTree(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                    SVirtualScanLogicNode* pVtableScan, SLogicNode* pRealTableScan) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pVtableScan->pTagRefSources == NULL || LIST_LENGTH(pVtableScan->pTagRefSources) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  planDebug("Building virtual scan tree with %d TagRefSource as children of pVtableScan",
            LIST_LENGTH(pVtableScan->pTagRefSources));

  SNode* pNode = NULL;
  FOREACH(pNode, pVtableScan->pTagRefSources) {
    STagRefSourceLogicNode* pRefSource = (STagRefSourceLogicNode*)pNode;

    // Set TagRefSource as child of pVtableScan (not pRealTableScan)
    // This allows virtualTableSplit to process TagRefSource into independent subplans
    pRefSource->node.pParent = (SLogicNode*)pVtableScan;
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, (SNode*)pRefSource));

    planDebug("Added TagRefSource as child of pVtableScan: %s.%s (filter=%d, proj=%d)",
              pRefSource->sourceTableName.dbname, pRefSource->sourceTableName.tname,
              pRefSource->isUsedInFilter, pRefSource->isUsedInProjection);
  }

  // IMPORTANT: Move pTagRefSources to pChildren is complete
  // We can clear pTagRefSources list to avoid double-free issues
  // The nodes are now owned by pVtableScan->pChildren and will be destroyed there
  nodesClearList(pVtableScan->pTagRefSources);
  pVtableScan->pTagRefSources = NULL;

  return TSDB_CODE_SUCCESS;
_return:
  return code;
}

static int32_t createTagScanLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SVirtualScanLogicNode* pVirtualScan,
                                      SLogicNode** pLogicNode) {
  SScanLogicNode* pScan = NULL;
  int32_t         code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SCAN, (SNode**)&pScan));

  PLAN_ERR_JRET(cloneVgroups(&pScan->pVgroupList, pVirtualScan->pVgroupList));
  pScan->tableId = pVirtualScan->tableId;
  pScan->stableId = pVirtualScan->stableId;
  pScan->tableType = pVirtualScan->tableType;
  pScan->scanSeq[0] = 1;
  pScan->scanSeq[1] = 0;
  TAOS_SET_OBJ_ALIGNED(&pScan->scanRange, TSWINDOW_INITIALIZER);
  pScan->tableName.type = TSDB_TABLE_NAME_T;
  pScan->tableName.acctId = pCxt->pPlanCxt->acctId;
  tstrncpy(pScan->tableName.dbname, pVirtualScan->tableName.dbname, TSDB_DB_NAME_LEN);
  tstrncpy(pScan->tableName.tname, pVirtualScan->tableName.tname, TSDB_TABLE_NAME_LEN);
  pScan->showRewrite = pCxt->pPlanCxt->showRewrite;
  pScan->dataRequired = FUNC_DATA_REQUIRED_DATA_LOAD;
  pScan->node.groupAction = GROUP_ACTION_NONE;
  pScan->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;

  PLAN_ERR_JRET(nodesCloneList(pVirtualScan->pScanPseudoCols, &pScan->pScanPseudoCols));

  pScan->scanType = SCAN_TYPE_TAG;

  PLAN_ERR_JRET(createColumnByRewriteExprs(pScan->pScanPseudoCols, &pScan->node.pTargets));

  pScan->onlyMetaCtbIdx = false;
  // pCxt->hasScan = true;
  pCxt->pPlanCxt->hasScan = true;
  *pLogicNode = (SLogicNode*)pScan;
  return code;
_return:
  planError("%s faild since %d", __func__ , code);
  nodesDestroyNode((SNode*)pScan);
  return code;
}

// ====================================================================
// TagRef Source Node Creation
// ====================================================================

/**
 * Create TagRef source logic node for tag reference
 *
 * This function creates a STagRefSourceLogicNode for a specific tag reference.
 * The TagRefSource node will scan the source table to get referenced tag values.
 * Filter conditions from WHERE clause are extracted and set on the node.
 *
 * @param pCxt Logic plan context
 * @param pSelect Select statement (for accessing WHERE clause)
 * @param pVirtualTable Virtual table node
 * @param pTagRef Tag reference info
 * @param pRefTable Source table node
 * @param ppTagRefSource Output STagRefSourceLogicNode
 */
static int32_t createTagRefSourceLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                           SVirtualTableNode* pVirtualTable,
                                           SColRef* pTagRef, SRealTableNode* pRefTable,
                                           STagRefSourceLogicNode** ppTagRefSource) {
  int32_t code = TSDB_CODE_SUCCESS;
  const char* pTagName = pTagRef->colName;

  if (pVirtualTable != NULL && pVirtualTable->pMeta != NULL) {
    int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
    int32_t schemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pTagRef->id);
    if (schemaIndex >= 0) {
      pTagName = pVirtualTable->pMeta->schema[schemaIndex].name;
    }
  }
  if (pTagName == NULL || pTagName[0] == '\0') {
    pTagName = pTagRef->refColName;
  }

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE, (SNode**)ppTagRefSource));
  STagRefSourceLogicNode* pTagRefSource = *ppTagRefSource;

  // Set source table name
  pTagRefSource->sourceTableName.type = TSDB_TABLE_NAME_T;
  pTagRefSource->sourceTableName.acctId = pCxt->pPlanCxt->acctId;
  tstrncpy(pTagRefSource->sourceTableName.dbname, pTagRef->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pTagRefSource->sourceTableName.tname, pTagRef->refTableName, TSDB_TABLE_NAME_LEN);

  // Set source super table UID
  pTagRefSource->sourceSuid = pRefTable->pMeta->suid;
  pTagRefSource->sourceId = 0; // Will be set during scheduling

  // Clone vgroup list from the source table
  if (pRefTable->pVgroupList) {
    PLAN_ERR_JRET(cloneVgroups(&pTagRefSource->pVgroupList, pRefTable->pVgroupList));
  }

  // Create tag reference column entry
  STagRefColumn* pTagRefCol = NULL;
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_TAG_REF_COLUMN, (SNode**)&pTagRefCol));

  // Find the schema info for this tag in the source table
  col_id_t sourceColId = 0;
  int32_t sourceColIdx = 0;
  PLAN_ERR_JRET(findRefColId((SNode*)pRefTable, pTagRef->refColName, &sourceColId, &sourceColIdx));

  const SSchema* pSourceSchema = &pRefTable->pMeta->schema[sourceColIdx];
  bool isTag = (sourceColIdx >= pRefTable->pMeta->tableInfo.numOfColumns);

  pTagRefCol->colId = pTagRef->id;
  pTagRefCol->sourceColId = sourceColId;
  tstrncpy(pTagRefCol->colName, pTagName, TSDB_COL_NAME_LEN);
  tstrncpy(pTagRefCol->sourceColName, pTagRef->refColName, TSDB_COL_NAME_LEN);
  pTagRefCol->bytes = pSourceSchema->bytes;
  pTagRefCol->dataType = pSourceSchema->type;

  PLAN_ERR_JRET(nodesMakeList(&pTagRefSource->pRefCols));
  PLAN_ERR_JRET(nodesListStrictAppend(pTagRefSource->pRefCols, (SNode*)pTagRefCol));
  pTagRefCol = NULL; // Ownership transferred

  // Set usage flags - will be determined by caller during classification
  pTagRefSource->isUsedInFilter = false;
  pTagRefSource->isUsedInProjection = false;

  // === Set pTargets for TagRefSource node ===
  // Create output target columns for this TagRefSource scan node
  // These are the columns that this node outputs (the referenced tag columns from source table)
  SNodeList* pTargetCols = NULL;
  PLAN_ERR_JRET(nodesMakeList(&pTargetCols));

  // Create a column node for the target (output column)
  // The target represents the tag value that will be fetched from the source table
  SColumnNode* pTargetCol = NULL;
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTargetCol));
  pTargetCol->node.resType.type = pSourceSchema->type;
  pTargetCol->node.resType.bytes = pSourceSchema->bytes;
  pTargetCol->colId = pTagRef->id;
  tstrncpy(pTargetCol->colName, pTagName, TSDB_COL_NAME_LEN);
  // Use virtual table info for the target column (as seen by the query)
  tstrncpy(pTargetCol->dbName, pVirtualTable != NULL ? pVirtualTable->table.dbName : "", TSDB_DB_NAME_LEN);
  if (pVirtualTable != NULL) {
    tstrncpy(pTargetCol->tableAlias,
             pVirtualTable->table.tableAlias[0] != '\0' ? pVirtualTable->table.tableAlias : pVirtualTable->table.tableName,
             TSDB_TABLE_NAME_LEN);
    tstrncpy(pTargetCol->tableName,
             pVirtualTable->table.tableAlias[0] != '\0' ? pVirtualTable->table.tableAlias : pVirtualTable->table.tableName,
             TSDB_TABLE_NAME_LEN);
  }
  pTargetCol->colType = COLUMN_TYPE_TAG;
  pTargetCol->hasRef = true;
  tstrncpy(pTargetCol->refDbName, pTagRef->refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(pTargetCol->refTableName, pTagRef->refTableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pTargetCol->refColName, pTagRef->refColName, TSDB_COL_NAME_LEN);

  PLAN_ERR_JRET(nodesListStrictAppend(pTargetCols, (SNode*)pTargetCol));
  pTargetCol = NULL; // Ownership transferred

  // Set pTargets for the TagRefSource node - this is required for exchange node creation
  PLAN_ERR_JRET(createColumnByRewriteExprs(pTargetCols, &pTagRefSource->node.pTargets));
  nodesDestroyList(pTargetCols); // Safe to destroy now as targets are cloned

  planDebug("Set pTargets for TagRefSource: tag %s from %s.%s",
            pTagName, pTagRef->refDbName, pTagRef->refTableName);

  // === Set filter conditions for this TagRef Source node ===
  // Extract filter conditions from WHERE clause that involve this tag reference
  if (pSelect != NULL && pSelect->pWhere != NULL && pVirtualTable != NULL) {
    // Create a temporary column node for this tagRef to match conditions in WHERE clause
    SColumnNode* pTagColNode = NULL;
    if (TSDB_CODE_SUCCESS == nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTagColNode)) {
      pTagColNode->node.resType.type = pSourceSchema->type;
      pTagColNode->colId = pTagRef->id;
      tstrncpy(pTagColNode->colName, pTagName, TSDB_COL_NAME_LEN);
      // Use virtual table info to match WHERE clause references
      tstrncpy(pTagColNode->dbName, pVirtualTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pTagColNode->tableName, pVirtualTable->table.tableAlias[0] != '\0' ?
               pVirtualTable->table.tableAlias : pVirtualTable->table.tableName, TSDB_TABLE_NAME_LEN);
      pTagColNode->colType = COLUMN_TYPE_TAG;
      pTagColNode->hasRef = true;
      tstrncpy(pTagColNode->refDbName, pTagRef->refDbName, TSDB_DB_NAME_LEN);
      tstrncpy(pTagColNode->refTableName, pTagRef->refTableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pTagColNode->refColName, pTagRef->refColName, TSDB_COL_NAME_LEN);

      // Extract filter conditions for this specific tag reference
      PLAN_ERR_JRET(extractFilterConditionForSingleRef(pSelect->pWhere, pTagColNode,
                                                       &pTagRefSource->node.pConditions));

      nodesDestroyNode((SNode*)pTagColNode);

      if (pTagRefSource->node.pConditions != NULL) {
        planDebug("Set filter condition on TagRefSource node for tag %s from %s.%s",
                  pTagName, pTagRef->refDbName, pTagRef->refTableName);
      }
    }
  }

  planDebug("Created TagRefSourceLogicNode for tag %s from %s.%s",
            pTagName, pTagRef->refDbName, pTagRef->refTableName);

_return:
  nodesDestroyNode((SNode*)pTagRefCol);
  nodesDestroyNode((SNode*)pTargetCol);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode((SNode*)*ppTagRefSource);
    *ppTagRefSource = NULL;
  }
  return code;
}

static int32_t createVirtualSuperTableLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                                SVirtualTableNode* pVirtualTable, SVirtualScanLogicNode* pVtableScan,
                                                SLogicNode** pLogicNode) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SLogicNode*             pRealTableScan = NULL;
  SLogicNode*             pInsColumnsScan = NULL;
  SDynQueryCtrlLogicNode* pDynCtrl = NULL;
  SNode*                  pNode = NULL;
  bool                    scanAllCols = true;
  SNode*                  pTagScan = NULL;

  // Virtual table scan node -> Real table scan node
  PLAN_ERR_JRET(createScanLogicNode(pCxt, pSelect, (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 0), &pRealTableScan));

  if (LIST_LENGTH(pVtableScan->pScanCols) == 0 && LIST_LENGTH(pVtableScan->pScanPseudoCols) == 0) {
    scanAllCols = false;
  }
  if (((SScanLogicNode*)pRealTableScan)->scanType == SCAN_TYPE_TAG) {
    ((SScanLogicNode*)pRealTableScan)->scanType = SCAN_TYPE_TABLE;
  }

  PLAN_ERR_JRET(addVtbPrimaryTsCol(pVirtualTable, &pVtableScan->pScanCols));

  FOREACH(pNode, pVtableScan->pScanCols) {
    SColumnNode *pCol = (SColumnNode*)pNode;
    if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      // do nothing
    } else {
      scanAllCols &= false;
    }
  }

  if (scanAllCols) {
    nodesDestroyList(((SScanLogicNode*)pRealTableScan)->node.pTargets);
    ((SScanLogicNode*)pRealTableScan)->node.pTargets = NULL;
    pVtableScan->scanAllCols = true;
    for (int32_t i = 0; i < pVirtualTable->pMeta->tableInfo.numOfColumns; i++) {
      if (pVirtualTable->pMeta->schema[i].colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        continue;
      } else {
        PLAN_ERR_JRET(scanAddCol(pRealTableScan, NULL, &pVirtualTable->table, &pVirtualTable->pMeta->schema[i], pVirtualTable->pMeta->schema[i].colId, NULL, false));
      }
    }
    PLAN_ERR_JRET(createColumnByRewriteExprs(((SScanLogicNode*)pRealTableScan)->pScanCols, &((SScanLogicNode*)pRealTableScan)->node.pTargets));
  }

  // ====================================================================
  // TagRef Processing for Virtual Super Table
  // ====================================================================
  // Virtual super table can have tagRef in filter/order/group/projection clauses.
  // RefSource nodes are created to scan source tables for referenced tag values

  SHashObj* pRefTableNodeMap = NULL;  // For tagRef processing

  // Initialize reference table node map for tagRef processing
  pRefTableNodeMap =
      taosHashInit(LIST_LENGTH(pVirtualTable->refTables), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                   HASH_ENTRY_LOCK);
  if (NULL == pRefTableNodeMap) {
    PLAN_ERR_JRET(terrno);
  }

  SNode* pRefNode = NULL;
  FOREACH(pRefNode, pVirtualTable->refTables) {
    SRealTableNode* pRefTable = (SRealTableNode*)pRefNode;
    char            tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
    buildRefTableKey(tableNameKey, sizeof(tableNameKey), pRefTable->table.dbName, pRefTable->table.tableName);
    if (tableNameKey[0] == '\0') {
      PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
    }
    PLAN_ERR_JRET(taosHashPut(pRefTableNodeMap, tableNameKey, strlen(tableNameKey), &pRefNode, POINTER_BYTES));
  }

  // === Phase 0: Classify tags into local tags and referenced tags ===
  STagClassifyResult tagResult = {0};
  PLAN_ERR_JRET(initTagClassifyResult(&tagResult));
  PLAN_ERR_JRET(appendCondTagPseudoCols(pSelect->pWhere, &pVtableScan->pScanPseudoCols));
  PLAN_ERR_JRET(eliminateDupVirtualTagCols(pVtableScan->pScanPseudoCols));
  PLAN_ERR_JRET(classifyTagColumns(pVirtualTable, pVtableScan, &tagResult));

  // Set flags based on classification
  pVtableScan->hasTagRef = (tagResult.pRefTagCols && LIST_LENGTH(tagResult.pRefTagCols) > 0);
  pVtableScan->hasLocalTag = (tagResult.pLocalTags && LIST_LENGTH(tagResult.pLocalTags) > 0);

  // Clone and set pLocalTags and pRefTagCols to pVtableScan for downstream processing
  if (tagResult.pLocalTags && LIST_LENGTH(tagResult.pLocalTags) > 0) {
    PLAN_ERR_JRET(nodesCloneList(tagResult.pLocalTags, &pVtableScan->pLocalTags));
  }
  if (tagResult.pRefTagCols && LIST_LENGTH(tagResult.pRefTagCols) > 0) {
    PLAN_ERR_JRET(nodesCloneList(tagResult.pRefTagCols, &pVtableScan->pRefTagCols));
    PLAN_ERR_JRET(eliminateDupVirtualTagCols(pVtableScan->pRefTagCols));
  }

  PLAN_ERR_JRET(keepOnlyLocalTagPseudoCols(pVirtualTable, pVtableScan));

  planDebug("[SuperTable] TagRef classification: hasTagRef=%d, hasLocalTag=%d, refTags=%d, localTags=%d",
            pVtableScan->hasTagRef, pVtableScan->hasLocalTag,
            tagResult.pRefTagCols ? LIST_LENGTH(tagResult.pRefTagCols) : 0,
            tagResult.pLocalTags ? LIST_LENGTH(tagResult.pLocalTags) : 0);

  // === Phase 1: Handle tagRef that need source table scan ===
  if (pVtableScan->pRefTagCols) {
    PLAN_ERR_JRET(nodesMakeList(&pVtableScan->pTagRefSources));

    FOREACH(pNode, pVtableScan->pRefTagCols) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        continue;
      }

      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t tagRefIndex = findTagRefIndex(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colId);
      if (tagRefIndex == -1) {
        tagRefIndex = findTagRefIndexByName(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colName);
      }
      if (tagRefIndex == -1) {
        int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
        int32_t tagSchemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pCol->colId);
        if (tagSchemaIndex >= pVirtualTable->pMeta->tableInfo.numOfColumns) {
          int32_t tagPos = tagSchemaIndex - pVirtualTable->pMeta->tableInfo.numOfColumns;
          if (tagPos >= 0 && tagPos < pVirtualTable->pMeta->numOfTagRefs &&
              pVirtualTable->pMeta->tagRef[tagPos].hasRef) {
            tagRefIndex = tagPos;
          }
        }
      }

      // Check if this tag reference needs a RefSource node
      if (tagRefIndex != -1 && pVirtualTable->pMeta->tagRef[tagRefIndex].hasRef) {
        SColRef* pTagRef = &pVirtualTable->pMeta->tagRef[tagRefIndex];

        bool usedInFilter = false;
        bool usedInProjection = false;

        if (isTagUsedInAnyClause(pSelect, pCol, &usedInFilter, &usedInProjection)) {
          planDebug("[SuperTable] TagRef %s (ref: %s.%s.%s) used in filter[%d] nonFilter[%d], creating TagRefSource",
                    pCol->colName, pTagRef->refDbName, pTagRef->refTableName, pTagRef->refColName,
                    usedInFilter, usedInProjection);

          // Find the reference table node
          SNode* pRefTable = NULL;
          PLAN_ERR_JRET(findRefTableNode(pRefTableNodeMap, pTagRef->refDbName, pTagRef->refTableName, &pRefTable));

          // Create RefSourceLogicNode for this tag reference
          STagRefSourceLogicNode* pRefSource = NULL;
          code = createTagRefSourceLogicNode(pCxt, pSelect, pVirtualTable, pTagRef,
                                                         (SRealTableNode*)pRefTable, &pRefSource);
          nodesDestroyNode((SNode*)pRefTable);  // pRefTable was cloned by findRefTableNode, need to free
          pRefTable = NULL;
          PLAN_ERR_JRET(code);

          // Set usage flags
          pRefSource->isUsedInFilter = usedInFilter;
          pRefSource->isUsedInProjection = usedInProjection;

          // Add to the refSources list
          PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->pTagRefSources, (SNode*)pRefSource));
        }
      }
    }

    // If no tag references need source table scan, clear the refSources list
      if (LIST_LENGTH(pVtableScan->pTagRefSources) == 0) {
        nodesDestroyList(pVtableScan->pTagRefSources);
        pVtableScan->pTagRefSources = NULL;
      } else {
        PLAN_ERR_JRET(eliminateDupTagRefSources(pVtableScan->pTagRefSources));
        // Set hasTagRef flag
        pVtableScan->hasTagRef = true;

      // === Step 1: Build pTagRefCols from pTagRefSources (before moving nodes) ===
      SNodeList* pTagRefCols = NULL;
      PLAN_ERR_JRET(nodesMakeList(&pTagRefCols));

      FOREACH(pRefNode, pVtableScan->pTagRefSources) {
        STagRefSourceLogicNode* pRefSource = (STagRefSourceLogicNode*)pRefNode;
        // Add the referenced columns from this TagRefSource
        if (pRefSource->pRefCols) {
          SNode* pRefCol = NULL;
          FOREACH(pRefCol, pRefSource->pRefCols) {
            SNode* pClone = NULL;
            PLAN_ERR_JRET(nodesCloneNode(pRefCol, &pClone));
            PLAN_ERR_JRET(nodesListStrictAppend(pTagRefCols, pClone));
          }
        }
      }

      // === Step 2: Set filter conditions for tag references ===
      if (LIST_LENGTH(pTagRefCols) > 0 && pSelect->pWhere != NULL) {
        PLAN_ERR_JRET(extractFilterConditionForRefs(pSelect->pWhere, pTagRefCols, &pVtableScan->pTagFilterCond));
        planDebug("[SuperTable] Set pTagFilterCond for virtual table scan: %s",
                  pVtableScan->pTagFilterCond ? "has conditions" : "no conditions");
      }

      // === Step 3: Build virtual scan tree with RefSource nodes as children ===
      // TagRefSource nodes are moved from pTagRefSources to pVtableScan->pChildren
      // This allows virtualTableSplit to process them into independent subplans
      PLAN_ERR_JRET(buildVirtualScanTree(pCxt, pSelect, pVtableScan, pRealTableScan));

      // Clean up pTagRefCols
      nodesDestroyList(pTagRefCols);
    }
  }

  if (pVtableScan->pScanPseudoCols && LIST_LENGTH(pVtableScan->pScanPseudoCols) > 0) {
    PLAN_ERR_JRET(createTagScanLogicNode(pCxt, pSelect, pVtableScan, (SLogicNode**)&pTagScan));
  }

  // NOTE: All refs in this implementation are TAG refs only
  // Column references (colRef) are handled separately and are not related to tagRef

  ((SScanLogicNode *)pRealTableScan)->node.dynamicOp = true;
  ((SScanLogicNode *)pRealTableScan)->virtualStableScan = true;
  if (pTagScan) {
    ((SScanLogicNode*)pTagScan)->node.dynamicOp = true;
    ((SScanLogicNode*)pTagScan)->virtualStableScan = true;
    ((SLogicNode *)pTagScan)->pParent = (SLogicNode *)pVtableScan;
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, pTagScan));
  }
  PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, (SNode*)(pRealTableScan)));
  pRealTableScan->pParent = (SLogicNode *)pVtableScan;

  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanCols, &pVtableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanPseudoCols, &pVtableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pRefTagCols, &pVtableScan->node.pTargets));

  // Virtual child table uid and ref col scan on ins_columns
  // TODO(smj) : create a fake logic node, no need to collect column
  PLAN_ERR_JRET(createScanLogicNode(pCxt, pSelect, (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 1), &pInsColumnsScan));
  nodesDestroyList(((SScanLogicNode*)pInsColumnsScan)->node.pTargets);
  ((SScanLogicNode*)pInsColumnsScan)->node.pTargets = NULL;  // Set to NULL after destroy to avoid use-after-free
  PLAN_ERR_JRET(addInsColumnScanCol((SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 1), &((SScanLogicNode*)pInsColumnsScan)->pScanCols));
  PLAN_ERR_JRET(createColumnByRewriteExprs(((SScanLogicNode*)pInsColumnsScan)->pScanCols, &((SScanLogicNode*)pInsColumnsScan)->node.pTargets));
  ((SScanLogicNode *)pInsColumnsScan)->node.dynamicOp = true;
  ((SScanLogicNode *)pInsColumnsScan)->virtualStableScan = true;
  ((SScanLogicNode *)pInsColumnsScan)->stableId = pVtableScan->stableId;

  // Dynamic query control node -> Virtual table scan node -> Real table scan node
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL, (SNode**)&pDynCtrl));
  pDynCtrl->qType = DYN_QTYPE_VTB_SCAN;
  pDynCtrl->vtbScan.scanAllCols = pVtableScan->scanAllCols;
  pDynCtrl->vtbScan.useTagScan = (pTagScan != NULL);
  if (pVtableScan->tableType == TSDB_SUPER_TABLE) {
    pDynCtrl->vtbScan.isSuperTable = true;
    pDynCtrl->vtbScan.suid = pVtableScan->stableId;
    pDynCtrl->vtbScan.rversion = 0;
  } else {
    pDynCtrl->vtbScan.isSuperTable = false;
    pDynCtrl->vtbScan.uid = pVtableScan->tableId;
    pDynCtrl->vtbScan.rversion = pVirtualTable->pMeta->rversion;
  }
  for (int32_t i = 0; i < ((SScanLogicNode*)pRealTableScan)->pVgroupList->numOfVgroups; i++) {
    int32_t    vgId = ((SScanLogicNode*)pRealTableScan)->pVgroupList->vgroups[i].vgId;
    SValueNode *pVal = NULL;
    PLAN_ERR_JRET(nodesMakeValueNodeFromInt32(vgId, (SNode**)&pVal));
    PLAN_ERR_JRET(nodesListMakeStrictAppend(&pDynCtrl->vtbScan.pOrgVgIds, (SNode*)pVal));
  }

  pDynCtrl->dynTbname = ((SScanLogicNode*)pRealTableScan)->phTbnameScan;
  ((SScanLogicNode*)pRealTableScan)->phTbnameScan = false;  // reset phTbnameQuery, it is only used for vtable scan

  tstrncpy(pDynCtrl->vtbScan.dbName, pVtableScan->tableName.dbname, TSDB_DB_NAME_LEN);
  tstrncpy(pDynCtrl->vtbScan.tbName, pVtableScan->tableName.tname, TSDB_TABLE_NAME_LEN);
  PLAN_ERR_JRET(nodesListMakeStrictAppend(&pDynCtrl->node.pChildren, (SNode*)pVtableScan));
  PLAN_ERR_JRET(nodesListMakeStrictAppend(&pDynCtrl->node.pChildren, (SNode*)pInsColumnsScan));
  PLAN_ERR_JRET(nodesCloneList(pVtableScan->node.pTargets, &pDynCtrl->node.pTargets));

  TSWAP(pVtableScan->pVgroupList, pDynCtrl->vtbScan.pVgroupList);
  pVtableScan->node.pParent = (SLogicNode*)pDynCtrl;
  ((SScanLogicNode*)pInsColumnsScan)->node.pParent = (SLogicNode*)pDynCtrl;
  pVtableScan->node.dynamicOp = true;
  *pLogicNode = (SLogicNode*)pDynCtrl;

  if (pRefTableNodeMap != NULL) {
    taosHashCleanup(pRefTableNodeMap);
    pRefTableNodeMap = NULL;
  }
  if (tagResult.pLocalTags != NULL) {
    nodesClearList(tagResult.pLocalTags);
  }
  freeTagClassifyResult(&tagResult);

  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  nodesDestroyNode((SNode*)pRealTableScan);
  nodesDestroyNode((SNode*)pDynCtrl);
  if (pRefTableNodeMap) {
    taosHashCleanup(pRefTableNodeMap);
  }
  freeTagClassifyResult(&tagResult);
  return code;
}

static int32_t createVirtualNormalChildTableLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                                      SVirtualTableNode* pVirtualTable, SVirtualScanLogicNode* pVtableScan,
                                                      SLogicNode** pLogicNode) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SNode*    pNode = NULL;
  bool      scanAllCols = true;
  SHashObj* pRefTablesMap = NULL;
  SHashObj* pRefTableNodeMap = NULL;
  SNode*    pTagScan = NULL;

  pRefTablesMap = taosHashInit(LIST_LENGTH(pVtableScan->pScanCols), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == pRefTablesMap) {
    PLAN_ERR_JRET(terrno);
  }
  pRefTableNodeMap =
      taosHashInit(LIST_LENGTH(pVirtualTable->refTables), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true,
                   HASH_ENTRY_LOCK);
  if (NULL == pRefTableNodeMap) {
    PLAN_ERR_JRET(terrno);
  }

  SNode* pRefNode = NULL;
  FOREACH(pRefNode, pVirtualTable->refTables) {
    SRealTableNode* pRefTable = (SRealTableNode*)pRefNode;
    char            tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
    buildRefTableKey(tableNameKey, sizeof(tableNameKey), pRefTable->table.dbName, pRefTable->table.tableName);
    if (tableNameKey[0] == '\0') {
      PLAN_ERR_JRET(TSDB_CODE_INVALID_PARA);
    }
    PLAN_ERR_JRET(taosHashPut(pRefTableNodeMap, tableNameKey, strlen(tableNameKey), &pRefNode, POINTER_BYTES));
  }

  PLAN_ERR_JRET(syncVirtualTablePrimaryTsRef(pVirtualTable));
  PLAN_ERR_JRET(syncVirtualTableColRefs(pVtableScan->pScanCols, pVirtualTable));
  PLAN_ERR_JRET(syncVirtualTableColRefs(pVtableScan->pScanPseudoCols, pVirtualTable));

  // ====================================================================
  // TagRef Processing for Virtual Child Table
  // ====================================================================

  // === Phase 0: Classify tags into local tags and referenced tags ===
  STagClassifyResult tagResult = {0};
  PLAN_ERR_JRET(initTagClassifyResult(&tagResult));
  PLAN_ERR_JRET(appendCondTagPseudoCols(pSelect->pWhere, &pVtableScan->pScanPseudoCols));
  PLAN_ERR_JRET(eliminateDupVirtualTagCols(pVtableScan->pScanPseudoCols));
  PLAN_ERR_JRET(classifyTagColumns(pVirtualTable, pVtableScan, &tagResult));

  // Set flags based on classification
  pVtableScan->hasTagRef = (tagResult.pRefTagCols && LIST_LENGTH(tagResult.pRefTagCols) > 0);
  pVtableScan->hasLocalTag = (tagResult.pLocalTags && LIST_LENGTH(tagResult.pLocalTags) > 0);

  // Clone and set pLocalTags and pRefTagCols to pVtableScan for downstream processing
  if (tagResult.pLocalTags && LIST_LENGTH(tagResult.pLocalTags) > 0) {
    PLAN_ERR_JRET(nodesCloneList(tagResult.pLocalTags, &pVtableScan->pLocalTags));
  }
  if (tagResult.pRefTagCols && LIST_LENGTH(tagResult.pRefTagCols) > 0) {
    PLAN_ERR_JRET(nodesCloneList(tagResult.pRefTagCols, &pVtableScan->pRefTagCols));
    PLAN_ERR_JRET(eliminateDupVirtualTagCols(pVtableScan->pRefTagCols));
  }

  planDebug("[ChildTable] TagRef classification: hasTagRef=%d, hasLocalTag=%d, refTags=%d, localTags=%d",
            pVtableScan->hasTagRef, pVtableScan->hasLocalTag,
            tagResult.pRefTagCols ? LIST_LENGTH(tagResult.pRefTagCols) : 0,
            tagResult.pLocalTags ? LIST_LENGTH(tagResult.pLocalTags) : 0);

  // === Phase 1: Handle tagRef that need source table scan ===
  // RefSource nodes are needed when tagRef is used in WHERE clause (for filtering)
  // or in SELECT projection (to return tag values)
  if (pVtableScan->pScanPseudoCols) {
    PLAN_ERR_JRET(nodesMakeList(&pVtableScan->pTagRefSources));

    FOREACH(pNode, pVtableScan->pScanPseudoCols) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        continue;
      }

      SColumnNode* pCol = (SColumnNode*)pNode;
      int32_t tagRefIndex = findTagRefIndex(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colId);
      if (tagRefIndex == -1) {
        tagRefIndex = findTagRefIndexByName(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colName);
      }
      if (tagRefIndex == -1) {
        int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
        int32_t tagSchemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pCol->colId);
        if (tagSchemaIndex >= pVirtualTable->pMeta->tableInfo.numOfColumns) {
          int32_t tagPos = tagSchemaIndex - pVirtualTable->pMeta->tableInfo.numOfColumns;
          if (tagPos >= 0 && tagPos < pVirtualTable->pMeta->numOfTagRefs &&
              pVirtualTable->pMeta->tagRef[tagPos].hasRef) {
            tagRefIndex = tagPos;
          }
        }
      }

      // Check if this tagRef needs a RefSource node
      if (tagRefIndex != -1 && pVirtualTable->pMeta->tagRef[tagRefIndex].hasRef) {
        SColRef* pTagRef = &pVirtualTable->pMeta->tagRef[tagRefIndex];

        // Check if tagRef needs source table scan (used in WHERE or SELECT)
        bool usedInFilter = isTagUsedInFilter(pSelect->pWhere, pCol);
        bool usedInProjection = isTagUsedInProjection(pSelect->pProjectionList, pCol);

        if (usedInFilter || usedInProjection) {
          planDebug("[ChildTable] TagRef %s (ref: %s.%s.%s) used in WHERE[%d] SELECT[%d], creating TagRefSource",
                    pCol->colName, pTagRef->refDbName, pTagRef->refTableName, pTagRef->refColName,
                    usedInFilter, usedInProjection);

          // Find the reference table node
          SNode* pRefTable = NULL;
          PLAN_ERR_JRET(findRefTableNode(pRefTableNodeMap, pTagRef->refDbName, pTagRef->refTableName, &pRefTable));

          // Create RefSourceLogicNode for this tag reference
          STagRefSourceLogicNode* pRefSource = NULL;
          code = createTagRefSourceLogicNode(pCxt, pSelect, pVirtualTable, pTagRef,
                                                         (SRealTableNode*)pRefTable, &pRefSource);
          nodesDestroyNode((SNode*)pRefTable);  // pRefTable was cloned by findRefTableNode, need to free
          pRefTable = NULL;
          PLAN_ERR_JRET(code);

          // Set usage flags for executor to know
          pRefSource->isUsedInFilter = usedInFilter;
          pRefSource->isUsedInProjection = usedInProjection;

          // Add to the refSources list
          PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->pTagRefSources, (SNode*)pRefSource));

          // The RefSource node will be used by the executor to:
          // 1. Scan the source table to get tag values
          // 2a. If usedInFilter: Apply WHERE conditions, return matching UIDs for filtering
          // 2b. If usedInProjection: Provide tag values for output
          // 3. Merge with main table scan results
        }
      }
    }

    // If no tag references need source table scan, clear the refSources list
    if (LIST_LENGTH(pVtableScan->pTagRefSources) == 0) {
      nodesDestroyList(pVtableScan->pTagRefSources);
      pVtableScan->pTagRefSources = NULL;
    } else {
      PLAN_ERR_JRET(eliminateDupTagRefSources(pVtableScan->pTagRefSources));
      // For child table, pass NULL to keep TagRefSource as children of pVtableScan
      // (since there's no single scanTable like in super table case)
      PLAN_ERR_JRET(buildVirtualScanTree(pCxt, pSelect, pVtableScan, NULL));
    }
  }

  // NOTE: Column references (colRef) are handled separately below
  // All tagRef processing is complete at this point

  if (inStreamCalcClause(pCxt->pPlanCxt) && pSelect->pTimeRange != NULL) {
    // ts column might be extract from where to time range. So, ts column won't be collected into pVtableScan->pScanCols.
    PLAN_ERR_JRET(addVtbPrimaryTsCol(pVirtualTable, &pVtableScan->pScanCols));
  }

  PLAN_ERR_JRET(eliminateDupScanCols(pVtableScan->pScanCols));

  FOREACH(pNode, pVtableScan->pScanCols) {
    SColumnNode *pCol = (SColumnNode*)pNode;
    int32_t colRefIndex = findColRefIndex(pVirtualTable->pMeta->colRef, pVirtualTable, pCol->colId);
    int32_t schemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, pVirtualTable->pMeta->tableInfo.numOfColumns, pCol->colId);
    if (colRefIndex != -1 && pVirtualTable->pMeta->colRef[colRefIndex].hasRef) {
      if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        continue;
      }
      scanAllCols &= false;
      PLAN_ERR_JRET(addSubScanNode(pCxt, pSelect, pVirtualTable, colRefIndex, schemaIndex, pRefTablesMap,
                                   pRefTableNodeMap));
    } else if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      // do nothing
    } else {
      scanAllCols &= false;
    }
  }

  // referenced virtual tags should be scanned from source tables, not from vtable tag-scan.
  if (pVtableScan->pScanPseudoCols) {
    SNodeList* pRemainPseudoCols = NULL;
    PLAN_ERR_JRET(nodesMakeList(&pRemainPseudoCols));

    FOREACH(pNode, pVtableScan->pScanPseudoCols) {
      bool moved = false;
      if (nodeType(pNode) == QUERY_NODE_COLUMN) {
        SColumnNode* pCol = (SColumnNode*)pNode;
        int32_t tagRefIndex = findTagRefIndex(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colId);
        if (tagRefIndex == -1) {
          tagRefIndex = findTagRefIndexByName(pVirtualTable->pMeta->tagRef, pVirtualTable, pCol->colName);
        }
        if (tagRefIndex == -1) {
          int32_t totalCols = pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags;
          int32_t tagSchemaIndex = findSchemaIndex(pVirtualTable->pMeta->schema, totalCols, pCol->colId);
          if (tagSchemaIndex >= pVirtualTable->pMeta->tableInfo.numOfColumns) {
            int32_t tagPos = tagSchemaIndex - pVirtualTable->pMeta->tableInfo.numOfColumns;
            if (tagPos >= 0 && tagPos < pVirtualTable->pMeta->numOfTagRefs &&
                pVirtualTable->pMeta->tagRef[tagPos].hasRef) {
              tagRefIndex = tagPos;
            }
          }
        }
        if (tagRefIndex == -1) {
          int32_t colRefIndex = findColRefIndex(pVirtualTable->pMeta->colRef, pVirtualTable, pCol->colId);
          if (colRefIndex != -1 && pVirtualTable->pMeta->colRef[colRefIndex].hasRef) {
            int32_t schemaIndex = findSchemaIndex(
                pVirtualTable->pMeta->schema,
                pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags, pCol->colId);
            if (schemaIndex < 0) {
              PLAN_ERR_JRET(TSDB_CODE_NOT_FOUND);
            }
            PLAN_ERR_JRET(addSubScanNodeByRef(pCxt, pSelect, pVirtualTable, &pVirtualTable->pMeta->colRef[colRefIndex],
                                              schemaIndex, pRefTablesMap, pRefTableNodeMap));
            scanAllCols &= false;

            SNode* pCloneToScanCols = NULL;
            PLAN_ERR_JRET(nodesCloneNode(pNode, &pCloneToScanCols));
            PLAN_ERR_JRET(nodesListMakeStrictAppend(&pVtableScan->pScanCols, pCloneToScanCols));
            moved = true;
          }
        }
        if (!moved && tagRefIndex != -1 && pVirtualTable->pMeta->tagRef[tagRefIndex].hasRef) {
          // Referenced tags are produced by TagRefSource + VirtualTableScan merge and
          // should not remain in scan pseudo cols, otherwise the executor's pseudo-column
          // refill path overwrites the merged ref-tag value with the virtual table's own
          // single-row tag block value.
          moved = true;
        } else if (pCol->hasRef) {
          int32_t schemaIndex = findSchemaIndex(
              pVirtualTable->pMeta->schema,
              pVirtualTable->pMeta->tableInfo.numOfColumns + pVirtualTable->pMeta->tableInfo.numOfTags, pCol->colId);
          if (schemaIndex < 0) {
            PLAN_ERR_JRET(TSDB_CODE_NOT_FOUND);
          }

          SColRef colRef = {0};
          colRef.hasRef = true;
          colRef.id = pCol->colId;
          tstrncpy(colRef.refDbName, pCol->refDbName, sizeof(colRef.refDbName));
          tstrncpy(colRef.refTableName, pCol->refTableName, sizeof(colRef.refTableName));
          tstrncpy(colRef.refColName, pCol->refColName, sizeof(colRef.refColName));
          PLAN_ERR_JRET(addSubScanNodeByRef(pCxt, pSelect, pVirtualTable, &colRef, schemaIndex, pRefTablesMap,
                                            pRefTableNodeMap));
          scanAllCols &= false;

          SNode* pCloneToScanCols = NULL;
          PLAN_ERR_JRET(nodesCloneNode(pNode, &pCloneToScanCols));
          PLAN_ERR_JRET(nodesListMakeStrictAppend(&pVtableScan->pScanCols, pCloneToScanCols));
          moved = true;
        }
      }

      if (!moved) {
        SNode* pCloneToPseudoCols = NULL;
        PLAN_ERR_JRET(nodesCloneNode(pNode, &pCloneToPseudoCols));
        PLAN_ERR_JRET(nodesListMakeStrictAppend(&pRemainPseudoCols, pCloneToPseudoCols));
      }
    }

    nodesDestroyList(pVtableScan->pScanPseudoCols);
    if (LIST_LENGTH(pRemainPseudoCols) == 0) {
      nodesDestroyList(pRemainPseudoCols);
      pVtableScan->pScanPseudoCols = NULL;
    } else {
      pVtableScan->pScanPseudoCols = pRemainPseudoCols;
    }
  }

  if (pVtableScan->pScanPseudoCols && LIST_LENGTH(pVtableScan->pScanPseudoCols) > 0) {
    PLAN_ERR_JRET(createTagScanLogicNode(pCxt, pSelect, pVtableScan, (SLogicNode**)&pTagScan));
  }

  if (scanAllCols) {
    pVtableScan->scanAllCols = true;
    taosHashClear(pRefTablesMap);
    for (int32_t i = 0; i < pVirtualTable->pMeta->tableInfo.numOfColumns; i++) {
      if (pVirtualTable->pMeta->schema[i].colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        continue;
      } else {
        col_id_t colRefIndex = (col_id_t)findColRefIndex(pVirtualTable->pMeta->colRef, pVirtualTable, pVirtualTable->pMeta->schema[i].colId);
        if (colRefIndex != -1 && pVirtualTable->pMeta->colRef[colRefIndex].hasRef) {
          PLAN_ERR_JRET(addSubScanNode(pCxt, pSelect, pVirtualTable, colRefIndex, i, pRefTablesMap, pRefTableNodeMap));
        }
      }
    }
  }

  // Iterate the table map, build scan logic node for each origin table and add these node to vtable scan's child list.
  void* pIter = NULL;
  SScanLogicNode* pOnlyRefScan = NULL;
  while ((pIter = taosHashIterate(pRefTablesMap, pIter))) {
    SScanLogicNode **pRefScanNode = (SScanLogicNode**)pIter;
    PLAN_ERR_JRET(createColumnByRewriteExprs((*pRefScanNode)->pScanCols, &(*pRefScanNode)->node.pTargets));
    PLAN_ERR_JRET(createColumnByRewriteExprs((*pRefScanNode)->pScanPseudoCols, &(*pRefScanNode)->node.pTargets));
    if (taosHashGetSize(pRefTablesMap) == 1) {
      pOnlyRefScan = *pRefScanNode;
    }
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, (SNode*)(*pRefScanNode)));
  }

  if (pTagScan) {
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, pTagScan));
  }

  // set output
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanCols, &pVtableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanPseudoCols, &pVtableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pRefTagCols, &pVtableScan->node.pTargets));
  if (NULL != pOnlyRefScan && 1 == LIST_LENGTH(pVtableScan->node.pChildren) &&
      (NULL == pVtableScan->pScanPseudoCols || LIST_LENGTH(pVtableScan->pScanPseudoCols) == 0) &&
      (NULL == pVtableScan->pRefTagCols || LIST_LENGTH(pVtableScan->pRefTagCols) == 0)) {
    nodesDestroyList(pOnlyRefScan->node.pTargets);
    pOnlyRefScan->node.pTargets = NULL;
    PLAN_ERR_JRET(rewriteFlatVirtualChildScanCols(pOnlyRefScan, pVirtualTable));
    PLAN_ERR_JRET(createColumnByRewriteExprs(pOnlyRefScan->pScanCols, &pOnlyRefScan->node.pTargets));
    nodesClearList(pVtableScan->node.pChildren);
    pVtableScan->node.pChildren = NULL;
    taosHashCleanup(pRefTablesMap);
    taosHashCleanup(pRefTableNodeMap);
    freeTagClassifyResult(&tagResult);
    nodesDestroyNode((SNode*)pVtableScan);
    *pLogicNode = (SLogicNode*)pOnlyRefScan;
    return code;
  }

  *pLogicNode = (SLogicNode*)pVtableScan;
  taosHashCleanup(pRefTablesMap);
  taosHashCleanup(pRefTableNodeMap);

  // Clean up tagRef classification result
  freeTagClassifyResult(&tagResult);

  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  taosHashSetFreeFp(pRefTablesMap, destroyScanLogicNode);
  taosHashCleanup(pRefTablesMap);
  taosHashCleanup(pRefTableNodeMap);

  // Clean up tagRef classification result on error
  freeTagClassifyResult(&tagResult);

  return code;
}


static int32_t comparePseudoColsByColId(SNode* pNode1, SNode* pNode2) {
  if (QUERY_NODE_COLUMN != nodeType(pNode1) || QUERY_NODE_COLUMN != nodeType(pNode2)) {
    return 0;
  }
  col_id_t id1 = ((SColumnNode*)pNode1)->colId;
  col_id_t id2 = ((SColumnNode*)pNode2)->colId;
  return (id1 < id2) ? -1 : ((id1 > id2) ? 1 : 0);
}

static int32_t createVirtualTableLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                           SVirtualTableNode* pVirtualTable, SLogicNode** pLogicNode) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  SVirtualScanLogicNode  *pVtableScan = NULL;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN, (SNode**)&pVtableScan));

  PLAN_ERR_JRET(nodesMakeList(&pVtableScan->node.pChildren));

  PLAN_ERR_JRET(makeVirtualScanLogicNode(pCxt, pVirtualTable, pVtableScan));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_COL,
                                    &pVtableScan->pScanCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_WHERE, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, pVirtualTable->table.tableAlias,
                                    COLLECT_COL_TYPE_TAG, &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_GROUP_BY, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_HAVING, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_SELECT, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_ORDER_BY, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectFuncs(pSelect, SQL_CLAUSE_FROM, pVirtualTable->table.tableAlias, fmIsScanPseudoColumnFunc,
                                  &pVtableScan->pScanPseudoCols));

  if (pVtableScan->pScanPseudoCols) {
    nodesSortList(&pVtableScan->pScanPseudoCols, comparePseudoColsByColId);
  }

  PLAN_ERR_JRET(rewriteExprsForSelect(pVtableScan->pScanPseudoCols, pSelect, SQL_CLAUSE_FROM, NULL));

  switch (pVtableScan->tableType) {
    case TSDB_SUPER_TABLE:
      PLAN_ERR_JRET(createVirtualSuperTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      break;
    case TSDB_VIRTUAL_NORMAL_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE: {
      if (inStreamCalcClause(pCxt->pPlanCxt)) {
        PLAN_ERR_JRET(createVirtualSuperTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      } else {
        PLAN_ERR_JRET(createVirtualNormalChildTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      }
      break;
    }
    default:
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_TABLE_TYPE);
  }
  pCxt->pPlanCxt->streamCxt.isVtableCalc = true;

  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  nodesDestroyNode((SNode*)pVtableScan);
  return code;
}

static int32_t doCreateLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                        SLogicNode** pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE:
      return createScanLogicNode(pCxt, pSelect, (SRealTableNode*)pTable, pLogicNode);
    case QUERY_NODE_TEMP_TABLE:
      return createSubqueryLogicNode(pCxt, pSelect, (STempTableNode*)pTable, pLogicNode);
    case QUERY_NODE_JOIN_TABLE:
      return createJoinLogicNode(pCxt, pSelect, (SJoinTableNode*)pTable, pLogicNode);
    case QUERY_NODE_VIRTUAL_TABLE:
      return createVirtualTableLogicNode(pCxt, pSelect, (SVirtualTableNode*)pTable, pLogicNode);
    default:
      code = TSDB_CODE_PLAN_INVALID_TABLE_TYPE;
      break;
  }
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  return code;
}

static int32_t createLogicNodeByTable(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SNode* pTable,
                                      SLogicNode** pLogicNode) {
  SLogicNode* pNode = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  PLAN_ERR_JRET(doCreateLogicNodeByTable(pCxt, pSelect, pTable, &pNode));
  pNode->pConditions = NULL;
  PLAN_ERR_JRET(nodesCloneNode(pSelect->pWhere, &pNode->pConditions));
  pNode->precision = pSelect->precision;
  *pLogicNode = pNode;
  pCxt->pCurrRoot = pNode;
  return code;

_return:
  planError("%s failed since %s", __func__, tstrerror(code));
  nodesDestroyNode((SNode*)pNode);
  return code;
}

static int32_t createGroupingSetNode(SNode* pExpr, SNode** ppNode) {
  SGroupingSetNode* pGroupingSet = NULL;
  int32_t           code = 0;
  *ppNode = NULL;
  code = nodesMakeNode(QUERY_NODE_GROUPING_SET, (SNode**)&pGroupingSet);
  if (NULL == pGroupingSet) {
    return code;
  }
  pGroupingSet->groupingSetType = GP_TYPE_NORMAL;
  SNode* pNew = NULL;
  code = nodesCloneNode(pExpr, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pGroupingSet->pParameterList, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *ppNode = (SNode*)pGroupingSet;
  }
  return code;
}

static bool isWindowJoinStmt(SSelectStmt* pSelect) {
  return (QUERY_NODE_JOIN_TABLE == nodeType(pSelect->pFromTable) &&
          IS_WINDOW_JOIN(((SJoinTableNode*)pSelect->pFromTable)->subType));
}

static EGroupAction getGroupAction(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  return ((NULL != pSelect->pLimit || NULL != pSelect->pSlimit) && !pSelect->isDistinct)
             ? GROUP_ACTION_KEEP
             : GROUP_ACTION_NONE;
}

static EDataOrderLevel getRequireDataOrder(bool needTimeline, SSelectStmt* pSelect) {
  return needTimeline ? (NULL != pSelect->pPartitionByList ? DATA_ORDER_LEVEL_IN_GROUP : DATA_ORDER_LEVEL_GLOBAL)
                      : DATA_ORDER_LEVEL_NONE;
}

static int32_t addWinJoinPrimKeyToAggFuncs(SSelectStmt* pSelect, SNodeList** pList) {
  SNodeList* pTargets = *pList;
  int32_t    code = 0;
  if (pTargets) {
    code = nodesMakeList(&pTargets);
  }
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  SJoinTableNode* pJoinTable = (SJoinTableNode*)pSelect->pFromTable;
  SRealTableNode* pProbeTable = NULL;
  switch (pJoinTable->joinType) {
    case JOIN_TYPE_LEFT:
      pProbeTable = (SRealTableNode*)pJoinTable->pLeft;
      break;
    case JOIN_TYPE_RIGHT:
      pProbeTable = (SRealTableNode*)pJoinTable->pRight;
      break;
    default:
      if (!*pList) nodesDestroyList(pTargets);
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  SColumnNode* pCol = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    if (!*pList) nodesDestroyList(pTargets);
    return code;
  }

  SSchema* pColSchema = &pProbeTable->pMeta->schema[0];
  tstrncpy(pCol->dbName, pProbeTable->table.dbName, TSDB_DB_NAME_LEN);
  tstrncpy(pCol->tableAlias, pProbeTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->tableName, pProbeTable->table.tableName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pCol->colName, pColSchema->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.aliasName, pColSchema->name, TSDB_COL_NAME_LEN);
  tstrncpy(pCol->node.userAlias, pColSchema->name, TSDB_COL_NAME_LEN);
  pCol->tableId = pProbeTable->pMeta->uid;
  pCol->tableType = pProbeTable->pMeta->tableType;
  pCol->colId = pColSchema->colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->hasIndex = (pColSchema != NULL && IS_IDX_ON(pColSchema));
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  pCol->node.resType.precision = pProbeTable->pMeta->tableInfo.precision;

  SNode* pFunc = (SNode*)createGroupKeyAggFunc(pCol);
  if (!pFunc) {
    nodesDestroyList(pTargets);
    return terrno;
  }

  code = nodesListStrictAppend(pTargets, pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pTargets);
  }

  return code;
}

static int32_t createAggLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasAggFuncs && NULL == pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  bool winJoin = isWindowJoinStmt(pSelect);
  pAgg->hasLastRow = pSelect->hasLastRowFunc;
  pAgg->hasLast = pSelect->hasLastFunc;
  pAgg->hasTimeLineFunc = pSelect->hasTimeLineFunc;
  pAgg->hasGroupKeyOptimized = false;
  pAgg->onlyHasKeepOrderFunc = pSelect->onlyHasKeepOrderFunc;
  pAgg->node.groupAction = winJoin ? GROUP_ACTION_NONE : getGroupAction(pCxt, pSelect);
  pAgg->node.requireDataOrder = getRequireDataOrder(pAgg->hasTimeLineFunc, pSelect);
  pAgg->node.resultDataOrder = pAgg->onlyHasKeepOrderFunc ? pAgg->node.requireDataOrder : DATA_ORDER_LEVEL_NONE;
  pAgg->node.forceCreateNonBlockingOptr = winJoin ? true : false;

  // set grouyp keys, agg funcs and having conditions
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, NULL, fmIsAggFunc, &pAgg->pAggFuncs);
  }

  // rewrite the expression in subsequent clauses
  if (TSDB_CODE_SUCCESS == code && NULL != pAgg->pAggFuncs) {
    code = rewriteExprsForSelect(pAgg->pAggFuncs, pSelect, SQL_CLAUSE_GROUP_BY, NULL);
  }

  if (NULL != pSelect->pGroupByList) {
    code = nodesListDeduplicate(&pSelect->pGroupByList);
    if (TSDB_CODE_SUCCESS == code) {
      pAgg->pGroupKeys = NULL;
      code = nodesCloneList(pSelect->pGroupByList, &pAgg->pGroupKeys);
    }
  }

  // rewrite the expression in subsequent clauses
  SNodeList* pOutputGroupKeys = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pAgg->pGroupKeys, pSelect, SQL_CLAUSE_GROUP_BY, &pOutputGroupKeys);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pAgg->node.pConditions = NULL;
    code = nodesCloneNode(pSelect->pHaving, &pAgg->node.pConditions);
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

  SIndefRowsFuncLogicNode* pIdfRowsFunc = NULL;
  int32_t                  code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC, (SNode**)&pIdfRowsFunc);
  if (NULL == pIdfRowsFunc) {
    return code;
  }

  pIdfRowsFunc->isTailFunc = pSelect->hasTailFunc;
  pIdfRowsFunc->isUniqueFunc = pSelect->hasUniqueFunc;
  pIdfRowsFunc->isTimeLineFunc = pSelect->hasTimeLineFunc;
  pIdfRowsFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pIdfRowsFunc->node.requireDataOrder = getRequireDataOrder(pIdfRowsFunc->isTimeLineFunc, pSelect);
  pIdfRowsFunc->node.resultDataOrder = pIdfRowsFunc->node.requireDataOrder;

  // indefinite rows functions and _select_values functions
  code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, fmIsVectorFunc, &pIdfRowsFunc->pFuncs);
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
  return fmIsInterpFunc(funcId) || fmIsInterpPseudoColumnFunc(funcId) || fmIsGroupKeyFunc(funcId) ||
         fmisSelectGroupConstValueFunc(funcId);
}

static int32_t createInterpFuncLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasInterpFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SInterpFuncLogicNode* pInterpFunc = NULL;
  int32_t               code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INTERP_FUNC, (SNode**)&pInterpFunc));

  pInterpFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pInterpFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pInterpFunc->node.resultDataOrder = pInterpFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  PLAN_ERR_JRET(nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isInterpFunc, &pInterpFunc->pFuncs));

  PLAN_ERR_JRET(rewriteExprsForSelect(pInterpFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL));

  if (NULL != pSelect->pFill) {
    SFillNode* pFill = (SFillNode*)pSelect->pFill;
    pInterpFunc->timeRange = pFill->timeRange;
    TSWAP(pInterpFunc->pTimeRange, pFill->pTimeRange);    
    pInterpFunc->fillMode = pFill->mode;
    pInterpFunc->pTimeSeries = NULL;
    PLAN_ERR_JRET(nodesCloneNode(pFill->pWStartTs, &pInterpFunc->pTimeSeries));
    PLAN_ERR_JRET(nodesCloneNode(pFill->pValues, &pInterpFunc->pFillValues));
    if (NULL != pFill->pSurroundingTime &&
        nodeType(pFill->pSurroundingTime) == QUERY_NODE_VALUE) {
      SValueNode* pSurroundingTime = (SValueNode*)pFill->pSurroundingTime;
      pInterpFunc->surroundingTime = pSurroundingTime->datum.i;
    }
  }

  if (NULL != pSelect->pEvery) {
    pInterpFunc->interval = ((SValueNode*)pSelect->pEvery)->datum.i;
    pInterpFunc->intervalUnit = ((SValueNode*)pSelect->pEvery)->unit;
    pInterpFunc->precision = pSelect->precision;
  }

  if (pSelect->pRangeAround) {
    SNode* pRangeInterval = ((SRangeAroundNode*)pSelect->pRangeAround)->pInterval;
    if (!pRangeInterval || nodeType(pRangeInterval) != QUERY_NODE_VALUE) {
      planError("%s failed at line %d since range interval is invalid", __func__, __LINE__);
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INTERNAL_ERROR);
    } else {
      pInterpFunc->surroundingTime = ((SValueNode*)pRangeInterval)->datum.i;
    }
  }


  // set the output
  PLAN_ERR_JRET(createColumnByRewriteExprs(pInterpFunc->pFuncs, &pInterpFunc->node.pTargets));

  *pLogicNode = (SLogicNode*)pInterpFunc;
  return code;

_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  nodesDestroyNode((SNode*)pInterpFunc);
  return code;
}

static bool isForecastFunc(int32_t funcId) {
  return fmIsForecastFunc(funcId) || fmIsAnalysisPseudoColumnFunc(funcId) || fmIsGroupKeyFunc(funcId) ||
         fmisSelectGroupConstValueFunc(funcId);
}

static int32_t createForecastFuncLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasForecastFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SForecastFuncLogicNode* pForecastFunc = NULL;
  int32_t                 code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC, (SNode**)&pForecastFunc);
  if (NULL == pForecastFunc) {
    return code;
  }

  pForecastFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pForecastFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pForecastFunc->node.resultDataOrder = pForecastFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isForecastFunc, &pForecastFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pForecastFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pForecastFunc->pFuncs, &pForecastFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pForecastFunc;
  } else {
    nodesDestroyNode((SNode*)pForecastFunc);
  }

  return code;
}

static bool isGenericAnalysisFunc(int32_t funcId) {
  return fmIsImputationCorrelationFunc(funcId) || fmIsGroupKeyFunc(funcId) || fmisSelectGroupConstValueFunc(funcId) ||
         fmIsAnalysisPseudoColumnFunc(funcId);
}

static int32_t createGenericAnalysisLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasGenericAnalysisFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SGenericAnalysisLogicNode * pFunc = NULL;
  int32_t                 code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_ANALYSIS_FUNC, (SNode**)&pFunc);
  if (NULL == pFunc) {
    return code;
  }

  pFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pFunc->node.resultDataOrder = pFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isGenericAnalysisFunc, &pFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pFunc->pFuncs, &pFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pFunc;
  } else {
    nodesDestroyNode((SNode*)pFunc);
  }

  return code;
}

static int32_t createWindowLogicNodeFinalize(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SWindowLogicNode* pWindow,
                                             SLogicNode** pLogicNode) {
  pWindow->node.inputTsOrder = ORDER_UNKNOWN;
  pWindow->node.outputTsOrder = ORDER_ASC;

  int32_t code = nodesCollectFuncs(pSelect, SQL_CLAUSE_WINDOW, NULL, fmIsWindowClauseFunc, &pWindow->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    code = nodesCloneNode(pSelect->pHaving, &pWindow->node.pConditions);
  }

  pSelect->hasAggFuncs = false;

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pWindow;
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  return code;
}

static int32_t createExternalWindowLogicNodeFinalize(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SWindowLogicNode* pWindow,
                                                     SLogicNode** pLogicNode) {
  pWindow->node.inputTsOrder = ORDER_ASC;
  pWindow->node.outputTsOrder = ORDER_ASC;

  int32_t code = TSDB_CODE_SUCCESS;
  SNodeList* pOutputPartitionKeys = NULL;
  // no agg func
  if (pSelect->pWindow && nodeType(pSelect->pWindow) != QUERY_NODE_EXTERNAL_WINDOW) {
    // just copy targets from child node, since the window node will process the functions
    PLAN_ERR_RET(nodesCloneList(pCxt->pCurrRoot->pTargets, &pWindow->node.pTargets));
    pWindow->node.requireDataOrder = pCxt->pCurrRoot->resultDataOrder;
    pWindow->node.resultDataOrder = pCxt->pCurrRoot->resultDataOrder;
  } else {
    if (!pSelect->hasAggFuncs) {
      if (pSelect->hasIndefiniteRowsFunc) {
        pWindow->node.requireDataOrder = getRequireDataOrder(pSelect->hasTimeLineFunc, pSelect);
        pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;
        nodesDestroyList(pWindow->pFuncs);
        pWindow->pFuncs = NULL;
        PLAN_ERR_RET(nodesCollectFuncs(pSelect, SQL_CLAUSE_EXT_WINDOW, NULL, fmIsStreamVectorFunc, &pWindow->pFuncs));
        PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_EXT_WINDOW, NULL));
        PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets));
        pWindow->indefRowsFunc = true;
        pSelect->hasIndefiniteRowsFunc = false;
      } else {
        pWindow->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
        pWindow->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
        PLAN_ERR_RET(nodesCloneList(pSelect->pProjectionList, &pWindow->pProjs));
        PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pProjs, pSelect, SQL_CLAUSE_EXT_WINDOW, NULL));

        // Supplement projection with ORDER BY simple columns (COLUMN) as hidden columns,
        // so upper Sort can bind keys above external_window using child output slots.
        if (pSelect->pOrderByList && LIST_LENGTH(pSelect->pOrderByList) > 0) {
          SNode* pOB = NULL;
          FOREACH(pOB, pSelect->pOrderByList) {
            SNode* pExpr = (nodeType(pOB) == QUERY_NODE_ORDER_BY_EXPR) ? ((SOrderByExprNode*)pOB)->pExpr : pOB;
            if (pExpr == NULL || QUERY_NODE_COLUMN != nodeType(pExpr)) {
              continue;
            }

            SColumnNode* pCol = (SColumnNode*)pExpr;
            if (0 == strcmp(pCol->colName, "*")) {
              continue;
            }

            // Deduplicate: skip when pProjs already contains same column name or alias.
            bool exists = false;
            SNode* pProj = NULL;
            FOREACH(pProj, pWindow->pProjs) {
              if (!nodesIsExprNode(pProj)) {
                continue;
              }

              const char* a1 = ((SExprNode*)pProj)->aliasName;
              const char* a2 = ((SExprNode*)pExpr)->aliasName;
              if (a1[0] != '\0' && a2[0] != '\0' && strcasecmp(a1, a2) == 0) {
                exists = true;
                break;
              }

              if (QUERY_NODE_COLUMN == nodeType(pProj)) {
                const char* c1 = ((SColumnNode*)pProj)->colName;
                if (strcasecmp(c1, pCol->colName) == 0) {
                  exists = true;
                  break;
                }
              }
            }

            if (exists) {
              continue;
            }

            // Append a cloned column and backfill stable aliases.
            SNode* pClone = NULL;
            PLAN_ERR_RET(nodesCloneNode(pExpr, &pClone));
            if (nodesIsExprNode(pClone)) {
              SExprNode* pE = (SExprNode*)pClone;
              if (pE->aliasName[0] == '\0') {
                tstrncpy(pE->aliasName, ((SColumnNode*)pClone)->colName, TSDB_COL_NAME_LEN);
              }
              if (pE->userAlias[0] == '\0') {
                tstrncpy(pE->userAlias, ((SColumnNode*)pClone)->colName, TSDB_COL_NAME_LEN);
              }
            }

            PLAN_ERR_RET(nodesListMakeStrictAppend(&pWindow->pProjs, pClone));
          }
        }

        PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pProjs, &pWindow->node.pTargets));
      }
    } else {
      // has agg func, collect again with placeholder func
      pWindow->node.requireDataOrder = getRequireDataOrder(pSelect->hasTimeLineFunc, pSelect);
      pWindow->node.resultDataOrder = pSelect->onlyHasKeepOrderFunc ? pWindow->node.requireDataOrder : DATA_ORDER_LEVEL_NONE;
      nodesDestroyList(pWindow->pFuncs);
      pWindow->pFuncs = NULL;
      PLAN_ERR_RET(nodesCollectFuncs(pSelect, SQL_CLAUSE_EXT_WINDOW, NULL, fmIsStreamWindowClauseFunc, &pWindow->pFuncs));
      PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_EXT_WINDOW, NULL));

      if (NULL != pSelect->pPartitionByList) {
        SNodeList* pPartKeys = NULL;
        PLAN_ERR_RET(nodesCloneList(pSelect->pPartitionByList, &pPartKeys));
        code = rewriteExprsForSelect(pPartKeys, pSelect, SQL_CLAUSE_EXT_WINDOW, &pOutputPartitionKeys);
        nodesDestroyList(pPartKeys);
        PLAN_ERR_RET(code);

        if (NULL != pOutputPartitionKeys) {
          SNode* pPartKey = NULL;
          FOREACH(pPartKey, pOutputPartitionKeys) {
            if (QUERY_NODE_COLUMN != nodeType(pPartKey)) {
              planError("external window partition key must be column node, nodeType:%d", nodeType(pPartKey));
              PLAN_ERR_RET(TSDB_CODE_PLAN_INTERNAL_ERROR);
            }

            SFunctionNode* pGroupKey = createGroupKeyAggFunc((SColumnNode*)pPartKey);
            if (NULL == pGroupKey) {
              PLAN_ERR_RET(terrno);
            }
            tstrncpy(pGroupKey->node.aliasName, ((SExprNode*)pPartKey)->aliasName, TSDB_COL_NAME_LEN);
            tstrncpy(pGroupKey->node.userAlias, ((SExprNode*)pPartKey)->userAlias, TSDB_COL_NAME_LEN);
            PLAN_ERR_RET(nodesListMakeStrictAppend(&pWindow->pFuncs, (SNode*)pGroupKey));
          }
        }
      }

      // Keep logic targets aligned with the physical external-window output order:
      // function outputs are materialized before projection outputs, and split Exchange
      // nodes clone targets from the logic child.
      PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets));

      SNodeList* pProjTargets = NULL;
      PLAN_ERR_RET(nodesCloneList(pWindow->pProjs, &pProjTargets));
      PLAN_ERR_RET(rewriteExprsForSelect(pProjTargets, pSelect, SQL_CLAUSE_EXT_WINDOW, NULL));
      PLAN_ERR_RET(createColumnByRewriteExprs(pProjTargets, &pWindow->node.pTargets));
      nodesDestroyList(pProjTargets);
      
      pSelect->hasAggFuncs = false;
    }
  }

  pWindow->inputHasOrder = (pWindow->isSingleTable || pWindow->node.requireDataOrder == DATA_ORDER_LEVEL_GLOBAL);

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    pWindow->node.pConditions = NULL;
    code = nodesCloneNode(pSelect->pHaving, &pWindow->node.pConditions);
  }

  nodesDestroyList(pOutputPartitionKeys);

  *pLogicNode = (SLogicNode*)pWindow;

  return code;
}

static int32_t createWindowLogicNodeByState(SLogicPlanContext* pCxt, SStateWindowNode* pState, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_STATE;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;
  pWindow->pStateExpr = NULL;
  pWindow->partType |= (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? WINDOW_PART_HAS : 0;
  pWindow->partType |= (pSelect->pPartitionByList && keysHasTbname(pSelect->pPartitionByList)) ? WINDOW_PART_TB : 0;
  code = nodesCloneNode(pState->pExpr, &pWindow->pStateExpr);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  code = nodesCloneNode(pState->pCol, &pWindow->pTspk);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  if (pState->pExtend) {
    pWindow->extendOption = ((SValueNode*)pState->pExtend)->datum.i;
  }
  if (pState->pTrueForLimit) {
    if (QUERY_NODE_VALUE == nodeType(pState->pTrueForLimit)) {
      pWindow->trueForType = TRUE_FOR_DURATION_ONLY;
      pWindow->trueForCount = 0;
      pWindow->trueForDuration = ((SValueNode*)pState->pTrueForLimit)->datum.i;
    } else {
      pWindow->trueForType = ((STrueForNode*)pState->pTrueForLimit)->trueForType;
      pWindow->trueForCount = ((STrueForNode*)pState->pTrueForLimit)->count;
      SNode* pDuration = ((STrueForNode*)pState->pTrueForLimit)->pDuration;
      pWindow->trueForDuration = pDuration ? ((SValueNode*)pDuration)->datum.i : 0;
    }
  }
  // rewrite the expression in subsequent clauses
  code = rewriteExprForSelect(pWindow->pStateExpr, pSelect, SQL_CLAUSE_WINDOW);
  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  return code;
}

static int32_t createWindowLogicNodeBySession(SLogicPlanContext* pCxt, SSessionWindowNode* pSession,
                                              SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_SESSION;
  pWindow->sessionGap = ((SValueNode*)pSession->pGap)->datum.i;

  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;
  pWindow->partType |= (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? WINDOW_PART_HAS : 0;
  pWindow->partType |= (pSelect->pPartitionByList && keysHasTbname(pSelect->pPartitionByList)) ? WINDOW_PART_TB : 0;

  pWindow->pTspk = NULL;
  code = nodesCloneNode((SNode*)pSession->pCol, &pWindow->pTspk);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  pWindow->pTsEnd = NULL;
  code = nodesCloneNode((SNode*)pSession->pCol, &pWindow->pTsEnd);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByInterval(SLogicPlanContext* pCxt, SIntervalWindowNode* pInterval,
                                               SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_INTERVAL;
  pWindow->interval = ((SValueNode*)pInterval->pInterval)->datum.i;
  pWindow->intervalUnit = ((SValueNode*)pInterval->pInterval)->unit;
  pWindow->offset = (NULL != pInterval->pOffset ? ((SValueNode*)pInterval->pOffset)->datum.i : 0);
  pWindow->sliding = (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->datum.i : pWindow->interval);
  pWindow->slidingUnit =
      (NULL != pInterval->pSliding ? ((SValueNode*)pInterval->pSliding)->unit : pWindow->intervalUnit);
  pWindow->windowAlgo = INTERVAL_ALGO_HASH;
  pWindow->node.groupAction = (NULL != pInterval->pFill ? GROUP_ACTION_KEEP : getGroupAction(pCxt, pSelect));
  pWindow->node.requireDataOrder = (pSelect->hasTimeLineFunc ? getRequireDataOrder(true, pSelect) : DATA_ORDER_LEVEL_NONE);
  pWindow->node.resultDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->pTspk = NULL;
  code = nodesCloneNode(pInterval->pCol, &pWindow->pTspk);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  pWindow->partType |= (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? WINDOW_PART_HAS : 0;
  pWindow->partType |= (pSelect->pPartitionByList && keysHasTbname(pSelect->pPartitionByList)) ? WINDOW_PART_TB : 0;
  pWindow->timeRange = pInterval->timeRange;

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByEvent(SLogicPlanContext* pCxt, SEventWindowNode* pEvent, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_EVENT;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder =  pWindow->node.requireDataOrder;
  pWindow->pStartCond = NULL;
  code = nodesCloneNode(pEvent->pStartCond, &pWindow->pStartCond);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  code = nodesCloneNode(pEvent->pEndCond, &pWindow->pEndCond);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  code = nodesCloneNode(pEvent->pCol, &pWindow->pTspk);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  if (NULL == pWindow->pStartCond || NULL == pWindow->pEndCond || NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pEvent->pTrueForLimit) {
    if (QUERY_NODE_VALUE == nodeType(pEvent->pTrueForLimit)) {
      pWindow->trueForType = TRUE_FOR_DURATION_ONLY;
      pWindow->trueForCount = 0;
      pWindow->trueForDuration = ((SValueNode*)pEvent->pTrueForLimit)->datum.i;
    } else {
      pWindow->trueForType = ((STrueForNode*)pEvent->pTrueForLimit)->trueForType;
      pWindow->trueForCount = ((STrueForNode*)pEvent->pTrueForLimit)->count;
      SNode* pDuration = ((STrueForNode*)pEvent->pTrueForLimit)->pDuration;
      pWindow->trueForDuration = pDuration ? ((SValueNode*)pDuration)->datum.i : 0;
    }
  }
  pWindow->partType |= (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? WINDOW_PART_HAS : 0;
  pWindow->partType |= (pSelect->pPartitionByList && keysHasTbname(pSelect->pPartitionByList)) ? WINDOW_PART_TB : 0;

  return createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
}

static int32_t createWindowLogicNodeByCount(SLogicPlanContext* pCxt, SCountWindowNode* pCount, SSelectStmt* pSelect,
                                            SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow));

  pWindow->winType = WINDOW_TYPE_COUNT;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;
  pWindow->windowCount = pCount->windowCount;
  pWindow->windowSliding = pCount->windowSliding;
  pWindow->pTspk = NULL;
  pWindow->partType |= (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? WINDOW_PART_HAS : 0;
  pWindow->partType |= (pSelect->pPartitionByList && keysHasTbname(pSelect->pPartitionByList)) ? WINDOW_PART_TB : 0;

  PLAN_ERR_JRET(nodesCloneNode(pCount->pCol, &pWindow->pTspk));
  if (pCount->pColList != NULL) {
    PLAN_ERR_JRET(nodesCloneList(pCount->pColList, &pWindow->pColList));
  }

  PLAN_ERR_JRET(createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode));
  return code;

_return:
  planError("%s failed, code:%d", __func__, code);
  nodesDestroyNode((SNode*)pWindow);
  return code;
}

static int32_t createWindowLogicNodeByAnomaly(SLogicPlanContext* pCxt, SAnomalyWindowNode* pAnomaly,
                                              SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_ANOMALY;
  pWindow->node.groupAction = getGroupAction(pCxt, pSelect);
  pWindow->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;

  pWindow->pAnomalyExpr = NULL;
  code = nodesCloneList(pAnomaly->pExpr, &pWindow->pAnomalyExpr);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }

  tstrncpy(pWindow->anomalyOpt, pAnomaly->anomalyOpt, sizeof(pWindow->anomalyOpt));

  pWindow->pTspk = NULL;
  code = nodesCloneNode(pAnomaly->pCol, &pWindow->pTspk);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }

  // rewrite the expression in subsequent clauses
  code = rewriteExprsForSelect(pWindow->pAnomalyExpr, pSelect, SQL_CLAUSE_WINDOW, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  return code;
}

static int32_t setColTableInfo(SNode* pFromTable, SColumnNode* pCol) {
  if (NULL == pFromTable || NULL == pCol) {
    return TSDB_CODE_SUCCESS;
  }

  // Default to a normal column
  pCol->colType = COLUMN_TYPE_COLUMN;

  switch (nodeType(pFromTable)) {
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pTable = (SRealTableNode*)pFromTable;
      tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
      if (pTable->pMeta) {
        pCol->tableId = pTable->pMeta->uid;
        pCol->tableType = pTable->pMeta->tableType;
      }
      break;
    }
    case QUERY_NODE_VIRTUAL_TABLE: {
      SVirtualTableNode* pTable = (SVirtualTableNode*)pFromTable;
      tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
      if (pTable->pMeta) {
        pCol->tableId = pTable->pMeta->uid;
        pCol->tableType = pTable->pMeta->tableType;
      }
      break;
    }
    case QUERY_NODE_PLACE_HOLDER_TABLE: {
      SPlaceHolderTableNode* pTable = (SPlaceHolderTableNode*)pFromTable;
      tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
      if (pTable->pMeta) {
        pCol->tableId = pTable->pMeta->uid;
        pCol->tableType = pTable->pMeta->tableType;
      }
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      STempTableNode* pTable = (STempTableNode*)pFromTable;
      tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
      // temp table may not have meta; leave tableId/tableType unset
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      // External window does not apply to joins; minimal alias propagation
      SJoinTableNode* pTable = (SJoinTableNode*)pFromTable;
      tstrncpy(pCol->dbName, pTable->table.dbName, TSDB_DB_NAME_LEN);
      tstrncpy(pCol->tableName, pTable->table.tableName, TSDB_TABLE_NAME_LEN);
      tstrncpy(pCol->tableAlias, pTable->table.tableAlias, TSDB_TABLE_NAME_LEN);
      break;
    }
    default: {
      // Fallback: try to copy common table fields if layout matches STableNode
      // No-op if not applicable
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// Helper: create a timestamp comparison operator with a placeholder function on the right
static int32_t makeTsPlaceholderOp(SNode* pFromTable, EOperatorType opType, const char* funcName, const char* alias,
                                   SOperatorNode** pOutOp) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SFunctionNode* pFunc = NULL;
  SOperatorNode* pOper = NULL;

  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);
  if (TSDB_CODE_SUCCESS != code || NULL == pFunc) return code;
  tstrncpy(pFunc->functionName, funcName, TSDB_FUNC_NAME_LEN);
  tstrncpy(pFunc->node.userAlias, alias, TSDB_FUNC_NAME_LEN);

  code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOper);
  if (TSDB_CODE_SUCCESS != code || NULL == pOper) {
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }

  pOper->opType = opType;
  code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pOper->pLeft);
  if (TSDB_CODE_SUCCESS != code || NULL == pOper->pLeft) {
    nodesDestroyNode((SNode*)pOper);
    nodesDestroyNode((SNode*)pFunc);
    return code;
  }
  ((SColumnNode*)pOper->pLeft)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  ((SColumnNode*)pOper->pLeft)->isPrimTs = true;
  snprintf(((SColumnNode*)pOper->pLeft)->colName, sizeof(((SColumnNode*)pOper->pLeft)->colName), "%s", "_c0");
  setColTableInfo(pFromTable, (SColumnNode*)pOper->pLeft);


  code = nodesCloneNode((SNode*)pFunc, &pOper->pRight);
  nodesDestroyNode((SNode*)pFunc); // function node no longer needed after clone
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pOper);
    return code;
  }

  *pOutOp = pOper;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDefaultTimeRangeForExternalWindow(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  if (pSelect->pWindow == NULL || nodeType(pSelect->pWindow) != QUERY_NODE_EXTERNAL_WINDOW ||
      pSelect->pTimeRange != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t         code = TSDB_CODE_SUCCESS;
  STimeRangeNode* pTimeRange = NULL;
  SOperatorNode*  pStartOp = NULL;
  SOperatorNode*  pEndOp = NULL;

  code = nodesMakeNode(QUERY_NODE_TIME_RANGE, (SNode**)&pTimeRange);
  if (TSDB_CODE_SUCCESS != code || NULL == pTimeRange) return code;

  pTimeRange->pStart = NULL;
  pTimeRange->pEnd = NULL;
  pTimeRange->needCalc = false;

  // ts >= _twstart, ts < _twend
  code = makeTsPlaceholderOp(pSelect->pFromTable, OP_TYPE_GREATER_EQUAL, "_twstart", "_twstart", &pStartOp);
  if (TSDB_CODE_SUCCESS != code || NULL == pStartOp) {
    nodesDestroyNode((SNode*)pTimeRange);
    return code;
  }
  code = makeTsPlaceholderOp(pSelect->pFromTable, OP_TYPE_LOWER_THAN, "_twend", "_twend", &pEndOp);
  if (TSDB_CODE_SUCCESS != code || NULL == pEndOp) {
    nodesDestroyNode((SNode*)pStartOp);
    nodesDestroyNode((SNode*)pTimeRange);
    return code;
  }

  pTimeRange->pStart = (SNode*)pStartOp;
  pTimeRange->pEnd = (SNode*)pEndOp;
  pSelect->pTimeRange = (SNode*)pTimeRange;
  return TSDB_CODE_SUCCESS;
}

static int32_t createWindowLogicNodeByExternal(SLogicPlanContext* pCxt, SExternalWindowNode* pExternal,
                                               SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  pCxt->pPlanCxt->streamCxt.hasExtWindow = true;

  SWindowLogicNode* pWindow = NULL;
  int32_t           code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow));

  pWindow->winType = WINDOW_TYPE_EXTERNAL;
  pWindow->node.groupAction = GROUP_ACTION_NONE;
  pWindow->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pWindow->node.resultDataOrder = (NULL != pSelect->pPartitionByList ? DATA_ORDER_LEVEL_IN_GROUP : DATA_ORDER_LEVEL_GLOBAL);
  pWindow->calcWithPartition = (NULL != pSelect->pPartitionByList);
  pWindow->needGroupSort = false;
  pWindow->partType = 0;
  pWindow->pTspk = NULL;
  bool isPartTb = pSelect->pPartitionByList ? keysHasTbname(pSelect->pPartitionByList) : 0;
  if (nodeType(pSelect->pFromTable) == QUERY_NODE_REAL_TABLE) {
    SRealTableNode* pTable = (SRealTableNode*)pSelect->pFromTable;
    if (pTable->pMeta->tableType == TSDB_NORMAL_TABLE || pTable->pMeta->tableType == TSDB_CHILD_TABLE || isPartTb || pTable->asSingleTable) {
      pWindow->isSingleTable = true;
    } else {
      pWindow->isSingleTable = false;
    }
  } else if (nodeType(pSelect->pFromTable) == QUERY_NODE_VIRTUAL_TABLE) {
    SVirtualTableNode* pTable = (SVirtualTableNode*)pSelect->pFromTable;
    if (pTable->pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE || pTable->pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE || isPartTb) {
      pWindow->isSingleTable = true;
    } else {
      pWindow->isSingleTable = false;
    }
  } else {
    pWindow->isSingleTable = false;
  }
  PLAN_ERR_RET(nodesCloneNode(pSelect->pTimeRange, &pWindow->pTimeRange));

  if (NULL == pExternal->pCol) {
    planError("%s failed, External window can not find pk column", __func__);
    nodesDestroyNode((SNode*)pWindow);
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  PLAN_ERR_RET(nodesCloneNode(pExternal->pCol, &pWindow->pTspk));

  pWindow->pSubquery = pExternal->pSubquery;
  return createExternalWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);

_return:
  planError("%s failed, code:%d", __func__, code);
  nodesDestroyNode((SNode*)pWindow);
  return code;
}

static int32_t createWindowLogicNodeByStreamExternal(SLogicPlanContext* pCxt, SExternalWindowNode* pExternal,
                                               SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SWindowLogicNode* pWindow = NULL;
  int32_t           code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_WINDOW, (SNode**)&pWindow);
  if (NULL == pWindow) {
    return code;
  }

  pWindow->winType = WINDOW_TYPE_EXTERNAL;
  pWindow->node.groupAction = GROUP_ACTION_NONE;
  pWindow->node.requireDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pWindow->node.resultDataOrder = DATA_ORDER_LEVEL_GLOBAL;
  pWindow->partType = 0;
  pWindow->pTspk = NULL;
  if (nodeType(pSelect->pFromTable) == QUERY_NODE_REAL_TABLE) {
    SRealTableNode* pTable = (SRealTableNode*)pSelect->pFromTable;
    if (pTable->pMeta->tableType == TSDB_NORMAL_TABLE || pTable->pMeta->tableType == TSDB_CHILD_TABLE ||
        pTable->asSingleTable) {
      pWindow->isSingleTable = true;
    } else {
      pWindow->isSingleTable = false;
    }
  } else if (nodeType(pSelect->pFromTable) == QUERY_NODE_VIRTUAL_TABLE) {
    SVirtualTableNode* pTable = (SVirtualTableNode*)pSelect->pFromTable;
    if (pTable->pMeta->tableType == TSDB_VIRTUAL_NORMAL_TABLE || pTable->pMeta->tableType == TSDB_VIRTUAL_CHILD_TABLE) {
      pWindow->isSingleTable = true;
    } else {
      pWindow->isSingleTable = false;
    }
  } else {
    pWindow->isSingleTable = false;
  }
  PLAN_ERR_RET(nodesCloneNode(pSelect->pTimeRange, &pWindow->pTimeRange));

  SNode* pNode = NULL;
  FOREACH(pNode, pCxt->pCurrRoot->pTargets) {
    if (QUERY_NODE_COLUMN == nodeType(pNode)) {
      SColumnNode* pCol = (SColumnNode*)pNode;
      
      if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        PLAN_ERR_RET(nodesCloneNode(pNode, &pWindow->pTspk));
        break;
      }
    }
  }

  if (pWindow->pTspk == NULL) {
    nodesDestroyNode((SNode*)pWindow);
    planError("External window can not find pk column, listSize:%d", pCxt->pCurrRoot->pTargets->length);
    // TODO(smj): proper error code;
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  }

  pWindow->pSubquery = pExternal->pSubquery;
  return createExternalWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
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
    case QUERY_NODE_ANOMALY_WINDOW:
      return createWindowLogicNodeByAnomaly(pCxt, (SAnomalyWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
    case QUERY_NODE_EXTERNAL_WINDOW:
      return TSDB_CODE_SUCCESS;
    default:
      break;
  }

  planError("%s failed, unsupported window node type:%d", __func__, nodeType(pSelect->pWindow));
  return TSDB_CODE_PLAN_INVALID_WINDOW_TYPE;
}

typedef struct SConditionCheckContext {
  bool    hasNotBasicOp;
  bool    hasNegativeConst;
  bool    hasOtherFunc;
  bool    placeholderAtRight;
  bool    hasPlaceHolder;
  int32_t placeholderType;
} SConditionCheckContext;

static EDealRes conditionOnlyPhAndConstImpl(SNode* pNode, void* pContext) {
  SConditionCheckContext* pCxt = (SConditionCheckContext*)pContext;
  if (nodeType(pNode) == QUERY_NODE_VALUE) {
    SValueNode *pVal = (SValueNode*)pNode;
    if (pVal->datum.i < 0) {
      pCxt->hasNegativeConst = true;
    }
  } else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode *pFunc = (SFunctionNode*)pNode;
    if(fmIsPlaceHolderFunc(pFunc->funcId)) {
      pCxt->hasPlaceHolder = true;
    }
    if (pFunc->funcType == FUNCTION_TYPE_TWSTART ||
      pFunc->funcType == FUNCTION_TYPE_TWEND ||
      pFunc->funcType == FUNCTION_TYPE_TPREV_TS ||
      pFunc->funcType == FUNCTION_TYPE_TNEXT_TS ||
      pFunc->funcType == FUNCTION_TYPE_TCURRENT_TS ||
      pFunc->funcType == FUNCTION_TYPE_TPREV_LOCALTIME ||
      pFunc->funcType == FUNCTION_TYPE_TNEXT_LOCALTIME ||
      pFunc->funcType == FUNCTION_TYPE_TLOCALTIME ||
      pFunc->funcType == FUNCTION_TYPE_TIDLESTART ||
      pFunc->funcType == FUNCTION_TYPE_TIDLEEND) {
      pCxt->placeholderType = pFunc->funcType;
    } else if (pFunc->funcType != FUNCTION_TYPE_NOW &&
      pFunc->funcType != FUNCTION_TYPE_TODAY &&
      pFunc->funcType != FUNCTION_TYPE_CAST) {
      pCxt->hasOtherFunc = true;
    }
  } else if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
    SOperatorNode *pOp = (SOperatorNode*)pNode;
    if (!nodesIsBasicArithmeticOp(pOp)) {
      pCxt->hasNotBasicOp = true;
    }
    if (pOp->opType == OP_TYPE_SUB || pOp->opType == OP_TYPE_DIV) {
      if (pOp->pRight) {
        SConditionCheckContext cxt = {.placeholderType = 0};
        nodesWalkExpr(pOp->pRight, conditionOnlyPhAndConstImpl, &cxt);
        pCxt->placeholderAtRight = (cxt.placeholderType != 0);
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool placeHolderCanMakeExternalWindow(int32_t startType, int32_t endType) {
  switch (startType) {
    case FUNCTION_TYPE_TPREV_TS: {
      return endType == FUNCTION_TYPE_TCURRENT_TS || endType == FUNCTION_TYPE_TNEXT_TS;
    }
    case FUNCTION_TYPE_TCURRENT_TS: {
      return endType == FUNCTION_TYPE_TNEXT_TS;
    }
    case FUNCTION_TYPE_TWSTART: {
      return endType == FUNCTION_TYPE_TWEND;
    }
    case FUNCTION_TYPE_TPREV_LOCALTIME: {
      return endType == FUNCTION_TYPE_TNEXT_LOCALTIME || endType == FUNCTION_TYPE_TLOCALTIME;
    }
    case FUNCTION_TYPE_TLOCALTIME: {
      return endType == FUNCTION_TYPE_TNEXT_LOCALTIME;
    }
    case FUNCTION_TYPE_TIDLESTART: {
      return endType == FUNCTION_TYPE_TIDLEEND;
    }
    default: {
      return false;
    }
  }
}

static bool filterHasPlaceHolderRange(SOperatorNode *pOperator) {
  SNode* pOpLeft = pOperator->pLeft;
  SNode* pOpRight = pOperator->pRight;

  if (pOpLeft == NULL || pOpRight == NULL) {
    return false;
  }

  if (nodeType(pOpLeft) == QUERY_NODE_COLUMN) {
    SColumnNode* pTsCol = (SColumnNode*)pOpLeft;
    if (pTsCol->colType != COLUMN_TYPE_COLUMN || pTsCol->colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
      return false;
    }
  } else {
    return false;
  }

  SConditionCheckContext opCxt = {.hasNotBasicOp = false,
                                  .hasNegativeConst = false,
                                  .hasOtherFunc = false,
                                  .placeholderAtRight = false,
                                  .placeholderType = 0};

  nodesWalkExpr(pOpRight, conditionOnlyPhAndConstImpl, &opCxt);
  if (opCxt.hasNotBasicOp || opCxt.hasNegativeConst || opCxt.hasOtherFunc || opCxt.placeholderAtRight) {
    return false;
  }
  return true;
}

static bool logicConditionSatisfyExternalWindow(SLogicConditionNode *pLogicCond) {
  if (pLogicCond->condType != LOGIC_COND_TYPE_AND || LIST_LENGTH(pLogicCond->pParameterList) == 0) {
    return false;
  }
  SNode *pOperator = NULL;
  FOREACH(pOperator, pLogicCond->pParameterList) {
    if (nodeType(pOperator) != QUERY_NODE_OPERATOR) {
      return false;
    }
    if (!filterHasPlaceHolderRange((SOperatorNode*)pOperator)) {
      return false;
    }
  }
  return true;
}


static bool timeRangeSatisfyExternalWindow(STimeRangeNode* pTimeRange) {
  if (!pTimeRange || !pTimeRange->pStart || !pTimeRange->pEnd) {
    return false;
  }

  if (nodeType(pTimeRange->pStart) == QUERY_NODE_OPERATOR) {
    if (!filterHasPlaceHolderRange((SOperatorNode*)pTimeRange->pStart)) {
      return false;
    }
  } else if (nodeType(pTimeRange->pStart) == QUERY_NODE_LOGIC_CONDITION) {
    if (!logicConditionSatisfyExternalWindow((SLogicConditionNode*)pTimeRange->pStart)) {
      return false;
    }
  } else {
    return false;
  }

  if (nodeType(pTimeRange->pEnd) == QUERY_NODE_OPERATOR) {
    if (!filterHasPlaceHolderRange((SOperatorNode*)pTimeRange->pEnd)) {
      return false;
    }
  } else if (nodeType(pTimeRange->pEnd) == QUERY_NODE_LOGIC_CONDITION) {
    if (!logicConditionSatisfyExternalWindow((SLogicConditionNode*)pTimeRange->pEnd)) {
      return false;
    }
  } else {
    return false;
  }

  return true;
}

static int32_t conditionHasPlaceHolder(SNode* pNode, bool* pHasPlaceHolder) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pCond = NULL;
  SNode*  pOtherCond = NULL;

  if (!pNode) {
    *pHasPlaceHolder = false;
    return code;
  }

  PAR_ERR_JRET(nodesCloneNode(pNode, &pCond));

  PAR_ERR_JRET(filterPartitionCond(&pCond, NULL, NULL, NULL, &pOtherCond));

  SConditionCheckContext cxt = {.hasPlaceHolder = false};
  nodesWalkExpr(pOtherCond, conditionOnlyPhAndConstImpl, &cxt);
  *pHasPlaceHolder = cxt.hasPlaceHolder;
_return:
  nodesDestroyNode(pOtherCond);
  nodesDestroyNode(pCond);
  return code;
}

/**
 * Determines whether the given external window is produced by a subquery
 * rather than by stream processing.
 *
 * In the query planning stage, an external window may originate from two
 * different sources:
 * 1. Generated by a subquery (e.g., derived from a relational subquery result).
 * 2. Generated by streaming computation (e.g., time window over a stream).
 *
 * This function checks if the external window belongs to the first case,
 * meaning it is derived from a subquery instead of being created by
 * stream-based window computation.
 *
 * @param pSelect The select statement containing the external window to check.
 * @return true if the window is produced by a subquery; false if it comes
 *         from stream computation.
 */
bool hasExternalWindowDerivedFromSubquery(SSelectStmt* pSelect) {
  if (pSelect->pWindow && QUERY_NODE_EXTERNAL_WINDOW == nodeType(pSelect->pWindow) &&
      ((SExternalWindowNode*)pSelect->pWindow)->pSubquery != NULL) {
    return true;
  }
  return false;
}

static int32_t checkExprListForExternalWin(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  if (pSelect->hasIndefiniteRowsFunc) {
    return generatePlanErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                              "Indefinite rows functions are not allowed in EXTERNAL_WINDOW query");
  }
  if (pSelect->hasMultiRowsFunc) {
    return generatePlanErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_FUNC,
                              "Multi-rows functions are not allowed in EXTERNAL_WINDOW query");
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkExternalWindow(SLogicPlanContext* pCxt, SSelectStmt* pSelect) {
  if (hasExternalWindowDerivedFromSubquery(pSelect)) {
    return checkExprListForExternalWin(pCxt, pSelect);
  }

  bool hasPlaceHolderCond = false;
  PAR_ERR_RET(conditionHasPlaceHolder(pSelect->pWhere, &hasPlaceHolderCond));

  pCxt->pPlanCxt->streamCxt.hasExtWindow = true;
  if (NULL != pSelect->pWindow || NULL != pSelect->pPartitionByList || NULL != pSelect->pGroupByList ||
      !inStreamCalcClause(pCxt->pPlanCxt) || pCxt->containsOuterJoin || hasPlaceHolderCond ||
      nodeType(pSelect->pFromTable) == QUERY_NODE_TEMP_TABLE ||
      nodeType(pSelect->pFromTable) == QUERY_NODE_JOIN_TABLE ||
      NULL != pSelect->pSlimit || NULL != pSelect->pLimit || pSelect->hasInterpFunc ||
      pSelect->hasUniqueFunc || pSelect->hasTailFunc || pSelect->hasForecastFunc ||
      (pSelect->pOrderByList != NULL && pCxt->pPlanCxt->streamCxt.hasForceOutput) ||
      !timeRangeSatisfyExternalWindow((STimeRangeNode*)pSelect->pTimeRange)) {
    pCxt->pPlanCxt->streamCxt.hasExtWindow = false;
  }

  if (pCxt->pPlanCxt->streamCxt.hasNotify || pCxt->pPlanCxt->streamCxt.hasForceOutput) {
    // stream has notify or force output, external window node must be the root node, if not, do not use external window
    if (pSelect->pFill || pSelect->hasInterpFunc || pSelect->hasForecastFunc || pSelect->hasGenericAnalysisFunc ||
        pSelect->isDistinct || pSelect->pOrderByList) {
      pCxt->pPlanCxt->streamCxt.hasExtWindow = false;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createExternalWindowLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (hasExternalWindowDerivedFromSubquery(pSelect)) {
    return createWindowLogicNodeByExternal(pCxt, (SExternalWindowNode*)pSelect->pWindow, pSelect, pLogicNode);
  }
  if (!pCxt->pPlanCxt->streamCxt.hasExtWindow) {
    return TSDB_CODE_SUCCESS;
  }

  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_EXTERNAL_WINDOW, &pSelect->pWindow));
  PLAN_RET(createWindowLogicNodeByStreamExternal(pCxt, (SExternalWindowNode*)pSelect->pWindow, pSelect, pLogicNode));

  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectFillExprsCtx {
  SHashObj*  pPseudoCols;
  SNodeList* pFillExprs;
  SNodeList* pNotFillExprs;
  bool       collectAggFuncs;
  SNodeList* pAggFuncCols;
} SCollectFillExprsCtx;

typedef struct SWalkFillSubExprCtx {
  bool                  hasFillCol;
  bool                  hasPseudoWinCol;
  bool                  hasGroupKeyCol;
  SCollectFillExprsCtx* pCollectFillCtx;
  int32_t               code;
} SWalkFillSubExprCtx;

static bool nodeAlreadyContained(SNodeList* pList, SNode* pNode) {
  SNode* pExpr = NULL;
  FOREACH(pExpr, pList) {
    if (nodesEqualNode(pExpr, pNode)) {
      return true;
    }
  }
  return false;
}

static EDealRes needFillValueImpl(SNode* pNode, void* pContext) {
  SWalkFillSubExprCtx* pCtx = pContext;
  EDealRes             res = DEAL_RES_CONTINUE;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (COLUMN_TYPE_WINDOW_START == pCol->colType || COLUMN_TYPE_WINDOW_END == pCol->colType ||
        COLUMN_TYPE_WINDOW_DURATION == pCol->colType || COLUMN_TYPE_IS_WINDOW_FILLED == pCol->colType) {
      pCtx->hasPseudoWinCol = true;
      pCtx->code =
          taosHashPut(pCtx->pCollectFillCtx->pPseudoCols, pCol->colName, TSDB_COL_NAME_LEN, &pNode, POINTER_BYTES);
    } else if (COLUMN_TYPE_GROUP_KEY == pCol->colType || COLUMN_TYPE_TBNAME == pCol->colType ||
               COLUMN_TYPE_TAG == pCol->colType) {
      pCtx->hasGroupKeyCol = true;
      pCtx->code =
          taosHashPut(pCtx->pCollectFillCtx->pPseudoCols, pCol->colName, TSDB_COL_NAME_LEN, &pNode, POINTER_BYTES);
    } else {
      pCtx->hasFillCol = true;
      if (pCtx->pCollectFillCtx->collectAggFuncs) {
        // Agg funcs has already been rewriten to columns by Interval
        // Here, we return DEAL_RES_CONTINUE cause we need to collect all agg funcs
        if (!nodeAlreadyContained(pCtx->pCollectFillCtx->pFillExprs, pNode) &&
            !nodeAlreadyContained(pCtx->pCollectFillCtx->pAggFuncCols, pNode))
          pCtx->code = nodesListMakeStrictAppend(&pCtx->pCollectFillCtx->pAggFuncCols, pNode);
      } else {
        res = DEAL_RES_END;
      }
    }
  }
  if (pCtx->code != TSDB_CODE_SUCCESS) res = DEAL_RES_ERROR;
  return res;
}

static void needFillValue(SNode* pNode, SWalkFillSubExprCtx* pCtx) { nodesWalkExpr(pNode, needFillValueImpl, pCtx); }

static int32_t collectFillExpr(SNode* pNode, SCollectFillExprsCtx* pCollectFillCtx) {
  SNode*              pNew = NULL;
  SWalkFillSubExprCtx collectFillSubExprCtx = {
      .hasFillCol = false, .hasPseudoWinCol = false, .hasGroupKeyCol = false, .pCollectFillCtx = pCollectFillCtx};
  needFillValue(pNode, &collectFillSubExprCtx);
  if (collectFillSubExprCtx.code != TSDB_CODE_SUCCESS) {
    return collectFillSubExprCtx.code;
  }

  if (collectFillSubExprCtx.hasFillCol && !pCollectFillCtx->collectAggFuncs) {
    if (nodeType(pNode) == QUERY_NODE_ORDER_BY_EXPR) {
      collectFillSubExprCtx.code = nodesCloneNode(((SOrderByExprNode*)pNode)->pExpr, &pNew);
    } else {
      collectFillSubExprCtx.code = nodesCloneNode(pNode, &pNew);
    }
    if (collectFillSubExprCtx.code == TSDB_CODE_SUCCESS) {
      collectFillSubExprCtx.code = nodesListMakeStrictAppend(&pCollectFillCtx->pFillExprs, pNew);
    }
  }
  return collectFillSubExprCtx.code;
}

static int32_t collectFillExprs(SSelectStmt* pSelect, SNodeList** pFillExprs, SNodeList** pNotFillExprs,
                                SNodeList** pPossibleFillNullCols) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SCollectFillExprsCtx collectFillCtx = {0};
  SNode*               pNode = NULL;
  collectFillCtx.pPseudoCols = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (!collectFillCtx.pPseudoCols) return terrno;

  FOREACH(pNode, pSelect->pProjectionList) {
    code = collectFillExpr(pNode, &collectFillCtx);
    if (code != TSDB_CODE_SUCCESS) break;
  }
  collectFillCtx.collectAggFuncs = true;
  if (code == TSDB_CODE_SUCCESS) {
    code = collectFillExpr(pSelect->pHaving, &collectFillCtx);
  }
  if (code == TSDB_CODE_SUCCESS) {
    FOREACH(pNode, pSelect->pOrderByList) {
      code = collectFillExpr(pNode, &collectFillCtx);
      if (code != TSDB_CODE_SUCCESS) break;
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    void* pIter = taosHashIterate(collectFillCtx.pPseudoCols, 0);
    while (pIter) {
      SNode *pNode = *(SNode**)pIter, *pNew = NULL;
      code = nodesCloneNode(pNode, &pNew);
      if (code == TSDB_CODE_SUCCESS) {
        code = nodesListMakeStrictAppend(&collectFillCtx.pNotFillExprs, pNew);
      }
      if (code == TSDB_CODE_SUCCESS) {
        pIter = taosHashIterate(collectFillCtx.pPseudoCols, pIter);
      } else {
        taosHashCancelIterate(collectFillCtx.pPseudoCols, pIter);
        break;
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      TSWAP(*pFillExprs, collectFillCtx.pFillExprs);
      TSWAP(*pNotFillExprs, collectFillCtx.pNotFillExprs);
      TSWAP(*pPossibleFillNullCols, collectFillCtx.pAggFuncCols);
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    if (collectFillCtx.pFillExprs) nodesDestroyList(collectFillCtx.pFillExprs);
    if (collectFillCtx.pNotFillExprs) nodesDestroyList(collectFillCtx.pNotFillExprs);
    if (collectFillCtx.pAggFuncCols) nodesDestroyList(collectFillCtx.pAggFuncCols);
  }
  taosHashCleanup(collectFillCtx.pPseudoCols);
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

  SFillLogicNode* pFill = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILL, (SNode**)&pFill);
  if (NULL == pFill) {
    return code;
  }

  pFill->node.groupAction = getGroupAction(pCxt, pSelect);
  pFill->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pFill->node.resultDataOrder = pFill->node.requireDataOrder;
  pFill->node.inputTsOrder = TSDB_ORDER_ASC;

  code = collectFillExprs(pSelect, &pFill->pFillExprs, &pFill->pNotFillExprs, &pFill->pFillNullExprs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pFill->pFillExprs, pSelect, SQL_CLAUSE_FILL, NULL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pFill->pNotFillExprs, pSelect, SQL_CLAUSE_FILL, NULL);
  }
  if (TSDB_CODE_SUCCESS == code && LIST_LENGTH(pFill->pFillNullExprs) > 0) {
    code = createColumnByRewriteExprs(pFill->pFillNullExprs, &pFill->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pFill->pFillExprs, &pFill->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pFill->pNotFillExprs, &pFill->node.pTargets);
  }

  pFill->mode = pFillNode->mode;
  pFill->timeRange = pFillNode->timeRange;
  TSWAP(pFill->pTimeRange, pFillNode->pTimeRange);
  pFill->pValues = NULL;
  code = nodesCloneNode(pFillNode->pValues, &pFill->pValues);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pFillNode->pWStartTs, &pFill->pWStartTs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesCloneNode(pFillNode->pSurroundingTime,
                          &pFill->pSurroundingTime);
  }

  if (TSDB_CODE_SUCCESS == code && 0 == LIST_LENGTH(pFill->node.pTargets)) {
    code = createColumnByRewriteExpr(pFill->pWStartTs, &pFill->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving) {
    code = nodesCloneNode(pSelect->pHaving, &pFill->node.pConditions);
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
  return isPrimaryKeyImpl(pExpr);
}

static int32_t createSortLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL == pSelect->pOrderByList || pSelect->pOrderByList->length == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSortLogicNode* pSort = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (NULL == pSort) {
    return code;
  }

  pSort->groupSort = pSelect->groupSort;
  pSort->node.groupAction = pSort->groupSort ? GROUP_ACTION_KEEP : GROUP_ACTION_CLEAR;
  pSort->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;

  pSort->node.resultDataOrder = isPrimaryKeySort(pSelect->pOrderByList)
                                    ? (pSort->groupSort ? DATA_ORDER_LEVEL_IN_GROUP : DATA_ORDER_LEVEL_GLOBAL)
                                    : DATA_ORDER_LEVEL_NONE;
  if (inStreamCalcClause(pCxt->pPlanCxt) && nodeType(pSelect->pFromTable) == QUERY_NODE_REAL_TABLE &&
      ((SRealTableNode*)pSelect->pFromTable)->placeholderType == SP_PARTITION_ROWS) {
    pSort->skipPKSortOpt = true;
  }

  code = nodesCollectColumns(pSelect, SQL_CLAUSE_ORDER_BY, NULL, COLLECT_COL_TYPE_ALL, &pSort->node.pTargets);
  if (TSDB_CODE_SUCCESS == code) {
    rewriteTargetsWithResId(pSort->node.pTargets);
  }
  if (TSDB_CODE_SUCCESS == code && NULL == pSort->node.pTargets) {
    SNode* pNew = NULL;
    code = nodesCloneNode(nodesListGetNode(pCxt->pCurrRoot->pTargets, 0), &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pSort->node.pTargets, pNew);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    pSort->pSortKeys = NULL;
    code = nodesCloneList(pSelect->pOrderByList, &pSort->pSortKeys);
    if (TSDB_CODE_SUCCESS == code) {
      SNode*            pNode = NULL;
      SOrderByExprNode* firstSortKey = (SOrderByExprNode*)nodesListGetNode(pSort->pSortKeys, 0);

      if (isPrimaryKeyImpl(firstSortKey->pExpr)) pSort->node.outputTsOrder = firstSortKey->order;
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
  SNodeList* pList = NULL;
  int32_t    code = nodesMakeList(&pList);
  if (NULL == pList) {
    return code;
  }

  SNode*  pNode;
  int32_t projIdx = 1;
  FOREACH(pNode, pExprs) {
    SColumnNode* pCol = createColumnByExpr(pStmtName, (SExprNode*)pNode);
    if (TSDB_CODE_SUCCESS != (code = nodesListStrictAppend(pList, (SNode*)pCol))) {
      nodesDestroyList(pList);
      return code;
    }
    pCol->resIdx = ((SExprNode*)pNode)->projIdx;
  }

  *pCols = pList;
  return TSDB_CODE_SUCCESS;
}

static int32_t createProjectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  SProjectLogicNode* pProject = NULL;
  int32_t            code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return code;
  }

  TSWAP(pProject->node.pLimit, pSelect->pLimit);
  TSWAP(pProject->node.pSlimit, pSelect->pSlimit);
  pProject->ignoreGroupId = pSelect->isSubquery ? true : (NULL == pSelect->pPartitionByList);
  pProject->node.groupAction = (hasExternalWindowDerivedFromSubquery(pSelect) ||
                                (inStreamCalcClause(pCxt->pPlanCxt) && pCxt->pPlanCxt->streamCxt.hasExtWindow))
                                   ? GROUP_ACTION_KEEP
                                   : GROUP_ACTION_CLEAR;
  pProject->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pProject->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  pProject->pProjections = NULL;
  code = nodesCloneList(pSelect->pProjectionList, &pProject->pProjections);
  tstrncpy(pProject->stmtName, pSelect->stmtName, TSDB_TABLE_NAME_LEN);

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

  SPartitionLogicNode* pPartition = NULL;
  int32_t              code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION, (SNode**)&pPartition));

  pPartition->node.groupAction = GROUP_ACTION_SET;
  pPartition->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pPartition->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, COLLECT_COL_TYPE_ALL, &pPartition->node.pTargets));

  if (NULL == pPartition->node.pTargets) {
    SNode* pNew = NULL;
    PLAN_ERR_JRET(nodesCloneNode(nodesListGetNode(pCxt->pCurrRoot->pTargets, 0), &pNew));
    PLAN_ERR_JRET(nodesListMakeStrictAppend(&pPartition->node.pTargets, pNew));
  }

  rewriteTargetsWithResId(pPartition->node.pTargets);

  PLAN_ERR_JRET(nodesCollectFuncs(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, fmIsAggFunc, &pPartition->pAggFuncs));

  pPartition->pPartitionKeys = NULL;
  PLAN_ERR_JRET(nodesCloneList(pSelect->pPartitionByList, &pPartition->pPartitionKeys));

  if (keysHasCol(pPartition->pPartitionKeys) && pSelect->pWindow &&
      nodeType(pSelect->pWindow) == QUERY_NODE_INTERVAL_WINDOW) {
    pPartition->needBlockOutputTsOrder = true;
    SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pSelect->pWindow;
    SColumnNode*         pTsCol = (SColumnNode*)pInterval->pCol;
    pPartition->pkTsColId = pTsCol->colId;
    pPartition->pkTsColTbId = pTsCol->tableId;
  }

  if (NULL != pSelect->pHaving && !pSelect->hasAggFuncs && NULL == pSelect->pGroupByList &&
      NULL == pSelect->pWindow) {
    pPartition->node.pConditions = NULL;
    PLAN_ERR_JRET(nodesCloneNode(pSelect->pHaving, &pPartition->node.pConditions));
  }

  *pLogicNode = (SLogicNode*)pPartition;
  return code;

_return:
  planError("%s failed ,code %d since %s", __func__, code, tstrerror(code));
  nodesDestroyNode((SNode*)pPartition);
  return code;
}

static int32_t createDistinctLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->isDistinct) {
    return TSDB_CODE_SUCCESS;
  }

  SAggLogicNode* pAgg = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  pAgg->node.groupAction = GROUP_ACTION_CLEAR;  // getDistinctGroupAction(pCxt, pSelect);
  pAgg->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pAgg->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  // set grouyp keys, agg funcs and having conditions
  SNodeList* pGroupKeys = NULL;
  SNode*     pProjection = NULL;
  FOREACH(pProjection, pSelect->pProjectionList) {
    SNode* pNew = NULL;
    code = createGroupingSetNode(pProjection, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pGroupKeys, pNew);
    }
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
  int32_t     code = TSDB_CODE_SUCCESS;

  PLAN_ERR_JRET(createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable, &pRoot));

  PLAN_ERR_JRET(checkExternalWindow(pCxt, pSelect));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createPartitionLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createExternalWindowLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createWindowLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createFillLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createAggLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createIndefRowsFuncLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createInterpFuncLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createForecastFuncLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createGenericAnalysisLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createDistinctLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createSortLogicNode, &pRoot));

  PLAN_ERR_JRET(createSelectRootLogicNode(pCxt, pSelect, createProjectLogicNode, &pRoot));

  *pLogicNode = pRoot;

  return code;
_return:
  planError("%s failed ,code %d since %s", __func__, code, tstrerror(code));
  nodesDestroyNode((SNode*)pRoot);
  return code;
}

static int32_t createSelectLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    oldContainsOuterJoin = pCxt->containsOuterJoin;
  pCxt->containsOuterJoin = pCxt->containsOuterJoin || pSelect->joinContains;

  if (NULL == pSelect->pFromTable) {
    code = createSelectWithoutFromLogicNode(pCxt, pSelect, pLogicNode);
  } else {
    code = createSelectFromLogicNode(pCxt, pSelect, pLogicNode);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != *pLogicNode) {
    (*pLogicNode)->stmtRoot = true;
    TSWAP((*pLogicNode)->pHint, pSelect->pHint);
  }

  pCxt->containsOuterJoin = oldContainsOuterJoin;
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

  SSortLogicNode* pSort = NULL;
  int32_t         code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_SORT, (SNode**)&pSort);
  if (NULL == pSort) {
    return code;
  }

  TSWAP(pSort->node.pLimit, pSetOperator->pLimit);

  pSort->node.pTargets = NULL;
  code = nodesCloneList(pSetOperator->pProjectionList, &pSort->node.pTargets);

  if (TSDB_CODE_SUCCESS == code) {
    pSort->pSortKeys = NULL;
    code = nodesCloneList(pSetOperator->pOrderByList, &pSort->pSortKeys);
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
  SProjectLogicNode* pProject = NULL;
  int32_t            code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PROJECT, (SNode**)&pProject);
  if (NULL == pProject) {
    return code;
  }

  if (NULL == pSetOperator->pOrderByList) {
    TSWAP(pProject->node.pLimit, pSetOperator->pLimit);
  }
  pProject->ignoreGroupId = true;
  pProject->isSetOpProj = true;

  pProject->pProjections = NULL;
  code = nodesCloneList(pSetOperator->pProjectionList, &pProject->pProjections);

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
  SAggLogicNode* pAgg = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  if (NULL == pSetOperator->pOrderByList) {
    TSWAP(pAgg->node.pSlimit, pSetOperator->pLimit);
  }

  pAgg->pGroupKeys = NULL;
  code = nodesCloneList(pSetOperator->pProjectionList, &pAgg->pGroupKeys);

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
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE:
    case QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT:
    case QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT:
      return TDMT_VND_CREATE_TABLE;
    case QUERY_NODE_DROP_TABLE_STMT:
    case QUERY_NODE_DROP_VIRTUAL_TABLE_STMT:
      return TDMT_VND_DROP_TABLE;
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT:
      return TDMT_VND_ALTER_TABLE;
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      return TDMT_VND_COMMIT;
    default:
      break;
  }
  return TDMT_VND_SUBMIT;
}

static int32_t createVnodeModifLogicNode(SLogicPlanContext* pCxt, SVnodeModifyOpStmt* pStmt, SLogicNode** pLogicNode) {
  SVnodeModifyLogicNode* pModif = NULL;
  int32_t                code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY, (SNode**)&pModif);
  if (NULL == pModif) {
    return code;
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
    pScan->scanRange = pDelete->timeRange;
    pScan->pScanCols = NULL;
    code = nodesCloneList(((SFunctionNode*)pDelete->pCountFunc)->pParameterList, &pScan->pScanCols);
  }

  STableMeta* pMeta = ((SRealTableNode*)pDelete->pFromTable)->pMeta;
  if (TSDB_CODE_SUCCESS == code && hasPkInTable(pMeta)) {
    code = addPrimaryKeyCol((SRealTableNode*)pDelete->pFromTable, &pScan->pScanCols);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pDelete->pTagCond) {
    code = nodesCloneNode(pDelete->pTagCond, &pScan->pTagCond);
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
  SAggLogicNode* pAgg = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_AGG, (SNode**)&pAgg);
  if (NULL == pAgg) {
    return code;
  }

  SNode* pNew = NULL;
  code = nodesCloneNode(pDelete->pCountFunc, &pNew);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeStrictAppend(&pAgg->pAggFuncs, pNew);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNew = NULL;
    code = nodesCloneNode(pDelete->pFirstFunc, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pAgg->pAggFuncs, pNew);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode* pNew = NULL;
    code = nodesCloneNode(pDelete->pLastFunc, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pAgg->pAggFuncs, pNew);
    }
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
  SVnodeModifyLogicNode* pModify = NULL;
  int32_t                code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY, (SNode**)&pModify);
  if (NULL == pModify) {
    return code;
  }

  SRealTableNode* pRealTable = (SRealTableNode*)pDelete->pFromTable;

  pModify->modifyType = MODIFY_TABLE_TYPE_DELETE;
  pModify->tableId = pRealTable->pMeta->uid;
  pModify->tableType = pRealTable->pMeta->tableType;
  snprintf(pModify->tableName, sizeof(pModify->tableName), "%s", pRealTable->table.tableName);
  tstrncpy(pModify->tsColName, pRealTable->pMeta->schema->name, TSDB_COL_NAME_LEN);
  pModify->deleteTimeRange = pDelete->timeRange;
  // merge: statement-level SECURE_DELETE keyword OR super table/db secureDelete metadata
  pModify->secureDelete = pDelete->secureDelete | pRealTable->pMeta->secureDelete;
  pModify->pAffectedRows = NULL;
  code = nodesCloneNode(pDelete->pCountFunc, &pModify->pAffectedRows);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pModify);
    return code;
  }
  code = nodesCloneNode(pDelete->pFirstFunc, &pModify->pStartTs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pModify);
    return code;
  }
  code = nodesCloneNode(pDelete->pLastFunc, &pModify->pEndTs);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode((SNode*)pModify);
    return code;
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
  SVnodeModifyLogicNode* pModify = NULL;
  int32_t                code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY, (SNode**)&pModify);
  if (NULL == pModify) {
    return code;
  }

  SRealTableNode* pRealTable = (SRealTableNode*)pInsert->pTable;

  pModify->modifyType = MODIFY_TABLE_TYPE_INSERT;
  pModify->tableId = pRealTable->pMeta->uid;
  pModify->stableId = pRealTable->pMeta->suid;
  pModify->tableType = pRealTable->pMeta->tableType;
  snprintf(pModify->tableName, sizeof(pModify->tableName), "%s", pRealTable->table.tableName);
  TSWAP(pModify->pVgroupList, pRealTable->pVgroupList);
  pModify->pInsertCols = NULL;
  code = nodesCloneList(pInsert->pCols, &pModify->pInsertCols);
  if (NULL == pModify->pInsertCols) {
    nodesDestroyNode((SNode*)pModify);
    return code;
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

static void setLogicSubplanType(SPlanContext* pCxt, SLogicSubplan* pSubplan) {
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY != nodeType(pSubplan->pNode)) {
    if (pCxt->hasScan) {
      pSubplan->subplanType = (IS_HSYS_SCAN(pCxt->sysScanFlag))? SUBPLAN_TYPE_HSYSSCAN:SUBPLAN_TYPE_SCAN;
    } else {
      pSubplan->subplanType = SUBPLAN_TYPE_MERGE; // todo: sys-merge????
    }
  } else {
    SVnodeModifyLogicNode* pModify = (SVnodeModifyLogicNode*)pSubplan->pNode;
    pSubplan->subplanType = (MODIFY_TABLE_TYPE_INSERT == pModify->modifyType && NULL != pModify->node.pChildren)
                                ? SUBPLAN_TYPE_SCAN
                                : SUBPLAN_TYPE_MODIFY;
  }
}

int32_t createLogicPlan(SPlanContext* pCxt, SLogicSubplan** pLogicSubplan) {
  SLogicPlanContext cxt = {.pPlanCxt = pCxt, .pCurrRoot = NULL};

  SLogicSubplan* pSubplan = NULL;
  int32_t        code = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, (SNode**)&pSubplan);
  if (NULL == pSubplan) {
    return code;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = ++pCxt->groupId;
  pSubplan->id.subplanId = 1;

  code = createQueryLogicNode(&cxt, pCxt->pAstRoot, &pSubplan->pNode);
  if (TSDB_CODE_SUCCESS == code) {
    setLogicNodeParent(pSubplan->pNode);
    setLogicSubplanType(pCxt, pSubplan);
    code = adjustLogicNodeDataRequirement(pSubplan->pNode, DATA_ORDER_LEVEL_NONE);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode((SNode*)pSubplan);
  }

  return code;
}
