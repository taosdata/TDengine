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
#include "systable.h"
#include "tglobal.h"

// primary key column always the second column if exists
#define PRIMARY_COLUMN_SLOT 1

typedef struct SLogicPlanContext {
  SPlanContext* pPlanCxt;
  SLogicNode*   pCurrRoot;
  SSHashObj*    pChildTables;
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
      pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pCol->colType = COLUMN_TYPE_WINDOW_START;
      if (!isPartitionBy) {
        pCol->isPrimTs = true;
      }
      break;
    case FUNCTION_TYPE_WEND:
      pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pCol->colType = COLUMN_TYPE_WINDOW_END;
      if (!isPartitionBy) {
        pCol->isPrimTs = true;
      }
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
        rewriteExprAliasName((SExprNode*)pNode, (int64_t)pNode);
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
  bool            isPartitionBy = (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? true : false;
  SRewriteExprCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS, .pExprs = NULL, .pOutputs = NULL, .isPartitionBy = isPartitionBy};
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
  bool            isPartitionBy = (pSelect->pPartitionByList && pSelect->pPartitionByList->length > 0) ? true : false;
  SRewriteExprCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS, .pExprs = pExprs, .pOutputs = NULL, .isPartitionBy = isPartitionBy};
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
  for (int32_t i = 1; i <= 8; i++) {
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
                               &pCxt->pPlanCxt->streamTriggerScanList);
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

static int32_t findRefTableNode(SNodeList *refTableList, const char *dbName, const char *tableName, SNode **pRefTable) {
  SNode *pRef = NULL;
  FOREACH(pRef, refTableList) {
    if (0 == strcasecmp(((SRealTableNode*)pRef)->table.tableName, tableName) &&
        0 == strcasecmp(((SRealTableNode*)pRef)->table.dbName, dbName)) {
      PLAN_RET(nodesCloneNode(pRef, pRefTable));
    }
  }
  return TSDB_CODE_NOT_FOUND;
}

static int32_t findRefColId(SNode *pRefTable, const char *colName, col_id_t *colId, int32_t *colIdx) {
  SRealTableNode *pRealTable = (SRealTableNode*)pRefTable;
  for (int32_t i = 0; i < pRealTable->pMeta->tableInfo.numOfColumns; ++i) {
    if (0 == strcasecmp(pRealTable->pMeta->schema[i].name, colName)) {
      *colId = pRealTable->pMeta->schema[i].colId;
      *colIdx = i;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_NOT_FOUND;
}

static int32_t scanAddCol(SLogicNode* pLogicNode, SColRef* colRef, STableNode* pVirtualTableNode, const SSchema* pSchema, col_id_t colId) {
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

  // eliminate duplicate scan cols.
  SNode *pCol = NULL;
  FOREACH(pCol, pLogicScan->pScanCols) {
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
  pRefTableScanCol->node.resType.bytes = pSchema->bytes;
  pRefTableScanCol->colType = COLUMN_TYPE_COLUMN;
  pRefTableScanCol->isPk = false;
  pRefTableScanCol->tableHasPk = false;
  pRefTableScanCol->numOfPKs = 0;
  pRefTableScanCol->hasRef = false;
  pRefTableScanCol->hasDep = true;

  PLAN_ERR_JRET(nodesListMakeAppend(&pLogicScan->pScanCols, (SNode*)pRefTableScanCol));
  return code;
_return:
  nodesDestroyNode((SNode*)pRefTableScanCol);
  return code;
}

static int32_t checkColRefType(const SSchema* vtbSchema, const SSchema* refSchema) {
  if (vtbSchema->type != refSchema->type || vtbSchema->bytes != refSchema->bytes) {
    qError("virtual table column:%s type mismatch, virtual table column type:%d, bytes:%d, "
        "ref table column:%s, type:%d, bytes:%d",
        vtbSchema->name, vtbSchema->type, vtbSchema->bytes, refSchema->name, refSchema->type, refSchema->bytes);
    return TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addSubScanNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SVirtualTableNode* pVirtualTable,
                              int32_t colRefIndex, int32_t schemaIndex, SHashObj *refTablesMap) {
  int32_t     code = TSDB_CODE_SUCCESS;
  col_id_t    colId = 0;
  int32_t     colIdx = 0;
  SColRef    *pColRef = &pVirtualTable->pMeta->colRef[colRefIndex];
  SNode      *pRefTable = NULL;
  SLogicNode *pRefScan = NULL;
  bool        put = false;

  PLAN_ERR_JRET(findRefTableNode(pVirtualTable->refTables, pColRef->refDbName, pColRef->refTableName, &pRefTable));
  PLAN_ERR_JRET(findRefColId(pRefTable, pColRef->refColName, &colId, &colIdx));

  char tableNameKey[TSDB_TABLE_FNAME_LEN] = {0};
  strcat(tableNameKey, pColRef->refDbName);
  strcat(tableNameKey, ".");
  strcat(tableNameKey, pColRef->refTableName);

  SLogicNode **ppRefScan = (SLogicNode **)taosHashGet(refTablesMap, &tableNameKey, strlen(tableNameKey));
  if (NULL == ppRefScan) {
    PLAN_ERR_JRET(createRefScanLogicNode(pCxt, pSelect, (SRealTableNode*)pRefTable, &pRefScan));
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], &((SRealTableNode*)pRefTable)->pMeta->schema[colIdx]));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId));
    PLAN_ERR_JRET(taosHashPut(refTablesMap, &tableNameKey, strlen(tableNameKey), &pRefScan, POINTER_BYTES));
    put = true;
  } else {
    pRefScan = *ppRefScan;
    PLAN_ERR_JRET(checkColRefType(&pVirtualTable->pMeta->schema[schemaIndex], &((SRealTableNode*)pRefTable)->pMeta->schema[colIdx]));
    PLAN_ERR_JRET(scanAddCol(pRefScan, pColRef, &pVirtualTable->table, &pVirtualTable->pMeta->schema[schemaIndex], colId));
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
    strcat(key, pCol->refDbName);
    strcat(key, ".");
    strcat(key, pCol->refTableName);
    strcat(key, ".");
    strcat(key, pCol->refColName);
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
  bool                    useTagScan = false;

  // Virtual table scan node -> Real table scan node
  PLAN_ERR_JRET(createScanLogicNode(pCxt, pSelect, (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 0), &pRealTableScan));

  if (LIST_LENGTH(pVtableScan->pScanCols) == 0 && LIST_LENGTH(pVtableScan->pScanPseudoCols) == 0) {
    scanAllCols = false;
  }
  if (((SScanLogicNode*)pRealTableScan)->scanType == SCAN_TYPE_TAG) {
    useTagScan = true;
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
        PLAN_ERR_JRET(scanAddCol(pRealTableScan, NULL, &pVirtualTable->table, &pVirtualTable->pMeta->schema[i], pVirtualTable->pMeta->schema[i].colId));
      }
    }
    PLAN_ERR_JRET(createColumnByRewriteExprs(((SScanLogicNode*)pRealTableScan)->pScanCols, &((SScanLogicNode*)pRealTableScan)->node.pTargets));
  }

  if (pVtableScan->pScanPseudoCols) {
    PLAN_ERR_JRET(createTagScanLogicNode(pCxt, pSelect, pVtableScan, (SLogicNode**)&pTagScan));
  }

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

  // Virtual child table uid and ref col scan on ins_columns
  // TODO(smj) : create a fake logic node, no need to collect column
  PLAN_ERR_JRET(createScanLogicNode(pCxt, pSelect, (SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 1), &pInsColumnsScan));
  nodesDestroyList(((SScanLogicNode*)pInsColumnsScan)->node.pTargets);
  PLAN_ERR_JRET(addInsColumnScanCol((SRealTableNode*)nodesListGetNode(pVirtualTable->refTables, 1), &((SScanLogicNode*)pInsColumnsScan)->pScanCols));
  PLAN_ERR_JRET(createColumnByRewriteExprs(((SScanLogicNode*)pInsColumnsScan)->pScanCols, &((SScanLogicNode*)pInsColumnsScan)->node.pTargets));
  ((SScanLogicNode *)pInsColumnsScan)->virtualStableScan = true;
  ((SScanLogicNode *)pInsColumnsScan)->stableId = pVtableScan->stableId;

  // Dynamic query control node -> Virtual table scan node -> Real table scan node
  PLAN_ERR_JRET(nodesMakeNode(QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL, (SNode**)&pDynCtrl));
  pDynCtrl->qType = DYN_QTYPE_VTB_SCAN;
  pDynCtrl->vtbScan.scanAllCols = pVtableScan->scanAllCols;
  pDynCtrl->vtbScan.useTagScan = useTagScan;
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

  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  nodesDestroyNode((SNode*)pRealTableScan);
  nodesDestroyNode((SNode*)pDynCtrl);
  return code;
}

static int32_t createVirtualNormalChildTableLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect,
                                                      SVirtualTableNode* pVirtualTable, SVirtualScanLogicNode* pVtableScan,
                                                      SLogicNode** pLogicNode) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SNode*    pNode = NULL;
  bool      scanAllCols = true;
  SHashObj* pRefTablesMap = NULL;
  SNode*    pTagScan = NULL;

  pRefTablesMap = taosHashInit(LIST_LENGTH(pVtableScan->pScanCols), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (NULL == pRefTablesMap) {
    PLAN_ERR_JRET(terrno);
  }

  if (pCxt->pPlanCxt->streamCalcQuery && pSelect->pTimeRange != NULL) {
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
        PLAN_ERR_JRET(TSDB_CODE_VTABLE_PRIMTS_HAS_REF);
      }
      scanAllCols &= false;
      PLAN_ERR_JRET(addSubScanNode(pCxt, pSelect, pVirtualTable, colRefIndex, schemaIndex, pRefTablesMap));
    } else if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      // do nothing
    } else {
      scanAllCols &= false;
    }
  }

  if (pVtableScan->pScanPseudoCols) {
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
          PLAN_ERR_JRET(addSubScanNode(pCxt, pSelect, pVirtualTable, colRefIndex, i, pRefTablesMap));
        }
      }
    }
  }

  // Iterate the table map, build scan logic node for each origin table and add these node to vtable scan's child list.
  void* pIter = NULL;
  while ((pIter = taosHashIterate(pRefTablesMap, pIter))) {
    SScanLogicNode **pRefScanNode = (SScanLogicNode**)pIter;
    PLAN_ERR_JRET(createColumnByRewriteExprs((*pRefScanNode)->pScanCols, &(*pRefScanNode)->node.pTargets));
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, (SNode*)(*pRefScanNode)));
  }

  if (pTagScan) {
    PLAN_ERR_JRET(nodesListStrictAppend(pVtableScan->node.pChildren, pTagScan));
  }

  // set output
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanCols, &pVtableScan->node.pTargets));
  PLAN_ERR_JRET(createColumnByRewriteExprs(pVtableScan->pScanPseudoCols, &pVtableScan->node.pTargets));

  *pLogicNode = (SLogicNode*)pVtableScan;
  taosHashCleanup(pRefTablesMap);
  return code;
_return:
  planError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  taosHashSetFreeFp(pRefTablesMap, destroyScanLogicNode);
  taosHashCleanup(pRefTablesMap);
  return code;
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

  PLAN_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, pVirtualTable->table.tableAlias, COLLECT_COL_TYPE_TAG,
                                    &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(nodesCollectFuncs(pSelect, SQL_CLAUSE_FROM, pVirtualTable->table.tableAlias, fmIsScanPseudoColumnFunc,
                                  &pVtableScan->pScanPseudoCols));

  PLAN_ERR_JRET(rewriteExprsForSelect(pVtableScan->pScanPseudoCols, pSelect, SQL_CLAUSE_FROM, NULL));

  switch (pVtableScan->tableType) {
    case TSDB_SUPER_TABLE:
      PLAN_ERR_JRET(createVirtualSuperTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      break;
    case TSDB_VIRTUAL_NORMAL_TABLE:
    case TSDB_VIRTUAL_CHILD_TABLE: {
      if (pCxt->pPlanCxt->streamCalcQuery) {
        PLAN_ERR_JRET(createVirtualSuperTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      } else {
        PLAN_ERR_JRET(createVirtualNormalChildTableLogicNode(pCxt, pSelect, pVirtualTable, pVtableScan, pLogicNode));
      }
      break;
    }
    default:
      PLAN_ERR_JRET(TSDB_CODE_PLAN_INVALID_TABLE_TYPE);
  }
  pCxt->pPlanCxt->streamVtableCalc = true;

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

static SColumnNode* createColumnByExpr(const char* pStmtName, SExprNode* pExpr) {
  SColumnNode* pCol = NULL;
  terrno = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = pExpr->resType;
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", pExpr->aliasName);
  if (NULL != pStmtName) {
    snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", pStmtName);
  }
  pCol->node.relatedTo = pExpr->relatedTo;
  return pCol;
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
  int32_t               code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_INTERP_FUNC, (SNode**)&pInterpFunc);
  if (NULL == pInterpFunc) {
    return code;
  }

  pInterpFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pInterpFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pInterpFunc->node.resultDataOrder = pInterpFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isInterpFunc, &pInterpFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pInterpFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pFill) {
    SFillNode* pFill = (SFillNode*)pSelect->pFill;
    pInterpFunc->timeRange = pFill->timeRange;
    pInterpFunc->fillMode = pFill->mode;
    pInterpFunc->pTimeSeries = NULL;
    code = nodesCloneNode(pFill->pWStartTs, &pInterpFunc->pTimeSeries);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesCloneNode(pFill->pValues, &pInterpFunc->pFillValues);
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pEvery) {
    pInterpFunc->interval = ((SValueNode*)pSelect->pEvery)->datum.i;
    pInterpFunc->intervalUnit = ((SValueNode*)pSelect->pEvery)->unit;
    pInterpFunc->precision = pSelect->precision;
  }

  if (TSDB_CODE_SUCCESS == code && pSelect->pRangeAround) {
    SNode* pRangeInterval = ((SRangeAroundNode*)pSelect->pRangeAround)->pInterval;
    if (!pRangeInterval || nodeType(pRangeInterval) != QUERY_NODE_VALUE) {
      code = TSDB_CODE_PAR_INTERNAL_ERROR;
    } else {
      pInterpFunc->rangeInterval = ((SValueNode*)pRangeInterval)->datum.i;
      pInterpFunc->rangeIntervalUnit = ((SValueNode*)pRangeInterval)->unit;
    }
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

static bool isImputationFunc(int32_t funcId) {
  return fmIsImputationFunc(funcId) || fmIsGroupKeyFunc(funcId) || fmisSelectGroupConstValueFunc(funcId) ||
         fmIsAnalysisPseudoColumnFunc(funcId);
}

static int32_t createImputationFuncLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (!pSelect->hasImputationFunc) {
    return TSDB_CODE_SUCCESS;
  }

  SImputationFuncLogicNode * pImputatFunc = NULL;
  int32_t                 code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_IMPUTATION_FUNC, (SNode**)&pImputatFunc);
  if (NULL == pImputatFunc) {
    return code;
  }

  pImputatFunc->node.groupAction = getGroupAction(pCxt, pSelect);
  pImputatFunc->node.requireDataOrder = getRequireDataOrder(true, pSelect);
  pImputatFunc->node.resultDataOrder = pImputatFunc->node.requireDataOrder;

  // interp functions and _group_key functions
  code = nodesCollectFuncs(pSelect, SQL_CLAUSE_SELECT, NULL, isImputationFunc, &pImputatFunc->pFuncs);
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteExprsForSelect(pImputatFunc->pFuncs, pSelect, SQL_CLAUSE_SELECT, NULL);
  }

  // set the output
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnByRewriteExprs(pImputatFunc->pFuncs, &pImputatFunc->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pLogicNode = (SLogicNode*)pImputatFunc;
  } else {
    nodesDestroyNode((SNode*)pImputatFunc);
  }

  return code;
}

static int32_t createWindowLogicNodeFinalize(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SWindowLogicNode* pWindow,
                                             SLogicNode** pLogicNode) {
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
  // no agg func
  if (!pSelect->hasAggFuncs) {
    if (pSelect->hasIndefiniteRowsFunc) {
      pWindow->node.requireDataOrder = getRequireDataOrder(pSelect->hasTimeLineFunc, pSelect);
      pWindow->node.resultDataOrder = pWindow->node.requireDataOrder;
      nodesDestroyList(pWindow->pFuncs);
      pWindow->pFuncs = NULL;
      PLAN_ERR_RET(nodesCollectFuncs(pSelect, SQL_CLAUSE_WINDOW, NULL, fmIsStreamVectorFunc, &pWindow->pFuncs));
      PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW, NULL));
      PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets));
      pWindow->indefRowsFunc = true;
      pSelect->hasIndefiniteRowsFunc = false;
    } else {
      pWindow->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
      pWindow->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;
      PLAN_ERR_RET(nodesCloneList(pSelect->pProjectionList, &pWindow->pProjs));
      PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pProjs, pSelect, SQL_CLAUSE_WINDOW, NULL));
      PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pProjs, &pWindow->node.pTargets));
    }
  } else {
    // has agg func, collect again with placeholder func
    pWindow->node.requireDataOrder = getRequireDataOrder(pSelect->hasTimeLineFunc, pSelect);
    pWindow->node.resultDataOrder = pSelect->onlyHasKeepOrderFunc ? pWindow->node.requireDataOrder : DATA_ORDER_LEVEL_NONE;
    nodesDestroyList(pWindow->pFuncs);
    pWindow->pFuncs = NULL;
    PLAN_ERR_RET(nodesCollectFuncs(pSelect, SQL_CLAUSE_WINDOW, NULL, fmIsStreamWindowClauseFunc, &pWindow->pFuncs));
    PLAN_ERR_RET(rewriteExprsForSelect(pWindow->pFuncs, pSelect, SQL_CLAUSE_WINDOW, NULL));
    PLAN_ERR_RET(createColumnByRewriteExprs(pWindow->pFuncs, &pWindow->node.pTargets));
    pSelect->hasAggFuncs = false;
  }

  pWindow->inputHasOrder = (pWindow->isSingleTable || pWindow->node.requireDataOrder == DATA_ORDER_LEVEL_GLOBAL);

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
    pWindow->trueForLimit = ((SValueNode*)pState->pTrueForLimit)->datum.i;
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
  pWindow->node.requireDataOrder = (pSelect->hasTimeLineFunc ? getRequireDataOrder(true, pSelect) : DATA_ORDER_LEVEL_IN_BLOCK);
  pWindow->node.resultDataOrder = getRequireDataOrder(true, pSelect);
  pWindow->pTspk = NULL;
  code = nodesCloneNode(pInterval->pCol, &pWindow->pTspk);
  if (NULL == pWindow->pTspk) {
    nodesDestroyNode((SNode*)pWindow);
    return code;
  }
  pWindow->isPartTb = pSelect->pPartitionByList ? keysHasTbname(pSelect->pPartitionByList) : 0;
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
    pWindow->trueForLimit = ((SValueNode*)pEvent->pTrueForLimit)->datum.i;
  }
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
  code = nodesCloneNode(pAnomaly->pExpr, &pWindow->pAnomalyExpr);
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
  code = rewriteExprForSelect(pWindow->pAnomalyExpr, pSelect, SQL_CLAUSE_WINDOW);
  if (TSDB_CODE_SUCCESS == code) {
    code = createWindowLogicNodeFinalize(pCxt, pSelect, pWindow, pLogicNode);
  } else {
    nodesDestroyNode((SNode*)pWindow);
  }

  return code;
}


static int32_t createWindowLogicNodeByExternal(SLogicPlanContext* pCxt, SExternalWindowNode* pExternal,
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
  pWindow->isPartTb = 0;
  pWindow->pTspk = NULL;
  if (nodeType(pSelect->pFromTable) == QUERY_NODE_REAL_TABLE) {
    SRealTableNode* pTable = (SRealTableNode*)pSelect->pFromTable;
    if (pTable->pMeta->tableType == TSDB_NORMAL_TABLE || pTable->pMeta->tableType == TSDB_CHILD_TABLE) {
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

  return TSDB_CODE_FAILED;
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
      pFunc->funcType == FUNCTION_TYPE_TLOCALTIME) {
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

static int32_t createExternalWindowLogicNode(SLogicPlanContext* pCxt, SSelectStmt* pSelect, SLogicNode** pLogicNode) {
  if (NULL != pSelect->pWindow || NULL != pSelect->pPartitionByList || NULL != pSelect->pGroupByList ||
      !pCxt->pPlanCxt->streamCalcQuery ||
      nodeType(pSelect->pFromTable) == QUERY_NODE_TEMP_TABLE ||
      nodeType(pSelect->pFromTable) == QUERY_NODE_JOIN_TABLE ||
      NULL != pSelect->pSlimit || NULL != pSelect->pLimit ||
      pSelect->hasUniqueFunc || pSelect->hasTailFunc || pSelect->hasForecastFunc ||
      !timeRangeSatisfyExternalWindow((STimeRangeNode*)pSelect->pTimeRange)) {
    pCxt->pPlanCxt->withExtWindow = false;
    return TSDB_CODE_SUCCESS;
  }

  bool hasPlaceHolderCond = false;
  PAR_ERR_RET(conditionHasPlaceHolder(pSelect->pWhere, &hasPlaceHolderCond));
  if (hasPlaceHolderCond) {
    pCxt->pPlanCxt->withExtWindow = false;
    return TSDB_CODE_SUCCESS;
  }

  PLAN_ERR_RET(nodesMakeNode(QUERY_NODE_EXTERNAL_WINDOW, &pSelect->pWindow));
  pCxt->pPlanCxt->withExtWindow = true;
  PLAN_RET(createWindowLogicNodeByExternal(pCxt, (SExternalWindowNode*)pSelect->pWindow, pSelect, pLogicNode));
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
  if (NULL == pSelect->pOrderByList) {
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
  if (pCxt->pPlanCxt->streamCalcQuery &&
      nodeType(pSelect->pFromTable) == QUERY_NODE_REAL_TABLE &&
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
  pProject->node.groupAction =
      (pCxt->pPlanCxt->streamCalcQuery && pCxt->pPlanCxt->withExtWindow) ? GROUP_ACTION_KEEP : GROUP_ACTION_CLEAR;
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
  int32_t              code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_PARTITION, (SNode**)&pPartition);
  if (NULL == pPartition) {
    return code;
  }

  pPartition->node.groupAction = GROUP_ACTION_SET;
  pPartition->node.requireDataOrder = DATA_ORDER_LEVEL_NONE;
  pPartition->node.resultDataOrder = DATA_ORDER_LEVEL_NONE;

  code = nodesCollectColumns(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, COLLECT_COL_TYPE_ALL, &pPartition->node.pTargets);
  if (TSDB_CODE_SUCCESS == code && NULL == pPartition->node.pTargets) {
    SNode* pNew = NULL;
    code = nodesCloneNode(nodesListGetNode(pCxt->pCurrRoot->pTargets, 0), &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListMakeStrictAppend(&pPartition->node.pTargets, pNew);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    rewriteTargetsWithResId(pPartition->node.pTargets);
  }

  if (TSDB_CODE_SUCCESS == code) {
    //    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_GROUP_BY, NULL, fmIsAggFunc, &pPartition->pAggFuncs);
    code = nodesCollectFuncs(pSelect, SQL_CLAUSE_PARTITION_BY, NULL, fmIsAggFunc, &pPartition->pAggFuncs);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pPartition->pPartitionKeys = NULL;
    code = nodesCloneList(pSelect->pPartitionByList, &pPartition->pPartitionKeys);
  }

  if (keysHasCol(pPartition->pPartitionKeys) && pSelect->pWindow &&
      nodeType(pSelect->pWindow) == QUERY_NODE_INTERVAL_WINDOW) {
    pPartition->needBlockOutputTsOrder = true;
    SIntervalWindowNode* pInterval = (SIntervalWindowNode*)pSelect->pWindow;
    SColumnNode*         pTsCol = (SColumnNode*)pInterval->pCol;
    pPartition->pkTsColId = pTsCol->colId;
    pPartition->pkTsColTbId = pTsCol->tableId;
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pSelect->pHaving && !pSelect->hasAggFuncs && NULL == pSelect->pGroupByList &&
      NULL == pSelect->pWindow) {
    pPartition->node.pConditions = NULL;
    code = nodesCloneNode(pSelect->pHaving, &pPartition->node.pConditions);
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
  int32_t     code = createLogicNodeByTable(pCxt, pSelect, pSelect->pFromTable, &pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createExternalWindowLogicNode, &pRoot);
  }
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
    code = createSelectRootLogicNode(pCxt, pSelect, createForecastFuncLogicNode, &pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createSelectRootLogicNode(pCxt, pSelect, createImputationFuncLogicNode, &pRoot);
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
  pSubplan->id.groupId = 1;
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
