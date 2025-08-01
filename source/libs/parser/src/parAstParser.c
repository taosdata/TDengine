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

#include "functionMgt.h"
#include "os.h"
#include "parAst.h"
#include "parInt.h"
#include "parToken.h"
#include "systable.h"
#include "tglobal.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* ParseAlloc(FMalloc);
extern void  Parse(void*, int, SToken, void*);
extern void  ParseFree(void*, FFree);
extern void  ParseTrace(FILE*, char*);

int32_t buildQueryAfterParse(SQuery** pQuery, SNode* pRootNode, int16_t placeholderNo, SArray** pPlaceholderValues) {
  *pQuery = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)pQuery);
  if (NULL == *pQuery) {
    return code;
  }
  (*pQuery)->pRoot = pRootNode;
  (*pQuery)->placeholderNum = placeholderNo;
  TSWAP((*pQuery)->pPlaceholderValues, *pPlaceholderValues);
  (*pQuery)->execStage = QUERY_EXEC_STAGE_ANALYSE;

  return TSDB_CODE_SUCCESS;
}

int32_t parse(SParseContext* pParseCxt, SQuery** pQuery) {
  SAstCreateContext cxt;
  initAstCreateContext(pParseCxt, &cxt);
  void* pParser = ParseAlloc((FMalloc)taosMemMalloc);
  if (!pParser) return terrno;
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      Parse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    if (!pParseCxt->hasDupQuoteChar) {
      char dupQuoteChar = 0;
      t0.n = tGetToken((char*)&cxt.pQueryCxt->pSql[i], &t0.type, &dupQuoteChar);
      if (dupQuoteChar) pParseCxt->hasDupQuoteChar = true;
    } else {
      t0.n = tGetToken((char*)&cxt.pQueryCxt->pSql[i], &t0.type, NULL);
    }
    t0.z = (char*)(cxt.pQueryCxt->pSql + i);
    i += t0.n;

    switch (t0.type) {
      case TK_NK_SPACE:
      case TK_NK_COMMENT: {
        break;
      }
      case TK_NK_SEMI: {
        Parse(pParser, 0, t0, &cxt);
        goto abort_parse;
      }
      case TK_NK_ILLEGAL: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        cxt.errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
        goto abort_parse;
      }
      case TK_NK_OCT: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        cxt.errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
        goto abort_parse;
      }
      default:
        // ParseTrace(stdout, "");
        Parse(pParser, t0.type, t0, &cxt);
        if (TSDB_CODE_SUCCESS != cxt.errCode) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  ParseFree(pParser, (FFree)taosAutoMemoryFree);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    int32_t code = buildQueryAfterParse(pQuery, cxt.pRootNode, cxt.placeholderNo, &cxt.pPlaceholderValues);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  taosArrayDestroy(cxt.pPlaceholderValues);
  return cxt.errCode;
}

typedef struct SCollectMetaKeyCxt {
  SParseContext*   pParseCxt;
  SParseMetaCache* pMetaCache;
  SNode*           pStmt;
  bool             collectVStbRefDbs;
} SCollectMetaKeyCxt;

typedef struct SCollectMetaKeyFromExprCxt {
  SCollectMetaKeyCxt* pComCxt;
  bool                hasLastRowOrLast;
  bool                tbnameCollect;
  int32_t             errCode;
} SCollectMetaKeyFromExprCxt;

static int32_t collectMetaKeyFromQuery(SCollectMetaKeyCxt* pCxt, SNode* pStmt);

static EDealRes collectMetaKeyFromFunction(SCollectMetaKeyFromExprCxt* pCxt, SFunctionNode* pFunc) {
  switch (fmGetFuncType(pFunc->functionName)) {
    case FUNCTION_TYPE_LAST_ROW:
    case FUNCTION_TYPE_LAST:
      pCxt->hasLastRowOrLast = true;
      break;
    case FUNCTION_TYPE_UDF:
      pCxt->errCode = reserveUdfInCache(pFunc->functionName, pCxt->pComCxt->pMetaCache);
      break;
    default:
      break;
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static bool needGetTableIndex(SNode* pStmt) {
  return false;
  if (QUERY_SMA_OPTIMIZE_ENABLE == tsQuerySmaOptimize && QUERY_NODE_SELECT_STMT == nodeType(pStmt) && !QUERY_SMA_OPTIMIZE_NOT_SUPPORT) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt;
    return (NULL != pSelect->pWindow && QUERY_NODE_INTERVAL_WINDOW == nodeType(pSelect->pWindow));
  }
  return false;
}

static int32_t collectMetaKeyFromInsTagsImpl(SCollectMetaKeyCxt* pCxt, SName* pName) {
  if (0 == pName->type) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_DB_NAME_T == pName->type) {
    return reserveDbVgInfoInCache(pName->acctId, pName->dbname, pCxt->pMetaCache);
  }
  return reserveTableVgroupInCacheExt(pName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromInsTags(SCollectMetaKeyCxt* pCxt) {
  SSelectStmt* pSelect = (SSelectStmt*)pCxt->pStmt;
  SName        name = {0};
  int32_t      code = getVnodeSysTableTargetName(pCxt->pParseCxt->acctId, pSelect->pWhere, &name);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyFromInsTagsImpl(pCxt, &name);
  }
  return code;
}

static int32_t collectMetaKeyFromRealTableImpl(SCollectMetaKeyCxt* pCxt, const char* pDb, const char* pTable,
                                               AUTH_TYPE authType) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pDb, pTable, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pDb, pTable, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pDb, pTable, authType,
                                  pCxt->pMetaCache);
  }
#ifdef TD_ENTERPRISE
  if (TSDB_CODE_SUCCESS == code && NULL != pCxt->pParseCxt->pEffectiveUser) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pEffectiveUser, pDb, pTable, authType,
                                  pCxt->pMetaCache);
  }
#endif
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pDb, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code && needGetTableIndex(pCxt->pStmt)) {
    code = reserveTableIndexInCache(pCxt->pParseCxt->acctId, pDb, pTable, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code && (0 == strcmp(pTable, TSDB_INS_TABLE_DNODE_VARIABLES))) {
    code = reserveDnodeRequiredInCache(pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code &&
      (0 == strcmp(pTable, TSDB_INS_TABLE_TAGS) || 0 == strcmp(pTable, TSDB_INS_TABLE_TABLES) ||
       0 == strcmp(pTable, TSDB_INS_TABLE_COLS) || 0 == strcmp(pTable, TSDB_INS_TABLE_VC_COLS) ||
       0 == strcmp(pTable, TSDB_INS_DISK_USAGE) || 0 == strcmp(pTable, TSDB_INS_TABLE_FILESETS)) &&
      QUERY_NODE_SELECT_STMT == nodeType(pCxt->pStmt)) {
    code = collectMetaKeyFromInsTags(pCxt);
  }
  if (TSDB_CODE_SUCCESS == code && QUERY_SMA_OPTIMIZE_ENABLE == tsQuerySmaOptimize &&
      QUERY_NODE_SELECT_STMT == nodeType(pCxt->pStmt) && !QUERY_SMA_OPTIMIZE_NOT_SUPPORT) {
    code = reserveTableTSMAInfoInCache(pCxt->pParseCxt->acctId, pDb, pTable, pCxt->pMetaCache);
  }
  return code;
}

static EDealRes collectMetaKeyFromRealTable(SCollectMetaKeyFromExprCxt* pCxt, SRealTableNode* pRealTable) {
  pCxt->errCode = collectMetaKeyFromRealTableImpl(pCxt->pComCxt, pRealTable->table.dbName, pRealTable->table.tableName,
                                                  AUTH_TYPE_READ);
  if (TSDB_CODE_SUCCESS == pCxt->errCode && pCxt->pComCxt->collectVStbRefDbs) {
    pCxt->errCode = reserveVStbRefDbsInCache(pCxt->pComCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, pCxt->pComCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes collectMetaKeyFromTempTable(SCollectMetaKeyFromExprCxt* pCxt, STempTableNode* pTempTable) {
  pCxt->errCode = collectMetaKeyFromQuery(pCxt->pComCxt, pTempTable->pSubquery);
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static int32_t isTbnameEqCondOperator(SOperatorNode* pOperator, char** ppTableName) {
  if (pOperator->opType != OP_TYPE_EQUAL) {
    return TSDB_CODE_SUCCESS;
  }

  SValueNode* pValueNode = NULL;
  if (nodeType(pOperator->pLeft) == QUERY_NODE_FUNCTION &&
      0 == strcasecmp(((SFunctionNode*)(pOperator->pLeft))->functionName, "tbname") &&
      nodeType(pOperator->pRight) == QUERY_NODE_VALUE) {
    pValueNode = (SValueNode*)pOperator->pRight;
  } else if (nodeType(pOperator->pRight) == QUERY_NODE_FUNCTION &&
             0 == strcasecmp(((SFunctionNode*)(pOperator->pRight))->functionName, "tbname") &&
             nodeType(pOperator->pLeft) == QUERY_NODE_VALUE) {
    pValueNode = (SValueNode*)pOperator->pLeft;
  } else {
    return TSDB_CODE_SUCCESS;
  }

  *ppTableName = pValueNode->literal;

  return TSDB_CODE_SUCCESS;
}

static EDealRes collectMetaKeyFromOperator(SCollectMetaKeyFromExprCxt* pCxt, SOperatorNode* pOpNode) {
  if (!pCxt->tbnameCollect) {
    return DEAL_RES_CONTINUE;
  }

  char*   pTableName = NULL;
  int32_t code = isTbnameEqCondOperator((SOperatorNode*)pOpNode, &pTableName);
  if (TSDB_CODE_SUCCESS != code) return DEAL_RES_CONTINUE;
  if (pTableName) {
    SSelectStmt* pSelect = (SSelectStmt*)pCxt->pComCxt->pStmt;
    pCxt->errCode = collectMetaKeyFromRealTableImpl(pCxt->pComCxt, ((SRealTableNode*)pSelect->pFromTable)->table.dbName,
                                                    pTableName, AUTH_TYPE_READ);
  }

  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes collectMetaKeyFromExprImpl(SNode* pNode, void* pContext) {
  SCollectMetaKeyFromExprCxt* pCxt = pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_FUNCTION:
      return collectMetaKeyFromFunction(pCxt, (SFunctionNode*)pNode);
    case QUERY_NODE_REAL_TABLE:
      return collectMetaKeyFromRealTable(pCxt, (SRealTableNode*)pNode);
    case QUERY_NODE_TEMP_TABLE:
      return collectMetaKeyFromTempTable(pCxt, (STempTableNode*)pNode);
    case QUERY_NODE_OPERATOR:
      return collectMetaKeyFromOperator(pCxt, (SOperatorNode*)pNode);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t collectMetaKeyFromExprs(SCollectMetaKeyCxt* pCxt, SNodeList* pList) {
  SCollectMetaKeyFromExprCxt cxt = {.pComCxt = pCxt, .errCode = TSDB_CODE_SUCCESS, .tbnameCollect = false};
  nodesWalkExprs(pList, collectMetaKeyFromExprImpl, &cxt);
  return cxt.errCode;
}

static int32_t collectMetaKeyFromSetOperator(SCollectMetaKeyCxt* pCxt, SSetOperator* pStmt) {
  int32_t code = collectMetaKeyFromQuery(pCxt, pStmt->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyFromQuery(pCxt, pStmt->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyFromExprs(pCxt, pStmt->pOrderByList);
  }
  return code;
}

static int32_t reserveDbCfgForLastRow(SCollectMetaKeyCxt* pCxt, SNode* pTable) {
  if (NULL == pTable || QUERY_NODE_REAL_TABLE != nodeType(pTable)) {
    return TSDB_CODE_SUCCESS;
  }
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, ((SRealTableNode*)pTable)->table.dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromSelect(SCollectMetaKeyCxt* pCxt, SSelectStmt* pStmt) {
  SCollectMetaKeyFromExprCxt cxt = {.pComCxt = pCxt, .hasLastRowOrLast = false, .errCode = TSDB_CODE_SUCCESS};
  if (pStmt->pFromTable && QUERY_NODE_REAL_TABLE == nodeType(pStmt->pFromTable)) {
    cxt.tbnameCollect = true;
    cxt.pComCxt->collectVStbRefDbs = true;
  }
  nodesWalkSelectStmt(pStmt, SQL_CLAUSE_FROM, collectMetaKeyFromExprImpl, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode && cxt.hasLastRowOrLast) {
    cxt.errCode = reserveDbCfgForLastRow(pCxt, pStmt->pFromTable);
  }
  return cxt.errCode;
}

static int32_t collectMetaKeyFromAlterDatabase(SCollectMetaKeyCxt* pCxt, SAlterDatabaseStmt* pStmt) {
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromFlushDatabase(SCollectMetaKeyCxt* pCxt, SFlushDatabaseStmt* pStmt) {
  return reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCreateTable(SCollectMetaKeyCxt* pCxt, SCreateTableStmt* pStmt) {
  int32_t code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code && NULL == pStmt->pTags) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, NULL, AUTH_TYPE_WRITE,
                                  pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromCreateVTable(SCollectMetaKeyCxt* pCxt, SCreateVTableStmt* pStmt) {
  int32_t code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, NULL, AUTH_TYPE_WRITE,
                                  pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    SNode *pNode = NULL;
    FOREACH(pNode, pStmt->pCols) {
      SColumnDefNode *pCol = (SColumnDefNode*)pNode;
      if (NULL == pCol) {
        code = TSDB_CODE_PAR_INVALID_COLUMN;
        break;
      }
      SColumnOptions *pOptions = (SColumnOptions*)pCol->pOptions;
      if (pOptions && pOptions->hasRef) {
        code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pOptions->refDb, pOptions->refTable, pCxt->pMetaCache);
        if (TSDB_CODE_SUCCESS == code) {
          code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pOptions->refDb, pOptions->refTable, pCxt->pMetaCache);
        }
        if (TSDB_CODE_SUCCESS == code) {
          code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pOptions->refDb,
                                        pOptions->refTable, AUTH_TYPE_READ, pCxt->pMetaCache);
        }
        if (TSDB_CODE_SUCCESS != code) {
          break;
        }
      }
    }
  }
  return code;
}

static int32_t collectMetaKeyFromCreateVSubTable(SCollectMetaKeyCxt* pCxt, SCreateVSubTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  PAR_ERR_RET(reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache));
  // super table's meta
  PAR_ERR_RET(reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->useDbName, pStmt->useTableName,pCxt->pMetaCache));
  // child table's meta
  PAR_ERR_RET(reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache));
  // check db's write auth
  PAR_ERR_RET(reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, NULL,
                                     AUTH_TYPE_WRITE, pCxt->pMetaCache));
  // check org table's read auth
  SNode     *pNode = NULL;
  SNodeList *pTmpNodeList = pStmt->pSpecificColRefs ? pStmt->pSpecificColRefs : pStmt->pColRefs;
  if (NULL == pTmpNodeList) {
    // no column reference
    return TSDB_CODE_SUCCESS;
  }

  FOREACH(pNode, pTmpNodeList) {
    SColumnRefNode *pColRef = (SColumnRefNode*)pNode;
    if (NULL == pColRef) {
      code = TSDB_CODE_PAR_INVALID_COLUMN;
      break;
    }
    PAR_ERR_RET(reserveTableMetaInCache(pCxt->pParseCxt->acctId, pColRef->refDbName, pColRef->refTableName,
                                        pCxt->pMetaCache));
    PAR_ERR_RET(reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pColRef->refDbName, pColRef->refTableName,
                                          pCxt->pMetaCache));
    PAR_ERR_RET(reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pColRef->refDbName,
                                       pColRef->refTableName, AUTH_TYPE_READ, pCxt->pMetaCache));
  }
  return code;
}

static int32_t collectMetaKeyFromCreateMultiTable(SCollectMetaKeyCxt* pCxt, SCreateMultiTablesStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pSubTables) {
    if (pNode->type == QUERY_NODE_CREATE_SUBTABLE_CLAUSE) {
      SCreateSubTableClause* pClause = (SCreateSubTableClause*)pNode;
      code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pClause->dbName, pCxt->pMetaCache);
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pClause->useTableName,
                                       pCxt->pMetaCache);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code =
            reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, pCxt->pMetaCache);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pClause->dbName, NULL,
                                      AUTH_TYPE_WRITE, pCxt->pMetaCache);
      }
    } else {
      SCreateSubTableFromFileClause* pClause = (SCreateSubTableFromFileClause*)pNode;
      code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pCxt->pMetaCache);
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pClause->useTableName,
                                       pCxt->pMetaCache);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pClause->useDbName, NULL,
                                      AUTH_TYPE_WRITE, pCxt->pMetaCache);
      }
    }

    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t collectMetaKeyFromCreateSubTableFromFile(SCollectMetaKeyCxt*            pCxt,
                                                        SCreateSubTableFromFileClause* pClause) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;

  code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveTableMetaInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pClause->useTableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pClause->useDbName, NULL,
                                  AUTH_TYPE_WRITE, pCxt->pMetaCache);
  }

  return code;
}

static int32_t collectMetaKeyFromDropTable(SCollectMetaKeyCxt* pCxt, SDropTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    if (pStmt->withOpt) {
      code = reserveTableUidInCache(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, pCxt->pMetaCache);
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pClause->dbName, pCxt->pMetaCache);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pClause->dbName, pCxt->pMetaCache);
      }
    } else {
      code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, pCxt->pMetaCache);
      if (TSDB_CODE_SUCCESS == code) {
        code =
            reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, pCxt->pMetaCache);
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pClause->dbName,
                                      pClause->tableName, AUTH_TYPE_WRITE, pCxt->pMetaCache);
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t collectMetaKeyFromDropStable(SCollectMetaKeyCxt* pCxt, SDropSuperTableStmt* pStmt) {
  if (pStmt->withOpt) {
    return reserveTableUidInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  return reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->tableName,
                                AUTH_TYPE_WRITE, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromDropVtable(SCollectMetaKeyCxt* pCxt, SDropVirtualTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pStmt->withOpt) {
    code = reserveTableUidInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
    }
  } else {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code =
          reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName,
                                    pStmt->tableName, AUTH_TYPE_WRITE, pCxt->pMetaCache);
    }
  }
  return code;
}

static int32_t collectMetaKeyFromAlterTable(SCollectMetaKeyCxt* pCxt, SAlterTableStmt* pStmt) {
  int32_t code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code && (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == pStmt->alterType || TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL == pStmt->alterType)) {
    SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
    tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(name.tname, pStmt->tableName, TSDB_TABLE_NAME_LEN);
    code = catalogRemoveTableRelatedMeta(pCxt->pParseCxt->pCatalog, &name);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->tableName,
                                  AUTH_TYPE_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromAlterStable(SCollectMetaKeyCxt* pCxt, SAlterTableStmt* pStmt) {
  int32_t code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->tableName,
                                  AUTH_TYPE_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromAlterVtable(SCollectMetaKeyCxt* pCxt, SAlterTableStmt* pStmt) {
  PAR_ERR_RET(reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache));
  if (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == pStmt->alterType || TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL == pStmt->alterType) {
    SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
    tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
    tstrncpy(name.tname, pStmt->tableName, TSDB_TABLE_NAME_LEN);
    PAR_ERR_RET(catalogRemoveTableRelatedMeta(pCxt->pParseCxt->pCatalog, &name));
  }
  PAR_ERR_RET(reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache));
  PAR_ERR_RET(reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache));
  PAR_ERR_RET(reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->tableName,
                                     AUTH_TYPE_WRITE, pCxt->pMetaCache));
  if (pStmt->alterType == TSDB_ALTER_TABLE_ALTER_COLUMN_REF || pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF) {
    PAR_ERR_RET(reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->refDbName, pStmt->refTableName, pCxt->pMetaCache));
    PAR_ERR_RET(reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->refDbName, pStmt->refTableName,
                                       AUTH_TYPE_READ, pCxt->pMetaCache));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromUseDatabase(SCollectMetaKeyCxt* pCxt, SUseDatabaseStmt* pStmt) {
  return reserveDbVgVersionInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCreateIndex(SCollectMetaKeyCxt* pCxt, SCreateIndexStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (INDEX_TYPE_SMA == pStmt->indexType || INDEX_TYPE_NORMAL == pStmt->indexType) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
    }
  }
  return code;
}

static int32_t collectMetaKeyFromCreateTopic(SCollectMetaKeyCxt* pCxt, SCreateTopicStmt* pStmt) {
  if (NULL != pStmt->pQuery) {
    return collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
  }
  if (NULL != pStmt->pWhere) {
    int32_t code = collectMetaKeyFromRealTableImpl(pCxt, pStmt->subDbName, pStmt->subSTbName, AUTH_TYPE_READ);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromExplain(SCollectMetaKeyCxt* pCxt, SExplainStmt* pStmt) {
  return collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
}

static int32_t collectMetaKeyFromDescribe(SCollectMetaKeyCxt* pCxt, SDescribeStmt* pStmt) {
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(name.tname, pStmt->tableName, TSDB_TABLE_NAME_LEN);
  int32_t code = catalogRemoveTableRelatedMeta(pCxt->pParseCxt->pCatalog, &name);
#ifdef TD_ENTERPRISE
  if (TSDB_CODE_SUCCESS == code) {
    char dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(&name, dbFName);
    code = catalogRemoveViewMeta(pCxt->pParseCxt->pCatalog, dbFName, 0, pStmt->tableName, 0);
  }
#endif
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromCreateStream(SCollectMetaKeyCxt* pCxt, SCreateStreamStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (strcmp(pStmt->targetTabName, "") != 0) {
    PAR_ERR_RET(reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->targetDbName, pStmt->targetTabName, pCxt->pMetaCache));
    reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->targetDbName, pStmt->targetTabName, pCxt->pMetaCache);
    PAR_ERR_RET(reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->targetDbName, pCxt->pMetaCache));
  }
  PAR_ERR_RET(reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->streamDbName, pCxt->pMetaCache));
  SRealTableNode *pTriggerTable = (SRealTableNode*)((SStreamTriggerNode*)pStmt->pTrigger)->pTrigerTable;
  if (pTriggerTable) {
    SCollectMetaKeyFromExprCxt cxt = {.pComCxt = pCxt, .hasLastRowOrLast = false, .tbnameCollect = true, .errCode = TSDB_CODE_SUCCESS};
    cxt.pComCxt->collectVStbRefDbs = true;
    EDealRes res = collectMetaKeyFromRealTable(&cxt, pTriggerTable);
    PAR_ERR_RET(cxt.errCode);
    PAR_ERR_RET(reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pTriggerTable->table.dbName, pCxt->pMetaCache));
    PAR_ERR_RET(reserveDbCfgInCache(pCxt->pParseCxt->acctId, pTriggerTable->table.dbName, pCxt->pMetaCache));
  }
  if (pStmt->pQuery) {
    PAR_ERR_RET(collectMetaKeyFromQuery(pCxt, pStmt->pQuery));
  }

  return code;
}

static int32_t collectMetaKeyFromRecalculateStream(SCollectMetaKeyCxt* pCxt, SRecalcStreamStmt* pStmt) {
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->streamDbName, pCxt->pMetaCache);
}


static int32_t collectMetaKeyFromShowDnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DNODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowMnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MNODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowModules(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MODULES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowQnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_QNODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowSnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_SNODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowAnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_ANODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowAnodesFull(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_ANODES_FULL,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowBnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_BNODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowBackupNodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_BACKUP_NODES,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowArbGroups(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_ARBGROUPS,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowCluster(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  if (pCxt->pParseCxt->enableSysInfo) {
    return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_CLUSTER,
                                   pCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromShowDatabases(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DATABASES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowFunctions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_FUNCTIONS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowIndexes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_INDEXES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowStables(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_STABLES,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowStreams(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_STREAMS,
                                 pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowTables(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TABLES,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowFilesets(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_FILESETS,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowTags(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TAGS,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal,
                                   ((SValueNode*)pStmt->pTbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pTbName) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal,
                                     ((SValueNode*)pStmt->pTbName)->literal, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowStableTags(SCollectMetaKeyCxt* pCxt, SShowTableTagsStmt* pStmt) {
  return collectMetaKeyFromRealTableImpl(pCxt, ((SValueNode*)pStmt->pDbName)->literal,
                                         ((SValueNode*)pStmt->pTbName)->literal, AUTH_TYPE_READ);
}

static int32_t collectMetaKeyFromShowUsers(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USERS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowUsersFull(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USERS_FULL,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowLicence(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_LICENCES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowVgroups(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VGROUPS,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    // just to verify whether the database exists
    code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowTopics(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TOPICS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowConsumers(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_CONSUMERS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowConnections(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_CONNECTIONS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowQueries(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_QUERIES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowVariables(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_CONFIGS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowDnodeVariables(SCollectMetaKeyCxt* pCxt, SShowDnodeVariablesStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB,
                                         TSDB_INS_TABLE_DNODE_VARIABLES, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDnodeRequiredInCache(pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowVnodes(SCollectMetaKeyCxt* pCxt, SShowVnodesStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowUserPrivileges(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_PRIVILEGES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowViews(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VIEWS,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowCompacts(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_COMPACTS,
                                         pCxt->pMetaCache);
  return code;
}

static int32_t collectMetaKeyFromShowCompactDetails(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB,
                                         TSDB_INS_TABLE_COMPACT_DETAILS, pCxt->pMetaCache);
  return code;
}

static int32_t collectMetaKeyFromShowTransactionDetails(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB,
                                         TSDB_INS_TABLE_TRANSACTION_DETAILS, pCxt->pMetaCache);
  return code;
}

static int32_t collectMetaKeyFromShowGrantsFull(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_GRANTS_FULL,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowGrantsLogs(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_GRANTS_LOGS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowClusterMachines(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MACHINES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowEncryptions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_ENCRYPTIONS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowMounts(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MOUNTS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowCreateDatabase(SCollectMetaKeyCxt* pCxt, SShowCreateDatabaseStmt* pStmt) {
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowCreateTable(SCollectMetaKeyCxt* pCxt, SShowCreateTableStmt* pStmt) {
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(name.tname, pStmt->tableName, TSDB_TABLE_NAME_LEN);
  int32_t code = catalogRemoveTableMeta(pCxt->pParseCxt->pCatalog, &name);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->tableName,
                                  AUTH_TYPE_READ, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowCreateView(SCollectMetaKeyCxt* pCxt, SShowCreateViewStmt* pStmt) {
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(name.tname, pStmt->viewName, TSDB_TABLE_NAME_LEN);
  char dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(&name, dbFName);
  int32_t code = catalogRemoveViewMeta(pCxt->pParseCxt->pCatalog, dbFName, 0, pStmt->viewName, 0);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveViewUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->viewName,
                                      AUTH_TYPE_READ, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->viewName, pCxt->pMetaCache);
  }
  pCxt->pMetaCache->forceFetchViewMeta = true;
  return code;
}

static int32_t collectMetaKeyFromShowApps(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_APPS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowTransactions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_TRANS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromDelete(SCollectMetaKeyCxt* pCxt, SDeleteStmt* pStmt) {
  STableNode* pTable = (STableNode*)pStmt->pFromTable;
  return collectMetaKeyFromRealTableImpl(pCxt, pTable->dbName, pTable->tableName, AUTH_TYPE_WRITE);
}

static int32_t collectMetaKeyFromInsert(SCollectMetaKeyCxt* pCxt, SInsertStmt* pStmt) {
  STableNode* pTable = (STableNode*)pStmt->pTable;
  int32_t     code = collectMetaKeyFromRealTableImpl(pCxt, pTable->dbName, pTable->tableName, AUTH_TYPE_WRITE);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
  }
  return code;
}

static int32_t collectMetaKeyFromShowBlockDist(SCollectMetaKeyCxt* pCxt, SShowTableDistributedStmt* pStmt) {
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  tstrncpy(name.dbname, pStmt->dbName, TSDB_DB_NAME_LEN);
  tstrncpy(name.tname, pStmt->tableName, TSDB_TABLE_NAME_LEN);
  int32_t code = catalogRemoveTableMeta(pCxt->pParseCxt->pCatalog, &name);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKeyFromRealTableImpl(pCxt, pStmt->dbName, pStmt->tableName, AUTH_TYPE_READ);
  }
  return code;
}

static int32_t collectMetaKeyFromShowSubscriptions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_SUBSCRIPTIONS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCompactDatabase(SCollectMetaKeyCxt* pCxt, SCompactDatabaseStmt* pStmt) {
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCompactVgroups(SCollectMetaKeyCxt* pCxt, SCompactVgroupsStmt* pStmt) {
  return reserveDbCfgInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromGrant(SCollectMetaKeyCxt* pCxt, SGrantStmt* pStmt) {
  if ('\0' == pStmt->tabName[0]) {
    return TSDB_CODE_SUCCESS;
  }
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->objName, pStmt->tabName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromRevoke(SCollectMetaKeyCxt* pCxt, SRevokeStmt* pStmt) {
  if ('\0' == pStmt->tabName[0]) {
    return TSDB_CODE_SUCCESS;
  }
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->objName, pStmt->tabName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCreateViewStmt(SCollectMetaKeyCxt* pCxt, SCreateViewStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->viewName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, NULL, AUTH_TYPE_WRITE,
                                  pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveViewUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName, pStmt->viewName,
                                      AUTH_TYPE_ALTER, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromDropViewStmt(SCollectMetaKeyCxt* pCxt, SDropViewStmt* pStmt) {
  int32_t code = reserveViewUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, pStmt->dbName,
                                            pStmt->viewName, AUTH_TYPE_ALTER, pCxt->pMetaCache);
  pCxt->pMetaCache->forceFetchViewMeta = true;
  return code;
}

static int32_t collectMetaKeyFromCreateTSMAStmt(SCollectMetaKeyCxt* pCxt, SCreateTSMAStmt* pStmt) {
  int32_t code;
  if (pStmt->pOptions->recursiveTsma) {
    // if creating recursive tsma, the tablename is tsmaName
    code = reserveTSMAInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      char dstTbName[TSDB_TABLE_NAME_LEN] = {0};
      snprintf(dstTbName, TSDB_TABLE_NAME_LEN, "%s" TSMA_RES_STB_POSTFIX, pStmt->tableName);
      code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, dstTbName, pCxt->pMetaCache);
      if (TSDB_CODE_SUCCESS == code) {
        code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, dstTbName, pCxt->pMetaCache);
      }
    }
  } else {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromDropTSMAStmt(SCollectMetaKeyCxt* pCxt, SDropTSMAStmt* pStmt) {
  int32_t code;
  code = reserveTSMAInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tsmaName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowUsage(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_DISK_USAGE,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        reserveUserAuthInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->pUser, ((SValueNode*)pStmt->pDbName)->literal,
                               NULL, AUTH_TYPE_READ_OR_WRITE, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromShowTSMASStmt(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_TSMAS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowAlive(SCollectMetaKeyCxt* pCxt, SShowAliveStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VGROUPS,
                                         pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code && pStmt->dbName[0]) {
    // just to verify whether the database exists
    code = reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromQuery(SCollectMetaKeyCxt* pCxt, SNode* pStmt) {
  pCxt->pStmt = pStmt;
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SET_OPERATOR:
      return collectMetaKeyFromSetOperator(pCxt, (SSetOperator*)pStmt);
    case QUERY_NODE_SELECT_STMT:
      return collectMetaKeyFromSelect(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_ALTER_DATABASE_STMT:
      return collectMetaKeyFromAlterDatabase(pCxt, (SAlterDatabaseStmt*)pStmt);
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      return collectMetaKeyFromFlushDatabase(pCxt, (SFlushDatabaseStmt*)pStmt);
    case QUERY_NODE_CREATE_TABLE_STMT:
      return collectMetaKeyFromCreateTable(pCxt, (SCreateTableStmt*)pStmt);
    case QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT:
      return collectMetaKeyFromCreateVTable(pCxt, (SCreateVTableStmt*)pStmt);
    case QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT:
      return collectMetaKeyFromCreateVSubTable(pCxt, (SCreateVSubTableStmt*)pStmt);
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      return collectMetaKeyFromCreateMultiTable(pCxt, (SCreateMultiTablesStmt*)pStmt);
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE:
      return collectMetaKeyFromCreateSubTableFromFile(pCxt, (SCreateSubTableFromFileClause*)pStmt);
    case QUERY_NODE_DROP_TABLE_STMT:
      return collectMetaKeyFromDropTable(pCxt, (SDropTableStmt*)pStmt);
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      return collectMetaKeyFromDropStable(pCxt, (SDropSuperTableStmt*)pStmt);
    case QUERY_NODE_DROP_VIRTUAL_TABLE_STMT:
      return collectMetaKeyFromDropVtable(pCxt, (SDropVirtualTableStmt*)pStmt);
    case QUERY_NODE_ALTER_TABLE_STMT:
      return collectMetaKeyFromAlterTable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT:
      return collectMetaKeyFromAlterStable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT:
      return collectMetaKeyFromAlterVtable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_USE_DATABASE_STMT:
      return collectMetaKeyFromUseDatabase(pCxt, (SUseDatabaseStmt*)pStmt);
    case QUERY_NODE_CREATE_INDEX_STMT:
      return collectMetaKeyFromCreateIndex(pCxt, (SCreateIndexStmt*)pStmt);
    case QUERY_NODE_CREATE_TOPIC_STMT:
      return collectMetaKeyFromCreateTopic(pCxt, (SCreateTopicStmt*)pStmt);
    case QUERY_NODE_EXPLAIN_STMT:
      return collectMetaKeyFromExplain(pCxt, (SExplainStmt*)pStmt);
    case QUERY_NODE_DESCRIBE_STMT:
      return collectMetaKeyFromDescribe(pCxt, (SDescribeStmt*)pStmt);
    case QUERY_NODE_COMPACT_DATABASE_STMT:
      return collectMetaKeyFromCompactDatabase(pCxt, (SCompactDatabaseStmt*)pStmt);
    case QUERY_NODE_COMPACT_VGROUPS_STMT:
      return collectMetaKeyFromCompactVgroups(pCxt, (SCompactVgroupsStmt*)pStmt);
    case QUERY_NODE_CREATE_STREAM_STMT:
      return collectMetaKeyFromCreateStream(pCxt, (SCreateStreamStmt*)pStmt);
    case QUERY_NODE_RECALCULATE_STREAM_STMT:
      return collectMetaKeyFromRecalculateStream(pCxt, (SRecalcStreamStmt*)pStmt);
    case QUERY_NODE_GRANT_STMT:
      return collectMetaKeyFromGrant(pCxt, (SGrantStmt*)pStmt);
    case QUERY_NODE_REVOKE_STMT:
      return collectMetaKeyFromRevoke(pCxt, (SRevokeStmt*)pStmt);
    case QUERY_NODE_SHOW_DNODES_STMT:
      return collectMetaKeyFromShowDnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_MNODES_STMT:
      return collectMetaKeyFromShowMnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_MODULES_STMT:
      return collectMetaKeyFromShowModules(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_QNODES_STMT:
      return collectMetaKeyFromShowQnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_SNODES_STMT:
      return collectMetaKeyFromShowSnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_ANODES_STMT:
      return collectMetaKeyFromShowAnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_ANODES_FULL_STMT:
      return collectMetaKeyFromShowAnodesFull(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_BNODES_STMT:
      return collectMetaKeyFromShowBnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_BACKUP_NODES_STMT:
      return collectMetaKeyFromShowBackupNodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_ARBGROUPS_STMT:
      return collectMetaKeyFromShowArbGroups(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CLUSTER_STMT:
      return collectMetaKeyFromShowCluster(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_DATABASES_STMT:
      return collectMetaKeyFromShowDatabases(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
      return collectMetaKeyFromShowFunctions(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_INDEXES_STMT:
      return collectMetaKeyFromShowIndexes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_STABLES_STMT:
      return collectMetaKeyFromShowStables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_STREAMS_STMT:
      return collectMetaKeyFromShowStreams(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_VTABLES_STMT:
      return collectMetaKeyFromShowTables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_FILESETS_STMT:
      return collectMetaKeyFromShowFilesets(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TAGS_STMT:
      return collectMetaKeyFromShowTags(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TABLE_TAGS_STMT:
      return collectMetaKeyFromShowStableTags(pCxt, (SShowTableTagsStmt*)pStmt);
    case QUERY_NODE_SHOW_USERS_STMT:
      return collectMetaKeyFromShowUsers(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_USERS_FULL_STMT:
      return collectMetaKeyFromShowUsersFull(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_LICENCES_STMT:
      return collectMetaKeyFromShowLicence(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_VGROUPS_STMT:
      return collectMetaKeyFromShowVgroups(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TOPICS_STMT:
      return collectMetaKeyFromShowTopics(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
      return collectMetaKeyFromShowConsumers(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
      return collectMetaKeyFromShowConnections(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_QUERIES_STMT:
      return collectMetaKeyFromShowQueries(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_VARIABLES_STMT:
      return collectMetaKeyFromShowVariables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
      return collectMetaKeyFromShowDnodeVariables(pCxt, (SShowDnodeVariablesStmt*)pStmt);
    case QUERY_NODE_SHOW_VNODES_STMT:
      return collectMetaKeyFromShowVnodes(pCxt, (SShowVnodesStmt*)pStmt);
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
      return collectMetaKeyFromShowUserPrivileges(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_VIEWS_STMT:
      return collectMetaKeyFromShowViews(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_COMPACTS_STMT:
      return collectMetaKeyFromShowCompacts(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_COMPACT_DETAILS_STMT:
      return collectMetaKeyFromShowCompactDetails(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT:
      return collectMetaKeyFromShowTransactionDetails(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_GRANTS_FULL_STMT:
      return collectMetaKeyFromShowGrantsFull(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_GRANTS_LOGS_STMT:
      return collectMetaKeyFromShowGrantsLogs(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT:
      return collectMetaKeyFromShowClusterMachines(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_ENCRYPTIONS_STMT:
      return collectMetaKeyFromShowEncryptions(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_MOUNTS_STMT:
      return collectMetaKeyFromShowMounts(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return collectMetaKeyFromShowCreateDatabase(pCxt, (SShowCreateDatabaseStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_VTABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return collectMetaKeyFromShowCreateTable(pCxt, (SShowCreateTableStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      return collectMetaKeyFromShowCreateView(pCxt, (SShowCreateViewStmt*)pStmt);
    case QUERY_NODE_SHOW_APPS_STMT:
      return collectMetaKeyFromShowApps(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      return collectMetaKeyFromShowTransactions(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_USAGE_STMT:
      return collectMetaKeyFromShowUsage(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_DELETE_STMT:
      return collectMetaKeyFromDelete(pCxt, (SDeleteStmt*)pStmt);
    case QUERY_NODE_INSERT_STMT:
      return collectMetaKeyFromInsert(pCxt, (SInsertStmt*)pStmt);
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
      return collectMetaKeyFromShowBlockDist(pCxt, (SShowTableDistributedStmt*)pStmt);
    case QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT:
      return collectMetaKeyFromShowSubscriptions(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_CREATE_VIEW_STMT:
      return collectMetaKeyFromCreateViewStmt(pCxt, (SCreateViewStmt*)pStmt);
    case QUERY_NODE_DROP_VIEW_STMT:
      return collectMetaKeyFromDropViewStmt(pCxt, (SDropViewStmt*)pStmt);
    case QUERY_NODE_CREATE_TSMA_STMT:
      return collectMetaKeyFromCreateTSMAStmt(pCxt, (SCreateTSMAStmt*)pStmt);
    case QUERY_NODE_DROP_TSMA_STMT:
      return collectMetaKeyFromDropTSMAStmt(pCxt, (SDropTSMAStmt*)pStmt);
      break;
    case QUERY_NODE_SHOW_TSMAS_STMT:
      return collectMetaKeyFromShowTSMASStmt(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
      return collectMetaKeyFromShowAlive(pCxt, (SShowAliveStmt*)pStmt);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t collectMetaKey(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
  SCollectMetaKeyCxt cxt = {.pParseCxt = pParseCxt, .pMetaCache = pMetaCache, .pStmt = pQuery->pRoot};
  return collectMetaKeyFromQuery(&cxt, pQuery->pRoot);
}
