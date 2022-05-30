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

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* ParseAlloc(FMalloc);
extern void  Parse(void*, int, SToken, void*);
extern void  ParseFree(void*, FFree);
extern void  ParseTrace(FILE*, char*);

int32_t parse(SParseContext* pParseCxt, SQuery** pQuery) {
  SAstCreateContext cxt;
  initAstCreateContext(pParseCxt, &cxt);
  void*   pParser = ParseAlloc((FMalloc)taosMemoryMalloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      Parse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    t0.n = tGetToken((char*)&cxt.pQueryCxt->pSql[i], &t0.type);
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
      case TK_NK_HEX:
      case TK_NK_OCT:
      case TK_NK_BIN: {
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
  ParseFree(pParser, (FFree)taosMemoryFree);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    *pQuery = taosMemoryCalloc(1, sizeof(SQuery));
    if (NULL == *pQuery) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    (*pQuery)->pRoot = cxt.pRootNode;
    (*pQuery)->placeholderNum = cxt.placeholderNo;
    TSWAP((*pQuery)->pPlaceholderValues, cxt.pPlaceholderValues);
  }
  taosArrayDestroy(cxt.pPlaceholderValues);
  return cxt.errCode;
}

typedef struct SCollectMetaKeyCxt {
  SParseContext*   pParseCxt;
  SParseMetaCache* pMetaCache;
} SCollectMetaKeyCxt;

static void destroyCollectMetaKeyCxt(SCollectMetaKeyCxt* pCxt) {
  if (NULL != pCxt->pMetaCache) {
    // TODO
  }
}

typedef struct SCollectMetaKeyFromExprCxt {
  SCollectMetaKeyCxt* pComCxt;
  int32_t             errCode;
} SCollectMetaKeyFromExprCxt;

static int32_t collectMetaKeyFromQuery(SCollectMetaKeyCxt* pCxt, SNode* pStmt);

static EDealRes collectMetaKeyFromFunction(SCollectMetaKeyFromExprCxt* pCxt, SFunctionNode* pFunc) {
  if (fmIsBuiltinFunc(pFunc->functionName)) {
    return TSDB_CODE_SUCCESS;
  }
  return reserveUdfInCache(pFunc->functionName, pCxt->pComCxt->pMetaCache);
}

static EDealRes collectMetaKeyFromRealTable(SCollectMetaKeyFromExprCxt* pCxt, SRealTableNode* pRealTable) {
  pCxt->errCode = reserveTableMetaInCache(pCxt->pComCxt->pParseCxt->acctId, pRealTable->table.dbName,
                                          pRealTable->table.tableName, pCxt->pComCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = reserveTableVgroupInCache(pCxt->pComCxt->pParseCxt->acctId, pRealTable->table.dbName,
                                              pRealTable->table.tableName, pCxt->pComCxt->pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode = reserveUserAuthInCache(pCxt->pComCxt->pParseCxt->acctId, pCxt->pComCxt->pParseCxt->pUser,
                                           pRealTable->table.dbName, AUTH_TYPE_READ, pCxt->pComCxt->pMetaCache);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes collectMetaKeyFromTempTable(SCollectMetaKeyFromExprCxt* pCxt, STempTableNode* pTempTable) {
  pCxt->errCode = collectMetaKeyFromQuery(pCxt->pComCxt, pTempTable->pSubquery);
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
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t collectMetaKeyFromExprs(SCollectMetaKeyCxt* pCxt, SNodeList* pList) {
  SCollectMetaKeyFromExprCxt cxt = {.pComCxt = pCxt, .errCode = TSDB_CODE_SUCCESS};
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

static int32_t collectMetaKeyFromSelect(SCollectMetaKeyCxt* pCxt, SSelectStmt* pStmt) {
  SCollectMetaKeyFromExprCxt cxt = {.pComCxt = pCxt, .errCode = TSDB_CODE_SUCCESS};
  nodesWalkSelectStmt(pStmt, SQL_CLAUSE_FROM, collectMetaKeyFromExprImpl, &cxt);
  return cxt.errCode;
}

static int32_t collectMetaKeyFromCreateTable(SCollectMetaKeyCxt* pCxt, SCreateTableStmt* pStmt) {
  if (NULL == pStmt->pTags) {
    return reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  } else {
    return reserveDbCfgInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
  }
}

static int32_t collectMetaKeyFromCreateMultiTable(SCollectMetaKeyCxt* pCxt, SCreateMultiTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pSubTables) {
    SCreateSubTableClause* pClause = (SCreateSubTableClause*)pNode;
    code =
        reserveTableMetaInCache(pCxt->pParseCxt->acctId, pClause->useDbName, pClause->useTableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, pCxt->pMetaCache);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t collectMetaKeyFromAlterTable(SCollectMetaKeyCxt* pCxt, SAlterTableStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, pCxt->pMetaCache);
  }
  return code;
}

static int32_t collectMetaKeyFromUseDatabase(SCollectMetaKeyCxt* pCxt, SUseDatabaseStmt* pStmt) {
  return reserveDbVgVersionInCache(pCxt->pParseCxt->acctId, pStmt->dbName, pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromCreateIndex(SCollectMetaKeyCxt* pCxt, SCreateIndexStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (INDEX_TYPE_SMA == pStmt->indexType) {
    code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->tableName, pCxt->pMetaCache);
    if (TSDB_CODE_SUCCESS == code) {
      code =
          reserveTableVgroupInCache(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->tableName, pCxt->pMetaCache);
    }
  }
  return code;
}

static int32_t collectMetaKeyFromCreateTopic(SCollectMetaKeyCxt* pCxt, SCreateTopicStmt* pStmt) {
  if (NULL != pStmt->pQuery) {
    return collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t collectMetaKeyFromExplain(SCollectMetaKeyCxt* pCxt, SExplainStmt* pStmt) {
  return collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
}

static int32_t collectMetaKeyFromCreateStream(SCollectMetaKeyCxt* pCxt, SCreateStreamStmt* pStmt) {
  return collectMetaKeyFromQuery(pCxt, pStmt->pQuery);
}

static int32_t collectMetaKeyFromShowDnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_DNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowMnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowModules(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_MODULES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowQnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_QNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowSnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_SNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowBnodes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_BNODES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowDatabases(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_DATABASES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowFunctions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_FUNCTIONS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowIndexes(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_INDEXES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowStables(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_STABLES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowStreams(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_STREAMS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowTables(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  int32_t code = reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB,
                                         TSDB_INS_TABLE_USER_TABLES, pCxt->pMetaCache);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pStmt->pDbName) {
      code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, ((SValueNode*)pStmt->pDbName)->literal, pCxt->pMetaCache);
    } else {
      code = reserveDbVgInfoInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, pCxt->pMetaCache);
    }
  }
  return code;
}

static int32_t collectMetaKeyFromShowUsers(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_USER_USERS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowLicence(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_LICENCES,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowVgroups(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_INFORMATION_SCHEMA_DB, TSDB_INS_TABLE_VGROUPS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowTopics(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_TOPICS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromShowTransactions(SCollectMetaKeyCxt* pCxt, SShowStmt* pStmt) {
  return reserveTableMetaInCache(pCxt->pParseCxt->acctId, TSDB_PERFORMANCE_SCHEMA_DB, TSDB_PERFS_TABLE_TRANS,
                                 pCxt->pMetaCache);
}

static int32_t collectMetaKeyFromQuery(SCollectMetaKeyCxt* pCxt, SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SET_OPERATOR:
      return collectMetaKeyFromSetOperator(pCxt, (SSetOperator*)pStmt);
    case QUERY_NODE_SELECT_STMT:
      return collectMetaKeyFromSelect(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_VNODE_MODIF_STMT:
    case QUERY_NODE_CREATE_DATABASE_STMT:
    case QUERY_NODE_DROP_DATABASE_STMT:
    case QUERY_NODE_ALTER_DATABASE_STMT:
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      return collectMetaKeyFromCreateTable(pCxt, (SCreateTableStmt*)pStmt);
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE:
      break;
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      return collectMetaKeyFromCreateMultiTable(pCxt, (SCreateMultiTableStmt*)pStmt);
    case QUERY_NODE_DROP_TABLE_CLAUSE:
    case QUERY_NODE_DROP_TABLE_STMT:
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
      return collectMetaKeyFromAlterTable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_CREATE_USER_STMT:
    case QUERY_NODE_ALTER_USER_STMT:
    case QUERY_NODE_DROP_USER_STMT:
      break;
    case QUERY_NODE_USE_DATABASE_STMT:
      return collectMetaKeyFromUseDatabase(pCxt, (SUseDatabaseStmt*)pStmt);
    case QUERY_NODE_CREATE_DNODE_STMT:
    case QUERY_NODE_DROP_DNODE_STMT:
    case QUERY_NODE_ALTER_DNODE_STMT:
      break;
    case QUERY_NODE_CREATE_INDEX_STMT:
      return collectMetaKeyFromCreateIndex(pCxt, (SCreateIndexStmt*)pStmt);
    case QUERY_NODE_DROP_INDEX_STMT:
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
      break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      return collectMetaKeyFromCreateTopic(pCxt, (SCreateTopicStmt*)pStmt);
    case QUERY_NODE_DROP_TOPIC_STMT:
    case QUERY_NODE_DROP_CGROUP_STMT:
    case QUERY_NODE_ALTER_LOCAL_STMT:
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      return collectMetaKeyFromExplain(pCxt, (SExplainStmt*)pStmt);
    case QUERY_NODE_DESCRIBE_STMT:
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
    case QUERY_NODE_COMPACT_STMT:
    case QUERY_NODE_CREATE_FUNCTION_STMT:
    case QUERY_NODE_DROP_FUNCTION_STMT:
      break;
    case QUERY_NODE_CREATE_STREAM_STMT:
      return collectMetaKeyFromCreateStream(pCxt, (SCreateStreamStmt*)pStmt);
    case QUERY_NODE_DROP_STREAM_STMT:
    case QUERY_NODE_MERGE_VGROUP_STMT:
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
    case QUERY_NODE_SPLIT_VGROUP_STMT:
    case QUERY_NODE_SYNCDB_STMT:
    case QUERY_NODE_GRANT_STMT:
    case QUERY_NODE_REVOKE_STMT:
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
    case QUERY_NODE_SHOW_BNODES_STMT:
      return collectMetaKeyFromShowBnodes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CLUSTER_STMT:
      break;
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
      return collectMetaKeyFromShowTables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_USERS_STMT:
      return collectMetaKeyFromShowUsers(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_LICENCE_STMT:
      return collectMetaKeyFromShowLicence(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_VGROUPS_STMT:
      return collectMetaKeyFromShowVgroups(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_TOPICS_STMT:
      return collectMetaKeyFromShowTopics(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIBES_STMT:
    case QUERY_NODE_SHOW_SMAS_STMT:
    case QUERY_NODE_SHOW_CONFIGS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_VARIABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      break;
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      return collectMetaKeyFromShowTransactions(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_KILL_CONNECTION_STMT:
    case QUERY_NODE_KILL_QUERY_STMT:
    case QUERY_NODE_KILL_TRANSACTION_STMT:
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t collectMetaKey(SParseContext* pParseCxt, SQuery* pQuery) {
  SCollectMetaKeyCxt cxt = {.pParseCxt = pParseCxt, .pMetaCache = taosMemoryCalloc(1, sizeof(SParseMetaCache))};
  if (NULL == cxt.pMetaCache) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = collectMetaKeyFromQuery(&cxt, pQuery->pRoot);
  if (TSDB_CODE_SUCCESS == code) {
    TSWAP(pQuery->pMetaCache, cxt.pMetaCache);
  }
  destroyCollectMetaKeyCxt(&cxt);
  return code;
}
