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

#include "catalog.h"
#include "cmdnodes.h"
#include "parInt.h"

typedef struct SAuthCxt {
  SParseContext*   pParseCxt;
  SParseMetaCache* pMetaCache;
  int32_t          errCode;
} SAuthCxt;

typedef struct SSelectAuthCxt {
  SAuthCxt*    pAuthCxt;
  SSelectStmt* pSelect;
} SSelectAuthCxt;

typedef struct SAuthRewriteCxt {
  STableNode*  pTarget;
} SAuthRewriteCxt;

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt);

static void setUserAuthInfo(SParseContext* pCxt, const char* pDbName, const char* pTabName, AUTH_TYPE type,
                            SUserAuthInfo* pAuth) {
  snprintf(pAuth->user, sizeof(pAuth->user), "%s", pCxt->pUser);
  if (NULL == pTabName) {
    tNameSetDbName(&pAuth->tbName, pCxt->acctId, pDbName, strlen(pDbName));
  } else {
    toName(pCxt->acctId, pDbName, pTabName, &pAuth->tbName);
  }
  pAuth->type = type;
}

static int32_t checkAuth(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, AUTH_TYPE type, SNode** pCond) {
  SParseContext* pParseCxt = pCxt->pParseCxt;
  if (pParseCxt->isSuperUser) {
    return TSDB_CODE_SUCCESS;
  }

  SUserAuthInfo authInfo = {0};
  setUserAuthInfo(pCxt->pParseCxt, pDbName, pTabName, type, &authInfo);
  int32_t      code = TSDB_CODE_SUCCESS;
  SUserAuthRes authRes = {0};
  if (NULL != pCxt->pMetaCache) {
    code = getUserAuthFromCache(pCxt->pMetaCache, &authInfo, &authRes);
  } else {
    SRequestConnInfo conn = {.pTrans = pParseCxt->pTransporter,
                             .requestId = pParseCxt->requestId,
                             .requestObjRefId = pParseCxt->requestRid,
                             .mgmtEps = pParseCxt->mgmtEpSet};
    code = catalogChkAuth(pParseCxt->pCatalog, &conn, &authInfo, &authRes);
  }
  if (TSDB_CODE_SUCCESS == code && NULL != pCond) {
    *pCond = authRes.pCond;
  }
  return TSDB_CODE_SUCCESS == code ? (authRes.pass ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED) : code;
}

static EDealRes authSubquery(SAuthCxt* pCxt, SNode* pStmt) {
  return TSDB_CODE_SUCCESS == authQuery(pCxt, pStmt) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static int32_t mergeStableTagCond(SNode** pWhere, SNode* pTagCond) {
  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pLogicCond) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  int32_t code = nodesListMakeStrictAppend(&pLogicCond->pParameterList, pTagCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesListMakeAppend(&pLogicCond->pParameterList, *pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pWhere = (SNode*)pLogicCond;
  } else {
    nodesDestroyNode((SNode*)pLogicCond);
  }
  return code;
}

EDealRes rewriteAuthTable(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    SAuthRewriteCxt* pCxt = (SAuthRewriteCxt*)pContext;
    strcpy(pCol->tableName, pCxt->pTarget->tableName);
    strcpy(pCol->tableAlias, pCxt->pTarget->tableAlias);
  }

  return DEAL_RES_CONTINUE;
}

static int32_t rewriteAppendStableTagCond(SNode** pWhere, SNode* pTagCond, STableNode* pTable) {
  SNode* pTagCondCopy = nodesCloneNode(pTagCond);
  if (NULL == pTagCondCopy) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SAuthRewriteCxt cxt = {.pTarget = pTable};
  nodesWalkExpr(pTagCondCopy, rewriteAuthTable, &cxt);

  if (NULL == *pWhere) {
    *pWhere = pTagCondCopy;
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pWhere) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)*pWhere)->condType) {
    return nodesListStrictAppend(((SLogicConditionNode*)*pWhere)->pParameterList, pTagCondCopy);
  }

  return mergeStableTagCond(pWhere, pTagCondCopy);
}

static EDealRes authSelectImpl(SNode* pNode, void* pContext) {
  SSelectAuthCxt* pCxt = pContext;
  SAuthCxt*       pAuthCxt = pCxt->pAuthCxt;
  if (QUERY_NODE_REAL_TABLE == nodeType(pNode)) {
    SNode*      pTagCond = NULL;
    STableNode* pTable = (STableNode*)pNode;
    pAuthCxt->errCode = checkAuth(pAuthCxt, pTable->dbName, pTable->tableName, AUTH_TYPE_READ, &pTagCond);
    if (TSDB_CODE_SUCCESS == pAuthCxt->errCode && NULL != pTagCond) {
      pAuthCxt->errCode = rewriteAppendStableTagCond(&pCxt->pSelect->pWhere, pTagCond, pTable);
    }
    return TSDB_CODE_SUCCESS == pAuthCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
  } else if (QUERY_NODE_TEMP_TABLE == nodeType(pNode)) {
    return authSubquery(pAuthCxt, ((STempTableNode*)pNode)->pSubquery);
  }
  return DEAL_RES_CONTINUE;
}

static int32_t authSelect(SAuthCxt* pCxt, SSelectStmt* pSelect) {
  SSelectAuthCxt cxt = {.pAuthCxt = pCxt, .pSelect = pSelect};
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, authSelectImpl, &cxt);
  return pCxt->errCode;
}

static int32_t authSetOperator(SAuthCxt* pCxt, SSetOperator* pSetOper) {
  int32_t code = authQuery(pCxt, pSetOper->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = authQuery(pCxt, pSetOper->pRight);
  }
  return code;
}

static int32_t authDropUser(SAuthCxt* pCxt, SDropUserStmt* pStmt) {
  if (!pCxt->pParseCxt->isSuperUser || 0 == strcmp(pStmt->userName, TSDB_DEFAULT_USER)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t authDelete(SAuthCxt* pCxt, SDeleteStmt* pDelete) {
  SNode*      pTagCond = NULL;
  STableNode* pTable = (STableNode*)pDelete->pFromTable;
  int32_t     code = checkAuth(pCxt, pTable->dbName, pTable->tableName, AUTH_TYPE_WRITE, &pTagCond);
  if (TSDB_CODE_SUCCESS == code && NULL != pTagCond) {
    code = rewriteAppendStableTagCond(&pDelete->pWhere, pTagCond, pTable);
  }
  return code;
}

static int32_t authInsert(SAuthCxt* pCxt, SInsertStmt* pInsert) {
  SNode*      pTagCond = NULL;
  STableNode* pTable = (STableNode*)pInsert->pTable;
  // todo check tag condition for subtable
  int32_t code = checkAuth(pCxt, pTable->dbName, pTable->tableName, AUTH_TYPE_WRITE, &pTagCond);
  if (TSDB_CODE_SUCCESS == code) {
    code = authQuery(pCxt, pInsert->pQuery);
  }
  return code;
}

static int32_t authShowTables(SAuthCxt* pCxt, SShowStmt* pStmt) {
  return checkAuth(pCxt, ((SValueNode*)pStmt->pDbName)->literal, NULL, AUTH_TYPE_READ_OR_WRITE, NULL);
}

static int32_t authShowCreateTable(SAuthCxt* pCxt, SShowCreateTableStmt* pStmt) {
  SNode* pTagCond = NULL;
  // todo check tag condition for subtable
  return checkAuth(pCxt, pStmt->dbName, pStmt->tableName, AUTH_TYPE_READ, &pTagCond);
}

static int32_t authCreateTable(SAuthCxt* pCxt, SCreateTableStmt* pStmt) {
  SNode* pTagCond = NULL;
  // todo check tag condition for subtable
  return checkAuth(pCxt, pStmt->dbName, NULL, AUTH_TYPE_WRITE, &pTagCond);
}

static int32_t authCreateMultiTable(SAuthCxt* pCxt, SCreateMultiTablesStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pSubTables) {
    SCreateSubTableClause* pClause = (SCreateSubTableClause*)pNode;
    code = checkAuth(pCxt, pClause->dbName, NULL, AUTH_TYPE_WRITE, NULL);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t authDropTable(SAuthCxt* pCxt, SDropTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    code = checkAuth(pCxt, pClause->dbName, pClause->tableName, AUTH_TYPE_WRITE, NULL);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t authDropStable(SAuthCxt* pCxt, SDropSuperTableStmt* pStmt) {
  return checkAuth(pCxt, pStmt->dbName, pStmt->tableName, AUTH_TYPE_WRITE, NULL);
}

static int32_t authAlterTable(SAuthCxt* pCxt, SAlterTableStmt* pStmt) {
  SNode* pTagCond = NULL;
  // todo check tag condition for subtable
  return checkAuth(pCxt, pStmt->dbName, pStmt->tableName, AUTH_TYPE_WRITE, NULL);
}

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SET_OPERATOR:
      return authSetOperator(pCxt, (SSetOperator*)pStmt);
    case QUERY_NODE_SELECT_STMT:
      return authSelect(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_DROP_USER_STMT:
      return authDropUser(pCxt, (SDropUserStmt*)pStmt);
    case QUERY_NODE_DELETE_STMT:
      return authDelete(pCxt, (SDeleteStmt*)pStmt);
    case QUERY_NODE_INSERT_STMT:
      return authInsert(pCxt, (SInsertStmt*)pStmt);
    case QUERY_NODE_CREATE_TABLE_STMT:
      return authCreateTable(pCxt, (SCreateTableStmt*)pStmt);
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      return authCreateMultiTable(pCxt, (SCreateMultiTablesStmt*)pStmt);
    case QUERY_NODE_DROP_TABLE_STMT:
      return authDropTable(pCxt, (SDropTableStmt*)pStmt);
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      return authDropStable(pCxt, (SDropSuperTableStmt*)pStmt);
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT:
      return authAlterTable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
      return !pCxt->pParseCxt->enableSysInfo ? TSDB_CODE_PAR_PERMISSION_DENIED : TSDB_CODE_SUCCESS;
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
      return authShowTables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return authShowCreateTable(pCxt, (SShowCreateTableStmt*)pStmt);
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
  SAuthCxt cxt = {.pParseCxt = pParseCxt, .pMetaCache = pMetaCache, .errCode = TSDB_CODE_SUCCESS};
  return authQuery(&cxt, pQuery->pRoot);
}
