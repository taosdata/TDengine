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

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt);

static int32_t checkAuth(SAuthCxt* pCxt, const char* pDbName, AUTH_TYPE type) {
  SParseContext* pParseCxt = pCxt->pParseCxt;
  if (pParseCxt->isSuperUser) {
    return TSDB_CODE_SUCCESS;
  }
  SName name;
  tNameSetDbName(&name, pParseCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
  int32_t code = TSDB_CODE_SUCCESS;
  bool    pass = false;
  if (NULL != pCxt->pMetaCache) {
    code = getUserAuthFromCache(pCxt->pMetaCache, pParseCxt->pUser, dbFname, type, &pass);
  } else {
    SRequestConnInfo conn = {.pTrans = pParseCxt->pTransporter,
                             .requestId = pParseCxt->requestId,
                             .requestObjRefId = pParseCxt->requestRid,
                             .mgmtEps = pParseCxt->mgmtEpSet};

    code = catalogChkAuth(pParseCxt->pCatalog, &conn, pParseCxt->pUser, dbFname, type, &pass);
  }
  return TSDB_CODE_SUCCESS == code ? (pass ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED) : code;
}

static EDealRes authSubquery(SAuthCxt* pCxt, SNode* pStmt) {
  return TSDB_CODE_SUCCESS == authQuery(pCxt, pStmt) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes authSelectImpl(SNode* pNode, void* pContext) {
  SAuthCxt* pCxt = pContext;
  if (QUERY_NODE_REAL_TABLE == nodeType(pNode)) {
    pCxt->errCode = checkAuth(pCxt, ((SRealTableNode*)pNode)->table.dbName, AUTH_TYPE_READ);
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
  } else if (QUERY_NODE_TEMP_TABLE == nodeType(pNode)) {
    return authSubquery(pCxt, ((STempTableNode*)pNode)->pSubquery);
  }
  return DEAL_RES_CONTINUE;
}

static int32_t authSelect(SAuthCxt* pCxt, SSelectStmt* pSelect) {
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, authSelectImpl, pCxt);
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
  if (!pCxt->pParseCxt->isSuperUser || 0 == strcmp(pStmt->useName, TSDB_DEFAULT_USER)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t authDelete(SAuthCxt* pCxt, SDeleteStmt* pDelete) {
  return checkAuth(pCxt, ((SRealTableNode*)pDelete->pFromTable)->table.dbName, AUTH_TYPE_WRITE);
}

static int32_t authInsert(SAuthCxt* pCxt, SInsertStmt* pInsert) {
  int32_t code = checkAuth(pCxt, ((SRealTableNode*)pInsert->pTable)->table.dbName, AUTH_TYPE_WRITE);
  if (TSDB_CODE_SUCCESS == code) {
    code = authQuery(pCxt, pInsert->pQuery);
  }
  return code;
}

static int32_t authShowTables(SAuthCxt* pCxt, SShowStmt* pStmt) {
  return checkAuth(pCxt, ((SValueNode*)pStmt->pDbName)->literal, AUTH_TYPE_READ_OR_WRITE);
}

static int32_t authShowCreateTable(SAuthCxt* pCxt, SShowCreateTableStmt* pStmt) {
  return checkAuth(pCxt, pStmt->dbName, AUTH_TYPE_READ);
}

static int32_t authCreateTable(SAuthCxt* pCxt, SCreateTableStmt* pStmt) {
  return checkAuth(pCxt, pStmt->dbName, AUTH_TYPE_WRITE);
}

static int32_t authCreateMultiTable(SAuthCxt* pCxt, SCreateMultiTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pSubTables) {
    code = checkAuth(pCxt, ((SCreateSubTableClause*)pNode)->dbName, AUTH_TYPE_WRITE);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
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
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      return authCreateMultiTable(pCxt, (SCreateMultiTableStmt*)pStmt);
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
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
