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
    code = catalogChkAuth(pParseCxt->pCatalog, pParseCxt->pTransporter, &pParseCxt->mgmtEpSet, pParseCxt->pUser,
                          dbFname, type, &pass);
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

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SET_OPERATOR:
      return authSetOperator(pCxt, (SSetOperator*)pStmt);
    case QUERY_NODE_SELECT_STMT:
      return authSelect(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_DROP_USER_STMT:
      return authDropUser(pCxt, (SDropUserStmt*)pStmt);
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery) {
  SAuthCxt cxt = {.pParseCxt = pParseCxt, .pMetaCache = pQuery->pMetaCache, .errCode = TSDB_CODE_SUCCESS};
  return authQuery(&cxt, pQuery->pRoot);
}
