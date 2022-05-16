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
  SParseContext* pParseCxt;
  int32_t        errCode;
} SAuthCxt;

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt);

static int32_t checkAuth(SParseContext* pCxt, const char* pDbName, AUTH_TYPE type) {
  if (pCxt->isSuperUser) {
    return TSDB_CODE_SUCCESS;
  }
  SName name;
  tNameSetDbName(&name, pCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
  bool    pass = false;
  int32_t code =
      catalogChkAuth(pCxt->pCatalog, pCxt->pTransporter, &pCxt->mgmtEpSet, pCxt->pUser, dbFname, type, &pass);
  return TSDB_CODE_SUCCESS == code ? (pass ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED) : code;
}

static EDealRes authSubquery(SAuthCxt* pCxt, SNode* pStmt) {
  return TSDB_CODE_SUCCESS == authQuery(pCxt, pStmt) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes authSelectImpl(SNode* pNode, void* pContext) {
  SAuthCxt* pCxt = pContext;
  if (QUERY_NODE_REAL_TABLE == nodeType(pNode)) {
    pCxt->errCode = checkAuth(pCxt->pParseCxt, ((SRealTableNode*)pNode)->table.dbName, AUTH_TYPE_READ);
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
    case QUERY_NODE_CREATE_DATABASE_STMT:
    case QUERY_NODE_DROP_DATABASE_STMT:
    case QUERY_NODE_ALTER_DATABASE_STMT:
    case QUERY_NODE_CREATE_TABLE_STMT:
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE:
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
    case QUERY_NODE_DROP_TABLE_CLAUSE:
    case QUERY_NODE_DROP_TABLE_STMT:
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_CREATE_USER_STMT:
    case QUERY_NODE_ALTER_USER_STMT:
      break;
    case QUERY_NODE_DROP_USER_STMT: {
      return authDropUser(pCxt, (SDropUserStmt*)pStmt);
    }
    case QUERY_NODE_USE_DATABASE_STMT:
    case QUERY_NODE_CREATE_DNODE_STMT:
    case QUERY_NODE_DROP_DNODE_STMT:
    case QUERY_NODE_ALTER_DNODE_STMT:
    case QUERY_NODE_CREATE_INDEX_STMT:
    case QUERY_NODE_DROP_INDEX_STMT:
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
    case QUERY_NODE_CREATE_TOPIC_STMT:
    case QUERY_NODE_DROP_TOPIC_STMT:
    case QUERY_NODE_ALTER_LOCAL_STMT:
    case QUERY_NODE_EXPLAIN_STMT:
    case QUERY_NODE_DESCRIBE_STMT:
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
    case QUERY_NODE_COMPACT_STMT:
    case QUERY_NODE_CREATE_FUNCTION_STMT:
    case QUERY_NODE_DROP_FUNCTION_STMT:
    case QUERY_NODE_CREATE_STREAM_STMT:
    case QUERY_NODE_DROP_STREAM_STMT:
    case QUERY_NODE_MERGE_VGROUP_STMT:
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
    case QUERY_NODE_SPLIT_VGROUP_STMT:
    case QUERY_NODE_SYNCDB_STMT:
    case QUERY_NODE_GRANT_STMT:
    case QUERY_NODE_REVOKE_STMT:
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
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
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
    case QUERY_NODE_KILL_CONNECTION_STMT:
    case QUERY_NODE_KILL_QUERY_STMT:
    case QUERY_NODE_KILL_TRANSACTION_STMT:
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery) {
  SAuthCxt cxt = {.pParseCxt = pParseCxt, .errCode = TSDB_CODE_SUCCESS};
  return authQuery(&cxt, pQuery->pRoot);
}
