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
#include "tconfig.h"

typedef struct SAuthCxt {
  SParseContext*   pParseCxt;
  SParseMetaCache* pMetaCache;
  int32_t          errCode;
  bool             macNruGuaranteed;
  bool             macNwdGuaranteed;
} SAuthCxt;

typedef struct SSelectAuthCxt {
  SAuthCxt*    pAuthCxt;
  SSelectStmt* pSelect;
} SSelectAuthCxt;

typedef struct SAuthRewriteCxt {
  STableNode* pTarget;
} SAuthRewriteCxt;

extern SConfig* tsCfg;

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt);

#ifdef TD_ENTERPRISE
static int32_t macCheckBySecLvl(SAuthCxt* pCxt, int8_t secLvl, bool checkNWD) {
  if (secLvl < 0) {
    // Unset security level: not subject to MAC enforcement
    return TSDB_CODE_SUCCESS;
  }

  if (!pCxt->macNruGuaranteed && pCxt->pParseCxt->maxSecLevel < secLvl) {
    return TSDB_CODE_MAC_INSUFFICIENT_LEVEL;  // NRU violation
  }

  if (checkNWD && !pCxt->macNwdGuaranteed && pCxt->pParseCxt->minSecLevel > secLvl) {
    return TSDB_CODE_MAC_NO_WRITE_DOWN;  // NWD violation
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Lightweight MAC check for table-level operations.
 *
 * Uses a 3-layer fast-path strategy to avoid expensive metadata fetches
 * in the common case where MAC is not actively used:
 *   Layer 1: User-level — if user's security range covers all levels, skip entirely.
 *   Layer 2: (DB-level — handled in checkAuthByOwner for PRIV_DB_USE, zero extra cost.)
 *   Layer 3: Table-level — fetch table meta from cache only when needed.
 *
 * @param pCxt      Auth context
 * @param dbName    Database name
 * @param tableName Table name
 * @param checkNWD  true for INSERT (needs No-Write-Down), false for SELECT/DELETE (NRU only)
 * @return TSDB_CODE_SUCCESS or TSDB_CODE_MAC_INSUFFICIENT_LEVEL
 */
static int32_t macCheckTableAccess(SAuthCxt* pCxt, const char* dbName, const char* tableName, bool checkNWD) {
  SParseContext* pParseCxt = pCxt->pParseCxt;

  // Fast-path: MAC not yet activated cluster-wide — skip all checks
  if (!pParseCxt->macMode) {
    return TSDB_CODE_SUCCESS;
  }

  // Layer 1: User-level fast-path — skip if user's security range guarantees MAC pass
  if (pCxt->macNruGuaranteed && (!checkNWD || pCxt->macNwdGuaranteed)) {
    return TSDB_CODE_SUCCESS;
  }

  // Layer 3: Table-level — fetch secLvl from metadata cache
  SName name = {0};
  toName(pParseCxt->acctId, dbName, tableName, &name);
  STableMeta* pTableMeta = NULL;
  int32_t     code = getTargetMetaImpl(pParseCxt, pCxt->pMetaCache, &name, &pTableMeta, true);
  if (TSDB_CODE_SUCCESS == code && pTableMeta != NULL) {
    int8_t secLvl = pTableMeta->secLvl;
    taosMemoryFree(pTableMeta);
    return macCheckBySecLvl(pCxt, secLvl, checkNWD);
  }
  return TSDB_CODE_SUCCESS;
}
#endif

static int32_t setUserAuthInfo(SParseContext* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                               EPrivObjType objType, bool isView, bool effective, SUserAuthInfo* pAuth) {
  if (effective) {
    snprintf(pAuth->user, sizeof(pAuth->user), "%s", pCxt->pEffectiveUser ? pCxt->pEffectiveUser : "");
    pAuth->userId = pCxt->effectiveUserId;  // TODO: assign the effective user id
  } else {
    snprintf(pAuth->user, sizeof(pAuth->user), "%s", pCxt->pUser);
    pAuth->userId = pCxt->userId;
  }

  if (NULL == pTabName) {
    if (pDbName) {
      int32_t code = tNameSetDbName(&pAuth->tbName, pCxt->acctId, pDbName, strlen(pDbName));
      if (TSDB_CODE_SUCCESS != code) return code;
    } else {
      pAuth->tbName.acctId = pCxt->acctId;
      pAuth->tbName.type = TSDB_SYS_NAME_T;
    }
  } else {
    toName(pCxt->acctId, pDbName, pTabName, &pAuth->tbName);
  }
  pAuth->privType = privType;
  pAuth->objType = objType;
  pAuth->isView = isView;
  return TSDB_CODE_SUCCESS;
}

static int32_t checkAuthByOwner(SAuthCxt* pCxt, SUserAuthInfo* pAuthInfo, SUserAuthRes* pAuthRes, bool *recheck) {
  SParseContext*   pParseCxt = pCxt->pParseCxt;
  const SPrivInfo* pPrivInfo = privInfoGet(pAuthInfo->privType);
  if (NULL == pPrivInfo) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }
  int32_t code = 0;
  if (pPrivInfo->category == PRIV_CATEGORY_OBJECT || pAuthInfo->objType == PRIV_OBJ_DB) {
    SPrivInfo privInfoDup = *pPrivInfo;
    if (privInfoDup.objType <= 0) privInfoDup.objType = PRIV_OBJ_DB;
    switch (privInfoDup.objType) {
      case PRIV_OBJ_DB: {
        SDbCfgInfo dbCfgInfo = {0};
        char       dbFName[TSDB_DB_FNAME_LEN] = {0};
        (void)tNameGetFullDbName(&pAuthInfo->tbName, dbFName);
        code = getDbCfgFromCache(pCxt->pMetaCache, dbFName, &dbCfgInfo);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
#ifdef TD_ENTERPRISE
        // Layer 2 MAC: DB-level NRU check — piggybacked on already-fetched SDbCfgInfo
        // Only for DB_USE to avoid blocking admin ops (ALTER/DROP by SYSSEC)
        // Skip when MAC is not yet activated cluster-wide
        if (pParseCxt->macMode && pAuthInfo->privType == PRIV_DB_USE && dbCfgInfo.securityLevel > 0 &&
            pParseCxt->maxSecLevel < (int8_t)dbCfgInfo.securityLevel) {
          pAuthRes->pass[pAuthInfo->isView ? AUTH_RES_VIEW : AUTH_RES_BASIC] = false;
          return TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
        }
#endif
        // rewrite privilege for audit db
        if (dbCfgInfo.isAudit && pAuthInfo->objType == PRIV_OBJ_DB) {
          if (pAuthInfo->privType == PRIV_DB_USE) {
            pAuthInfo->useDb = AUTH_OWNED_MASK;
            if (recheck) *recheck = true;  // recheck since the cached key is changed
          } else if (pAuthInfo->privType == PRIV_CM_ALTER) {
            pAuthInfo->privType = PRIV_AUDIT_DB_ALTER;
            pAuthInfo->objType = PRIV_OBJ_CLUSTER;
            if (recheck) *recheck = true;  // recheck since the cached key is changed
          } else if (pAuthInfo->privType == PRIV_CM_DROP) {
            pAuthInfo->privType = PRIV_AUDIT_DB_DROP;
            pAuthInfo->objType = PRIV_OBJ_CLUSTER;
            if (recheck) *recheck = true;  // recheck since the cached key is changed
          } else if (pAuthInfo->privType == PRIV_TBL_CREATE) {
            pAuthInfo->privType = PRIV_AUDIT_TBL_CREATE;
            pAuthInfo->objType = PRIV_OBJ_CLUSTER;
            if (recheck) *recheck = true;  // recheck since the cached key is changed
          }
          return TSDB_CODE_SUCCESS;
        }
        if (dbCfgInfo.ownerId == pAuthInfo->userId) {
          pAuthRes->pass[pAuthInfo->isView ? AUTH_RES_VIEW : AUTH_RES_BASIC] = true;
          return TSDB_CODE_SUCCESS;
        }
        break;
      }
      default:
        return TSDB_CODE_SUCCESS;
    }
  }
_exit:
  return TSDB_CODE_SUCCESS;
}

static int32_t checkAuthImpl(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                             EPrivObjType objType, SNode** pCond, SArray** pPrivCols, bool isView, bool effective) {
  SParseContext* pParseCxt = pCxt->pParseCxt;
  if (pParseCxt->isSuperUser) {
    return TSDB_CODE_SUCCESS;
  }

  AUTH_RES_TYPE auth_res_type = isView ? AUTH_RES_VIEW : AUTH_RES_BASIC;
  SUserAuthInfo authInfo = {0};
  int32_t code = setUserAuthInfo(pCxt->pParseCxt, pDbName, pTabName, privType, objType, isView, effective, &authInfo);
  if (TSDB_CODE_SUCCESS != code) return code;
  SUserAuthRes authRes = {0};
  bool         recheck = false;
  if (NULL != pCxt->pMetaCache && privType != PRIV_VIEW_SELECT && privType != PRIV_AUDIT_TBL_SELECT) {
    code = checkAuthByOwner(pCxt, &authInfo, &authRes, &recheck);
#ifdef TD_ENTERPRISE
    // MAC enforcement: do NOT let DAC grants override a MAC NRU/NWD block
    if (code == TSDB_CODE_MAC_INSUFFICIENT_LEVEL || code == TSDB_CODE_MAC_NO_WRITE_DOWN) {
      return code;
    }
#endif
    if (code == TSDB_CODE_SUCCESS && authRes.pass[auth_res_type]) {
      goto _exit;
    }
    code = getUserAuthFromCache(pCxt->pMetaCache, &authInfo, &authRes);
#ifdef TD_ENTERPRISE
    if (isView && TSDB_CODE_PAR_INTERNAL_ERROR == code) {
      authInfo.isView = false;
      code = getUserAuthFromCache(pCxt->pMetaCache, &authInfo, &authRes);
    }
#endif
  } else {
    recheck = true;  // recheck since the cached key is changed
  }
  if (recheck) {  // the priv type of view and audit may be rewritten, need to recheck from catalog
    SRequestConnInfo conn = {.pTrans = pParseCxt->pTransporter,
                             .requestId = pParseCxt->requestId,
                             .requestObjRefId = pParseCxt->requestRid,
                             .mgmtEps = pParseCxt->mgmtEpSet};
    code = catalogChkAuth(pParseCxt->pCatalog, &conn, &authInfo, &authRes);
  }

_exit:
  if (TSDB_CODE_SUCCESS == code) {
    if (pCond) *pCond = authRes.pCond[auth_res_type];
    if (pPrivCols) *pPrivCols = authRes.pCols;
    if (taosArrayGetSize(authRes.pCols) > 0) {
      pCxt->pParseCxt->hasPrivCols = 1; // used later in translateCheckPrivCols for select *
    }
  }
  return TSDB_CODE_SUCCESS == code ? (authRes.pass[auth_res_type] ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_PERMISSION_DENIED)
                                   : code;
}

static int32_t checkAuth(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                         EPrivObjType objType, SNode** pCond, SArray** pPrivCols) {
#ifdef TD_ENTERPRISE
  return checkAuthImpl(pCxt, pDbName, pTabName, privType, objType, pCond, pPrivCols, false, false);
#else
  return TSDB_CODE_SUCCESS;
#endif
}

static int32_t authSysPrivileges(SAuthCxt* pCxt, SNode* pStmt, EPrivType type) {
  return checkAuth(pCxt, NULL, NULL, type, 0, NULL, NULL);
}

static int32_t authObjPrivileges(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                                 EPrivObjType objType) {
  if (!pDbName) {
    return TSDB_CODE_PAR_INTERNAL_ERROR;
  }

  return checkAuth(pCxt, pDbName, pTabName, privType, objType, NULL, NULL);
}

// Checks DB_USE privilege and maps non-MAC errors to PAR_DB_USE_PERMISSION_DENIED.
// Callers use PAR_ERR_RET(checkDbUseAuth(pCxt, dbName)) for the common pattern.
static int32_t checkDbUseAuth(SAuthCxt* pCxt, const char* pDbName) {
  int32_t code = checkAuth(pCxt, pDbName, NULL, PRIV_DB_USE, PRIV_OBJ_DB, NULL, NULL);
#ifdef TD_ENTERPRISE
  if (code == TSDB_CODE_MAC_INSUFFICIENT_LEVEL || code == TSDB_CODE_MAC_NO_WRITE_DOWN) return code;
#endif
  return code == TSDB_CODE_SUCCESS ? TSDB_CODE_SUCCESS : TSDB_CODE_PAR_DB_USE_PERMISSION_DENIED;
}

static int32_t checkEffectiveAuth(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                                  EPrivObjType objType, SNode** pCond) {
  return checkAuthImpl(pCxt, pDbName, pTabName, privType, objType, NULL, NULL, false, true);
}

static int32_t checkViewAuth(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                             EPrivObjType objType, SNode** pCond) {
  return checkAuthImpl(pCxt, pDbName, pTabName, privType, objType, pCond, NULL, true, false);
}

static int32_t checkViewEffectiveAuth(SAuthCxt* pCxt, const char* pDbName, const char* pTabName, EPrivType privType,
                                      EPrivObjType objType, SNode** pCond) {
  return checkAuthImpl(pCxt, pDbName, pTabName, privType, objType, pCond, NULL, true, true);
}

static EDealRes authSubquery(SAuthCxt* pCxt, SNode* pStmt) {
  return TSDB_CODE_SUCCESS == authQuery(pCxt, pStmt) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static int32_t mergeStableTagCond(SNode** pWhere, SNode* pTagCond) {
  SLogicConditionNode* pLogicCond = NULL;
  int32_t              code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond);
  if (NULL == pLogicCond) {
    return code;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  code = nodesListMakeStrictAppend(&pLogicCond->pParameterList, pTagCond);
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
    SColumnNode*     pCol = (SColumnNode*)pNode;
    SAuthRewriteCxt* pCxt = (SAuthRewriteCxt*)pContext;
    tstrncpy(pCol->tableName, pCxt->pTarget->tableName, TSDB_TABLE_NAME_LEN);
    tstrncpy(pCol->tableAlias, pCxt->pTarget->tableAlias, TSDB_TABLE_NAME_LEN);
    pCol->appendByPrivCond = 1;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t rewriteAppendStableTagCond(SNode** pWhere, SNode* pTagCond, STableNode* pTable) {
  SNode*  pTagCondCopy = NULL;
  int32_t code = nodesCloneNode(pTagCond, &pTagCondCopy);
  if (NULL == pTagCondCopy) {
    return code;
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
#if 0
/**
 * @brief Fast fail path if no star(*) specified in select clause
 */
static int32_t authSelectTblCols(SSelectStmt* pSelect, STableNode* pTable, SArray* pPrivCols) {
  int32_t    code = 0;
  SNodeList* pRetrievedCols = NULL;
  int32_t    nCols = taosArrayGetSize(pPrivCols);

  if (nCols <= 0) {
    goto _return;
  }

  PAR_ERR_JRET(nodesCollectColumns(pSelect, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pRetrievedCols));

  int32_t i = 0, j = 0, k = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pRetrievedCols) {
    SColumnNode* pColNode = (SColumnNode*)pNode;

    j = i;

    // search in the remaining columns first for better performance if ordered
    bool found = false;
    for (; i < nCols; ++i) {
      SColNameFlag* pColNameFlag = (SColNameFlag*)TARRAY_GET_ELEM(pPrivCols, i);
      if (strcmp(pColNode->colName, pColNameFlag->colName) == 0) {
        found = true;
        ++i;
        break;
      }
    }
    if (!found) {
      for (k = 0; k < j; ++k) {
        SColNameFlag* pColNameFlag = (SColNameFlag*)TARRAY_GET_ELEM(pPrivCols, k);
        if (strcmp(pColNode->colName, pColNameFlag->colName) == 0) {
          found = true;
          break;
        }
      }
    }
    if (!found) {
      code = TSDB_CODE_PAR_COL_PERMISSION_DENIED;
      goto _return;
    }
  }
_return:
  nodesDestroyList(pRetrievedCols);
  return code;
}
#endif

static EDealRes authSelectImpl(SNode* pNode, void* pContext) {
  SSelectAuthCxt* pCxt = pContext;
  SAuthCxt*       pAuthCxt = pCxt->pAuthCxt;
  bool            isView = false;
  bool            isAudit = false;
  if (QUERY_NODE_REAL_TABLE == nodeType(pNode)) {
    SNode*      pTagCond = NULL;
    // SArray*     pPrivCols = NULL;
    STableNode* pTable = (STableNode*)pNode;
    if ((pAuthCxt->pParseCxt->enableSysInfo == 0) && IS_INFORMATION_SCHEMA_DB(pTable->dbName) &&
        (strcmp(pTable->tableName, TSDB_INS_TABLE_VGROUPS) == 0)) {
      pAuthCxt->errCode = TSDB_CODE_PAR_PERMISSION_DENIED;
      return DEAL_RES_ERROR;
    }
    int32_t dbUseCode = checkDbUseAuth(pAuthCxt, pTable->dbName);
    if (dbUseCode != TSDB_CODE_SUCCESS) {
      pAuthCxt->errCode = dbUseCode;
      return DEAL_RES_ERROR;
    }
#ifdef TD_ENTERPRISE
    SName name = {0};
    toName(pAuthCxt->pParseCxt->acctId, pTable->dbName, pTable->tableName, &name);
    STableMeta* pTableMeta = NULL;
    int32_t code = getTargetMetaImpl(pAuthCxt->pParseCxt, pAuthCxt->pMetaCache, &name, &pTableMeta, true);
    if (TSDB_CODE_SUCCESS == code) {
      // MAC NRU: user.maxSecLevel must be >= table.securityLevel for SELECT.
      // Reuse secLvl from this already-fetched table meta to avoid extra metadata round-trips.
      if (pAuthCxt->pParseCxt->macMode && macCheckBySecLvl(pAuthCxt, pTableMeta->secLvl, false) != TSDB_CODE_SUCCESS) {
        taosMemoryFree(pTableMeta);
        pAuthCxt->errCode = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
        return DEAL_RES_ERROR;
      }
      if (pTableMeta->isAudit) {
        isAudit = true;
      } else if (!pTableMeta->isAudit && (pTableMeta->ownerId == pAuthCxt->pParseCxt->userId)) {
        // owner has all privileges on the table he owns except audit table
        taosMemoryFree(pTableMeta);
        return DEAL_RES_CONTINUE;
      }
      if (TSDB_VIEW_TABLE == pTableMeta->tableType) {
        isView = true;
      }
    }
    taosMemoryFree(pTableMeta);
#endif
    if (!isView) {
      pAuthCxt->errCode =
          checkAuth(pAuthCxt, pTable->dbName, pTable->tableName, isAudit ? PRIV_AUDIT_TBL_SELECT : PRIV_TBL_SELECT,
                    PRIV_OBJ_TBL, &pTagCond, NULL);  //&pPrivCols);
      if (TSDB_CODE_SUCCESS != pAuthCxt->errCode && NULL != pAuthCxt->pParseCxt->pEffectiveUser) {
        pAuthCxt->errCode = checkEffectiveAuth(pAuthCxt, pTable->dbName, pTable->tableName,
                                               isAudit ? PRIV_AUDIT_TBL_SELECT : PRIV_TBL_SELECT, PRIV_OBJ_TBL, NULL);
      }
#if 0
      if (TSDB_CODE_SUCCESS == pAuthCxt->errCode && NULL != pPrivCols) {
        pAuthCxt->errCode = authSelectTblCols(pCxt->pSelect, pTable, pPrivCols);
      }
#endif
      if (TSDB_CODE_SUCCESS == pAuthCxt->errCode && NULL != pTagCond) {
        pAuthCxt->errCode = rewriteAppendStableTagCond(&pCxt->pSelect->pWhere, pTagCond, pTable);
      }
    } else {
      pAuthCxt->errCode =
          checkViewAuth(pAuthCxt, pTable->dbName, pTable->tableName, PRIV_VIEW_SELECT, PRIV_OBJ_VIEW, NULL);
      if (TSDB_CODE_SUCCESS != pAuthCxt->errCode && NULL != pAuthCxt->pParseCxt->pEffectiveUser) {
        pAuthCxt->errCode =
            checkViewEffectiveAuth(pAuthCxt, pTable->dbName, pTable->tableName, PRIV_VIEW_SELECT, PRIV_OBJ_VIEW, NULL);
      }
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
  // if (!pCxt->pParseCxt->isSuperUser || 0 == strcmp(pStmt->userName, TSDB_DEFAULT_USER)) {
  //   return TSDB_CODE_PAR_PERMISSION_DENIED;
  // }
  if (0 == strcmp(pStmt->userName, TSDB_DEFAULT_USER)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  return authSysPrivileges(pCxt, (void*)pStmt, PRIV_USER_DROP);  // root has SYSDBA role with USER_DROP privilege
}

static int32_t authDelete(SAuthCxt* pCxt, SDeleteStmt* pDelete) {
  SNode*      pTagCond = NULL;
  STableNode* pTable = (STableNode*)pDelete->pFromTable;
  int32_t     code = checkDbUseAuth(pCxt, pTable->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAuth(pCxt, pTable->dbName, pTable->tableName, PRIV_TBL_DELETE, PRIV_OBJ_TBL, &pTagCond, NULL);
  }
#ifdef TD_ENTERPRISE
  // MAC clearance check: user.maxSecLevel must be >= table.secLvl for DELETE
  if (TSDB_CODE_SUCCESS == code) {
    code = macCheckTableAccess(pCxt, pTable->dbName, pTable->tableName, false);
  }
#endif
  if (TSDB_CODE_SUCCESS == code && NULL != pTagCond) {
    code = rewriteAppendStableTagCond(&pDelete->pWhere, pTagCond, pTable);
  }
  return code;
}

static int32_t authInsert(SAuthCxt* pCxt, SInsertStmt* pInsert) {
  SNode*      pTagCond = NULL;
  SArray*     pPrivCols = NULL;
  STableNode* pTable = (STableNode*)pInsert->pTable;
  // todo check tag condition for subtable
  int32_t code = checkDbUseAuth(pCxt, pTable->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAuth(pCxt, pTable->dbName, pTable->tableName, PRIV_TBL_INSERT, PRIV_OBJ_TBL, &pTagCond, &pPrivCols);
  }
#ifdef TD_ENTERPRISE
  // MAC NWD+NRU: for INSERT, user.minSecLevel <= table.secLvl <= user.maxSecLevel
  if (TSDB_CODE_SUCCESS == code) {
    code = macCheckTableAccess(pCxt, pTable->dbName, pTable->tableName, true);
  }
#endif
  return code;
}

static int32_t authShowTables(SAuthCxt* pCxt, SShowStmt* pStmt) {
  // return checkAuth(pCxt, ((SValueNode*)pStmt->pDbName)->literal, NULL, AUTH_TYPE_READ_OR_WRITE, NULL);
  // stb: more check in server, child table(TODO): more check when filter query result
  PAR_ERR_RET(checkDbUseAuth(pCxt, ((SValueNode*)pStmt->pDbName)->literal));
  return 0;
}

static int32_t authShowVtables(SAuthCxt* pCxt, SShowStmt* pStmt) { return authShowTables(pCxt, pStmt); }

static int32_t authShowUsage(SAuthCxt* pCxt, SShowStmt* pStmt) {
  PAR_ERR_RET(checkDbUseAuth(pCxt, ((SValueNode*)pStmt->pDbName)->literal));
  return 0;
}

static int32_t authShowCreateTable(SAuthCxt* pCxt, SShowCreateTableStmt* pStmt) {
  // SNode* pTagCond = NULL;
  // todo check tag condition for subtable
  // return checkAuth(pCxt, pStmt->dbName, pStmt->tableName, AUTH_TYPE_READ, &pTagCond);
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
#ifdef TD_ENTERPRISE
  // MAC NRU: table-level check — user.maxSecLevel must be >= table.securityLevel
  PAR_ERR_RET(macCheckTableAccess(pCxt, pStmt->dbName, pStmt->tableName, false));
#endif
  return authObjPrivileges(pCxt, pStmt->dbName, pStmt->tableName, PRIV_CM_SHOW_CREATE, PRIV_OBJ_TBL);
}

static int32_t authShowCreateView(SAuthCxt* pCxt, SShowCreateViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  int32_t code = checkDbUseAuth(pCxt, ((SShowCreateViewStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkViewAuth(pCxt, ((SShowCreateViewStmt*)pStmt)->dbName, ((SShowCreateViewStmt*)pStmt)->viewName,
                         PRIV_CM_SHOW_CREATE, PRIV_OBJ_VIEW, NULL);
  }
  if (code == 0) pStmt->hasPrivilege = true;
  return 0;  // return 0 and check owner later in translateShowCreateView
#endif
}

static int32_t authCreateTable(SAuthCxt* pCxt, SCreateTableStmt* pStmt) {
  // SNode* pTagCond = NULL;
  // todo check tag condition for subtable
  // return checkAuth(pCxt, pStmt->dbName, NULL, AUTH_TYPE_WRITE, &pTagCond);
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  int32_t code = authObjPrivileges(pCxt, pStmt->dbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB);
#ifdef TD_ENTERPRISE
  // Per FS §4.2.1.4: specifying SECURITY_LEVEL > 0 requires PRIV_SECURITY_POLICY_ALTER regardless
  // of MAC activation state. SECURITY_LEVEL = 0 is equivalent to the default and always allowed.
  if (TSDB_CODE_SUCCESS == code) {
    if (pStmt->pOptions && pStmt->pOptions->securityLevel > 0) {
      if (pCxt->pParseCxt->macMode && pCxt->pParseCxt->maxSecLevel < pStmt->pOptions->securityLevel) {
        code = TSDB_CODE_MAC_INSUFFICIENT_LEVEL;
      } else {
        code = authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_SECURITY_POLICY_ALTER);
      }
    }
  }
#endif
  return code;
}

static int32_t authCreateVTable(SAuthCxt* pCxt, SCreateVTableStmt* pStmt) {
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  PAR_ERR_RET(authObjPrivileges(pCxt, pStmt->dbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB));
  SNode* pCol = NULL;
  FOREACH(pCol, pStmt->pCols) {
    SColumnDefNode* pColDef = (SColumnDefNode*)pCol;
    if (NULL == pColDef) {
      PAR_ERR_RET(TSDB_CODE_PAR_INVALID_COLUMN);
    }
    SColumnOptions* pOptions = (SColumnOptions*)pColDef->pOptions;
    if (pOptions && pOptions->hasRef) {
      if (authObjPrivileges(pCxt, pOptions->refDb, pOptions->refTable, PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
        return TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t authCreateVSubTable(SAuthCxt* pCxt, SCreateVSubTableStmt* pStmt) {
  SNode*     pNode = NULL;
  SNodeList* pTmpList = pStmt->pSpecificColRefs ? pStmt->pSpecificColRefs : pStmt->pColRefs;
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  PAR_ERR_RET(authObjPrivileges(pCxt, pStmt->dbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB));
  if (NULL == pTmpList) {
    // no column reference
    return TSDB_CODE_SUCCESS;
  }

  FOREACH(pNode, pTmpList) {
    SColumnRefNode* pColRef = (SColumnRefNode*)pNode;
    if (NULL == pColRef) {
      PAR_ERR_RET(TSDB_CODE_PAR_INVALID_COLUMN);
    }
    if (authObjPrivileges(pCxt, pColRef->refDbName, pColRef->refTableName, PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
      return TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t authCreateStream(SAuthCxt* pCxt, SCreateStreamStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_SYS_DBNAME(pStmt->streamDbName)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  if (IS_SYS_DBNAME(pStmt->targetDbName)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  if (pStmt->pTrigger) {
    SStreamTriggerNode* pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
    STableNode*         pTriggerTable = (STableNode*)pTrigger->pTrigerTable;
    if (pTriggerTable) {
      if (IS_SYS_DBNAME(pTriggerTable->dbName)) return TSDB_CODE_PAR_PERMISSION_DENIED;
      if (authObjPrivileges(pCxt, pTriggerTable->dbName, pTriggerTable->tableName, PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
        return TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
      }
      PAR_ERR_RET(checkDbUseAuth(pCxt, pTriggerTable->dbName));
    }
  }

  PAR_ERR_RET(checkDbUseAuth(pCxt, ((SCreateStreamStmt*)pStmt)->streamDbName));
  PAR_ERR_RET(
      authObjPrivileges(pCxt, ((SCreateStreamStmt*)pStmt)->streamDbName, NULL, PRIV_STREAM_CREATE, PRIV_OBJ_DB));
  if (pStmt->targetDbName[0] != '\0') {
    PAR_ERR_RET(checkDbUseAuth(pCxt, ((SCreateStreamStmt*)pStmt)->targetDbName));
    if (authObjPrivileges(pCxt, ((SCreateStreamStmt*)pStmt)->targetDbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB)) {
      return TSDB_CODE_PAR_TB_CREATE_PERMISSION_DENIED;
    }
  }
  if (pStmt->pQuery) {
    PAR_ERR_RET(authQuery(pCxt, pStmt->pQuery));
  }
  return code;
}

static int32_t authCreateTopic(SAuthCxt* pCxt, SCreateTopicStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_SYS_DBNAME(pStmt->subDbName)) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  if (NULL != pStmt->pQuery) {
    PAR_ERR_RET(authQuery(pCxt, pStmt->pQuery));
  }
  if (NULL != pStmt->pWhere) {
    if (authObjPrivileges(pCxt, ((SCreateTopicStmt*)pStmt)->subDbName, ((SCreateTopicStmt*)pStmt)->subSTbName,
                          PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
      return TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }
  if (((SCreateTopicStmt*)pStmt)->subDbName[0] != '\0') {
    PAR_ERR_RET(checkDbUseAuth(pCxt, ((SCreateTopicStmt*)pStmt)->subDbName));
  }

  return code;
}

static int32_t authCreateMultiTable(SAuthCxt* pCxt, SCreateMultiTablesStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pStmt->pSubTables) {
    if (pNode->type == QUERY_NODE_CREATE_SUBTABLE_CLAUSE) {
      SCreateSubTableClause* pClause = (SCreateSubTableClause*)pNode;
      code = checkDbUseAuth(pCxt, pClause->dbName);
      if (TSDB_CODE_SUCCESS != code) break;
      code = authObjPrivileges(pCxt, pClause->dbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    } else {
      SCreateSubTableFromFileClause* pClause = (SCreateSubTableFromFileClause*)pNode;
      code = checkDbUseAuth(pCxt, pClause->useDbName);
      if (TSDB_CODE_SUCCESS != code) break;
      code = authObjPrivileges(pCxt, pClause->useDbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }
  return code;
}

static int32_t authDropTable(SAuthCxt* pCxt, SDropTableStmt* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pStmt->withOpt && !pCxt->pParseCxt->isSuperUser) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pStmt->pTables) {
    SDropTableClause* pClause = (SDropTableClause*)pNode;
    code = checkDbUseAuth(pCxt, pClause->dbName);
    if (TSDB_CODE_SUCCESS != code) break;

    if (!pStmt->withOpt) {
      // for child table, check privileges of its super table later
      if (checkAuth(pCxt, pClause->dbName, pClause->tableName, PRIV_CM_DROP, PRIV_OBJ_TBL, NULL, NULL)) {
        code = TSDB_CODE_PAR_PERMISSION_DENIED;
        break;
      }
    }
  }

  return code;
}

static int32_t authDropStable(SAuthCxt* pCxt, SDropSuperTableStmt* pStmt) {
  if (pStmt->withOpt && !pCxt->pParseCxt->isSuperUser) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  if (!pStmt->withOpt) {
    PAR_ERR_RET(checkAuth(pCxt, pStmt->dbName, pStmt->tableName, PRIV_CM_DROP, PRIV_OBJ_TBL, NULL, NULL));
  }
  return 0;
}

static int32_t authDropVtable(SAuthCxt* pCxt, SDropVirtualTableStmt* pStmt) {
  if (pStmt->withOpt && !pCxt->pParseCxt->isSuperUser) {
    return TSDB_CODE_PAR_PERMISSION_DENIED;
  }
  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  if (!pStmt->withOpt) {
    PAR_ERR_RET(checkAuth(pCxt, pStmt->dbName, pStmt->tableName, PRIV_CM_DROP, PRIV_OBJ_TBL, NULL, NULL));
  }
  return 0;
}

static int32_t authAlterTable(SAuthCxt* pCxt, SAlterTableStmt* pStmt) {
  // TODO: if alterType is TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL, the tables to
  // change tag value are child tables but we only have the super table name here.
  // the auth logic below haven't handled this case, but as this case is only for internal
  // use and not exposed to users, we can live with this for now and improve it later if needed.

  if (pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
    int32_t code = 0;
    SNode* pTableNode = NULL;
    FOREACH(pTableNode, pStmt->pList) {
      SAlterTableUpdateTagValClause* pClause = (SAlterTableUpdateTagValClause*)pTableNode;
      PAR_ERR_RET(checkDbUseAuth(pCxt, pClause->dbName));
      code = checkAuth(pCxt, pClause->dbName, pClause->tableName, PRIV_CM_ALTER, PRIV_OBJ_TBL, NULL, NULL);
#ifdef TD_ENTERPRISE
      // MAC clearance check: child table inherits secLvl from STB; user clearance must dominate object level
      if (TSDB_CODE_SUCCESS == code) {
        code = macCheckTableAccess(pCxt, pClause->dbName, pClause->tableName, false);
      }
#endif
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
    return code;
  } else {
    // todo check tag condition for subtable
#ifdef TD_ENTERPRISE
    // MAC domain: security_level changes require only PRIV_SECURITY_POLICY_ALTER (no CM_ALTER needed)
    if (pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_OPTIONS && pStmt->pOptions && pStmt->pOptions->securityLevel >= 0) {
      // Trusted subject: PRIV_SECURITY_POLICY_ALTER holder is exempt from maxSecLevel constraint
      return authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_SECURITY_POLICY_ALTER);
    }
#endif
    // DAC domain: non-security ALTER requires DB_USE + CM_ALTER + MAC clearance
    PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
    int32_t code = checkAuth(pCxt, pStmt->dbName, pStmt->tableName, PRIV_CM_ALTER, PRIV_OBJ_TBL, NULL, NULL);
#ifdef TD_ENTERPRISE
    if (TSDB_CODE_SUCCESS == code) {
      // MAC clearance check: secLvl inherited from STB for child tables
      code = macCheckTableAccess(pCxt, pStmt->dbName, pStmt->tableName, false);
    }
#endif
    return code;
  }
}

static int32_t authAlterVTable(SAuthCxt* pCxt, SAlterTableStmt* pStmt) {
  // TODO: if alterType is TSDB_ALTER_TABLE_UPDATE_CHILD_TABLE_TAG_VAL, the tables to
  // change tag value are child tables but we only have the super table name here.
  // the auth logic below haven't handled this case, but as this case is only for internal
  // use and not exposed to users, we can live with this for now and improve it later if needed.

  if (pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_MULTI_TABLE_TAG_VAL) {
    int32_t code = 0;
    SNode* pTableNode = NULL;
    FOREACH(pTableNode, pStmt->pList) {
      SAlterTableUpdateTagValClause* pClause = (SAlterTableUpdateTagValClause*)pTableNode;
      PAR_ERR_RET(checkDbUseAuth(pCxt, pClause->dbName));
      code = checkAuth(pCxt, pClause->dbName, pClause->tableName, PRIV_CM_ALTER, PRIV_OBJ_TBL, NULL, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
    PAR_RET(code);
  }

  PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->dbName));
  PAR_ERR_RET(checkAuth(pCxt, pStmt->dbName, pStmt->tableName, PRIV_CM_ALTER, PRIV_OBJ_TBL, NULL, NULL));
  if (pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF ||
      pStmt->alterType == TSDB_ALTER_TABLE_ALTER_COLUMN_REF) {
    PAR_ERR_RET(checkDbUseAuth(pCxt, pStmt->refDbName));
    if (checkAuth(pCxt, pStmt->refDbName, pStmt->refTableName, PRIV_TBL_SELECT, PRIV_OBJ_TBL, NULL, NULL)) {
      return TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }
  PAR_RET(TSDB_CODE_SUCCESS);
}

static int32_t authCreateView(SAuthCxt* pCxt, SCreateViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  int32_t code = checkDbUseAuth(pCxt, pStmt->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAuth(pCxt, pStmt->dbName, NULL, PRIV_VIEW_CREATE, PRIV_OBJ_DB, NULL, NULL);
    if (code != TSDB_CODE_SUCCESS && pStmt->orReplace) {
      code = checkAuth(pCxt, pStmt->dbName, pStmt->viewName, PRIV_CM_ALTER, PRIV_OBJ_VIEW, NULL, NULL);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    if ((code = authQuery(pCxt, pStmt->pQuery))) {
      if (code == TSDB_CODE_PAR_PERMISSION_DENIED) code = TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }
  return code;
#endif
}

static int32_t authDropView(SAuthCxt* pCxt, SDropViewStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  int32_t code = checkDbUseAuth(pCxt, pStmt->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkViewAuth(pCxt, pStmt->dbName, pStmt->viewName, PRIV_CM_DROP, PRIV_OBJ_VIEW, NULL);
  }
  if (code == 0) {
    pStmt->hasPrivilege = true;
  } else {
    code = 0;  // check owner in parTranslater
  }
  return code;
#endif
}

static int32_t authCreateIndex(SAuthCxt* pCxt, SCreateIndexStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SCreateIndexStmt*)pStmt)->dbName);

  if (TSDB_CODE_SUCCESS == code) {
    if (authObjPrivileges(pCxt, ((SCreateIndexStmt*)pStmt)->dbName, ((SCreateIndexStmt*)pStmt)->tableName,
                          PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
      code = TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SCreateIndexStmt*)pStmt)->dbName, ((SCreateIndexStmt*)pStmt)->tableName,
                             PRIV_IDX_CREATE, PRIV_OBJ_TBL);
  }

  return code;
}

static int32_t authDropIndex(SAuthCxt* pCxt, SDropIndexStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SDropIndexStmt*)pStmt)->indexDbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SDropIndexStmt*)pStmt)->indexDbName, ((SDropIndexStmt*)pStmt)->indexName,
                             PRIV_CM_DROP, PRIV_OBJ_IDX);
  }
  return code;
}

static int32_t authShowIndexes(SAuthCxt* pCxt, SShowStmt* pStmt) { return authShowTables(pCxt, pStmt); }

static int32_t authCreateTsma(SAuthCxt* pCxt, SCreateTSMAStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SCreateTSMAStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    if (authObjPrivileges(pCxt, ((SCreateTSMAStmt*)pStmt)->dbName, NULL, PRIV_TBL_CREATE, PRIV_OBJ_DB)) {
      code = TSDB_CODE_PAR_TB_CREATE_PERMISSION_DENIED;
    }
  }
  if (!pStmt->pOptions->recursiveTsma) {
    if (TSDB_CODE_SUCCESS == code) {
      if (authObjPrivileges(pCxt, ((SCreateTSMAStmt*)pStmt)->dbName, ((SCreateTSMAStmt*)pStmt)->tableName,
                            PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
        code = TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      if (authObjPrivileges(pCxt, ((SCreateTSMAStmt*)pStmt)->dbName, NULL, PRIV_STREAM_CREATE, PRIV_OBJ_DB)) {
        code = TSDB_CODE_PAR_STREAM_CREATE_PERMISSION_DENIED;
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = authObjPrivileges(pCxt, ((SCreateTSMAStmt*)pStmt)->dbName, ((SCreateTSMAStmt*)pStmt)->tableName,
                               PRIV_TSMA_CREATE, PRIV_OBJ_TBL);
    }
  }

  return code;
}

static int32_t authDropTsma(SAuthCxt* pCxt, SDropTSMAStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SDropTSMAStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SDropTSMAStmt*)pStmt)->dbName, ((SDropTSMAStmt*)pStmt)->tsmaName, PRIV_CM_DROP,
                             PRIV_OBJ_TSMA);
  }
  return code;
}

static int32_t authCreateRsma(SAuthCxt* pCxt, SCreateRsmaStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SCreateRsmaStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    if (authObjPrivileges(pCxt, ((SCreateRsmaStmt*)pStmt)->dbName, ((SCreateRsmaStmt*)pStmt)->tableName,
                          PRIV_TBL_SELECT, PRIV_OBJ_TBL)) {
      code = TSDB_CODE_PAR_TB_SELECT_PERMISSION_DENIED;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (authObjPrivileges(pCxt, ((SCreateRsmaStmt*)pStmt)->dbName, ((SCreateRsmaStmt*)pStmt)->tableName,
                          PRIV_TBL_INSERT, PRIV_OBJ_TBL)) {
      code = TSDB_CODE_PAR_TB_INSERT_PERMISSION_DENIED;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SCreateRsmaStmt*)pStmt)->dbName, ((SCreateRsmaStmt*)pStmt)->tableName,
                             PRIV_RSMA_CREATE, PRIV_OBJ_TBL);
  }
  return code;
}

static int32_t authDropRsma(SAuthCxt* pCxt, SDropRsmaStmt* pStmt) {
  int32_t code = checkDbUseAuth(pCxt, ((SDropRsmaStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SDropRsmaStmt*)pStmt)->dbName, ((SDropRsmaStmt*)pStmt)->rsmaName, PRIV_CM_DROP,
                             PRIV_OBJ_RSMA);
  }
  return code;
}

static int32_t authShowCreateRsma(SAuthCxt* pCxt, SShowCreateRsmaStmt* pStmt) {
#ifndef TD_ENTERPRISE
  return TSDB_CODE_OPS_NOT_SUPPORT;
#else
  int32_t code = checkDbUseAuth(pCxt, ((SShowCreateRsmaStmt*)pStmt)->dbName);
  if (TSDB_CODE_SUCCESS == code) {
    code = authObjPrivileges(pCxt, ((SShowCreateRsmaStmt*)pStmt)->dbName, ((SShowCreateRsmaStmt*)pStmt)->rsmaName,
                             PRIV_CM_SHOW_CREATE, PRIV_OBJ_RSMA);
  }
  if (code == 0) pStmt->hasPrivilege = true;
  return 0;  // return 0 and check owner later in translateShowCreateRsma since rsma ctgCatalog not available yet
#endif
}

static int32_t authCreateDatabase(SAuthCxt* pCxt, SCreateDatabaseStmt* pStmt) {
  int32_t code = authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_DB_CREATE);
#ifdef TD_ENTERPRISE
  // Per FS §4.2.1.4: specifying SECURITY_LEVEL > 0 requires PRIV_SECURITY_POLICY_ALTER regardless
  // of MAC activation state. SECURITY_LEVEL = 0 is equivalent to the default and always allowed.
  if (TSDB_CODE_SUCCESS == code) {
    if (pStmt->pOptions && pStmt->pOptions->securityLevel > 0) {
      // Trusted subject: PRIV_SECURITY_POLICY_ALTER holder is exempt from maxSecLevel constraint
      code = authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_SECURITY_POLICY_ALTER);
    }
  }
#endif
  return code;
}

static int32_t authAlterDatabase(SAuthCxt* pCxt, SAlterDatabaseStmt* pStmt) {
#ifdef TD_ENTERPRISE
  // MAC domain: security_level changes require only PRIV_SECURITY_POLICY_ALTER (no CM_ALTER needed)
  if (pStmt->pOptions && pStmt->pOptions->securityLevel >= 0) {
    // Trusted subject: PRIV_SECURITY_POLICY_ALTER holder is exempt from maxSecLevel constraint
    return authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_SECURITY_POLICY_ALTER);
  }
#endif
  // DAC domain: non-security ALTER requires CM_ALTER
  return authObjPrivileges(pCxt, ((SAlterDatabaseStmt*)pStmt)->dbName, NULL, PRIV_CM_ALTER, PRIV_OBJ_DB);
}

static int32_t authAlterLocal(SAuthCxt* pCxt, SAlterLocalStmt* pStmt) {
  int32_t privType = cfgGetPrivType(tsCfg, pStmt->config, 0);
  return authSysPrivileges(pCxt, (void*)pStmt, privType);
}

static int32_t authDropRole(SAuthCxt* pCxt, SDropRoleStmt* pStmt) {
  return authSysPrivileges(pCxt, (SNode*)pStmt, PRIV_ROLE_DROP);
}

static int32_t authDropDatabase(SAuthCxt* pCxt, SDropDatabaseStmt* pStmt) {
  return authObjPrivileges(pCxt, ((SDropDatabaseStmt*)pStmt)->dbName, NULL, PRIV_CM_DROP, PRIV_OBJ_DB);
}

static int32_t authUseDatabase(SAuthCxt* pCxt, SUseDatabaseStmt* pStmt) {
  return authObjPrivileges(pCxt, ((SUseDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_USE, PRIV_OBJ_DB);
}

static int32_t authGrant(SAuthCxt* pCxt, SGrantStmt* pStmt) {
  bool sodInitial = pCxt->pParseCxt->sodInitial;
  if (pStmt->optrType == TSDB_ALTER_ROLE_ROLE) {
    if (IS_SYS_PREFIX(pStmt->roleName)) {
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSDBA) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_GRANT_SYSDBA);
      }
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSSEC) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_GRANT_SYSSEC);
      }
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSAUDIT) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_GRANT_SYSAUDIT);
      }
    } else if (sodInitial) {
      return TSDB_CODE_MND_SOD_RESTRICTED;
    }
  } else if (sodInitial) {
    return TSDB_CODE_MND_SOD_RESTRICTED;
  }
  return authSysPrivileges(pCxt, (void*)pStmt, PRIV_GRANT_PRIVILEGE);
}

static int32_t authRevoke(SAuthCxt* pCxt, SRevokeStmt* pStmt) {
  bool sodInitial = pCxt->pParseCxt->sodInitial;
  if (pStmt->optrType == TSDB_ALTER_ROLE_ROLE) {
    if (IS_SYS_PREFIX(pStmt->roleName)) {
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSDBA) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_REVOKE_SYSDBA);
      }
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSSEC) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_REVOKE_SYSSEC);
      }
      if (strcmp(pStmt->roleName, TSDB_ROLE_SYSAUDIT) == 0) {
        return authSysPrivileges(pCxt, (void*)pStmt, PRIV_REVOKE_SYSAUDIT);
      }
    } else if (sodInitial) {
      return TSDB_CODE_MND_SOD_RESTRICTED;
    }
  } else if (sodInitial) {
    return TSDB_CODE_MND_SOD_RESTRICTED;
  }
  return authSysPrivileges(pCxt, (void*)pStmt, PRIV_REVOKE_PRIVILEGE);
}

static int32_t authQuery(SAuthCxt* pCxt, SNode* pStmt) {
  int32_t code = TSDB_CODE_SUCCESS;
#ifdef TD_ENTERPRISE
  switch (nodeType(pStmt)) {
    case QUERY_NODE_SET_OPERATOR:
      return authSetOperator(pCxt, (SSetOperator*)pStmt);
    case QUERY_NODE_SELECT_STMT:
      return authSelect(pCxt, (SSelectStmt*)pStmt);
    case QUERY_NODE_CREATE_ROLE_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_ROLE_CREATE);
    case QUERY_NODE_DROP_ROLE_STMT:
      return authDropRole(pCxt, (SDropRoleStmt*)pStmt);
    case QUERY_NODE_CREATE_USER_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_USER_CREATE);
    case QUERY_NODE_ALTER_USER_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_USER_ALTER);
    case QUERY_NODE_DROP_USER_STMT:
      return authDropUser(pCxt, (SDropUserStmt*)pStmt);
    case QUERY_NODE_DELETE_STMT:
      return authDelete(pCxt, (SDeleteStmt*)pStmt);
    case QUERY_NODE_INSERT_STMT:
      return authInsert(pCxt, (SInsertStmt*)pStmt);
    case QUERY_NODE_CREATE_TABLE_STMT:
      return authCreateTable(pCxt, (SCreateTableStmt*)pStmt);
    case QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT:
      return authCreateVTable(pCxt, (SCreateVTableStmt*)pStmt);
    case QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT:
      return authCreateVSubTable(pCxt, (SCreateVSubTableStmt*)pStmt);
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      return authCreateMultiTable(pCxt, (SCreateMultiTablesStmt*)pStmt);
    case QUERY_NODE_CREATE_STREAM_STMT:
      return authCreateStream(pCxt, (SCreateStreamStmt*)pStmt);
    case QUERY_NODE_CREATE_TOPIC_STMT:
      return authCreateTopic(pCxt, (SCreateTopicStmt*)pStmt);
    case QUERY_NODE_DROP_TABLE_STMT:
      return authDropTable(pCxt, (SDropTableStmt*)pStmt);
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      return authDropStable(pCxt, (SDropSuperTableStmt*)pStmt);
    case QUERY_NODE_DROP_VIRTUAL_TABLE_STMT:
      return authDropVtable(pCxt, (SDropVirtualTableStmt*)pStmt);
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT:
      return authAlterTable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT:
      return authAlterVTable(pCxt, (SAlterTableStmt*)pStmt);
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_BACKUP_NODES_STMT:
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    // case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:  // TODO: check in mnode
    // case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT: // not check local variables
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_ARBGROUPS_STMT:
    case QUERY_NODE_SHOW_ENCRYPTIONS_STMT:
    case QUERY_NODE_SHOW_MOUNTS_STMT:
    case QUERY_NODE_SHOW_ENCRYPT_ALGORITHMS_STMT:
    case QUERY_NODE_SHOW_ENCRYPT_STATUS_STMT:
      return !pCxt->pParseCxt->enableSysInfo ? TSDB_CODE_PAR_PERMISSION_DENIED : TSDB_CODE_SUCCESS;
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SShowCreateDatabaseStmt*)pStmt)->dbName, NULL, PRIV_CM_SHOW_CREATE, PRIV_OBJ_DB);
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_USERS_FULL_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_USER_SHOW);
    case QUERY_NODE_SHOW_ROLES_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_ROLE_SHOW);
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
    case QUERY_NODE_SHOW_ROLE_PRIVILEGES_STMT:
    case QUERY_NODE_SHOW_ROLE_COL_PRIVILEGES_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_SHOW_PRIVILEGES);
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_NODES_SHOW);
    case QUERY_NODE_SHOW_ANODES_STMT:
    case QUERY_NODE_SHOW_ANODES_FULL_STMT:
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_SHOW_XNODES_STMT:
    case QUERY_NODE_SHOW_XNODE_AGENTS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_NODES_SHOW);
    case QUERY_NODE_SHOW_XNODE_TASKS_STMT:
    case QUERY_NODE_SHOW_XNODE_JOBS_STMT:
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_CREATE_XNODE_STMT:
    case QUERY_NODE_DROP_XNODE_STMT:
      return TSDB_CODE_SUCCESS;
    case QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT:
    // case QUERY_NODE_SHOW_LICENCES_STMT: // do not check auth for basic licence info since it's used for taos logon
    case QUERY_NODE_SHOW_GRANTS_FULL_STMT:
    case QUERY_NODE_SHOW_GRANTS_LOGS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_GRANTS_SHOW);
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
      return authShowTables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_VTABLES_STMT:
      return authShowVtables(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_VTABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      return authShowCreateTable(pCxt, (SShowCreateTableStmt*)pStmt);
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      return authShowCreateView(pCxt, (SShowCreateViewStmt*)pStmt);
    case QUERY_NODE_CREATE_VIEW_STMT:
      return authCreateView(pCxt, (SCreateViewStmt*)pStmt);
    case QUERY_NODE_DROP_VIEW_STMT:
      return authDropView(pCxt, (SDropViewStmt*)pStmt);
    case QUERY_NODE_CREATE_INDEX_STMT:
      return authCreateIndex(pCxt, (SCreateIndexStmt*)pStmt);
    case QUERY_NODE_DROP_INDEX_STMT:
      return authDropIndex(pCxt, (SDropIndexStmt*)pStmt);
    case QUERY_NODE_SHOW_INDEXES_STMT:
      return authShowIndexes(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_CREATE_TSMA_STMT:
      return authCreateTsma(pCxt, (SCreateTSMAStmt*)pStmt);
    case QUERY_NODE_DROP_TSMA_STMT:
      return authDropTsma(pCxt, (SDropTSMAStmt*)pStmt);
    case QUERY_NODE_CREATE_RSMA_STMT:
      return authCreateRsma(pCxt, (SCreateRsmaStmt*)pStmt);
    case QUERY_NODE_DROP_RSMA_STMT:
      return authDropRsma(pCxt, (SDropRsmaStmt*)pStmt);
    case QUERY_NODE_ALTER_RSMA_STMT:
      return authObjPrivileges(pCxt, ((SAlterRsmaStmt*)pStmt)->dbName, ((SAlterRsmaStmt*)pStmt)->rsmaName,
                               PRIV_CM_ALTER, PRIV_OBJ_RSMA);
    case QUERY_NODE_SHOW_CREATE_RSMA_STMT:
      return authShowCreateRsma(pCxt, (SShowCreateRsmaStmt*)pStmt);
    case QUERY_NODE_CREATE_DATABASE_STMT:
      return authCreateDatabase(pCxt, (SCreateDatabaseStmt*)pStmt);
    case QUERY_NODE_BALANCE_VGROUP_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_VG_BALANCE);
    case QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT:
    case QUERY_NODE_BALANCE_VGROUP_LEADER_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_VG_BALANCE_LEADER);
    case QUERY_NODE_MERGE_VGROUP_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_VG_MERGE);
    case QUERY_NODE_SPLIT_VGROUP_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_VG_SPLIT);
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_VG_REDISTRIBUTE);
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_FUNC_CREATE);
    case QUERY_NODE_DROP_FUNCTION_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_FUNC_DROP);
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_FUNC_SHOW);
    case QUERY_NODE_GRANT_STMT:
      return authGrant(pCxt, (SGrantStmt*)pStmt);
    case QUERY_NODE_REVOKE_STMT:
      return authRevoke(pCxt, (SRevokeStmt*)pStmt);
    case QUERY_NODE_CREATE_DNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_CREATE_ANODE_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_NODE_CREATE);
    case QUERY_NODE_DROP_DNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_DROP_ANODE_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_NODE_DROP);
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
    case QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_TRANS_SHOW);
    case QUERY_NODE_KILL_TRANSACTION_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_TRANS_KILL);
    case QUERY_NODE_SHOW_QUERIES_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_QUERY_SHOW);
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_CONN_SHOW);
    case QUERY_NODE_KILL_QUERY_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_QUERY_KILL);
    case QUERY_NODE_KILL_CONNECTION_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_CONN_KILL);
    case QUERY_NODE_ALTER_DATABASE_STMT:
      return authAlterDatabase(pCxt, (SAlterDatabaseStmt*)pStmt);
    case QUERY_NODE_ALTER_LOCAL_STMT:
      return authAlterLocal(pCxt, (SAlterLocalStmt*)pStmt);
    case QUERY_NODE_DROP_DATABASE_STMT:
      return authDropDatabase(pCxt, (SDropDatabaseStmt*)pStmt);
    case QUERY_NODE_USE_DATABASE_STMT:
      return authUseDatabase(pCxt, (SUseDatabaseStmt*)pStmt);
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SFlushDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_FLUSH, PRIV_OBJ_DB);
    case QUERY_NODE_COMPACT_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SCompactDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_COMPACT, PRIV_OBJ_DB);
    case QUERY_NODE_TRIM_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((STrimDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_TRIM, PRIV_OBJ_DB);
    case QUERY_NODE_ROLLUP_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SRollupDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_ROLLUP, PRIV_OBJ_DB);
    case QUERY_NODE_SCAN_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SScanDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_SCAN, PRIV_OBJ_DB);
    case QUERY_NODE_SSMIGRATE_DATABASE_STMT:
      return authObjPrivileges(pCxt, ((SSsMigrateDatabaseStmt*)pStmt)->dbName, NULL, PRIV_DB_SSMIGRATE, PRIV_OBJ_DB);
    case QUERY_NODE_SHOW_USAGE_STMT:  // disk info
      return authShowUsage(pCxt, (SShowStmt*)pStmt);
    case QUERY_NODE_SHOW_APPS_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_APPS_SHOW);
    case QUERY_NODE_SHOW_CLUSTER_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_CLUSTER_SHOW);
    case QUERY_NODE_SHOW_SECURITY_POLICIES_STMT:
      return authSysPrivileges(pCxt, pStmt, PRIV_SECURITY_POLICIES_SHOW); 
      // check in mnode
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_COMPACTS_STMT:
    case QUERY_NODE_SHOW_RETENTIONS_STMT:
    case QUERY_NODE_SHOW_SCANS_STMT:
    case QUERY_NODE_SHOW_SSMIGRATES_STMT:
      return TSDB_CODE_SUCCESS;
    default:
      break;
  }
#endif
  return code;
}

int32_t authenticate(SParseContext* pParseCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
  SAuthCxt cxt = {
      .pParseCxt = pParseCxt,
      .pMetaCache = pMetaCache,
      .errCode = TSDB_CODE_SUCCESS,
      .macNruGuaranteed = (pParseCxt->maxSecLevel >= SECURITY_LEVEL_TOP_SECRET),
      .macNwdGuaranteed = (pParseCxt->minSecLevel == 0),
  };
#ifdef TD_ENTERPRISE
  if (pParseCxt->sodInitial) {
    int32_t rootNodeType = nodeType(pQuery->pRoot);
    if (rootNodeType == QUERY_NODE_SELECT_STMT) {
      SSelectStmt* pSelect = (SSelectStmt*)pQuery->pRoot;
      STableNode*  pTable = (STableNode*)(pSelect->pFromTable);
      if (NULL == pTable || QUERY_NODE_REAL_TABLE != nodeType(pTable) || !IS_INFORMATION_SCHEMA_DB(pTable->dbName) ||
          (strcmp(pTable->tableName, TSDB_INS_TABLE_USERS) != 0)) {
        return TSDB_CODE_MND_SOD_RESTRICTED;
      }
    } else if (rootNodeType != QUERY_NODE_GRANT_STMT && rootNodeType != QUERY_NODE_REVOKE_STMT &&
               rootNodeType != QUERY_NODE_CREATE_USER_STMT && rootNodeType != QUERY_NODE_DROP_USER_STMT &&
               rootNodeType != QUERY_NODE_ALTER_USER_STMT && rootNodeType != QUERY_NODE_SHOW_USERS_STMT &&
               rootNodeType != QUERY_NODE_SHOW_SECURITY_POLICIES_STMT) {
      return TSDB_CODE_MND_SOD_RESTRICTED;
    }
  }
#endif
  return authQuery(&cxt, pQuery->pRoot);
}
