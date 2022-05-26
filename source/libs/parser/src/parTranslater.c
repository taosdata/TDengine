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

#include "parInt.h"

#include "catalog.h"
#include "cmdnodes.h"
#include "filter.h"
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "systable.h"
#include "tglobal.h"
#include "ttime.h"

#define generateDealNodeErrMsg(pCxt, code, ...) \
  (pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, code, ##__VA_ARGS__), DEAL_RES_ERROR)

typedef struct STranslateContext {
  SParseContext*   pParseCxt;
  int32_t          errCode;
  SMsgBuf          msgBuf;
  SArray*          pNsLevel;  // element is SArray*, the element of this subarray is STableNode*
  int32_t          currLevel;
  ESqlClause       currClause;
  SSelectStmt*     pCurrStmt;
  SCmdMsgInfo*     pCmdMsg;
  SHashObj*        pDbs;
  SHashObj*        pTables;
  SExplainOptions* pExplainOpt;
} STranslateContext;

typedef struct SFullDatabaseName {
  char fullDbName[TSDB_DB_FNAME_LEN];
} SFullDatabaseName;

static int32_t  translateSubquery(STranslateContext* pCxt, SNode* pNode);
static int32_t  translateQuery(STranslateContext* pCxt, SNode* pNode);
static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal);

static bool afterGroupBy(ESqlClause clause) { return clause > SQL_CLAUSE_GROUP_BY; }

static bool beforeHaving(ESqlClause clause) { return clause < SQL_CLAUSE_HAVING; }

static bool afterHaving(ESqlClause clause) { return clause > SQL_CLAUSE_HAVING; }

static int32_t addNamespace(STranslateContext* pCxt, void* pTable) {
  size_t currTotalLevel = taosArrayGetSize(pCxt->pNsLevel);
  if (currTotalLevel > pCxt->currLevel) {
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    taosArrayPush(pTables, &pTable);
  } else {
    do {
      SArray* pTables = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
      if (pCxt->currLevel == currTotalLevel) {
        taosArrayPush(pTables, &pTable);
      }
      taosArrayPush(pCxt->pNsLevel, &pTables);
      ++currTotalLevel;
    } while (currTotalLevel <= pCxt->currLevel);
  }
  return TSDB_CODE_SUCCESS;
}

static SName* toName(int32_t acctId, const char* pDbName, const char* pTableName, SName* pName) {
  pName->type = TSDB_TABLE_NAME_T;
  pName->acctId = acctId;
  strcpy(pName->dbname, pDbName);
  strcpy(pName->tname, pTableName);
  return pName;
}

static int32_t collectUseDatabaseImpl(const char* pFullDbName, SHashObj* pDbs) {
  SFullDatabaseName name = {0};
  strcpy(name.fullDbName, pFullDbName);
  return taosHashPut(pDbs, pFullDbName, strlen(pFullDbName), &name, sizeof(SFullDatabaseName));
}

static int32_t collectUseDatabase(const SName* pName, SHashObj* pDbs) {
  char dbFName[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(pName, dbFName);
  return collectUseDatabaseImpl(dbFName, pDbs);
}

static int32_t collectUseTable(const SName* pName, SHashObj* pDbs) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);
  return taosHashPut(pDbs, fullName, strlen(fullName), pName, sizeof(SName));
}

static int32_t getTableMetaImpl(STranslateContext* pCxt, const SName* pName, STableMeta** pMeta) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetTableMeta(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, pName, pMeta);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetTableMeta error, code:%s, dbName:%s, tbName:%s", tstrerror(code), pName->dbname,
                pName->tname);
  }
  return code;
}

static int32_t getTableMeta(STranslateContext* pCxt, const char* pDbName, const char* pTableName, STableMeta** pMeta) {
  SName name;
  return getTableMetaImpl(pCxt, toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name), pMeta);
}

static int32_t refreshGetTableMeta(STranslateContext* pCxt, const char* pDbName, const char* pTableName,
                                   STableMeta** pMeta) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  SName          name;
  toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name);
  int32_t code =
      catalogRefreshGetTableMeta(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, &name, pMeta, false);
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogRefreshGetTableMeta error, code:%s, dbName:%s, tbName:%s", tstrerror(code), pDbName,
                pTableName);
  }
  return code;
}

static int32_t getTableDistVgInfo(STranslateContext* pCxt, const SName* pName, SArray** pVgInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetTableDistVgInfo(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, pName, pVgInfo);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetTableDistVgInfo error, code:%s, dbName:%s, tbName:%s", tstrerror(code), pName->dbname,
                pName->tname);
  }
  return code;
}

static int32_t getDBVgInfoImpl(STranslateContext* pCxt, const SName* pName, SArray** pVgInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  char           fullDbName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pName, fullDbName);
  int32_t code = collectUseDatabaseImpl(fullDbName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetDBVgInfo(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, fullDbName, pVgInfo);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetDBVgInfo error, code:%s, dbFName:%s", tstrerror(code), fullDbName);
  }
  return code;
}

static int32_t getDBVgInfo(STranslateContext* pCxt, const char* pDbName, SArray** pVgInfo) {
  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
  return getDBVgInfoImpl(pCxt, &name, pVgInfo);
}

static int32_t getTableHashVgroupImpl(STranslateContext* pCxt, const SName* pName, SVgroupInfo* pInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabase(pName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectUseTable(pName, pCxt->pTables);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetTableHashVgroup(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, pName, pInfo);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetTableHashVgroup error, code:%s, dbName:%s, tbName:%s", tstrerror(code), pName->dbname,
                pName->tname);
  }
  return code;
}

static int32_t getTableHashVgroup(STranslateContext* pCxt, const char* pDbName, const char* pTableName,
                                  SVgroupInfo* pInfo) {
  SName name;
  return getTableHashVgroupImpl(pCxt, toName(pCxt->pParseCxt->acctId, pDbName, pTableName, &name), pInfo);
}

static int32_t getDBVgVersion(STranslateContext* pCxt, const char* pDbFName, int32_t* pVersion, int64_t* pDbId,
                              int32_t* pTableNum) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  int32_t        code = collectUseDatabaseImpl(pDbFName, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetDBVgVersion(pParCxt->pCatalog, pDbFName, pVersion, pDbId, pTableNum);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetDBVgVersion error, code:%s, dbFName:%s", tstrerror(code), pDbFName);
  }
  return code;
}

static int32_t getDBCfg(STranslateContext* pCxt, const char* pDbName, SDbCfgInfo* pInfo) {
  SParseContext* pParCxt = pCxt->pParseCxt;
  SName          name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pDbName, strlen(pDbName));
  char dbFname[TSDB_DB_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, dbFname);
  int32_t code = collectUseDatabaseImpl(dbFname, pCxt->pDbs);
  if (TSDB_CODE_SUCCESS == code) {
    code = catalogGetDBCfg(pParCxt->pCatalog, pParCxt->pTransporter, &pParCxt->mgmtEpSet, dbFname, pInfo);
  }
  if (TSDB_CODE_SUCCESS != code) {
    parserError("catalogGetDBCfg error, code:%s, dbFName:%s", tstrerror(code), dbFname);
  }
  return code;
}

static int32_t initTranslateContext(SParseContext* pParseCxt, STranslateContext* pCxt) {
  pCxt->pParseCxt = pParseCxt;
  pCxt->errCode = TSDB_CODE_SUCCESS;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  pCxt->currLevel = 0;
  pCxt->currClause = 0;
  pCxt->pDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pCxt->pTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (NULL == pCxt->pNsLevel || NULL == pCxt->pDbs || NULL == pCxt->pTables) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t resetTranslateNamespace(STranslateContext* pCxt) {
  if (NULL != pCxt->pNsLevel) {
    size_t size = taosArrayGetSize(pCxt->pNsLevel);
    for (size_t i = 0; i < size; ++i) {
      taosArrayDestroy(taosArrayGetP(pCxt->pNsLevel, i));
    }
    taosArrayDestroy(pCxt->pNsLevel);
  }
  pCxt->pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  if (NULL == pCxt->pNsLevel) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static void destroyTranslateContext(STranslateContext* pCxt) {
  if (NULL != pCxt->pNsLevel) {
    size_t size = taosArrayGetSize(pCxt->pNsLevel);
    for (size_t i = 0; i < size; ++i) {
      taosArrayDestroy(taosArrayGetP(pCxt->pNsLevel, i));
    }
    taosArrayDestroy(pCxt->pNsLevel);
  }

  if (NULL != pCxt->pCmdMsg) {
    taosMemoryFreeClear(pCxt->pCmdMsg->pMsg);
    taosMemoryFreeClear(pCxt->pCmdMsg);
  }

  taosHashCleanup(pCxt->pDbs);
  taosHashCleanup(pCxt->pTables);
}

static bool isAliasColumn(const SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]));
}

static bool isAggFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isSelectFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsSelectFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isTimelineFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsTimelineFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isScanPseudoColumnFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsScanPseudoColumnFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isIndefiniteRowsFunc(const SNode* pNode) {
  return (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsIndefiniteRowsFunc(((SFunctionNode*)pNode)->funcId));
}

static bool isDistinctOrderBy(STranslateContext* pCxt) {
  return (SQL_CLAUSE_ORDER_BY == pCxt->currClause && pCxt->pCurrStmt->isDistinct);
}

static bool belongTable(const char* currentDb, const SColumnNode* pCol, const STableNode* pTable) {
  int cmp = 0;
  if ('\0' != pCol->dbName[0]) {
    cmp = strcmp(pCol->dbName, pTable->dbName);
  } else {
    cmp = (QUERY_NODE_REAL_TABLE == nodeType(pTable) ? strcmp(currentDb, pTable->dbName) : 0);
  }
  if (0 == cmp) {
    cmp = strcmp(pCol->tableAlias, pTable->tableAlias);
  }
  return (0 == cmp);
}

static SNodeList* getProjectList(const SNode* pNode) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pNode)) {
    return ((SSelectStmt*)pNode)->pProjectionList;
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pNode)) {
    return ((SSetOperator*)pNode)->pProjectionList;
  }
  return NULL;
}

static void setColumnInfoBySchema(const SRealTableNode* pTable, const SSchema* pColSchema, bool isTag,
                                  SColumnNode* pCol) {
  strcpy(pCol->dbName, pTable->table.dbName);
  strcpy(pCol->tableAlias, pTable->table.tableAlias);
  strcpy(pCol->tableName, pTable->table.tableName);
  strcpy(pCol->colName, pColSchema->name);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pColSchema->name);
  }
  pCol->tableId = pTable->pMeta->uid;
  pCol->tableType = pTable->pMeta->tableType;
  pCol->colId = pColSchema->colId;
  pCol->colType = isTag ? COLUMN_TYPE_TAG : COLUMN_TYPE_COLUMN;
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == pCol->node.resType.type) {
    pCol->node.resType.precision = pTable->pMeta->tableInfo.precision;
  }
}

static void setColumnInfoByExpr(const STableNode* pTable, SExprNode* pExpr, SColumnNode** pColRef) {
  SColumnNode* pCol = *pColRef;

  pCol->pProjectRef = (SNode*)pExpr;
  if (NULL == pExpr->pAssociation) {
    pExpr->pAssociation = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  }
  taosArrayPush(pExpr->pAssociation, &pColRef);
  if (NULL != pTable) {
    strcpy(pCol->tableAlias, pTable->tableAlias);
  } else if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    SColumnNode* pProjCol = (SColumnNode*)pExpr;
    strcpy(pCol->tableAlias, pProjCol->tableAlias);
    pCol->tableId = pProjCol->tableId;
    pCol->colId = pProjCol->colId;
    pCol->colType = pProjCol->colType;
  }
  strcpy(pCol->colName, pExpr->aliasName);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pCol->colName);
  }
  pCol->node.resType = pExpr->resType;
}

static int32_t createColumnsByTable(STranslateContext* pCxt, const STableNode* pTable, SNodeList* pList) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t           nums =
        pMeta->tableInfo.numOfColumns + ((TSDB_SUPER_TABLE == pMeta->tableType) ? pMeta->tableInfo.numOfTags : 0);
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
      }
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i >= pMeta->tableInfo.numOfColumns), pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode*     pNode;
    FOREACH(pNode, pProjectList) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
      }
      setColumnInfoByExpr(pTable, (SExprNode*)pNode, &pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isInternalPrimaryKey(const SColumnNode* pCol) {
  return PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId && 0 == strcmp(pCol->colName, PK_TS_COL_INTERNAL_NAME);
}

static bool isTimeOrderQuery(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return ((SSelectStmt*)pStmt)->isTimeOrderQuery;
  } else {
    return false;
  }
}

static bool isPrimaryKeyImpl(STempTableNode* pTable, SNode* pExpr) {
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    return (PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pExpr)->colId);
  } else if (QUERY_NODE_FUNCTION == nodeType(pExpr)) {
    SFunctionNode* pFunc = (SFunctionNode*)pExpr;
    if (FUNCTION_TYPE_SELECT_VALUE == pFunc->funcType) {
      return isPrimaryKeyImpl(pTable, nodesListGetNode(pFunc->pParameterList, 0));
    } else if (FUNCTION_TYPE_WSTARTTS == pFunc->funcType || FUNCTION_TYPE_WENDTS == pFunc->funcType) {
      return true;
    }
  }
  return false;
}

static bool isPrimaryKey(STempTableNode* pTable, SNode* pExpr) {
  if (!isTimeOrderQuery(pTable->pSubquery)) {
    return false;
  }
  return isPrimaryKeyImpl(pTable, pExpr);
}

static bool findAndSetColumn(SColumnNode** pColRef, const STableNode* pTable) {
  SColumnNode* pCol = *pColRef;
  bool         found = false;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    if (isInternalPrimaryKey(pCol)) {
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema, false, pCol);
      return true;
    }
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      if (0 == strcmp(pCol->colName, pMeta->schema[i].name)) {
        setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i >= pMeta->tableInfo.numOfColumns), pCol);
        found = true;
        break;
      }
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode*     pNode;
    FOREACH(pNode, pProjectList) {
      SExprNode* pExpr = (SExprNode*)pNode;
      if (0 == strcmp(pCol->colName, pExpr->aliasName) ||
          (isPrimaryKey((STempTableNode*)pTable, pNode) && isInternalPrimaryKey(pCol))) {
        setColumnInfoByExpr(pTable, pExpr, pColRef);
        found = true;
        break;
      }
    }
  }
  return found;
}

static EDealRes translateColumnWithPrefix(STranslateContext* pCxt, SColumnNode** pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    foundTable = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (belongTable(pCxt->pParseCxt->db, (*pCol), pTable)) {
      foundTable = true;
      if (findAndSetColumn(pCol, pTable)) {
        break;
      }
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
    }
  }
  if (!foundTable) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_TABLE_NOT_EXIST, (*pCol)->tableAlias);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateColumnWithoutPrefix(STranslateContext* pCxt, SColumnNode** pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    found = false;
  bool    isInternalPk = isInternalPrimaryKey(*pCol);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (findAndSetColumn(pCol, pTable)) {
      if (found) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, (*pCol)->colName);
      }
      found = true;
      if (isInternalPk) {
        break;
      }
    }
  }
  if (!found) {
    if (isInternalPk) {
      if (NULL != pCxt->pCurrStmt->pWindow) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY);
      }
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_INTERNAL_PK);
    } else {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, (*pCol)->colName);
    }
  }
  return DEAL_RES_CONTINUE;
}

static bool translateColumnUseAlias(STranslateContext* pCxt, SColumnNode** pCol) {
  SNodeList* pProjectionList = pCxt->pCurrStmt->pProjectionList;
  SNode*     pNode;
  FOREACH(pNode, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (0 == strcmp((*pCol)->colName, pExpr->aliasName)) {
      setColumnInfoByExpr(NULL, pExpr, pCol);
      return true;
    }
  }
  return false;
}

static EDealRes translateColumn(STranslateContext* pCxt, SColumnNode** pCol) {
  // count(*)/first(*)/last(*) and so on
  if (0 == strcmp((*pCol)->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }

  EDealRes res = DEAL_RES_CONTINUE;
  if ('\0' != (*pCol)->tableAlias[0]) {
    res = translateColumnWithPrefix(pCxt, pCol);
  } else {
    bool found = false;
    if (SQL_CLAUSE_ORDER_BY == pCxt->currClause) {
      found = translateColumnUseAlias(pCxt, pCol);
    }
    res = (found ? DEAL_RES_CONTINUE : translateColumnWithoutPrefix(pCxt, pCol));
  }
  return res;
}

static int32_t parseTimeFromValueNode(STranslateContext* pCxt, SValueNode* pVal) {
  if (IS_NUMERIC_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (IS_UNSIGNED_NUMERIC_TYPE(pVal->node.resType.type)) {
      pVal->datum.i = pVal->datum.u;
    } else if (IS_FLOAT_TYPE(pVal->node.resType.type)) {
      pVal->datum.i = pVal->datum.d;
    } else if (TSDB_DATA_TYPE_BOOL == pVal->node.resType.type) {
      pVal->datum.i = pVal->datum.b;
    }
    return TSDB_CODE_SUCCESS;
  } else if (IS_VAR_DATA_TYPE(pVal->node.resType.type) || TSDB_DATA_TYPE_TIMESTAMP == pVal->node.resType.type) {
    if (TSDB_CODE_SUCCESS == taosParseTime(pVal->literal, &pVal->datum.i, pVal->node.resType.bytes,
                                           pVal->node.resType.precision, tsDaylight)) {
      return TSDB_CODE_SUCCESS;
    }
    char* pEnd = NULL;
    pVal->datum.i = taosStr2Int64(pVal->literal, &pEnd, 10);
    return (NULL != pEnd && '\0' == *pEnd) ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
  } else {
    return TSDB_CODE_FAILED;
  }
}

static EDealRes translateValueImpl(STranslateContext* pCxt, SValueNode* pVal, SDataType targetDt) {
  uint8_t precision = (NULL != pCxt->pCurrStmt ? pCxt->pCurrStmt->precision : targetDt.precision);
  pVal->node.resType.precision = precision;
  if (pVal->placeholderNo > 0) {
    return DEAL_RES_CONTINUE;
  }
  if (pVal->isDuration) {
    if (parseNatualDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &pVal->unit, precision) !=
        TSDB_CODE_SUCCESS) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
    }
    *(int64_t*)&pVal->typeData = pVal->datum.i;
  } else {
    switch (targetDt.type) {
      case TSDB_DATA_TYPE_NULL:
        break;
      case TSDB_DATA_TYPE_BOOL:
        pVal->datum.b = (0 == strcasecmp(pVal->literal, "true"));
        *(bool*)&pVal->typeData = pVal->datum.b;
        break;
      case TSDB_DATA_TYPE_TINYINT: {
        pVal->datum.i = taosStr2Int64(pVal->literal, NULL, 10);
        *(int8_t*)&pVal->typeData = pVal->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        pVal->datum.i = taosStr2Int64(pVal->literal, NULL, 10);
        *(int16_t*)&pVal->typeData = pVal->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        pVal->datum.i = taosStr2Int64(pVal->literal, NULL, 10);
        *(int32_t*)&pVal->typeData = pVal->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        pVal->datum.i = taosStr2Int64(pVal->literal, NULL, 10);
        *(int64_t*)&pVal->typeData = pVal->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        pVal->datum.u = taosStr2UInt64(pVal->literal, NULL, 10);
        *(uint8_t*)&pVal->typeData = pVal->datum.u;
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        pVal->datum.u = taosStr2UInt64(pVal->literal, NULL, 10);
        *(uint16_t*)&pVal->typeData = pVal->datum.u;
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        pVal->datum.u = taosStr2UInt64(pVal->literal, NULL, 10);
        *(uint32_t*)&pVal->typeData = pVal->datum.u;
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        pVal->datum.u = taosStr2UInt64(pVal->literal, NULL, 10);
        *(uint64_t*)&pVal->typeData = pVal->datum.u;
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        pVal->datum.d = taosStr2Double(pVal->literal, NULL);
        *(float*)&pVal->typeData = pVal->datum.d;
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        pVal->datum.d = taosStr2Double(pVal->literal, NULL);
        *(double*)&pVal->typeData = pVal->datum.d;
        break;
      }
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY: {
        pVal->datum.p = taosMemoryCalloc(1, targetDt.bytes + 1);
        if (NULL == pVal->datum.p) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
        }
        int32_t len = TMIN(targetDt.bytes - VARSTR_HEADER_SIZE, pVal->node.resType.bytes);
        varDataSetLen(pVal->datum.p, len);
        strncpy(varDataVal(pVal->datum.p), pVal->literal, len);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        if (TSDB_CODE_SUCCESS != parseTimeFromValueNode(pCxt, pVal)) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
        }
        *(int64_t*)&pVal->typeData = pVal->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        pVal->datum.p = taosMemoryCalloc(1, targetDt.bytes + 1);
        if (NULL == pVal->datum.p) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
          ;
        }

        int32_t len = 0;
        if (!taosMbsToUcs4(pVal->literal, pVal->node.resType.bytes, (TdUcs4*)varDataVal(pVal->datum.p),
                           targetDt.bytes - VARSTR_HEADER_SIZE, &len)) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
        }
        varDataSetLen(pVal->datum.p, len);
        break;
      }
      case TSDB_DATA_TYPE_DECIMAL:
      case TSDB_DATA_TYPE_BLOB:
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      default:
        break;
    }
  }
  pVal->node.resType = targetDt;
  pVal->translate = true;
  return DEAL_RES_CONTINUE;
}

static int32_t calcTypeBytes(SDataType dt) {
  if (TSDB_DATA_TYPE_BINARY == dt.type) {
    return dt.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    return dt.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  } else {
    return dt.bytes;
  }
}

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  SDataType dt = pVal->node.resType;
  dt.bytes = calcTypeBytes(dt);
  return translateValueImpl(pCxt, pVal, dt);
}

static bool isMultiResFunc(SNode* pNode) {
  if (NULL == pNode) {
    return false;
  }
  if (QUERY_NODE_FUNCTION != nodeType(pNode) || !fmIsMultiResFunc(((SFunctionNode*)pNode)->funcId)) {
    return false;
  }
  SNodeList* pParameterList = ((SFunctionNode*)pNode)->pParameterList;
  if (LIST_LENGTH(pParameterList) > 1) {
    return true;
  }
  SNode* pParam = nodesListGetNode(pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pParam) ? 0 == strcmp(((SColumnNode*)pParam)->colName, "*") : false);
}

static EDealRes translateUnaryOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  if (OP_TYPE_MINUS == pOp->opType) {
    if (!IS_MATHABLE_TYPE(((SExprNode*)(pOp->pLeft))->resType.type)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pLeft))->aliasName);
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  } else {
    pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateArithmeticOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (TSDB_DATA_TYPE_BLOB == ldt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }
  if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_TIMESTAMP == rdt.type) ||
      (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && (IS_VAR_DATA_TYPE(rdt.type) || IS_FLOAT_TYPE(rdt.type))) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && (IS_VAR_DATA_TYPE(ldt.type) || IS_FLOAT_TYPE(ldt.type)))) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }

  if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && IS_INTEGER_TYPE(rdt.type)) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && IS_INTEGER_TYPE(ldt.type)) ||
      (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_BOOL == rdt.type) ||
      (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && TSDB_DATA_TYPE_BOOL == ldt.type)) {
    pOp->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
  } else {
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateComparisonOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (TSDB_DATA_TYPE_BLOB == ldt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }
  if (OP_TYPE_IN == pOp->opType || OP_TYPE_NOT_IN == pOp->opType) {
    ((SExprNode*)pOp->pRight)->resType = ((SExprNode*)pOp->pLeft)->resType;
  }
  if (nodesIsRegularOp(pOp)) {
    if (!IS_VAR_DATA_TYPE(((SExprNode*)(pOp->pLeft))->resType.type)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pLeft))->aliasName);
    }
    if (QUERY_NODE_VALUE != nodeType(pOp->pRight) || !IS_STR_DATA_TYPE(((SExprNode*)(pOp->pRight))->resType.type)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
    }
  }
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateJsonOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (TSDB_DATA_TYPE_JSON != ldt.type || TSDB_DATA_TYPE_BINARY != rdt.type) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }
  pOp->node.resType.type = TSDB_DATA_TYPE_JSON;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_JSON].bytes;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  if (isMultiResFunc(pOp->pLeft)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pLeft))->aliasName);
  }
  if (isMultiResFunc(pOp->pRight)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
  }

  if (nodesIsUnaryOp(pOp)) {
    return translateUnaryOperator(pCxt, pOp);
  } else if (nodesIsArithmeticOp(pOp)) {
    return translateArithmeticOperator(pCxt, pOp);
  } else if (nodesIsComparisonOp(pOp)) {
    return translateComparisonOperator(pCxt, pOp);
  } else if (nodesIsJsonOp(pOp)) {
    return translateJsonOperator(pCxt, pOp);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes haveVectorFunction(SNode* pNode, void* pContext) {
  if (isAggFunc(pNode)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  } else if (isIndefiniteRowsFunc(pNode)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t findTable(STranslateContext* pCxt, const char* pTableAlias, STableNode** pOutput) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (NULL == pTableAlias || 0 == strcmp(pTable->tableAlias, pTableAlias)) {
      *pOutput = pTable;
      return TSDB_CODE_SUCCESS;
    }
  }
  return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, pTableAlias);
}

static bool isCountStar(SFunctionNode* pFunc) {
  if (FUNCTION_TYPE_COUNT != pFunc->funcType || 1 != LIST_LENGTH(pFunc->pParameterList)) {
    return false;
  }
  SNode* pPara = nodesListGetNode(pFunc->pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pPara) && 0 == strcmp(((SColumnNode*)pPara)->colName, "*"));
}

// count(*) is rewritten as count(ts) for scannning optimization
static int32_t rewriteCountStar(STranslateContext* pCxt, SFunctionNode* pCount) {
  SColumnNode* pCol = nodesListGetNode(pCount->pParameterList, 0);
  STableNode*  pTable = NULL;
  int32_t      code = findTable(pCxt, ('\0' == pCol->tableAlias[0] ? NULL : pCol->tableAlias), &pTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, false, pCol);
  }
  return code;
}

static bool hasInvalidFuncNesting(SNodeList* pParameterList) {
  bool hasInvalidFunc = false;
  nodesWalkExprs(pParameterList, haveVectorFunction, &hasInvalidFunc);
  return hasInvalidFunc;
}

static int32_t getFuncInfo(STranslateContext* pCxt, SFunctionNode* pFunc) {
  SFmGetFuncInfoParam param = {.pCtg = pCxt->pParseCxt->pCatalog,
                               .pRpc = pCxt->pParseCxt->pTransporter,
                               .pMgmtEps = &pCxt->pParseCxt->mgmtEpSet,
                               .pErrBuf = pCxt->msgBuf.buf,
                               .errBufLen = pCxt->msgBuf.len};
  return fmGetFuncInfo(&param, pFunc);
}

static int32_t translateAggFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (beforeHaving(pCxt->currClause)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);
  }
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  if (pCxt->pCurrStmt->hasIndefiniteRowsFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }

  if (isCountStar(pFunc)) {
    return rewriteCountStar(pCxt, pFunc);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateScanPseudoColumnFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (0 == LIST_LENGTH(pFunc->pParameterList)) {
    if (QUERY_NODE_REAL_TABLE != nodeType(pCxt->pCurrStmt->pFromTable)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
  } else {
    SValueNode* pVal = nodesListGetNode(pFunc->pParameterList, 0);
    STableNode* pTable = NULL;
    pCxt->errCode = findTable(pCxt, pVal->literal, &pTable);
    if (TSDB_CODE_SUCCESS == pCxt->errCode && (NULL == pTable || QUERY_NODE_REAL_TABLE != nodeType(pTable))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TBNAME);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateIndefiniteRowsFunc(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (SQL_CLAUSE_SELECT != pCxt->currClause || pCxt->pCurrStmt->hasIndefiniteRowsFunc || pCxt->pCurrStmt->hasAggFuncs) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  if (hasInvalidFuncNesting(pFunc->pParameterList)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_AGG_FUNC_NESTING);
  }
  return TSDB_CODE_SUCCESS;
}

static void setFuncClassification(SSelectStmt* pSelect, SFunctionNode* pFunc) {
  pSelect->hasAggFuncs = pSelect->hasAggFuncs ? true : fmIsAggFunc(pFunc->funcId);
  pSelect->hasRepeatScanFuncs = pSelect->hasRepeatScanFuncs ? true : fmIsRepeatScanFunc(pFunc->funcId);
  pSelect->hasIndefiniteRowsFunc = pSelect->hasIndefiniteRowsFunc ? true : fmIsIndefiniteRowsFunc(pFunc->funcId);
}

static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode* pFunc) {
  SNode* pParam = NULL;
  FOREACH(pParam, pFunc->pParameterList) {
    if (isMultiResFunc(pParam)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)pParam)->aliasName);
    }
  }

  pCxt->errCode = getFuncInfo(pCxt, pFunc);
  if (TSDB_CODE_SUCCESS == pCxt->errCode && fmIsAggFunc(pFunc->funcId)) {
    pCxt->errCode = translateAggFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode && fmIsScanPseudoColumnFunc(pFunc->funcId)) {
    pCxt->errCode = translateScanPseudoColumnFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode && fmIsIndefiniteRowsFunc(pFunc->funcId)) {
    pCxt->errCode = translateIndefiniteRowsFunc(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    setFuncClassification(pCxt->pCurrStmt, pFunc);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_CONTINUE : DEAL_RES_ERROR;
}

static EDealRes translateExprSubquery(STranslateContext* pCxt, SNode* pNode) {
  return (TSDB_CODE_SUCCESS == translateSubquery(pCxt, pNode) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}

static EDealRes translateLogicCond(STranslateContext* pCxt, SLogicConditionNode* pCond) {
  pCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return DEAL_RES_CONTINUE;
}

static EDealRes doTranslateExpr(SNode** pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_COLUMN:
      return translateColumn(pCxt, (SColumnNode**)pNode);
    case QUERY_NODE_VALUE:
      return translateValue(pCxt, (SValueNode*)*pNode);
    case QUERY_NODE_OPERATOR:
      return translateOperator(pCxt, (SOperatorNode*)*pNode);
    case QUERY_NODE_FUNCTION:
      return translateFunction(pCxt, (SFunctionNode*)*pNode);
    case QUERY_NODE_LOGIC_CONDITION:
      return translateLogicCond(pCxt, (SLogicConditionNode*)*pNode);
    case QUERY_NODE_TEMP_TABLE:
      return translateExprSubquery(pCxt, ((STempTableNode*)*pNode)->pSubquery);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t translateExpr(STranslateContext* pCxt, SNode** pNode) {
  nodesRewriteExprPostOrder(pNode, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static int32_t translateExprList(STranslateContext* pCxt, SNodeList* pList) {
  nodesRewriteExprsPostOrder(pList, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static SNodeList* getGroupByList(STranslateContext* pCxt) {
  if (isDistinctOrderBy(pCxt)) {
    return pCxt->pCurrStmt->pProjectionList;
  }
  return pCxt->pCurrStmt->pGroupByList;
}

static SNode* getGroupByNode(SNode* pNode) {
  if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
    return nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0);
  }
  return pNode;
}

static int32_t getGroupByErrorCode(STranslateContext* pCxt) {
  if (isDistinctOrderBy(pCxt)) {
    return TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION;
  }
  return TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION;
}

typedef struct SCheckExprForGroupByCxt {
  STranslateContext* pTranslateCxt;
  int32_t            selectFuncNum;
  bool               hasSelectValFunc;
  bool               hasOtherAggFunc;
} SCheckExprForGroupByCxt;

static EDealRes rewriteColToSelectValFunc(STranslateContext* pCxt, SNode** pNode) {
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
    return DEAL_RES_ERROR;
  }
  strcpy(pFunc->functionName, "_select_value");
  strcpy(pFunc->node.aliasName, ((SExprNode*)*pNode)->aliasName);
  pCxt->errCode = nodesListMakeAppend(&pFunc->pParameterList, *pNode);
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    pCxt->errCode == getFuncInfo(pCxt, pFunc);
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    *pNode = (SNode*)pFunc;
    pCxt->pCurrStmt->hasSelectValFunc = true;
  } else {
    nodesDestroyNode(pFunc);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
}

static EDealRes doCheckExprForGroupBy(SNode** pNode, void* pContext) {
  SCheckExprForGroupByCxt* pCxt = (SCheckExprForGroupByCxt*)pContext;
  if (!nodesIsExprNode(*pNode) || isAliasColumn(*pNode)) {
    return DEAL_RES_CONTINUE;
  }
  if (isSelectFunc(*pNode)) {
    ++(pCxt->selectFuncNum);
  } else if (isAggFunc(*pNode)) {
    pCxt->hasOtherAggFunc = true;
  }
  if ((pCxt->selectFuncNum > 1 && pCxt->hasSelectValFunc) || (pCxt->hasOtherAggFunc && pCxt->hasSelectValFunc)) {
    return generateDealNodeErrMsg(pCxt->pTranslateCxt, getGroupByErrorCode(pCxt->pTranslateCxt));
  }
  if (isAggFunc(*pNode) && !isDistinctOrderBy(pCxt->pTranslateCxt)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  SNode* pGroupNode;
  FOREACH(pGroupNode, getGroupByList(pCxt->pTranslateCxt)) {
    if (nodesEqualNode(getGroupByNode(pGroupNode), *pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    if (pCxt->selectFuncNum > 1 || pCxt->hasOtherAggFunc) {
      return generateDealNodeErrMsg(pCxt->pTranslateCxt, getGroupByErrorCode(pCxt->pTranslateCxt));
    } else {
      pCxt->hasSelectValFunc = true;
      return rewriteColToSelectValFunc(pCxt->pTranslateCxt, pNode);
    }
  }
  if (isAggFunc(*pNode) && isDistinctOrderBy(pCxt->pTranslateCxt)) {
    return generateDealNodeErrMsg(pCxt->pTranslateCxt, getGroupByErrorCode(pCxt->pTranslateCxt));
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkExprForGroupBy(STranslateContext* pCxt, SNode** pNode) {
  SCheckExprForGroupByCxt cxt = {
      .pTranslateCxt = pCxt, .selectFuncNum = 0, .hasSelectValFunc = false, .hasOtherAggFunc = false};
  nodesRewriteExpr(pNode, doCheckExprForGroupBy, &cxt);
  if (cxt.selectFuncNum != 1 && cxt.hasSelectValFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, getGroupByErrorCode(pCxt));
  }
  return pCxt->errCode;
}

static int32_t checkExprListForGroupBy(STranslateContext* pCxt, SNodeList* pList) {
  if (NULL == getGroupByList(pCxt)) {
    return TSDB_CODE_SUCCESS;
  }
  SCheckExprForGroupByCxt cxt = {
      .pTranslateCxt = pCxt, .selectFuncNum = 0, .hasSelectValFunc = false, .hasOtherAggFunc = false};
  nodesRewriteExprs(pList, doCheckExprForGroupBy, &cxt);
  if (cxt.selectFuncNum != 1 && cxt.hasSelectValFunc) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, getGroupByErrorCode(pCxt));
  }
  return pCxt->errCode;
}

static EDealRes rewriteColsToSelectValFuncImpl(SNode** pNode, void* pContext) {
  if (isAggFunc(*pNode) || isIndefiniteRowsFunc(*pNode)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isScanPseudoColumnFunc(*pNode) || QUERY_NODE_COLUMN == nodeType(*pNode)) {
    return rewriteColToSelectValFunc((STranslateContext*)pContext, pNode);
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteColsToSelectValFunc(STranslateContext* pCxt, SSelectStmt* pSelect) {
  nodesRewriteExprs(pSelect->pProjectionList, rewriteColsToSelectValFuncImpl, pCxt);
  if (TSDB_CODE_SUCCESS == pCxt->errCode && !pSelect->isDistinct) {
    nodesRewriteExprs(pSelect->pOrderByList, rewriteColsToSelectValFuncImpl, pCxt);
  }
  return pCxt->errCode;
}

typedef struct CheckAggColCoexistCxt {
  STranslateContext* pTranslateCxt;
  bool               existAggFunc;
  bool               existCol;
  bool               existIndefiniteRowsFunc;
  int32_t            selectFuncNum;
  bool               existOtherAggFunc;
} CheckAggColCoexistCxt;

static EDealRes doCheckAggColCoexist(SNode* pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  if (isSelectFunc(pNode)) {
    ++(pCxt->selectFuncNum);
  } else if (isAggFunc(pNode)) {
    pCxt->existOtherAggFunc = true;
  }
  if (isAggFunc(pNode)) {
    pCxt->existAggFunc = true;
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isIndefiniteRowsFunc(pNode)) {
    pCxt->existIndefiniteRowsFunc = true;
    return DEAL_RES_IGNORE_CHILD;
  }
  if (isScanPseudoColumnFunc(pNode) || QUERY_NODE_COLUMN == nodeType(pNode)) {
    pCxt->existCol = true;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt,
                               .existAggFunc = false,
                               .existCol = false,
                               .existIndefiniteRowsFunc = false,
                               .selectFuncNum = 0,
                               .existOtherAggFunc = false};
  nodesWalkExprs(pSelect->pProjectionList, doCheckAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesWalkExprs(pSelect->pOrderByList, doCheckAggColCoexist, &cxt);
  }
  if (1 == cxt.selectFuncNum && !cxt.existOtherAggFunc) {
    return rewriteColsToSelectValFunc(pCxt, pSelect);
  }
  if ((cxt.selectFuncNum > 1 || cxt.existAggFunc || NULL != pSelect->pWindow) && cxt.existCol) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SINGLE_GROUP);
  }
  if (cxt.existIndefiniteRowsFunc && cxt.existCol) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_ALLOWED_FUNC);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t toVgroupsInfo(SArray* pVgs, SVgroupsInfo** pVgsInfo) {
  size_t vgroupNum = taosArrayGetSize(pVgs);
  *pVgsInfo = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * vgroupNum);
  if (NULL == *pVgsInfo) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pVgsInfo)->numOfVgroups = vgroupNum;
  for (int32_t i = 0; i < vgroupNum; ++i) {
    SVgroupInfo* vg = taosArrayGet(pVgs, i);
    (*pVgsInfo)->vgroups[i] = *vg;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t addMnodeToVgroupList(const SEpSet* pEpSet, SArray** pVgroupList) {
  if (NULL == *pVgroupList) {
    *pVgroupList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVgroupInfo));
    if (NULL == *pVgroupList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  SVgroupInfo vg = {.vgId = MNODE_HANDLE};
  memcpy(&vg.epSet, pEpSet, sizeof(SEpSet));
  taosArrayPush(*pVgroupList, &vg);
  return TSDB_CODE_SUCCESS;
}

static int32_t setSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_USER_TABLES)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SArray* vgroupList = NULL;
  if ('\0' != pRealTable->qualDbName[0]) {
    // todo release after mnode can be processed
    if (0 != strcmp(pRealTable->qualDbName, TSDB_INFORMATION_SCHEMA_DB)) {
      code = getDBVgInfo(pCxt, pRealTable->qualDbName, &vgroupList);
    }
  } else {
    code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
  }

  // todo release after mnode can be processed
  if (TSDB_CODE_SUCCESS == code) {
    code = addMnodeToVgroupList(&pCxt->pParseCxt->mgmtEpSet, &vgroupList);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = toVgroupsInfo(vgroupList, &pRealTable->pVgroupList);
  }
  taosArrayDestroy(vgroupList);

  return code;
}

static int32_t setTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (pCxt->pParseCxt->topicQuery) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (TSDB_SUPER_TABLE == pRealTable->pMeta->tableType) {
    SArray* vgroupList = NULL;
    code = getTableDistVgInfo(pCxt, pName, &vgroupList);
    if (TSDB_CODE_SUCCESS == code) {
      code = toVgroupsInfo(vgroupList, &pRealTable->pVgroupList);
    }
    taosArrayDestroy(vgroupList);
  } else if (TSDB_SYSTEM_TABLE == pRealTable->pMeta->tableType) {
    code = setSysTableVgroupList(pCxt, pName, pRealTable);
  } else {
    pRealTable->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
    if (NULL == pRealTable->pVgroupList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pRealTable->pVgroupList->numOfVgroups = 1;
    code = getTableHashVgroupImpl(pCxt, pName, pRealTable->pVgroupList->vgroups);
  }
  return code;
}

static uint8_t getStmtPrecision(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return ((SSelectStmt*)pStmt)->precision;
  }
  return 0;
}

static bool stmtIsSingleTable(SNode* pStmt) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    return ((STableNode*)((SSelectStmt*)pStmt)->pFromTable)->singleTable;
  }
  return false;
}

static uint8_t getJoinTablePrecision(SJoinTableNode* pJoinTable) {
  uint8_t lp = ((STableNode*)pJoinTable->pLeft)->precision;
  uint8_t rp = ((STableNode*)pJoinTable->pRight)->precision;
  return (lp > rp ? rp : lp);
}

static bool joinTableIsSingleTable(SJoinTableNode* pJoinTable) {
  return (((STableNode*)pJoinTable->pLeft)->singleTable && ((STableNode*)pJoinTable->pRight)->singleTable);
}

static bool isSingleTable(SRealTableNode* pRealTable) {
  int8_t tableType = pRealTable->pMeta->tableType;
  if (TSDB_SYSTEM_TABLE == tableType) {
    return 0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_USER_TABLES);
  }
  return (TSDB_CHILD_TABLE == tableType || TSDB_NORMAL_TABLE == tableType);
}

static int32_t translateTable(STranslateContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pRealTable = (SRealTableNode*)pTable;
      pRealTable->ratio = (NULL != pCxt->pExplainOpt ? pCxt->pExplainOpt->ratio : 1.0);
      // The SRealTableNode created through ROLLUP already has STableMeta.
      if (NULL == pRealTable->pMeta) {
        SName name;
        code = getTableMetaImpl(
            pCxt, toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name),
            &(pRealTable->pMeta));
        if (TSDB_CODE_SUCCESS != code) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, pRealTable->table.tableName);
        }
        code = setTableVgroupList(pCxt, &name, pRealTable);
      }
      pRealTable->table.precision = pRealTable->pMeta->tableInfo.precision;
      pRealTable->table.singleTable = isSingleTable(pRealTable);
      if (TSDB_CODE_SUCCESS == code) {
        code = addNamespace(pCxt, pRealTable);
      }
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      STempTableNode* pTempTable = (STempTableNode*)pTable;
      code = translateSubquery(pCxt, pTempTable->pSubquery);
      if (TSDB_CODE_SUCCESS == code) {
        pTempTable->table.precision = getStmtPrecision(pTempTable->pSubquery);
        pTempTable->table.singleTable = stmtIsSingleTable(pTempTable->pSubquery);
        code = addNamespace(pCxt, pTempTable);
      }
      break;
    }
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoinTable = (SJoinTableNode*)pTable;
      code = translateTable(pCxt, pJoinTable->pLeft);
      if (TSDB_CODE_SUCCESS == code) {
        code = translateTable(pCxt, pJoinTable->pRight);
      }
      if (TSDB_CODE_SUCCESS == code) {
        pJoinTable->table.precision = getJoinTablePrecision(pJoinTable);
        pJoinTable->table.singleTable = joinTableIsSingleTable(pJoinTable);
        code = translateExpr(pCxt, &pJoinTable->pOnCond);
      }
      break;
    }
    default:
      break;
  }
  return code;
}

static int32_t createAllColumns(STranslateContext* pCxt, SNodeList** pCols) {
  *pCols = nodesMakeList();
  if (NULL == *pCols) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
  }
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    int32_t     code = createColumnsByTable(pCxt, pTable, *pCols);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SNode* createMultiResFunc(SFunctionNode* pSrcFunc, SExprNode* pExpr) {
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  pFunc->pParameterList = nodesMakeList();
  if (NULL == pFunc->pParameterList ||
      TSDB_CODE_SUCCESS != nodesListStrictAppend(pFunc->pParameterList, nodesCloneNode(pExpr))) {
    nodesDestroyNode(pFunc);
    return NULL;
  }

  pFunc->node.resType = pExpr->resType;
  pFunc->funcId = pSrcFunc->funcId;
  pFunc->funcType = pSrcFunc->funcType;
  strcpy(pFunc->functionName, pSrcFunc->functionName);
  char    buf[TSDB_FUNC_NAME_LEN + TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = 0;
  if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
    SColumnNode* pCol = (SColumnNode*)pExpr;
    len = snprintf(buf, sizeof(buf), "%s(%s.%s)", pSrcFunc->functionName, pCol->tableAlias, pCol->colName);
  } else {
    len = snprintf(buf, sizeof(buf), "%s(%s)", pSrcFunc->functionName, pExpr->aliasName);
  }
  strncpy(pFunc->node.aliasName, buf, TMIN(len, sizeof(pFunc->node.aliasName) - 1));

  return (SNode*)pFunc;
}

static int32_t createTableAllCols(STranslateContext* pCxt, SColumnNode* pCol, SNodeList** pOutput) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, pCol->tableAlias, &pTable);
  if (TSDB_CODE_SUCCESS == code && NULL == *pOutput) {
    *pOutput = nodesMakeList();
    if (NULL == *pOutput) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = createColumnsByTable(pCxt, pTable, *pOutput);
  }
  return code;
}

static bool isStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

static bool isTableStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' != ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

static int32_t createMultiResFuncsParas(STranslateContext* pCxt, SNodeList* pSrcParas, SNodeList** pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList* pExprs = NULL;
  SNode*     pPara = NULL;
  FOREACH(pPara, pSrcParas) {
    if (isStar(pPara)) {
      code = createAllColumns(pCxt, &pExprs);
      // The syntax definition ensures that * and other parameters do not appear at the same time
      break;
    } else if (isTableStar(pPara)) {
      code = createTableAllCols(pCxt, (SColumnNode*)pPara, &pExprs);
    } else {
      code = nodesListMakeStrictAppend(&pExprs, nodesCloneNode(pPara));
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pExprs;
  } else {
    nodesDestroyList(pExprs);
  }

  return code;
}

static int32_t createMultiResFuncs(SFunctionNode* pSrcFunc, SNodeList* pExprs, SNodeList** pOutput) {
  SNodeList* pFuncs = nodesMakeList();
  if (NULL == pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pExpr = NULL;
  FOREACH(pExpr, pExprs) {
    code = nodesListStrictAppend(pFuncs, createMultiResFunc(pSrcFunc, (SExprNode*)pExpr));
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pFuncs;
  } else {
    nodesDestroyList(pFuncs);
  }

  return code;
}

static int32_t createMultiResFuncsFromStar(STranslateContext* pCxt, SFunctionNode* pSrcFunc, SNodeList** pOutput) {
  SNodeList* pExprs = NULL;
  int32_t    code = createMultiResFuncsParas(pCxt, pSrcFunc->pParameterList, &pExprs);
  if (TSDB_CODE_SUCCESS == code) {
    code = createMultiResFuncs(pSrcFunc, pExprs, pOutput);
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pExprs);
  }

  return code;
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pProjectionList) {  // select * ...
    return createAllColumns(pCxt, &pSelect->pProjectionList);
  } else {
    SNode* pNode = NULL;
    WHERE_EACH(pNode, pSelect->pProjectionList) {
      int32_t code = TSDB_CODE_SUCCESS;
      if (isMultiResFunc(pNode)) {
        SNodeList* pFuncs = NULL;
        code = createMultiResFuncsFromStar(pCxt, (SFunctionNode*)pNode, &pFuncs);
        if (TSDB_CODE_SUCCESS == code) {
          INSERT_LIST(pSelect->pProjectionList, pFuncs);
          ERASE_NODE(pSelect->pProjectionList);
          continue;
        }
      } else if (isTableStar(pNode)) {
        SNodeList* pCols = NULL;
        code = createTableAllCols(pCxt, (SColumnNode*)pNode, &pCols);
        if (TSDB_CODE_SUCCESS == code) {
          INSERT_LIST(pSelect->pProjectionList, pCols);
          ERASE_NODE(pSelect->pProjectionList);
          continue;
        }
      }
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      WHERE_NEXT;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getPositionValue(const SValueNode* pVal) {
  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
      return -1;
    case TSDB_DATA_TYPE_BOOL:
      return (pVal->datum.b ? 1 : 0);
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
      return pVal->datum.i;
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return pVal->datum.d;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return pVal->datum.u;
    default:
      break;
  }
  return -1;
}

static int32_t translateOrderByPosition(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pOrderByList,
                                        bool* pOther) {
  *pOther = false;
  SNode* pNode = NULL;
  WHERE_EACH(pNode, pOrderByList) {
    SNode* pExpr = ((SOrderByExprNode*)pNode)->pExpr;
    if (QUERY_NODE_VALUE == nodeType(pExpr)) {
      SValueNode* pVal = (SValueNode*)pExpr;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
      int32_t pos = getPositionValue(pVal);
      if (pos < 0) {
        ERASE_NODE(pOrderByList);
        continue;
      } else if (0 == pos || pos > LIST_LENGTH(pProjectionList)) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);
      } else {
        SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
        if (NULL == pCol) {
          return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_OUT_OF_MEMORY);
        }
        setColumnInfoByExpr(NULL, (SExprNode*)nodesListGetNode(pProjectionList, pos - 1), &pCol);
        ((SOrderByExprNode*)pNode)->pExpr = (SNode*)pCol;
        nodesDestroyNode(pExpr);
      }
    } else {
      *pOther = true;
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateOrderBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  bool    other;
  int32_t code = translateOrderByPosition(pCxt, pSelect->pProjectionList, pSelect->pOrderByList, &other);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  if (!other) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->currClause = SQL_CLAUSE_ORDER_BY;
  code = translateExprList(pCxt, pSelect->pOrderByList);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprListForGroupBy(pCxt, pSelect->pOrderByList);
  }
  return code;
}

static int32_t translateSelectList(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_SELECT;
  int32_t code = translateExprList(pCxt, pSelect->pProjectionList);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateStar(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprListForGroupBy(pCxt, pSelect->pProjectionList);
  }
  return code;
}

static int32_t translateHaving(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList && NULL != pSelect->pHaving) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
  }
  pCxt->currClause = SQL_CLAUSE_HAVING;
  int32_t code = translateExpr(pCxt, &pSelect->pHaving);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprForGroupBy(pCxt, &pSelect->pHaving);
  }
  return code;
}

static int32_t translateGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList && NULL != pSelect->pWindow) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST);
  }
  if (NULL != pSelect->pGroupByList) {
    pCxt->currClause = SQL_CLAUSE_GROUP_BY;
    pSelect->isTimeOrderQuery = false;
    return translateExprList(pCxt, pSelect->pGroupByList);
  }
  return TSDB_CODE_SUCCESS;
}

static EDealRes isPrimaryKeyCondImpl(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *((bool*)pContext) = ((PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pNode)->colId) ? true : false);
    return *((bool*)pContext) ? DEAL_RES_CONTINUE : DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static bool isPrimaryKeyCond(SNode* pNode) {
  bool isPrimaryKeyCond = false;
  nodesWalkExpr(pNode, isPrimaryKeyCondImpl, &isPrimaryKeyCond);
  return isPrimaryKeyCond;
}

static int32_t getTimeRangeFromLogicCond(STranslateContext* pCxt, SLogicConditionNode* pLogicCond,
                                         STimeWindow* pTimeRange) {
  SNodeList* pPrimaryKeyConds = NULL;
  SNode*     pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    if (isPrimaryKeyCond(pCond)) {
      if (TSDB_CODE_SUCCESS != nodesListMakeAppend(&pPrimaryKeyConds, pCond)) {
        nodesClearList(pPrimaryKeyConds);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  if (NULL == pPrimaryKeyConds) {
    *pTimeRange = TSWINDOW_INITIALIZER;
    return TSDB_CODE_SUCCESS;
  }

  SLogicConditionNode* pPrimaryKeyLogicCond = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pPrimaryKeyLogicCond) {
    nodesClearList(pPrimaryKeyConds);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pPrimaryKeyLogicCond->condType = LOGIC_COND_TYPE_AND;
  pPrimaryKeyLogicCond->pParameterList = pPrimaryKeyConds;
  bool    isStrict = false;
  int32_t code = filterGetTimeRange((SNode*)pPrimaryKeyLogicCond, pTimeRange, &isStrict);
  nodesClearList(pPrimaryKeyConds);
  pPrimaryKeyLogicCond->pParameterList = NULL;
  nodesDestroyNode(pPrimaryKeyLogicCond);
  return code;
}

static int32_t getTimeRange(STranslateContext* pCxt, SNode* pWhere, STimeWindow* pTimeRange) {
  if (NULL == pWhere) {
    *pTimeRange = TSWINDOW_INITIALIZER;
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pWhere) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode*)pWhere)->condType) {
    return getTimeRangeFromLogicCond(pCxt, (SLogicConditionNode*)pWhere, pTimeRange);
  }

  if (isPrimaryKeyCond(pWhere)) {
    bool isStrict = false;
    return filterGetTimeRange(pWhere, pTimeRange, &isStrict);
  } else {
    *pTimeRange = TSWINDOW_INITIALIZER;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkFill(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  SFillNode* pFill = (SFillNode*)pInterval->pFill;
  if (TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_INITIALIZER) ||
      TSWINDOW_IS_EQUAL(pFill->timeRange, TSWINDOW_DESC_INITIALIZER)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  int64_t     timeRange = TABS(pFill->timeRange.skey - pFill->timeRange.ekey);
  int64_t     intervalRange = 0;
  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  if (TIME_IS_VAR_DURATION(pInter->unit)) {
    int64_t f = 1;
    if (pInter->unit == 'n') {
      f = 30L * MILLISECOND_PER_DAY;
    } else if (pInter->unit == 'y') {
      f = 365L * MILLISECOND_PER_DAY;
    }
    intervalRange = pInter->datum.i * f;
  } else {
    intervalRange = pInter->datum.i;
  }
  if ((timeRange == 0) || (timeRange / intervalRange) >= MAX_INTERVAL_TIME_WINDOW) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateFill(STranslateContext* pCxt, SNode* pWhere, SIntervalWindowNode* pInterval) {
  if (NULL == pInterval->pFill) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = getTimeRange(pCxt, pWhere, &(((SFillNode*)pInterval->pFill)->timeRange));
  if (TSDB_CODE_SUCCESS == code) {
    code = checkFill(pCxt, pInterval);
  }
  return code;
}

static int64_t getMonthsFromTimeVal(int64_t val, int32_t fromPrecision, char unit) {
  int64_t days = convertTimeFromPrecisionToUnit(val, fromPrecision, 'd');
  switch (unit) {
    case 'b':
    case 'u':
    case 'a':
    case 's':
    case 'm':
    case 'h':
    case 'd':
    case 'w':
      return days / 28;
    case 'n':
      return val;
    case 'y':
      return val * 12;
    default:
      break;
  }
  return -1;
}

static int32_t checkIntervalWindow(STranslateContext* pCxt, SNode* pWhere, SIntervalWindowNode* pInterval) {
  uint8_t precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;

  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  bool        valInter = TIME_IS_VAR_DURATION(pInter->unit);
  if (pInter->datum.i <= 0 ||
      (!valInter && convertTimePrecision(pInter->datum.i, precision, TSDB_TIME_PRECISION_MICRO) < tsMinIntervalTime)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL, tsMinIntervalTime);
  }

  if (NULL != pInterval->pOffset) {
    SValueNode* pOffset = (SValueNode*)pInterval->pOffset;
    if (pOffset->datum.i <= 0) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE);
    }
    if (pInter->unit == 'n' && pOffset->unit == 'y') {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_UNIT);
    }
    bool fixed = !TIME_IS_VAR_DURATION(pOffset->unit) && !valInter;
    if ((fixed && pOffset->datum.i >= pInter->datum.i) ||
        (!fixed && getMonthsFromTimeVal(pOffset->datum.i, precision, pOffset->unit) >=
                       getMonthsFromTimeVal(pInter->datum.i, precision, pInter->unit))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);
    }
  }

  if (NULL != pInterval->pSliding) {
    const static int32_t INTERVAL_SLIDING_FACTOR = 100;

    SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
    if (TIME_IS_VAR_DURATION(pSliding->unit)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_UNIT);
    }
    if ((pSliding->datum.i < convertTimePrecision(tsMinSlidingTime, TSDB_TIME_PRECISION_MILLI, precision)) ||
        (pInter->datum.i / pSliding->datum.i > INTERVAL_SLIDING_FACTOR)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL);
    }
    if (pSliding->datum.i > pInter->datum.i) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG);
    }
  }

  return translateFill(pCxt, pWhere, pInterval);
}

static EDealRes checkStateExpr(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    STranslateContext* pCxt = pContext;
    SColumnNode*       pCol = (SColumnNode*)pNode;

    int32_t type = pCol->node.resType.type;
    if (!IS_INTEGER_TYPE(type) && type != TSDB_DATA_TYPE_BOOL && !IS_VAR_DATA_TYPE(type)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE);
    }
    if (COLUMN_TYPE_TAG == pCol->colType) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STATE_WIN_COL);
    }
    if (TSDB_SUPER_TABLE == pCol->tableType) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STATE_WIN_TABLE);
    }
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkStateWindow(STranslateContext* pCxt, SStateWindowNode* pState) {
  nodesWalkExprPostOrder(pState->pExpr, checkStateExpr, pCxt);
  // todo check for "function not support for state_window"
  return pCxt->errCode;
}

static int32_t checkSessionWindow(STranslateContext* pCxt, SSessionWindowNode* pSession) {
  if ('y' == pSession->pGap->unit || 'n' == pSession->pGap->unit || 0 == pSession->pGap->datum.i) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_GAP);
  }
  if (PRIMARYKEY_TIMESTAMP_COL_ID != pSession->pCol->colId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_SESSION_COL);
  }
  // todo check for "function not support for session"
  return TSDB_CODE_SUCCESS;
}

static int32_t checkWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  switch (nodeType(pSelect->pWindow)) {
    case QUERY_NODE_STATE_WINDOW:
      return checkStateWindow(pCxt, (SStateWindowNode*)pSelect->pWindow);
    case QUERY_NODE_SESSION_WINDOW:
      return checkSessionWindow(pCxt, (SSessionWindowNode*)pSelect->pWindow);
    case QUERY_NODE_INTERVAL_WINDOW:
      return checkIntervalWindow(pCxt, pSelect->pWhere, (SIntervalWindowNode*)pSelect->pWindow);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateWindow(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pWindow) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->currClause = SQL_CLAUSE_WINDOW;
  int32_t code = translateExpr(pCxt, &pSelect->pWindow);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkWindow(pCxt, pSelect);
  }
  return code;
}

static int32_t translatePartitionBy(STranslateContext* pCxt, SNodeList* pPartitionByList) {
  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  return translateExprList(pCxt, pPartitionByList);
}

static int32_t translateWhere(STranslateContext* pCxt, SNode* pWhere) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  return translateExpr(pCxt, &pWhere);
}

static int32_t translateFrom(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  int32_t code = translateTable(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    pSelect->precision = ((STableNode*)pSelect->pFromTable)->precision;
  }
  return code;
}

static int32_t checkLimit(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if ((NULL != pSelect->pLimit && pSelect->pLimit->offset < 0) ||
      (NULL != pSelect->pSlimit && pSelect->pSlimit->offset < 0)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_OFFSET_LESS_ZERO);
  }

  if (NULL != pSelect->pSlimit && NULL == pSelect->pPartitionByList) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_BY);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createPrimaryKeyColByTable(STranslateContext* pCxt, STableNode* pTable, SNode** pPrimaryKey) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(pCol->colName, PK_TS_COL_INTERNAL_NAME);
  if (!findAndSetColumn(&pCol, pTable)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TIMELINE_FUNC);
  }
  *pPrimaryKey = (SNode*)pCol;
  return TSDB_CODE_SUCCESS;
}

static int32_t createPrimaryKeyCol(STranslateContext* pCxt, SNode** pPrimaryKey) {
  STableNode* pTable = NULL;
  int32_t     code = findTable(pCxt, NULL, &pTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = createPrimaryKeyColByTable(pCxt, pTable, pPrimaryKey);
  }
  return code;
}

static EDealRes rewriteTimelineFuncImpl(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = pContext;
  if (isTimelineFunc(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    SNode*         pPrimaryKey = NULL;
    pCxt->errCode = createPrimaryKeyCol(pCxt, &pPrimaryKey);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListMakeStrictAppend(&pFunc->pParameterList, pPrimaryKey);
    }
    return TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewriteTimelineFunc(STranslateContext* pCxt, SSelectStmt* pSelect) {
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_FROM, rewriteTimelineFuncImpl, pCxt);
  return pCxt->errCode;
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = pSelect;
  int32_t code = translateFrom(pCxt, pSelect);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateWhere(pCxt, pSelect->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translatePartitionBy(pCxt, pSelect->pPartitionByList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateWindow(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateGroupBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateHaving(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSelectList(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateOrderBy(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkAggColCoexist(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkLimit(pCxt, pSelect);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteTimelineFunc(pCxt, pSelect);
  }
  return code;
}

static SNode* createSetOperProject(const char* pTableAlias, SNode* pNode) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = ((SExprNode*)pNode)->resType;
  strcpy(pCol->tableAlias, pTableAlias);
  strcpy(pCol->colName, ((SExprNode*)pNode)->aliasName);
  strcpy(pCol->node.aliasName, pCol->colName);
  return (SNode*)pCol;
}

static bool dataTypeEqual(const SDataType* l, const SDataType* r) {
  return (l->type == r->type && l->bytes == r->bytes && l->precision == r->precision && l->scale == r->scale);
}

static int32_t createCastFunc(STranslateContext* pCxt, SNode* pExpr, SDataType dt, SNode** pCast) {
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "cast");
  pFunc->node.resType = dt;
  if (TSDB_CODE_SUCCESS != nodesListMakeAppend(&pFunc->pParameterList, pExpr)) {
    nodesDestroyNode(pFunc);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS != getFuncInfo(pCxt, pFunc)) {
    nodesClearList(pFunc->pParameterList);
    pFunc->pParameterList = NULL;
    nodesDestroyNode(pFunc);
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)pExpr)->aliasName);
  }
  *pCast = (SNode*)pFunc;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSetOperatorImpl(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  SNodeList* pLeftProjections = getProjectList(pSetOperator->pLeft);
  SNodeList* pRightProjections = getProjectList(pSetOperator->pRight);
  if (LIST_LENGTH(pLeftProjections) != LIST_LENGTH(pRightProjections)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCORRECT_NUM_OF_COL);
  }

  SNode* pLeft = NULL;
  SNode* pRight = NULL;
  FORBOTH(pLeft, pLeftProjections, pRight, pRightProjections) {
    SExprNode* pLeftExpr = (SExprNode*)pLeft;
    SExprNode* pRightExpr = (SExprNode*)pRight;
    if (!dataTypeEqual(&pLeftExpr->resType, &pRightExpr->resType)) {
      SNode*  pRightFunc = NULL;
      int32_t code = createCastFunc(pCxt, pRight, pLeftExpr->resType, &pRightFunc);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      REPLACE_LIST2_NODE(pRightFunc);
      pRightExpr = (SExprNode*)pRightFunc;
    }
    strcpy(pRightExpr->aliasName, pLeftExpr->aliasName);
    pRightExpr->aliasName[strlen(pLeftExpr->aliasName)] = '\0';
    if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pSetOperator->pProjectionList,
                                                       createSetOperProject(pSetOperator->stmtName, pLeft))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSetOperator(STranslateContext* pCxt, SSetOperator* pSetOperator) {
  int32_t code = translateQuery(pCxt, pSetOperator->pLeft);
  if (TSDB_CODE_SUCCESS == code) {
    code = resetTranslateNamespace(pCxt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(pCxt, pSetOperator->pRight);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateSetOperatorImpl(pCxt, pSetOperator);
  }
  return code;
}

static int64_t getUnitPerMinute(uint8_t precision) {
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      return MILLISECOND_PER_MINUTE;
    case TSDB_TIME_PRECISION_MICRO:
      return MILLISECOND_PER_MINUTE * 1000L;
    case TSDB_TIME_PRECISION_NANO:
      return NANOSECOND_PER_MINUTE;
    default:
      break;
  }
  return MILLISECOND_PER_MINUTE;
}

static int64_t getBigintFromValueNode(SValueNode* pVal) {
  if (pVal->isDuration) {
    return pVal->datum.i / getUnitPerMinute(pVal->node.resType.precision);
  }
  return pVal->datum.i;
}

static int32_t buildCreateDbRetentions(const SNodeList* pRetentions, SCreateDbReq* pReq) {
  if (NULL != pRetentions) {
    pReq->pRetensions = taosArrayInit(LIST_LENGTH(pRetentions), sizeof(SRetention));
    if (NULL == pReq->pRetensions) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SValueNode* pFreq = NULL;
    SValueNode* pKeep = NULL;
    SNode*      pNode = NULL;
    int32_t     index = 0;
    FOREACH(pNode, pRetentions) {
      pFreq = (SValueNode*)nodesListGetNode(((SNodeListNode*)pNode)->pNodeList, 0);
      pKeep = (SValueNode*)nodesListGetNode(((SNodeListNode*)pNode)->pNodeList, 1);
      SRetention retention = {
          .freq = pFreq->datum.i, .freqUnit = pFreq->unit, .keep = pKeep->datum.i, .keepUnit = pKeep->unit};
      taosArrayPush(pReq->pRetensions, &retention);
    }
    pReq->numOfRetensions = taosArrayGetSize(pReq->pRetensions);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildCreateDbReq(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt, SCreateDbReq* pReq) {
  SName name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, pReq->db);
  pReq->numOfVgroups = pStmt->pOptions->numOfVgroups;
  pReq->numOfStables = pStmt->pOptions->singleStable;
  pReq->buffer = pStmt->pOptions->buffer;
  pReq->pageSize = pStmt->pOptions->pagesize;
  pReq->pages = pStmt->pOptions->pages;
  pReq->daysPerFile = pStmt->pOptions->daysPerFile;
  pReq->daysToKeep0 = pStmt->pOptions->keep[0];
  pReq->daysToKeep1 = pStmt->pOptions->keep[1];
  pReq->daysToKeep2 = pStmt->pOptions->keep[2];
  pReq->minRows = pStmt->pOptions->minRowsPerBlock;
  pReq->maxRows = pStmt->pOptions->maxRowsPerBlock;
  pReq->fsyncPeriod = pStmt->pOptions->fsyncPeriod;
  pReq->walLevel = pStmt->pOptions->walLevel;
  pReq->precision = pStmt->pOptions->precision;
  pReq->compression = pStmt->pOptions->compressionLevel;
  pReq->replications = pStmt->pOptions->replica;
  pReq->strict = pStmt->pOptions->strict;
  pReq->cacheLastRow = pStmt->pOptions->cachelast;
  pReq->schemaless = pStmt->pOptions->schemaless;
  pReq->ignoreExist = pStmt->ignoreExists;
  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t minVal,
                                int32_t maxVal) {
  if (val >= 0 && (val < minVal || val > maxVal)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_RANGE_OPTION, pName, val, minVal, maxVal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbDaysOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL != pOptions->pDaysPerFile) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pOptions->pDaysPerFile)) {
      return pCxt->errCode;
    }
    if (TIME_UNIT_MINUTE != pOptions->pDaysPerFile->unit && TIME_UNIT_HOUR != pOptions->pDaysPerFile->unit &&
        TIME_UNIT_DAY != pOptions->pDaysPerFile->unit) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_UNIT, "daysPerFile",
                                  pOptions->pDaysPerFile->unit);
    }
    pOptions->daysPerFile = getBigintFromValueNode(pOptions->pDaysPerFile);
  }
  return checkRangeOption(pCxt, "daysPerFile", pOptions->daysPerFile, TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
}

static int32_t checkDbKeepOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if (NULL == pOptions->pKeep) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfKeep = LIST_LENGTH(pOptions->pKeep);
  if (numOfKeep > 3 || numOfKeep < 1) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_NUM);
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pOptions->pKeep) {
    SValueNode* pVal = (SValueNode*)pNode;
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (pVal->isDuration && TIME_UNIT_MINUTE != pVal->unit && TIME_UNIT_HOUR != pVal->unit &&
        TIME_UNIT_DAY != pVal->unit) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_UNIT, pVal->unit);
    }
    if (!pVal->isDuration) {
      pVal->datum.i = pVal->datum.i * 1440;
    }
  }

  pOptions->keep[0] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 0));

  if (numOfKeep < 2) {
    pOptions->keep[1] = pOptions->keep[0];
  } else {
    pOptions->keep[1] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 1));
  }
  if (numOfKeep < 3) {
    pOptions->keep[2] = pOptions->keep[1];
  } else {
    pOptions->keep[2] = getBigintFromValueNode((SValueNode*)nodesListGetNode(pOptions->pKeep, 2));
  }

  if (pOptions->keep[0] < TSDB_MIN_KEEP || pOptions->keep[1] < TSDB_MIN_KEEP || pOptions->keep[2] < TSDB_MIN_KEEP ||
      pOptions->keep[0] > TSDB_MAX_KEEP || pOptions->keep[1] > TSDB_MAX_KEEP || pOptions->keep[2] > TSDB_MAX_KEEP) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_VALUE, pOptions->keep[0], pOptions->keep[1],
                                pOptions->keep[2], TSDB_MIN_KEEP, TSDB_MAX_KEEP);
  }

  if (!((pOptions->keep[0] <= pOptions->keep[1]) && (pOptions->keep[1] <= pOptions->keep[2]))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_ORDER);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbPrecisionOption(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
  if ('\0' != pOptions->precisionStr[0]) {
    if (0 == strcmp(pOptions->precisionStr, TSDB_TIME_PRECISION_MILLI_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strcmp(pOptions->precisionStr, TSDB_TIME_PRECISION_MICRO_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_MICRO;
    } else if (0 == strcmp(pOptions->precisionStr, TSDB_TIME_PRECISION_NANO_STR)) {
      pOptions->precision = TSDB_TIME_PRECISION_NANO;
    } else {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STR_OPTION, "precision", pOptions->precisionStr);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbEnumOption(STranslateContext* pCxt, const char* pName, int32_t val, int32_t v1, int32_t v2) {
  if (val >= 0 && val != v1 && val != v2) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ENUM_OPTION, pName, val, v1, v2);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRetentionsOption(STranslateContext* pCxt, SNodeList* pRetentions) {
  if (NULL == pRetentions) {
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pRetentions) > 3) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
  }

  SNode* pRetention = NULL;
  FOREACH(pRetention, pRetentions) {
    SNode* pNode = NULL;
    FOREACH(pNode, ((SNodeListNode*)pRetention)->pNodeList) {
      SValueNode* pVal = (SValueNode*)pNode;
      if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkOptionsDependency(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t daysPerFile = pOptions->daysPerFile;
  int32_t daysToKeep0 = pOptions->keep[0];
  if (-1 == daysPerFile && -1 == daysToKeep0) {
    return TSDB_CODE_SUCCESS;
  } else if (-1 == daysPerFile || -1 == daysToKeep0) {
    SDbCfgInfo dbCfg;
    int32_t    code = getDBCfg(pCxt, pDbName, &dbCfg);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    daysPerFile = (-1 == daysPerFile ? dbCfg.daysPerFile : daysPerFile);
    daysToKeep0 = (-1 == daysToKeep0 ? dbCfg.daysToKeep0 : daysToKeep0);
  }
  if (daysPerFile > daysToKeep0) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DAYS_VALUE);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDatabaseOptions(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions) {
  int32_t code =
      checkRangeOption(pCxt, "buffer", pOptions->buffer, TSDB_MIN_BUFFER_PER_VNODE, TSDB_MAX_BUFFER_PER_VNODE);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "cacheLast", pOptions->cachelast, TSDB_MIN_DB_CACHE_LAST_ROW,
                            TSDB_MAX_DB_CACHE_LAST_ROW);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "compression", pOptions->compressionLevel, TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbDaysOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "fsyncPeriod", pOptions->fsyncPeriod, TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "maxRowsPerBlock", pOptions->maxRowsPerBlock, TSDB_MIN_MAXROWS_FBLOCK,
                            TSDB_MAX_MAXROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "minRowsPerBlock", pOptions->minRowsPerBlock, TSDB_MIN_MINROWS_FBLOCK,
                            TSDB_MAX_MINROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbKeepOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "pages", pOptions->pages, TSDB_MIN_PAGES_PER_VNODE, TSDB_MAX_PAGES_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "pagesize", pOptions->pagesize, TSDB_MIN_PAGESIZE_PER_VNODE,
                            TSDB_MAX_PAGESIZE_PER_VNODE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbPrecisionOption(pCxt, pOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "replications", pOptions->replica, TSDB_MIN_DB_REPLICA, TSDB_MAX_DB_REPLICA);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "strict", pOptions->strict, TSDB_DB_STRICT_OFF, TSDB_DB_STRICT_ON);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "walLevel", pOptions->walLevel, TSDB_MIN_WAL_LEVEL, TSDB_MAX_WAL_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "vgroups", pOptions->numOfVgroups, TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "singleStable", pOptions->singleStable, TSDB_DB_SINGLE_STABLE_ON,
                             TSDB_DB_SINGLE_STABLE_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRetentionsOption(pCxt, pOptions->pRetentions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "schemaless", pOptions->schemaless, TSDB_DB_SCHEMALESS_ON, TSDB_DB_SCHEMALESS_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkOptionsDependency(pCxt, pDbName, pOptions);
  }
  return code;
}

static int32_t checkCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  return checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
}

typedef int32_t (*FSerializeFunc)(void* pBuf, int32_t bufLen, void* pReq);

static int32_t buildCmdMsg(STranslateContext* pCxt, int16_t msgType, FSerializeFunc func, void* pReq) {
  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = msgType;
  pCxt->pCmdMsg->msgLen = func(NULL, 0, pReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  func(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, pReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  SCreateDbReq createReq = {0};

  int32_t code = checkCreateDatabase(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateDbReq(pCxt, pStmt, &createReq);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_DB, (FSerializeFunc)tSerializeSCreateDbReq, &createReq);
  }

  return code;
}

static int32_t translateDropDatabase(STranslateContext* pCxt, SDropDatabaseStmt* pStmt) {
  SDropDbReq dropReq = {0};
  SName      name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, dropReq.db);
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_DB, (FSerializeFunc)tSerializeSDropDbReq, &dropReq);
}

static void buildAlterDbReq(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt, SAlterDbReq* pReq) {
  SName name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, pReq->db);
  pReq->buffer = pStmt->pOptions->buffer;
  pReq->pageSize = -1;
  pReq->pages = pStmt->pOptions->pages;
  pReq->daysPerFile = -1;
  pReq->daysToKeep0 = pStmt->pOptions->keep[0];
  pReq->daysToKeep1 = pStmt->pOptions->keep[1];
  pReq->daysToKeep2 = pStmt->pOptions->keep[2];
  pReq->fsyncPeriod = pStmt->pOptions->fsyncPeriod;
  pReq->walLevel = pStmt->pOptions->walLevel;
  pReq->strict = pStmt->pOptions->strict;
  pReq->cacheLastRow = pStmt->pOptions->cachelast;
  pReq->replications = pStmt->pOptions->replica;
  return;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  int32_t code = checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SAlterDbReq alterReq = {0};
  buildAlterDbReq(pCxt, pStmt, &alterReq);

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_DB, (FSerializeFunc)tSerializeSAlterDbReq, &alterReq);
}

static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    SField          field = {.type = pCol->dataType.type, .bytes = calcTypeBytes(pCol->dataType)};
    strcpy(field.name, pCol->colName);
    if (pCol->sma) {
      field.flags |= COL_SMA_ON;
    }
    taosArrayPush(*pArray, &field);
  }
  return TSDB_CODE_SUCCESS;
}

static SColumnDefNode* findColDef(SNodeList* pCols, const SColumnNode* pCol) {
  SNode* pColDef = NULL;
  FOREACH(pColDef, pCols) {
    if (0 == strcmp(pCol->colName, ((SColumnDefNode*)pColDef)->colName)) {
      return (SColumnDefNode*)pColDef;
    }
  }
  return NULL;
}

static int32_t checTableFactorOption(STranslateContext* pCxt, float val) {
  if (val < TSDB_MIN_ROLLUP_FILE_FACTOR || val > TSDB_MAX_ROLLUP_FILE_FACTOR) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_F_RANGE_OPTION, "file_factor", val,
                                TSDB_MIN_ROLLUP_FILE_FACTOR, TSDB_MAX_ROLLUP_FILE_FACTOR);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTableSmaOption(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  if (NULL != pStmt->pOptions->pSma) {
    SNode* pNode = NULL;
    FOREACH(pNode, pStmt->pCols) { ((SColumnDefNode*)pNode)->sma = false; }
    FOREACH(pNode, pStmt->pOptions->pSma) {
      SColumnNode*    pSmaCol = (SColumnNode*)pNode;
      SColumnDefNode* pColDef = findColDef(pStmt->pCols, pSmaCol);
      if (NULL == pColDef) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pSmaCol->colName);
      }
      pSmaCol->node.resType = pColDef->dataType;
      pColDef->sma = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTableRollupOption(STranslateContext* pCxt, SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 != LIST_LENGTH(pFuncs)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROLLUP_OPTION);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTableTagsSchema(STranslateContext* pCxt, SHashObj* pHash, SNodeList* pTags) {
  int32_t ntags = LIST_LENGTH(pTags);
  if (0 == ntags) {
    return TSDB_CODE_SUCCESS;
  } else if (ntags > TSDB_MAX_TAGS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_NUM);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t tagsSize = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pTags) {
    SColumnDefNode* pTag = (SColumnDefNode*)pNode;
    int32_t         len = strlen(pTag->colName);
    if (NULL != taosHashGet(pHash, pTag->colName, len)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
    }
    if (TSDB_CODE_SUCCESS == code && pTag->dataType.type == TSDB_DATA_TYPE_JSON && ntags > 1) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if ((TSDB_DATA_TYPE_VARCHAR == pTag->dataType.type && pTag->dataType.bytes > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pTag->dataType.type && pTag->dataType.bytes > TSDB_MAX_NCHAR_LEN)) {
        code = code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pTag->colName, len, &pTag, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      tagsSize += pTag->dataType.bytes;
    } else {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && tagsSize > TSDB_MAX_TAGS_LEN) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAGS_LENGTH, TSDB_MAX_TAGS_LEN);
  }

  return code;
}

static int32_t checkTableColsSchema(STranslateContext* pCxt, SHashObj* pHash, SNodeList* pCols) {
  int32_t ncols = LIST_LENGTH(pCols);
  if (ncols < TSDB_MIN_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  } else if (ncols > TSDB_MAX_COLUMNS) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TOO_MANY_COLUMNS);
  }

  int32_t code = TSDB_CODE_SUCCESS;

  bool    first = true;
  int32_t rowSize = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pCols) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (first) {
      first = false;
      if (TSDB_DATA_TYPE_TIMESTAMP != pCol->dataType.type) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FIRST_COLUMN);
      }
    }
    if (TSDB_CODE_SUCCESS == code && pCol->dataType.type == TSDB_DATA_TYPE_JSON) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
    }
    int32_t len = strlen(pCol->colName);
    if (TSDB_CODE_SUCCESS == code && NULL != taosHashGet(pHash, pCol->colName, len)) {
      code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
    }
    if (TSDB_CODE_SUCCESS == code) {
      if ((TSDB_DATA_TYPE_VARCHAR == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_BINARY_LEN) ||
          (TSDB_DATA_TYPE_NCHAR == pCol->dataType.type && calcTypeBytes(pCol->dataType) > TSDB_MAX_NCHAR_LEN)) {
        code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = taosHashPut(pHash, pCol->colName, len, &pCol, POINTER_BYTES);
    }
    if (TSDB_CODE_SUCCESS == code) {
      rowSize += pCol->dataType.bytes;
    } else {
      break;
    }
  }

  if (TSDB_CODE_SUCCESS == code && rowSize > TSDB_MAX_BYTES_PER_ROW) {
    code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ROW_LENGTH, TSDB_MAX_BYTES_PER_ROW);
  }

  return code;
}

static int32_t checkTableSchema(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  SHashObj* pHash = taosHashInit(LIST_LENGTH(pStmt->pTags) + LIST_LENGTH(pStmt->pCols),
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (NULL == pHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = checkTableTagsSchema(pCxt, pHash, pStmt->pTags);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableColsSchema(pCxt, pHash, pStmt->pCols);
  }

  taosHashCleanup(pHash);
  return code;
}

static int32_t checkCreateTable(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  int32_t code = checkRangeOption(pCxt, "delay", pStmt->pOptions->delay, TSDB_MIN_ROLLUP_DELAY, TSDB_MAX_ROLLUP_DELAY);
  if (TSDB_CODE_SUCCESS == code) {
    code = checTableFactorOption(pCxt, pStmt->pOptions->filesFactor);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableRollupOption(pCxt, pStmt->pOptions->pRollupFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "ttl", pStmt->pOptions->ttl, TSDB_MIN_TABLE_TTL, INT32_MAX);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSmaOption(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSchema(pCxt, pStmt);
  }
  return code;
}

static void toSchema(const SColumnDefNode* pCol, col_id_t colId, SSchema* pSchema) {
  int8_t flags = 0;
  if (pCol->sma) {
    flags |= COL_SMA_ON;
  }
  pSchema->colId = colId;
  pSchema->type = pCol->dataType.type;
  pSchema->bytes = calcTypeBytes(pCol->dataType);
  pSchema->flags = flags;
  strcpy(pSchema->name, pCol->colName);
}

typedef struct SSampleAstInfo {
  const char* pDbName;
  const char* pTableName;
  SNodeList*  pFuncs;
  SNode*      pInterval;
  SNode*      pOffset;
  SNode*      pSliding;
  STableMeta* pRollupTableMeta;
} SSampleAstInfo;

static int32_t buildSampleAst(STranslateContext* pCxt, SSampleAstInfo* pInfo, char** pAst, int32_t* pLen) {
  SSelectStmt* pSelect = nodesMakeNode(QUERY_NODE_SELECT_STMT);
  if (NULL == pSelect) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

  SRealTableNode* pTable = nodesMakeNode(QUERY_NODE_REAL_TABLE);
  if (NULL == pTable) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pTable->table.dbName, pInfo->pDbName);
  strcpy(pTable->table.tableName, pInfo->pTableName);
  TSWAP(pTable->pMeta, pInfo->pRollupTableMeta);
  pSelect->pFromTable = (SNode*)pTable;

  TSWAP(pSelect->pProjectionList, pInfo->pFuncs);
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pSelect->pProjectionList || NULL == pFunc) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "_wstartts");
  nodesListPushFront(pSelect->pProjectionList, pFunc);
  SNode* pProject = NULL;
  FOREACH(pProject, pSelect->pProjectionList) { sprintf(((SExprNode*)pProject)->aliasName, "#%p", pProject); }

  SIntervalWindowNode* pInterval = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  if (NULL == pInterval) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSelect->pWindow = (SNode*)pInterval;
  TSWAP(pInterval->pInterval, pInfo->pInterval);
  TSWAP(pInterval->pOffset, pInfo->pOffset);
  TSWAP(pInterval->pSliding, pInfo->pSliding);
  pInterval->pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pInterval->pCol) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SColumnNode*)pInterval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(((SColumnNode*)pInterval->pCol)->colName, PK_TS_COL_INTERNAL_NAME);

  int32_t code = translateQuery(pCxt, (SNode*)pSelect);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString(pSelect, false, pAst, pLen);
  }
  nodesDestroyNode(pSelect);
  return code;
}

static void clearSampleAstInfo(SSampleAstInfo* pInfo) {
  nodesDestroyList(pInfo->pFuncs);
  nodesDestroyNode(pInfo->pInterval);
  nodesDestroyNode(pInfo->pOffset);
  nodesDestroyNode(pInfo->pSliding);
}

static SNode* makeIntervalVal(SRetention* pRetension, int8_t precision) {
  SValueNode* pVal = nodesMakeNode(QUERY_NODE_VALUE);
  if (NULL == pVal) {
    return NULL;
  }
  int64_t timeVal = convertTimeFromPrecisionToUnit(pRetension->freq, precision, pRetension->freqUnit);
  char    buf[20] = {0};
  int32_t len = snprintf(buf, sizeof(buf), "%" PRId64 "%c", timeVal, pRetension->freqUnit);
  pVal->literal = strndup(buf, len);
  if (NULL == pVal->literal) {
    nodesDestroyNode(pVal);
    return NULL;
  }
  pVal->isDuration = true;
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  pVal->node.resType.precision = precision;
  return (SNode*)pVal;
}

static SNode* createColumnFromDef(SColumnDefNode* pDef) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  strcpy(pCol->colName, pDef->colName);
  return (SNode*)pCol;
}

static SNode* createRollupFunc(SNode* pSrcFunc, SColumnDefNode* pColDef) {
  SFunctionNode* pFunc = nodesCloneNode(pSrcFunc);
  if (NULL == pFunc) {
    return NULL;
  }
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pFunc->pParameterList, createColumnFromDef(pColDef))) {
    nodesDestroyNode(pFunc);
    return NULL;
  }
  return (SNode*)pFunc;
}

static SNodeList* createRollupFuncs(SCreateTableStmt* pStmt) {
  SNodeList* pFuncs = nodesMakeList();
  if (NULL == pFuncs) {
    return NULL;
  }

  SNode* pFunc = NULL;
  FOREACH(pFunc, pStmt->pOptions->pRollupFuncs) {
    SNode* pCol = NULL;
    bool   primaryKey = true;
    FOREACH(pCol, pStmt->pCols) {
      if (primaryKey) {
        primaryKey = false;
        continue;
      }
      if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pFuncs, createRollupFunc(pFunc, (SColumnDefNode*)pCol))) {
        nodesDestroyList(pFuncs);
        return NULL;
      }
    }
  }

  return pFuncs;
}

static STableMeta* createRollupTableMeta(SCreateTableStmt* pStmt, int8_t precision) {
  int32_t     numOfField = LIST_LENGTH(pStmt->pCols) + LIST_LENGTH(pStmt->pTags);
  STableMeta* pMeta = taosMemoryCalloc(1, sizeof(STableMeta) + numOfField * sizeof(SSchema));
  if (NULL == pMeta) {
    return NULL;
  }
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->tableInfo.numOfTags = LIST_LENGTH(pStmt->pTags);
  pMeta->tableInfo.precision = precision;
  pMeta->tableInfo.numOfColumns = LIST_LENGTH(pStmt->pCols);

  int32_t index = 0;
  SNode*  pCol = NULL;
  FOREACH(pCol, pStmt->pCols) {
    toSchema((SColumnDefNode*)pCol, index + 1, pMeta->schema + index);
    ++index;
  }
  SNode* pTag = NULL;
  FOREACH(pTag, pStmt->pTags) {
    toSchema((SColumnDefNode*)pTag, index + 1, pMeta->schema + index);
    ++index;
  }

  return pMeta;
}

static int32_t buildSampleAstInfoByTable(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension,
                                         int8_t precision, SSampleAstInfo* pInfo) {
  pInfo->pDbName = pStmt->dbName;
  pInfo->pTableName = pStmt->tableName;
  pInfo->pFuncs = createRollupFuncs(pStmt);
  pInfo->pInterval = makeIntervalVal(pRetension, precision);
  pInfo->pRollupTableMeta = createRollupTableMeta(pStmt, precision);
  if (NULL == pInfo->pFuncs || NULL == pInfo->pInterval || NULL == pInfo->pRollupTableMeta) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getRollupAst(STranslateContext* pCxt, SCreateTableStmt* pStmt, SRetention* pRetension, int8_t precision,
                            char** pAst, int32_t* pLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByTable(pCxt, pStmt, pRetension, precision, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen);
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t buildRollupAst(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  SDbCfgInfo dbCfg = {0};
  int32_t    code = getDBCfg(pCxt, pStmt->dbName, &dbCfg);
  int32_t    num = taosArrayGetSize(dbCfg.pRetensions);
  if (TSDB_CODE_SUCCESS != code || num < 2) {
    taosArrayDestroy(dbCfg.pRetensions);
    return code;
  }
  for (int32_t i = 1; i < num; ++i) {
    SRetention*       pRetension = taosArrayGet(dbCfg.pRetensions, i);
    STranslateContext cxt = {0};
    initTranslateContext(pCxt->pParseCxt, &cxt);
    code = getRollupAst(&cxt, pStmt, pRetension, dbCfg.precision, 1 == i ? &pReq->pAst1 : &pReq->pAst2,
                        1 == i ? &pReq->ast1Len : &pReq->ast2Len);
    destroyTranslateContext(&cxt);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  taosArrayDestroy(dbCfg.pRetensions);
  return code;
}

static int32_t buildCreateStbReq(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;
  pReq->xFilesFactor = pStmt->pOptions->filesFactor;
  pReq->delay = pStmt->pOptions->delay;
  pReq->ttl = pStmt->pOptions->ttl;
  columnDefNodeToField(pStmt->pCols, &pReq->pColumns);
  columnDefNodeToField(pStmt->pTags, &pReq->pTags);
  pReq->numOfColumns = LIST_LENGTH(pStmt->pCols);
  pReq->numOfTags = LIST_LENGTH(pStmt->pTags);
  if ('\0' != pStmt->pOptions->comment[0]) {
    pReq->comment = strdup(pStmt->pOptions->comment);
    if (NULL == pReq->comment) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pReq->commentLen = strlen(pStmt->pOptions->comment) + 1;
  }

  SName tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), pReq->name);
  int32_t code = collectUseTable(&tableName, pCxt->pTables);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildRollupAst(pCxt, pStmt, pReq);
  }
  return code;
}

static int32_t translateCreateSuperTable(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  SMCreateStbReq createReq = {0};
  int32_t        code = checkCreateTable(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateStbReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_STB, (FSerializeFunc)tSerializeSMCreateStbReq, &createReq);
  }
  tFreeSMCreateStbReq(&createReq);
  return code;
}

static int32_t doTranslateDropSuperTable(STranslateContext* pCxt, const SName* pTableName, bool ignoreNotExists) {
  SMDropStbReq dropReq = {0};
  tNameExtractFullName(pTableName, dropReq.name);
  dropReq.igNotExists = ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_STB, (FSerializeFunc)tSerializeSMDropStbReq, &dropReq);
}

static int32_t translateDropTable(STranslateContext* pCxt, SDropTableStmt* pStmt) {
  SDropTableClause* pClause = nodesListGetNode(pStmt->pTables, 0);

  STableMeta* pTableMeta = NULL;
  SName       tableName;
  int32_t     code = getTableMetaImpl(
          pCxt, toName(pCxt->pParseCxt->acctId, pClause->dbName, pClause->tableName, &tableName), &pTableMeta);
  if ((TSDB_CODE_PAR_TABLE_NOT_EXIST == code || TSDB_CODE_VND_TB_NOT_EXIST == code) && pClause->ignoreNotExists) {
    return TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS == code) {
    if (TSDB_SUPER_TABLE == pTableMeta->tableType) {
      code = doTranslateDropSuperTable(pCxt, &tableName, pClause->ignoreNotExists);
    } else {
      // todo : drop normal table or child table
      code = TSDB_CODE_FAILED;
    }
    taosMemoryFreeClear(pTableMeta);
  }

  return code;
}

static int32_t translateDropSuperTable(STranslateContext* pCxt, SDropSuperTableStmt* pStmt) {
  SName tableName;
  return doTranslateDropSuperTable(pCxt, toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName),
                                   pStmt->ignoreNotExists);
}

static int32_t setAlterTableField(SAlterTableStmt* pStmt, SMAlterStbReq* pAlterReq) {
  if (TSDB_ALTER_TABLE_UPDATE_OPTIONS == pStmt->alterType) {
    pAlterReq->ttl = pStmt->pOptions->ttl;
    if ('\0' != pStmt->pOptions->comment[0]) {
      pAlterReq->comment = strdup(pStmt->pOptions->comment);
      if (NULL == pAlterReq->comment) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pAlterReq->commentLen = strlen(pStmt->pOptions->comment) + 1;
    }
    return TSDB_CODE_SUCCESS;
  }

  pAlterReq->pFields = taosArrayInit(2, sizeof(TAOS_FIELD));
  if (NULL == pAlterReq->pFields) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_ADD_COLUMN:
    case TSDB_ALTER_TABLE_DROP_COLUMN:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES: {
      TAOS_FIELD field = {.type = pStmt->dataType.type, .bytes = calcTypeBytes(pStmt->dataType)};
      strcpy(field.name, pStmt->colName);
      taosArrayPush(pAlterReq->pFields, &field);
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME: {
      TAOS_FIELD oldField = {0};
      strcpy(oldField.name, pStmt->colName);
      taosArrayPush(pAlterReq->pFields, &oldField);
      TAOS_FIELD newField = {0};
      strcpy(newField.name, pStmt->newColName);
      taosArrayPush(pAlterReq->pFields, &newField);
      break;
    }
    default:
      break;
  }

  pAlterReq->numOfFields = taosArrayGetSize(pAlterReq->pFields);
  return TSDB_CODE_SUCCESS;
}

static int32_t translateAlterTable(STranslateContext* pCxt, SAlterTableStmt* pStmt) {
  SMAlterStbReq alterReq = {0};
  SName         tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), alterReq.name);
  alterReq.alterType = pStmt->alterType;
  if (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == pStmt->alterType) {
    return TSDB_CODE_FAILED;
  } else {
    if (TSDB_CODE_SUCCESS != setAlterTableField(pStmt, &alterReq)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_STB, (FSerializeFunc)tSerializeSMAlterStbReq, &alterReq);
}

static int32_t translateUseDatabase(STranslateContext* pCxt, SUseDatabaseStmt* pStmt) {
  SUseDbReq usedbReq = {0};
  SName     name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameExtractFullName(&name, usedbReq.db);
  int32_t code = getDBVgVersion(pCxt, usedbReq.db, &usedbReq.vgVersion, &usedbReq.dbId, &usedbReq.numOfTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_USE_DB, (FSerializeFunc)tSerializeSUseDbReq, &usedbReq);
  }
  return code;
}

static int32_t translateCreateUser(STranslateContext* pCxt, SCreateUserStmt* pStmt) {
  SCreateUserReq createReq = {0};
  strcpy(createReq.user, pStmt->useName);
  createReq.createType = 0;
  createReq.superUser = 0;
  strcpy(createReq.pass, pStmt->password);

  return buildCmdMsg(pCxt, TDMT_MND_CREATE_USER, (FSerializeFunc)tSerializeSCreateUserReq, &createReq);
}

static int32_t translateAlterUser(STranslateContext* pCxt, SAlterUserStmt* pStmt) {
  SAlterUserReq alterReq = {0};
  strcpy(alterReq.user, pStmt->useName);
  alterReq.alterType = pStmt->alterType;
  alterReq.superUser = 0;
  strcpy(alterReq.pass, pStmt->password);
  if (NULL != pCxt->pParseCxt->db) {
    strcpy(alterReq.dbname, pCxt->pParseCxt->db);
  }

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &alterReq);
}

static int32_t translateDropUser(STranslateContext* pCxt, SDropUserStmt* pStmt) {
  SDropUserReq dropReq = {0};
  strcpy(dropReq.user, pStmt->useName);

  return buildCmdMsg(pCxt, TDMT_MND_DROP_USER, (FSerializeFunc)tSerializeSDropUserReq, &dropReq);
}

static int32_t translateCreateDnode(STranslateContext* pCxt, SCreateDnodeStmt* pStmt) {
  SCreateDnodeReq createReq = {0};
  strcpy(createReq.fqdn, pStmt->fqdn);
  createReq.port = pStmt->port;

  return buildCmdMsg(pCxt, TDMT_MND_CREATE_DNODE, (FSerializeFunc)tSerializeSCreateDnodeReq, &createReq);
}

static int32_t translateDropDnode(STranslateContext* pCxt, SDropDnodeStmt* pStmt) {
  SDropDnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;
  strcpy(dropReq.fqdn, pStmt->fqdn);
  dropReq.port = pStmt->port;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_DNODE, (FSerializeFunc)tSerializeSDropDnodeReq, &dropReq);
}

static int32_t translateAlterDnode(STranslateContext* pCxt, SAlterDnodeStmt* pStmt) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = pStmt->dnodeId;
  strcpy(cfgReq.config, pStmt->config);
  strcpy(cfgReq.value, pStmt->value);

  return buildCmdMsg(pCxt, TDMT_MND_CONFIG_DNODE, (FSerializeFunc)tSerializeSMCfgDnodeReq, &cfgReq);
}

static int32_t nodeTypeToShowType(ENodeType nt) {
  switch (nt) {
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
      return TSDB_MGMT_TABLE_CONNS;
    case QUERY_NODE_SHOW_LICENCE_STMT:
      return TSDB_MGMT_TABLE_GRANTS;
    case QUERY_NODE_SHOW_QUERIES_STMT:
      return TSDB_MGMT_TABLE_QUERIES;
    case QUERY_NODE_SHOW_VARIABLE_STMT:
      return 0;  // todo
    default:
      break;
  }
  return 0;
}

static int32_t translateShow(STranslateContext* pCxt, SShowStmt* pStmt) {
  SShowReq showReq = {.type = nodeTypeToShowType(nodeType(pStmt))};
  return buildCmdMsg(pCxt, TDMT_MND_SHOW, (FSerializeFunc)tSerializeSShowReq, &showReq);
}

static int32_t getSmaIndexDstVgId(STranslateContext* pCxt, char* pTableName, int32_t* pVgId) {
  SVgroupInfo vg = {0};
  int32_t     code = getTableHashVgroup(pCxt, pCxt->pParseCxt->db, pTableName, &vg);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgId = vg.vgId;
  }
  return code;
}

static int32_t getSmaIndexSql(STranslateContext* pCxt, char** pSql, int32_t* pLen) {
  *pSql = strdup(pCxt->pParseCxt->pSql);
  if (NULL == *pSql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pLen = pCxt->pParseCxt->sqlLen + 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t getSmaIndexExpr(STranslateContext* pCxt, SCreateIndexStmt* pStmt, char** pExpr, int32_t* pLen) {
  return nodesListToString(pStmt->pOptions->pFuncs, false, pExpr, pLen);
}

static int32_t buildSampleAstInfoByIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SSampleAstInfo* pInfo) {
  pInfo->pDbName = pCxt->pParseCxt->db;
  pInfo->pTableName = pStmt->tableName;
  pInfo->pFuncs = nodesCloneList(pStmt->pOptions->pFuncs);
  pInfo->pInterval = nodesCloneNode(pStmt->pOptions->pInterval);
  pInfo->pOffset = nodesCloneNode(pStmt->pOptions->pOffset);
  pInfo->pSliding = nodesCloneNode(pStmt->pOptions->pSliding);
  if (NULL == pInfo->pFuncs || NULL == pInfo->pInterval ||
      (NULL != pStmt->pOptions->pOffset && NULL == pInfo->pOffset) ||
      (NULL != pStmt->pOptions->pSliding && NULL == pInfo->pSliding)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getSmaIndexAst(STranslateContext* pCxt, SCreateIndexStmt* pStmt, char** pAst, int32_t* pLen) {
  SSampleAstInfo info = {0};
  int32_t        code = buildSampleAstInfoByIndex(pCxt, pStmt, &info);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildSampleAst(pCxt, &info, pAst, pLen);
  }
  clearSampleAstInfo(&info);
  return code;
}

static int32_t buildCreateSmaReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateSmaReq* pReq) {
  SName name;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->indexName, &name), pReq->name);
  strcpy(name.tname, pStmt->tableName);
  name.tname[strlen(pStmt->tableName)] = '\0';
  tNameExtractFullName(&name, pReq->stb);
  pReq->igExists = pStmt->ignoreExists;
  pReq->interval = ((SValueNode*)pStmt->pOptions->pInterval)->datum.i;
  pReq->intervalUnit = ((SValueNode*)pStmt->pOptions->pInterval)->unit;
  pReq->offset = (NULL != pStmt->pOptions->pOffset ? ((SValueNode*)pStmt->pOptions->pOffset)->datum.i : 0);
  pReq->sliding =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->datum.i : pReq->interval);
  pReq->slidingUnit =
      (NULL != pStmt->pOptions->pSliding ? ((SValueNode*)pStmt->pOptions->pSliding)->unit : pReq->intervalUnit);

  int32_t code = getSmaIndexDstVgId(pCxt, pStmt->tableName, &pReq->dstVgId);
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexSql(pCxt, &pReq->sql, &pReq->sqlLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexExpr(pCxt, pStmt, &pReq->expr, &pReq->exprLen);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = getSmaIndexAst(pCxt, pStmt, &pReq->ast, &pReq->astLen);
  }

  return code;
}

static int32_t translateCreateSmaIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pInterval) ||
      (NULL != pStmt->pOptions->pOffset &&
       DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pOffset)) ||
      (NULL != pStmt->pOptions->pSliding &&
       DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pSliding))) {
    return pCxt->errCode;
  }

  SMCreateSmaReq createSmaReq = {0};
  int32_t        code = buildCreateSmaReq(pCxt, pStmt, &createSmaReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_SMA, (FSerializeFunc)tSerializeSMCreateSmaReq, &createSmaReq);
  }
  tFreeSMCreateSmaReq(&createSmaReq);
  return code;
}

static int32_t buildCreateFullTextReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateFullTextReq* pReq) {
  // impl later
  return 0;
}

static int32_t translateCreateFullTextIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  SMCreateFullTextReq createFTReq = {0};
  int32_t             code = buildCreateFullTextReq(pCxt, pStmt, &createFTReq);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_INDEX, (FSerializeFunc)tSerializeSMCreateFullTextReq, &createFTReq);
  }
  tFreeSMCreateFullTextReq(&createFTReq);
  return code;
}

static int32_t translateCreateIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (INDEX_TYPE_SMA == pStmt->indexType) {
    return translateCreateSmaIndex(pCxt, pStmt);
  } else if (INDEX_TYPE_FULLTEXT == pStmt->indexType) {
    return translateCreateFullTextIndex(pCxt, pStmt);
  }
  return TSDB_CODE_FAILED;
}

static int32_t translateDropIndex(STranslateContext* pCxt, SDropIndexStmt* pStmt) {
  SEncoder      encoder = {0};
  int32_t       contLen = 0;
  SVDropTSmaReq dropSmaReq = {0};
  strcpy(dropSmaReq.indexName, pStmt->indexName);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t ret = 0;
  tEncodeSize(tEncodeSVDropTSmaReq, &dropSmaReq, contLen, ret);
  if (ret < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_VND_DROP_SMA;
  pCxt->pCmdMsg->msgLen = contLen;
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  void* pBuf = pCxt->pCmdMsg->pMsg;
  if (tEncodeSVDropTSmaReq(&encoder, &dropSmaReq) < 0) {
    tEncoderClear(&encoder);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tEncoderClear(&encoder);
  return TSDB_CODE_SUCCESS;
}

static int16_t getCreateComponentNodeMsgType(ENodeType type) {
  switch (type) {
    case QUERY_NODE_CREATE_QNODE_STMT:
      return TDMT_MND_CREATE_QNODE;
    case QUERY_NODE_CREATE_BNODE_STMT:
      return TDMT_MND_CREATE_BNODE;
    case QUERY_NODE_CREATE_SNODE_STMT:
      return TDMT_MND_CREATE_SNODE;
    case QUERY_NODE_CREATE_MNODE_STMT:
      return TDMT_MND_CREATE_MNODE;
    default:
      break;
  }
  return -1;
}

static int32_t translateCreateComponentNode(STranslateContext* pCxt, SCreateComponentNodeStmt* pStmt) {
  SMCreateQnodeReq createReq = {.dnodeId = pStmt->dnodeId};
  return buildCmdMsg(pCxt, getCreateComponentNodeMsgType(nodeType(pStmt)),
                     (FSerializeFunc)tSerializeSCreateDropMQSBNodeReq, &createReq);
}

static int16_t getDropComponentNodeMsgType(ENodeType type) {
  switch (type) {
    case QUERY_NODE_DROP_QNODE_STMT:
      return TDMT_MND_DROP_QNODE;
    case QUERY_NODE_DROP_BNODE_STMT:
      return TDMT_MND_DROP_BNODE;
    case QUERY_NODE_DROP_SNODE_STMT:
      return TDMT_MND_DROP_SNODE;
    case QUERY_NODE_DROP_MNODE_STMT:
      return TDMT_MND_DROP_MNODE;
    default:
      break;
  }
  return -1;
}

static int32_t translateDropComponentNode(STranslateContext* pCxt, SDropComponentNodeStmt* pStmt) {
  SDDropQnodeReq dropReq = {.dnodeId = pStmt->dnodeId};
  return buildCmdMsg(pCxt, getDropComponentNodeMsgType(nodeType(pStmt)),
                     (FSerializeFunc)tSerializeSCreateDropMQSBNodeReq, &dropReq);
}

static int32_t buildCreateTopicReq(STranslateContext* pCxt, SCreateTopicStmt* pStmt, SCMCreateTopicReq* pReq) {
  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, pReq->name);
  pReq->igExists = pStmt->ignoreExists;
  pReq->withTbName = pStmt->pOptions->withTable;
  pReq->withSchema = pStmt->pOptions->withSchema;
  pReq->withTag = pStmt->pOptions->withTag;

  pReq->sql = strdup(pCxt->pParseCxt->pSql);
  if (NULL == pReq->sql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  const char* dbName;
  if (NULL != pStmt->pQuery) {
    dbName = ((SRealTableNode*)(((SSelectStmt*)pStmt->pQuery)->pFromTable))->table.dbName;
    pCxt->pParseCxt->topicQuery = true;
    code = translateQuery(pCxt, pStmt->pQuery);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
    }
  } else {
    dbName = pStmt->subscribeDbName;
  }
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, dbName, strlen(dbName));
  tNameGetFullDbName(&name, pReq->subscribeDbName);

  return code;
}

static int32_t checkCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  if (NULL == pStmt->pQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt->pQuery)) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
    if (!pSelect->isDistinct && QUERY_NODE_REAL_TABLE == nodeType(pSelect->pFromTable) &&
        NULL == pSelect->pGroupByList && NULL == pSelect->pLimit && NULL == pSelect->pSlimit &&
        NULL == pSelect->pOrderByList && NULL == pSelect->pPartitionByList) {
      return TSDB_CODE_SUCCESS;
    }
  }

  return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TOPIC_QUERY);
}

static int32_t translateCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  SCMCreateTopicReq createReq = {0};
  int32_t           code = checkCreateTopic(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateTopicReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_TOPIC, (FSerializeFunc)tSerializeSCMCreateTopicReq, &createReq);
  }
  tFreeSCMCreateTopicReq(&createReq);
  return code;
}

static int32_t translateDropTopic(STranslateContext* pCxt, SDropTopicStmt* pStmt) {
  SMDropTopicReq dropReq = {0};

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_TOPIC, (FSerializeFunc)tSerializeSMDropTopicReq, &dropReq);
}

static int32_t translateDropCGroup(STranslateContext* pCxt, SDropCGroupStmt* pStmt) {
  SMDropCgroupReq dropReq = {0};

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->topicName, strlen(pStmt->topicName));
  tNameGetFullDbName(&name, dropReq.topic);
  dropReq.igNotExists = pStmt->ignoreNotExists;
  strcpy(dropReq.cgroup, pStmt->cgroup);

  return buildCmdMsg(pCxt, TDMT_MND_DROP_CGROUP, (FSerializeFunc)tSerializeSMDropCgroupReq, &dropReq);
}

static int32_t translateAlterLocal(STranslateContext* pCxt, SAlterLocalStmt* pStmt) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t translateExplain(STranslateContext* pCxt, SExplainStmt* pStmt) {
  if (pStmt->analyze) {
    pCxt->pExplainOpt = pStmt->pOptions;
  }
  return translateQuery(pCxt, pStmt->pQuery);
}

static int32_t translateDescribe(STranslateContext* pCxt, SDescribeStmt* pStmt) {
  return refreshGetTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pStmt->pMeta);
}

static int32_t translateKillConnection(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillConnReq killReq = {0};
  killReq.connId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_CONN, (FSerializeFunc)tSerializeSKillQueryReq, &killReq);
}

static int32_t translateKillQuery(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillQueryReq killReq = {0};
  killReq.queryId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_QUERY, (FSerializeFunc)tSerializeSKillQueryReq, &killReq);
}

static int32_t translateKillTransaction(STranslateContext* pCxt, SKillStmt* pStmt) {
  SKillTransReq killReq = {0};
  killReq.transId = pStmt->targetId;
  return buildCmdMsg(pCxt, TDMT_MND_KILL_TRANS, (FSerializeFunc)tSerializeSKillTransReq, &killReq);
}

static int32_t checkCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  if (NULL == pStmt->pQuery) {
    return TSDB_CODE_SUCCESS;
  }

  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt->pQuery)) {
    SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
    if (QUERY_NODE_REAL_TABLE == nodeType(pSelect->pFromTable)) {
      return TSDB_CODE_SUCCESS;
    }
  }

  return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY);
}

static void getSourceDatabase(SNode* pStmt, int32_t acctId, char* pDbFName) {
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, ((SRealTableNode*)(((SSelectStmt*)pStmt)->pFromTable))->table.dbName);
  tNameGetFullDbName(&name, pDbFName);
}

static int32_t buildCreateStreamReq(STranslateContext* pCxt, SCreateStreamStmt* pStmt, SCMCreateStreamReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;

  SName name;
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->streamName, strlen(pStmt->streamName));
  tNameGetFullDbName(&name, pReq->name);
  // tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->streamName, &name), pReq->name);

  if ('\0' != pStmt->targetTabName[0]) {
    strcpy(name.dbname, pStmt->targetDbName);
    strcpy(name.tname, pStmt->targetTabName);
    tNameExtractFullName(&name, pReq->targetStbFullName);
  }

  int32_t code = translateQuery(pCxt, pStmt->pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    getSourceDatabase(pStmt->pQuery, pCxt->pParseCxt->acctId, pReq->sourceDB);
    code = nodesNodeToString(pStmt->pQuery, false, &pReq->ast, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pReq->sql = strdup(pCxt->pParseCxt->pSql);
    if (NULL == pReq->sql) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pWatermark) {
    code = (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pWatermark)) ? pCxt->errCode
                                                                                              : TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS == code) {
    pReq->triggerType = pStmt->pOptions->triggerType;
    pReq->watermark = (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
  }

  return code;
}

static int32_t translateCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  SCMCreateStreamReq createReq = {0};

  int32_t code = checkCreateStream(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateStreamReq(pCxt, pStmt, &createReq);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_STREAM, (FSerializeFunc)tSerializeSCMCreateStreamReq, &createReq);
  }

  tFreeSCMCreateStreamReq(&createReq);
  return code;
}

static int32_t translateDropStream(STranslateContext* pCxt, SDropStreamStmt* pStmt) {
  // todo
  return TSDB_CODE_SUCCESS;
}

static int32_t readFromFile(char* pName, int32_t* len, char** buf) {
  int64_t filesize = 0;
  if (taosStatFile(pName, &filesize, NULL) < 0) {
    return TAOS_SYSTEM_ERROR(errno);
  }

  *len = filesize;

  if (*len <= 0) {
    return TSDB_CODE_TSC_FILE_EMPTY;
  }

  *buf = taosMemoryCalloc(1, *len);
  if (*buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TdFilePtr tfile = taosOpenFile(pName, O_RDONLY | O_BINARY);
  if (NULL == tfile) {
    taosMemoryFreeClear(*buf);
    return TAOS_SYSTEM_ERROR(errno);
  }

  int64_t s = taosReadFile(tfile, *buf, *len);
  if (s != *len) {
    taosCloseFile(&tfile);
    taosMemoryFreeClear(*buf);
    return TSDB_CODE_TSC_APP_ERROR;
  }
  taosCloseFile(&tfile);
  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateFunction(STranslateContext* pCxt, SCreateFunctionStmt* pStmt) {
  if (fmIsBuiltinFunc(pStmt->funcName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_FUNCTION_NAME);
  }
  SCreateFuncReq req = {0};
  strcpy(req.name, pStmt->funcName);
  req.igExists = pStmt->ignoreExists;
  req.funcType = pStmt->isAgg ? TSDB_FUNC_TYPE_AGGREGATE : TSDB_FUNC_TYPE_SCALAR;
  req.scriptType = TSDB_FUNC_SCRIPT_BIN_LIB;
  req.outputType = pStmt->outputDt.type;
  req.outputLen = pStmt->outputDt.bytes;
  req.bufSize = pStmt->bufSize;
  int32_t code = readFromFile(pStmt->libraryPath, &req.codeLen, &req.pCode);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCmdMsg(pCxt, TDMT_MND_CREATE_FUNC, (FSerializeFunc)tSerializeSCreateFuncReq, &req);
  }
  return code;
}

static int32_t translateDropFunction(STranslateContext* pCxt, SDropFunctionStmt* pStmt) {
  SDropFuncReq req = {0};
  strcpy(req.name, pStmt->funcName);
  req.igNotExists = pStmt->ignoreNotExists;
  return buildCmdMsg(pCxt, TDMT_MND_DROP_FUNC, (FSerializeFunc)tSerializeSDropFuncReq, &req);
}

static int32_t translateGrant(STranslateContext* pCxt, SGrantStmt* pStmt) {
  SAlterUserReq req = {0};
  if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_ALL) ||
      (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ) &&
       PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE))) {
    req.alterType = TSDB_ALTER_USER_ADD_ALL_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ)) {
    req.alterType = TSDB_ALTER_USER_ADD_READ_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE)) {
    req.alterType = TSDB_ALTER_USER_ADD_WRITE_DB;
  }
  strcpy(req.user, pStmt->userName);
  sprintf(req.dbname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->dbName);
  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
}

static int32_t translateRevoke(STranslateContext* pCxt, SRevokeStmt* pStmt) {
  SAlterUserReq req = {0};
  if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_ALL) ||
      (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ) &&
       PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE))) {
    req.alterType = TSDB_ALTER_USER_REMOVE_ALL_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_READ)) {
    req.alterType = TSDB_ALTER_USER_REMOVE_READ_DB;
  } else if (PRIVILEGE_TYPE_TEST_MASK(pStmt->privileges, PRIVILEGE_TYPE_WRITE)) {
    req.alterType = TSDB_ALTER_USER_REMOVE_WRITE_DB;
  }
  strcpy(req.user, pStmt->userName);
  sprintf(req.dbname, "%d.%s", pCxt->pParseCxt->acctId, pStmt->dbName);
  return buildCmdMsg(pCxt, TDMT_MND_ALTER_USER, (FSerializeFunc)tSerializeSAlterUserReq, &req);
}

static int32_t translateQuery(STranslateContext* pCxt, SNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT:
      code = translateSelect(pCxt, (SSelectStmt*)pNode);
      break;
    case QUERY_NODE_SET_OPERATOR:
      code = translateSetOperator(pCxt, (SSetOperator*)pNode);
      break;
    case QUERY_NODE_CREATE_DATABASE_STMT:
      code = translateCreateDatabase(pCxt, (SCreateDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_DROP_DATABASE_STMT:
      code = translateDropDatabase(pCxt, (SDropDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_DATABASE_STMT:
      code = translateAlterDatabase(pCxt, (SAlterDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      code = translateCreateSuperTable(pCxt, (SCreateTableStmt*)pNode);
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = translateDropTable(pCxt, (SDropTableStmt*)pNode);
      break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      code = translateDropSuperTable(pCxt, (SDropSuperTableStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
      code = translateAlterTable(pCxt, (SAlterTableStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_USER_STMT:
      code = translateCreateUser(pCxt, (SCreateUserStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_USER_STMT:
      code = translateAlterUser(pCxt, (SAlterUserStmt*)pNode);
      break;
    case QUERY_NODE_DROP_USER_STMT:
      code = translateDropUser(pCxt, (SDropUserStmt*)pNode);
      break;
    case QUERY_NODE_USE_DATABASE_STMT:
      code = translateUseDatabase(pCxt, (SUseDatabaseStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_DNODE_STMT:
      code = translateCreateDnode(pCxt, (SCreateDnodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_DNODE_STMT:
      code = translateDropDnode(pCxt, (SDropDnodeStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_DNODE_STMT:
      code = translateAlterDnode(pCxt, (SAlterDnodeStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
      code = translateShow(pCxt, (SShowStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_INDEX_STMT:
      code = translateCreateIndex(pCxt, (SCreateIndexStmt*)pNode);
      break;
    case QUERY_NODE_DROP_INDEX_STMT:
      code = translateDropIndex(pCxt, (SDropIndexStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
      code = translateCreateComponentNode(pCxt, (SCreateComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
      code = translateDropComponentNode(pCxt, (SDropComponentNodeStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      code = translateCreateTopic(pCxt, (SCreateTopicStmt*)pNode);
      break;
    case QUERY_NODE_DROP_TOPIC_STMT:
      code = translateDropTopic(pCxt, (SDropTopicStmt*)pNode);
      break;
    case QUERY_NODE_DROP_CGROUP_STMT:
      code = translateDropCGroup(pCxt, (SDropCGroupStmt*)pNode);
      break;
    case QUERY_NODE_ALTER_LOCAL_STMT:
      code = translateAlterLocal(pCxt, (SAlterLocalStmt*)pNode);
      break;
    case QUERY_NODE_EXPLAIN_STMT:
      code = translateExplain(pCxt, (SExplainStmt*)pNode);
      break;
    case QUERY_NODE_DESCRIBE_STMT:
      code = translateDescribe(pCxt, (SDescribeStmt*)pNode);
      break;
    case QUERY_NODE_KILL_CONNECTION_STMT:
      code = translateKillConnection(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_QUERY_STMT:
      code = translateKillQuery(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_KILL_TRANSACTION_STMT:
      code = translateKillTransaction(pCxt, (SKillStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_STREAM_STMT:
      code = translateCreateStream(pCxt, (SCreateStreamStmt*)pNode);
      break;
    case QUERY_NODE_DROP_STREAM_STMT:
      code = translateDropStream(pCxt, (SDropStreamStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      code = translateCreateFunction(pCxt, (SCreateFunctionStmt*)pNode);
      break;
    case QUERY_NODE_DROP_FUNCTION_STMT:
      code = translateDropFunction(pCxt, (SDropFunctionStmt*)pNode);
      break;
    case QUERY_NODE_GRANT_STMT:
      code = translateGrant(pCxt, (SGrantStmt*)pNode);
      break;
    case QUERY_NODE_REVOKE_STMT:
      code = translateRevoke(pCxt, (SRevokeStmt*)pNode);
      break;
    default:
      break;
  }
  return code;
}

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode) {
  ++(pCxt->currLevel);
  ESqlClause   currClause = pCxt->currClause;
  SSelectStmt* pCurrStmt = pCxt->pCurrStmt;
  int32_t      code = translateQuery(pCxt, pNode);
  --(pCxt->currLevel);
  pCxt->currClause = currClause;
  pCxt->pCurrStmt = pCurrStmt;
  return code;
}

static int32_t extractQueryResultSchema(const SNodeList* pProjections, int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = LIST_LENGTH(pProjections);
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode*  pNode;
  int32_t index = 0;
  FOREACH(pNode, pProjections) {
    SExprNode* pExpr = (SExprNode*)pNode;
    (*pSchema)[index].type = pExpr->resType.type;
    (*pSchema)[index].bytes = pExpr->resType.bytes;
    (*pSchema)[index].colId = index + 1;
    if ('\0' != pExpr->userAlias[0]) {
      strcpy((*pSchema)[index].name, pExpr->userAlias);
    } else {
      strcpy((*pSchema)[index].name, pExpr->aliasName);
    }
    index += 1;
  }

  return TSDB_CODE_SUCCESS;
}

static int8_t extractResultTsPrecision(const SSelectStmt* pSelect) { return pSelect->precision; }

static int32_t extractExplainResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = 1;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = TSDB_EXPLAIN_RESULT_ROW_SIZE;
  strcpy((*pSchema)[0].name, TSDB_EXPLAIN_RESULT_COLUMN_NAME);
  return TSDB_CODE_SUCCESS;
}

static int32_t extractDescribeResultSchema(int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = DESCRIBE_RESULT_COLS;
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pSchema)[0].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[0].bytes = DESCRIBE_RESULT_FIELD_LEN;
  strcpy((*pSchema)[0].name, "field");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = DESCRIBE_RESULT_TYPE_LEN;
  strcpy((*pSchema)[1].name, "type");

  (*pSchema)[2].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[2].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  strcpy((*pSchema)[2].name, "length");

  (*pSchema)[3].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[3].bytes = DESCRIBE_RESULT_NOTE_LEN;
  strcpy((*pSchema)[3].name, "note");

  return TSDB_CODE_SUCCESS;
}

int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  if (NULL == pRoot) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SET_OPERATOR:
      return extractQueryResultSchema(getProjectList(pRoot), numOfCols, pSchema);
    case QUERY_NODE_EXPLAIN_STMT:
      return extractExplainResultSchema(numOfCols, pSchema);
    case QUERY_NODE_DESCRIBE_STMT:
      return extractDescribeResultSchema(numOfCols, pSchema);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static const char* getSysDbName(ENodeType type) {
  switch (type) {
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
      return TSDB_INFORMATION_SCHEMA_DB;
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      return TSDB_PERFORMANCE_SCHEMA_DB;
    default:
      break;
  }
  return NULL;
}

static const char* getSysTableName(ENodeType type) {
  switch (type) {
    case QUERY_NODE_SHOW_DATABASES_STMT:
      return TSDB_INS_TABLE_USER_DATABASES;
    case QUERY_NODE_SHOW_TABLES_STMT:
      return TSDB_INS_TABLE_USER_TABLES;
    case QUERY_NODE_SHOW_STABLES_STMT:
      return TSDB_INS_TABLE_USER_STABLES;
    case QUERY_NODE_SHOW_USERS_STMT:
      return TSDB_INS_TABLE_USER_USERS;
    case QUERY_NODE_SHOW_DNODES_STMT:
      return TSDB_INS_TABLE_DNODES;
    case QUERY_NODE_SHOW_VGROUPS_STMT:
      return TSDB_INS_TABLE_VGROUPS;
    case QUERY_NODE_SHOW_MNODES_STMT:
      return TSDB_INS_TABLE_MNODES;
    case QUERY_NODE_SHOW_MODULES_STMT:
      return TSDB_INS_TABLE_MODULES;
    case QUERY_NODE_SHOW_QNODES_STMT:
      return TSDB_INS_TABLE_QNODES;
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
      return TSDB_INS_TABLE_USER_FUNCTIONS;
    case QUERY_NODE_SHOW_INDEXES_STMT:
      return TSDB_INS_TABLE_USER_INDEXES;
    case QUERY_NODE_SHOW_STREAMS_STMT:
      return TSDB_INS_TABLE_USER_STREAMS;
    case QUERY_NODE_SHOW_BNODES_STMT:
      return TSDB_INS_TABLE_BNODES;
    case QUERY_NODE_SHOW_SNODES_STMT:
      return TSDB_INS_TABLE_SNODES;
    case QUERY_NODE_SHOW_LICENCE_STMT:
      return TSDB_INS_TABLE_LICENCES;
    case QUERY_NODE_SHOW_CLUSTER_STMT:
      return TSDB_INS_TABLE_CLUSTER;
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
      return TSDB_PERFS_TABLE_CONNECTIONS;
    case QUERY_NODE_SHOW_QUERIES_STMT:
      return TSDB_PERFS_TABLE_QUERIES;
    case QUERY_NODE_SHOW_TOPICS_STMT:
      return TSDB_PERFS_TABLE_TOPICS;
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      return TSDB_PERFS_TABLE_TRANS;
    default:
      break;
  }
  return NULL;
}

static int32_t createSelectStmtForShow(ENodeType showType, SSelectStmt** pStmt) {
  SSelectStmt* pSelect = nodesMakeNode(QUERY_NODE_SELECT_STMT);
  if (NULL == pSelect) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  sprintf(pSelect->stmtName, "%p", pSelect);

  SRealTableNode* pTable = nodesMakeNode(QUERY_NODE_REAL_TABLE);
  if (NULL == pTable) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pTable->table.dbName, getSysDbName(showType));
  strcpy(pTable->table.tableName, getSysTableName(showType));
  strcpy(pTable->table.tableAlias, pTable->table.tableName);
  pSelect->pFromTable = (SNode*)pTable;

  *pStmt = pSelect;

  return TSDB_CODE_SUCCESS;
}

static int32_t createOperatorNode(EOperatorType opType, const char* pColName, SNode* pRight, SNode** pOp) {
  if (NULL == pRight) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode* pOper = nodesMakeNode(QUERY_NODE_OPERATOR);
  if (NULL == pOper) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pOper->opType = opType;
  pOper->pLeft = nodesMakeNode(QUERY_NODE_COLUMN);
  pOper->pRight = nodesCloneNode(pRight);
  if (NULL == pOper->pLeft || NULL == pOper->pRight) {
    nodesDestroyNode(pOper);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(((SColumnNode*)pOper->pLeft)->colName, pColName);

  *pOp = (SNode*)pOper;
  return TSDB_CODE_SUCCESS;
}

static const char* getTbNameColName(ENodeType type) {
  return (QUERY_NODE_SHOW_STABLES_STMT == type ? "stable_name" : "table_name");
}

static int32_t createLogicCondNode(SNode* pCond1, SNode* pCond2, SNode** pCond) {
  SLogicConditionNode* pCondition = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pCondition) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCondition->condType = LOGIC_COND_TYPE_AND;
  pCondition->pParameterList = nodesMakeList();
  if (NULL == pCondition->pParameterList) {
    nodesDestroyNode(pCondition);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS != nodesListAppend(pCondition->pParameterList, pCond1) ||
      TSDB_CODE_SUCCESS != nodesListAppend(pCondition->pParameterList, pCond2)) {
    nodesDestroyNode(pCondition);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pCond = (SNode*)pCondition;
  return TSDB_CODE_SUCCESS;
}

static int32_t createShowCondition(const SShowStmt* pShow, SSelectStmt* pSelect) {
  SNode* pDbCond = NULL;
  SNode* pTbCond = NULL;
  if (TSDB_CODE_SUCCESS != createOperatorNode(OP_TYPE_EQUAL, "db_name", pShow->pDbName, &pDbCond) ||
      TSDB_CODE_SUCCESS !=
          createOperatorNode(OP_TYPE_LIKE, getTbNameColName(nodeType(pShow)), pShow->pTbNamePattern, &pTbCond)) {
    nodesDestroyNode(pDbCond);
    nodesDestroyNode(pTbCond);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (NULL != pDbCond && NULL != pTbCond) {
    if (TSDB_CODE_SUCCESS != createLogicCondNode(pDbCond, pTbCond, &pSelect->pWhere)) {
      nodesDestroyNode(pDbCond);
      nodesDestroyNode(pTbCond);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pSelect->pWhere = (NULL == pDbCond ? pTbCond : pDbCond);
  }

  if (NULL != pShow->pDbName) {
    strcpy(((SRealTableNode*)pSelect->pFromTable)->qualDbName, ((SValueNode*)pShow->pDbName)->literal);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t rewriteShow(STranslateContext* pCxt, SQuery* pQuery) {
  SSelectStmt* pStmt = NULL;
  int32_t      code = createSelectStmtForShow(nodeType(pQuery->pRoot), &pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = createShowCondition((SShowStmt*)pQuery->pRoot, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pQuery->showRewrite = true;
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = (SNode*)pStmt;
  }
  return code;
}

typedef struct SVgroupCreateTableBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
  char               dbName[TSDB_DB_NAME_LEN];
} SVgroupCreateTableBatch;

static void destroyCreateTbReq(SVCreateTbReq* pReq) {
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->ntb.schemaRow.pSchema);
}

static int32_t buildNormalTableBatchReq(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                        SVgroupCreateTableBatch* pBatch) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pStmt->dbName);
  tNameGetFullDbName(&name, dbFName);

  SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = strdup(pStmt->tableName);
  req.ntb.schemaRow.nCols = LIST_LENGTH(pStmt->pCols);
  req.ntb.schemaRow.version = 1;
  req.ntb.schemaRow.pSchema = taosMemoryCalloc(req.ntb.schemaRow.nCols, sizeof(SSchema));
  if (NULL == req.name || NULL == req.ntb.schemaRow.pSchema) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }
  SNode*   pCol;
  col_id_t index = 0;
  FOREACH(pCol, pStmt->pCols) {
    toSchema((SColumnDefNode*)pCol, index + 1, req.ntb.schemaRow.pSchema + index);
    ++index;
  }
  pBatch->info = *pVgroupInfo;
  strcpy(pBatch->dbName, pStmt->dbName);
  pBatch->req.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  if (NULL == pBatch->req.pArray) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosArrayPush(pBatch->req.pArray, &req);

  return TSDB_CODE_SUCCESS;
}

static int32_t serializeVgroupCreateTableBatch(SVgroupCreateTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder coder = {0};

  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchReq, &pTbBatch->req, tlen, ret);
  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  tEncodeSVCreateTbBatchReq(&coder, &pTbBatch->req);
  tEncoderClear(&coder);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  taosArrayPush(pBufArray, &pVgData);

  return TSDB_CODE_SUCCESS;
}

static void destroyCreateTbReqBatch(SVgroupCreateTableBatch* pTbBatch) {
  size_t size = taosArrayGetSize(pTbBatch->req.pArray);
  for (int32_t i = 0; i < size; ++i) {
    SVCreateTbReq* pTableReq = taosArrayGet(pTbBatch->req.pArray, i);
    taosMemoryFreeClear(pTableReq->name);

    if (pTableReq->type == TSDB_NORMAL_TABLE) {
      taosMemoryFreeClear(pTableReq->ntb.schemaRow.pSchema);
    } else if (pTableReq->type == TSDB_CHILD_TABLE) {
      taosMemoryFreeClear(pTableReq->ctb.pTag);
    }
  }

  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t rewriteToVnodeModifyOpStmt(SQuery* pQuery, SArray* pBufArray) {
  SVnodeModifOpStmt* pNewStmt = nodesMakeNode(QUERY_NODE_VNODE_MODIF_STMT);
  if (pNewStmt == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pNewStmt->sqlNodeType = nodeType(pQuery->pRoot);
  pNewStmt->pDataBlocks = pBufArray;
  nodesDestroyNode(pQuery->pRoot);
  pQuery->pRoot = (SNode*)pNewStmt;
  return TSDB_CODE_SUCCESS;
}

static void destroyCreateTbReqArray(SArray* pArray) {
  size_t size = taosArrayGetSize(pArray);
  for (size_t i = 0; i < size; ++i) {
    SVgDataBlocks* pVg = taosArrayGetP(pArray, i);
    taosMemoryFreeClear(pVg->pData);
    taosMemoryFreeClear(pVg);
  }
  taosArrayDestroy(pArray);
}

static int32_t buildCreateTableDataBlock(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pInfo,
                                         SArray** pBufArray) {
  *pBufArray = taosArrayInit(1, POINTER_BYTES);
  if (NULL == *pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SVgroupCreateTableBatch tbatch = {0};
  int32_t                 code = buildNormalTableBatchReq(acctId, pStmt, pInfo, &tbatch);
  if (TSDB_CODE_SUCCESS == code) {
    code = serializeVgroupCreateTableBatch(&tbatch, *pBufArray);
  }

  destroyCreateTbReqBatch(&tbatch);
  if (TSDB_CODE_SUCCESS != code) {
    destroyCreateTbReqArray(*pBufArray);
  }
  return code;
}

static int32_t rewriteCreateTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateTableStmt* pStmt = (SCreateTableStmt*)pQuery->pRoot;

  int32_t     code = checkCreateTable(pCxt, pStmt);
  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &info);
  }
  SArray* pBufArray = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateTableDataBlock(pCxt->pParseCxt->acctId, pStmt, &info, &pBufArray);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      destroyCreateTbReqArray(pBufArray);
    }
  }

  return code;
}

static void addCreateTbReqIntoVgroup(int32_t acctId, SHashObj* pVgroupHashmap, SCreateSubTableClause* pStmt, SKVRow row,
                                     uint64_t suid, SVgroupInfo* pVgInfo) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pStmt->dbName);
  tNameGetFullDbName(&name, dbFName);

  struct SVCreateTbReq req = {0};
  req.type = TD_CHILD_TABLE;
  req.name = strdup(pStmt->tableName);
  req.ctb.suid = suid;
  req.ctb.pTag = row;
  if (pStmt->ignoreExists) {
    req.flags |= TD_CREATE_IF_NOT_EXISTS;
  }

  SVgroupCreateTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (pTableBatch == NULL) {
    SVgroupCreateTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    strcpy(tBatch.dbName, pStmt->dbName);

    tBatch.req.pArray = taosArrayInit(4, sizeof(struct SVCreateTbReq));
    taosArrayPush(tBatch.req.pArray, &req);

    taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
  } else {  // add to the correct vgroup
    taosArrayPush(pTableBatch->req.pArray, &req);
  }
}

static int32_t addValToKVRow(STranslateContext* pCxt, SValueNode* pVal, const SSchema* pSchema,
                             SKVRowBuilder* pBuilder) {
  if (pSchema->type == TSDB_DATA_TYPE_JSON) {
    if (pVal->literal && strlen(pVal->literal) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
      return buildSyntaxErrMsg(&pCxt->msgBuf, "json string too long than 4095", pVal->literal);
    }

    return parseJsontoTagData(pVal->literal, pBuilder, &pCxt->msgBuf, pSchema->colId);
  }

  if (pVal->node.resType.type != TSDB_DATA_TYPE_NULL) {
    tdAddColToKVRow(pBuilder, pSchema->colId, nodesGetValueFromNode(pVal),
                    IS_VAR_DATA_TYPE(pSchema->type) ? varDataTLen(pVal->datum.p) : TYPE_BYTES[pSchema->type]);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createValueFromFunction(STranslateContext* pCxt, SFunctionNode* pFunc, SValueNode** pVal) {
  int32_t code = getFuncInfo(pCxt, pFunc);
  if (TSDB_CODE_SUCCESS == code) {
    code = scalarCalculateConstants((SNode*)pFunc, (SNode**)pVal);
  }
  return code;
}

static SDataType schemaToDataType(uint8_t precision, SSchema* pSchema) {
  SDataType dt = {.type = pSchema->type, .bytes = pSchema->bytes, .precision = precision, .scale = 0};
  return dt;
}

static int32_t translateTagVal(STranslateContext* pCxt, uint8_t precision, SSchema* pSchema, SNode* pNode,
                               SValueNode** pVal) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    return createValueFromFunction(pCxt, (SFunctionNode*)pNode, pVal);
  } else if (QUERY_NODE_VALUE == nodeType(pNode)) {
    return (DEAL_RES_ERROR == translateValueImpl(pCxt, (SValueNode*)pNode, schemaToDataType(precision, pSchema))
                ? pCxt->errCode
                : TSDB_CODE_SUCCESS);
  } else {
    return TSDB_CODE_FAILED;
  }
}

static int32_t buildKVRowForBindTags(STranslateContext* pCxt, SCreateSubTableClause* pStmt, STableMeta* pSuperTableMeta,
                                     SKVRowBuilder* pBuilder) {
  int32_t numOfTags = getNumOfTags(pSuperTableMeta);
  if (LIST_LENGTH(pStmt->pValsOfTags) != LIST_LENGTH(pStmt->pSpecificTags) ||
      numOfTags < LIST_LENGTH(pStmt->pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SSchema* pTagSchema = getTableTagSchema(pSuperTableMeta);
  SNode *  pTag, *pNode;
  FORBOTH(pTag, pStmt->pSpecificTags, pNode, pStmt->pValsOfTags) {
    SColumnNode* pCol = (SColumnNode*)pTag;
    SSchema*     pSchema = NULL;
    for (int32_t i = 0; i < numOfTags; ++i) {
      if (0 == strcmp(pCol->colName, pTagSchema[i].name)) {
        pSchema = pTagSchema + i;
        break;
      }
    }
    if (NULL == pSchema) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TAG_NAME, pCol->colName);
    }
    SValueNode* pVal = NULL;
    int32_t     code = translateTagVal(pCxt, pSuperTableMeta->tableInfo.precision, pSchema, pNode, &pVal);
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL == pVal) {
        pVal = (SValueNode*)pNode;
      } else {
        REPLACE_LIST2_NODE(pVal);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = addValToKVRow(pCxt, pVal, pSchema, pBuilder);
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildKVRowForAllTags(STranslateContext* pCxt, SCreateSubTableClause* pStmt, STableMeta* pSuperTableMeta,
                                    SKVRowBuilder* pBuilder) {
  if (getNumOfTags(pSuperTableMeta) != LIST_LENGTH(pStmt->pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SSchema* pTagSchema = getTableTagSchema(pSuperTableMeta);
  SNode*   pNode;
  int32_t  index = 0;
  FOREACH(pNode, pStmt->pValsOfTags) {
    SValueNode* pVal = NULL;
    int32_t     code = translateTagVal(pCxt, pSuperTableMeta->tableInfo.precision, pTagSchema + index, pNode, &pVal);
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL == pVal) {
        pVal = (SValueNode*)pNode;
      } else {
        REPLACE_NODE(pVal);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = addValToKVRow(pCxt, pVal, pTagSchema + index++, pBuilder);
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkCreateSubTable(STranslateContext* pCxt, SCreateSubTableClause* pStmt) {
  if (0 != strcmp(pStmt->dbName, pStmt->useDbName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR);
  }
  return TSDB_CODE_SUCCESS;
}
static int32_t rewriteCreateSubTable(STranslateContext* pCxt, SCreateSubTableClause* pStmt, SHashObj* pVgroupHashmap) {
  int32_t code = checkCreateSubTable(pCxt, pStmt);

  STableMeta* pSuperTableMeta = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableMeta(pCxt, pStmt->useDbName, pStmt->useTableName, &pSuperTableMeta);
  }

  SKVRowBuilder kvRowBuilder = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = tdInitKVRowBuilder(&kvRowBuilder);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pStmt->pSpecificTags) {
      code = buildKVRowForBindTags(pCxt, pStmt, pSuperTableMeta, &kvRowBuilder);
    } else {
      code = buildKVRowForAllTags(pCxt, pStmt, pSuperTableMeta, &kvRowBuilder);
    }
  }

  SKVRow row = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    row = tdGetKVRowFromBuilder(&kvRowBuilder);
    if (NULL == row) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      tdSortKVRowByColIdx(row);
    }
  }

  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    addCreateTbReqIntoVgroup(pCxt->pParseCxt->acctId, pVgroupHashmap, pStmt, row, pSuperTableMeta->uid, &info);
  }

  taosMemoryFreeClear(pSuperTableMeta);
  tdDestroyKVRowBuilder(&kvRowBuilder);
  return code;
}

static SArray* serializeVgroupsCreateTableBatch(int32_t acctId, SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return NULL;
  }

  int32_t                  code = TSDB_CODE_SUCCESS;
  SVgroupCreateTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    serializeVgroupCreateTableBatch(pTbBatch, pBufArray);
    destroyCreateTbReqBatch(pTbBatch);
  } while (true);

  return pBufArray;
}

static int32_t rewriteCreateMultiTable(STranslateContext* pCxt, SQuery* pQuery) {
  SCreateMultiTableStmt* pStmt = (SCreateMultiTableStmt*)pQuery->pRoot;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode;
  FOREACH(pNode, pStmt->pSubTables) {
    code = rewriteCreateSubTable(pCxt, (SCreateSubTableClause*)pNode, pVgroupHashmap);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
  }

  SArray* pBufArray = serializeVgroupsCreateTableBatch(pCxt->pParseCxt->acctId, pVgroupHashmap);
  taosHashCleanup(pVgroupHashmap);
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

typedef struct SVgroupDropTableBatch {
  SVDropTbBatchReq req;
  SVgroupInfo      info;
  char             dbName[TSDB_DB_NAME_LEN];
} SVgroupDropTableBatch;

static void addDropTbReqIntoVgroup(SHashObj* pVgroupHashmap, SDropTableClause* pClause, SVgroupInfo* pVgInfo) {
  SVDropTbReq            req = {.name = pClause->tableName, .igNotExists = pClause->ignoreNotExists};
  SVgroupDropTableBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (NULL == pTableBatch) {
    SVgroupDropTableBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    tBatch.req.pArray = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SVDropTbReq));
    taosArrayPush(tBatch.req.pArray, &req);

    taosHashPut(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId), &tBatch, sizeof(tBatch));
  } else {  // add to the correct vgroup
    taosArrayPush(pTableBatch->req.pArray, &req);
  }
}

static int32_t buildDropTableVgroupHashmap(STranslateContext* pCxt, SDropTableClause* pClause, bool* pIsSuperTable,
                                           SHashObj* pVgroupHashmap) {
  STableMeta* pTableMeta = NULL;
  int32_t     code = getTableMeta(pCxt, pClause->dbName, pClause->tableName, &pTableMeta);

  if (TSDB_CODE_SUCCESS == code && TSDB_SUPER_TABLE == pTableMeta->tableType) {
    *pIsSuperTable = true;
    goto over;
  }

  if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code && pClause->ignoreNotExists) {
    code = TSDB_CODE_SUCCESS;
  }

  *pIsSuperTable = false;

  SVgroupInfo info = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = getTableHashVgroup(pCxt, pClause->dbName, pClause->tableName, &info);
  }
  if (TSDB_CODE_SUCCESS == code) {
    addDropTbReqIntoVgroup(pVgroupHashmap, pClause, &info);
  }

over:
  taosMemoryFreeClear(pTableMeta);
  return code;
}

static void destroyDropTbReqBatch(SVgroupDropTableBatch* pTbBatch) { taosArrayDestroy(pTbBatch->req.pArray); }

static int32_t serializeVgroupDropTableBatch(SVgroupDropTableBatch* pTbBatch, SArray* pBufArray) {
  int      tlen;
  SEncoder coder = {0};

  int32_t ret = 0;
  tEncodeSize(tEncodeSVDropTbBatchReq, &pTbBatch->req, tlen, ret);
  tlen += sizeof(SMsgHead);
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
  tEncodeSVDropTbBatchReq(&coder, &pTbBatch->req);
  tEncoderClear(&coder);

  SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
  if (NULL == pVgData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgData->vg = pTbBatch->info;
  pVgData->pData = buf;
  pVgData->size = tlen;
  pVgData->numOfTables = (int32_t)taosArrayGetSize(pTbBatch->req.pArray);
  taosArrayPush(pBufArray, &pVgData);

  return TSDB_CODE_SUCCESS;
}

static SArray* serializeVgroupsDropTableBatch(int32_t acctId, SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return NULL;
  }

  int32_t                code = TSDB_CODE_SUCCESS;
  SVgroupDropTableBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    serializeVgroupDropTableBatch(pTbBatch, pBufArray);
    destroyDropTbReqBatch(pTbBatch);
  } while (true);

  return pBufArray;
}

static int32_t rewriteDropTable(STranslateContext* pCxt, SQuery* pQuery) {
  SDropTableStmt* pStmt = (SDropTableStmt*)pQuery->pRoot;

  SHashObj* pVgroupHashmap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pVgroupHashmap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  bool   isSuperTable = false;
  SNode* pNode;
  FOREACH(pNode, pStmt->pTables) {
    int32_t code = buildDropTableVgroupHashmap(pCxt, (SDropTableClause*)pNode, &isSuperTable, pVgroupHashmap);
    if (TSDB_CODE_SUCCESS != code) {
      taosHashCleanup(pVgroupHashmap);
      return code;
    }
    if (isSuperTable && LIST_LENGTH(pStmt->pTables) > 1) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_STABLE);
    }
  }

  if (isSuperTable) {
    taosHashCleanup(pVgroupHashmap);
    return TSDB_CODE_SUCCESS;
  }

  SArray* pBufArray = serializeVgroupsDropTableBatch(pCxt->pParseCxt->acctId, pVgroupHashmap);
  taosHashCleanup(pVgroupHashmap);
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return rewriteToVnodeModifyOpStmt(pQuery, pBufArray);
}

static SSchema* getColSchema(STableMeta* pTableMeta, const char* pTagName) {
  int32_t numOfFields = getNumOfTags(pTableMeta) + getNumOfColumns(pTableMeta);
  for (int32_t i = 0; i < numOfFields; ++i) {
    SSchema* pTagSchema = pTableMeta->schema + i;
    if (0 == strcmp(pTagName, pTagSchema->name)) {
      return pTagSchema;
    }
  }
  return NULL;
}

static int32_t buildUpdateTagValReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                    SVAlterTbReq* pReq) {
  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  pReq->tagName = strdup(pStmt->colName);
  if (NULL == pReq->tagName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (DEAL_RES_ERROR ==
      translateValueImpl(pCxt, pStmt->pVal, schemaToDataType(pTableMeta->tableInfo.precision, pSchema))) {
    return pCxt->errCode;
  }

  pReq->isNull = (TSDB_DATA_TYPE_NULL == pStmt->pVal->node.resType.type);
  if (pStmt->pVal->node.resType.type == TSDB_DATA_TYPE_JSON) {
    SKVRowBuilder kvRowBuilder = {0};
    int32_t       code = tdInitKVRowBuilder(&kvRowBuilder);

    if (TSDB_CODE_SUCCESS != code) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    if (pStmt->pVal->literal &&
        strlen(pStmt->pVal->literal) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
      return buildSyntaxErrMsg(&pCxt->msgBuf, "json string too long than 4095", pStmt->pVal->literal);
    }

    code = parseJsontoTagData(pStmt->pVal->literal, &kvRowBuilder, &pCxt->msgBuf, pSchema->colId);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    SKVRow row = tdGetKVRowFromBuilder(&kvRowBuilder);
    if (NULL == row) {
      tdDestroyKVRowBuilder(&kvRowBuilder);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pReq->nTagVal = kvRowLen(row);
    pReq->pTagVal = row;
    pStmt->pVal->datum.p = row;  // for free
    tdDestroyKVRowBuilder(&kvRowBuilder);
  } else {
    pReq->nTagVal = pStmt->pVal->node.resType.bytes;
    if (TSDB_DATA_TYPE_NCHAR == pStmt->pVal->node.resType.type) {
      pReq->nTagVal = pReq->nTagVal * TSDB_NCHAR_SIZE;
    }
    pReq->pTagVal = nodesGetValueFromNode(pStmt->pVal);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildAddColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                              SVAlterTbReq* pReq) {
  if (NULL != getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
  }

  pReq->colName = strdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pReq->type = pStmt->dataType.type;
  pReq->flags = COL_SMA_ON;
  pReq->bytes = pStmt->dataType.bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildDropColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  if (2 == getNumOfColumns(pTableMeta)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DROP_COL);
  }
  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (PRIMARYKEY_TIMESTAMP_COL_ID == pSchema->colId) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_CANNOT_DROP_PRIMARY_KEY);
  }

  pReq->colName = strdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildUpdateColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
  pReq->colModBytes = calcTypeBytes(pStmt->dataType);

  SSchema* pSchema = getColSchema(pTableMeta, pStmt->colName);
  if (NULL == pSchema) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  } else if (!IS_VAR_DATA_TYPE(pSchema->type) || pSchema->type != pStmt->dataType.type ||
             pSchema->bytes >= pReq->colModBytes) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_MODIFY_COL);
  }

  pReq->colName = strdup(pStmt->colName);
  if (NULL == pReq->colName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildRenameColReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                                 SVAlterTbReq* pReq) {
  if (NULL == getColSchema(pTableMeta, pStmt->colName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COLUMN, pStmt->colName);
  }
  if (NULL != getColSchema(pTableMeta, pStmt->newColName)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DUPLICATED_COLUMN);
  }

  pReq->colName = strdup(pStmt->colName);
  pReq->colNewName = strdup(pStmt->newColName);
  if (NULL == pReq->colName || NULL == pReq->colNewName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t buildUpdateOptionsReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (-1 != pStmt->pOptions->ttl) {
    code = checkRangeOption(pCxt, "ttl", pStmt->pOptions->ttl, TSDB_MIN_TABLE_TTL, INT32_MAX);
    if (TSDB_CODE_SUCCESS == code) {
      pReq->updateTTL = true;
      pReq->newTTL = pStmt->pOptions->ttl;
    }
  }

  if (TSDB_CODE_SUCCESS == code && '\0' != pStmt->pOptions->comment[0]) {
    pReq->updateComment = true;
    pReq->newComment = strdup(pStmt->pOptions->comment);
    if (NULL == pReq->newComment) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return code;
}

static int32_t buildAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, STableMeta* pTableMeta,
                               SVAlterTbReq* pReq) {
  pReq->tbName = strdup(pStmt->tableName);
  if (NULL == pReq->tbName) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->action = pStmt->alterType;

  switch (pStmt->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
    case TSDB_ALTER_TABLE_DROP_TAG:
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      return buildUpdateTagValReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      return buildAddColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      return buildDropColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      return buildUpdateColReq(pCxt, pStmt, pTableMeta, pReq);
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      return buildUpdateOptionsReq(pCxt, pStmt, pReq);
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      return buildRenameColReq(pCxt, pStmt, pTableMeta, pReq);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
}

static int32_t serializeAlterTbReq(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq,
                                   SArray* pArray) {
  SVgroupInfo vg = {0};
  int32_t     code = getTableHashVgroup(pCxt, pStmt->dbName, pStmt->tableName, &vg);
  int         tlen = 0;
  if (TSDB_CODE_SUCCESS == code) {
    tEncodeSize(tEncodeSVAlterTbReq, pReq, tlen, code);
  }
  if (TSDB_CODE_SUCCESS == code) {
    tlen += sizeof(SMsgHead);
    void* pMsg = taosMemoryMalloc(tlen);
    if (NULL == pMsg) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    ((SMsgHead*)pMsg)->vgId = htonl(vg.vgId);
    ((SMsgHead*)pMsg)->contLen = htonl(tlen);
    void*    pBuf = POINTER_SHIFT(pMsg, sizeof(SMsgHead));
    SEncoder coder = {0};
    tEncoderInit(&coder, pBuf, tlen - sizeof(SMsgHead));
    tEncodeSVAlterTbReq(&coder, pReq);
    tEncoderClear(&coder);

    SVgDataBlocks* pVgData = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == pVgData) {
      taosMemoryFree(pMsg);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pVgData->vg = vg;
    pVgData->pData = pMsg;
    pVgData->size = tlen;
    pVgData->numOfTables = 1;
    taosArrayPush(pArray, &pVgData);
  }

  return code;
}

static int32_t buildModifyVnodeArray(STranslateContext* pCxt, SAlterTableStmt* pStmt, SVAlterTbReq* pReq,
                                     SArray** pArray) {
  SArray* pTmpArray = taosArrayInit(1, sizeof(void*));
  if (NULL == pTmpArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = serializeAlterTbReq(pCxt, pStmt, pReq, pTmpArray);
  if (TSDB_CODE_SUCCESS == code) {
    *pArray = pTmpArray;
  } else {
    taosArrayDestroy(pTmpArray);
  }

  return code;
}

static int32_t rewriteAlterTable(STranslateContext* pCxt, SQuery* pQuery) {
  SAlterTableStmt* pStmt = (SAlterTableStmt*)pQuery->pRoot;

  STableMeta* pTableMeta = NULL;
  int32_t     code = getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pTableMeta);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_TAG) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
  }

  if (pStmt->dataType.type == TSDB_DATA_TYPE_JSON && pStmt->alterType == TSDB_ALTER_TABLE_ADD_COLUMN) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COL_JSON);
  }

  if (getNumOfTags(pTableMeta) == 1 && pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE,
                                "can not drop tag if there is only one tag");
  }

  if (TSDB_SUPER_TABLE == pTableMeta->tableType) {
    SSchema* pTagsSchema = getTableTagSchema(pTableMeta);
    if (getNumOfTags(pTableMeta) == 1 && pTagsSchema->type == TSDB_DATA_TYPE_JSON &&
        (pStmt->alterType == TSDB_ALTER_TABLE_ADD_TAG || pStmt->alterType == TSDB_ALTER_TABLE_DROP_TAG ||
         pStmt->alterType == TSDB_ALTER_TABLE_UPDATE_TAG_BYTES)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
    }
    return TSDB_CODE_SUCCESS;
  } else if (TSDB_CHILD_TABLE != pTableMeta->tableType && TSDB_NORMAL_TABLE != pTableMeta->tableType) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ALTER_TABLE);
  }

  SVAlterTbReq req = {0};
  code = buildAlterTbReq(pCxt, pStmt, pTableMeta, &req);

  SArray* pArray = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = buildModifyVnodeArray(pCxt, pStmt, &req, &pArray);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteToVnodeModifyOpStmt(pQuery, pArray);
  }

  return code;
}

static int32_t rewriteQuery(STranslateContext* pCxt, SQuery* pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pQuery->pRoot)) {
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
      code = rewriteShow(pCxt, pQuery);
      break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      if (NULL == ((SCreateTableStmt*)pQuery->pRoot)->pTags) {
        code = rewriteCreateTable(pCxt, pQuery);
      }
      break;
    case QUERY_NODE_CREATE_MULTI_TABLE_STMT:
      code = rewriteCreateMultiTable(pCxt, pQuery);
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = rewriteDropTable(pCxt, pQuery);
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
      code = rewriteAlterTable(pCxt, pQuery);
      break;
    default:
      break;
  }
  return code;
}

static int32_t setQuery(STranslateContext* pCxt, SQuery* pQuery) {
  switch (nodeType(pQuery->pRoot)) {
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SET_OPERATOR:
    case QUERY_NODE_EXPLAIN_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->haveResultSet = true;
      pQuery->msgType = TDMT_VND_QUERY;
      break;
    case QUERY_NODE_VNODE_MODIF_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
      pQuery->msgType = TDMT_VND_CREATE_TABLE;
      break;
    case QUERY_NODE_DESCRIBE_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      pQuery->haveResultSet = true;
      break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      pQuery->execMode = QUERY_EXEC_MODE_LOCAL;
      break;
    default:
      pQuery->execMode = QUERY_EXEC_MODE_RPC;
      if (NULL != pCxt->pCmdMsg) {
        TSWAP(pQuery->pCmdMsg, pCxt->pCmdMsg);
        pQuery->msgType = pQuery->pCmdMsg->msgType;
      }
      break;
  }

  if (pQuery->haveResultSet) {
    if (TSDB_CODE_SUCCESS != extractResultSchema(pQuery->pRoot, &pQuery->numOfResCols, &pQuery->pResSchema)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (nodeType(pQuery->pRoot) == QUERY_NODE_SELECT_STMT) {
      pQuery->precision = extractResultTsPrecision((SSelectStmt*)pQuery->pRoot);
    }
  }

  if (NULL != pCxt->pDbs) {
    pQuery->pDbList = taosArrayInit(taosHashGetSize(pCxt->pDbs), TSDB_DB_FNAME_LEN);
    if (NULL == pQuery->pDbList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SFullDatabaseName* pDb = taosHashIterate(pCxt->pDbs, NULL);
    while (NULL != pDb) {
      taosArrayPush(pQuery->pDbList, pDb->fullDbName);
      pDb = taosHashIterate(pCxt->pDbs, pDb);
    }
  }

  if (NULL != pCxt->pTables) {
    pQuery->pTableList = taosArrayInit(taosHashGetSize(pCxt->pTables), sizeof(SName));
    if (NULL == pQuery->pTableList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SName* pTable = taosHashIterate(pCxt->pTables, NULL);
    while (NULL != pTable) {
      taosArrayPush(pQuery->pTableList, pTable);
      pTable = taosHashIterate(pCxt->pTables, pTable);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t translate(SParseContext* pParseCxt, SQuery* pQuery) {
  STranslateContext cxt = {0};

  int32_t code = initTranslateContext(pParseCxt, &cxt);
  if (TSDB_CODE_SUCCESS == code) {
    code = fmFuncMgtInit();
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = rewriteQuery(&cxt, pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(&cxt, pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = setQuery(&cxt, pQuery);
  }
  destroyTranslateContext(&cxt);
  return code;
}
