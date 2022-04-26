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
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "tglobal.h"
#include "ttime.h"

#define GET_OPTION_VAL(pVal, defaultVal) (NULL == (pVal) ? (defaultVal) : getBigintFromValueNode((SValueNode*)(pVal)))

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

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode);
static int32_t translateQuery(STranslateContext* pCxt, SNode* pNode);

static bool afterGroupBy(ESqlClause clause) { return clause > SQL_CLAUSE_GROUP_BY; }

static bool beforeHaving(ESqlClause clause) { return clause < SQL_CLAUSE_HAVING; }

enum EDealRes generateDealNodeErrMsg(STranslateContext* pCxt, int32_t code, ...) {
  va_list ap;
  va_start(ap, code);
  generateSyntaxErrMsg(&pCxt->msgBuf, code, ap);
  va_end(ap);
  pCxt->errCode = code;
  return DEAL_RES_ERROR;
}

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

static void setColumnInfoByExpr(const STableNode* pTable, SExprNode* pExpr, SColumnNode* pCol) {
  pCol->pProjectRef = (SNode*)pExpr;
  if (NULL == pExpr->pAssociation) {
    pExpr->pAssociation = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  }
  taosArrayPush(pExpr->pAssociation, &pCol);
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

static int32_t createColumnNodeByTable(STranslateContext* pCxt, const STableNode* pTable, SNodeList* pList) {
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
      setColumnInfoByExpr(pTable, (SExprNode*)pNode, pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool findAndSetColumn(SColumnNode* pCol, const STableNode* pTable) {
  bool found = false;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId && 0 == strcmp(pCol->colName, PK_TS_COL_INTERNAL_NAME)) {
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
      if (0 == strcmp(pCol->colName, pExpr->aliasName)) {
        setColumnInfoByExpr(pTable, pExpr, pCol);
        found = true;
        break;
      }
    }
  }
  return found;
}

static EDealRes translateColumnWithPrefix(STranslateContext* pCxt, SColumnNode* pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    foundTable = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (belongTable(pCxt->pParseCxt->db, pCol, pTable)) {
      foundTable = true;
      if (findAndSetColumn(pCol, pTable)) {
        break;
      }
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
    }
  }
  if (!foundTable) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_TABLE_NOT_EXIST, pCol->tableAlias);
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateColumnWithoutPrefix(STranslateContext* pCxt, SColumnNode* pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t  nums = taosArrayGetSize(pTables);
  bool    found = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (findAndSetColumn(pCol, pTable)) {
      if (found) {
        return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pCol->colName);
      }
      found = true;
    }
  }
  if (!found) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
  }
  return DEAL_RES_CONTINUE;
}

static bool translateColumnUseAlias(STranslateContext* pCxt, SColumnNode* pCol) {
  SNodeList* pProjectionList = pCxt->pCurrStmt->pProjectionList;
  SNode*     pNode;
  FOREACH(pNode, pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    if (0 == strcmp(pCol->colName, pExpr->aliasName)) {
      setColumnInfoByExpr(NULL, pExpr, pCol);
      return true;
    }
  }
  return false;
}

static EDealRes translateColumn(STranslateContext* pCxt, SColumnNode* pCol) {
  // count(*)/first(*)/last(*) and so on
  if (0 == strcmp(pCol->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }

  EDealRes res = DEAL_RES_CONTINUE;
  if ('\0' != pCol->tableAlias[0]) {
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

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  uint8_t precision = (NULL != pCxt->pCurrStmt ? pCxt->pCurrStmt->precision : pVal->node.resType.precision);
  pVal->node.resType.precision = precision;
  if (pVal->placeholderNo > 0) {
    return DEAL_RES_CONTINUE;
  }
  if (pVal->isDuration) {
    if (parseNatualDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &pVal->unit, precision) !=
        TSDB_CODE_SUCCESS) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
    }
  } else {
    switch (pVal->node.resType.type) {
      case TSDB_DATA_TYPE_NULL:
        break;
      case TSDB_DATA_TYPE_BOOL:
        pVal->datum.b = (0 == strcasecmp(pVal->literal, "true"));
        break;
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_BIGINT: {
        char* endPtr = NULL;
        pVal->datum.i = strtoll(pVal->literal, &endPtr, 10);
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_UBIGINT: {
        char* endPtr = NULL;
        pVal->datum.u = strtoull(pVal->literal, &endPtr, 10);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE: {
        char* endPtr = NULL;
        pVal->datum.d = strtold(pVal->literal, &endPtr);
        break;
      }
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY: {
        pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
        if (NULL == pVal->datum.p) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
        }
        varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
        strncpy(varDataVal(pVal->datum.p), pVal->literal, pVal->node.resType.bytes);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        if (taosParseTime(pVal->literal, &pVal->datum.i, pVal->node.resType.bytes, precision, tsDaylight) !=
            TSDB_CODE_SUCCESS) {
          return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
        }
        break;
      }
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_JSON:
      case TSDB_DATA_TYPE_DECIMAL:
      case TSDB_DATA_TYPE_BLOB:
        // todo
      default:
        break;
    }
  }
  pVal->translate = true;
  return DEAL_RES_CONTINUE;
}

static EDealRes translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  if (nodesIsUnaryOp(pOp)) {
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
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (nodesIsArithmeticOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type || TSDB_DATA_TYPE_JSON == rdt.type ||
        TSDB_DATA_TYPE_BLOB == rdt.type) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
    }
    if ((TSDB_DATA_TYPE_TIMESTAMP == ldt.type && TSDB_DATA_TYPE_TIMESTAMP == rdt.type) ||
        (TSDB_DATA_TYPE_TIMESTAMP == ldt.type && IS_VAR_DATA_TYPE(rdt.type)) ||
        (TSDB_DATA_TYPE_TIMESTAMP == rdt.type && IS_VAR_DATA_TYPE(ldt.type))) {
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
  } else if (nodesIsComparisonOp(pOp)) {
    if (TSDB_DATA_TYPE_BLOB == ldt.type || TSDB_DATA_TYPE_JSON == rdt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
    }
    if (OP_TYPE_IN == pOp->opType || OP_TYPE_NOT_IN == pOp->opType) {
      ((SExprNode*)pOp->pRight)->resType = ((SExprNode*)pOp->pLeft)->resType;
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  } else if (nodesIsJsonOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON != ldt.type || TSDB_DATA_TYPE_BINARY != rdt.type) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_JSON;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_JSON].bytes;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes haveAggFunction(SNode* pNode, void* pContext) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId)) {
    *((bool*)pContext) = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode* pFunc) {
  SFmGetFuncInfoParam param = {.pCtg = pCxt->pParseCxt->pCatalog,
                               .pRpc = pCxt->pParseCxt->pTransporter,
                               .pMgmtEps = &pCxt->pParseCxt->mgmtEpSet,
                               .pErrBuf = pCxt->msgBuf.buf,
                               .errBufLen = pCxt->msgBuf.len};
  pCxt->errCode = fmGetFuncInfo(&param, pFunc);
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    return DEAL_RES_ERROR;
  }
  if (fmIsAggFunc(pFunc->funcId)) {
    if (beforeHaving(pCxt->currClause)) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);
    }
    bool haveAggFunc = false;
    nodesWalkExprs(pFunc->pParameterList, haveAggFunction, &haveAggFunc);
    if (haveAggFunc) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_AGG_FUNC_NESTING);
    }
    pCxt->pCurrStmt->hasAggFuncs = true;
  }

  return DEAL_RES_CONTINUE;
}

static EDealRes translateExprSubquery(STranslateContext* pCxt, SNode* pNode) {
  return (TSDB_CODE_SUCCESS == translateSubquery(pCxt, pNode) ? DEAL_RES_CONTINUE : DEAL_RES_ERROR);
}

static EDealRes translateLogicCond(STranslateContext* pCxt, SLogicConditionNode* pCond) {
  pCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  return DEAL_RES_CONTINUE;
}

static EDealRes doTranslateExpr(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
      return translateColumn(pCxt, (SColumnNode*)pNode);
    case QUERY_NODE_VALUE:
      return translateValue(pCxt, (SValueNode*)pNode);
    case QUERY_NODE_OPERATOR:
      return translateOperator(pCxt, (SOperatorNode*)pNode);
    case QUERY_NODE_FUNCTION:
      return translateFunction(pCxt, (SFunctionNode*)pNode);
    case QUERY_NODE_LOGIC_CONDITION:
      return translateLogicCond(pCxt, (SLogicConditionNode*)pNode);
    case QUERY_NODE_TEMP_TABLE:
      return translateExprSubquery(pCxt, ((STempTableNode*)pNode)->pSubquery);
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t translateExpr(STranslateContext* pCxt, SNode* pNode) {
  nodesWalkExprPostOrder(pNode, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static int32_t translateExprList(STranslateContext* pCxt, SNodeList* pList) {
  nodesWalkExprsPostOrder(pList, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static bool isAliasColumn(SColumnNode* pCol) { return ('\0' == pCol->tableAlias[0]); }

static bool isDistinctOrderBy(STranslateContext* pCxt) {
  return (SQL_CLAUSE_ORDER_BY == pCxt->currClause && pCxt->pCurrStmt->isDistinct);
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

static EDealRes doCheckExprForGroupBy(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  if (!nodesIsExprNode(pNode) || (QUERY_NODE_COLUMN == nodeType(pNode) && isAliasColumn((SColumnNode*)pNode))) {
    return DEAL_RES_CONTINUE;
  }
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId) &&
      !isDistinctOrderBy(pCxt)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  SNode* pGroupNode;
  FOREACH(pGroupNode, getGroupByList(pCxt)) {
    if (nodesEqualNode(getGroupByNode(pGroupNode), pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
  }
  if (QUERY_NODE_COLUMN == nodeType(pNode) ||
      (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId) &&
       isDistinctOrderBy(pCxt))) {
    return generateDealNodeErrMsg(pCxt, getGroupByErrorCode(pCxt));
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkExprForGroupBy(STranslateContext* pCxt, SNode* pNode) {
  nodesWalkExpr(pNode, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

static int32_t checkExprListForGroupBy(STranslateContext* pCxt, SNodeList* pList) {
  if (NULL == getGroupByList(pCxt)) {
    return TSDB_CODE_SUCCESS;
  }
  nodesWalkExprs(pList, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

typedef struct CheckAggColCoexistCxt {
  STranslateContext* pTranslateCxt;
  bool               existAggFunc;
  bool               existCol;
} CheckAggColCoexistCxt;

static EDealRes doCheckAggColCoexist(SNode* pNode, void* pContext) {
  CheckAggColCoexistCxt* pCxt = (CheckAggColCoexistCxt*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId)) {
    pCxt->existAggFunc = true;
    return DEAL_RES_IGNORE_CHILD;
  }
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    pCxt->existCol = true;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkAggColCoexist(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList) {
    return TSDB_CODE_SUCCESS;
  }
  CheckAggColCoexistCxt cxt = {.pTranslateCxt = pCxt, .existAggFunc = false, .existCol = false};
  nodesWalkExprs(pSelect->pProjectionList, doCheckAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesWalkExprs(pSelect->pOrderByList, doCheckAggColCoexist, &cxt);
  }
  if ((cxt.existAggFunc || NULL != pSelect->pWindow) && cxt.existCol) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NOT_SINGLE_GROUP);
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
    // if (0 != strcmp(pRealTable->qualDbName, TSDB_INFORMATION_SCHEMA_DB)) {
    code = getDBVgInfo(pCxt, pRealTable->qualDbName, &vgroupList);
    // }
  } else {
    code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
  }

  // todo release after mnode can be processed
  // if (TSDB_CODE_SUCCESS == code) {
  //   code = addMnodeToVgroupList(&pCxt->pParseCxt->mgmtEpSet, &vgroupList);
  // }

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

static uint8_t getJoinTablePrecision(SJoinTableNode* pJoinTable) {
  uint8_t lp = ((STableNode*)pJoinTable->pLeft)->precision;
  uint8_t rp = ((STableNode*)pJoinTable->pRight)->precision;
  return (lp > rp ? rp : lp);
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
        code = translateExpr(pCxt, pJoinTable->pOnCond);
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
    int32_t     code = createColumnNodeByTable(pCxt, pTable, *pCols);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool isMultiResFunc(SNode* pNode) {
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
    code = createColumnNodeByTable(pCxt, pTable, *pOutput);
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

static bool isCountStar(SNode* pNode) {
  if (QUERY_NODE_FUNCTION != nodeType(pNode) || 1 != LIST_LENGTH(((SFunctionNode*)pNode)->pParameterList)) {
    return false;
  }
  SNode* pPara = nodesListGetNode(((SFunctionNode*)pNode)->pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pPara) && 0 == strcmp(((SColumnNode*)pPara)->colName, "*"));
}

static int32_t rewriteCountStar(STranslateContext* pCxt, SFunctionNode* pCount) {
  SColumnNode* pCol = nodesListGetNode(pCount->pParameterList, 0);
  STableNode*  pTable = NULL;
  int32_t      code = findTable(pCxt, ('\0' == pCol->tableAlias[0] ? NULL : pCol->tableAlias), &pTable);
  if (TSDB_CODE_SUCCESS == code && QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    setColumnInfoBySchema((SRealTableNode*)pTable, ((SRealTableNode*)pTable)->pMeta->schema, false, pCol);
  }
  return code;
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pProjectionList) {  // select * ...
    return createAllColumns(pCxt, &pSelect->pProjectionList);
  } else {
    SNode* pNode = NULL;
    WHERE_EACH(pNode, pSelect->pProjectionList) {
      if (isMultiResFunc(pNode)) {
        SNodeList* pFuncs = NULL;
        if (TSDB_CODE_SUCCESS != createMultiResFuncsFromStar(pCxt, (SFunctionNode*)pNode, &pFuncs)) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        INSERT_LIST(pSelect->pProjectionList, pFuncs);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      } else if (isTableStar(pNode)) {
        SNodeList* pCols = NULL;
        if (TSDB_CODE_SUCCESS != createTableAllCols(pCxt, (SColumnNode*)pNode, &pCols)) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        INSERT_LIST(pSelect->pProjectionList, pCols);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
      } else if (isCountStar(pNode)) {
        int32_t code = rewriteCountStar(pCxt, (SFunctionNode*)pNode);
        if (TSDB_CODE_SUCCESS != code) {
          return code;
        }
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
        setColumnInfoByExpr(NULL, (SExprNode*)nodesListGetNode(pProjectionList, pos - 1), pCol);
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
  int32_t code = translateExpr(pCxt, pSelect->pHaving);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprForGroupBy(pCxt, pSelect->pHaving);
  }
  return code;
}

static int32_t translateGroupBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL != pSelect->pGroupByList && NULL != pSelect->pWindow) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST);
  }
  pCxt->currClause = SQL_CLAUSE_GROUP_BY;
  return translateExprList(pCxt, pSelect->pGroupByList);
}

static bool isValTimeUnit(char unit) { return ('n' == unit || 'y' == unit); }

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

static int32_t checkIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  uint8_t precision = ((SColumnNode*)pInterval->pCol)->node.resType.precision;

  SValueNode* pInter = (SValueNode*)pInterval->pInterval;
  bool        valInter = isValTimeUnit(pInter->unit);
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
    bool fixed = !isValTimeUnit(pOffset->unit) && !valInter;
    if ((fixed && pOffset->datum.i >= pInter->datum.i) ||
        (!fixed && getMonthsFromTimeVal(pOffset->datum.i, precision, pOffset->unit) >=
                       getMonthsFromTimeVal(pInter->datum.i, precision, pInter->unit))) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);
    }
  }

  if (NULL != pInterval->pSliding) {
    const static int32_t INTERVAL_SLIDING_FACTOR = 100;

    SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
    if (pInter->unit == 'n' || pInter->unit == 'y') {
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

  return TSDB_CODE_SUCCESS;
}

static EDealRes checkStateExpr(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    STranslateContext* pCxt = pContext;
    SColumnNode*       pCol = (SColumnNode*)pNode;
    if (!IS_INTEGER_TYPE(pCol->node.resType.type)) {
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

static int32_t checkWindow(STranslateContext* pCxt, SNode* pWindow) {
  switch (nodeType(pWindow)) {
    case QUERY_NODE_STATE_WINDOW:
      return checkStateWindow(pCxt, (SStateWindowNode*)pWindow);
    case QUERY_NODE_SESSION_WINDOW:
      return checkSessionWindow(pCxt, (SSessionWindowNode*)pWindow);
    case QUERY_NODE_INTERVAL_WINDOW:
      return checkIntervalWindow(pCxt, (SIntervalWindowNode*)pWindow);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateWindow(STranslateContext* pCxt, SNode* pWindow) {
  if (NULL == pWindow) {
    return TSDB_CODE_SUCCESS;
  }
  pCxt->currClause = SQL_CLAUSE_WINDOW;
  int32_t code = translateExpr(pCxt, pWindow);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkWindow(pCxt, pWindow);
  }
  return code;
}

static int32_t translatePartitionBy(STranslateContext* pCxt, SNodeList* pPartitionByList) {
  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  return translateExprList(pCxt, pPartitionByList);
}

static int32_t translateWhere(STranslateContext* pCxt, SNode* pWhere) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  return translateExpr(pCxt, pWhere);
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
    code = translateWindow(pCxt, pSelect->pWindow);
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
  return code;
}

static SNode* createSetOperProject(SNode* pNode) {
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    return NULL;
  }
  pCol->node.resType = ((SExprNode*)pNode)->resType;
  strcpy(pCol->colName, ((SExprNode*)pNode)->aliasName);
  strcpy(pCol->node.aliasName, pCol->colName);
  return (SNode*)pCol;
}

static bool dataTypeEqual(const SDataType* l, const SDataType* r) {
  return (l->type == r->type && l->bytes == l->bytes && l->precision == r->precision && l->scale == l->scale);
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
  if (DEAL_RES_ERROR == translateFunction(pCxt, pFunc)) {
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
    if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pSetOperator->pProjectionList, createSetOperProject(pLeft))) {
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
  pReq->numOfVgroups = GET_OPTION_VAL(pStmt->pOptions->pNumOfVgroups, TSDB_DEFAULT_VN_PER_DB);
  pReq->cacheBlockSize = GET_OPTION_VAL(pStmt->pOptions->pCacheBlockSize, TSDB_DEFAULT_CACHE_BLOCK_SIZE);
  pReq->totalBlocks = GET_OPTION_VAL(pStmt->pOptions->pNumOfBlocks, TSDB_DEFAULT_TOTAL_BLOCKS);
  pReq->daysPerFile = GET_OPTION_VAL(pStmt->pOptions->pDaysPerFile, TSDB_DEFAULT_DAYS_PER_FILE);
  pReq->daysToKeep0 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 0), TSDB_DEFAULT_KEEP);
  pReq->daysToKeep1 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 1), TSDB_DEFAULT_KEEP);
  pReq->daysToKeep2 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 2), TSDB_DEFAULT_KEEP);
  pReq->minRows = GET_OPTION_VAL(pStmt->pOptions->pMinRowsPerBlock, TSDB_DEFAULT_MINROWS_FBLOCK);
  pReq->maxRows = GET_OPTION_VAL(pStmt->pOptions->pMaxRowsPerBlock, TSDB_DEFAULT_MAXROWS_FBLOCK);
  pReq->commitTime = -1;
  pReq->fsyncPeriod = GET_OPTION_VAL(pStmt->pOptions->pFsyncPeriod, TSDB_DEFAULT_FSYNC_PERIOD);
  pReq->walLevel = GET_OPTION_VAL(pStmt->pOptions->pWalLevel, TSDB_DEFAULT_WAL_LEVEL);
  pReq->precision = GET_OPTION_VAL(pStmt->pOptions->pPrecision, TSDB_TIME_PRECISION_MILLI);
  pReq->compression = GET_OPTION_VAL(pStmt->pOptions->pCompressionLevel, TSDB_DEFAULT_COMP_LEVEL);
  pReq->replications = GET_OPTION_VAL(pStmt->pOptions->pReplica, TSDB_DEFAULT_DB_REPLICA);
  pReq->update = -1;
  pReq->cacheLastRow = GET_OPTION_VAL(pStmt->pOptions->pCachelast, TSDB_DEFAULT_CACHE_LAST_ROW);
  pReq->ignoreExist = pStmt->ignoreExists;
  pReq->streamMode = GET_OPTION_VAL(pStmt->pOptions->pStreamMode, TSDB_DEFAULT_DB_STREAM_MODE);
  pReq->ttl = GET_OPTION_VAL(pStmt->pOptions->pTtl, TSDB_DEFAULT_DB_TTL);
  pReq->singleSTable = GET_OPTION_VAL(pStmt->pOptions->pSingleStable, TSDB_DEFAULT_DB_SINGLE_STABLE);
  pReq->strict = GET_OPTION_VAL(pStmt->pOptions->pStrict, TSDB_DEFAULT_DB_STRICT);

  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, const char* pName, SValueNode* pVal, int32_t minVal,
                                int32_t maxVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (pVal->isDuration &&
        (TIME_UNIT_MINUTE != pVal->unit && TIME_UNIT_HOUR != pVal->unit && TIME_UNIT_DAY != pVal->unit)) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_OPTION_UNIT, pName, pVal->unit);
    }
    int64_t val = getBigintFromValueNode(pVal);
    if (val < minVal || val > maxVal) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_RANGE_OPTION, pName, val, minVal, maxVal);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void convertValueFromStrToInt(SValueNode* pVal, int64_t val) {
  taosMemoryFreeClear(pVal->datum.p);
  pVal->datum.i = val;
  pVal->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pVal->node.resType.bytes = tDataTypes[pVal->node.resType.type].bytes;
}

static int32_t checkDbPrecisionOption(STranslateContext* pCxt, SValueNode* pVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    char* pRrecision = varDataVal(pVal->datum.p);
    if (0 == strcmp(pRrecision, TSDB_TIME_PRECISION_MILLI_STR)) {
      convertValueFromStrToInt(pVal, TSDB_TIME_PRECISION_MILLI);
    } else if (0 == strcmp(pRrecision, TSDB_TIME_PRECISION_MICRO_STR)) {
      convertValueFromStrToInt(pVal, TSDB_TIME_PRECISION_MICRO);
    } else if (0 == strcmp(pRrecision, TSDB_TIME_PRECISION_NANO_STR)) {
      convertValueFromStrToInt(pVal, TSDB_TIME_PRECISION_NANO);
    } else {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STR_OPTION, "precision", pVal->datum.p);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbEnumOption(STranslateContext* pCxt, const char* pName, SValueNode* pVal, int32_t v1, int32_t v2) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    int64_t val = pVal->datum.i;
    if (val != v1 && val != v2) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ENUM_OPTION, pName, val, v1, v2);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkTtlOption(STranslateContext* pCxt, SValueNode* pVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    int64_t val = pVal->datum.i;
    if (val < TSDB_MIN_DB_TTL) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_TTL_OPTION, val, TSDB_MIN_DB_TTL);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkKeepOption(STranslateContext* pCxt, SNodeList* pKeep) {
  if (NULL == pKeep) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfKeep = LIST_LENGTH(pKeep);
  if (numOfKeep > 3 || numOfKeep < 1) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_NUM);
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pKeep) {
    if (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pNode)) {
      return pCxt->errCode;
    }
  }

  if (1 == numOfKeep) {
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pKeep, nodesCloneNode(nodesListGetNode(pKeep, 0)))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    ++numOfKeep;
  }
  if (2 == numOfKeep) {
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pKeep, nodesCloneNode(nodesListGetNode(pKeep, 1)))) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  SValueNode* pKeep0 = (SValueNode*)nodesListGetNode(pKeep, 0);
  SValueNode* pKeep1 = (SValueNode*)nodesListGetNode(pKeep, 1);
  SValueNode* pKeep2 = (SValueNode*)nodesListGetNode(pKeep, 2);
  if ((pKeep0->isDuration &&
       (TIME_UNIT_MINUTE != pKeep0->unit && TIME_UNIT_HOUR != pKeep0->unit && TIME_UNIT_DAY != pKeep0->unit)) ||
      (pKeep1->isDuration &&
       (TIME_UNIT_MINUTE != pKeep1->unit && TIME_UNIT_HOUR != pKeep1->unit && TIME_UNIT_DAY != pKeep1->unit)) ||
      (pKeep2->isDuration &&
       (TIME_UNIT_MINUTE != pKeep2->unit && TIME_UNIT_HOUR != pKeep2->unit && TIME_UNIT_DAY != pKeep2->unit))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_UNIT, pKeep0->unit, pKeep1->unit,
                                pKeep2->unit);
  }

  int32_t daysToKeep0 = getBigintFromValueNode(pKeep0);
  int32_t daysToKeep1 = getBigintFromValueNode(pKeep1);
  int32_t daysToKeep2 = getBigintFromValueNode(pKeep2);
  if (daysToKeep0 < TSDB_MIN_KEEP || daysToKeep1 < TSDB_MIN_KEEP || daysToKeep2 < TSDB_MIN_KEEP ||
      daysToKeep0 > TSDB_MAX_KEEP || daysToKeep1 > TSDB_MAX_KEEP || daysToKeep2 > TSDB_MAX_KEEP) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_VALUE, daysToKeep0, daysToKeep1, daysToKeep2,
                                TSDB_MIN_KEEP, TSDB_MAX_KEEP);
  }

  if (!((daysToKeep0 <= daysToKeep1) && (daysToKeep1 <= daysToKeep2))) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_KEEP_ORDER);
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

  SNode* pNode = NULL;
  FOREACH(pNode, pRetentions) {
    SNode* pVal = NULL;
    FOREACH(pVal, ((SNodeListNode*)pNode)->pNodeList) {
      if (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pVal)) {
        return pCxt->errCode;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkOptionsDependency(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions,
                                      bool alter) {
  if (NULL == pOptions->pDaysPerFile && NULL == pOptions->pKeep) {
    return TSDB_CODE_SUCCESS;
  }
  int64_t daysPerFile = GET_OPTION_VAL(pOptions->pDaysPerFile, alter ? -1 : TSDB_DEFAULT_DAYS_PER_FILE);
  int64_t daysToKeep0 = GET_OPTION_VAL(nodesListGetNode(pOptions->pKeep, 0), alter ? -1 : TSDB_DEFAULT_KEEP);
  if (alter && (-1 == daysPerFile || -1 == daysToKeep0)) {
    SDbCfgInfo dbCfg;
    int32_t    code = getDBCfg(pCxt, pDbName, &dbCfg);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    daysPerFile = (-1 == daysPerFile ? dbCfg.daysPerFile : daysPerFile);
    daysToKeep0 = (-1 == daysPerFile ? dbCfg.daysToKeep0 : daysToKeep0);
  }
  if (daysPerFile > daysToKeep0) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_DAYS_VALUE);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checkDatabaseOptions(STranslateContext* pCxt, const char* pDbName, SDatabaseOptions* pOptions,
                                    bool alter) {
  int32_t code =
      checkRangeOption(pCxt, "totalBlocks", pOptions->pNumOfBlocks, TSDB_MIN_TOTAL_BLOCKS, TSDB_MAX_TOTAL_BLOCKS);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "cacheBlockSize", pOptions->pCacheBlockSize, TSDB_MIN_CACHE_BLOCK_SIZE,
                            TSDB_MAX_CACHE_BLOCK_SIZE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "cacheLast", pOptions->pCachelast, TSDB_MIN_DB_CACHE_LAST_ROW,
                            TSDB_MAX_DB_CACHE_LAST_ROW);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "compression", pOptions->pCompressionLevel, TSDB_MIN_COMP_LEVEL, TSDB_MAX_COMP_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkRangeOption(pCxt, "daysPerFile", pOptions->pDaysPerFile, TSDB_MIN_DAYS_PER_FILE, TSDB_MAX_DAYS_PER_FILE);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "fsyncPeriod", pOptions->pFsyncPeriod, TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "maxRowsPerBlock", pOptions->pMaxRowsPerBlock, TSDB_MIN_MAXROWS_FBLOCK,
                            TSDB_MAX_MAXROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "minRowsPerBlock", pOptions->pMinRowsPerBlock, TSDB_MIN_MINROWS_FBLOCK,
                            TSDB_MAX_MINROWS_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkKeepOption(pCxt, pOptions->pKeep);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbPrecisionOption(pCxt, pOptions->pPrecision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "replications", pOptions->pReplica, TSDB_MIN_DB_REPLICA, TSDB_MAX_DB_REPLICA);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTtlOption(pCxt, pOptions->pTtl);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "walLevel", pOptions->pWalLevel, TSDB_MIN_WAL_LEVEL, TSDB_MAX_WAL_LEVEL);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "vgroups", pOptions->pNumOfVgroups, TSDB_MIN_VNODES_PER_DB, TSDB_MAX_VNODES_PER_DB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "singleStable", pOptions->pSingleStable, TSDB_DB_SINGLE_STABLE_ON,
                             TSDB_DB_SINGLE_STABLE_OFF);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code =
        checkDbEnumOption(pCxt, "streamMode", pOptions->pStreamMode, TSDB_DB_STREAM_MODE_OFF, TSDB_DB_STREAM_MODE_ON);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRetentionsOption(pCxt, pOptions->pRetentions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "strict", pOptions->pStrict, TSDB_DB_STRICT_OFF, TSDB_DB_STRICT_ON);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkOptionsDependency(pCxt, pDbName, pOptions, alter);
  }
  return code;
}

static int32_t checkCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  return checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions, false);
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
  pReq->totalBlocks = GET_OPTION_VAL(pStmt->pOptions->pNumOfBlocks, -1);
  pReq->daysToKeep0 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 0), -1);
  pReq->daysToKeep1 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 1), -1);
  pReq->daysToKeep2 = GET_OPTION_VAL(nodesListGetNode(pStmt->pOptions->pKeep, 2), -1);
  pReq->fsyncPeriod = GET_OPTION_VAL(pStmt->pOptions->pFsyncPeriod, -1);
  pReq->walLevel = GET_OPTION_VAL(pStmt->pOptions->pWalLevel, -1);
  pReq->strict = GET_OPTION_VAL(pStmt->pOptions->pQuorum, -1);
  pReq->cacheLastRow = GET_OPTION_VAL(pStmt->pOptions->pCachelast, -1);
  pReq->replications = GET_OPTION_VAL(pStmt->pOptions->pReplica, -1);
  return;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  int32_t code = checkDatabaseOptions(pCxt, pStmt->dbName, pStmt->pOptions, true);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SAlterDbReq alterReq = {0};
  buildAlterDbReq(pCxt, pStmt, &alterReq);

  return buildCmdMsg(pCxt, TDMT_MND_ALTER_DB, (FSerializeFunc)tSerializeSAlterDbReq, &alterReq);
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

static int32_t columnDefNodeToField(SNodeList* pList, SArray** pArray) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    SField          field = {.type = pCol->dataType.type, .bytes = calcTypeBytes(pCol->dataType)};
    strcpy(field.name, pCol->colName);
    if (pCol->sma) {
      field.flags |= SCHEMA_SMA_ON;
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

static int32_t checkTableCommentOption(STranslateContext* pCxt, SValueNode* pVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (pVal->node.resType.bytes >= TSDB_STB_COMMENT_LEN) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_COMMENT_OPTION, TSDB_STB_COMMENT_LEN - 1);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t checTableFactorOption(STranslateContext* pCxt, SValueNode* pVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    if (pVal->datum.d < TSDB_MIN_DB_FILE_FACTOR || pVal->datum.d > TSDB_MAX_DB_FILE_FACTOR) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_F_RANGE_OPTION, "file_factor", pVal->datum.d,
                                  TSDB_MIN_DB_FILE_FACTOR, TSDB_MAX_DB_FILE_FACTOR);
    }
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

static int32_t checkTableTags(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  SNode* pNode;
  FOREACH(pNode, pStmt->pTags) {
    SColumnDefNode* pCol = (SColumnDefNode*)pNode;
    if (pCol->dataType.type == TSDB_DATA_TYPE_JSON && LIST_LENGTH(pStmt->pTags) > 1) {
      return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_ONLY_ONE_JSON_TAG);
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

static int32_t checkCreateTable(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  int32_t code = checkKeepOption(pCxt, pStmt->pOptions->pKeep);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTtlOption(pCxt, pStmt->pOptions->pTtl);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableCommentOption(pCxt, pStmt->pOptions->pComments);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableSmaOption(pCxt, pStmt);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableRollupOption(pCxt, pStmt->pOptions->pFuncs);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checTableFactorOption(pCxt, pStmt->pOptions->pFilesFactor);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "delay", pStmt->pOptions->pDelay, TSDB_MIN_DB_DELAY, TSDB_MAX_DB_DELAY);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkTableTags(pCxt, pStmt);
  }
  return code;
}

static void toSchema(const SColumnDefNode* pCol, col_id_t colId, SSchema* pSchema) {
  int8_t flags = 0;
  if (pCol->sma) {
    flags |= SCHEMA_SMA_ON;
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
  TSWAP(pTable->pMeta, pInfo->pRollupTableMeta, STableMeta*);
  pSelect->pFromTable = (SNode*)pTable;

  TSWAP(pSelect->pProjectionList, pInfo->pFuncs, SNodeList*);
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
  TSWAP(pInterval->pInterval, pInfo->pInterval, SNode*);
  TSWAP(pInterval->pOffset, pInfo->pOffset, SNode*);
  TSWAP(pInterval->pSliding, pInfo->pSliding, SNode*);
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
  FOREACH(pFunc, pStmt->pOptions->pFuncs) {
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
  return code;
}

static int32_t buildCreateStbReq(STranslateContext* pCxt, SCreateTableStmt* pStmt, SMCreateStbReq* pReq) {
  pReq->igExists = pStmt->ignoreExists;
  pReq->xFilesFactor = GET_OPTION_VAL(pStmt->pOptions->pFilesFactor, TSDB_DEFAULT_DB_FILE_FACTOR);
  pReq->delay = GET_OPTION_VAL(pStmt->pOptions->pDelay, TSDB_DEFAULT_DB_DELAY);
  columnDefNodeToField(pStmt->pCols, &pReq->pColumns);
  columnDefNodeToField(pStmt->pTags, &pReq->pTags);
  pReq->numOfColumns = LIST_LENGTH(pStmt->pCols);
  pReq->numOfTags = LIST_LENGTH(pStmt->pTags);

  SName tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), pReq->name);

  return buildRollupAst(pCxt, pStmt, pReq);
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
  if ((TSDB_CODE_TDB_INVALID_TABLE_ID == code || TSDB_CODE_VND_TB_NOT_EXIST == code) && pClause->ignoreNotExists) {
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

static int32_t setAlterTableField(SAlterTableStmt* pStmt, SMAltertbReq* pAlterReq) {
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
      TAOS_FIELD field = {.type = pStmt->dataType.type, .bytes = pStmt->dataType.bytes};
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
      strcpy(oldField.name, pStmt->newColName);
      taosArrayPush(pAlterReq->pFields, &newField);
      break;
    }
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t translateAlterTable(STranslateContext* pCxt, SAlterTableStmt* pStmt) {
  SMAltertbReq alterReq = {0};
  SName        tableName;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pStmt->dbName, pStmt->tableName, &tableName), alterReq.name);
  alterReq.alterType = pStmt->alterType;
  alterReq.numOfFields = 1;
  if (TSDB_ALTER_TABLE_UPDATE_OPTIONS == pStmt->alterType) {
    // todo
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
  SVDropTSmaReq dropSmaReq = {0};
  strcpy(dropSmaReq.indexName, pStmt->indexName);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_VND_DROP_SMA;
  pCxt->pCmdMsg->msgLen = tSerializeSVDropTSmaReq(NULL, &dropSmaReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  void* pBuf = pCxt->pCmdMsg->pMsg;
  tSerializeSVDropTSmaReq(&pBuf, &dropSmaReq);

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
  /*tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->topicName, &name), pReq->name);*/
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
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->topicName, &name), dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;

  return buildCmdMsg(pCxt, TDMT_MND_DROP_TOPIC, (FSerializeFunc)tSerializeSMDropTopicReq, &dropReq);
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
  return getTableMeta(pCxt, pStmt->dbName, pStmt->tableName, &pStmt->pMeta);
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

static int32_t translateCreateStream(STranslateContext* pCxt, SCreateStreamStmt* pStmt) {
  SCMCreateStreamReq createReq = {0};

  createReq.igExists = pStmt->ignoreExists;

  SName name;
  tNameExtractFullName(toName(pCxt->pParseCxt->acctId, pCxt->pParseCxt->db, pStmt->streamName, &name), createReq.name);

  if ('\0' != pStmt->targetTabName[0]) {
    strcpy(name.dbname, pStmt->targetDbName);
    strcpy(name.tname, pStmt->targetTabName);
    tNameExtractFullName(&name, createReq.outputSTbName);
  }

  int32_t code = translateQuery(pCxt, pStmt->pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString(pStmt->pQuery, false, &createReq.ast, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    createReq.sql = strdup(pCxt->pParseCxt->pSql);
    if (NULL == createReq.sql) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (TSDB_CODE_SUCCESS == code && NULL != pStmt->pOptions->pWatermark) {
    code = (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pWatermark)) ? pCxt->errCode
                                                                                              : TSDB_CODE_SUCCESS;
  }
  if (TSDB_CODE_SUCCESS == code) {
    createReq.triggerType = pStmt->pOptions->triggerType;
    createReq.watermark =
        (NULL != pStmt->pOptions->pWatermark ? ((SValueNode*)pStmt->pOptions->pWatermark)->datum.i : 0);
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
    case QUERY_NODE_CREATE_STREAM_STMT:
      code = translateCreateStream(pCxt, (SCreateStreamStmt*)pNode);
      break;
    case QUERY_NODE_DROP_STREAM_STMT:
      code = translateDropStream(pCxt, (SDropStreamStmt*)pNode);
      break;
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      code = translateCreateFunction(pCxt, (SCreateFunctionStmt*)pNode);
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
    strcpy((*pSchema)[index].name, pExpr->aliasName);
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

typedef struct SVgroupTablesBatch {
  SVCreateTbBatchReq req;
  SVgroupInfo        info;
  char               dbName[TSDB_DB_NAME_LEN];
} SVgroupTablesBatch;

static void destroyCreateTbReq(SVCreateTbReq* pReq) {
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->ntb.schema.pSchema);
}

static int32_t buildSmaParam(STableOptions* pOptions, SVCreateTbReq* pReq) {
  if (0 == LIST_LENGTH(pOptions->pFuncs)) {
    return TSDB_CODE_SUCCESS;
  }

#if 0
  pReq->ntbCfg.pRSmaParam = taosMemoryCalloc(1, sizeof(SRSmaParam));
  if (NULL == pReq->ntbCfg.pRSmaParam) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->ntbCfg.pRSmaParam->delay = GET_OPTION_VAL(pOptions->pDelay, TSDB_DEFAULT_DB_DELAY);
  pReq->ntbCfg.pRSmaParam->xFilesFactor = GET_OPTION_VAL(pOptions->pFilesFactor, TSDB_DEFAULT_DB_FILE_FACTOR);
#endif

  return TSDB_CODE_SUCCESS;
}

static int32_t buildNormalTableBatchReq(int32_t acctId, const SCreateTableStmt* pStmt, const SVgroupInfo* pVgroupInfo,
                                        SVgroupTablesBatch* pBatch) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pStmt->dbName);
  tNameGetFullDbName(&name, dbFName);

  SVCreateTbReq req = {0};
  req.type = TD_NORMAL_TABLE;
  req.name = strdup(pStmt->tableName);
  req.ntb.schema.nCols = LIST_LENGTH(pStmt->pCols);
  req.ntb.schema.sver = 0;
  req.ntb.schema.pSchema = taosMemoryCalloc(req.ntb.schema.nCols, sizeof(SSchema));
  if (NULL == req.name || NULL == req.ntb.schema.pSchema) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SNode*   pCol;
  col_id_t index = 0;
  FOREACH(pCol, pStmt->pCols) {
    toSchema((SColumnDefNode*)pCol, index + 1, req.ntb.schema.pSchema + index);
    ++index;
  }
  if (TSDB_CODE_SUCCESS != buildSmaParam(pStmt->pOptions, &req)) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
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

static int32_t serializeVgroupTablesBatch(SVgroupTablesBatch* pTbBatch, SArray* pBufArray) {
  int    tlen;
  SCoder coder = {0};

  tEncodeSize(tEncodeSVCreateTbBatchReq, &pTbBatch->req, tlen);
  tlen += sizeof(SMsgHead);  //+ tSerializeSVCreateTbBatchReq(NULL, &(pTbBatch->req));
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  tCoderInit(&coder, TD_LITTLE_ENDIAN, pBuf, tlen - sizeof(SMsgHead), TD_ENCODER);
  tEncodeSVCreateTbBatchReq(&coder, &pTbBatch->req);
  tCoderClear(&coder);

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

static void destroyCreateTbReqBatch(SVgroupTablesBatch* pTbBatch) {
  size_t size = taosArrayGetSize(pTbBatch->req.pArray);
  for (int32_t i = 0; i < size; ++i) {
    SVCreateTbReq* pTableReq = taosArrayGet(pTbBatch->req.pArray, i);
    taosMemoryFreeClear(pTableReq->name);

    if (pTableReq->type == TSDB_NORMAL_TABLE) {
      taosMemoryFreeClear(pTableReq->ntb.schema.pSchema);
    } else if (pTableReq->type == TSDB_CHILD_TABLE) {
      taosMemoryFreeClear(pTableReq->ctb.pTag);
    }
  }

  taosArrayDestroy(pTbBatch->req.pArray);
}

static int32_t rewriteToVnodeModifOpStmt(SQuery* pQuery, SArray* pBufArray) {
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

  SVgroupTablesBatch tbatch = {0};
  int32_t            code = buildNormalTableBatchReq(acctId, pStmt, pInfo, &tbatch);
  if (TSDB_CODE_SUCCESS == code) {
    code = serializeVgroupTablesBatch(&tbatch, *pBufArray);
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
    code = rewriteToVnodeModifOpStmt(pQuery, pBufArray);
    if (TSDB_CODE_SUCCESS != code) {
      destroyCreateTbReqArray(pBufArray);
    }
  }

  return code;
}

static void addCreateTbReqIntoVgroup(int32_t acctId, SHashObj* pVgroupHashmap, const char* pDbName,
                                     const char* pTableName, SKVRow row, uint64_t suid, SVgroupInfo* pVgInfo) {
  char  dbFName[TSDB_DB_FNAME_LEN] = {0};
  SName name = {.type = TSDB_DB_NAME_T, .acctId = acctId};
  strcpy(name.dbname, pDbName);
  tNameGetFullDbName(&name, dbFName);

  struct SVCreateTbReq req = {0};
  req.type = TD_CHILD_TABLE;
  req.name = strdup(pTableName);
  req.ctb.suid = suid;
  req.ctb.pTag = row;

  SVgroupTablesBatch* pTableBatch = taosHashGet(pVgroupHashmap, &pVgInfo->vgId, sizeof(pVgInfo->vgId));
  if (pTableBatch == NULL) {
    SVgroupTablesBatch tBatch = {0};
    tBatch.info = *pVgInfo;
    strcpy(tBatch.dbName, pDbName);

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

  if (pVal->node.resType.type == TSDB_DATA_TYPE_NULL) {
    // todo
  } else {
    tdAddColToKVRow(pBuilder, pSchema->colId, &(pVal->datum.p),
                    IS_VAR_DATA_TYPE(pSchema->type) ? varDataTLen(pVal->datum.p) : TYPE_BYTES[pSchema->type]);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t createValueFromFunction(STranslateContext* pCxt, SFunctionNode* pFunc, SValueNode** pVal) {
  if (DEAL_RES_ERROR == translateFunction(pCxt, pFunc)) {
    return pCxt->errCode;
  }
  return scalarCalculateConstants((SNode*)pFunc, (SNode**)pVal);
}

static int32_t translateTagVal(STranslateContext* pCxt, SNode* pNode, SValueNode** pVal) {
  if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    return createValueFromFunction(pCxt, (SFunctionNode*)pNode, pVal);
  } else if (QUERY_NODE_VALUE == nodeType(pNode)) {
    return (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pNode) ? pCxt->errCode : TSDB_CODE_SUCCESS);
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
    int32_t     code = translateTagVal(pCxt, pNode, &pVal);
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
    int32_t     code = translateTagVal(pCxt, pNode, &pVal);
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
    addCreateTbReqIntoVgroup(pCxt->pParseCxt->acctId, pVgroupHashmap, pStmt->dbName, pStmt->tableName, row,
                             pSuperTableMeta->uid, &info);
  }

  taosMemoryFreeClear(pSuperTableMeta);
  tdDestroyKVRowBuilder(&kvRowBuilder);
  return code;
}

static SArray* serializeVgroupsTablesBatch(int32_t acctId, SHashObj* pVgroupHashmap) {
  SArray* pBufArray = taosArrayInit(taosHashGetSize(pVgroupHashmap), sizeof(void*));
  if (NULL == pBufArray) {
    return NULL;
  }

  int32_t             code = TSDB_CODE_SUCCESS;
  SVgroupTablesBatch* pTbBatch = NULL;
  do {
    pTbBatch = taosHashIterate(pVgroupHashmap, pTbBatch);
    if (pTbBatch == NULL) {
      break;
    }

    serializeVgroupTablesBatch(pTbBatch, pBufArray);
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

  SArray* pBufArray = serializeVgroupsTablesBatch(pCxt->pParseCxt->acctId, pVgroupHashmap);
  taosHashCleanup(pVgroupHashmap);
  if (NULL == pBufArray) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return rewriteToVnodeModifOpStmt(pQuery, pBufArray);
}

static int32_t rewriteAlterTable(STranslateContext* pCxt, SQuery* pQuery) {
  // todo
  return TSDB_CODE_SUCCESS;
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
    case QUERY_NODE_ALTER_TABLE_STMT:
      if (TSDB_ALTER_TABLE_UPDATE_TAG_VAL == ((SAlterTableStmt*)pQuery->pRoot)->alterType) {
        code = rewriteAlterTable(pCxt, pQuery);
      }
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
        TSWAP(pQuery->pCmdMsg, pCxt->pCmdMsg, SCmdMsgInfo*);
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
  int32_t           code = initTranslateContext(pParseCxt, &cxt);
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
