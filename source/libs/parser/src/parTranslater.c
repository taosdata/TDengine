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
#include "ttime.h"

#define GET_OPTION_VAL(pVal, defaultVal) (NULL == (pVal) ? (defaultVal) : ((SValueNode*)(pVal))->datum.i)

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

#define generateDealNodeErrMsg(pCxt, code, ...)               \
  ({                                                          \
    generateSyntaxErrMsg(&pCxt->msgBuf, code, ##__VA_ARGS__); \
    pCxt->errCode = code;                                     \
    DEAL_RES_ERROR;                                           \
  })

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
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(name.dbname, pDbName);
  strcpy(name.tname, pTableName);
  return getTableMetaImpl(pCxt, &name, pMeta);
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
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(name.dbname, pDbName);
  strcpy(name.tname, pTableName);
  return getTableHashVgroupImpl(pCxt, &name, pInfo);
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

static SNodeList* getProjectList(SNode* pNode) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pNode)) {
    return ((SSelectStmt*)pNode)->pProjectionList;
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
  nodesListAppend(pExpr->pAssociationList, (SNode*)pCol);
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
  // count(*)/first(*)/last(*)
  if (0 == strcmp(pCol->colName, "*")) {
    return DEAL_RES_CONTINUE;
  }
  if ('\0' != pCol->tableAlias[0]) {
    return translateColumnWithPrefix(pCxt, pCol);
  }
  bool found = false;
  if (SQL_CLAUSE_ORDER_BY == pCxt->currClause) {
    found = translateColumnUseAlias(pCxt, pCol);
  }
  return found ? DEAL_RES_CONTINUE : translateColumnWithoutPrefix(pCxt, pCol);
}

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  uint8_t precision = (NULL != pCxt->pCurrStmt ? pCxt->pCurrStmt->precision : pVal->node.resType.precision);
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
      case TSDB_DATA_TYPE_NCHAR:
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
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  } else if (nodesIsComparisonOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type || TSDB_DATA_TYPE_JSON == rdt.type ||
        TSDB_DATA_TYPE_BLOB == rdt.type) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  } else {
    // todo json operator
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateFunction(STranslateContext* pCxt, SFunctionNode* pFunc) {
  if (TSDB_CODE_SUCCESS != fmGetFuncInfo(pFunc->functionName, &pFunc->funcId, &pFunc->funcType)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_FUNTION, pFunc->functionName);
  }
  int32_t code = fmGetFuncResultType(pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    return generateDealNodeErrMsg(pCxt, code, pFunc->functionName);
  }
  if (fmIsAggFunc(pFunc->funcId) && beforeHaving(pCxt->currClause)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);
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
  if (cxt.existAggFunc && cxt.existCol) {
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

static int32_t setSysTableVgroupList(STranslateContext* pCxt, SName* pName, SRealTableNode* pRealTable) {
  if (0 != strcmp(pRealTable->table.tableName, TSDB_INS_TABLE_USER_TABLES)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SArray* vgroupList = NULL;
  if ('\0' != pRealTable->useDbName[0]) {
    code = getDBVgInfo(pCxt, pRealTable->useDbName, &vgroupList);
  } else {
    code = getDBVgInfoImpl(pCxt, pName, &vgroupList);
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
      SName name;
      code = getTableMetaImpl(
          pCxt, toName(pCxt->pParseCxt->acctId, pRealTable->table.dbName, pRealTable->table.tableName, &name),
          &(pRealTable->pMeta));
      if (TSDB_CODE_SUCCESS != code) {
        return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TABLE_NOT_EXIST, pRealTable->table.tableName);
      }
      pRealTable->table.precision = pRealTable->pMeta->tableInfo.precision;
      code = setTableVgroupList(pCxt, &name, pRealTable);
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

static bool isFirstLastFunc(SFunctionNode* pFunc) {
  return (FUNCTION_TYPE_FIRST == pFunc->funcType || FUNCTION_TYPE_LAST == pFunc->funcType);
}

static bool isFirstLastStar(SNode* pNode) {
  if (QUERY_NODE_FUNCTION != nodeType(pNode) || !isFirstLastFunc((SFunctionNode*)pNode)) {
    return false;
  }
  SNodeList* pParameterList = ((SFunctionNode*)pNode)->pParameterList;
  if (LIST_LENGTH(pParameterList) != 1) {
    return false;
  }
  SNode* pParam = nodesListGetNode(pParameterList, 0);
  return (QUERY_NODE_COLUMN == nodeType(pParam) ? 0 == strcmp(((SColumnNode*)pParam)->colName, "*") : false);
}

static SNode* createFirstLastFunc(SFunctionNode* pSrcFunc, SColumnNode* pCol) {
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pFunc) {
    return NULL;
  }
  pFunc->pParameterList = nodesMakeList();
  if (NULL == pFunc->pParameterList || TSDB_CODE_SUCCESS != nodesListAppend(pFunc->pParameterList, pCol)) {
    nodesDestroyNode(pFunc);
    return NULL;
  }

  pFunc->node.resType = pCol->node.resType;
  pFunc->funcId = pSrcFunc->funcId;
  pFunc->funcType = pSrcFunc->funcType;
  strcpy(pFunc->functionName, pSrcFunc->functionName);
  snprintf(pFunc->node.aliasName, sizeof(pFunc->node.aliasName),
           (FUNCTION_TYPE_FIRST == pSrcFunc->funcType ? "first(%s)" : "last(%s)"), pCol->colName);

  return (SNode*)pFunc;
}

static int32_t createFirstLastAllCols(STranslateContext* pCxt, SFunctionNode* pSrcFunc, SNodeList** pOutput) {
  SNodeList* pCols = NULL;
  if (TSDB_CODE_SUCCESS != createAllColumns(pCxt, &pCols)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNodeList* pFuncs = nodesMakeList();
  if (NULL == pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SNode* pCol = NULL;
  FOREACH(pCol, pCols) {
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pFuncs, createFirstLastFunc(pSrcFunc, (SColumnNode*)pCol))) {
      nodesDestroyNode(pFuncs);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  *pOutput = pFuncs;
  return TSDB_CODE_SUCCESS;
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pProjectionList) {  // select * ...
    return createAllColumns(pCxt, &pSelect->pProjectionList);
  } else {
    // todo : t.*
    SNode* pNode = NULL;
    WHERE_EACH(pNode, pSelect->pProjectionList) {
      if (isFirstLastStar(pNode)) {
        SNodeList* pFuncs = NULL;
        if (TSDB_CODE_SUCCESS != createFirstLastAllCols(pCxt, (SFunctionNode*)pNode, &pFuncs)) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        INSERT_LIST(pSelect->pProjectionList, pFuncs);
        ERASE_NODE(pSelect->pProjectionList);
        continue;
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

static int32_t translateIntervalWindow(STranslateContext* pCxt, SIntervalWindowNode* pInterval) {
  SValueNode* pIntervalVal = (SValueNode*)pInterval->pInterval;
  SValueNode* pIntervalOffset = (SValueNode*)pInterval->pOffset;
  SValueNode* pSliding = (SValueNode*)pInterval->pSliding;
  if (pIntervalVal->datum.i <= 0) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INTERVAL_VALUE_TOO_SMALL, pIntervalVal->literal);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t doTranslateWindow(STranslateContext* pCxt, SNode* pWindow) {
  switch (nodeType(pWindow)) {
    case QUERY_NODE_INTERVAL_WINDOW:
      return translateIntervalWindow(pCxt, (SIntervalWindowNode*)pWindow);
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
    code = doTranslateWindow(pCxt, pWindow);
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
  return code;
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
  pReq->minRows = GET_OPTION_VAL(pStmt->pOptions->pMinRowsPerBlock, TSDB_DEFAULT_MIN_ROW_FBLOCK);
  pReq->maxRows = GET_OPTION_VAL(pStmt->pOptions->pMaxRowsPerBlock, TSDB_DEFAULT_MAX_ROW_FBLOCK);
  pReq->commitTime = -1;
  pReq->fsyncPeriod = GET_OPTION_VAL(pStmt->pOptions->pFsyncPeriod, TSDB_DEFAULT_FSYNC_PERIOD);
  pReq->walLevel = GET_OPTION_VAL(pStmt->pOptions->pWalLevel, TSDB_DEFAULT_WAL_LEVEL);
  pReq->precision = GET_OPTION_VAL(pStmt->pOptions->pPrecision, TSDB_TIME_PRECISION_MILLI);
  pReq->compression = GET_OPTION_VAL(pStmt->pOptions->pCompressionLevel, TSDB_DEFAULT_COMP_LEVEL);
  pReq->replications = GET_OPTION_VAL(pStmt->pOptions->pReplica, TSDB_DEFAULT_DB_REPLICA_OPTION);
  pReq->quorum = GET_OPTION_VAL(pStmt->pOptions->pQuorum, TSDB_DEFAULT_DB_QUORUM_OPTION);
  pReq->update = -1;
  pReq->cacheLastRow = GET_OPTION_VAL(pStmt->pOptions->pCachelast, TSDB_DEFAULT_CACHE_LAST_ROW);
  pReq->ignoreExist = pStmt->ignoreExists;
  pReq->streamMode = GET_OPTION_VAL(pStmt->pOptions->pStreamMode, TSDB_DEFAULT_DB_STREAM_MODE_OPTION);
  pReq->ttl = GET_OPTION_VAL(pStmt->pOptions->pTtl, TSDB_DEFAULT_DB_TTL_OPTION);
  pReq->singleSTable = GET_OPTION_VAL(pStmt->pOptions->pSingleStable, TSDB_DEFAULT_DB_SINGLE_STABLE_OPTION);
  return buildCreateDbRetentions(pStmt->pOptions->pRetentions, pReq);
}

static int32_t checkRangeOption(STranslateContext* pCxt, const char* pName, SValueNode* pVal, int32_t minVal,
                                int32_t maxVal) {
  if (NULL != pVal) {
    if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
      return pCxt->errCode;
    }
    int64_t val = pVal->datum.i;
    if (val < minVal || val > maxVal) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_RANGE_OPTION, pName, val, minVal, maxVal);
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
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_STR_OPTION, "precision", pVal->datum.p);
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
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_ENUM_OPTION, pName, val, v1, v2);
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
    if (val < TSDB_MIN_DB_TTL_OPTION) {
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_TTL_OPTION, val, TSDB_MIN_DB_TTL_OPTION);
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
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_KEEP_NUM);
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

  int32_t daysToKeep0 = ((SValueNode*)nodesListGetNode(pKeep, 0))->datum.i;
  int32_t daysToKeep1 = ((SValueNode*)nodesListGetNode(pKeep, 1))->datum.i;
  int32_t daysToKeep2 = ((SValueNode*)nodesListGetNode(pKeep, 2))->datum.i;
  if (daysToKeep0 < TSDB_MIN_KEEP || daysToKeep1 < TSDB_MIN_KEEP || daysToKeep2 < TSDB_MIN_KEEP ||
      daysToKeep0 > TSDB_MAX_KEEP || daysToKeep1 > TSDB_MAX_KEEP || daysToKeep2 > TSDB_MAX_KEEP) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_KEEP_VALUE, daysToKeep0, daysToKeep1, daysToKeep2,
                                  TSDB_MIN_KEEP, TSDB_MAX_KEEP);
  }

  if (!((daysToKeep0 <= daysToKeep1) && (daysToKeep1 <= daysToKeep2))) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_KEEP_ORDER);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkDbRetentionsOption(STranslateContext* pCxt, SNodeList* pRetentions) {
  if (NULL == pRetentions) {
    return TSDB_CODE_SUCCESS;
  }

  if (LIST_LENGTH(pRetentions) > 3) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_RETENTIONS_OPTION);
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

static int32_t checkDatabaseOptions(STranslateContext* pCxt, SDatabaseOptions* pOptions) {
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
    code = checkRangeOption(pCxt, "maxRowsPerBlock", pOptions->pMaxRowsPerBlock, TSDB_MIN_MAX_ROW_FBLOCK,
                            TSDB_MAX_MAX_ROW_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "minRowsPerBlock", pOptions->pMinRowsPerBlock, TSDB_MIN_MIN_ROW_FBLOCK,
                            TSDB_MAX_MIN_ROW_FBLOCK);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkKeepOption(pCxt, pOptions->pKeep);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbPrecisionOption(pCxt, pOptions->pPrecision);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkRangeOption(pCxt, "quorum", pOptions->pQuorum, TSDB_MIN_DB_QUORUM_OPTION, TSDB_MAX_DB_QUORUM_OPTION);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "replications", pOptions->pReplica, TSDB_MIN_DB_REPLICA_OPTION,
                             TSDB_MAX_DB_REPLICA_OPTION);
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
    code = checkDbEnumOption(pCxt, "singleStable", pOptions->pSingleStable, TSDB_MIN_DB_SINGLE_STABLE_OPTION,
                             TSDB_MAX_DB_SINGLE_STABLE_OPTION);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbEnumOption(pCxt, "streamMode", pOptions->pStreamMode, TSDB_MIN_DB_STREAM_MODE_OPTION,
                             TSDB_MAX_DB_STREAM_MODE_OPTION);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkDbRetentionsOption(pCxt, pOptions->pRetentions);
  }
  return code;
}

static int32_t checkCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  return checkDatabaseOptions(pCxt, pStmt->pOptions);
}

static int32_t translateCreateDatabase(STranslateContext* pCxt, SCreateDatabaseStmt* pStmt) {
  SCreateDbReq createReq = {0};

  int32_t code = checkCreateDatabase(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCreateDbReq(pCxt, pStmt, &createReq);
  }

  if (TSDB_CODE_SUCCESS == code) {
    pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
    if (NULL == pCxt->pCmdMsg) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
    pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_DB;
    pCxt->pCmdMsg->msgLen = tSerializeSCreateDbReq(NULL, 0, &createReq);
    pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
    if (NULL == pCxt->pCmdMsg->pMsg) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tSerializeSCreateDbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);
  }

  return code;
}

static int32_t translateDropDatabase(STranslateContext* pCxt, SDropDatabaseStmt* pStmt) {
  SDropDbReq dropReq = {0};
  SName      name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameGetFullDbName(&name, dropReq.db);
  dropReq.ignoreNotExists = pStmt->ignoreNotExists;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_DROP_DB;
  pCxt->pCmdMsg->msgLen = tSerializeSDropDbReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSDropDbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
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
  pReq->quorum = GET_OPTION_VAL(pStmt->pOptions->pQuorum, -1);
  pReq->cacheLastRow = GET_OPTION_VAL(pStmt->pOptions->pCachelast, -1);
  pReq->replications = GET_OPTION_VAL(pStmt->pOptions->pReplica, -1);
  return;
}

static int32_t translateAlterDatabase(STranslateContext* pCxt, SAlterDatabaseStmt* pStmt) {
  int32_t code = checkDatabaseOptions(pCxt, pStmt->pOptions);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SAlterDbReq alterReq = {0};
  buildAlterDbReq(pCxt, pStmt, &alterReq);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_ALTER_DB;
  pCxt->pCmdMsg->msgLen = tSerializeSAlterDbReq(NULL, 0, &alterReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSAlterDbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &alterReq);

  return TSDB_CODE_SUCCESS;
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
    taosArrayPush(*pArray, &field);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t columnNodeToField(SNodeList* pList, SArray** pArray) {
  *pArray = taosArrayInit(LIST_LENGTH(pList), sizeof(SField));
  SNode* pNode;
  FOREACH(pNode, pList) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    SField       field = {.type = pCol->node.resType.type, .bytes = calcTypeBytes(pCol->node.resType)};
    strcpy(field.name, pCol->colName);
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
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COMMENT_OPTION, TSDB_STB_COMMENT_LEN - 1);
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
      return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_F_RANGE_OPTION, "file_factor", pVal->datum.d,
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

static int32_t checkTableRollupOption(STranslateContext* pCxt, SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 != LIST_LENGTH(pFuncs)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_ROLLUP_OPTION);
  }
  SFunctionNode* pFunc = nodesListGetNode(pFuncs, 0);
  if (TSDB_CODE_SUCCESS != fmGetFuncInfo(pFunc->functionName, &pFunc->funcId, &pFunc->funcType)) {
    return generateDealNodeErrMsg(pCxt, TSDB_CODE_PAR_INVALID_FUNTION, pFunc->functionName);
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
  return code;
}

static int32_t getAggregationMethod(SNodeList* pFuncs) {
  if (NULL == pFuncs) {
    return -1;
  }
  return ((SFunctionNode*)nodesListGetNode(pFuncs, 0))->funcId;
}

static int32_t translateCreateSuperTable(STranslateContext* pCxt, SCreateTableStmt* pStmt) {
  int32_t code = checkCreateTable(pCxt, pStmt);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SMCreateStbReq createReq = {0};
  createReq.igExists = pStmt->ignoreExists;
  createReq.aggregationMethod = getAggregationMethod(pStmt->pOptions->pFuncs);
  createReq.xFilesFactor = GET_OPTION_VAL(pStmt->pOptions->pFilesFactor, TSDB_DEFAULT_DB_FILE_FACTOR);
  createReq.delay = GET_OPTION_VAL(pStmt->pOptions->pDelay, TSDB_DEFAULT_DB_DELAY);
  columnDefNodeToField(pStmt->pCols, &createReq.pColumns);
  columnDefNodeToField(pStmt->pTags, &createReq.pTags);
  createReq.numOfColumns = LIST_LENGTH(pStmt->pCols);
  createReq.numOfTags = LIST_LENGTH(pStmt->pTags);
  if (NULL == pStmt->pOptions->pSma) {
    columnDefNodeToField(pStmt->pCols, &createReq.pSmas);
    createReq.numOfSmas = createReq.numOfColumns;
  } else {
    columnNodeToField(pStmt->pOptions->pSma, &createReq.pSmas);
    createReq.numOfSmas = LIST_LENGTH(pStmt->pOptions->pSma);
  }

  SName tableName = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(tableName.dbname, pStmt->dbName);
  strcpy(tableName.tname, pStmt->tableName);
  tNameExtractFullName(&tableName, createReq.name);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    tFreeSMCreateStbReq(&createReq);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_STB;
  pCxt->pCmdMsg->msgLen = tSerializeSMCreateStbReq(NULL, 0, &createReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    tFreeSMCreateStbReq(&createReq);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMCreateStbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);

  tFreeSMCreateStbReq(&createReq);
  return TSDB_CODE_SUCCESS;
}

static int32_t doTranslateDropSuperTable(STranslateContext* pCxt, const SName* pTableName, bool ignoreNotExists) {
  SMDropStbReq dropReq = {0};
  tNameExtractFullName(pTableName, dropReq.name);
  dropReq.igNotExists = ignoreNotExists;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_DROP_STB;
  pCxt->pCmdMsg->msgLen = tSerializeSMDropStbReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMDropStbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
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
  SName tableName = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(tableName.dbname, pStmt->dbName);
  strcpy(tableName.tname, pStmt->tableName);
  return doTranslateDropSuperTable(pCxt, &tableName, pStmt->ignoreNotExists);
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
  SName        tableName = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(tableName.dbname, pStmt->dbName);
  strcpy(tableName.tname, pStmt->tableName);
  tNameExtractFullName(&tableName, alterReq.name);
  alterReq.alterType = pStmt->alterType;
  alterReq.numOfFields = 1;
  if (TSDB_ALTER_TABLE_UPDATE_OPTIONS == pStmt->alterType) {
    // todo
  } else {
    if (TSDB_CODE_SUCCESS != setAlterTableField(pStmt, &alterReq)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_ALTER_STB;
  pCxt->pCmdMsg->msgLen = tSerializeSMAlterStbReq(NULL, 0, &alterReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMAlterStbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &alterReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateUseDatabase(STranslateContext* pCxt, SUseDatabaseStmt* pStmt) {
  SUseDbReq usedbReq = {0};
  SName     name = {0};
  tNameSetDbName(&name, pCxt->pParseCxt->acctId, pStmt->dbName, strlen(pStmt->dbName));
  tNameExtractFullName(&name, usedbReq.db);
  int32_t code = getDBVgVersion(pCxt, usedbReq.db, &usedbReq.vgVersion, &usedbReq.dbId, &usedbReq.numOfTable);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_USE_DB;
  pCxt->pCmdMsg->msgLen = tSerializeSUseDbReq(NULL, 0, &usedbReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSUseDbReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &usedbReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateUser(STranslateContext* pCxt, SCreateUserStmt* pStmt) {
  SCreateUserReq createReq = {0};
  strcpy(createReq.user, pStmt->useName);
  createReq.createType = 0;
  createReq.superUser = 0;
  strcpy(createReq.pass, pStmt->password);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_USER;
  pCxt->pCmdMsg->msgLen = tSerializeSCreateUserReq(NULL, 0, &createReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSCreateUserReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);

  return TSDB_CODE_SUCCESS;
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

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_ALTER_USER;
  pCxt->pCmdMsg->msgLen = tSerializeSAlterUserReq(NULL, 0, &alterReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSAlterUserReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &alterReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateDropUser(STranslateContext* pCxt, SDropUserStmt* pStmt) {
  SDropUserReq dropReq = {0};
  strcpy(dropReq.user, pStmt->useName);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_DROP_USER;
  pCxt->pCmdMsg->msgLen = tSerializeSDropUserReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSDropUserReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateDnode(STranslateContext* pCxt, SCreateDnodeStmt* pStmt) {
  SCreateDnodeReq createReq = {0};
  strcpy(createReq.fqdn, pStmt->fqdn);
  createReq.port = pStmt->port;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_DNODE;
  pCxt->pCmdMsg->msgLen = tSerializeSCreateDnodeReq(NULL, 0, &createReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSCreateDnodeReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateDropDnode(STranslateContext* pCxt, SDropDnodeStmt* pStmt) {
  SDropDnodeReq dropReq = {0};
  dropReq.dnodeId = pStmt->dnodeId;
  strcpy(dropReq.fqdn, pStmt->fqdn);
  dropReq.port = pStmt->port;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_DROP_DNODE;
  pCxt->pCmdMsg->msgLen = tSerializeSDropDnodeReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSDropDnodeReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateAlterDnode(STranslateContext* pCxt, SAlterDnodeStmt* pStmt) {
  SMCfgDnodeReq cfgReq = {0};
  cfgReq.dnodeId = pStmt->dnodeId;
  strcpy(cfgReq.config, pStmt->config);
  strcpy(cfgReq.value, pStmt->value);

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CONFIG_DNODE;
  pCxt->pCmdMsg->msgLen = tSerializeSMCfgDnodeReq(NULL, 0, &cfgReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMCfgDnodeReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &cfgReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t nodeTypeToShowType(ENodeType nt) {
  switch (nt) {
    case QUERY_NODE_SHOW_APPS_STMT:
      return 0;  // todo
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
      return TSDB_MGMT_TABLE_CONNS;
    case QUERY_NODE_SHOW_LICENCE_STMT:
      return TSDB_MGMT_TABLE_GRANTS;
    case QUERY_NODE_SHOW_QUERIES_STMT:
      return TSDB_MGMT_TABLE_QUERIES;
    case QUERY_NODE_SHOW_SCORES_STMT:
      return 0;  // todo
    case QUERY_NODE_SHOW_TOPICS_STMT:
      return 0;  // todo
    case QUERY_NODE_SHOW_VARIABLE_STMT:
      return TSDB_MGMT_TABLE_VARIABLES;
    default:
      break;
  }
  return 0;
}

static int32_t translateShow(STranslateContext* pCxt, SShowStmt* pStmt) {
  SShowReq showReq = {.type = nodeTypeToShowType(nodeType(pStmt))};

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_SHOW;
  pCxt->pCmdMsg->msgLen = tSerializeSShowReq(NULL, 0, &showReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSShowReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &showReq);

  return TSDB_CODE_SUCCESS;
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

static int32_t getSmaIndexBuildAst(STranslateContext* pCxt, SCreateIndexStmt* pStmt, char** pAst, int32_t* pLen) {
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
  strcpy(pTable->table.dbName, pCxt->pParseCxt->db);
  strcpy(pTable->table.tableName, pStmt->tableName);
  pSelect->pFromTable = (SNode*)pTable;

  pSelect->pProjectionList = nodesCloneList(pStmt->pOptions->pFuncs);
  SFunctionNode* pFunc = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == pSelect->pProjectionList || NULL == pFunc) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(pFunc->functionName, "_wstartts");
  nodesListPushFront(pSelect->pProjectionList, pFunc);
  SNode* pProject = NULL;
  FOREACH(pProject, pSelect->pProjectionList) { sprintf(((SExprNode*)pProject)->aliasName, "#sma_%p", pProject); }

  SIntervalWindowNode* pInterval = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  if (NULL == pInterval) {
    nodesDestroyNode(pSelect);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSelect->pWindow = (SNode*)pInterval;
  pInterval->pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  pInterval->pInterval = nodesCloneNode(pStmt->pOptions->pInterval);
  pInterval->pOffset = nodesCloneNode(pStmt->pOptions->pOffset);
  pInterval->pSliding = nodesCloneNode(pStmt->pOptions->pSliding);
  if (NULL == pInterval->pCol || NULL == pInterval->pInterval ||
      (NULL != pStmt->pOptions->pOffset && NULL == pInterval->pOffset) ||
      (NULL != pStmt->pOptions->pSliding && NULL == pInterval->pSliding)) {
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

static int32_t buildCreateSmaReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateSmaReq* pReq) {
  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(name.dbname, pCxt->pParseCxt->db);
  strcpy(name.tname, pStmt->indexName);
  tNameExtractFullName(&name, pReq->name);
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
    code = getSmaIndexBuildAst(pCxt, pStmt, &pReq->ast, &pReq->astLen);
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

  int32_t code = buildCreateSmaReq(pCxt, pStmt, &createSmaReq);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_SMA;
  pCxt->pCmdMsg->msgLen = tSerializeSMCreateSmaReq(NULL, 0, &createSmaReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMCreateSmaReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createSmaReq);
  tFreeSMCreateSmaReq(&createSmaReq);

  return TSDB_CODE_SUCCESS;
}
static int32_t buildCreateFullTextReq(STranslateContext* pCxt, SCreateIndexStmt* pStmt, SMCreateFullTextReq* pReq) {
  // impl later
  return 0;
}
static int32_t translateCreateFullTextIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pInterval) ||
      (NULL != pStmt->pOptions->pOffset &&
       DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pOffset)) ||
      (NULL != pStmt->pOptions->pSliding &&
       DEAL_RES_ERROR == translateValue(pCxt, (SValueNode*)pStmt->pOptions->pSliding))) {
    return pCxt->errCode;
  }
  SMCreateFullTextReq createFTReq = {0};

  int32_t code = buildCreateFullTextReq(pCxt, pStmt, &createFTReq);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_INDEX;
  pCxt->pCmdMsg->msgLen = tSerializeSMCreateFullTextReq(NULL, 0, &createFTReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMCreateFullTextReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createFTReq);
  tFreeSMCreateFullTextReq(&createFTReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateIndex(STranslateContext* pCxt, SCreateIndexStmt* pStmt) {
  if (INDEX_TYPE_SMA == pStmt->indexType) {
    return translateCreateSmaIndex(pCxt, pStmt);
  } else if (INDEX_TYPE_FULLTEXT == pStmt->indexType) {
    return translateCreateFullTextIndex(pCxt, pStmt);
    // todo fulltext index
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

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = getCreateComponentNodeMsgType(nodeType(pStmt));
  pCxt->pCmdMsg->msgLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &createReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSCreateDropMQSBNodeReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);

  return TSDB_CODE_SUCCESS;
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

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = getDropComponentNodeMsgType(nodeType(pStmt));
  pCxt->pCmdMsg->msgLen = tSerializeSCreateDropMQSBNodeReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSCreateDropMQSBNodeReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateCreateTopic(STranslateContext* pCxt, SCreateTopicStmt* pStmt) {
  SCMCreateTopicReq createReq = {0};

  if (NULL != pStmt->pQuery) {
    pCxt->pParseCxt->topicQuery = true;
    int32_t code = translateQuery(pCxt, pStmt->pQuery);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesNodeToString(pStmt->pQuery, false, &createReq.ast, NULL);
    }
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  } else {
    strcpy(createReq.subscribeDbName, pStmt->subscribeDbName);
  }

  createReq.sql = strdup(pCxt->pParseCxt->pSql);
  if (NULL == createReq.sql) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(name.dbname, pCxt->pParseCxt->db);
  strcpy(name.tname, pStmt->topicName);
  tNameExtractFullName(&name, createReq.name);
  createReq.igExists = pStmt->ignoreExists;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_CREATE_TOPIC;
  pCxt->pCmdMsg->msgLen = tSerializeSCMCreateTopicReq(NULL, 0, &createReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSCMCreateTopicReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &createReq);
  tFreeSCMCreateTopicReq(&createReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t translateDropTopic(STranslateContext* pCxt, SDropTopicStmt* pStmt) {
  SMDropTopicReq dropReq = {0};

  SName name = {.type = TSDB_TABLE_NAME_T, .acctId = pCxt->pParseCxt->acctId};
  strcpy(name.dbname, pCxt->pParseCxt->db);
  strcpy(name.tname, pStmt->topicName);
  tNameExtractFullName(&name, dropReq.name);
  dropReq.igNotExists = pStmt->ignoreNotExists;

  pCxt->pCmdMsg = taosMemoryMalloc(sizeof(SCmdMsgInfo));
  if (NULL == pCxt->pCmdMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCxt->pCmdMsg->epSet = pCxt->pParseCxt->mgmtEpSet;
  pCxt->pCmdMsg->msgType = TDMT_MND_DROP_TOPIC;
  pCxt->pCmdMsg->msgLen = tSerializeSMDropTopicReq(NULL, 0, &dropReq);
  pCxt->pCmdMsg->pMsg = taosMemoryMalloc(pCxt->pCmdMsg->msgLen);
  if (NULL == pCxt->pCmdMsg->pMsg) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSMDropTopicReq(pCxt->pCmdMsg->pMsg, pCxt->pCmdMsg->msgLen, &dropReq);

  return TSDB_CODE_SUCCESS;
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

static int32_t translateQuery(STranslateContext* pCxt, SNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT:
      code = translateSelect(pCxt, (SSelectStmt*)pNode);
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
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_LICENCE_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_VARIABLE_STMT:
      code = translateShow(pCxt, (SShowStmt*)pNode);
      break;
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      // todo
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

static int32_t extractSelectResultSchema(const SSelectStmt* pSelect, int32_t* numOfCols, SSchema** pSchema) {
  *numOfCols = LIST_LENGTH(pSelect->pProjectionList);
  *pSchema = taosMemoryCalloc((*numOfCols), sizeof(SSchema));
  if (NULL == (*pSchema)) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode*  pNode;
  int32_t index = 0;
  FOREACH(pNode, pSelect->pProjectionList) {
    SExprNode* pExpr = (SExprNode*)pNode;
    (*pSchema)[index].type = pExpr->resType.type;
    (*pSchema)[index].bytes = pExpr->resType.bytes;
    (*pSchema)[index].colId = index + 1;
    strcpy((*pSchema)[index].name, pExpr->aliasName);
    index += 1;
  }

  return TSDB_CODE_SUCCESS;
}

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
  strcpy((*pSchema)[0].name, "Field");

  (*pSchema)[1].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[1].bytes = DESCRIBE_RESULT_TYPE_LEN;
  strcpy((*pSchema)[1].name, "Type");

  (*pSchema)[2].type = TSDB_DATA_TYPE_INT;
  (*pSchema)[2].bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  strcpy((*pSchema)[2].name, "Length");

  (*pSchema)[3].type = TSDB_DATA_TYPE_BINARY;
  (*pSchema)[3].bytes = DESCRIBE_RESULT_NOTE_LEN;
  strcpy((*pSchema)[3].name, "Note");

  return TSDB_CODE_SUCCESS;
}

int32_t extractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  if (NULL == pRoot) {
    return TSDB_CODE_SUCCESS;
  }

  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
      return extractSelectResultSchema((SSelectStmt*)pRoot, numOfCols, pSchema);
    case QUERY_NODE_EXPLAIN_STMT:
      return extractExplainResultSchema(numOfCols, pSchema);
    case QUERY_NODE_DESCRIBE_STMT:
      return extractDescribeResultSchema(numOfCols, pSchema);
    default:
      break;
  }

  return TSDB_CODE_FAILED;
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
  strcpy(pTable->table.dbName, TSDB_INFORMATION_SCHEMA_DB);
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
    strcpy(((SRealTableNode*)pSelect->pFromTable)->useDbName, ((SValueNode*)pShow->pDbName)->literal);
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

static void toSchemaEx(const SColumnDefNode* pCol, col_id_t colId, SSchemaEx* pSchema) {
  pSchema->colId = colId;
  pSchema->type = pCol->dataType.type;
  pSchema->bytes = calcTypeBytes(pCol->dataType);
  pSchema->sma = pCol->sma ? TSDB_BSMA_TYPE_LATEST : TSDB_BSMA_TYPE_NONE;
  strcpy(pSchema->name, pCol->colName);
}

static void destroyCreateTbReq(SVCreateTbReq* pReq) {
  taosMemoryFreeClear(pReq->dbFName);
  taosMemoryFreeClear(pReq->name);
  taosMemoryFreeClear(pReq->ntbCfg.pSchema);
}

static int32_t buildSmaParam(STableOptions* pOptions, SVCreateTbReq* pReq) {
  if (0 == LIST_LENGTH(pOptions->pFuncs)) {
    return TSDB_CODE_SUCCESS;
  }

  pReq->ntbCfg.pRSmaParam = taosMemoryCalloc(1, sizeof(SRSmaParam));
  if (NULL == pReq->ntbCfg.pRSmaParam) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pReq->ntbCfg.pRSmaParam->delay = GET_OPTION_VAL(pOptions->pDelay, TSDB_DEFAULT_DB_DELAY);
  pReq->ntbCfg.pRSmaParam->xFilesFactor = GET_OPTION_VAL(pOptions->pFilesFactor, TSDB_DEFAULT_DB_FILE_FACTOR);
  pReq->ntbCfg.pRSmaParam->nFuncIds = LIST_LENGTH(pOptions->pFuncs);
  pReq->ntbCfg.pRSmaParam->pFuncIds = taosMemoryCalloc(pReq->ntbCfg.pRSmaParam->nFuncIds, sizeof(func_id_t));
  if (NULL == pReq->ntbCfg.pRSmaParam->pFuncIds) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t index = 0;
  SNode*  pFunc = NULL;
  FOREACH(pFunc, pOptions->pFuncs) { pReq->ntbCfg.pRSmaParam->pFuncIds[index++] = ((SFunctionNode*)pFunc)->funcId; }

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
  req.dbFName = strdup(dbFName);
  req.name = strdup(pStmt->tableName);
  req.ntbCfg.nCols = LIST_LENGTH(pStmt->pCols);
  req.ntbCfg.pSchema = taosMemoryCalloc(req.ntbCfg.nCols, sizeof(SSchemaEx));
  if (NULL == req.name || NULL == req.ntbCfg.pSchema) {
    destroyCreateTbReq(&req);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SNode*   pCol;
  col_id_t index = 0;
  FOREACH(pCol, pStmt->pCols) {
    toSchemaEx((SColumnDefNode*)pCol, index + 1, req.ntbCfg.pSchema + index);
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
  int   tlen = sizeof(SMsgHead) + tSerializeSVCreateTbBatchReq(NULL, &(pTbBatch->req));
  void* buf = taosMemoryMalloc(tlen);
  if (NULL == buf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  ((SMsgHead*)buf)->vgId = htonl(pTbBatch->info.vgId);
  ((SMsgHead*)buf)->contLen = htonl(tlen);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tSerializeSVCreateTbBatchReq(&pBuf, &(pTbBatch->req));

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
    taosMemoryFreeClear(pTableReq->dbFName);
    taosMemoryFreeClear(pTableReq->name);

    if (pTableReq->type == TSDB_NORMAL_TABLE) {
      taosMemoryFreeClear(pTableReq->ntbCfg.pSchema);
    } else if (pTableReq->type == TSDB_CHILD_TABLE) {
      taosMemoryFreeClear(pTableReq->ctbCfg.pTag);
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
  req.dbFName = strdup(dbFName);
  req.name = strdup(pTableName);
  req.ctbCfg.suid = suid;
  req.ctbCfg.pTag = row;

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
  if (DEAL_RES_ERROR == translateValue(pCxt, pVal)) {
    return pCxt->errCode;
  }
  SVariant var;
  valueNodeToVariant(pVal, &var);
  char    tagVal[TSDB_MAX_TAGS_LEN] = {0};
  int32_t code = taosVariantDump(&var, tagVal, pSchema->type, true);
  if (TSDB_CODE_SUCCESS == code) {
    tdAddColToKVRow(pBuilder, pSchema->colId, pSchema->type, tagVal);
  }
  return code;
}

static int32_t buildKVRowForBindTags(STranslateContext* pCxt, SCreateSubTableClause* pStmt, STableMeta* pSuperTableMeta,
                                     SKVRowBuilder* pBuilder) {
  int32_t numOfTags = getNumOfTags(pSuperTableMeta);
  if (LIST_LENGTH(pStmt->pValsOfTags) != LIST_LENGTH(pStmt->pSpecificTags) ||
      numOfTags < LIST_LENGTH(pStmt->pValsOfTags)) {
    return generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_TAGS_NOT_MATCHED);
  }

  SSchema* pTagSchema = getTableTagSchema(pSuperTableMeta);
  SNode *  pTag, *pVal;
  FORBOTH(pTag, pStmt->pSpecificTags, pVal, pStmt->pValsOfTags) {
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
    int32_t code = addValToKVRow(pCxt, (SValueNode*)pVal, pSchema, pBuilder);
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
  SNode*   pVal;
  int32_t  index = 0;
  FOREACH(pVal, pStmt->pValsOfTags) {
    int32_t code = addValToKVRow(pCxt, (SValueNode*)pVal, pTagSchema + index++, pBuilder);
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
  STranslateContext cxt = {
      .pParseCxt = pParseCxt,
      .errCode = TSDB_CODE_SUCCESS,
      .msgBuf = {.buf = pParseCxt->pMsg, .len = pParseCxt->msgLen},
      .pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES),
      .currLevel = 0,
      .currClause = 0,
      .pDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK),
      .pTables = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pNsLevel) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = fmFuncMgtInit();
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
