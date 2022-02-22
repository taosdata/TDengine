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

#include "parserImpl.h"

#include "astCreateContext.h"
#include "functionMgt.h"
#include "parserInt.h"
#include "tglobal.h"
#include "ttime.h"
#include "ttoken.h"

typedef void* (*FMalloc)(size_t);
typedef void (*FFree)(void*);

extern void* NewParseAlloc(FMalloc);
extern void NewParse(void*, int, SToken, void*);
extern void NewParseFree(void*, FFree);
extern void NewParseTrace(FILE*, char*);

static uint32_t toNewTokenId(uint32_t tokenId) {
  switch (tokenId) {
    case TK_OR:
      return NEW_TK_OR;
    case TK_AND:
      return NEW_TK_AND;
    case TK_UNION:
      return NEW_TK_UNION;
    case TK_ALL:
      return NEW_TK_ALL;
    case TK_MINUS:
      return NEW_TK_NK_MINUS;
    case TK_PLUS:
      return NEW_TK_NK_PLUS;
    case TK_STAR:
      return NEW_TK_NK_STAR;
    case TK_SLASH:
      return NEW_TK_NK_SLASH;
    case TK_REM:
      return NEW_TK_NK_REM;
    case TK_SHOW:
      return NEW_TK_SHOW;
    case TK_DATABASES:
      return NEW_TK_DATABASES;
    case TK_INTEGER:
      return NEW_TK_NK_INTEGER;
    case TK_FLOAT:
      return NEW_TK_NK_FLOAT;
    case TK_STRING:
      return NEW_TK_NK_STRING;
    case TK_BOOL:
      return NEW_TK_NK_BOOL;
    case TK_TIMESTAMP:
      return NEW_TK_TIMESTAMP;
    case TK_VARIABLE:
      return NEW_TK_NK_VARIABLE;
    case TK_COMMA:
      return NEW_TK_NK_COMMA;
    case TK_ID:
      return NEW_TK_NK_ID;
    case TK_LP:
      return NEW_TK_NK_LP;
    case TK_RP:
      return NEW_TK_NK_RP;
    case TK_DOT:
      return NEW_TK_NK_DOT;
    case TK_BETWEEN:
      return NEW_TK_BETWEEN;
    case TK_NOT:
      return NEW_TK_NOT;
    case TK_IS:
      return NEW_TK_IS;
    case TK_NULL:
      return NEW_TK_NULL;
    case TK_LT:
      return NEW_TK_NK_LT;
    case TK_GT:
      return NEW_TK_NK_GT;
    case TK_LE:
      return NEW_TK_NK_LE;
    case TK_GE:
      return NEW_TK_NK_GE;
    case TK_NE:
      return NEW_TK_NK_NE;
    case TK_EQ:
      return NEW_TK_NK_EQ;
    case TK_LIKE:
      return NEW_TK_LIKE;
    case TK_MATCH:
      return NEW_TK_MATCH;
    case TK_NMATCH:
      return NEW_TK_NMATCH;
    case TK_IN:
      return NEW_TK_IN;
    case TK_SELECT:
      return NEW_TK_SELECT;
    case TK_DISTINCT:
      return NEW_TK_DISTINCT;
    case TK_WHERE:
      return NEW_TK_WHERE;
    case TK_AS:
      return NEW_TK_AS;
    case TK_FROM:
      return NEW_TK_FROM;
    case TK_JOIN:
      return NEW_TK_JOIN;
    // case TK_PARTITION:
    //   return NEW_TK_PARTITION;
    case TK_SESSION:
      return NEW_TK_SESSION;
    case TK_STATE_WINDOW:
      return NEW_TK_STATE_WINDOW;
    case TK_INTERVAL:
      return NEW_TK_INTERVAL;
    case TK_SLIDING:
      return NEW_TK_SLIDING;
    case TK_FILL:
      return NEW_TK_FILL;
    // case TK_VALUE:
    //   return NEW_TK_VALUE;
    case TK_NONE:
      return NEW_TK_NONE;
    case TK_PREV:
      return NEW_TK_PREV;
    case TK_LINEAR:
      return NEW_TK_LINEAR;
    // case TK_NEXT:
    //   return NEW_TK_NEXT;
    case TK_GROUP:
      return NEW_TK_GROUP;
    case TK_HAVING:
      return NEW_TK_HAVING;
    case TK_ORDER:
      return NEW_TK_ORDER;
    case TK_BY:
      return NEW_TK_BY;
    case TK_ASC:
      return NEW_TK_ASC;
    case TK_DESC:
      return NEW_TK_DESC;
    case TK_SLIMIT:
      return NEW_TK_SLIMIT;
    case TK_SOFFSET:
      return NEW_TK_SOFFSET;
    case TK_LIMIT:
      return NEW_TK_LIMIT;
    case TK_OFFSET:
      return NEW_TK_OFFSET;
    case TK_SPACE:
    case NEW_TK_ON:
    case NEW_TK_INNER:
      break;
    default:
      printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!tokenId = %d\n", tokenId);
  }
  return tokenId;
}

static uint32_t getToken(const char* z, uint32_t* tokenId) {
  uint32_t n = tGetToken(z, tokenId);
  *tokenId = toNewTokenId(*tokenId);
  return n;
}

static EStmtType getStmtType(const SNode* pRootNode) {
  if (NULL == pRootNode) {
    return STMT_TYPE_CMD;
  }
  switch (nodeType(pRootNode)) {
    case QUERY_NODE_SELECT_STMT:
      return STMT_TYPE_QUERY;
    default:
      break;
  }
  return STMT_TYPE_CMD;
}

int32_t doParse(SParseContext* pParseCxt, SQuery* pQuery) {
  SAstCreateContext cxt;
  createAstCreateContext(pParseCxt, &cxt);
  void *pParser = NewParseAlloc(malloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    t0.n = getToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
    t0.z = (char *)(cxt.pQueryCxt->pSql + i);
    i += t0.n;

    switch (t0.type) {
      case TK_SPACE:
      case TK_COMMENT: {
        break;
      }
      case TK_SEMI: {
        NewParse(pParser, 0, t0, &cxt);
        goto abort_parse;
      }
      case TK_QUESTION:
      case TK_ILLEGAL: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      case TK_HEX:
      case TK_OCT:
      case TK_BIN: {
        snprintf(cxt.pQueryCxt->pMsg, cxt.pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        cxt.valid = false;
        goto abort_parse;
      }
      default:
        NewParse(pParser, t0.type, t0, &cxt);
        // NewParseTrace(stdout, "");
        if (!cxt.valid) {
          goto abort_parse;
        }
    }
  }

abort_parse:
  NewParseFree(pParser, free);
  destroyAstCreateContext(&cxt);
  pQuery->stmtType = getStmtType(cxt.pRootNode);
  pQuery->pRoot = cxt.pRootNode;
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}

static bool afterGroupBy(ESqlClause clause) {
  return clause > SQL_CLAUSE_GROUP_BY;
}

static bool beforeHaving(ESqlClause clause) {
  return clause < SQL_CLAUSE_HAVING;
}

typedef struct STranslateContext {
  SParseContext* pParseCxt;
  int32_t errCode;
  SMsgBuf msgBuf;
  SArray* pNsLevel; // element is SArray*, the element of this subarray is STableNode*
  int32_t currLevel;
  ESqlClause currClause;
  SSelectStmt* pCurrStmt;
} STranslateContext;

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode);

static char* getSyntaxErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PAR_INVALID_COLUMN:
      return "Invalid column name : %s";
    case TSDB_CODE_PAR_TABLE_NOT_EXIST:
      return "Table does not exist : %s";
    case TSDB_CODE_PAR_AMBIGUOUS_COLUMN:
      return "Column ambiguously defined : %s";
    case TSDB_CODE_PAR_WRONG_VALUE_TYPE:
      return "Invalid value type : %s";
    case TSDB_CODE_PAR_INVALID_FUNTION:
      return "Invalid function name : %s";
    case TSDB_CODE_PAR_FUNTION_PARA_NUM:
      return "Invalid number of arguments : %s";
    case TSDB_CODE_PAR_FUNTION_PARA_TYPE:
      return "Inconsistent datatypes : %s";
    case TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION:
      return "There mustn't be aggregation";
    case TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT:
      return "ORDER BY item must be the number of a SELECT-list expression";
    case TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION:
      return "Not a GROUP BY expression";
    case TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION:
      return "Not SELECTed expression";
    case TSDB_CODE_PAR_NOT_SINGLE_GROUP:
      return "Not a single-group group function";
    case TSDB_CODE_OUT_OF_MEMORY:
      return "Out of memory";
    default:
      return "Unknown error";
  }
}

static int32_t generateSyntaxErrMsg(STranslateContext* pCxt, int32_t errCode, ...) {
  va_list vArgList;
  va_start(vArgList, errCode);
  vsnprintf(pCxt->msgBuf.buf, pCxt->msgBuf.len, getSyntaxErrFormat(errCode), vArgList);
  va_end(vArgList);
  pCxt->errCode = errCode;
  return errCode;
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

static SName* toName(int32_t acctId, const SRealTableNode* pRealTable, SName* pName) {
  pName->type = TSDB_TABLE_NAME_T;
  pName->acctId = acctId;
  strcpy(pName->dbname, pRealTable->table.dbName);
  strcpy(pName->tname, pRealTable->table.tableName);
  return pName;
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

static void setColumnInfoBySchema(const SRealTableNode* pTable, const SSchema* pColSchema, bool isTag, SColumnNode* pCol) {
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
}

static void setColumnInfoByExpr(const STableNode* pTable, SExprNode* pExpr, SColumnNode* pCol) {
  pCol->pProjectRef = (SNode*)pExpr;
  nodesListAppend(pExpr->pAssociationList, (SNode*)pCol);
  if (NULL != pTable) {
    strcpy(pCol->tableAlias, pTable->tableAlias);
  }
  strcpy(pCol->colName, pExpr->aliasName);
  pCol->node.resType = pExpr->resType;
}

static int32_t createColumnNodeByTable(STranslateContext* pCxt, const STableNode* pTable, SNodeList* pList) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
      }
      setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i < pMeta->tableInfo.numOfTags), pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode* pNode;
    FOREACH(pNode, pProjectList) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pCol) {
        return generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
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
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      if (0 == strcmp(pCol->colName, pMeta->schema[i].name)) {
        setColumnInfoBySchema((SRealTableNode*)pTable, pMeta->schema + i, (i < pMeta->tableInfo.numOfTags), pCol);
        found = true;
        break;
      }
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode* pNode;
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
  size_t nums = taosArrayGetSize(pTables);
  bool foundTable = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (belongTable(pCxt->pParseCxt->db, pCol, pTable)) {
      foundTable = true;
      if (findAndSetColumn(pCol, pTable)) {
        break;
      }
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
      return DEAL_RES_ERROR;
    }
  }
  if (!foundTable) {
    generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_TABLE_NOT_EXIST, pCol->tableAlias);
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes translateColumnWithoutPrefix(STranslateContext* pCxt, SColumnNode* pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t nums = taosArrayGetSize(pTables);
  bool found = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (findAndSetColumn(pCol, pTable)) {
      if (found) {
        generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_AMBIGUOUS_COLUMN, pCol->colName);
        return DEAL_RES_ERROR;
      }
      found = true;
    }
  }
  if (!found) {
    generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_INVALID_COLUMN, pCol->colName);
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static bool translateColumnUseAlias(STranslateContext* pCxt, SColumnNode* pCol) {
  SNodeList* pProjectionList = pCxt->pCurrStmt->pProjectionList;
  SNode* pNode;
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

static int32_t trimStringCopy(const char* src, int32_t len, char* dst) {
  // delete escape character: \\, \', \"
  char delim = src[0];
  int32_t cnt = 0;
  int32_t j = 0;
  for (uint32_t k = 1; k < len - 1; ++k) {
    if (src[k] == '\\' || (src[k] == delim && src[k + 1] == delim)) {
      dst[j] = src[k + 1];
      cnt++;
      j++;
      k++;
      continue;
    }
    dst[j] = src[k];
    j++;
  }
  dst[j] = '\0';
  return j;
}

static EDealRes translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  if (pVal->isDuration) {
    char unit = 0;
    if (parseAbsoluteDuration(pVal->literal, strlen(pVal->literal), &pVal->datum.i, &unit, pVal->node.resType.precision) != TSDB_CODE_SUCCESS) {
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
      return DEAL_RES_ERROR;
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
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY: {
        int32_t n = strlen(pVal->literal);
        pVal->datum.p = calloc(1, n);
        if (NULL == pVal->datum.p) {
          generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
          return DEAL_RES_ERROR;
        }
        trimStringCopy(pVal->literal, n, pVal->datum.p);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        int32_t n = strlen(pVal->literal);
        char* tmp = calloc(1, n);
        if (NULL == tmp) {
          generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
          return DEAL_RES_ERROR;
        }
        int32_t len = trimStringCopy(pVal->literal, n, tmp);
        if (taosParseTime(tmp, &pVal->datum.i, len, pVal->node.resType.precision, tsDaylight) != TSDB_CODE_SUCCESS) {
          tfree(tmp);
          generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, pVal->literal);
          return DEAL_RES_ERROR;
        }
        tfree(tmp);
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
  return DEAL_RES_CONTINUE;
}

static EDealRes translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (nodesIsArithmeticOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type ||
        TSDB_DATA_TYPE_JSON == rdt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
      return DEAL_RES_ERROR;
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  } else if (nodesIsComparisonOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type ||
        TSDB_DATA_TYPE_JSON == rdt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
      return DEAL_RES_ERROR;
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
    generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_INVALID_FUNTION, pFunc->functionName);
    return DEAL_RES_ERROR;
  }
  int32_t code = fmGetFuncResultType(pFunc);
  if (TSDB_CODE_SUCCESS != code) {
    generateSyntaxErrMsg(pCxt, code, pFunc->functionName);
    return DEAL_RES_ERROR;
  }
  if (fmIsAggFunc(pFunc->funcId) && beforeHaving(pCxt->currClause)) {
    generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION);
    return DEAL_RES_ERROR;
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
  nodesWalkNodePostOrder(pNode, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static int32_t translateExprList(STranslateContext* pCxt, SNodeList* pList) {
  nodesWalkListPostOrder(pList, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static bool isAliasColumn(SColumnNode* pCol) {
  return ('\0' == pCol->tableAlias[0]);
}

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
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId) && !isDistinctOrderBy(pCxt)) {
    return DEAL_RES_IGNORE_CHILD;
  }
  SNode* pGroupNode;
  FOREACH(pGroupNode, getGroupByList(pCxt)) {
    if (nodesEqualNode(getGroupByNode(pGroupNode), pNode)) {
      return DEAL_RES_IGNORE_CHILD;
    }
  }
  if (QUERY_NODE_COLUMN == nodeType(pNode) ||
      (QUERY_NODE_FUNCTION == nodeType(pNode) && fmIsAggFunc(((SFunctionNode*)pNode)->funcId) && isDistinctOrderBy(pCxt))) {
    generateSyntaxErrMsg(pCxt, getGroupByErrorCode(pCxt));
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t checkExprForGroupBy(STranslateContext* pCxt, SNode* pNode) {
  nodesWalkNode(pNode, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

static int32_t checkExprListForGroupBy(STranslateContext* pCxt, SNodeList* pList) {
  if (NULL == getGroupByList(pCxt)) {
    return TSDB_CODE_SUCCESS;
  }
  nodesWalkList(pList, doCheckExprForGroupBy, pCxt);
  return pCxt->errCode;
}

typedef struct CheckAggColCoexistCxt {
  STranslateContext* pTranslateCxt;
  bool existAggFunc;
  bool existCol;
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
  CheckAggColCoexistCxt cxt = { .pTranslateCxt = pCxt, .existAggFunc = false, .existCol = false };
  nodesWalkList(pSelect->pProjectionList, doCheckAggColCoexist, &cxt);
  if (!pSelect->isDistinct) {
    nodesWalkList(pSelect->pOrderByList, doCheckAggColCoexist, &cxt);
  }
  if (cxt.existAggFunc && cxt.existCol) {
    return generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_NOT_SINGLE_GROUP);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateTable(STranslateContext* pCxt, SNode* pTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pTable)) {
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pRealTable = (SRealTableNode*)pTable;
      SName name;
      code = catalogGetTableMeta(pCxt->pParseCxt->pCatalog, pCxt->pParseCxt->pTransporter, &(pCxt->pParseCxt->mgmtEpSet),
          toName(pCxt->pParseCxt->acctId, pRealTable, &name), &(pRealTable->pMeta));
      if (TSDB_CODE_SUCCESS != code) {
        return generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_TABLE_NOT_EXIST, pRealTable->table.tableName);
      }
      code = addNamespace(pCxt, pRealTable);
      break;
    }
    case QUERY_NODE_TEMP_TABLE: {
      STempTableNode* pTempTable = (STempTableNode*)pTable;
      code = translateSubquery(pCxt, pTempTable->pSubquery);
      if (TSDB_CODE_SUCCESS == code) {
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
        code = translateExpr(pCxt, pJoinTable->pOnCond);
      }
      break;
    }
    default:
      break;
  }
  return code;
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect, bool* pIsSelectStar) {
  if (NULL == pSelect->pProjectionList) { // select * ...
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    size_t nums = taosArrayGetSize(pTables);
    pSelect->pProjectionList = nodesMakeList();
    if (NULL == pSelect->pProjectionList) {
      return generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
    }
    for (size_t i = 0; i < nums; ++i) {
      STableNode* pTable = taosArrayGetP(pTables, i);
      int32_t code = createColumnNodeByTable(pCxt, pTable, pSelect->pProjectionList);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
    *pIsSelectStar = true;
  } else {

  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getPositionValue(const SValueNode* pVal) {
  switch (pVal->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_BINARY:
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

static int32_t translateOrderByPosition(STranslateContext* pCxt, SNodeList* pProjectionList, SNodeList* pOrderByList, bool* pOther) {
  *pOther = false;
  SNode* pNode;
  FOREACH(pNode, pOrderByList) {
    SNode* pExpr = ((SOrderByExprNode*)pNode)->pExpr;
    if (QUERY_NODE_VALUE == nodeType(pExpr)) {
      SValueNode* pVal = (SValueNode*)pExpr;
      if (!translateValue(pCxt, pVal)) {
        return pCxt->errCode;
      }
      int32_t pos = getPositionValue(pVal);
      if (pos < 0) {
        ERASE_NODE(pOrderByList);
        continue;
      } else if (0 == pos || pos > LIST_LENGTH(pProjectionList)) {
        return generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT);
      } else {
        SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
        if (NULL == pCol) {
          return generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
        }
        setColumnInfoByExpr(NULL, (SExprNode*)nodesListGetNode(pProjectionList, pos - 1), pCol);
        ((SOrderByExprNode*)pNode)->pExpr = (SNode*)pCol;
        nodesDestroyNode(pExpr);
      }
    } else {
      *pOther = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateOrderBy(STranslateContext* pCxt, SSelectStmt* pSelect) {
  bool other;
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
  bool isSelectStar = false;
  int32_t code = translateStar(pCxt, pSelect, &isSelectStar);
  if (TSDB_CODE_SUCCESS == code && !isSelectStar) {
    pCxt->currClause = SQL_CLAUSE_SELECT;
    code = translateExprList(pCxt, pSelect->pProjectionList);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprListForGroupBy(pCxt, pSelect->pProjectionList);
  }
  return code;
}

static int32_t translateHaving(STranslateContext* pCxt, SSelectStmt* pSelect) {
  if (NULL == pSelect->pGroupByList && NULL != pSelect->pHaving) {
    return generateSyntaxErrMsg(pCxt, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION);
  }
  pCxt->currClause = SQL_CLAUSE_HAVING;
  int32_t code = translateExpr(pCxt, pSelect->pHaving);
  if (TSDB_CODE_SUCCESS == code) {
    code = checkExprForGroupBy(pCxt, pSelect->pHaving);
  }
  return code;
}

static int32_t translateGroupBy(STranslateContext* pCxt, SNodeList* pGroupByList) {
  pCxt->currClause = SQL_CLAUSE_GROUP_BY;
  return translateExprList(pCxt, pGroupByList);
}

static int32_t translateWindow(STranslateContext* pCxt, SNode* pWindow) {
  pCxt->currClause = SQL_CLAUSE_WINDOW;
  return translateExpr(pCxt, pWindow);
}

static int32_t translatePartitionBy(STranslateContext* pCxt, SNodeList* pPartitionByList) {
  pCxt->currClause = SQL_CLAUSE_PARTITION_BY;
  return translateExprList(pCxt, pPartitionByList);
}

static int32_t translateWhere(STranslateContext* pCxt, SNode* pWhere) {
  pCxt->currClause = SQL_CLAUSE_WHERE;
  return translateExpr(pCxt, pWhere);
}

static int32_t translateFrom(STranslateContext* pCxt, SNode* pTable) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  return translateTable(pCxt, pTable);
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  pCxt->pCurrStmt = pSelect;
  int32_t code = translateFrom(pCxt, pSelect->pFromTable);
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
    code = translateGroupBy(pCxt, pSelect->pGroupByList);
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

static int32_t translateQuery(STranslateContext* pCxt, SNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pNode)) {
    case QUERY_NODE_SELECT_STMT:
      code = translateSelect(pCxt, (SSelectStmt*)pNode);
      break;
    default:
      break;
  }
  return code;
}

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode) {
  ++(pCxt->currLevel);
  ESqlClause currClause = pCxt->currClause;
  SSelectStmt* pCurrStmt = pCxt->pCurrStmt;
  int32_t code = translateQuery(pCxt, pNode);
  --(pCxt->currLevel);
  pCxt->currClause = currClause;
  pCxt->pCurrStmt = pCurrStmt;
  return code;
}

int32_t setReslutSchema(STranslateContext* pCxt, SQuery* pQuery) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pQuery->pRoot)) {
    SSelectStmt* pSelect = (SSelectStmt*)pQuery->pRoot;
    pQuery->numOfResCols = LIST_LENGTH(pSelect->pProjectionList);
    pQuery->pResSchema = calloc(pQuery->numOfResCols, sizeof(SSchema));
    if (NULL == pQuery->pResSchema) {
      return generateSyntaxErrMsg(pCxt, TSDB_CODE_OUT_OF_MEMORY);
    }
    SNode* pNode;
    int32_t index = 0;
    FOREACH(pNode, pSelect->pProjectionList) {
      SExprNode* pExpr = (SExprNode*)pNode;
      pQuery->pResSchema[index].type = pExpr->resType.type;
      pQuery->pResSchema[index].bytes = pExpr->resType.bytes;
      strcpy(pQuery->pResSchema[index].name, pExpr->aliasName);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t doTranslate(SParseContext* pParseCxt, SQuery* pQuery) {
  STranslateContext cxt = {
    .pParseCxt = pParseCxt,
    .errCode = TSDB_CODE_SUCCESS,
    .msgBuf = { .buf = pParseCxt->pMsg, .len = pParseCxt->msgLen },
    .pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES),
    .currLevel = 0,
    .currClause = 0
  };
  int32_t code = fmFuncMgtInit();
  if (TSDB_CODE_SUCCESS == code) {
    code = translateQuery(&cxt, pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code && STMT_TYPE_QUERY == pQuery->stmtType) {
    code = setReslutSchema(&cxt, pQuery);
  }
  return code;
}

int32_t parser(SParseContext* pParseCxt, SQuery* pQuery) {
  int32_t code = doParse(pParseCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = doTranslate(pParseCxt, pQuery);
  }
  return code;
}
