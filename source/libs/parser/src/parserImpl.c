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
#include "parserInt.h"
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
    // case TK_ON:
    //   return NEW_TK_ON;
    // case TK_INNER:
    //   return NEW_TK_INNER;
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

int32_t doParse(SParseContext* pParseCxt, SQuery* pQuery) {
  SAstCreateContext cxt;
  createAstCreateContext(pParseCxt, &cxt);
  void *pParser = NewParseAlloc(malloc);
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    // printf("===========================\n");
    if (cxt.pQueryCxt->pSql[i] == 0) {
      NewParse(pParser, 0, t0, &cxt);
      goto abort_parse;
    }
    // printf("input: [%s]\n", cxt.pQueryCxt->pSql + i);
    t0.n = getToken((char *)&cxt.pQueryCxt->pSql[i], &t0.type);
    t0.z = (char *)(cxt.pQueryCxt->pSql + i);
    // printf("token : %d %d [%s]\n", t0.type, t0.n, t0.z);
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
  // printf("doParse completed.\n");
  NewParseFree(pParser, free);
  destroyAstCreateContext(&cxt);
  pQuery->pRoot = cxt.pRootNode;
  return cxt.valid ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}

typedef enum ESqlClause {
  SQL_CLAUSE_FROM = 1,
  SQL_CLAUSE_WHERE
} ESqlClause;

typedef struct STranslateContext {
  SParseContext* pParseCxt;
  int32_t errCode;
  SMsgBuf msgBuf;
  SArray* pNsLevel; // element is SArray*, the element of this subarray is STableNode*
  int32_t currLevel;
  ESqlClause currClause;
} STranslateContext;

static int32_t translateSubquery(STranslateContext* pCxt, SNode* pNode);

static char* getSyntaxErrFormat(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_PARSER_INVALID_COLUMN:
      return "Invalid column name : %s";
    case TSDB_CODE_PARSER_TABLE_NOT_EXIST:
      return "Table does not exist : %s";
    case TSDB_CODE_PARSER_AMBIGUOUS_COLUMN:
      return "Column ambiguously defined : %s";
    case TSDB_CODE_PARSER_WRONG_VALUE_TYPE:
      return "Invalid value type : %s";
    default:
      return "Unknown error";
  }
}

static int32_t generateSyntaxErrMsg(STranslateContext* pCxt, int32_t errCode, const char* additionalInfo) {
  snprintf(pCxt->msgBuf.buf, pCxt->msgBuf.len, getSyntaxErrFormat(errCode), additionalInfo);
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
    cmp = strcmp(currentDb, pTable->dbName);
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

static void setColumnInfoBySchema(const STableNode* pTable, const SSchema* pColSchema, SColumnNode* pCol) {
  strcpy(pCol->dbName, pTable->dbName);
  strcpy(pCol->tableAlias, pTable->tableAlias);
  strcpy(pCol->tableName, pTable->tableName);
  strcpy(pCol->colName, pColSchema->name);
  if ('\0' == pCol->node.aliasName[0]) {
    strcpy(pCol->node.aliasName, pColSchema->name);
  }
  pCol->colId = pColSchema->colId;
  // pCol->colType = pColSchema->type;
  pCol->node.resType.type = pColSchema->type;
  pCol->node.resType.bytes = pColSchema->bytes;
}

static void setColumnInfoByExpr(const STableNode* pTable, SExprNode* pExpr, SColumnNode* pCol) {
  pCol->pProjectRef = (SNode*)pExpr;
  pExpr->pAssociationList = nodesListAppend(pExpr->pAssociationList, (SNode*)pCol);
  strcpy(pCol->tableAlias, pTable->tableAlias);
  strcpy(pCol->colName, pExpr->aliasName);
  pCol->node.resType = pExpr->resType;
}

static int32_t createColumnNodeByTable(const STableNode* pTable, SNodeList* pList) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      setColumnInfoBySchema(pTable, pMeta->schema + i, pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode* pNode;
    FOREACH(pNode, pProjectList) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      setColumnInfoByExpr(pTable, (SExprNode*)pNode, pCol);
      nodesListAppend(pList, (SNode*)pCol);
    }
  }
}

static bool findAndSetColumn(SColumnNode* pCol, const STableNode* pTable) {
  bool found = false;
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      if (0 == strcmp(pCol->colName, pMeta->schema[i].name)) {
        setColumnInfoBySchema(pTable, pMeta->schema + i, pCol);
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

static bool translateColumnWithPrefix(STranslateContext* pCxt, SColumnNode* pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t nums = taosArrayGetSize(pTables);
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (belongTable(pCxt->pParseCxt->db, pCol, pTable)) {
      if (findAndSetColumn(pCol, pTable)) {
        break;
      }
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_INVALID_COLUMN, pCol->colName);
      return false;
    }
  }
  return true;
}

static bool translateColumnWithoutPrefix(STranslateContext* pCxt, SColumnNode* pCol) {
  SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  size_t nums = taosArrayGetSize(pTables);
  bool found = false;
  for (size_t i = 0; i < nums; ++i) {
    STableNode* pTable = taosArrayGetP(pTables, i);
    if (findAndSetColumn(pCol, pTable)) {
      if (found) {
        generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_AMBIGUOUS_COLUMN, pCol->colName);
        return false;
      }
      found = true;
    }
  }
  if (!found) {
    generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_INVALID_COLUMN, pCol->colName);
    return false;
  }
  return true;
}

static bool translateColumn(STranslateContext* pCxt, SColumnNode* pCol) {
  if ('\0' != pCol->tableAlias[0]) {
    return translateColumnWithPrefix(pCxt, pCol);
  }
  return translateColumnWithoutPrefix(pCxt, pCol);
}

// check literal format
static bool translateValue(STranslateContext* pCxt, SValueNode* pVal) {
  return true;
}

static bool translateOperator(STranslateContext* pCxt, SOperatorNode* pOp) {
  SDataType ldt = ((SExprNode*)(pOp->pLeft))->resType;
  SDataType rdt = ((SExprNode*)(pOp->pRight))->resType;
  if (nodesIsArithmeticOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type ||
        TSDB_DATA_TYPE_JSON == rdt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
      return false;
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
    return true;
  } else if (nodesIsComparisonOp(pOp)) {
    if (TSDB_DATA_TYPE_JSON == ldt.type || TSDB_DATA_TYPE_BLOB == ldt.type ||
        TSDB_DATA_TYPE_JSON == rdt.type || TSDB_DATA_TYPE_BLOB == rdt.type) {
      generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_WRONG_VALUE_TYPE, ((SExprNode*)(pOp->pRight))->aliasName);
      return false;
    }
    pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    return true;
  } else {
    // todo json operator
    return true;
  }
  return true;
}

static bool translateFunction(STranslateContext* pCxt, SFunctionNode* pFunc) {
  return true;
}

static bool doTranslateExpr(SNode* pNode, void* pContext) {
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
    case QUERY_NODE_TEMP_TABLE:
      return translateSubquery(pCxt, ((STempTableNode*)pNode)->pSubquery);
    default:
      break;
  }
  return true;
}

static int32_t translateExpr(STranslateContext* pCxt, SNode* pNode) {
  nodesWalkNodePostOrder(pNode, doTranslateExpr, pCxt);
  return pCxt->errCode;
}

static int32_t translateExprList(STranslateContext* pCxt, SNodeList* pList) {
  nodesWalkListPostOrder(pList, doTranslateExpr, pCxt);
  return pCxt->errCode;
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
        return generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_TABLE_NOT_EXIST, pRealTable->table.tableName);
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

static int32_t translateFrom(STranslateContext* pCxt, SNode* pTable) {
  pCxt->currClause = SQL_CLAUSE_FROM;
  return translateTable(pCxt, pTable);
}

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect, bool* pIsSelectStar) {
  if (NULL == pSelect->pProjectionList) { // select * ...
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    size_t nums = taosArrayGetSize(pTables);
    pSelect->pProjectionList = nodesMakeList();
    for (size_t i = 0; i < nums; ++i) {
      STableNode* pTable = taosArrayGetP(pTables, i);
      createColumnNodeByTable(pTable, pSelect->pProjectionList);
    }
    *pIsSelectStar = true;
  } else {

  }
  return TSDB_CODE_SUCCESS;
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = translateFrom(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, pSelect->pWhere);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExprList(pCxt, pSelect->pGroupByList);
  }
  bool isSelectStar = false;
  if (TSDB_CODE_SUCCESS == code) {
    code = translateStar(pCxt, pSelect, &isSelectStar);
  }
  if (TSDB_CODE_SUCCESS == code && !isSelectStar) {
    code = translateExprList(pCxt, pSelect->pProjectionList);
  }
  // printf("%s:%d code = %d\n", __FUNCTION__, __LINE__, code);
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
  int32_t code = translateQuery(pCxt, pNode);
  --(pCxt->currLevel);
  pCxt->currClause = currClause;
  return code;
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
  return translateQuery(&cxt, pQuery->pRoot);
}
