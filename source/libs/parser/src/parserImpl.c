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

static uint32_t toNewTokenId(uint32_t tokenId) {
  switch (tokenId) {
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
    case TK_SHOW:
      return NEW_TK_SHOW;
    case TK_DATABASES:
      return NEW_TK_DATABASES;
    case TK_ID:
      return NEW_TK_NK_ID;
    case TK_LP:
      return NEW_TK_NK_LP;
    case TK_RP:
      return NEW_TK_NK_RP;
    case TK_COMMA:
      return NEW_TK_NK_COMMA;
    case TK_DOT:
      return NEW_TK_NK_DOT;
    case TK_SELECT:
      return NEW_TK_SELECT;
    case TK_DISTINCT:
      return NEW_TK_DISTINCT;
    case TK_AS:
      return NEW_TK_AS;
    case TK_FROM:
      return NEW_TK_FROM;
    case TK_ORDER:
      return NEW_TK_ORDER;
    case TK_BY:
      return NEW_TK_BY;
    case TK_ASC:
      return NEW_TK_ASC;
    case TK_DESC:
      return NEW_TK_DESC;
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

// typedef struct SNamespace {
//   int16_t level; // todo for correlated subquery
//   char dbName[TSDB_DB_NAME_LEN];
//   char tableAlias[TSDB_TABLE_NAME_LEN];
//   SHashObj* pColHash; // key is colname, value is index of STableMeta.schema
//   STableMeta* pMeta;
// } SNamespace;

typedef struct STranslateContext {
  SParseContext* pParseCxt;
  int32_t errCode;
  SMsgBuf msgBuf;
  SArray* pNsLevel; // element is SArray*, the element of this subarray is STableNode*
  int32_t currLevel;
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
  SArray* pTables = NULL;
  if (taosArrayGetSize(pCxt->pNsLevel) > pCxt->currLevel) {
    pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
  } else {
    pTables = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  }
  taosArrayPush(pTables, &pTable);
  return TSDB_CODE_SUCCESS;
}

static SName* toName(const SRealTableNode* pRealTable, SName* pName) {
  strncpy(pName->dbname, pRealTable->table.dbName, strlen(pRealTable->table.dbName));
  strncpy(pName->dbname, pRealTable->table.tableName, strlen(pRealTable->table.tableName));
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

static int32_t createColumnNodeByTable(const STableNode* pTable, SNodeList* pList) {
  if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
    const STableMeta* pMeta = ((SRealTableNode*)pTable)->pMeta;
    int32_t nums = pMeta->tableInfo.numOfTags + pMeta->tableInfo.numOfColumns;
    for (int32_t i = 0; i < nums; ++i) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->colId = pMeta->schema[i].colId;
      pCol->colType = pMeta->schema[i].type;
      pCol->node.resType.bytes = pMeta->schema[i].bytes;
      nodesListAppend(pList, (SNode*)pCol);
    }
  } else {
    SNodeList* pProjectList = getProjectList(((STempTableNode*)pTable)->pSubquery);
    SNode* pNode;
    FOREACH(pNode, pProjectList) {
      SExprNode* pExpr = (SExprNode*)pNode;
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->pProjectRef = (SNode*)pExpr;
      pExpr->pAssociationList = nodesListAppend(pExpr->pAssociationList, (SNode*)pCol);
      pCol->node.resType = pExpr->resType;
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
        pCol->colId = pMeta->schema[i].colId;
        pCol->colType = pMeta->schema[i].type;
        pCol->node.resType.bytes = pMeta->schema[i].bytes;
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
        pCol->pProjectRef = (SNode*)pExpr;
        pExpr->pAssociationList = nodesListAppend(pExpr->pAssociationList, (SNode*)pCol);
        pCol->node.resType = pExpr->resType;
        found = true;
        break;
      }
    }
  }
  return found;
}

static bool doTranslateExpr(SNode* pNode, void* pContext) {
  STranslateContext* pCxt = (STranslateContext*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN: {
      SColumnNode* pCol = (SColumnNode*)pNode;
      SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
      size_t nums = taosArrayGetSize(pTables);
      bool hasTableAlias = ('\0' != pCol->tableAlias[0]);
      bool found = false;
      for (size_t i = 0; i < nums; ++i) {
        STableNode* pTable = taosArrayGetP(pTables, i);
        if (hasTableAlias) {
          if (belongTable(pCxt->pParseCxt->db, pCol, pTable)) {
            if (findAndSetColumn(pCol, pTable)) {
              break;
            }
            generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_INVALID_COLUMN, pCol->colName);
            return false;
          }
        } else {
          if (findAndSetColumn(pCol, pTable)) {
            if (found) {
              generateSyntaxErrMsg(pCxt, TSDB_CODE_PARSER_AMBIGUOUS_COLUMN, pCol->colName);
              return false;
            }
            found = true;
          }
        }
      }
      break;
    }
    case QUERY_NODE_VALUE:
      break; // todo check literal format
    case QUERY_NODE_OPERATOR: {

      break;
    }
    case QUERY_NODE_FUNCTION:
      break; // todo
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
      if ('\0' == pRealTable->table.dbName[0]) {
        strcpy(pRealTable->table.dbName, pCxt->pParseCxt->db);
      }
      if ('\0' == pRealTable->table.tableAlias[0]) {
        strcpy(pRealTable->table.tableAlias, pRealTable->table.tableName);
      }
      SName name;
      code = catalogGetTableMeta(
          pCxt->pParseCxt->pCatalog, pCxt->pParseCxt->pTransporter, &(pCxt->pParseCxt->mgmtEpSet), toName(pRealTable, &name), &(pRealTable->pMeta));
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

static int32_t translateStar(STranslateContext* pCxt, SSelectStmt* pSelect, bool* pIsSelectStar) {
  if (NULL == pSelect->pProjectionList) { // select * ...
    SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
    size_t nums = taosArrayGetSize(pTables);
    for (size_t i = 0; i < nums; ++i) {
      STableNode* pTable = taosArrayGetP(pTables, i);
      createColumnNodeByTable(pTable, pSelect->pProjectionList);
    }
    *pIsSelectStar = true;
  } else {

  }
}

static int32_t translateSelect(STranslateContext* pCxt, SSelectStmt* pSelect) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = translateTable(pCxt, pSelect->pFromTable);
  if (TSDB_CODE_SUCCESS == code) {
    code = translateExpr(pCxt, pSelect->pWhere);
  }
  bool isSelectStar = false;
  if (TSDB_CODE_SUCCESS == code) {
    code = translateStar(pCxt, pSelect, &isSelectStar);
  }
  if (TSDB_CODE_SUCCESS == code && !isSelectStar) {
    code = translateExprList(pCxt, pSelect->pProjectionList);
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
  int32_t code = translateQuery(pCxt, pNode);
  --(pCxt->currLevel);
  return code;
}

int32_t doTranslate(SParseContext* pParseCxt, SQuery* pQuery) {
  STranslateContext cxt = { .pParseCxt = pParseCxt, .pNsLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES), .currLevel = 0 };
  return translateQuery(&cxt, pQuery->pRoot);
}
