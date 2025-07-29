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

#include "parser.h"
#include "os.h"

#include <stdio.h>
#include <string.h>
#include "parInsertUtil.h"
#include "parInt.h"
#include "parToken.h"
#include "tname.h"

bool qIsInsertValuesSql(const char* pStr, size_t length) {
  if (NULL == pStr) {
    return false;
  }

  const char* pSql = pStr;

  int32_t index = 0;
  SToken  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (TK_INSERT != t.type && TK_IMPORT != t.type) {
    return false;
  }

  do {
    pStr += index;
    index = 0;
    t = tStrGetToken((char*)pStr, &index, false, NULL);
    if (TK_USING == t.type || TK_VALUES == t.type || TK_FILE == t.type) {
      return true;
    } else if (TK_SELECT == t.type) {
      return false;
    }
    if (0 == t.type || 0 == t.n) {
      break;
    }
  } while (pStr - pSql < length);
  return false;
}

bool qIsUpdateSetSql(const char* pStr, size_t length, SName* pTableName, int32_t acctId, const char* dbName,
                     char* msgBuf, int32_t msgBufLen) {
  if (NULL == pStr) {
    return false;
  }

  const char* pSql = pStr;

  int32_t index = 0;
  SToken  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (TK_UPDATE != t.type) {
    return false;
  }

  pStr += index;
  index = 0;
  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (t.n == 0 || t.z == NULL) {
    return false;
  }

  if (pTableName != NULL) {
    SMsgBuf pMsgBuf = {.len = msgBufLen, .buf = msgBuf};
    int32_t code = insCreateSName(pTableName, &t, acctId, dbName, &pMsgBuf);
    if (code != TSDB_CODE_SUCCESS) {
      parserError("stmt update sql tbname error: %s", pMsgBuf.buf);
      return TSDB_CODE_TSC_STMT_TBNAME_ERROR;
    }
  }

  do {
    pStr += index;
    index = 0;
    t = tStrGetToken((char*)pStr, &index, false, NULL);
    if (TK_SET == t.type) {
      return true;
    }
    if (0 == t.type || 0 == t.n) {
      break;
    }
  } while (pStr - pSql < length);
  return false;
}

static bool isColumnPrimaryKey(const STableMeta* pTableMeta, const char* colName, int32_t colNameLen) {
  if (pTableMeta == NULL || colName == NULL) {
    return false;
  }

  for (int32_t i = 0; i < pTableMeta->tableInfo.numOfColumns; i++) {
    const SSchema* pSchema = &pTableMeta->schema[i];
    if ((pSchema->flags & COL_IS_KEY || pSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) &&
        strncmp(pSchema->name, colName, colNameLen) == 0 && strlen(pSchema->name) == colNameLen) {
      return true;
    }
  }
  return false;
}

int32_t convertUpdateToInsert(const char* pSql, char** pNewSql, STableMeta* pTableMeta) {
  if (NULL == pSql || NULL == pNewSql) {
    return TSDB_CODE_INVALID_PARA;
  }

  const char* pEnd = pSql + strlen(pSql);
  size_t      maxSqlLen = strlen(pSql) * 2;
  char*  newSql = taosMemoryMalloc(maxSqlLen);
  if (newSql == NULL) {
    return terrno;
  }
  char*   p = newSql;
  int32_t index = 0;
  SToken  t;

  // UPDATE
  t = tStrGetToken((char*)pSql, &index, false, NULL);
  if (TK_UPDATE != t.type) {
    taosMemoryFree(newSql);
    return TSDB_CODE_PAR_SYNTAX_ERROR;
  }
  pSql += index;

  // tbname
  index = 0;
  t = tStrGetToken((char*)pSql, &index, false, NULL);
  if (t.n == 0 || t.z == NULL) {
    taosMemoryFree(newSql);
    return TSDB_CODE_PAR_SYNTAX_ERROR;
  }

  p += sprintf(p, "INSERT INTO ");
  memcpy(p, t.z, t.n);
  p += t.n;
  p += sprintf(p, " (");
  pSql += index;

  // SET
  index = 0;
  t = tStrGetToken((char*)pSql, &index, false, NULL);
  if (TK_SET != t.type) {
    taosMemoryFree(newSql);
    return TSDB_CODE_PAR_SYNTAX_ERROR;
  }
  pSql += index;

  bool    firstColumn = true;
  int32_t columnCount = 0;
  bool inSetClause = true;

  // col name
  while (inSetClause && pSql < pEnd) {
    index = 0;
    t = tStrGetToken((char*)pSql, &index, false, NULL);
    if (t.n == 0 || t.z == NULL) {
      break;
    }

    // pk can't set
    if (pTableMeta != NULL && isColumnPrimaryKey(pTableMeta, t.z, t.n)) {
      taosMemoryFree(newSql);
      return TSDB_CODE_PAR_SYNTAX_ERROR;
    }

    if (!firstColumn) {
      *p++ = ',';
    }
    memcpy(p, t.z, t.n);
    p += t.n;
    firstColumn = false;
    columnCount++;
    pSql += index;

    index = 0;
    t = tStrGetToken((char*)pSql, &index, false, NULL);
    if (t.type != TK_NK_EQ) {
      taosMemoryFree(newSql);
      return TSDB_CODE_PAR_SYNTAX_ERROR;
    }
    pSql += index;

    // value must be ?
    index = 0;
    t = tStrGetToken((char*)pSql, &index, false, NULL);
    if (t.n == 0 || t.z == NULL) {
      break;
    }
    if (t.type != TK_NK_QUESTION) {
      taosMemoryFree(newSql);
      return TSDB_CODE_PAR_SYNTAX_ERROR;
    }
    pSql += index;

    index = 0;
    t = tStrGetToken((char*)pSql, &index, false, NULL);
    if (t.type == TK_WHERE) {
      inSetClause = false;
      pSql += index;
    }
  }

  // where clause
  if (pSql < pEnd) {
    bool inWhereClause = true;
    while (inWhereClause && pSql < pEnd) {
      index = 0;
      t = tStrGetToken((char*)pSql, &index, false, NULL);
      if (t.n == 0 || t.z == NULL) {
        break;
      }

      const char* colName = t.z;
      int32_t     colNameLen = t.n;
      pSql += index;

      index = 0;
      t = tStrGetToken((char*)pSql, &index, false, NULL);
      if (t.n == 0 || t.z == NULL) {
        break;
      }
      pSql += index;

      index = 0;
      t = tStrGetToken((char*)pSql, &index, false, NULL);
      if (t.n == 0 || t.z == NULL) {
        break;
      }

      // where cols muset be pk, ignore others
      if (t.type == TK_NK_QUESTION && pTableMeta != NULL && isColumnPrimaryKey(pTableMeta, colName, colNameLen)) {
        if (!firstColumn) {
          *p++ = ',';
        }
        memcpy(p, colName, colNameLen);
        p += colNameLen;
        firstColumn = false;
        columnCount++;
      }

      pSql += index;

      index = 0;
      t = tStrGetToken((char*)pSql, &index, false, NULL);
      if (t.type == TK_AND || t.type == TK_OR) {
        pSql += index;
      } else {
        break;
      }
    }
  }

  p += sprintf(p, ") VALUES (");
  for (int32_t i = 0; i < columnCount; i++) {
    if (i > 0) {
      *p++ = ',';
    }
    *p++ = '?';
  }
  *p++ = ')';
  *p = '\0';

  *pNewSql = newSql;
  return TSDB_CODE_SUCCESS;
}

bool qIsCreateTbFromFileSql(const char* pStr, size_t length) {
  if (NULL == pStr) {
    return false;
  }

  const char* pSql = pStr;

  int32_t index = 0;
  SToken  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (TK_CREATE != t.type) {
    return false;
  }

  do {
    pStr += index;
    index = 0;
    t = tStrGetToken((char*)pStr, &index, false, NULL);
    if (TK_FILE == t.type) {
      return true;
    }
    if (0 == t.type || 0 == t.n) {
      break;
    }
  } while (pStr - pSql < length);
  return false;
}

bool qParseDbName(const char* pStr, size_t length, char** pDbName) {
  (void)length;
  int32_t index = 0;
  SToken  t;

  if (NULL == pStr) {
    *pDbName = NULL;
    return false;
  }

  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (TK_INSERT != t.type && TK_IMPORT != t.type) {
    *pDbName = NULL;
    return false;
  }

  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (TK_INTO != t.type) {
    *pDbName = NULL;
    return false;
  }

  t = tStrGetToken((char*)pStr, &index, false, NULL);
  if (t.n == 0 || t.z == NULL) {
    *pDbName = NULL;
    return false;
  }
  char* dotPos = strnchr(t.z, '.', t.n, true);
  if (dotPos != NULL) {
    int dbNameLen = dotPos - t.z;
    *pDbName = taosMemoryMalloc(dbNameLen + 1);
    if (*pDbName == NULL) {
      return false;
    }
    strncpy(*pDbName, t.z, dbNameLen);
    (*pDbName)[dbNameLen] = '\0';
    return true;
  }
  return false;
}

static int32_t analyseSemantic(SParseContext* pCxt, SQuery* pQuery, SParseMetaCache* pMetaCache) {
  int32_t code = authenticate(pCxt, pQuery, pMetaCache);

  if (pCxt->parseOnly) {
    return code;
  }

  if (TSDB_CODE_SUCCESS == code && pQuery->placeholderNum > 0) {
    TSWAP(pQuery->pPrepareRoot, pQuery->pRoot);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translate(pCxt, pQuery, pMetaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, pQuery);
  }
  return code;
}

static int32_t parseSqlIntoAst(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = parse(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseSemantic(pCxt, *pQuery, NULL);
  }
  return code;
}

static int32_t parseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, SParseMetaCache* pMetaCache) {
  int32_t code = parse(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = collectMetaKey(pCxt, *pQuery, pMetaCache);
  }
  return code;
}

static int32_t setValueByBindParam(SValueNode* pVal, TAOS_MULTI_BIND* pParam, void *charsetCxt) {
  if (!pParam || IS_NULL_TYPE(pParam->buffer_type)) {
    return TSDB_CODE_APP_ERROR;
  }
  if (IS_VAR_DATA_TYPE(pVal->node.resType.type)) {
    taosMemoryFreeClear(pVal->datum.p);
  }

  if (pParam->is_null && 1 == *(pParam->is_null)) {
    pVal->node.resType.type = TSDB_DATA_TYPE_NULL;
    pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return TSDB_CODE_SUCCESS;
  }

  int32_t inputSize = (NULL != pParam->length ? *(pParam->length) : tDataTypes[pParam->buffer_type].bytes);
  pVal->node.resType.type = pParam->buffer_type;
  pVal->node.resType.bytes = inputSize;

  switch (pParam->buffer_type) {
    case TSDB_DATA_TYPE_VARBINARY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      memcpy(varDataVal(pVal->datum.p), pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      strncpy(varDataVal(pVal->datum.p), (const char*)pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      pVal->node.resType.bytes *= TSDB_NCHAR_SIZE;
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4(pParam->buffer, inputSize, (TdUcs4*)varDataVal(pVal->datum.p), pVal->node.resType.bytes,
                         &output, charsetCxt)) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, output);
      pVal->node.resType.bytes = output + VARSTR_HEADER_SIZE;
      break;
    }
    default: {
      int32_t code = nodesSetValueNodeValue(pVal, pParam->buffer);
      if (code) {
        return code;
      }
      break;
    }
  }
  pVal->translate = true;
  return TSDB_CODE_SUCCESS;
}

static EDealRes rewriteQueryExprAliasImpl(SNode* pNode, void* pContext) {
  if (nodesIsExprNode(pNode) && QUERY_NODE_COLUMN != nodeType(pNode)) {
    snprintf(((SExprNode*)pNode)->aliasName, TSDB_COL_NAME_LEN, "#%d", *(int32_t*)pContext);
    ++(*(int32_t*)pContext);
  }
  return DEAL_RES_CONTINUE;
}

static void rewriteQueryExprAlias(SNode* pRoot, int32_t* pNo) {
  switch (nodeType(pRoot)) {
    case QUERY_NODE_SELECT_STMT:
      nodesWalkSelectStmt((SSelectStmt*)pRoot, SQL_CLAUSE_FROM, rewriteQueryExprAliasImpl, pNo);
      break;
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pSetOper = (SSetOperator*)pRoot;
      rewriteQueryExprAlias(pSetOper->pLeft, pNo);
      rewriteQueryExprAlias(pSetOper->pRight, pNo);
      break;
    }
    default:
      break;
  }
}

static void rewriteExprAlias(SNode* pRoot) {
  int32_t no = 1;
  rewriteQueryExprAlias(pRoot, &no);
}

int32_t qParseSql(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (qIsInsertValuesSql(pCxt->pSql, pCxt->sqlLen)) {
    code = parseInsertSql(pCxt, pQuery, NULL, NULL);
  } else {
    code = parseSqlIntoAst(pCxt, pQuery);
  }
  terrno = code;
  return code;
}

static int32_t parseQuerySyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq) {
  SParseMetaCache metaCache = {0};
  int32_t         code = parseSqlSyntax(pCxt, pQuery, &metaCache);
  if (TSDB_CODE_SUCCESS == code) {
    code = buildCatalogReq(&metaCache, pCatalogReq);
  }
  destoryParseMetaCache(&metaCache, true);
  return code;
}

static int32_t parseCreateTbFromFileSyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq) {
  if (NULL == *pQuery) return parseQuerySyntax(pCxt, pQuery, pCatalogReq);

  return continueCreateTbFromFile(pCxt, pQuery);
}

int32_t qParseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq) {
  int32_t code = nodesAcquireAllocator(pCxt->allocatorId);
  if (TSDB_CODE_SUCCESS == code) {
    if (qIsInsertValuesSql(pCxt->pSql, pCxt->sqlLen)) {
      code = parseInsertSql(pCxt, pQuery, pCatalogReq, NULL);
    } else if (qIsCreateTbFromFileSql(pCxt->pSql, pCxt->sqlLen)) {
      code = parseCreateTbFromFileSyntax(pCxt, pQuery, pCatalogReq);
    } else {
      code = parseQuerySyntax(pCxt, pQuery, pCatalogReq);
    }
  }
  (void)nodesReleaseAllocator(pCxt->allocatorId);
  terrno = code;
  return code;
}

int32_t qAnalyseSqlSemantic(SParseContext* pCxt, const struct SCatalogReq* pCatalogReq,
                            struct SMetaData* pMetaData, SQuery* pQuery) {
  SParseMetaCache metaCache = {0};
  int32_t         code = nodesAcquireAllocator(pCxt->allocatorId);
  if (TSDB_CODE_SUCCESS == code && pCatalogReq) {
    code = putMetaDataToCache(pCatalogReq, pMetaData, &metaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseSemantic(pCxt, pQuery, &metaCache);
  }
  (void)nodesReleaseAllocator(pCxt->allocatorId);
  destoryParseMetaCache(&metaCache, false);
  terrno = code;
  return code;
}

int32_t qContinueParseSql(SParseContext* pCxt, struct SCatalogReq* pCatalogReq, const struct SMetaData* pMetaData,
                          SQuery* pQuery) {
  return parseInsertSql(pCxt, &pQuery, pCatalogReq, pMetaData);
}

int32_t qContinueParsePostQuery(SParseContext* pCxt, SQuery* pQuery, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (nodeType(pQuery->pRoot)) {
    default:
      break;
  }

  return code;
}

static void destoryTablesReq(void* p) {
  STablesReq* pRes = (STablesReq*)p;
  taosArrayDestroy(pRes->pTables);
}

void destoryCatalogReq(SCatalogReq* pCatalogReq) {
  if (NULL == pCatalogReq) {
    return;
  }
  taosArrayDestroy(pCatalogReq->pDbVgroup);
  taosArrayDestroy(pCatalogReq->pDbCfg);
  taosArrayDestroy(pCatalogReq->pDbInfo);
  if (pCatalogReq->cloned) {
    taosArrayDestroy(pCatalogReq->pTableMeta);
    taosArrayDestroy(pCatalogReq->pTableHash);
#ifdef TD_ENTERPRISE
    taosArrayDestroy(pCatalogReq->pView);
#endif
    taosArrayDestroy(pCatalogReq->pTableTSMAs);
    taosArrayDestroy(pCatalogReq->pTSMAs);
    taosArrayDestroy(pCatalogReq->pTableName);
  } else {
    taosArrayDestroyEx(pCatalogReq->pTableMeta, destoryTablesReq);
    taosArrayDestroyEx(pCatalogReq->pTableHash, destoryTablesReq);
#ifdef TD_ENTERPRISE
    taosArrayDestroyEx(pCatalogReq->pView, destoryTablesReq);
#endif
    taosArrayDestroyEx(pCatalogReq->pTableTSMAs, destoryTablesReq);
    taosArrayDestroyEx(pCatalogReq->pTSMAs, destoryTablesReq);
    taosArrayDestroyEx(pCatalogReq->pTableName, destoryTablesReq);
  }
  taosArrayDestroy(pCatalogReq->pUdf);
  taosArrayDestroy(pCatalogReq->pIndex);
  taosArrayDestroy(pCatalogReq->pUser);
  taosArrayDestroy(pCatalogReq->pTableIndex);
  taosArrayDestroy(pCatalogReq->pTableCfg);
  taosArrayDestroy(pCatalogReq->pTableTag);
  taosArrayDestroy(pCatalogReq->pVStbRefDbs);
}

void tfreeSParseQueryRes(void* p) {
  if (NULL == p) {
    return;
  }

  SParseQueryRes* pRes = p;
  destoryCatalogReq(pRes->pCatalogReq);
  taosMemoryFree(pRes->pCatalogReq);
  catalogFreeMetaData(&pRes->meta);
}

void qDestroyParseContext(SParseContext* pCxt) {
  if (NULL == pCxt) {
    return;
  }

  taosArrayDestroyEx(pCxt->pSubMetaList, tfreeSParseQueryRes);
  taosArrayDestroy(pCxt->pTableMetaPos);
  taosArrayDestroy(pCxt->pTableVgroupPos);
  taosMemoryFree(pCxt);
}

void qDestroyQuery(SQuery* pQueryNode) { nodesDestroyNode((SNode*)pQueryNode); }

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  return extractResultSchema(pRoot, numOfCols, pSchema, NULL);
}

int32_t qSetSTableIdForRsma(SNode* pStmt, int64_t uid) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    SNode* pTable = ((SSelectStmt*)pStmt)->pFromTable;
    if (QUERY_NODE_REAL_TABLE == nodeType(pTable)) {
      ((SRealTableNode*)pTable)->pMeta->uid = uid;
      ((SRealTableNode*)pTable)->pMeta->suid = uid;
      return TSDB_CODE_SUCCESS;
    }
  }
  return TSDB_CODE_FAILED;
}

int32_t qInitKeywordsTable() { return taosInitKeywordsTable(); }

void qCleanupKeywordsTable() { taosCleanupKeywordsTable(); }

int32_t qStmtBindParams(SQuery* pQuery, TAOS_MULTI_BIND* pParams, int32_t colIdx, void *charsetCxt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (colIdx < 0) {
    int32_t size = taosArrayGetSize(pQuery->pPlaceholderValues);
    for (int32_t i = 0; i < size; ++i) {
      code = setValueByBindParam((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, i), pParams + i, charsetCxt);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } else {
    code = setValueByBindParam((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, colIdx), pParams, charsetCxt);
  }

  if (TSDB_CODE_SUCCESS == code && (colIdx < 0 || colIdx + 1 == pQuery->placeholderNum)) {
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = NULL;
    code = nodesCloneNode(pQuery->pPrepareRoot, &pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    rewriteExprAlias(pQuery->pRoot);
  }
  return code;
}

static int32_t setValueByBindParam2(SValueNode* pVal, TAOS_STMT2_BIND* pParam, void* charsetCxt) {
  if (!pParam || IS_NULL_TYPE(pParam->buffer_type)) {
    return TSDB_CODE_APP_ERROR;
  }
  if (IS_VAR_DATA_TYPE(pVal->node.resType.type)) {
    taosMemoryFreeClear(pVal->datum.p);
  }

  if (pParam->is_null && 1 == *(pParam->is_null)) {
    pVal->node.resType.type = TSDB_DATA_TYPE_NULL;
    pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return TSDB_CODE_SUCCESS;
  }

  int32_t inputSize = (NULL != pParam->length ? *(pParam->length) : tDataTypes[pParam->buffer_type].bytes);
  pVal->node.resType.type = pParam->buffer_type;
  pVal->node.resType.bytes = inputSize;

  switch (pParam->buffer_type) {
    case TSDB_DATA_TYPE_VARBINARY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      memcpy(varDataVal(pVal->datum.p), pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      strncpy(varDataVal(pVal->datum.p), (const char*)pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      pVal->node.resType.bytes *= TSDB_NCHAR_SIZE;
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return terrno;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4(pParam->buffer, inputSize, (TdUcs4*)varDataVal(pVal->datum.p), pVal->node.resType.bytes,
                         &output, charsetCxt)) {
        return terrno;
      }
      varDataSetLen(pVal->datum.p, output);
      pVal->node.resType.bytes = output + VARSTR_HEADER_SIZE;
      break;
    }
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      return TSDB_CODE_BLOB_NOT_SUPPORT;  // BLOB data type is not supported in stmt2
    default: {
      int32_t code = nodesSetValueNodeValue(pVal, pParam->buffer);
      if (code) {
        return code;
      }
      break;
    }
  }
  pVal->translate = true;
  return TSDB_CODE_SUCCESS;
}

int32_t qStmtBindParams2(SQuery* pQuery, TAOS_STMT2_BIND* pParams, int32_t colIdx, void* charsetCxt) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (colIdx < 0) {
    int32_t size = taosArrayGetSize(pQuery->pPlaceholderValues);
    for (int32_t i = 0; i < size; ++i) {
      code = setValueByBindParam2((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, i), pParams + i, charsetCxt);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } else {
    code = setValueByBindParam2((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, colIdx), pParams, charsetCxt);
  }

  if (TSDB_CODE_SUCCESS == code && (colIdx < 0 || colIdx + 1 == pQuery->placeholderNum)) {
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = NULL;
    code = nodesCloneNode(pQuery->pPrepareRoot, &pQuery->pRoot);
  }
  if (TSDB_CODE_SUCCESS == code) {
    rewriteExprAlias(pQuery->pRoot);
  }
  return code;
}

int32_t qStmtParseQuerySql(SParseContext* pCxt, SQuery* pQuery) {
  int32_t code = translate(pCxt, pQuery, NULL);
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, pQuery);
  }
  return code;
}
