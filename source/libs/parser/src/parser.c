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

#include "parInt.h"
#include "parToken.h"

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

static int32_t setValueByBindParam(SValueNode* pVal, TAOS_MULTI_BIND* pParam) {
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
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      memcpy(varDataVal(pVal->datum.p), pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      strncpy(varDataVal(pVal->datum.p), (const char*)pParam->buffer, pVal->node.resType.bytes);
      pVal->node.resType.bytes += VARSTR_HEADER_SIZE;
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      pVal->node.resType.bytes *= TSDB_NCHAR_SIZE;
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t output = 0;
      if (!taosMbsToUcs4(pParam->buffer, inputSize, (TdUcs4*)varDataVal(pVal->datum.p), pVal->node.resType.bytes,
                         &output)) {
        return errno;
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
    sprintf(((SExprNode*)pNode)->aliasName, "#%d", *(int32_t*)pContext);
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

int32_t qParseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq) {
  int32_t code = nodesAcquireAllocator(pCxt->allocatorId);
  if (TSDB_CODE_SUCCESS == code) {
    if (qIsInsertValuesSql(pCxt->pSql, pCxt->sqlLen)) {
      code = parseInsertSql(pCxt, pQuery, pCatalogReq, NULL);
    } else {
      code = parseQuerySyntax(pCxt, pQuery, pCatalogReq);
    }
  }
  nodesReleaseAllocator(pCxt->allocatorId);
  terrno = code;
  return code;
}

int32_t qAnalyseSqlSemantic(SParseContext* pCxt, const struct SCatalogReq* pCatalogReq,
                            const struct SMetaData* pMetaData, SQuery* pQuery) {
  SParseMetaCache metaCache = {0};
  int32_t         code = nodesAcquireAllocator(pCxt->allocatorId);
  if (TSDB_CODE_SUCCESS == code && pCatalogReq) {
    code = putMetaDataToCache(pCatalogReq, pMetaData, &metaCache);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = analyseSemantic(pCxt, pQuery, &metaCache);
  }
  nodesReleaseAllocator(pCxt->allocatorId);
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
    case QUERY_NODE_CREATE_STREAM_STMT: {
      code = translatePostCreateStream(pCxt, pQuery, pBlock);
      break;
    }
    case QUERY_NODE_CREATE_INDEX_STMT: {
      code = translatePostCreateSmaIndex(pCxt, pQuery, pBlock);
      break;
    }
    default:
      break;
  }

  return code;
}


static void destoryTablesReq(void *p) {
  STablesReq *pRes = (STablesReq *)p;
  taosArrayDestroy(pRes->pTables);
}

void destoryCatalogReq(SCatalogReq *pCatalogReq) {
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
  } else {
    taosArrayDestroyEx(pCatalogReq->pTableMeta, destoryTablesReq);
    taosArrayDestroyEx(pCatalogReq->pTableHash, destoryTablesReq);
#ifdef TD_ENTERPRISE
    taosArrayDestroyEx(pCatalogReq->pView, destoryTablesReq);
#endif  
  }
  taosArrayDestroy(pCatalogReq->pUdf);
  taosArrayDestroy(pCatalogReq->pIndex);
  taosArrayDestroy(pCatalogReq->pUser);
  taosArrayDestroy(pCatalogReq->pTableIndex);
  taosArrayDestroy(pCatalogReq->pTableCfg);
  taosArrayDestroy(pCatalogReq->pTableTag);
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
  return extractResultSchema(pRoot, numOfCols, pSchema);
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

void qCleanupKeywordsTable() { taosCleanupKeywordsTable(); }

int32_t qStmtBindParams(SQuery* pQuery, TAOS_MULTI_BIND* pParams, int32_t colIdx) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (colIdx < 0) {
    int32_t size = taosArrayGetSize(pQuery->pPlaceholderValues);
    for (int32_t i = 0; i < size; ++i) {
      code = setValueByBindParam((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, i), pParams + i);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } else {
    code = setValueByBindParam((SValueNode*)taosArrayGetP(pQuery->pPlaceholderValues, colIdx), pParams);
  }

  if (TSDB_CODE_SUCCESS == code && (colIdx < 0 || colIdx + 1 == pQuery->placeholderNum)) {
    nodesDestroyNode(pQuery->pRoot);
    pQuery->pRoot = nodesCloneNode(pQuery->pPrepareRoot);
    if (NULL == pQuery->pRoot) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
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
