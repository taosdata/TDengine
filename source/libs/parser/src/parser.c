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

bool qIsInsertSql(const char* pStr, size_t length) {
  if (NULL == pStr) {
    return false;
  }

  int32_t index = 0;

  do {
    SToken t0 = tStrGetToken((char*)pStr, &index, false);
    if (t0.type != TK_NK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

static int32_t parseSqlIntoAst(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = parse(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = authenticate(pCxt, *pQuery);
  }

  if (TSDB_CODE_SUCCESS == code && (*pQuery)->placeholderNum > 0) {
    TSWAP((*pQuery)->pPrepareRoot, (*pQuery)->pRoot);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = translate(pCxt, *pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, *pQuery);
  }
  return code;
}

static int32_t setValueByBindParam(SValueNode* pVal, TAOS_MULTI_BIND* pParam) {
  if (pParam->is_null && 1 == *(pParam->is_null)) {
    pVal->node.resType.type = TSDB_DATA_TYPE_NULL;
    pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return TSDB_CODE_SUCCESS;
  }
  int32_t inputSize = (NULL != pParam->length ? *(pParam->length) : tDataTypes[pParam->buffer_type].bytes);
  pVal->node.resType.type = pParam->buffer_type;
  pVal->node.resType.bytes = inputSize;
  switch (pParam->buffer_type) {
    case TSDB_DATA_TYPE_BOOL:
      pVal->datum.b = *((bool*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pVal->datum.i = *((int8_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pVal->datum.i = *((int16_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_INT:
      pVal->datum.i = *((int32_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pVal->datum.i = *((int64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pVal->datum.d = *((float*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pVal->datum.d = *((double*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
      pVal->datum.p = taosMemoryCalloc(1, pVal->node.resType.bytes + VARSTR_HEADER_SIZE + 1);
      if (NULL == pVal->datum.p) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      varDataSetLen(pVal->datum.p, pVal->node.resType.bytes);
      strncpy(varDataVal(pVal->datum.p), (const char*)pParam->buffer, pVal->node.resType.bytes);
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
      pVal->node.resType.bytes = output;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
      pVal->datum.i = *((int64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pVal->datum.u = *((uint8_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pVal->datum.u = *((uint16_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UINT:
      pVal->datum.u = *((uint32_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pVal->datum.u = *((uint64_t*)pParam->buffer);
      break;
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      // todo
    default:
      break;
  }
  pVal->translate = true;
  return TSDB_CODE_SUCCESS;
}

static EDealRes rewriteQueryExprAliasImpl(SNode* pNode, void* pContext) {
  if (nodesIsExprNode(pNode) && QUERY_NODE_COLUMN != nodeType(pNode) && '\0' == ((SExprNode*)pNode)->userAlias[0]) {
    strcpy(((SExprNode*)pNode)->userAlias, ((SExprNode*)pNode)->aliasName);
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
  if (qIsInsertSql(pCxt->pSql, pCxt->sqlLen)) {
    code = parseInsertSql(pCxt, pQuery);
  } else {
    code = parseSqlIntoAst(pCxt, pQuery);
  }
  terrno = code;
  return code;
}

void qDestroyQuery(SQuery* pQueryNode) { nodesDestroyNode(pQueryNode); }

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  return extractResultSchema(pRoot, numOfCols, pSchema);
}

int32_t qSetSTableIdForRSma(SNode* pStmt, int64_t uid) {
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
  int32_t code = translate(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, pQuery);
  }

  return code;
}
