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

#include "astGenerator.h"
#include "parserInt.h"
#include "parserUtil.h"
#include "ttoken.h"
#include "executor.h"

bool qIsInsertSql(const char* pStr, size_t length) {
  return false;
}

int32_t qParseQuerySql(const char* pStr, size_t length, struct SQueryStmtInfo** pQueryInfo, int64_t id, char* msg, int32_t msgLen) {
  *pQueryInfo = calloc(1, sizeof(SQueryStmtInfo));
  if (*pQueryInfo == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY; // set correct error code.
  }

  SSqlInfo info = doGenerateAST(pStr);
  if (!info.valid) {
    strncpy(msg, info.msg, msgLen);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  struct SCatalog* pCatalog = getCatalogHandle(NULL);
  int32_t code = qParserValidateSqlNode(pCatalog, &info, *pQueryInfo, id, msg, msgLen);
  if (code != 0) {
    return code;
  }

  return 0;
}

int32_t qParseInsertSql(const char* pStr, size_t length, struct SInsertStmtInfo** pInsertInfo, int64_t id, char* msg, int32_t msgLen) {
  return 0;
}

int32_t qParserConvertSql(const char* pStr, size_t length, char** pConvertSql) {
  return 0;
}

static int32_t getTableNameFromSqlNode(SSqlNode* pSqlNode, SArray* tableNameList, char* msg, int32_t msgBufLen);

static int32_t tnameComparFn(const void* p1, const void* p2) {
  SName* pn1 = (SName*)p1;
  SName* pn2 = (SName*)p2;

  int32_t ret = strncmp(pn1->acctId, pn2->acctId, tListLen(pn1->acctId));
  if (ret != 0) {
    return ret > 0? 1:-1;
  } else {
    ret = strncmp(pn1->dbname, pn2->dbname, tListLen(pn1->dbname));
    if (ret != 0) {
      return ret > 0? 1:-1;
    } else {
      ret = strncmp(pn1->tname, pn2->tname, tListLen(pn1->tname));
      if (ret != 0) {
        return ret > 0? 1:-1;
      } else {
        return 0;
      }
    }
  }
}

static int32_t getTableNameFromSubquery(SSqlNode* pSqlNode, SArray* tableNameList, char* msgBuf, int32_t msgBufLen) {
  int32_t numOfSub = (int32_t)taosArrayGetSize(pSqlNode->from->list);

  for (int32_t j = 0; j < numOfSub; ++j) {
    SRelElementPair* sub = taosArrayGet(pSqlNode->from->list, j);

    int32_t num = (int32_t)taosArrayGetSize(sub->pSubquery);
    for (int32_t i = 0; i < num; ++i) {
      SSqlNode* p = taosArrayGetP(sub->pSubquery, i);
      if (p->from->type == SQL_NODE_FROM_TABLELIST) {
        int32_t code = getTableNameFromSqlNode(p, tableNameList, msgBuf, msgBufLen);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        getTableNameFromSubquery(p, tableNameList, msgBuf, msgBufLen);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getTableNameFromSqlNode(SSqlNode* pSqlNode, SArray* tableNameList, char* msg, int32_t msgBufLen) {
  const char* msg1 = "invalid table name";

  int32_t numOfTables = (int32_t) taosArrayGetSize(pSqlNode->from->list);
  assert(pSqlNode->from->type == SQL_NODE_FROM_TABLELIST);

  for(int32_t j = 0; j < numOfTables; ++j) {
    SRelElementPair* item = taosArrayGet(pSqlNode->from->list, j);

    SToken* t = &item->tableName;
    if (t->type == TK_INTEGER || t->type == TK_FLOAT || t->type == TK_STRING) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg1);
    }

//    tscDequoteAndTrimToken(t);
    if (parserValidateIdToken(t) != TSDB_CODE_SUCCESS) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg1);
    }

    SName name = {0};
//    int32_t code = tscSetTableFullName(&name, t, pSql);
//    if (code != TSDB_CODE_SUCCESS) {
//      return code;
//    }

    taosArrayPush(tableNameList, &name);
  }

  return TSDB_CODE_SUCCESS;
}

static void freePtrElem(void* p) {
  tfree(*(char**)p);
}

int32_t qParserExtractRequestedMetaInfo(const SSqlInfo* pSqlInfo, SMetaReq* pMetaInfo, char* msg, int32_t msgBufLen) {
  int32_t code  = TSDB_CODE_SUCCESS;

  pMetaInfo->pTableName = taosArrayInit(4, sizeof(SName));
  pMetaInfo->pUdf = taosArrayInit(4, POINTER_BYTES);

  size_t size = taosArrayGetSize(pSqlInfo->list);
  for (int32_t i = 0; i < size; ++i) {
    SSqlNode* pSqlNode = taosArrayGetP(pSqlInfo->list, i);
    if (pSqlNode->from == NULL) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, "invalid from clause");
    }

    // load the table meta in the FROM clause
    if (pSqlNode->from->type == SQL_NODE_FROM_TABLELIST) {
      code = getTableNameFromSqlNode(pSqlNode, pMetaInfo->pTableName, msg, msgBufLen);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      code = getTableNameFromSubquery(pSqlNode, pMetaInfo->pTableName, msg, msgBufLen);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  taosArraySort(pMetaInfo->pTableName, tnameComparFn);
  taosArrayRemoveDuplicate(pMetaInfo->pTableName, tnameComparFn, NULL);

  size_t funcSize = 0;
  if (pSqlInfo->funcs) {
    funcSize = taosArrayGetSize(pSqlInfo->funcs);
  }

  if (funcSize > 0) {
    for (size_t i = 0; i < funcSize; ++i) {
      SToken* t = taosArrayGet(pSqlInfo->funcs, i);
      assert(t != NULL);

      if (t->n >= TSDB_FUNC_NAME_LEN) {
        return parserSetSyntaxErrMsg(msg, msgBufLen, "too long function name", t->z);
      }

      // Let's assume that it is an UDF/UDAF, if it is not a built-in function.
      if (!isBuiltinFunction(t->z, t->n)) {
        char* fname = strndup(t->z, t->n);
        taosArrayPush(pMetaInfo->pUdf, &fname);
      }
    }
  }
}