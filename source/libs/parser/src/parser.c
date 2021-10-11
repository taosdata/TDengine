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

#include "parserInt.h"
#include "ttoken.h"
#include "astGenerator.h"
#include "parserUtil.h"

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

static int32_t getTableNameFromSubquery(SSqlNode* pSqlNode, SArray* tableNameList, char* msgBuf) {
  int32_t numOfSub = (int32_t)taosArrayGetSize(pSqlNode->from->list);

  for (int32_t j = 0; j < numOfSub; ++j) {
    SRelElementPair* sub = taosArrayGet(pSqlNode->from->list, j);

    int32_t num = (int32_t)taosArrayGetSize(sub->pSubquery);
    for (int32_t i = 0; i < num; ++i) {
      SSqlNode* p = taosArrayGetP(sub->pSubquery, i);
      if (p->from->type == SQL_NODE_FROM_TABLELIST) {
        int32_t code = getTableNameFromSqlNode(p, tableNameList, msgBuf);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        getTableNameFromSubquery(p, tableNameList, msgBuf);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getTableNameFromSqlNode(SSqlNode* pSqlNode, SArray* tableNameList, char* msg, int32_t msgBufLen) {
  const char* msg1 = "invalid table name";

  int32_t numOfTables = (int32_t) taosArrayGetSize(pSqlNode->from->list);
  assert(pSqlNode->from->type == SQL_NODE_FROM_TABLELIST);

  for(int32_t j = 0; j < numOfTables; ++j) {
    SRelElementPair* item = taosArrayGet(pSqlNode->from->list, j);

    SToken* t = &item->tableName;
    if (t->type == TK_INTEGER || t->type == TK_FLOAT) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg1);
    }

    tscDequoteAndTrimToken(t);
    if (parserValidateIdToken(t) != TSDB_CODE_SUCCESS) {
      return parserSetInvalidOperatorMsg(msg, msgBufLen, msg1);
    }

    SName name = {0};
    int32_t code = tscSetTableFullName(&name, t, pSql);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    taosArrayPush(tableNameList, &name);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qParserExtractRequestedMetaInfo(const SArray* pSqlNodeList, SMetaReq* pMetaInfo, char* msg, int32_t msgBufLen) {
  int32_t code = TSDB_CODE_SUCCESS;

  SArray* tableNameList = NULL;
  SArray* pVgroupList   = NULL;
  SArray* plist         = NULL;
  STableMeta* pTableMeta = NULL;
//  size_t    tableMetaCapacity = 0;
//  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

//  pCmd->pTableMetaMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  tableNameList = taosArrayInit(4, sizeof(SName));
  size_t size = taosArrayGetSize(pSqlNodeList);
  for (int32_t i = 0; i < size; ++i) {
    SSqlNode* pSqlNode = taosArrayGetP(pSqlNodeList, i);
    if (pSqlNode->from == NULL) {
      goto _end;
    }

    // load the table meta in the from clause
    if (pSqlNode->from->type == SQL_NODE_FROM_TABLELIST) {
      code = getTableNameFromSqlNode(pSqlNode, tableNameList, msg, msgBufLen);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
    } else {
      code = getTableNameFromSubquery(pSqlNode, tableNameList, msg, msgBufLen);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
    }
  }

  char name[TSDB_TABLE_FNAME_LEN] = {0};

  plist = taosArrayInit(4, POINTER_BYTES);
  pVgroupList = taosArrayInit(4, POINTER_BYTES);

  taosArraySort(tableNameList, tnameComparFn);
  taosArrayRemoveDuplicate(tableNameList, tnameComparFn, NULL);

  STableMeta* pSTMeta = (STableMeta *)(pSql->pBuf);
  size_t numOfTables = taosArrayGetSize(tableNameList);
  for (int32_t i = 0; i < numOfTables; ++i) {
    SName* pname = taosArrayGet(tableNameList, i);
    tNameExtractFullName(pname, name);

    size_t len = strlen(name);

    if (NULL == taosHashGetCloneExt(tscTableMetaMap, name, len, NULL,  (void **)&pTableMeta, &tableMetaCapacity)) {
      // not found
      tfree(pTableMeta);
    }

    if (pTableMeta && pTableMeta->id.uid > 0) {
      tscDebug("0x%"PRIx64" retrieve table meta %s from local buf", pSql->self, name);

      // avoid mem leak, may should update pTableMeta
      void* pVgroupIdList = NULL;
      if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
        code = tscCreateTableMetaFromSTableMeta((STableMeta **)(&pTableMeta), name, &tableMetaCapacity, (STableMeta **)(&pSTMeta));
        pSql->pBuf = (void *)pSTMeta;

        // create the child table meta from super table failed, try load it from mnode
        if (code != TSDB_CODE_SUCCESS) {
          char* t = strdup(name);
          taosArrayPush(plist, &t);
          continue;
        }
      } else if (pTableMeta->tableType == TSDB_SUPER_TABLE) {
        // the vgroup list of super table is not kept in local buffer, so here need retrieve it from the mnode each time
        tscDebug("0x%"PRIx64" try to acquire cached super table %s vgroup id list", pSql->self, name);
        void* pv = taosCacheAcquireByKey(tscVgroupListBuf, name, len);
        if (pv == NULL) {
          char* t = strdup(name);
          taosArrayPush(pVgroupList, &t);
          tscDebug("0x%"PRIx64" failed to retrieve stable %s vgroup id list in cache, try fetch from mnode", pSql->self, name);
        } else {
          tFilePage* pdata = (tFilePage*) pv;
          pVgroupIdList = taosArrayInit((size_t) pdata->num, sizeof(int32_t));
          if (pVgroupIdList == NULL) {
            return TSDB_CODE_TSC_OUT_OF_MEMORY;
          }

          taosArrayAddBatch(pVgroupIdList, pdata->data, (int32_t) pdata->num);
          taosCacheRelease(tscVgroupListBuf, &pv, false);
        }
      }

      if (taosHashGet(pCmd->pTableMetaMap, name, len) == NULL) {
        STableMeta* pMeta = tscTableMetaDup(pTableMeta);
        STableMetaVgroupInfo tvi = { .pTableMeta = pMeta,  .vgroupIdList = pVgroupIdList};
        taosHashPut(pCmd->pTableMetaMap, name, len, &tvi, sizeof(STableMetaVgroupInfo));
      }
    } else {
      // Add to the retrieve table meta array list.
      // If the tableMeta is missing, the cached vgroup list for the corresponding super table will be ignored.
      tscDebug("0x%"PRIx64" failed to retrieve table meta %s from local buf", pSql->self, name);

      char* t = strdup(name);
      taosArrayPush(plist, &t);
    }
  }

  size_t funcSize = 0;
  if (pInfo->funcs) {
    funcSize = taosArrayGetSize(pInfo->funcs);
  }

  if (funcSize > 0) {
    for (size_t i = 0; i < funcSize; ++i) {
      SToken* t = taosArrayGet(pInfo->funcs, i);
      if (NULL == t) {
        continue;
      }

      if (t->n >= TSDB_FUNC_NAME_LEN) {
        code = tscSQLSyntaxErrMsg(tscGetErrorMsgPayload(pCmd), "too long function name", t->z);
        if (code != TSDB_CODE_SUCCESS) {
          goto _end;
        }
      }

      int32_t functionId = isValidFunction(t->z, t->n);
      if (functionId < 0) {
        struct SUdfInfo info = {0};
        info.name = strndup(t->z, t->n);
        if (pQueryInfo->pUdfInfo == NULL) {
          pQueryInfo->pUdfInfo = taosArrayInit(4, sizeof(struct SUdfInfo));
        }

        info.functionId = (int32_t)taosArrayGetSize(pQueryInfo->pUdfInfo) * (-1) - 1;;
        taosArrayPush(pQueryInfo->pUdfInfo, &info);
      }
    }
  }

  // load the table meta for a given table name list
  if (taosArrayGetSize(plist) > 0 || taosArrayGetSize(pVgroupList) > 0 || (pQueryInfo->pUdfInfo && taosArrayGetSize(pQueryInfo->pUdfInfo) > 0)) {
    code = getMultiTableMetaFromMnode(pSql, plist, pVgroupList, pQueryInfo->pUdfInfo, tscTableMetaCallBack, true);
  }

  _end:
  if (plist != NULL) {
    taosArrayDestroyEx(plist, freeElem);
  }

  if (pVgroupList != NULL) {
    taosArrayDestroyEx(pVgroupList, freeElem);
  }

  if (tableNameList != NULL) {
    taosArrayDestroy(tableNameList);
  }

  tfree(pTableMeta);
  return code;
}