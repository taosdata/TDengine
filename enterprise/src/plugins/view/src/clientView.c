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
#include "clientInt.h"
#include "clientLog.h"

int32_t asyncParseSql(void* param) {
  doAsyncQuery((SRequestObj*)param, false);
  return TSDB_CODE_SUCCESS;
}

static int32_t buildParseSqlRes(SRequestObj* pRequest, SParseSqlRes* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (pRes->resType) {
    case PARSE_SQL_RES_QUERY: {
      SParseQueryRes* pQueryRes = &pRes->queryRes;
      if (NULL != pRequest->pQuery) {
        pQueryRes->pQuery = NULL;
        code = nodesCloneNode(pRequest->pQuery->pRoot, &pQueryRes->pQuery);
        if (NULL == pQueryRes->pQuery) {
           return code;
        }
      }
      if (NULL != pRequest->pWrapper) {
        SSqlCallbackWrapper *pWrapper = pRequest->pWrapper;
        TSWAP(pQueryRes->pCatalogReq, pWrapper->pCatalogReq);
      }
      TAOS_MEMCPY(&pQueryRes->meta, &pRequest->parseMeta, sizeof(pRequest->parseMeta));
      TAOS_MEMSET(&pRequest->parseMeta, 0, sizeof(pRequest->parseMeta));
      break;
    } 
    case PARSE_SQL_RES_SCHEMA: {
      if (NULL != pRequest->pQuery) {
        SQuery* pQuery = pRequest->pQuery;
        SParseSchemaRes* pSchemaRes = &pRes->schemaRes;
        pSchemaRes->numOfCols = pQuery->numOfResCols;
        pSchemaRes->precision = pQuery->precision;
        pSchemaRes->pSchema = taosMemoryMalloc(pQuery->numOfResCols * sizeof(SSchema));
        if (NULL == pSchemaRes->pSchema) {
          code = terrno = TSDB_CODE_OUT_OF_MEMORY;
        } else {
          TAOS_MEMCPY(pSchemaRes->pSchema, pQuery->pResSchema, pQuery->numOfResCols * sizeof(SSchema));
        }
      }
      break;
    }
    default:
      break;
  }

  return code;
}
 
int32_t clientParseSqlImpl(void* param, const char* dbName, const char* sql, bool parseOnly, const char* effeciveUser, SParseSqlRes* pRes) {
   SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
   SSyncQueryParam* syncParam = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
   if (NULL == syncParam) {
     QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
   }
   
   if (tsem_init(&syncParam->sem, 0, 0)) {
     taosMemoryFree(syncParam);
     QRY_ERR_RET(terrno);
   }
 
   SRequestObj* pRequest = pWrapper->pRequest;
   SRequestObj* pNewRequest = NULL;
   int32_t      code = buildRequest(pRequest->pTscObj->id, sql, strlen(sql), syncParam, true, &pNewRequest, 0);
   if (code != TSDB_CODE_SUCCESS) {
     terrno = code;
     return code;
   }

   if (NULL != effeciveUser) {
     pNewRequest->effectiveUser = tstrdup(effeciveUser);
     if (NULL == pNewRequest) {
        freeQueryParam(syncParam);
        destroyRequest(pNewRequest);
        QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
     }
   }

   taosMemoryFree(pNewRequest->pDb);
   pNewRequest->pDb = tstrdup(dbName);
   if (NULL == pNewRequest->pDb) {
      freeQueryParam(syncParam);
      destroyRequest(pNewRequest);
      QRY_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
   }
   pNewRequest->parseOnly = parseOnly;
   pNewRequest->body.queryFp = syncQueryFn;
 
   code = taosAsyncExec(asyncParseSql, pNewRequest, NULL);
   if (TSDB_CODE_SUCCESS != code) {
     tscError("failed to sched async parse sql");
     return code;
   }

   code = taosAsyncWait();
   if (TSDB_CODE_SUCCESS != code) {
     tscError("failed to sched async parse sql");
     return code;
   }

   if (tsem_wait(&syncParam->sem)) {
     tscError("tsem_wait view syncParam sem failed, error:%s", tstrerror(terrno));
     return terrno;
   }

   code = taosAsyncRecover();
   if (TSDB_CODE_SUCCESS != code) {
     tscError("failed to sched async parse sql");
     return code;
   }
 
   code = pNewRequest->code;
   pRequest->code = code;
 
   code = buildParseSqlRes(pNewRequest, pRes);
   if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == pRequest->code) {
     pRequest->code = code;
   }
   
   if (0 != pNewRequest->msgBuf[0]) {
     tstrncpy(pRequest->msgBuf, pNewRequest->msgBuf, pRequest->msgBufLen);
   }
   
   freeQueryParam(syncParam);
   destroyRequest(pNewRequest);
   
   return pRequest->code;
}

