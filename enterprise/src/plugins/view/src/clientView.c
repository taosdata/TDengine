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
      SSqlCallbackWrapper *pWrapper = pRequest->pWrapper;
      pQueryRes->pQuery = nodesCloneNode(pRequest->pQuery->pRoot);
      if (NULL == pQueryRes->pQuery) {
         return TSDB_CODE_OUT_OF_MEMORY;
      }
      TSWAP(pQueryRes->pCatalogReq, pWrapper->pCatalogReq);
      memcpy(&pQueryRes->meta, &pRequest->parseMeta, sizeof(pRequest->parseMeta));
      memset(&pRequest->parseMeta, 0, sizeof(pRequest->parseMeta));
      break;
    } 
    case PARSE_SQL_RES_SCHEMA: {
      SQuery* pQuery = pRequest->pQuery;
      SParseSchemaRes* pSchemaRes = &pRes->schemaRes;
      pSchemaRes->numOfCols = pQuery->numOfResCols;
      pSchemaRes->precision = pQuery->precision;
      pSchemaRes->pSchema = taosMemoryMalloc(pQuery->numOfResCols * sizeof(SSchema));
      if (NULL == pSchemaRes->pSchema) {
        code = terrno = TSDB_CODE_OUT_OF_MEMORY;
      } else {
        memcpy(pSchemaRes->pSchema, pQuery->pResSchema, pQuery->numOfResCols * sizeof(SSchema));
      }
      break;
    }
    default:
      break;
  }

  return code;
}
 
int32_t clientParseSqlImpl(void* param, const char* sql, bool parseOnly, const char* effeciveUser, SParseSqlRes* pRes) {
   SSqlCallbackWrapper *pWrapper = (SSqlCallbackWrapper *)param;
   SSyncQueryParam* syncParam = taosMemoryCalloc(1, sizeof(SSyncQueryParam));
   tsem_init(&syncParam->sem, 0, 0);
 
   SRequestObj* pRequest = pWrapper->pRequest;
   SRequestObj* pNewRequest = NULL;
   int32_t      code = buildRequest(pRequest->pTscObj->id, sql, strlen(sql), syncParam, true, &pNewRequest, 0);
   if (code != TSDB_CODE_SUCCESS) {
     terrno = code;
     return code;
   }

   if (NULL != effeciveUser) {
     pNewRequest->effectiveUser = strdup(effeciveUser);
   }
   
   pNewRequest->parseOnly = parseOnly;
   pNewRequest->body.queryFp = syncQueryFn;
 
   code = taosAsyncExec(asyncParseSql, pNewRequest, NULL);
   if (TSDB_CODE_SUCCESS != code) {
     tscError("failed to sched async parse sql");
     return code;
   }
 
   tsem_wait(&syncParam->sem);
 
   code = pNewRequest->code;
   pRequest->code = code;
 
   if (TSDB_CODE_SUCCESS == code && NULL != pNewRequest->pQuery) {
     code = buildParseSqlRes(pNewRequest, pRes);
   } else if (0 != pNewRequest->msgBuf[0]) {
     strncpy(pRequest->msgBuf, pNewRequest->msgBuf, pRequest->msgBufLen - 1);
     pRequest->msgBuf[pRequest->msgBufLen - 1] = 0;
   }
   
   freeQueryParam(syncParam);
   destroyRequest(pNewRequest);
   
   return code;
}

