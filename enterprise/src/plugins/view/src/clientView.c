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


 int32_t asyncValidateSql(void* param) {
   doAsyncQuery((SRequestObj*)param, false);
   return TSDB_CODE_SUCCESS;
 }
 
 
 int32_t clientValidateSqlImpl(void* param, const char* sql, SCMCreateViewReq* pReq) {
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
 
   pNewRequest->body.queryFp = syncQueryFn;
 
   code = taosAsyncExec(asyncValidateSql, pNewRequest, NULL);
   if (TSDB_CODE_SUCCESS != code) {
     tscError("failed to sched async validate sql");
     return code;
   }
 
   tsem_wait(&syncParam->sem);
 
   code = pNewRequest->code;
   pRequest->code = code;
 
   if (TSDB_CODE_SUCCESS == code && NULL != pNewRequest->pQuery) {
     SQuery* pQuery = pNewRequest->pQuery;
     
     pReq->numOfCols = pQuery->numOfResCols;
     pReq->precision = pQuery->precision;
     pReq->pSchema = taosMemoryMalloc(pQuery->numOfResCols * sizeof(SSchema));
     if (NULL == pReq->pSchema) {
       code = terrno = TSDB_CODE_OUT_OF_MEMORY;
     } else {
       memcpy(pReq->pSchema, pQuery->pResSchema, pQuery->numOfResCols * sizeof(SSchema));
     }
   } else if (0 != pNewRequest->msgBuf[0]) {
     strncpy(pRequest->msgBuf, pNewRequest->msgBuf, pRequest->msgBufLen - 1);
     pRequest->msgBuf[pRequest->msgBufLen - 1] = 0;
   }
   
   freeQueryParam(syncParam);
   destroyRequest(pNewRequest);
   
   return code;
 }
 



