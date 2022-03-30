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

#include "os.h"
#include "tcmdtype.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscDelete.h"

int32_t executeDelete(SSqlObj* pSql, SQueryInfo* pQueryInfo) {

  int32_t ret = TSDB_CODE_SUCCESS;
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  if(!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    // not super table
    pSql->cmd.active = pQueryInfo;
    return tscBuildAndSendRequest(pSql, pQueryInfo);
  }

  //
  // super table
  //

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;
  
  // pRes->code check only serves in launching super table sub-queries
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_GLOBALMERGE;  // enable the abort of kill super table function.
    return pRes->code;
  }
  
  tExtMemBuffer   **pMemoryBuf = NULL;
  tOrderDescriptor *pDesc  = NULL;

  pRes->qId = 0x1;  // hack the qhandle check

  uint32_t nBufferSize = (1u << 18u);  // 256KB, default buffer size
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSubqueryState *pState = &pSql->subState;
  int32_t numOfSub = (pTableMetaInfo->pVgroupTables == NULL) ? pTableMetaInfo->vgroupList->numOfVgroups
                                                             : (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);

  int32_t ret = doInitSubState(pSql, numOfSub);
  if (ret != 0) {
    tscAsyncResultOnError(pSql);
    return ret;
  }

  ret = tscCreateGlobalMergerEnv(pQueryInfo, &pMemoryBuf, pSql->subState.numOfSub, &pDesc, &nBufferSize, pSql->self);
  if (ret != 0) {
    pRes->code = ret;
    tscAsyncResultOnError(pSql);
    tfree(pDesc);
    tfree(pMemoryBuf);
    return ret;
  }

  tscDebug("0x%"PRIx64" retrieved query data from %d vnode(s)", pSql->self, pState->numOfSub);
  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i = 0;
  for (; i < pState->numOfSub; ++i) {
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("0x%"PRIx64" failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql->self, i, strerror(errno));
      break;
    }
    
    trs->pExtMemBuffer = pMemoryBuf;
    trs->pOrderDescriptor = pDesc;

    trs->localBuffer = (tFilePage *)calloc(1, nBufferSize + sizeof(tFilePage));
    trs->localBufferSize = nBufferSize + sizeof(tFilePage);
    if (trs->localBuffer == NULL) {
      tscError("0x%"PRIx64" failed to malloc buffer for local buffer, orderOfSub:%d, reason:%s", pSql->self, i, strerror(errno));
      tfree(trs);
      break;
    }

    trs->localBuffer->num = 0;
    trs->subqueryIndex = i;
    trs->pParentSql    = pSql;

    SSqlObj *pNew = tscCreateSTableSubquery(pSql, trs, NULL);
    if (pNew == NULL) {
      tscError("0x%"PRIx64" failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql->self, i, strerror(errno));
      tfree(trs->localBuffer);
      tfree(trs);
      break;
    }
    
    // todo handle multi-vnode situation
    if (pQueryInfo->tsBuf) {
      SQueryInfo *pNewQueryInfo = tscGetQueryInfo(&pNew->cmd);
      pNewQueryInfo->tsBuf = tsBufClone(pQueryInfo->tsBuf);
      assert(pNewQueryInfo->tsBuf != NULL);
    }
    
    tscDebug("0x%"PRIx64" sub:0x%"PRIx64" create subquery success. orderOfSub:%d", pSql->self, pNew->self,
        trs->subqueryIndex);
  }
  
  if (i < pState->numOfSub) {
    tscError("0x%"PRIx64" failed to prepare subquery structure and launch subqueries", pSql->self);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    
    tscDestroyGlobalMergerEnv(pMemoryBuf, pDesc, pState->numOfSub);
    doCleanupSubqueries(pSql, i);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscDestroyGlobalMergerEnv(pMemoryBuf, pDesc, pState->numOfSub);
    doCleanupSubqueries(pSql, i);
    return pRes->code;
  }

  doConcurrentlySendSubQueries(pSql);

  return TSDB_CODE_SUCCESS;
}