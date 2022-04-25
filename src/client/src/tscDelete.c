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
#include "taosmsg.h"
#include "tcmdtype.h"
#include "tscLog.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "tscDelete.h"
#include "tscSubquery.h"


void tscDumpEpSetFromVgroupInfo(SRpcEpSet *pEpSet, SNewVgroupInfo *pVgroupInfo);

//
// handle error
//
void tscHandleSubDeleteError(SRetrieveSupport *trsupport, SSqlObj *pSql, int numOfRows) {

}

//
// sub delete sql callback
//
void tscSubDeleteCallback(void *param, TAOS_RES *tres, int code) {
  // the param may be null, since it may be done by other query threads. and the asyncOnError may enter in this
  // function while kill query by a user.
  if (param == NULL) {
    assert(code != TSDB_CODE_SUCCESS);
    return;
  }

  SRetrieveSupport *trsupport = (SRetrieveSupport *) param;
  
  SSqlObj*  pParentSql = trsupport->pParentSql;
  SSqlObj*  pSql       = (SSqlObj *) tres;
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pParentSql->cmd, 0);
  SVgroupMsg* pVgroup = &pTableMetaInfo->vgroupList->vgroups[trsupport->subqueryIndex];

  //
  // stable query killed or other subquery failed, all query stopped
  //
  if (pParentSql->res.code != TSDB_CODE_SUCCESS) {
    trsupport->numOfRetry = MAX_NUM_OF_SUBQUERY_RETRY;
    tscError("0x%"PRIx64" query cancelled or failed, sub:0x%"PRIx64", vgId:%d, orderOfSub:%d, code:%s, global code:%s",
        pParentSql->self, pSql->self, pVgroup->vgId, trsupport->subqueryIndex, tstrerror(code), tstrerror(pParentSql->res.code));
    if (subAndCheckDone(pSql, pParentSql, trsupport->subqueryIndex)) {
      // all sub done, call parentSQL callback to finish
      (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.numOfRows);
    }
    tfree(pSql->param);
    return;
  }
  
  /*
   * if a subquery on a vnode failed, all retrieve operations from vnode that occurs later
   * than this one are actually not necessary, we simply call the tscRetrieveFromDnodeCallBack
   * function to abort current and remain retrieve process.
   *
   * NOTE: thread safe is required.
   */
  if (taos_errno(pSql) != TSDB_CODE_SUCCESS) {
    tscError(":CDEL 0x%"PRIx64" sub:0x%"PRIx64" reach the max retry times or no need to retry, set global code:%s", pParentSql->self, pSql->self, tstrerror(code));
    atomic_val_compare_exchange_32(&pParentSql->res.code, TSDB_CODE_SUCCESS, code);  // set global code and abort
    tscHandleSubDeleteError(param, tres, pParentSql->res.code);
    if (subAndCheckDone(pSql, pParentSql, trsupport->subqueryIndex)) {
      // all sub done, call parentSQL callback to finish
      (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.numOfRows);
    }
    tfree(pSql->param);
    return;
  }

  // record
  tscInfo("0x%"PRIx64":CDEL sub:0x%"PRIx64" query complete, ep:%s, vgId:%d, orderOfSub:%d, retrieve row(s)=%d tables(s)=%d", trsupport->pParentSql->self,
      pSql->self, pVgroup->epAddr[pSql->epSet.inUse].fqdn, pVgroup->vgId, trsupport->subqueryIndex, pSql->res.numOfRows, pSql->res.numOfTables);

  // success do total count
  SSubqueryState *subState = &pParentSql->subState;
  pthread_mutex_lock(&subState->mutex);
  pParentSql->res.numOfRows   += pSql->res.numOfRows;
  pParentSql->res.numOfTables += pSql->res.numOfTables;
  pthread_mutex_unlock(&subState->mutex);

  if (subAndCheckDone(pSql, pParentSql, trsupport->subqueryIndex)) {
    // all sub done, call parentSQL callback to finish
    (*pParentSql->fp)(pParentSql->param, pParentSql, pParentSql->res.numOfRows);
  }
  tfree(pSql->param);

  return ;
}

void writeMsgVgId(char * payload, int32_t vgId) {
  SSubmitMsg* pSubmitMsg = (SSubmitMsg *)(payload + sizeof(SMsgDesc)); 
  // SSubmitMsg
  pSubmitMsg->header.vgId = htonl(vgId);
}

//
// STable malloc sub delete
//
SSqlObj *tscCreateSTableSubDelete(SSqlObj *pSql, SVgroupMsg* pVgroupMsg, SRetrieveSupport *trsupport) {
  // Init
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("0x%"PRIx64":CDEL new subdelete failed.", pSql->self);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pNew->pTscObj   = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->sqlstr    = strdup(pSql->sqlstr);
  pNew->rootObj   = pSql->rootObj;
  pNew->fp        = tscSubDeleteCallback;
  pNew->fetchFp   = tscSubDeleteCallback;
  pNew->param     = trsupport;
  pNew->maxRetry  = TSDB_MAX_REPLICA;

  SSqlCmd* pNewCmd  = &pNew->cmd;
  memcpy(pNewCmd, pCmd, sizeof(SSqlCmd));
  // set zero
  pNewCmd->pQueryInfo = NULL;
  pNewCmd->active = NULL;
  pNewCmd->payload = NULL;
  pNewCmd->allocSize = 0;
  
  // payload copy
  int32_t ret = tscAllocPayload(pNewCmd, pCmd->payloadLen);
  if (ret != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64":CDEL , sub delete alloc payload failed. errcode=%d", pSql->self, ret);
    free(pNew);
    return NULL;
  }
  memcpy(pNewCmd->payload, pCmd->payload, pCmd->payloadLen);
  
  // update vgroup id
  writeMsgVgId(pNewCmd->payload ,pVgroupMsg->vgId);

  tsem_init(&pNew->rspSem, 0, 0);
  registerSqlObj(pNew);
  tscDebug("0x%"PRIx64":CDEL new sub insertion: %p", pSql->self, pNew);

  SNewVgroupInfo vgroupInfo = {0};
  taosHashGetClone(UTIL_GET_VGROUPMAP(pSql), &pVgroupMsg->vgId, sizeof(pVgroupMsg->vgId), NULL, &vgroupInfo);
  tscDumpEpSetFromVgroupInfo(&pNew->epSet, &vgroupInfo);

  return pNew;
}

//
// execute delete sql
//
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
  
  // check cancel
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    pCmd->command = TSDB_SQL_RETRIEVE_GLOBALMERGE;  // enable the abort of kill super table function.
    return pRes->code;
  }

  if(pTableMetaInfo->vgroupList == NULL) {
    tscError(":CDEL SQL:%p tablename=%s vgroupList is NULL.", pSql, pTableMetaInfo->name.tname);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
  
  SSubqueryState *pState = &pSql->subState;
  int32_t numOfSub = pTableMetaInfo->vgroupList->numOfVgroups;

  ret = doInitSubState(pSql, numOfSub);
  if (ret != 0) {
    tscAsyncResultOnError(pSql);
    return ret;
  }

  tscDebug("0x%"PRIx64":CDEL retrieved query data from %d vnode(s)", pSql->self, pState->numOfSub);
  pRes->code = TSDB_CODE_SUCCESS;
  
  int32_t i;
  for (i = 0; i < pState->numOfSub; ++i) {
    // vgroup
    SVgroupMsg* pVgroupMsg = &pTableMetaInfo->vgroupList->vgroups[i];

    // malloc each support
    SRetrieveSupport *trs = (SRetrieveSupport *)calloc(1, sizeof(SRetrieveSupport));
    if (trs == NULL) {
      tscError("0x%"PRIx64" failed to malloc buffer for SRetrieveSupport, orderOfSub:%d, reason:%s", pSql->self, i, strerror(errno));
      break;
    }
    trs->subqueryIndex = i;
    trs->pParentSql    = pSql;

    // malloc sub SSqlObj
    SSqlObj *pNew = tscCreateSTableSubDelete(pSql, pVgroupMsg, trs);
    if (pNew == NULL) {
      tscError("0x%"PRIx64"CDEL failed to malloc buffer for subObj, orderOfSub:%d, reason:%s", pSql->self, i, strerror(errno));
      tfree(trs);
      break;
    }
    pSql->pSubs[i] = pNew;
  }
  
  if (i < pState->numOfSub) {
    tscError("0x%"PRIx64":CDEL failed to prepare subdelete structure and launch subqueries", pSql->self);
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    
    doCleanupSubqueries(pSql, i);
    return pRes->code;   // free all allocated resource
  }
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    doCleanupSubqueries(pSql, i);
    return pRes->code;
  }

  // send sub sql
  doConcurrentlySendSubQueries(pSql);
  //return TSDB_CODE_TSC_QUERY_CANCELLED;

  return TSDB_CODE_SUCCESS;
}
