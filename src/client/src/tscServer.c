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

#include <tscompression.h>
#include "os.h"
#include "qPlan.h"
#include "qTableMeta.h"
#include "tcmdtype.h"
#include "tlockfree.h"
#include "trpc.h"
#include "tscGlobalmerge.h"
#include "tscLog.h"
#include "tscProfile.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "ttimer.h"

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo) = {0};

int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};

TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid, TSKEY dflt);
void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts);
void tscSaveSubscriptionProgress(void* sub);
static int32_t extractSTableQueryVgroupId(STableMetaInfo* pTableMetaInfo);

static int32_t minMsgSize() { return tsRpcHeadSize + 100; }
static int32_t getWaitingTimeInterval(int32_t count) {
  int32_t initial = 100; // 100 ms by default
  if (count <= 1) {
    return 0;
  }

  return initial * ((2u)<<(count - 2));
}

static int32_t vgIdCompare(const void *lhs, const void *rhs) {
  int32_t left = *(int32_t *)lhs;
  int32_t right = *(int32_t *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}
static int32_t removeDupVgid(int32_t *src, int32_t sz) {
  if (src == NULL || sz <= 0) {
    return 0;
  } 
  qsort(src, sz, sizeof(src[0]), vgIdCompare);

  int32_t ret = 1;
  for (int i = 1; i < sz; i++) {
    if (src[i] != src[i - 1]) {
      src[ret++] = src[i];
    }
  }
  return ret;
}

static void tscSetDnodeEpSet(SRpcEpSet* pEpSet, SVgroupInfo* pVgroupInfo) {
  assert(pEpSet != NULL && pVgroupInfo != NULL && pVgroupInfo->numOfEps > 0);

  // Issue the query to one of the vnode among a vgroup randomly.
  // change the inUse property would not affect the isUse attribute of STableMeta
  pEpSet->inUse = rand() % pVgroupInfo->numOfEps;

  // apply the FQDN string length check here
  bool existed = false;

  pEpSet->numOfEps = pVgroupInfo->numOfEps;
  for(int32_t i = 0; i < pVgroupInfo->numOfEps; ++i) {
    pEpSet->port[i] = pVgroupInfo->epAddr[i].port;

    int32_t len = (int32_t) strnlen(pVgroupInfo->epAddr[i].fqdn, TSDB_FQDN_LEN);
    if (len > 0) {
      tstrncpy(pEpSet->fqdn[i], pVgroupInfo->epAddr[i].fqdn, tListLen(pEpSet->fqdn[i]));
      existed = true;
    }
  }
  assert(existed);
}

static void tscDumpMgmtEpSet(SSqlObj *pSql) {
  SRpcCorEpSet *pCorEpSet = pSql->pTscObj->tscCorMgmtEpSet;
  taosCorBeginRead(&pCorEpSet->version);
  pSql->epSet = pCorEpSet->epSet;
  taosCorEndRead(&pCorEpSet->version);
}  
static void tscEpSetHtons(SRpcEpSet *s) {
   for (int32_t i = 0; i < s->numOfEps; i++) {
      s->port[i] = htons(s->port[i]);    
   }
}

bool tscEpSetIsEqual(SRpcEpSet *s1, SRpcEpSet *s2) {
   if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
     return false;
   }

   for (int32_t i = 0; i < s1->numOfEps; i++) {
     if (s1->port[i] != s2->port[i] 
        || strncmp(s1->fqdn[i], s2->fqdn[i], TSDB_FQDN_LEN) != 0)
        return false;
   }
   return true;
}

void tscUpdateMgmtEpSet(SSqlObj *pSql, SRpcEpSet *pEpSet) {
  // no need to update if equal
  SRpcCorEpSet *pCorEpSet = pSql->pTscObj->tscCorMgmtEpSet;
  taosCorBeginWrite(&pCorEpSet->version);
  pCorEpSet->epSet = *pEpSet;
  taosCorEndWrite(&pCorEpSet->version);
}

static void tscDumpEpSetFromVgroupInfo(SRpcEpSet *pEpSet, SNewVgroupInfo *pVgroupInfo) {
  if (pVgroupInfo == NULL) { return;}
  int8_t inUse = pVgroupInfo->inUse;
  pEpSet->inUse = (inUse >= 0 && inUse < TSDB_MAX_REPLICA) ? inUse: 0; 
  pEpSet->numOfEps = pVgroupInfo->numOfEps;  
  for (int32_t i = 0; i < pVgroupInfo->numOfEps; ++i) {
    tstrncpy(pEpSet->fqdn[i], pVgroupInfo->ep[i].fqdn, sizeof(pEpSet->fqdn[i]));
    pEpSet->port[i] = pVgroupInfo->ep[i].port;
  }
}

static void tscUpdateVgroupInfo(SSqlObj *pSql, SRpcEpSet *pEpSet) {
  SSqlCmd *pCmd = &pSql->cmd;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  if (pTableMetaInfo == NULL || pTableMetaInfo->pTableMeta == NULL) {
    return;
  }

  int32_t vgId = -1;
  if (pTableMetaInfo->pTableMeta->tableType == TSDB_SUPER_TABLE) {
    vgId = extractSTableQueryVgroupId(pTableMetaInfo);
  } else {
    vgId = pTableMetaInfo->pTableMeta->vgId;
  }

  assert(vgId > 0);

  SNewVgroupInfo vgroupInfo = {.vgId = -1};
  taosHashGetClone(tscVgroupMap, &vgId, sizeof(vgId), NULL, &vgroupInfo);
  assert(vgroupInfo.numOfEps > 0 && vgroupInfo.vgId > 0);

  tscDebug("before: Endpoint in use:%d, numOfEps:%d", vgroupInfo.inUse, vgroupInfo.numOfEps);
  vgroupInfo.inUse    = pEpSet->inUse;
  vgroupInfo.numOfEps = pEpSet->numOfEps;
  for (int32_t i = 0; i < vgroupInfo.numOfEps; i++) {
    tstrncpy(vgroupInfo.ep[i].fqdn, pEpSet->fqdn[i], TSDB_FQDN_LEN);
    vgroupInfo.ep[i].port = pEpSet->port[i];
  }

  tscDebug("after: EndPoint in use:%d, numOfEps:%d", vgroupInfo.inUse, vgroupInfo.numOfEps);
  taosHashPut(tscVgroupMap, &vgId, sizeof(vgId), &vgroupInfo, sizeof(SNewVgroupInfo));

  // Update the local cached epSet info cached by SqlObj
  int32_t inUse = pSql->epSet.inUse;
  tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);
  tscDebug("0x%"PRIx64" update the epSet in SqlObj, in use before:%d, after:%d", pSql->self, inUse, pSql->epSet.inUse);

}

int32_t extractSTableQueryVgroupId(STableMetaInfo* pTableMetaInfo) {
  assert(pTableMetaInfo != NULL);

  int32_t vgIndex = pTableMetaInfo->vgroupIndex;
  int32_t vgId = -1;

  if (pTableMetaInfo->pVgroupTables == NULL) {
    SVgroupsInfo *pVgroupInfo = pTableMetaInfo->vgroupList;
    assert(pVgroupInfo->vgroups[vgIndex].vgId > 0 && vgIndex < pTableMetaInfo->vgroupList->numOfVgroups);
    vgId = pVgroupInfo->vgroups[vgIndex].vgId;
  } else {
    int32_t numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
    assert(vgIndex >= 0 && vgIndex < numOfVgroups);

    SVgroupTableInfo *pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, vgIndex);
    vgId = pTableIdList->vgInfo.vgId;
  }

  return vgId;
}

void tscProcessHeartBeatRsp(void *param, TAOS_RES *tres, int code) {
  STscObj *pObj = (STscObj *)param;
  if (pObj == NULL) return;

  if (pObj != pObj->signature) {
    tscError("heartbeat msg, pObj:%p, signature:%p invalid", pObj, pObj->signature);
    return;
  }

  SSqlObj *pSql = tres;
  SSqlRes *pRes = &pSql->res;

  if (code == TSDB_CODE_SUCCESS) {
    SHeartBeatRsp *pRsp = (SHeartBeatRsp *)pRes->pRsp;
    SRpcEpSet     *epSet = &pRsp->epSet;
    if (epSet->numOfEps > 0) {
      tscEpSetHtons(epSet);

      //SRpcCorEpSet *pCorEpSet = pSql->pTscObj->tscCorMgmtEpSet;
      //if (!tscEpSetIsEqual(&pCorEpSet->epSet, epSet)) {
      //  tscTrace("%p updating epset: numOfEps: %d, inUse: %d", pSql, epSet->numOfEps, epSet->inUse);
      //  for (int8_t i = 0; i < epSet->numOfEps; i++) {
      //    tscTrace("endpoint %d: fqdn=%s, port=%d", i, epSet->fqdn[i], epSet->port[i]);
      //  }
      //}
      //concurrency problem, update mgmt epset anyway 
      tscUpdateMgmtEpSet(pSql, epSet);
    }

    pSql->pTscObj->connId = htonl(pRsp->connId);

    if (pRsp->killConnection) {
      tscKillConnection(pObj);
      return;
    } else {
      if (pRsp->queryId) {
        tscKillQuery(pObj, htonl(pRsp->queryId));
      }

      if (pRsp->streamId) {
        tscKillStream(pObj, htonl(pRsp->streamId));
      }
    }

    int32_t total  = htonl(pRsp->totalDnodes);
    int32_t online = htonl(pRsp->onlineDnodes);
    assert(online <= total);

    if (online < total) {
      tscError("0x%"PRIx64", HB, total dnode:%d, online dnode:%d", pSql->self, total, online);
      pSql->res.code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    }

    if (pRes->length == NULL) {
      pRes->length = calloc(2,  sizeof(int32_t));
    }

    pRes->length[0] = total;
    pRes->length[1] = online;
  } else {
    tscDebug("%" PRId64 " heartbeat failed, code:%s", pObj->hbrid, tstrerror(code));
    if (pRes->length == NULL) {
      pRes->length = calloc(2, sizeof(int32_t));
    }

    pRes->length[1] = 0;
    if (pRes->length[0] == 0) {
      pRes->length[0] = 1; // make sure that the value of the total node is greater than the online node
    }
  }

  if (pObj->hbrid != 0) {
    int32_t waitingDuring = tsShellActivityTimer * 500;
    tscDebug("0x%"PRIx64" send heartbeat in %dms", pSql->self, waitingDuring);

    taosTmrReset(tscProcessActivityTimer, waitingDuring, (void *)pObj->rid, tscTmr, &pObj->pTimer);
  } else {
    tscDebug("0x%"PRIx64" start to close tscObj:%p, not send heartbeat again", pSql->self, pObj);
  }
}

void tscProcessActivityTimer(void *handle, void *tmrId) {
  int64_t rid = (int64_t) handle;
  STscObj *pObj = taosAcquireRef(tscRefId, rid);
  if (pObj == NULL) {
    return;
  }

  SSqlObj* pHB = taosAcquireRef(tscObjRef, pObj->hbrid);
  if (pHB == NULL) {
    taosReleaseRef(tscRefId, rid);
    return;
  }

  assert(pHB->self == pObj->hbrid);

  pHB->retry = 0;
  int32_t code = tscBuildAndSendRequest(pHB, NULL);
  taosReleaseRef(tscObjRef, pObj->hbrid);

  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" failed to sent HB to server, reason:%s", pHB->self, tstrerror(code));
  }

  taosReleaseRef(tscRefId, rid);
}

int tscSendMsgToServer(SSqlObj *pSql) {
  STscObj* pObj = pSql->pTscObj;
  SSqlCmd* pCmd = &pSql->cmd;
  
  char *pMsg = rpcMallocCont(pCmd->payloadLen);
  if (NULL == pMsg) {
    tscError("0x%"PRIx64" msg:%s malloc failed", pSql->self, taosMsg[pSql->cmd.msgType]);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  // set the mgmt ip list
  if (pSql->cmd.command >= TSDB_SQL_MGMT) {
    tscDumpMgmtEpSet(pSql);
  }

  memcpy(pMsg, pSql->cmd.payload, pSql->cmd.payloadLen);

  SRpcMsg rpcMsg = {
      .msgType = pSql->cmd.msgType,
      .pCont   = pMsg,
      .contLen = pSql->cmd.payloadLen,
      .ahandle = (void*)pSql->self,
      .handle  = NULL,
      .code    = 0
  };

  
  rpcSendRequest(pObj->pRpcObj->pDnodeConn, &pSql->epSet, &rpcMsg, &pSql->rpcRid);
  return TSDB_CODE_SUCCESS;
}

static void doProcessMsgFromServer(SSchedMsg* pSchedMsg) {
  SRpcMsg* rpcMsg = pSchedMsg->ahandle;
  SRpcEpSet* pEpSet = pSchedMsg->thandle;

  TSDB_CACHE_PTR_TYPE handle = (TSDB_CACHE_PTR_TYPE) rpcMsg->ahandle;
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, handle);
  if (pSql == NULL) {
    rpcFreeCont(rpcMsg->pCont);
    free(rpcMsg);
    free(pEpSet);
    return;
  }

  assert(pSql->self == handle);

  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pSql->rpcRid = -1;

  if (pObj->signature != pObj) {
    tscDebug("0x%"PRIx64" DB connection is closed, cmd:%d pObj:%p signature:%p", pSql->self, pCmd->command, pObj, pObj->signature);

    taosRemoveRef(tscObjRef, handle);
    taosReleaseRef(tscObjRef, handle);
    rpcFreeCont(rpcMsg->pCont);
    free(rpcMsg);
    free(pEpSet);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if (pQueryInfo != NULL && pQueryInfo->type == TSDB_QUERY_TYPE_FREE_RESOURCE) {
    tscDebug("0x%"PRIx64" sqlObj needs to be released or DB connection is closed, cmd:%d type:%d, pObj:%p signature:%p",
        pSql->self, pCmd->command, pQueryInfo->type, pObj, pObj->signature);

    taosRemoveRef(tscObjRef, handle);
    taosReleaseRef(tscObjRef, handle);
    rpcFreeCont(rpcMsg->pCont);
    free(rpcMsg);
    free(pEpSet);
    return;
  }

  if (pEpSet) {
    if (!tscEpSetIsEqual(&pSql->epSet, pEpSet)) {
      if (pCmd->command < TSDB_SQL_MGMT) {
        tscUpdateVgroupInfo(pSql, pEpSet);
      } else {
        tscUpdateMgmtEpSet(pSql, pEpSet);
      }
    }
  }

  int32_t cmd = pCmd->command;

  // set the flag to denote that sql string needs to be re-parsed and build submit block with table schema
  if (cmd == TSDB_SQL_INSERT && rpcMsg->code == TSDB_CODE_TDB_TABLE_RECONFIGURE) {
    pSql->cmd.insertParam.schemaAttached = 1;
  }

  // single table query error need to be handled here.
  if ((cmd == TSDB_SQL_SELECT || cmd == TSDB_SQL_UPDATE_TAGS_VAL) &&
      (((rpcMsg->code == TSDB_CODE_TDB_INVALID_TABLE_ID || rpcMsg->code == TSDB_CODE_VND_INVALID_VGROUP_ID)) ||
       rpcMsg->code == TSDB_CODE_RPC_NETWORK_UNAVAIL || rpcMsg->code == TSDB_CODE_APP_NOT_READY)) {

    // 1. super table subquery
    // 2. nest queries are all not updated the tablemeta and retry parse the sql after cleanup local tablemeta/vgroup id buffer
    if ((TSDB_QUERY_HAS_TYPE(pQueryInfo->type, (TSDB_QUERY_TYPE_STABLE_SUBQUERY | TSDB_QUERY_TYPE_SUBQUERY |
                                               TSDB_QUERY_TYPE_TAG_FILTER_QUERY)) &&
                                               !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_PROJECTION_QUERY)) ||
                                               (TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_NEST_SUBQUERY))) {
      // do nothing in case of super table subquery
    }  else {
      pSql->retry += 1;
      tscWarn("0x%" PRIx64 " it shall renew table meta, code:%s, retry:%d", pSql->self, tstrerror(rpcMsg->code), pSql->retry);

      pSql->res.code = rpcMsg->code;  // keep the previous error code
      if (pSql->retry > pSql->maxRetry) {
        tscError("0x%" PRIx64 " max retry %d reached, give up", pSql->self, pSql->maxRetry);
      } else {
        // wait for a little bit moment and then retry
        // todo do not sleep in rpc callback thread, add this process into queue to process
        if (rpcMsg->code == TSDB_CODE_APP_NOT_READY || rpcMsg->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
          int32_t duration = getWaitingTimeInterval(pSql->retry);
          taosMsleep(duration);
        }

        pSql->retryReason = rpcMsg->code;
        rpcMsg->code = tscRenewTableMeta(pSql, 0);
        // if there is an error occurring, proceed to the following error handling procedure.
        if (rpcMsg->code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
          taosReleaseRef(tscObjRef, handle);
          rpcFreeCont(rpcMsg->pCont);
          free(rpcMsg);
          free(pEpSet);
          return;
        }
      }
    }
  }

  pRes->rspLen = 0;

  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscDebug("0x%"PRIx64" query is cancelled, code:%s", pSql->self, tstrerror(pRes->code));
  } else {
    pRes->code = rpcMsg->code;
  }

  if (pRes->code == TSDB_CODE_SUCCESS) {
    tscDebug("0x%"PRIx64" reset retry counter to be 0 due to success rsp, old:%d", pSql->self, pSql->retry);
    pSql->retry = 0;
  }

  if (pRes->code != TSDB_CODE_TSC_QUERY_CANCELLED) {
    assert(rpcMsg->msgType == pCmd->msgType + 1);
    pRes->code    = rpcMsg->code;
    pRes->rspType = rpcMsg->msgType;
    pRes->rspLen  = rpcMsg->contLen;

    if (pRes->rspLen > 0 && rpcMsg->pCont) {
      char *tmp = (char *)realloc(pRes->pRsp, pRes->rspLen);
      if (tmp == NULL) {
        pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      } else {
        pRes->pRsp = tmp;
        memcpy(pRes->pRsp, rpcMsg->pCont, pRes->rspLen);
      }
    } else {
      tfree(pRes->pRsp);
    }

    /*
     * There is not response callback function for submit response.
     * The actual inserted number of points is the first number.
     */
    if (rpcMsg->msgType == TSDB_MSG_TYPE_SUBMIT_RSP && pRes->pRsp != NULL) {
      SShellSubmitRspMsg *pMsg = (SShellSubmitRspMsg*)pRes->pRsp;
      pMsg->code = htonl(pMsg->code);
      pMsg->numOfRows = htonl(pMsg->numOfRows);
      pMsg->affectedRows = htonl(pMsg->affectedRows);
      pMsg->failedRows = htonl(pMsg->failedRows);
      pMsg->numOfFailedBlocks = htonl(pMsg->numOfFailedBlocks);

      pRes->numOfRows += pMsg->affectedRows;
      tscDebug("0x%"PRIx64" SQL cmd:%s, code:%s inserted rows:%d rspLen:%d", pSql->self, sqlCmd[pCmd->command],
          tstrerror(pRes->code), pMsg->affectedRows, pRes->rspLen);
    } else {
      tscDebug("0x%"PRIx64" SQL cmd:%s, code:%s rspLen:%d", pSql->self, sqlCmd[pCmd->command], tstrerror(pRes->code), pRes->rspLen);
    }
  }

  if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command]) {
    rpcMsg->code = (*tscProcessMsgRsp[pCmd->command])(pSql);
  }

  bool shouldFree = tscShouldBeFreed(pSql);
  if (rpcMsg->code != TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    if (rpcMsg->code != TSDB_CODE_SUCCESS) {
      pRes->code = rpcMsg->code;
    }
    rpcMsg->code = (pRes->code == TSDB_CODE_SUCCESS) ? (int32_t)pRes->numOfRows : pRes->code;
    if (pRes->code == TSDB_CODE_RPC_FQDN_ERROR) {
      tscAllocPayload(pCmd, TSDB_FQDN_LEN + 64); 
      // handle three situation 
      // 1. epset retry, only return last failure ep   
      // 2. no epset retry, like 'taos -h invalidFqdn', return invalidFqdn 
      // 3. other situation, no expected 
      if (pEpSet) {
        sprintf(tscGetErrorMsgPayload(pCmd), "%s\"%s\"", tstrerror(pRes->code),pEpSet->fqdn[(pEpSet->inUse)%(pEpSet->numOfEps)]);
      } else if (pCmd->command >= TSDB_SQL_MGMT) {
        SRpcEpSet tEpset;

        SRpcCorEpSet *pCorEpSet = pSql->pTscObj->tscCorMgmtEpSet;
        taosCorBeginRead(&pCorEpSet->version); 
        tEpset = pCorEpSet->epSet;   
        taosCorEndRead(&pCorEpSet->version); 

        sprintf(tscGetErrorMsgPayload(pCmd), "%s\"%s\"", tstrerror(pRes->code),tEpset.fqdn[(tEpset.inUse)%(tEpset.numOfEps)]);
      } else {
        sprintf(tscGetErrorMsgPayload(pCmd), "%s", tstrerror(pRes->code));
      }
    }
    (*pSql->fp)(pSql->param, pSql, rpcMsg->code);
  }

  if (shouldFree) { // in case of table-meta/vgrouplist query, automatically free it
    tscDebug("0x%"PRIx64" sqlObj is automatically freed", pSql->self);
    taosRemoveRef(tscObjRef, handle);
  }

  taosReleaseRef(tscObjRef, handle);
  rpcFreeCont(rpcMsg->pCont);
  free(rpcMsg);
  free(pEpSet);
}

void tscProcessMsgFromServer(SRpcMsg *rpcMsg, SRpcEpSet *pEpSet) {
  int64_t st = taosGetTimestampUs();
  SSchedMsg schedMsg = {0};

  schedMsg.fp = doProcessMsgFromServer;

  SRpcMsg* rpcMsgCopy = calloc(1, sizeof(SRpcMsg));
  memcpy(rpcMsgCopy, rpcMsg, sizeof(struct SRpcMsg));
  schedMsg.ahandle = (void*)rpcMsgCopy;

  SRpcEpSet* pEpSetCopy = NULL;
  if (pEpSet != NULL) {
    pEpSetCopy = calloc(1, sizeof(SRpcEpSet));
    memcpy(pEpSetCopy, pEpSet, sizeof(SRpcEpSet));
  }

  schedMsg.thandle = (void*)pEpSetCopy;
  schedMsg.msg = NULL;

  taosScheduleTask(tscQhandle, &schedMsg);

  int64_t et = taosGetTimestampUs();
  if (et - st > 100) {
    tscDebug("add message to task queue, elapsed time:%"PRId64, et - st);
  }
}

int doBuildAndSendMsg(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pCmd->command == TSDB_SQL_SELECT ||
      pCmd->command == TSDB_SQL_FETCH ||
      pCmd->command == TSDB_SQL_RETRIEVE ||
      pCmd->command == TSDB_SQL_INSERT ||
      pCmd->command == TSDB_SQL_CONNECT ||
      pCmd->command == TSDB_SQL_HB ||
      pCmd->command == TSDB_SQL_RETRIEVE_FUNC ||
      pCmd->command == TSDB_SQL_STABLEVGROUP) {
    pRes->code = tscBuildMsg[pCmd->command](pSql, NULL);
  }
  
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tscSendMsgToServer(pSql);

  // NOTE: if code is TSDB_CODE_SUCCESS, pSql may have been released here already by other threads.
  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    tscAsyncResultOnError(pSql);
    return  TSDB_CODE_SUCCESS;
  }
  
  return TSDB_CODE_SUCCESS;
}

int tscBuildAndSendRequest(SSqlObj *pSql, SQueryInfo* pQueryInfo) {
  char name[TSDB_TABLE_FNAME_LEN] = {0};

  SSqlCmd *pCmd = &pSql->cmd;
  uint32_t type = 0;

  if (pQueryInfo == NULL) {
     pQueryInfo = tscGetQueryInfo(pCmd);
  }

  STableMetaInfo *pTableMetaInfo = NULL;

  if (pQueryInfo != NULL) {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    if (pTableMetaInfo != NULL) {
      tNameExtractFullName(&pTableMetaInfo->name, name);
    }

    type = pQueryInfo->type;

    // while numOfTables equals to 0, it must be Heartbeat
    assert((pQueryInfo->numOfTables == 0 && (pQueryInfo->command == TSDB_SQL_HB || pSql->cmd.command == TSDB_SQL_RETRIEVE_FUNC)) || pQueryInfo->numOfTables > 0);
  }

  tscDebug("0x%"PRIx64" SQL cmd:%s will be processed, name:%s, type:%d", pSql->self, sqlCmd[pCmd->command], name, type);
  if (pCmd->command < TSDB_SQL_MGMT) { // the pTableMetaInfo cannot be NULL
    if (pTableMetaInfo == NULL) {
      pSql->res.code = TSDB_CODE_TSC_APP_ERROR;
      return pSql->res.code;
    }
  } else if (pCmd->command >= TSDB_SQL_LOCAL) {
    return (*tscProcessMsgRsp[pCmd->command])(pSql);
  }
  
  return doBuildAndSendMsg(pSql);
}

int tscBuildFetchMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg *) pSql->cmd.payload;

  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);

  pRetrieveMsg->free = htons(pQueryInfo->type);
  pRetrieveMsg->qId  = htobe64(pSql->res.qId);

  // todo valid the vgroupId at the client side
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    int32_t vgIndex = pTableMetaInfo->vgroupIndex;
    int32_t vgId = -1;

    if (pTableMetaInfo->pVgroupTables == NULL) {
      SVgroupsInfo *pVgroupInfo = pTableMetaInfo->vgroupList;
      assert(pVgroupInfo->vgroups[vgIndex].vgId > 0 && vgIndex < pTableMetaInfo->vgroupList->numOfVgroups);
      vgId = pVgroupInfo->vgroups[vgIndex].vgId;
    } else {
      int32_t numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
      assert(vgIndex >= 0 && vgIndex < numOfVgroups);

      SVgroupTableInfo* pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, vgIndex);
      vgId = pTableIdList->vgInfo.vgId;
    }

    pRetrieveMsg->header.vgId = htonl(vgId);
    tscDebug("0x%"PRIx64" build fetch msg from vgId:%d, vgIndex:%d, qId:0x%" PRIx64, pSql->self, vgId, vgIndex, pSql->res.qId);
  } else {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    pRetrieveMsg->header.vgId = htonl(pTableMeta->vgId);
    tscDebug("0x%"PRIx64" build fetch msg from only one vgroup, vgId:%d, qId:0x%" PRIx64, pSql->self, pTableMeta->vgId,
        pSql->res.qId);
  }

  pSql->cmd.payloadLen = sizeof(SRetrieveTableMsg);
  pSql->cmd.msgType = TSDB_MSG_TYPE_FETCH;

  pRetrieveMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  return TSDB_CODE_SUCCESS;
}

int tscBuildSubmitMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SQueryInfo *pQueryInfo = tscGetQueryInfo(&pSql->cmd);
  STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;

  // pSql->cmd.payloadLen is set during copying data into payload
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;

  SNewVgroupInfo vgroupInfo = {0};
  taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo);
  tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);

  tscDebug("0x%"PRIx64" submit msg built, numberOfEP:%d", pSql->self, pSql->epSet.numOfEps);

  return TSDB_CODE_SUCCESS;
}

/*
 * for table query, simply return the size <= 1k
 */
static int32_t tscEstimateQueryMsgSize(SSqlObj *pSql) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;

  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

  int32_t srcColListSize = (int32_t)(taosArrayGetSize(pQueryInfo->colList) * sizeof(SColumnInfo));
  int32_t srcColFilterSize = 0;
  int32_t srcTagFilterSize = tscGetTagFilterSerializeLen(pQueryInfo);

  size_t  numOfExprs = tscNumOfExprs(pQueryInfo);
  int32_t exprSize = (int32_t)(sizeof(SSqlExpr) * numOfExprs * 2);

  int32_t tsBufSize = (pQueryInfo->tsBuf != NULL) ? pQueryInfo->tsBuf->fileSize : 0;
  int32_t sqlLen = (int32_t) strlen(pSql->sqlstr) + 1;

  int32_t tableSerialize = 0;
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;  
  if (pTableMetaInfo->pVgroupTables != NULL) {
    size_t numOfGroups = taosArrayGetSize(pTableMetaInfo->pVgroupTables);

    int32_t totalTables = 0;
    for (int32_t i = 0; i < numOfGroups; ++i) {
      SVgroupTableInfo *pTableInfo = taosArrayGet(pTableMetaInfo->pVgroupTables, i);
      totalTables += (int32_t) taosArrayGetSize(pTableInfo->itemList);
    }

    tableSerialize = totalTables * sizeof(STableIdInfo);
  }

  if (pQueryInfo->colCond && taosArrayGetSize(pQueryInfo->colCond) > 0) {
    STblCond *pCond = tsGetTableFilter(pQueryInfo->colCond, pTableMeta->id.uid, 0);
    if (pCond != NULL && pCond->cond != NULL) {
      srcColFilterSize = pCond->len;
    }
  }

  return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryTableMsg) + srcColListSize + srcColFilterSize + srcTagFilterSize + exprSize + tsBufSize +
         tableSerialize + sqlLen + 4096 + pQueryInfo->bufLen;
}

static char *doSerializeTableInfo(SQueryTableMsg *pQueryMsg, SSqlObj *pSql, STableMetaInfo *pTableMetaInfo, char *pMsg,
                                  int32_t *succeed) {
  TSKEY dfltKey = htobe64(pQueryMsg->window.skey);

  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo) || pTableMetaInfo->pVgroupTables == NULL) {
    
    int32_t vgId = -1;
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      int32_t index = pTableMetaInfo->vgroupIndex;
      assert(index >= 0);

      SVgroupInfo* pVgroupInfo = NULL;
      if (pTableMetaInfo->vgroupList && pTableMetaInfo->vgroupList->numOfVgroups > 0) {
        assert(index < pTableMetaInfo->vgroupList->numOfVgroups);
        pVgroupInfo = &pTableMetaInfo->vgroupList->vgroups[index];
      } else {
        tscError("0x%"PRIx64" No vgroup info found", pSql->self);
        
        *succeed = 0;
        return pMsg;
      }

      vgId = pVgroupInfo->vgId;
      tscSetDnodeEpSet(&pSql->epSet, pVgroupInfo);
      tscDebug("0x%"PRIx64" query on stable, vgIndex:%d, numOfVgroups:%d", pSql->self, index, pTableMetaInfo->vgroupList->numOfVgroups);
    } else {
      vgId = pTableMeta->vgId;

      SNewVgroupInfo vgroupInfo = {0};
      taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo);
      tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);
    }

    if (pSql->epSet.numOfEps > 0){
      pSql->epSet.inUse = rand()%pSql->epSet.numOfEps;
    }
    pQueryMsg->head.vgId = htonl(vgId);

    STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
    pTableIdInfo->tid = htonl(pTableMeta->id.tid);
    pTableIdInfo->uid = htobe64(pTableMeta->id.uid);
    pTableIdInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pTableMeta->id.uid, dfltKey));

    pQueryMsg->numOfTables = htonl(1);  // set the number of tables
    pMsg += sizeof(STableIdInfo);
  } else { // it is a subquery of the super table query, this EP info is acquired from vgroupInfo
    int32_t index = pTableMetaInfo->vgroupIndex;
    int32_t numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
    assert(index >= 0 && index < numOfVgroups);

    SVgroupTableInfo* pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, index);

    // set the vgroup info 
    tscSetDnodeEpSet(&pSql->epSet, &pTableIdList->vgInfo);
    pQueryMsg->head.vgId = htonl(pTableIdList->vgInfo.vgId);
    
    int32_t numOfTables = (int32_t)taosArrayGetSize(pTableIdList->itemList);
    pQueryMsg->numOfTables = htonl(numOfTables);  // set the number of tables

    tscDebug("0x%"PRIx64" query on stable, vgId:%d, numOfTables:%d, vgIndex:%d, numOfVgroups:%d", pSql->self,
             pTableIdList->vgInfo.vgId, numOfTables, index, numOfVgroups);

    // serialize each table id info
    for(int32_t i = 0; i < numOfTables; ++i) {
      STableIdInfo* pItem = taosArrayGet(pTableIdList->itemList, i);
      
      STableIdInfo *pTableIdInfo = (STableIdInfo *)pMsg;
      pTableIdInfo->tid = htonl(pItem->tid);
      pTableIdInfo->uid = htobe64(pItem->uid);
      pTableIdInfo->key = htobe64(tscGetSubscriptionProgress(pSql->pSubscription, pItem->uid, dfltKey));
      pMsg += sizeof(STableIdInfo);
    }
  }

  char n[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, n);

  tscDebug("0x%"PRIx64" vgId:%d, query on table:%s, tid:%d, uid:%" PRIu64, pSql->self, htonl(pQueryMsg->head.vgId), n, pTableMeta->id.tid, pTableMeta->id.uid);
  return pMsg;
}

// TODO refactor
static int32_t serializeColFilterInfo(SColumnFilterInfo* pColFilters, int16_t numOfFilters, char** pMsg) {
  // append the filter information after the basic column information
  for (int32_t f = 0; f < numOfFilters; ++f) {
    SColumnFilterInfo *pColFilter = &pColFilters[f];

    SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)(*pMsg);
    pFilterMsg->filterstr = htons(pColFilter->filterstr);

    (*pMsg) += sizeof(SColumnFilterInfo);

    if (pColFilter->filterstr) {
      pFilterMsg->len = htobe64(pColFilter->len);
      memcpy(*pMsg, (void *)pColFilter->pz, (size_t)(pColFilter->len + 1));
      (*pMsg) += (pColFilter->len + 1);  // append the additional filter binary info
    } else {
      pFilterMsg->lowerBndi = htobe64(pColFilter->lowerBndi);
      pFilterMsg->upperBndi = htobe64(pColFilter->upperBndi);
    }

    pFilterMsg->lowerRelOptr = htons(pColFilter->lowerRelOptr);
    pFilterMsg->upperRelOptr = htons(pColFilter->upperRelOptr);

    if (pColFilter->lowerRelOptr == TSDB_RELATION_INVALID && pColFilter->upperRelOptr == TSDB_RELATION_INVALID) {
      tscError("invalid filter info");
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t serializeSqlExpr(SSqlExpr* pExpr, STableMetaInfo* pTableMetaInfo, char** pMsg, int64_t id, bool validateColumn) {
  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;

  // the queried table has been removed and a new table with the same name has already been created already
  // return error msg
  if (pExpr->uid != pTableMeta->id.uid) {
    tscError("0x%"PRIx64" table has already been destroyed", id);
    return TSDB_CODE_TSC_INVALID_TABLE_NAME;
  }

  if (validateColumn && !tscValidateColumnId(pTableMetaInfo, pExpr->colInfo.colId, pExpr->numOfParams)) {
    tscError("0x%"PRIx64" table schema is not matched with parsed sql", id);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  assert(pExpr->resColId < 0);
  SSqlExpr* pSqlExpr = (SSqlExpr *)(*pMsg);

  SColIndex* pIndex = &pSqlExpr->colInfo;

  pIndex->colId         = htons(pExpr->colInfo.colId);
  pIndex->colIndex      = htons(pExpr->colInfo.colIndex);
  pIndex->flag          = htons(pExpr->colInfo.flag);
  pSqlExpr->uid         = htobe64(pExpr->uid);
  pSqlExpr->colType     = htons(pExpr->colType);
  pSqlExpr->colBytes    = htons(pExpr->colBytes);
  pSqlExpr->resType     = htons(pExpr->resType);
  pSqlExpr->resBytes    = htons(pExpr->resBytes);
  pSqlExpr->interBytes  = htonl(pExpr->interBytes);
  pSqlExpr->functionId  = htons(pExpr->functionId);
  pSqlExpr->numOfParams = htons(pExpr->numOfParams);
  pSqlExpr->resColId    = htons(pExpr->resColId);
  pSqlExpr->flist.numOfFilters = htons(pExpr->flist.numOfFilters);

  (*pMsg) += sizeof(SSqlExpr);
  for (int32_t j = 0; j < pExpr->numOfParams; ++j) { // todo add log
    pSqlExpr->param[j].nType = htons((uint16_t)pExpr->param[j].nType);
    pSqlExpr->param[j].nLen = htons(pExpr->param[j].nLen);

    if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
      memcpy((*pMsg), pExpr->param[j].pz, pExpr->param[j].nLen);
      (*pMsg) += pExpr->param[j].nLen;
    } else {
      pSqlExpr->param[j].i64 = htobe64(pExpr->param[j].i64);
    }
  }

  serializeColFilterInfo(pExpr->flist.filterInfo, pExpr->flist.numOfFilters, pMsg);

  return TSDB_CODE_SUCCESS;
}

int tscBuildQueryMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t size = tscEstimateQueryMsgSize(pSql);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_INVALID_OPERATION;  // todo add test for this
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;

  SQueryAttr query = {{0}};
  tscCreateQueryFromQueryInfo(pQueryInfo, &query, pSql);
  query.vgId = pTableMeta->vgId;

  SArray* tableScanOperator = createTableScanPlan(&query);
  SArray* queryOperator = createExecOperatorPlan(&query);

  SQueryTableMsg *pQueryMsg = (SQueryTableMsg *)pCmd->payload;
  tstrncpy(pQueryMsg->version, version, tListLen(pQueryMsg->version));

  int32_t numOfTags = query.numOfTags;
  int32_t sqlLen = (int32_t) strlen(pSql->sqlstr);

  if (taosArrayGetSize(tableScanOperator) == 0) {
    pQueryMsg->tableScanOperator = htonl(-1);
  } else {
    int32_t* tablescanOp = taosArrayGet(tableScanOperator, 0);
    pQueryMsg->tableScanOperator = htonl(*tablescanOp);
  }

  pQueryMsg->window.skey = htobe64(query.window.skey);
  pQueryMsg->window.ekey = htobe64(query.window.ekey);

  pQueryMsg->order          = htons(query.order.order);
  pQueryMsg->orderColId     = htons(query.order.orderColId);
  pQueryMsg->fillType       = htons(query.fillType);
  pQueryMsg->limit          = htobe64(query.limit.limit);
  pQueryMsg->offset         = htobe64(query.limit.offset);
  pQueryMsg->numOfCols      = htons(query.numOfCols);

  pQueryMsg->interval.interval     = htobe64(query.interval.interval);
  pQueryMsg->interval.sliding      = htobe64(query.interval.sliding);
  pQueryMsg->interval.offset       = htobe64(query.interval.offset);
  pQueryMsg->interval.intervalUnit = query.interval.intervalUnit;
  pQueryMsg->interval.slidingUnit  = query.interval.slidingUnit;
  pQueryMsg->interval.offsetUnit   = query.interval.offsetUnit;

  pQueryMsg->stableQuery      = query.stableQuery;
  pQueryMsg->topBotQuery      = query.topBotQuery;
  pQueryMsg->groupbyColumn    = query.groupbyColumn;
  pQueryMsg->hasTagResults    = query.hasTagResults;
  pQueryMsg->timeWindowInterpo = query.timeWindowInterpo;
  pQueryMsg->queryBlockDist   = query.queryBlockDist;
  pQueryMsg->stabledev        = query.stabledev;
  pQueryMsg->tsCompQuery      = query.tsCompQuery;
  pQueryMsg->simpleAgg        = query.simpleAgg;
  pQueryMsg->pointInterpQuery = query.pointInterpQuery;
  pQueryMsg->needReverseScan  = query.needReverseScan;
  pQueryMsg->stateWindow      = query.stateWindow;

  pQueryMsg->numOfTags        = htonl(numOfTags);
  pQueryMsg->sqlstrLen        = htonl(sqlLen);
  pQueryMsg->sw.gap           = htobe64(query.sw.gap);
  pQueryMsg->sw.primaryColId  = htonl(PRIMARYKEY_TIMESTAMP_COL_INDEX);

  pQueryMsg->secondStageOutput = htonl(query.numOfExpr2);
  pQueryMsg->numOfOutput = htons((int16_t)query.numOfOutput);  // this is the stage one output column number

  pQueryMsg->numOfGroupCols = htons(pQueryInfo->groupbyExpr.numOfGroupCols);
  pQueryMsg->tagNameRelType = htons(pQueryInfo->tagCond.relType);
  pQueryMsg->tbnameCondLen  = htonl(pQueryInfo->tagCond.tbnameCond.len);
  pQueryMsg->queryType      = htonl(pQueryInfo->type);
  pQueryMsg->prevResultLen  = htonl(pQueryInfo->bufLen);

  // set column list ids
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  char *pMsg = (char *)(pQueryMsg->tableCols) + numOfCols * sizeof(SColumnInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfo *pCol = &query.tableCols[i];

    pQueryMsg->tableCols[i].colId = htons(pCol->colId);
    pQueryMsg->tableCols[i].bytes = htons(pCol->bytes);
    pQueryMsg->tableCols[i].type  = htons(pCol->type);
    //pQueryMsg->tableCols[i].flist.numOfFilters = htons(pCol->flist.numOfFilters);
    pQueryMsg->tableCols[i].flist.numOfFilters = 0;

    // append the filter information after the basic column information
    //serializeColFilterInfo(pCol->flist.filterInfo, pCol->flist.numOfFilters, &pMsg);
  }

  if (pQueryInfo->colCond && taosArrayGetSize(pQueryInfo->colCond) > 0 && !onlyQueryTags(&query) ) {
    STblCond *pCond = tsGetTableFilter(pQueryInfo->colCond, pTableMeta->id.uid, 0);
    if (pCond != NULL && pCond->cond != NULL) {
      pQueryMsg->colCondLen = htons(pCond->len);
      memcpy(pMsg, pCond->cond, pCond->len);

      pMsg += pCond->len;
    }
  }

  for (int32_t i = 0; i < query.numOfOutput; ++i) {
    code = serializeSqlExpr(&query.pExpr1[i].base, pTableMetaInfo, &pMsg, pSql->self, true);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  for (int32_t i = 0; i < query.numOfExpr2; ++i) {
    code = serializeSqlExpr(&query.pExpr2[i].base, pTableMetaInfo, &pMsg, pSql->self, false);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  int32_t succeed = 1;

  // serialize the table info (sid, uid, tags)
  pMsg = doSerializeTableInfo(pQueryMsg, pSql, pTableMetaInfo, pMsg, &succeed);
  if (succeed == 0) {
    code = TSDB_CODE_TSC_APP_ERROR;
    goto _end;
  }
  
  SGroupbyExpr *pGroupbyExpr = query.pGroupbyExpr;
  if (pGroupbyExpr != NULL && pGroupbyExpr->numOfGroupCols > 0) {
    pQueryMsg->orderByIdx = htons(pGroupbyExpr->orderIndex);
    pQueryMsg->orderType = htons(pGroupbyExpr->orderType);

    for (int32_t j = 0; j < pGroupbyExpr->numOfGroupCols; ++j) {
      SColIndex* pCol = taosArrayGet(pGroupbyExpr->columnInfo, j);

      *((int16_t *)pMsg) = htons(pCol->colId);
      pMsg += sizeof(pCol->colId);

      *((int16_t *)pMsg) += htons(pCol->colIndex);
      pMsg += sizeof(pCol->colIndex);

      *((int16_t *)pMsg) += htons(pCol->flag);
      pMsg += sizeof(pCol->flag);

      memcpy(pMsg, pCol->name, tListLen(pCol->name));
      pMsg += tListLen(pCol->name);
    }
  }

  if (query.fillType != TSDB_FILL_NONE) {
    for (int32_t i = 0; i < query.numOfOutput; ++i) {
      *((int64_t *)pMsg) = htobe64(query.fillVal[i]);
      pMsg += sizeof(query.fillVal[0]);
    }
  }

  if (query.numOfTags > 0 && query.tagColList != NULL) {
    for (int32_t i = 0; i < query.numOfTags; ++i) {
      SColumnInfo* pTag = &query.tagColList[i];

      SColumnInfo* pTagCol = (SColumnInfo*) pMsg;
      pTagCol->colId = htons(pTag->colId);
      pTagCol->bytes = htons(pTag->bytes);
      pTagCol->type  = htons(pTag->type);
      pTagCol->flist.numOfFilters = 0;

      pMsg += sizeof(SColumnInfo);
    }
  }

  // serialize tag column query condition
  if (pQueryInfo->tagCond.pCond != NULL && taosArrayGetSize(pQueryInfo->tagCond.pCond) > 0) {
    STagCond* pTagCond = &pQueryInfo->tagCond;

    SCond *pCond = tsGetSTableQueryCond(pTagCond, pTableMeta->id.uid);
    if (pCond != NULL && pCond->cond != NULL) {
      pQueryMsg->tagCondLen = htons(pCond->len);
      memcpy(pMsg, pCond->cond, pCond->len);

      pMsg += pCond->len;
    }
  }

  if (pQueryInfo->bufLen > 0) {
    memcpy(pMsg, pQueryInfo->buf, pQueryInfo->bufLen);
    pMsg += pQueryInfo->bufLen;
  }

  SCond* pCond = &pQueryInfo->tagCond.tbnameCond;
  if (pCond->len > 0) {
    strncpy(pMsg, pCond->cond, pCond->len);
    pMsg += pCond->len;
  }

  // compressed ts block
  pQueryMsg->tsBuf.tsOffset = htonl((int32_t)(pMsg - pCmd->payload));

  if (pQueryInfo->tsBuf != NULL) {
    // note: here used the index instead of actual vnode id.
    int32_t vnodeIndex = pTableMetaInfo->vgroupIndex;
    code = dumpFileBlockByGroupId(pQueryInfo->tsBuf, vnodeIndex, pMsg, &pQueryMsg->tsBuf.tsLen, &pQueryMsg->tsBuf.tsNumOfBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }

    pMsg += pQueryMsg->tsBuf.tsLen;

    pQueryMsg->tsBuf.tsOrder = htonl(pQueryInfo->tsBuf->tsOrder);
    pQueryMsg->tsBuf.tsLen   = htonl(pQueryMsg->tsBuf.tsLen);
    pQueryMsg->tsBuf.tsNumOfBlocks = htonl(pQueryMsg->tsBuf.tsNumOfBlocks);
  }

  int32_t numOfOperator = (int32_t) taosArrayGetSize(queryOperator);
  pQueryMsg->numOfOperator = htonl(numOfOperator);
  for(int32_t i = 0; i < numOfOperator; ++i) {
    int32_t *operator = taosArrayGet(queryOperator, i);
    *(int32_t*)pMsg = htonl(*operator);

    pMsg += sizeof(int32_t);
  }

  // support only one udf
  if (pQueryInfo->pUdfInfo != NULL && taosArrayGetSize(pQueryInfo->pUdfInfo) > 0) {
    pQueryMsg->udfContentOffset = htonl((int32_t) (pMsg - pCmd->payload));
    for(int32_t i = 0; i < taosArrayGetSize(pQueryInfo->pUdfInfo); ++i) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, i);
      *(int8_t*) pMsg = pUdfInfo->resType;
      pMsg += sizeof(pUdfInfo->resType);

      *(int16_t*) pMsg = htons(pUdfInfo->resBytes);
      pMsg += sizeof(pUdfInfo->resBytes);

      STR_TO_VARSTR(pMsg, pUdfInfo->name);

      pMsg += varDataTLen(pMsg);

      *(int32_t*) pMsg = htonl(pUdfInfo->funcType);
      pMsg += sizeof(pUdfInfo->funcType);

      *(int32_t*) pMsg = htonl(pUdfInfo->bufSize);
      pMsg += sizeof(pUdfInfo->bufSize);

      pQueryMsg->udfContentLen = htonl(pUdfInfo->contLen);
      memcpy(pMsg, pUdfInfo->content, pUdfInfo->contLen);

      pMsg += pUdfInfo->contLen;
    }
  }

  memcpy(pMsg, pSql->sqlstr, sqlLen);
  pMsg += sqlLen;

  int32_t msgLen = (int32_t)(pMsg - pCmd->payload);

  tscDebug("0x%"PRIx64" msg built success, len:%d bytes", pSql->self, msgLen);
  pCmd->payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_QUERY;

  pQueryMsg->head.contLen = htonl(msgLen);
  assert(msgLen + minMsgSize() <= (int32_t)pCmd->allocSize);

  _end:
  freeQueryAttr(&query);
  taosArrayDestroy(tableScanOperator);
  taosArrayDestroy(queryOperator);
  return code;
}

int32_t tscBuildCreateDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateDbMsg);
  
  pCmd->msgType = (pInfo->pMiscInfo->dbOpt.dbType == TSDB_DB_TYPE_DEFAULT) ? TSDB_MSG_TYPE_CM_CREATE_DB : TSDB_MSG_TYPE_CM_CREATE_TP;

  SCreateDbMsg *pCreateDbMsg = (SCreateDbMsg *)pCmd->payload;

//  assert(pCmd->numOfClause == 1);
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pCreateDbMsg->db);
  assert(code == TSDB_CODE_SUCCESS);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateFuncMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  SCreateFuncMsg *pCreateFuncMsg = (SCreateFuncMsg *)pCmd->payload;

  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_FUNCTION;

  pCmd->payloadLen = sizeof(SCreateFuncMsg) + htonl(pCreateFuncMsg->codeLen);

  return TSDB_CODE_SUCCESS;
}


int32_t tscBuildCreateDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateDnodeMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCreateDnodeMsg *pCreate = (SCreateDnodeMsg *)pCmd->payload;

  SStrToken* t0 = taosArrayGet(pInfo->pMiscInfo->a, 0);
  strncpy(pCreate->ep, t0->z, t0->n);
  
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateAcctMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCreateAcctMsg *pAlterMsg = (SCreateAcctMsg *)pCmd->payload;

  SStrToken *pName = &pInfo->pMiscInfo->user.user;
  SStrToken *pPwd = &pInfo->pMiscInfo->user.passwd;

  strncpy(pAlterMsg->user, pName->z, pName->n);
  strncpy(pAlterMsg->pass, pPwd->z, pPwd->n);

  SCreateAcctInfo *pAcctOpt = &pInfo->pMiscInfo->acctOpt;

  pAlterMsg->cfg.maxUsers = htonl(pAcctOpt->maxUsers);
  pAlterMsg->cfg.maxDbs = htonl(pAcctOpt->maxDbs);
  pAlterMsg->cfg.maxTimeSeries = htonl(pAcctOpt->maxTimeSeries);
  pAlterMsg->cfg.maxStreams = htonl(pAcctOpt->maxStreams);
  pAlterMsg->cfg.maxPointsPerSecond = htonl(pAcctOpt->maxPointsPerSecond);
  pAlterMsg->cfg.maxStorage = htobe64(pAcctOpt->maxStorage);
  pAlterMsg->cfg.maxQueryTime = htobe64(pAcctOpt->maxQueryTime);
  pAlterMsg->cfg.maxConnections = htonl(pAcctOpt->maxConnections);

  if (pAcctOpt->stat.n == 0) {
    pAlterMsg->cfg.accessState = -1;
  } else {
    if (pAcctOpt->stat.z[0] == 'r' && pAcctOpt->stat.n == 1) {
      pAlterMsg->cfg.accessState = TSDB_VN_READ_ACCCESS;
    } else if (pAcctOpt->stat.z[0] == 'w' && pAcctOpt->stat.n == 1) {
      pAlterMsg->cfg.accessState = TSDB_VN_WRITE_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "all", 3) == 0 && pAcctOpt->stat.n == 3) {
      pAlterMsg->cfg.accessState = TSDB_VN_ALL_ACCCESS;
    } else if (strncmp(pAcctOpt->stat.z, "no", 2) == 0 && pAcctOpt->stat.n == 2) {
      pAlterMsg->cfg.accessState = 0;
    }
  }

  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_ACCT;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUserMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateUserMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCreateUserMsg *pAlterMsg = (SCreateUserMsg *)pCmd->payload;

  SUserInfo *pUser = &pInfo->pMiscInfo->user;
  strncpy(pAlterMsg->user, pUser->user.z, pUser->user.n);
  pAlterMsg->flag = (int8_t)pUser->type;

  if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    pAlterMsg->privilege = (char)pCmd->count;
  } else if (pUser->type == TSDB_ALTER_USER_PASSWD) {
    strncpy(pAlterMsg->pass, pUser->passwd.z, pUser->passwd.n);
  } else { // create user password info
    strncpy(pAlterMsg->pass, pUser->passwd.z, pUser->passwd.n);
  }

  if (pUser->type == TSDB_ALTER_USER_PASSWD || pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
    pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_USER;
  } else {
    pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_USER;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCfgDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCfgDnodeMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_CONFIG_DNODE;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SDropDbMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SDropDbMsg *pDropDbMsg = (SDropDbMsg*)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);

  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pDropDbMsg->db);
  assert(code == TSDB_CODE_SUCCESS && pTableMetaInfo->name.type == TSDB_DB_NAME_T);

  pDropDbMsg->ignoreNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;

  pCmd->msgType = (pInfo->pMiscInfo->dbType == TSDB_DB_TYPE_DEFAULT) ? TSDB_MSG_TYPE_CM_DROP_DB : TSDB_MSG_TYPE_CM_DROP_TP;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropFuncMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_FUNCTION;

  pCmd->payloadLen = sizeof(SDropFuncMsg);

  return TSDB_CODE_SUCCESS;
}


int32_t tscBuildDropTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCMDropTableMsg *pDropTableMsg = (SCMDropTableMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  tNameExtractFullName(&pTableMetaInfo->name, pDropTableMsg->name);

  pDropTableMsg->supertable = (pInfo->pMiscInfo->tableType == TSDB_SUPER_TABLE)? 1:0;
  pDropTableMsg->igNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;
  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_TABLE;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  char dnodeEp[TSDB_EP_LEN] = {0};
  tstrncpy(dnodeEp, pCmd->payload, TSDB_EP_LEN);

  pCmd->payloadLen = sizeof(SDropDnodeMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SDropDnodeMsg * pDrop = (SDropDnodeMsg *)pCmd->payload;
  tstrncpy(pDrop->ep, dnodeEp, tListLen(pDrop->ep));
  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_DNODE;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropUserAcctMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  char user[TSDB_USER_LEN] = {0};
  tstrncpy(user, pCmd->payload, TSDB_USER_LEN);

  pCmd->payloadLen = sizeof(SDropUserMsg);
  pCmd->msgType = (pInfo->type == TSDB_SQL_DROP_USER)? TSDB_MSG_TYPE_CM_DROP_USER:TSDB_MSG_TYPE_CM_DROP_ACCT;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SDropUserMsg *pDropMsg = (SDropUserMsg *)pCmd->payload;
  tstrncpy(pDropMsg->user, user, tListLen(user));

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildUseDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SUseDbMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SUseDbMsg *pUseDbMsg = (SUseDbMsg *)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  tNameExtractFullName(&pTableMetaInfo->name, pUseDbMsg->db);
  pCmd->msgType = TSDB_MSG_TYPE_CM_USE_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildSyncDbReplicaMsg(SSqlObj* pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SSyncDbMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SSyncDbMsg *pSyncMsg = (SSyncDbMsg *)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  tNameExtractFullName(&pTableMetaInfo->name, pSyncMsg->db);
  pCmd->msgType = TSDB_MSG_TYPE_CM_SYNC_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildShowMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_SHOW;
  pCmd->payloadLen = sizeof(SShowMsg) + 100;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SShowInfo *pShowInfo = &pInfo->pMiscInfo->showOpt;
  SShowMsg  *pShowMsg = (SShowMsg *)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  if (pShowInfo->showType == TSDB_MGMT_TABLE_FUNCTION) {
    pShowMsg->type = pShowInfo->showType;
    pShowMsg->payloadLen = 0;
    pCmd->payloadLen = sizeof(SShowMsg);

    return TSDB_CODE_SUCCESS;
  }

  if (tNameIsEmpty(&pTableMetaInfo->name)) {
    pthread_mutex_lock(&pObj->mutex);
    tstrncpy(pShowMsg->db, pObj->db, sizeof(pShowMsg->db));  
    pthread_mutex_unlock(&pObj->mutex);
  } else {
    tNameGetFullDbName(&pTableMetaInfo->name, pShowMsg->db);
  }

  pShowMsg->type = pShowInfo->showType;

  if (pShowInfo->showType != TSDB_MGMT_TABLE_VNODES) {
    SStrToken *pPattern = &pShowInfo->pattern;
    if (pPattern->type > 0) {  // only show tables support wildcard query
      strncpy(pShowMsg->payload, pPattern->z, pPattern->n);
      pShowMsg->payloadLen = htons(pPattern->n);
    }
  } else {
    SStrToken *pEpAddr = &pShowInfo->prefix;
    assert(pEpAddr->n > 0 && pEpAddr->type > 0);

    strncpy(pShowMsg->payload, pEpAddr->z, pEpAddr->n);
    pShowMsg->payloadLen = htons(pEpAddr->n);
  }

  pCmd->payloadLen = sizeof(SShowMsg) + htons(pShowMsg->payloadLen);
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildKillMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SKillQueryMsg);

  switch (pCmd->command) {
    case TSDB_SQL_KILL_QUERY:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_QUERY;
      break;
    case TSDB_SQL_KILL_CONNECTION:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_CONN;
      break;
    case TSDB_SQL_KILL_STREAM:
      pCmd->msgType = TSDB_MSG_TYPE_CM_KILL_STREAM;
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int tscEstimateCreateTableMsgLength(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &(pSql->cmd);
  int32_t size = minMsgSize() + sizeof(SCMCreateTableMsg) + sizeof(SCreateTableMsg);

  SCreateTableSql *pCreateTableInfo = pInfo->pCreateTableInfo;
  if (pCreateTableInfo->type == TSQL_CREATE_TABLE_FROM_STABLE) {
    int32_t numOfTables = (int32_t)taosArrayGetSize(pInfo->pCreateTableInfo->childTableInfo);
    size += numOfTables * (sizeof(SCreateTableMsg) + TSDB_MAX_TAGS_LEN);
  } else {
    size += sizeof(SSchema) * (pCmd->numOfCols + pCmd->count);
  }

  if (pCreateTableInfo->pSelect != NULL) {
    size += (pCreateTableInfo->pSelect->sqlstr.n + 1);
  }

  return size + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildCreateTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  int      msgLen = 0;
  int      size = 0;
  SSchema *pSchema;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo     *pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql, pInfo);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("0x%"PRIx64" failed to malloc for create table msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCMCreateTableMsg *pCreateTableMsg = (SCMCreateTableMsg *)pCmd->payload;

  SCreateTableMsg* pCreateMsg = (SCreateTableMsg*)((char*) pCreateTableMsg + sizeof(SCMCreateTableMsg));
  char* pMsg = NULL;

  int8_t type = pInfo->pCreateTableInfo->type;
  if (type == TSQL_CREATE_TABLE_FROM_STABLE) {  // create by using super table, tags value
    SArray* list = pInfo->pCreateTableInfo->childTableInfo;

    int32_t numOfTables = (int32_t) taosArrayGetSize(list);
    pCreateTableMsg->numOfTables = htonl(numOfTables);

    pMsg = (char*) pCreateMsg;
    for(int32_t i = 0; i < numOfTables; ++i) {
      SCreateTableMsg* pCreate = (SCreateTableMsg*) pMsg;

      pCreate->numOfColumns = htons(pCmd->numOfCols);
      pCreate->numOfTags = htons(pCmd->count);
      pMsg += sizeof(SCreateTableMsg);

      SCreatedTableInfo* p = taosArrayGet(list, i);
      strcpy(pCreate->tableName, p->fullname);
      pCreate->igExists = (p->igExist)? 1 : 0;

      // use dbinfo from table id without modifying current db info
      pMsg = serializeTagData(&p->tagdata, pMsg);

      int32_t len = (int32_t)(pMsg - (char*) pCreate);
      pCreate->len = htonl(len);
    }
  } else {  // create (super) table
    pCreateTableMsg->numOfTables = htonl(1); // only one table will be created

    int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pCreateMsg->tableName);
    assert(code == 0);

    SCreateTableSql *pCreateTable = pInfo->pCreateTableInfo;

    pCreateMsg->igExists = pCreateTable->existCheck ? 1 : 0;
    pCreateMsg->numOfColumns = htons(pCmd->numOfCols);
    pCreateMsg->numOfTags = htons(pCmd->count);

    pCreateMsg->sqlLen = 0;
    pMsg = (char *)pCreateMsg->schema;

    pSchema = (SSchema *)pCreateMsg->schema;

    for (int i = 0; i < pCmd->numOfCols + pCmd->count; ++i) {
      TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);

      pSchema->type = pField->type;
      strcpy(pSchema->name, pField->name);
      pSchema->bytes = htons(pField->bytes);

      pSchema++;
    }

    pMsg = (char *)pSchema;
    if (type == TSQL_CREATE_STREAM) {  // check if it is a stream sql
      SSqlNode *pQuerySql = pInfo->pCreateTableInfo->pSelect;

      strncpy(pMsg, pQuerySql->sqlstr.z, pQuerySql->sqlstr.n + 1);
      pCreateMsg->sqlLen = htons(pQuerySql->sqlstr.n + 1);
      pMsg += pQuerySql->sqlstr.n + 1;
    }
  }

  tscFieldInfoClear(&pQueryInfo->fieldsInfo);

  msgLen = (int32_t)(pMsg - (char*)pCreateTableMsg);
  pCreateTableMsg->contLen = htonl(msgLen);
  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;

  assert(msgLen + minMsgSize() <= size);
  return TSDB_CODE_SUCCESS;
}

int tscEstimateAlterTableMsgLength(SSqlCmd *pCmd) {
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  return minMsgSize() + sizeof(SAlterTableMsg) + sizeof(SSchema) * tscNumOfFields(pQueryInfo) + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildAlterTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *pMsg;
  int   msgLen = 0;

  SSqlCmd    *pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  SAlterTableInfo *pAlterInfo = pInfo->pAlterInfo;
  int size = tscEstimateAlterTableMsgLength(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("0x%"PRIx64" failed to malloc for alter table msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  SAlterTableMsg *pAlterTableMsg = (SAlterTableMsg *)pCmd->payload;

  tNameExtractFullName(&pTableMetaInfo->name, pAlterTableMsg->tableFname);
  pAlterTableMsg->type = htons(pAlterInfo->type);

  pAlterTableMsg->numOfCols = htons(tscNumOfFields(pQueryInfo));
  SSchema *pSchema = pAlterTableMsg->schema;
  for (int i = 0; i < tscNumOfFields(pQueryInfo); ++i) {
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, i);
  
    pSchema->type = pField->type;
    strcpy(pSchema->name, pField->name);
    pSchema->bytes = htons(pField->bytes);
    pSchema++;
  }

  pMsg = (char *)pSchema;
  pAlterTableMsg->tagValLen = htonl(pAlterInfo->tagData.dataLen);
  if (pAlterInfo->tagData.dataLen > 0) {
 	 memcpy(pMsg, pAlterInfo->tagData.data, pAlterInfo->tagData.dataLen);
  }
  pMsg += pAlterInfo->tagData.dataLen;

  msgLen = (int32_t)(pMsg - (char*)pAlterTableMsg);

  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_TABLE;

  assert(msgLen + minMsgSize() <= size);

  return TSDB_CODE_SUCCESS;
}

int tscBuildUpdateTagMsg(SSqlObj* pSql, SSqlInfo *pInfo) {
  SSqlCmd* pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_UPDATE_TAG_VAL;
  
  SUpdateTableTagValMsg* pUpdateMsg = (SUpdateTableTagValMsg*) pCmd->payload;
  pCmd->payloadLen = htonl(pUpdateMsg->head.contLen);

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  STableMeta *pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;

  SNewVgroupInfo vgroupInfo = {.vgId = -1};
  taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo);
  assert(vgroupInfo.vgId > 0);

  tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);

  return TSDB_CODE_SUCCESS;
}

int tscAlterDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SAlterDbMsg);
  pCmd->msgType = (pInfo->pMiscInfo->dbOpt.dbType == TSDB_DB_TYPE_DEFAULT) ? TSDB_MSG_TYPE_CM_ALTER_DB : TSDB_MSG_TYPE_CM_ALTER_TP;

  SAlterDbMsg *pAlterDbMsg = (SAlterDbMsg* )pCmd->payload;
  pAlterDbMsg->dbType = -1;
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);
  tNameExtractFullName(&pTableMetaInfo->name, pAlterDbMsg->db);

  return TSDB_CODE_SUCCESS;
}
int tscBuildCompactMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  if (pInfo->list == NULL || taosArrayGetSize(pInfo->list) <= 0) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  SArray *pList = pInfo->list;
  int32_t size  = (int32_t)taosArrayGetSize(pList);

  int32_t *result = malloc(sizeof(int32_t) * size);
  if (result == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  for (int32_t i = 0; i < size; i++) {
    tSqlExprItem* pSub = taosArrayGet(pList, i);
    tVariant* pVar = &pSub->pNode->value;
    if (pVar->nType >= TSDB_DATA_TYPE_TINYINT && pVar->nType <= TSDB_DATA_TYPE_BIGINT) {
      result[i] = (int32_t)(pVar->i64); 
    } else { 
      free(result);
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  int count = removeDupVgid(result, size);
  pCmd->payloadLen = sizeof(SCompactMsg) + count * sizeof(int32_t);
  pCmd->msgType = TSDB_MSG_TYPE_CM_COMPACT_VNODE;  

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    free(result);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  SCompactMsg *pCompactMsg = (SCompactMsg *)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, 0);
  
  if (tNameIsEmpty(&pTableMetaInfo->name)) {    
    pthread_mutex_lock(&pObj->mutex);
    tstrncpy(pCompactMsg->db, pObj->db, sizeof(pCompactMsg->db));  
    pthread_mutex_unlock(&pObj->mutex);
  } else {
    tNameGetFullDbName(&pTableMetaInfo->name, pCompactMsg->db);
  } 
 
  pCompactMsg->numOfVgroup = htons(count);
  for (int32_t i = 0; i < count; i++) {
    pCompactMsg->vgid[i] = htons(result[i]);   
  } 
  free(result);

  return TSDB_CODE_SUCCESS;
}

int tscBuildRetrieveFromMgmtMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_RETRIEVE;
  pCmd->payloadLen = sizeof(SRetrieveTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg*)pCmd->payload;
  pRetrieveMsg->qId  = htobe64(pSql->res.qId);
  pRetrieveMsg->free = htons(pQueryInfo->type);

  return TSDB_CODE_SUCCESS;
}

/*
 * this function can only be called once.
 * by using pRes->rspType to denote its status
 *
 * if pRes->rspType is 1, no more result
 */
static int tscLocalResultCommonBuilder(SSqlObj *pSql, int32_t numOfRes) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pRes->code = TSDB_CODE_SUCCESS;
  if (pRes->rspType == 0) {
    pRes->numOfRows = numOfRes;
    pRes->row = 0;
    pRes->rspType = 1;

    SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
    if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
      return pRes->code;
    }

    tscSetResRawPtr(pRes, pQueryInfo);
  } else {
    tscResetForNextRetrieve(pRes);
  }

  uint8_t code = pSql->res.code;
  if (pSql->fp) {
    if (code == TSDB_CODE_SUCCESS) {
      (*pSql->fp)(pSql->param, pSql, pSql->res.numOfRows);
    } else {
      tscAsyncResultOnError(pSql);
    }
  }

  return code;
}

int tscProcessDescribeTableRsp(SSqlObj *pSql) {
  SSqlCmd *       pCmd = &pSql->cmd;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd,  0);

  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int32_t numOfRes = tinfo.numOfColumns + tinfo.numOfTags;
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessLocalRetrieveRsp(SSqlObj *pSql) {
  int32_t numOfRes = 1;
  pSql->res.completed = true;
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessRetrieveGlobalMergeRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd* pCmd = &pSql->cmd;

  int32_t code = pRes->code;
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return code;
  }

  if (pRes->pMerger == NULL) { // no result from subquery, so abort here directly.
    (*pSql->fp)(pSql->param, pSql, pRes->numOfRows);
    return code;
  }

  // global aggregation may be the upstream for parent query
  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);
  if (pQueryInfo->pQInfo == NULL) {
    STableGroupInfo tableGroupInfo = {.numOfTables = 1, .pGroupList = taosArrayInit(1, POINTER_BYTES),};
    tableGroupInfo.map = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

    STableKeyInfo tableKeyInfo = {.pTable = NULL, .lastKey = INT64_MIN};

    SArray* group = taosArrayInit(1, sizeof(STableKeyInfo));
    taosArrayPush(group, &tableKeyInfo);
    taosArrayPush(tableGroupInfo.pGroupList, &group);

    tscDebug("0x%"PRIx64" create QInfo 0x%"PRIx64" to execute query processing", pSql->self, pSql->self);
    pQueryInfo->pQInfo = createQInfoFromQueryNode(pQueryInfo, &tableGroupInfo, NULL, NULL, pRes->pMerger, MERGE_STAGE, pSql->self);
  }

  uint64_t localQueryId = pSql->self;
  qTableQuery(pQueryInfo->pQInfo, &localQueryId);
  convertQueryResult(pRes, pQueryInfo, pSql->self, true);

  code = pRes->code;
  if (pRes->code == TSDB_CODE_SUCCESS) {
    (*pSql->fp)(pSql->param, pSql, pRes->numOfRows);
  } else {
    tscAsyncResultOnError(pSql);
  }

  return code;
}

int tscProcessEmptyResultRsp(SSqlObj *pSql) { return tscLocalResultCommonBuilder(pSql, 0); }

int tscBuildConnectMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_CONNECT;
  pCmd->payloadLen = sizeof(SConnectMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("0x%"PRIx64" failed to malloc for query msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SConnectMsg *pConnect = (SConnectMsg*)pCmd->payload;

  // TODO refactor full_name
  char *db;  // ugly code to move the space
  
  pthread_mutex_lock(&pObj->mutex);
  db = strstr(pObj->db, TS_PATH_DELIMITER);

  db = (db == NULL) ? pObj->db : db + 1;
  tstrncpy(pConnect->db, db, sizeof(pConnect->db));
  pthread_mutex_unlock(&pObj->mutex);

  tstrncpy(pConnect->clientVersion, version, sizeof(pConnect->clientVersion));
  tstrncpy(pConnect->msgVersion, "", sizeof(pConnect->msgVersion));

  pConnect->pid = htonl(taosGetPId());
  taosGetCurrentAPPName(pConnect->appName, NULL);

  return TSDB_CODE_SUCCESS;
}

/**
 *  multi table meta req pkg format:
 *  |SMultiTableInfoMsg | tableId0 | tableId1 | tableId2 | ......
 *      4B
 **/
int tscBuildMultiTableMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLES_META;
  assert(pCmd->payloadLen + minMsgSize() <= pCmd->allocSize);

  tscDebug("0x%"PRIx64" build load multi-tablemeta msg completed, numOfTables:%d, msg size:%d", pSql->self, pCmd->count,
           pCmd->payloadLen);

  return pCmd->payloadLen;
}

int tscBuildSTableVgroupMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  
  char* pMsg = pCmd->payload;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  SSTableVgroupMsg *pStableVgroupMsg = (SSTableVgroupMsg *)pMsg;
  pStableVgroupMsg->numOfTables = htonl(pQueryInfo->numOfTables);
  pMsg += sizeof(SSTableVgroupMsg);

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, i);
    int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pMsg);
    assert(code == TSDB_CODE_SUCCESS);

    pMsg += TSDB_TABLE_FNAME_LEN;
  }

  pCmd->msgType = TSDB_MSG_TYPE_CM_STABLE_VGROUP;
  pCmd->payloadLen = (int32_t)(pMsg - pCmd->payload);

  return TSDB_CODE_SUCCESS;
}

int tscBuildRetrieveFuncMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  char *pMsg = pCmd->payload;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  int32_t numOfFuncs = (int32_t)taosArrayGetSize(pQueryInfo->pUdfInfo);

  SRetrieveFuncMsg *pRetrieveFuncMsg = (SRetrieveFuncMsg *)pMsg;
  pRetrieveFuncMsg->num = htonl(numOfFuncs);

  pMsg += sizeof(SRetrieveFuncMsg);
  for(int32_t i = 0; i < numOfFuncs; ++i) {
    SUdfInfo* pUdf = taosArrayGet(pQueryInfo->pUdfInfo, i);
    STR_TO_NET_VARSTR(pMsg, pUdf->name);
    pMsg += varDataNetTLen(pMsg);
  }

  pCmd->msgType = TSDB_MSG_TYPE_CM_RETRIEVE_FUNC;
  pCmd->payloadLen = (int32_t)(pMsg - pCmd->payload);

  return TSDB_CODE_SUCCESS;
}

int tscBuildHeartBeatMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  STscObj *pObj = pSql->pTscObj;

  pthread_mutex_lock(&pObj->mutex);

  int32_t numOfQueries = 2;
  SSqlObj *tpSql = pObj->sqlList;
  while (tpSql) {
    tpSql = tpSql->next;
    numOfQueries++;
  }

  int32_t numOfStreams = 2;
  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    pStream = pStream->next;
    numOfStreams++;
  }

  int size = numOfQueries * sizeof(SQueryDesc) + numOfStreams * sizeof(SStreamDesc) + sizeof(SHeartBeatMsg) + 100;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    pthread_mutex_unlock(&pObj->mutex);
    tscError("0x%"PRIx64" failed to create heartbeat msg", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  // TODO the expired hb and client can not be identified by server till now.
  SHeartBeatMsg *pHeartbeat = (SHeartBeatMsg *)pCmd->payload;
  tstrncpy(pHeartbeat->clientVer, version, tListLen(pHeartbeat->clientVer));

  pHeartbeat->numOfQueries = numOfQueries;
  pHeartbeat->numOfStreams = numOfStreams;

  pHeartbeat->pid = htonl(taosGetPId());
  taosGetCurrentAPPName(pHeartbeat->appName, NULL);

  int msgLen = tscBuildQueryStreamDesc(pHeartbeat, pObj);

  pthread_mutex_unlock(&pObj->mutex);

  pCmd->payloadLen = msgLen;
  pCmd->msgType = TSDB_MSG_TYPE_CM_HEARTBEAT;

  return TSDB_CODE_SUCCESS;
}

static int32_t tableMetaMsgConvert(STableMetaMsg* pMetaMsg) {
  pMetaMsg->tid = htonl(pMetaMsg->tid);
  pMetaMsg->sversion = htons(pMetaMsg->sversion);
  pMetaMsg->tversion = htons(pMetaMsg->tversion);
  pMetaMsg->vgroup.vgId = htonl(pMetaMsg->vgroup.vgId);

  pMetaMsg->uid = htobe64(pMetaMsg->uid);
  pMetaMsg->numOfColumns = htons(pMetaMsg->numOfColumns);

  if ((pMetaMsg->tableType != TSDB_SUPER_TABLE) &&
      (pMetaMsg->tid <= 0 || pMetaMsg->vgroup.vgId < 2 || pMetaMsg->vgroup.numOfEps <= 0)) {
    tscError("invalid value in table numOfEps:%d, vgId:%d tid:%d, name:%s", pMetaMsg->vgroup.numOfEps, pMetaMsg->vgroup.vgId,
             pMetaMsg->tid, pMetaMsg->tableFname);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->numOfTags > TSDB_MAX_TAGS) {
    tscError("invalid numOfTags:%d", pMetaMsg->numOfTags);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pMetaMsg->numOfColumns > TSDB_MAX_COLUMNS || pMetaMsg->numOfColumns <= 0) {
    tscError("invalid numOfColumns:%d", pMetaMsg->numOfColumns);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  for (int i = 0; i < pMetaMsg->vgroup.numOfEps; ++i) {
    pMetaMsg->vgroup.epAddr[i].port = htons(pMetaMsg->vgroup.epAddr[i].port);
  }

  SSchema* pSchema = pMetaMsg->schema;

  int32_t numOfTotalCols = pMetaMsg->numOfColumns + pMetaMsg->numOfTags;
  for (int i = 0; i < numOfTotalCols; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);

    if (pSchema->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
      assert(i == 0);
    }

    pSchema++;
  }

  return TSDB_CODE_SUCCESS;
}

// update the vgroupInfo if needed
static void doUpdateVgroupInfo(int32_t vgId, SVgroupMsg *pVgroupMsg) {
  assert(vgId > 0);

  SNewVgroupInfo vgroupInfo = {.inUse = -1};
  taosHashGetClone(tscVgroupMap, &vgId, sizeof(vgId), NULL, &vgroupInfo);

  // vgroup info exists, compare with it
  if (((vgroupInfo.inUse >= 0) && !vgroupInfoIdentical(&vgroupInfo, pVgroupMsg)) || (vgroupInfo.inUse < 0)) {
    vgroupInfo = createNewVgroupInfo(pVgroupMsg);
    taosHashPut(tscVgroupMap, &vgId, sizeof(vgId), &vgroupInfo, sizeof(vgroupInfo));
    tscDebug("add/update new VgroupInfo, vgId:%d, total cached:%d", vgId, (int32_t) taosHashGetSize(tscVgroupMap));
  }
}

static void doAddTableMetaToLocalBuf(STableMeta* pTableMeta, STableMetaMsg* pMetaMsg, bool updateSTable) {
  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
    // add or update the corresponding super table meta data info
    int32_t len = (int32_t) strnlen(pTableMeta->sTableName, TSDB_TABLE_FNAME_LEN);

    // The super tableMeta already exists, create it according to tableMeta and add it to hash map
    if (updateSTable) {
      STableMeta* pSupTableMeta = createSuperTableMeta(pMetaMsg);
      uint32_t size = tscGetTableMetaSize(pSupTableMeta);
      int32_t code = taosHashPut(tscTableMetaMap, pTableMeta->sTableName, len, pSupTableMeta, size);
      assert(code == TSDB_CODE_SUCCESS);

      tfree(pSupTableMeta);
    }

    CChildTableMeta* cMeta = tscCreateChildMeta(pTableMeta);
    taosHashPut(tscTableMetaMap, pMetaMsg->tableFname, strlen(pMetaMsg->tableFname), cMeta, sizeof(CChildTableMeta));
    tfree(cMeta);
  } else {
    uint32_t s = tscGetTableMetaSize(pTableMeta);
    taosHashPut(tscTableMetaMap, pMetaMsg->tableFname, strlen(pMetaMsg->tableFname), pTableMeta, s);
  }
}

int tscProcessTableMetaRsp(SSqlObj *pSql) {
  STableMetaMsg *pMetaMsg = (STableMetaMsg *)pSql->res.pRsp;
  int32_t code = tableMetaMsgConvert(pMetaMsg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0);
  assert(pTableMetaInfo->pTableMeta == NULL);

  STableMeta* pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg);
  if (pTableMeta == NULL){
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  if (!tIsValidSchema(pTableMeta->schema, pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.numOfTags)) {
    tscError("0x%"PRIx64" invalid table meta from mnode, name:%s", pSql->self, tNameGetTableName(&pTableMetaInfo->name));
    tfree(pTableMeta);
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);
  assert(strncmp(pMetaMsg->tableFname, name, tListLen(pMetaMsg->tableFname)) == 0);

  doAddTableMetaToLocalBuf(pTableMeta, pMetaMsg, true);
  if (pTableMeta->tableType != TSDB_SUPER_TABLE) {
    doUpdateVgroupInfo(pTableMeta->vgId, &pMetaMsg->vgroup);
  }

  tscDebug("0x%"PRIx64" recv table meta, uid:%" PRIu64 ", tid:%d, name:%s, numOfCols:%d, numOfTags:%d", pSql->self,
      pTableMeta->id.uid, pTableMeta->id.tid, tNameGetTableName(&pTableMetaInfo->name), pTableMeta->tableInfo.numOfColumns,
      pTableMeta->tableInfo.numOfTags);

  free(pTableMeta);
  return TSDB_CODE_SUCCESS;
}

static SArray* createVgroupIdListFromMsg(char* pMsg, SHashObj* pSet, char* name, int32_t* size, uint64_t id) {
  SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)pMsg;

  pVgroupMsg->numOfVgroups = htonl(pVgroupMsg->numOfVgroups);
  *size = (int32_t)(sizeof(SVgroupMsg) * pVgroupMsg->numOfVgroups + sizeof(SVgroupsMsg));

  SArray* vgroupIdList = taosArrayInit(pVgroupMsg->numOfVgroups, sizeof(int32_t));

  if (pVgroupMsg->numOfVgroups <= 0) {
    tscDebug("0x%" PRIx64 " empty vgroup id list, no corresponding tables for stable:%s", id, name);
  } else {
    // just init, no need to lock
    for (int32_t j = 0; j < pVgroupMsg->numOfVgroups; ++j) {
      SVgroupMsg *vmsg = &pVgroupMsg->vgroups[j];
      vmsg->vgId = htonl(vmsg->vgId);
      for (int32_t k = 0; k < vmsg->numOfEps; ++k) {
        vmsg->epAddr[k].port = htons(vmsg->epAddr[k].port);
      }

      taosArrayPush(vgroupIdList, &vmsg->vgId);

      if (taosHashGet(pSet, &vmsg->vgId, sizeof(vmsg->vgId)) == NULL) {
        taosHashPut(pSet, &vmsg->vgId, sizeof(vmsg->vgId), "", 0);
        doUpdateVgroupInfo(vmsg->vgId, vmsg);
      }
    }
  }

  return vgroupIdList;
}

static SVgroupsInfo* createVgroupInfoFromMsg(char* pMsg, int32_t* size, uint64_t id) {
  SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)pMsg;
  pVgroupMsg->numOfVgroups = htonl(pVgroupMsg->numOfVgroups);

  *size = (int32_t)(sizeof(SVgroupMsg) * pVgroupMsg->numOfVgroups + sizeof(SVgroupsMsg));

  size_t        vgroupsz = sizeof(SVgroupInfo) * pVgroupMsg->numOfVgroups + sizeof(SVgroupsInfo);
  SVgroupsInfo *pVgroupInfo = calloc(1, vgroupsz);
  assert(pVgroupInfo != NULL);

  pVgroupInfo->numOfVgroups = pVgroupMsg->numOfVgroups;
  if (pVgroupInfo->numOfVgroups <= 0) {
    tscDebug("0x%" PRIx64 " empty vgroup info, no corresponding tables for stable", id);
  } else {
    for (int32_t j = 0; j < pVgroupInfo->numOfVgroups; ++j) {
      // just init, no need to lock
      SVgroupInfo *pVgroup = &pVgroupInfo->vgroups[j];

      SVgroupMsg *vmsg = &pVgroupMsg->vgroups[j];
      vmsg->vgId = htonl(vmsg->vgId);
      for (int32_t k = 0; k < vmsg->numOfEps; ++k) {
        vmsg->epAddr[k].port = htons(vmsg->epAddr[k].port);
      }

      pVgroup->numOfEps = vmsg->numOfEps;
      pVgroup->vgId = vmsg->vgId;
      for (int32_t k = 0; k < vmsg->numOfEps; ++k) {
        pVgroup->epAddr[k].port = vmsg->epAddr[k].port;
        pVgroup->epAddr[k].fqdn = strndup(vmsg->epAddr[k].fqdn, TSDB_FQDN_LEN);
      }

      doUpdateVgroupInfo(pVgroup->vgId, vmsg);
    }
  }

  return pVgroupInfo;
}

int tscProcessRetrieveFuncRsp(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SUdfFuncMsg* pFuncMsg = (SUdfFuncMsg *)pSql->res.pRsp;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  pFuncMsg->num = htonl(pFuncMsg->num);
  assert(pFuncMsg->num == taosArrayGetSize(pQueryInfo->pUdfInfo));

  char* pMsg = pFuncMsg->content;
  for(int32_t i = 0; i < pFuncMsg->num; ++i) {
    SFunctionInfoMsg* pFunc = (SFunctionInfoMsg*) pMsg;

    for(int32_t j = 0; j < pFuncMsg->num; ++j) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, j);
      if (strcmp(pUdfInfo->name, pFunc->name) != 0) {
        continue;
      }

      if (pUdfInfo->content) {
        continue;
      }

      pUdfInfo->resBytes = htons(pFunc->resBytes);
      pUdfInfo->resType  = pFunc->resType;
      pUdfInfo->funcType = htonl(pFunc->funcType);
      pUdfInfo->contLen  = htonl(pFunc->len);
      pUdfInfo->bufSize  = htonl(pFunc->bufSize);

      pUdfInfo->content = malloc(pUdfInfo->contLen);
      memcpy(pUdfInfo->content, pFunc->content, pUdfInfo->contLen);

      pMsg += sizeof(SFunctionInfoMsg) + pUdfInfo->contLen;
    }
  }

  // master sqlObj locates in param
  SSqlObj* parent = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)pSql->param);
  if(parent == NULL) {
    return pSql->res.code;
  }

  SQueryInfo* parQueryInfo = tscGetQueryInfo(&parent->cmd);

  assert(parent->signature == parent && (int64_t)pSql->param == parent->self);
  taosArrayDestroy(parQueryInfo->pUdfInfo);

  parQueryInfo->pUdfInfo = pQueryInfo->pUdfInfo;   // assigned to parent sql obj.
  pQueryInfo->pUdfInfo = NULL;
  return TSDB_CODE_SUCCESS;
}

int tscProcessMultiTableMetaRsp(SSqlObj *pSql) {
  char *rsp = pSql->res.pRsp;

  SMultiTableMeta *pMultiMeta = (SMultiTableMeta *)rsp;
  pMultiMeta->numOfTables = htonl(pMultiMeta->numOfTables);
  pMultiMeta->numOfVgroup = htonl(pMultiMeta->numOfVgroup);
  pMultiMeta->numOfUdf = htonl(pMultiMeta->numOfUdf);

  rsp += sizeof(SMultiTableMeta);

  SSqlObj* pParentSql = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)pSql->param);
  if(pParentSql == NULL) {
    return pSql->res.code;
  }

  SSqlCmd *pParentCmd = &pParentSql->cmd;
  SHashObj *pSet = taosHashInit(pMultiMeta->numOfVgroup, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);

  char* buf = NULL;
  char* pMsg = pMultiMeta->meta;

  // decompresss the message payload
  if (pMultiMeta->compressed) {
    buf = malloc(pMultiMeta->rawLen - sizeof(SMultiTableMeta));
    int32_t len = tsDecompressString(pMultiMeta->meta, pMultiMeta->contLen - sizeof(SMultiTableMeta), 1,
        buf, pMultiMeta->rawLen - sizeof(SMultiTableMeta), ONE_STAGE_COMP, NULL, 0);
    assert(len == pMultiMeta->rawLen - sizeof(SMultiTableMeta));

    pMsg = buf;
  }

  if (pParentCmd->pTableMetaMap == NULL) {
    pParentCmd->pTableMetaMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  }

  for (int32_t i = 0; i < pMultiMeta->numOfTables; i++) {
    STableMetaMsg *pMetaMsg = (STableMetaMsg *)pMsg;
    int32_t code = tableMetaMsgConvert(pMetaMsg);
    if (code != TSDB_CODE_SUCCESS) {
      taosHashCleanup(pSet);
      taosReleaseRef(tscObjRef, pParentSql->self);

      tfree(buf);
      return code;
    }

    bool freeMeta = false;
    STableMeta* pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg);
    if (!tIsValidSchema(pTableMeta->schema, pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.numOfTags)) {
      tscError("0x%"PRIx64" invalid table meta from mnode, name:%s", pSql->self, pMetaMsg->tableFname);
      tfree(pTableMeta);
      taosHashCleanup(pSet);
      taosReleaseRef(tscObjRef, pParentSql->self);

      tfree(buf);
      return TSDB_CODE_TSC_INVALID_VALUE;
    }

    if (pMultiMeta->metaClone == 1 || pTableMeta->tableType == TSDB_SUPER_TABLE) {
      STableMetaVgroupInfo p = {.pTableMeta = pTableMeta,};
      size_t keyLen = strnlen(pMetaMsg->tableFname, TSDB_TABLE_FNAME_LEN);
      void* t = taosHashGet(pParentCmd->pTableMetaMap, pMetaMsg->tableFname, keyLen);
      assert(t == NULL);

      taosHashPut(pParentCmd->pTableMetaMap, pMetaMsg->tableFname, keyLen, &p, sizeof(STableMetaVgroupInfo));
    } else {
      freeMeta = true;
    }

    // for each super table, only update meta information once
    bool updateStableMeta = false;
    if (pTableMeta->tableType == TSDB_CHILD_TABLE && taosHashGet(pSet, &pMetaMsg->suid, sizeof(pMetaMsg->suid)) == NULL) {
      updateStableMeta = true;
      taosHashPut(pSet, &pTableMeta->suid, sizeof(pMetaMsg->suid), "", 0);
    }

    // create the tableMeta and add it into the TableMeta map
    doAddTableMetaToLocalBuf(pTableMeta, pMetaMsg, updateStableMeta);

    // for each vgroup, only update the information once.
    int64_t vgId = pMetaMsg->vgroup.vgId;
    if (pTableMeta->tableType != TSDB_SUPER_TABLE && taosHashGet(pSet, &vgId, sizeof(vgId)) == NULL) {
      doUpdateVgroupInfo((int32_t) vgId, &pMetaMsg->vgroup);
      taosHashPut(pSet, &vgId, sizeof(vgId), "", 0);
    }

    pMsg += pMetaMsg->contLen;
    if (freeMeta) {
      tfree(pTableMeta);
    }
  }

  for(int32_t i = 0; i < pMultiMeta->numOfVgroup; ++i) {
    char fname[TSDB_TABLE_FNAME_LEN] = {0};
    tstrncpy(fname, pMsg, TSDB_TABLE_FNAME_LEN);
    size_t len = strnlen(fname, TSDB_TABLE_FNAME_LEN);

    pMsg += TSDB_TABLE_FNAME_LEN;

    STableMetaVgroupInfo* p = taosHashGet(pParentCmd->pTableMetaMap, fname, len);
    assert(p != NULL);

    int32_t size = 0;
    if (p->vgroupIdList!= NULL) {
      taosArrayDestroy(p->vgroupIdList);
    }

    p->vgroupIdList = createVgroupIdListFromMsg(pMsg, pSet, fname, &size, pSql->self);

    int32_t numOfVgId = (int32_t) taosArrayGetSize(p->vgroupIdList);
    int32_t s = sizeof(tFilePage) + numOfVgId * sizeof(int32_t);

    tFilePage* idList = calloc(1, s);
    idList->num = numOfVgId;
    memcpy(idList->data, TARRAY_GET_START(p->vgroupIdList), numOfVgId * sizeof(int32_t));

    void* idListInst = taosCachePut(tscVgroupListBuf, fname, len, idList, s, 5000);
    taosCacheRelease(tscVgroupListBuf, (void*) &idListInst, false);

    tfree(idList);
    pMsg += size;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfo(pParentCmd);
  if (pMultiMeta->numOfUdf > 0) {
    assert(pQueryInfo->pUdfInfo != NULL);
  }

  for(int32_t i = 0; i < pMultiMeta->numOfUdf; ++i) {
    SFunctionInfoMsg* pFunc = (SFunctionInfoMsg*) pMsg;

    for(int32_t j = 0; j < pMultiMeta->numOfUdf; ++j) {
      SUdfInfo* pUdfInfo = taosArrayGet(pQueryInfo->pUdfInfo, j);
      if (strcmp(pUdfInfo->name, pFunc->name) != 0) {
        continue;
      }

      if (pUdfInfo->content) {
        continue;
      }

      pUdfInfo->resBytes = htons(pFunc->resBytes);
      pUdfInfo->resType  = pFunc->resType;
      pUdfInfo->funcType = htonl(pFunc->funcType);
      pUdfInfo->contLen  = htonl(pFunc->len);
      pUdfInfo->bufSize  = htonl(pFunc->bufSize);

      pUdfInfo->content = malloc(pUdfInfo->contLen);
      memcpy(pUdfInfo->content, pFunc->content, pUdfInfo->contLen);

      pMsg += sizeof(SFunctionInfoMsg) + pUdfInfo->contLen;
    }
  }

  pSql->res.code = TSDB_CODE_SUCCESS;
  pSql->res.numOfTotal = pMultiMeta->numOfTables;
  tscDebug("0x%"PRIx64" load multi-tableMeta from mnode, numOfTables:%d", pSql->self, pMultiMeta->numOfTables);

  taosHashCleanup(pSet);
  taosReleaseRef(tscObjRef, pParentSql->self);

  tfree(buf);
  return TSDB_CODE_SUCCESS;
}

int tscProcessSTableVgroupRsp(SSqlObj *pSql) {
  // master sqlObj locates in param
  SSqlObj* parent = (SSqlObj*)taosAcquireRef(tscObjRef, (int64_t)pSql->param);
  if(parent == NULL) {
    return pSql->res.code;
  }

  assert(parent->signature == parent && (int64_t)pSql->param == parent->self);

  SSqlRes* pRes = &pSql->res;

  // NOTE: the order of several table must be preserved.
  SSTableVgroupRspMsg *pStableVgroup = (SSTableVgroupRspMsg *)pRes->pRsp;
  pStableVgroup->numOfTables = htonl(pStableVgroup->numOfTables);
  char *pMsg = pRes->pRsp + sizeof(SSTableVgroupRspMsg);

  SSqlCmd* pCmd = &parent->cmd;
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);

  char fName[TSDB_TABLE_FNAME_LEN] = {0};
  for(int32_t i = 0; i < pStableVgroup->numOfTables; ++i) {
    char* name = pMsg;
    pMsg += TSDB_TABLE_FNAME_LEN;

    STableMetaInfo *pInfo = NULL;
    for(int32_t j = 0; j < pQueryInfo->numOfTables; ++j) {
      STableMetaInfo *pInfo1 = tscGetTableMetaInfoFromCmd(pCmd, j);
      memset(fName, 0, tListLen(fName));

      tNameExtractFullName(&pInfo1->name, fName);
      if (strcmp(name, fName) != 0) {
        continue;
      }

      pInfo = pInfo1;
      break;
    }

    if (!pInfo){
      continue;
    }
    int32_t size = 0;
    pInfo->vgroupList = createVgroupInfoFromMsg(pMsg, &size, pSql->self);
    pMsg += size;
  }

  taosReleaseRef(tscObjRef, parent->self);
  return pSql->res.code;
}

int tscProcessShowRsp(SSqlObj *pSql) {
  STableMetaMsg *pMetaMsg;
  SShowRsp *     pShow;
  SSchema *      pSchema;

  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo *pQueryInfo = tscGetQueryInfo(pCmd);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  pShow = (SShowRsp *)pRes->pRsp;
  pShow->qhandle = htobe64(pShow->qhandle);
  pRes->qId = pShow->qhandle;

  tscResetForNextRetrieve(pRes);
  pMetaMsg = &(pShow->tableMeta);

  pMetaMsg->numOfColumns = ntohs(pMetaMsg->numOfColumns);

  pSchema = pMetaMsg->schema;
  pMetaMsg->tid = ntohs(pMetaMsg->tid);
  for (int i = 0; i < pMetaMsg->numOfColumns; ++i) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema++;
  }

  tfree(pTableMetaInfo->pTableMeta);
  pTableMetaInfo->pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg);

  SSchema *pTableSchema = tscGetTableSchema(pTableMetaInfo->pTableMeta);
  if (pQueryInfo->colList == NULL) {
    pQueryInfo->colList = taosArrayInit(4, POINTER_BYTES);
  }
  
  SFieldInfo* pFieldInfo = &pQueryInfo->fieldsInfo;
  
  SColumnIndex index = {0};
  pSchema = pMetaMsg->schema;

  uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;
  for (int16_t i = 0; i < pMetaMsg->numOfColumns; ++i, ++pSchema) {
    index.columnIndex = i;
    tscColumnListInsert(pQueryInfo->colList, i, uid, pSchema);
    
    TAOS_FIELD f = tscCreateField(pSchema->type, pSchema->name, pSchema->bytes);
    SInternalField* pInfo = tscFieldInfoAppend(pFieldInfo, &f);
    
    pInfo->pExpr = tscExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index,
                     pTableSchema[i].type, pTableSchema[i].bytes, getNewResColId(pCmd), pTableSchema[i].bytes, false);
  }
  
  pCmd->numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
  tscFieldInfoUpdateOffset(pQueryInfo);
  return 0;
}

static void createHbObj(STscObj* pObj) {
  if (pObj->hbrid != 0) {
    return;
  }

  SSqlObj *pSql = (SSqlObj *)calloc(1, sizeof(SSqlObj));
  if (NULL == pSql) return;

  pSql->fp = tscProcessHeartBeatRsp;

  SQueryInfo *pQueryInfo = tscGetQueryInfoS(&pSql->cmd);
  if (pQueryInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tfree(pSql);
    return;
  }

  pQueryInfo->command = TSDB_SQL_HB;

  pSql->cmd.command = pQueryInfo->command;
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&(pSql->cmd), TSDB_DEFAULT_PAYLOAD_SIZE)) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    tfree(pSql);
    return;
  }

  pSql->param = pObj;
  pSql->pTscObj = pObj;
  pSql->signature = pSql;

  registerSqlObj(pSql);
  tscDebug("0x%"PRIx64" HB is allocated, pObj:%p", pSql->self, pObj);

  pObj->hbrid = pSql->self;
}

int tscProcessConnectRsp(SSqlObj *pSql) {
  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;

  char temp[TSDB_TABLE_FNAME_LEN * 2] = {0};

  SConnectRsp *pConnect = (SConnectRsp *)pRes->pRsp;
  tstrncpy(pObj->acctId, pConnect->acctId, sizeof(pObj->acctId));  // copy acctId from response
  
  pthread_mutex_lock(&pObj->mutex);
  int32_t len = sprintf(temp, "%s%s%s", pObj->acctId, TS_PATH_DELIMITER, pObj->db);

  assert(len <= sizeof(pObj->db));
  tstrncpy(pObj->db, temp, sizeof(pObj->db));
  pthread_mutex_unlock(&pObj->mutex);
  
  if (pConnect->epSet.numOfEps > 0) {
    tscEpSetHtons(&pConnect->epSet);
    tscUpdateMgmtEpSet(pSql, &pConnect->epSet);

    for (int i = 0; i < pConnect->epSet.numOfEps; ++i) {
      tscDebug("0x%"PRIx64" epSet.fqdn[%d]: %s, pObj:%p", pSql->self, i, pConnect->epSet.fqdn[i], pObj);
    }
  } 

  strcpy(pObj->sversion, pConnect->serverVersion);
  pObj->writeAuth = pConnect->writeAuth;
  pObj->superAuth = pConnect->superAuth;
  pObj->connId = htonl(pConnect->connId);

  createHbObj(pObj);

  //launch a timer to send heartbeat to maintain the connection and send status to mnode
  taosTmrReset(tscProcessActivityTimer, tsShellActivityTimer * 500, (void *)pObj->rid, tscTmr, &pObj->pTimer);

  return 0;
}

int tscProcessUseDbRsp(SSqlObj *pSql) {
  STscObj *       pObj = pSql->pTscObj;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0);
  
  pthread_mutex_lock(&pObj->mutex);
  int ret = tNameExtractFullName(&pTableMetaInfo->name, pObj->db);
  pthread_mutex_unlock(&pObj->mutex);
  
  return ret;
}

//todo only invalid the buffered data that belongs to dropped databases
int tscProcessDropDbRsp(SSqlObj *pSql) {
  //TODO LOCK DB WHEN MODIFY IT
  //pSql->pTscObj->db[0] = 0;
  
  taosHashClear(tscTableMetaMap);
  taosHashClear(tscVgroupMap);
  taosCacheEmpty(tscVgroupListBuf);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0);
  tscRemoveCachedTableMeta(pTableMetaInfo, pSql->self);
  tfree(pTableMetaInfo->pTableMeta);
  return 0;
}

int tscProcessAlterTableMsgRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0);

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  tscDebug("0x%"PRIx64" remove tableMeta in hashMap after alter-table: %s", pSql->self, name);

  bool isSuperTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  taosHashRemove(tscTableMetaMap, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
  tfree(pTableMetaInfo->pTableMeta);

  if (isSuperTable) {  // if it is a super table, iterate the hashTable and remove all the childTableMeta
    taosHashClear(tscTableMetaMap);
  }

  return 0;
}

int tscProcessAlterDbMsgRsp(SSqlObj *pSql) {
  UNUSED(pSql);
  return 0;
}
int tscProcessCompactRsp(SSqlObj *pSql) {
  UNUSED(pSql);
  return TSDB_CODE_SUCCESS; 
}

int tscProcessShowCreateRsp(SSqlObj *pSql) {
  return tscLocalResultCommonBuilder(pSql, 1);
}

int tscProcessQueryRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  SQueryTableRsp *pQueryAttr = (SQueryTableRsp *)pRes->pRsp;
  pQueryAttr->qId = htobe64(pQueryAttr->qId);

  pRes->qId  = pQueryAttr->qId;
  pRes->data = NULL;

  tscResetForNextRetrieve(pRes);
  tscDebug("0x%"PRIx64" query rsp received, qId:0x%"PRIx64, pSql->self, pRes->qId);
  return 0;
}

int tscProcessRetrieveRspFromNode(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  assert(pRes->rspLen >= sizeof(SRetrieveTableRsp));

  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *)pRes->pRsp;
  if (pRetrieve == NULL) {
    pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return pRes->code;
  }

  pRes->numOfRows = htonl(pRetrieve->numOfRows);
  pRes->precision = htons(pRetrieve->precision);
  pRes->offset    = htobe64(pRetrieve->offset);
  pRes->useconds  = htobe64(pRetrieve->useconds);
  pRes->completed = (pRetrieve->completed == 1);
  pRes->data      = pRetrieve->data;
  
  SQueryInfo* pQueryInfo = tscGetQueryInfo(pCmd);
  if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if ((pCmd->command == TSDB_SQL_RETRIEVE) ||
      ((UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo) || UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) &&
       !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_SUBQUERY)) ||
      (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
       !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY) &&
       !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE))) {
    tscSetResRawPtr(pRes, pQueryInfo);
  }

  if (pSql->pSubscription != NULL) {
    int32_t numOfCols = pQueryInfo->fieldsInfo.numOfOutput;
    
    TAOS_FIELD *pField = tscFieldInfoGetField(&pQueryInfo->fieldsInfo, numOfCols - 1);
    int16_t     offset = tscFieldInfoGetOffset(pQueryInfo, numOfCols - 1);
    
    char* p = pRes->data + (pField->bytes + offset) * pRes->numOfRows;

    int32_t numOfTables = htonl(*(int32_t*)p);
    p += sizeof(int32_t);
    for (int i = 0; i < numOfTables; i++) {
      int64_t uid = htobe64(*(int64_t*)p);
      p += sizeof(int64_t);
      p += sizeof(int32_t); // skip tid
      TSKEY key = htobe64(*(TSKEY*)p);
      p += sizeof(TSKEY);
      tscUpdateSubscriptionProgress(pSql->pSubscription, uid, key);
    }
  }

  pRes->row = 0;
  tscDebug("0x%"PRIx64" numOfRows:%d, offset:%" PRId64 ", complete:%d, qId:0x%"PRIx64, pSql->self, pRes->numOfRows, pRes->offset,
      pRes->completed, pRes->qId);

  return 0;
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code);

static int32_t getTableMetaFromMnode(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo, bool autocreate) {
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("0x%"PRIx64" malloc failed for new sqlobj to get table meta", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pNew->pTscObj     = pSql->pTscObj;
  pNew->signature   = pNew;
  pNew->cmd.command = TSDB_SQL_META;

  tscAddQueryInfo(&pNew->cmd);

  SQueryInfo *pNewQueryInfo = tscGetQueryInfoS(&pNew->cmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE + pSql->cmd.payloadLen)) {
    tscError("0x%"PRIx64" malloc failed for payload to get table meta", pSql->self);

    tscFreeSqlObj(pNew);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  STableMetaInfo *pNewTableMetaInfo = tscAddEmptyMetaInfo(pNewQueryInfo);
  assert(pNewQueryInfo->numOfTables == 1);

  tNameAssign(&pNewTableMetaInfo->name, &pTableMetaInfo->name);

  registerSqlObj(pNew);

  pNew->fp    = tscTableMetaCallBack;
  pNew->param = (void *)pSql->self;

  tscDebug("0x%"PRIx64" new pSqlObj:0x%"PRIx64" to get tableMeta, auto create:%d, metaRid from %"PRId64" to %"PRId64,
      pSql->self, pNew->self, autocreate, pSql->metaRid, pNew->self);
  pSql->metaRid = pNew->self;

  {
    STableInfoMsg  *pInfoMsg = (STableInfoMsg *)pNew->cmd.payload;
    int32_t code = tNameExtractFullName(&pNewTableMetaInfo->name, pInfoMsg->tableFname);
    if (code != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }

    pInfoMsg->createFlag = htons(autocreate? 1 : 0);
    char *pMsg = (char *)pInfoMsg + sizeof(STableInfoMsg);

    // tag data exists
    if (autocreate && pSql->cmd.insertParam.tagData.dataLen != 0) {
      pMsg = serializeTagData(&pSql->cmd.insertParam.tagData, pMsg);
    }

    pNew->cmd.payloadLen = (int32_t)(pMsg - (char*)pInfoMsg);
    pNew->cmd.msgType = TSDB_MSG_TYPE_CM_TABLE_META;
  }

  int32_t code = tscBuildAndSendRequest(pNew, NULL);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_ACTION_IN_PROGRESS;  // notify application that current process needs to be terminated
  }

  return code;
}

int32_t getMultiTableMetaFromMnode(SSqlObj *pSql, SArray* pNameList, SArray* pVgroupNameList, SArray* pUdfList, __async_cb_func_t fp, bool metaClone) {
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("0x%"PRIx64" failed to allocate sqlobj to get multiple table meta", pSql->self);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pNew->pTscObj     = pSql->pTscObj;
  pNew->signature   = pNew;
  pNew->cmd.command = TSDB_SQL_MULTI_META;

  int32_t numOfTable      = (int32_t) taosArrayGetSize(pNameList);
  int32_t numOfVgroupList = (int32_t) taosArrayGetSize(pVgroupNameList);
  int32_t numOfUdf        = pUdfList ? (int32_t)taosArrayGetSize(pUdfList) : 0;

  int32_t size = (numOfTable + numOfVgroupList) * TSDB_TABLE_FNAME_LEN + TSDB_FUNC_NAME_LEN * numOfUdf + sizeof(SMultiTableInfoMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, size)) {
    tscError("0x%"PRIx64" malloc failed for payload to get table meta", pSql->self);
    tscFreeSqlObj(pNew);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SMultiTableInfoMsg* pInfo = (SMultiTableInfoMsg*) pNew->cmd.payload;
  pInfo->metaClone    = metaClone? 1:0;
  pInfo->numOfTables  = htonl((uint32_t) taosArrayGetSize(pNameList));
  pInfo->numOfVgroups = htonl((uint32_t) taosArrayGetSize(pVgroupNameList));
  pInfo->numOfUdfs    = htonl(numOfUdf);

  char* start = pInfo->tableNames;
  int32_t len = 0;
  for(int32_t i = 0; i < numOfTable; ++i) {
    char* name = taosArrayGetP(pNameList, i);
    if (i < numOfTable - 1 || numOfVgroupList > 0 || numOfUdf > 0) {
      len = sprintf(start, "%s,", name);
    } else {
      len = sprintf(start, "%s", name);
    }

    start += len;
  }

  for(int32_t i = 0; i < numOfVgroupList; ++i) {
    char* name = taosArrayGetP(pVgroupNameList, i);
    if (i < numOfVgroupList - 1 || numOfUdf > 0) {
      len = sprintf(start, "%s,", name);
    } else {
      len = sprintf(start, "%s", name);
    }

    start += len;
  }

  for(int32_t i = 0; i < numOfUdf; ++i) {
    SUdfInfo * u = taosArrayGet(pUdfList, i);
    if (i < numOfUdf - 1) {
      len = sprintf(start, "%s,", u->name);
    } else {
      len = sprintf(start, "%s", u->name);
    }

    start += len;
  }

  pNew->cmd.payloadLen = (int32_t) ((start - pInfo->tableNames) + sizeof(SMultiTableInfoMsg));
  pNew->cmd.msgType = TSDB_MSG_TYPE_CM_TABLES_META;

  registerSqlObj(pNew);
  tscDebug("0x%"PRIx64" new pSqlObj:0x%"PRIx64" to get %d tableMeta, vgroupInfo:%d, udf:%d, msg size:%d", pSql->self,
      pNew->self, numOfTable, numOfVgroupList, numOfUdf, pNew->cmd.payloadLen);

  pNew->fp = fp;
  pNew->param = (void *)pSql->self;

  tscDebug("0x%"PRIx64" metaRid from 0x%" PRIx64 " to 0x%" PRIx64 , pSql->self, pSql->metaRid, pNew->self);
  
  pSql->metaRid = pNew->self;
  int32_t code = tscBuildAndSendRequest(pNew, NULL);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_ACTION_IN_PROGRESS;  // notify application that current process needs to be terminated
  }

  return code;
}

int32_t tscGetTableMetaImpl(SSqlObj* pSql, STableMetaInfo *pTableMetaInfo, bool autocreate, bool onlyLocal) {
  assert(tIsValidName(&pTableMetaInfo->name));

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  size_t len = strlen(name);
   // just make runtime happy
  if (pTableMetaInfo->tableMetaCapacity != 0 && pTableMetaInfo->pTableMeta != NULL) {
    memset(pTableMetaInfo->pTableMeta, 0, pTableMetaInfo->tableMetaCapacity);
  } 
  taosHashGetCloneExt(tscTableMetaMap, name, len, NULL, (void **)&(pTableMetaInfo->pTableMeta), &pTableMetaInfo->tableMetaCapacity);
  
  STableMeta* pMeta   = pTableMetaInfo->pTableMeta;
  STableMeta* pSTMeta = (STableMeta *)(pSql->pBuf);
  if (pMeta && pMeta->id.uid > 0) {
    // in case of child table, here only get the
    if (pMeta->tableType == TSDB_CHILD_TABLE) {
      int32_t code = tscCreateTableMetaFromSTableMeta(&pTableMetaInfo->pTableMeta, name, &pTableMetaInfo->tableMetaCapacity, (STableMeta **)(&pSTMeta));
      pSql->pBuf   = (void *)(pSTMeta); 
      if (code != TSDB_CODE_SUCCESS) {
        return getTableMetaFromMnode(pSql, pTableMetaInfo, autocreate);
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  if (onlyLocal) {
    return TSDB_CODE_TSC_NO_META_CACHED;
  }
  
  return getTableMetaFromMnode(pSql, pTableMetaInfo, autocreate);
}

int32_t tscGetTableMeta(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo) {
  return tscGetTableMetaImpl(pSql, pTableMetaInfo, false, false);
}

int tscGetTableMetaEx(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo, bool createIfNotExists, bool onlyLocal) {
  return tscGetTableMetaImpl(pSql, pTableMetaInfo, createIfNotExists, onlyLocal);
}

int32_t tscGetUdfFromNode(SSqlObj *pSql, SQueryInfo* pQueryInfo) {
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("%p malloc failed for new sqlobj to get user-defined functions", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_RETRIEVE_FUNC;

  if (tscAddQueryInfo(&pNew->cmd) != TSDB_CODE_SUCCESS) {
    tscError("%p malloc failed for new queryinfo", pSql);
    tscFreeSqlObj(pNew);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SQueryInfo *pNewQueryInfo = tscGetQueryInfo(&pNew->cmd);

  pNewQueryInfo->pUdfInfo = taosArrayInit(4, sizeof(SUdfInfo));
  for(int32_t i = 0; i < taosArrayGetSize(pQueryInfo->pUdfInfo); ++i) {
    SUdfInfo info = {0};
    SUdfInfo* p1 = taosArrayGet(pQueryInfo->pUdfInfo, i);
    info = *p1;
    info.name = strdup(p1->name);
    taosArrayPush(pNewQueryInfo->pUdfInfo, &info);
  }

  pNew->cmd.active = pNewQueryInfo;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE + pSql->cmd.payloadLen)) {
    tscError("%p malloc failed for payload to get table meta", pSql);
    tscFreeSqlObj(pNew);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscDebug("%p new pSqlObj:%p to retrieve udf", pSql, pNew);
  registerSqlObj(pNew);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = (void *)pSql->self;

  tscDebug("%p metaRid from %" PRId64 " to %" PRId64 , pSql, pSql->metaRid, pNew->self);

  pSql->metaRid = pNew->self;

  int32_t code = tscBuildAndSendRequest(pNew, NULL);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_ACTION_IN_PROGRESS;  // notify application that current process needs to be terminated
  }

  return code;
}

static void freeElem(void* p) {
  tfree(*(char**)p);
}

/**
 * retrieve table meta from mnode, and then update the local table meta hashmap.
 * @param pSql          sql object
 * @param tableIndex    table index
 * @return              status code
 */
int tscRenewTableMeta(SSqlObj *pSql, int32_t tableIndex) {
  SSqlCmd* pCmd = &pSql->cmd;

  SQueryInfo     *pQueryInfo = tscGetQueryInfo(pCmd);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, name);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("0x%"PRIx64" failed to generate the table full name", pSql->self);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  if (pTableMeta) {
    tscDebug("0x%"PRIx64" update table meta:%s, old meta numOfTags:%d, numOfCols:%d, uid:%" PRIu64, pSql->self, name,
             tscGetNumOfTags(pTableMeta), tscGetNumOfColumns(pTableMeta), pTableMeta->id.uid);
  }


  // remove stored tableMeta info in hash table
  tscResetSqlCmd(pCmd, true, pSql->self);

  SArray* pNameList  = taosArrayInit(1, POINTER_BYTES);
  SArray* vgroupList = taosArrayInit(1, POINTER_BYTES);

  char* n = strdup(name);
  taosArrayPush(pNameList, &n);
  code = getMultiTableMetaFromMnode(pSql, pNameList, vgroupList, NULL, tscTableMetaCallBack, true);
  taosArrayDestroyEx(pNameList, freeElem);
  taosArrayDestroyEx(vgroupList, freeElem);

  return code;
}

static bool allVgroupInfoRetrieved(SQueryInfo* pQueryInfo) {
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->vgroupList == NULL) {
      return false;
    }
  }
  
  // all super tables vgroupinfo are retrieved, no need to retrieve vgroup info anymore
  return true;
}

int tscGetSTableVgroupInfo(SSqlObj *pSql, SQueryInfo* pQueryInfo) {
  int32_t code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
  if (allVgroupInfoRetrieved(pQueryInfo)) {
    return TSDB_CODE_SUCCESS;
  }
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_STABLEVGROUP;

  // TODO TEST IT
  SQueryInfo *pNewQueryInfo = tscGetQueryInfoS(&pNew->cmd);
  if (pNewQueryInfo == NULL) {
    tscFreeSqlObj(pNew);
    return code;
  }

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pMInfo = tscGetMetaInfo(pQueryInfo, i);
    STableMeta* pTableMeta = tscTableMetaDup(pMInfo->pTableMeta);
    tscAddTableMetaInfo(pNewQueryInfo, &pMInfo->name, pTableMeta, NULL, pMInfo->tagColList, pMInfo->pVgroupTables);
  }

  if ((code = tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE)) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return code;
  }

  pNewQueryInfo->numOfTables = pQueryInfo->numOfTables;
  registerSqlObj(pNew);

  tscDebug("0x%"PRIx64" svgroupRid from %" PRId64 " to %" PRId64 , pSql->self, pSql->svgroupRid, pNew->self);
  
  pSql->svgroupRid = pNew->self;
  tscDebug("0x%"PRIx64" new sqlObj:%p to get vgroupInfo, numOfTables:%d", pSql->self, pNew, pNewQueryInfo->numOfTables);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = (void *)pSql->self;
  code = tscBuildAndSendRequest(pNew, NULL);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_ACTION_IN_PROGRESS;
  }

  return code;
}

void tscInitMsgsFp() {
  tscBuildMsg[TSDB_SQL_SELECT] = tscBuildQueryMsg;
  tscBuildMsg[TSDB_SQL_INSERT] = tscBuildSubmitMsg;
  tscBuildMsg[TSDB_SQL_FETCH] = tscBuildFetchMsg;

  tscBuildMsg[TSDB_SQL_CREATE_DB] = tscBuildCreateDbMsg;
  tscBuildMsg[TSDB_SQL_CREATE_USER] = tscBuildUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_FUNCTION] = tscBuildCreateFuncMsg;

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_ACCT] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_DB] = tscBuildDropDbMsg;
  tscBuildMsg[TSDB_SQL_DROP_FUNCTION] = tscBuildDropFuncMsg;
  tscBuildMsg[TSDB_SQL_SYNC_DB_REPLICA] = tscBuildSyncDbReplicaMsg;
  tscBuildMsg[TSDB_SQL_DROP_TABLE] = tscBuildDropTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_USER] = tscBuildUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_DNODE] = tscBuildCreateDnodeMsg;
  tscBuildMsg[TSDB_SQL_DROP_DNODE] = tscBuildDropDnodeMsg;
  tscBuildMsg[TSDB_SQL_CFG_DNODE] = tscBuildCfgDnodeMsg;
  tscBuildMsg[TSDB_SQL_ALTER_TABLE] = tscBuildAlterTableMsg;
  tscBuildMsg[TSDB_SQL_UPDATE_TAGS_VAL] = tscBuildUpdateTagMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;
  tscBuildMsg[TSDB_SQL_COMPACT_VNODE] = tscBuildCompactMsg;  

  tscBuildMsg[TSDB_SQL_CONNECT] = tscBuildConnectMsg;
  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_STABLEVGROUP] = tscBuildSTableVgroupMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE_FUNC] = tscBuildRetrieveFuncMsg;

  tscBuildMsg[TSDB_SQL_HB] = tscBuildHeartBeatMsg;
  tscBuildMsg[TSDB_SQL_SHOW] = tscBuildShowMsg;
  tscBuildMsg[TSDB_SQL_RETRIEVE] = tscBuildRetrieveFromMgmtMsg;
  tscBuildMsg[TSDB_SQL_KILL_QUERY] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_STREAM] = tscBuildKillMsg;
  tscBuildMsg[TSDB_SQL_KILL_CONNECTION] = tscBuildKillMsg;

  tscProcessMsgRsp[TSDB_SQL_SELECT] = tscProcessQueryRsp;
  tscProcessMsgRsp[TSDB_SQL_FETCH] = tscProcessRetrieveRspFromNode;

  tscProcessMsgRsp[TSDB_SQL_DROP_DB] = tscProcessDropDbRsp;
  tscProcessMsgRsp[TSDB_SQL_DROP_TABLE] = tscProcessDropTableRsp;
  tscProcessMsgRsp[TSDB_SQL_CONNECT] = tscProcessConnectRsp;
  tscProcessMsgRsp[TSDB_SQL_USE_DB] = tscProcessUseDbRsp;
  tscProcessMsgRsp[TSDB_SQL_META] = tscProcessTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_STABLEVGROUP] = tscProcessSTableVgroupRsp;
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiTableMetaRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_FUNC] = tscProcessRetrieveFuncRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromNode;  // rsp handled by same function.
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;

  tscProcessMsgRsp[TSDB_SQL_CURRENT_DB]   = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_USER] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_VERSION] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CLI_VERSION]  = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_STATUS]  = tscProcessLocalRetrieveRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_EMPTY_RESULT] = tscProcessEmptyResultRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_GLOBALMERGE] = tscProcessRetrieveGlobalMergeRsp;

  tscProcessMsgRsp[TSDB_SQL_ALTER_TABLE] = tscProcessAlterTableMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_ALTER_DB] = tscProcessAlterDbMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_COMPACT_VNODE] = tscProcessCompactRsp; 

  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_TABLE] = tscProcessShowCreateRsp;
  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_STABLE] = tscProcessShowCreateRsp;
  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_DATABASE] = tscProcessShowCreateRsp;

  tscKeepConn[TSDB_SQL_SHOW] = 1;
  tscKeepConn[TSDB_SQL_RETRIEVE] = 1;
  tscKeepConn[TSDB_SQL_SELECT] = 1;
  tscKeepConn[TSDB_SQL_FETCH] = 1;
  tscKeepConn[TSDB_SQL_HB] = 1;
}

