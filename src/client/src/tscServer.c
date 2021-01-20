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
#include "trpc.h"
#include "tscLocalMerge.h"
#include "tscLog.h"
#include "tscProfile.h"
#include "tscUtil.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "ttimer.h"
#include "tlockfree.h"

int (*tscBuildMsg[TSDB_SQL_MAX])(SSqlObj *pSql, SSqlInfo *pInfo) = {0};

int (*tscProcessMsgRsp[TSDB_SQL_MAX])(SSqlObj *pSql);
void tscProcessActivityTimer(void *handle, void *tmrId);
int tscKeepConn[TSDB_SQL_MAX] = {0};

TSKEY tscGetSubscriptionProgress(void* sub, int64_t uid, TSKEY dflt);
void tscUpdateSubscriptionProgress(void* sub, int64_t uid, TSKEY ts);
void tscSaveSubscriptionProgress(void* sub);

static int32_t minMsgSize() { return tsRpcHeadSize + 100; }
static int32_t getWaitingTimeInterval(int32_t count) {
  int32_t initial = 100; // 100 ms by default
  if (count <= 1) {
    return 0;
  }

  return initial * ((2u)<<(count - 2));
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

static void tscUpdateVgroupInfo(SSqlObj *pObj, SRpcEpSet *pEpSet) {
  SSqlCmd *pCmd = &pObj->cmd;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  if (pTableMetaInfo == NULL || pTableMetaInfo->pTableMeta == NULL) {
    return;
  }

  int32_t vgId = pTableMetaInfo->pTableMeta->vgId;
  if (pTableMetaInfo->pTableMeta->tableType == TSDB_SUPER_TABLE) {
    assert(vgId == 0);
    return;
  }

  SNewVgroupInfo vgroupInfo = {.vgId = -1};
  taosHashGetClone(tscVgroupMap, &vgId, sizeof(vgId), NULL, &vgroupInfo, sizeof(SNewVgroupInfo));
  assert(vgroupInfo.numOfEps > 0 && vgroupInfo.vgId > 0);

  tscDebug("before: Endpoint in use:%d, numOfEps:%d", vgroupInfo.inUse, vgroupInfo.numOfEps);
  vgroupInfo.inUse    = pEpSet->inUse;
  vgroupInfo.numOfEps = pEpSet->numOfEps;
  for (int32_t i = 0; i < vgroupInfo.numOfEps; i++) {
    strncpy(vgroupInfo.ep[i].fqdn, pEpSet->fqdn[i], TSDB_FQDN_LEN);
    vgroupInfo.ep[i].port = pEpSet->port[i];
  }

  tscDebug("after: EndPoint in use:%d, numOfEps:%d", vgroupInfo.inUse, vgroupInfo.numOfEps);
  taosHashPut(tscVgroupMap, &vgId, sizeof(vgId), &vgroupInfo, sizeof(SNewVgroupInfo));
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
      if (!tscEpSetIsEqual(&pSql->pTscObj->tscCorMgmtEpSet->epSet, epSet)) {
        tscTrace("%p updating epset: numOfEps: %d, inUse: %d", pSql, epSet->numOfEps, epSet->inUse);
        for (int8_t i = 0; i < epSet->numOfEps; i++) {
          tscTrace("endpoint %d: fqdn=%s, port=%d", i, epSet->fqdn[i], epSet->port[i]);
        }
        tscUpdateMgmtEpSet(pSql, epSet);
      }
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
      tscError("HB:%p, total dnode:%d, online dnode:%d", pSql, total, online);
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
    tscDebug("%p send heartbeat in %dms", pSql, waitingDuring);

    taosTmrReset(tscProcessActivityTimer, waitingDuring, (void *)pObj->rid, tscTmr, &pObj->pTimer);
  } else {
    tscDebug("%p start to close tscObj:%p, not send heartbeat again", pSql, pObj);
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
  int32_t code = tscProcessSql(pHB);
  taosReleaseRef(tscObjRef, pObj->hbrid);

  if (code != TSDB_CODE_SUCCESS) {
    tscError("%p failed to sent HB to server, reason:%s", pHB, tstrerror(code));
  }

  taosReleaseRef(tscRefId, rid);
}

int tscSendMsgToServer(SSqlObj *pSql) {
  STscObj* pObj = pSql->pTscObj;
  SSqlCmd* pCmd = &pSql->cmd;
  
  char *pMsg = rpcMallocCont(pCmd->payloadLen);
  if (NULL == pMsg) {
    tscError("%p msg:%s malloc failed", pSql, taosMsg[pSql->cmd.msgType]);
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

  rpcSendRequest(pObj->pDnodeConn, &pSql->epSet, &rpcMsg, &pSql->rpcRid);
  return TSDB_CODE_SUCCESS;
}

void tscProcessMsgFromServer(SRpcMsg *rpcMsg, SRpcEpSet *pEpSet) {
  TSDB_CACHE_PTR_TYPE handle = (TSDB_CACHE_PTR_TYPE) rpcMsg->ahandle;
  SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, handle);
  if (pSql == NULL) {
    rpcFreeCont(rpcMsg->pCont);
    return;
  }
  assert(pSql->self == handle);

  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

  pSql->rpcRid = -1;

  if (pObj->signature != pObj) {
    tscDebug("%p DB connection is closed, cmd:%d pObj:%p signature:%p", pSql, pCmd->command, pObj, pObj->signature);

    taosRemoveRef(tscObjRef, pSql->self);
    taosReleaseRef(tscObjRef, pSql->self);
    rpcFreeCont(rpcMsg->pCont);
    return;
  }

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  if (pQueryInfo != NULL && pQueryInfo->type == TSDB_QUERY_TYPE_FREE_RESOURCE) {
    tscDebug("%p sqlObj needs to be released or DB connection is closed, cmd:%d type:%d, pObj:%p signature:%p",
        pSql, pCmd->command, pQueryInfo->type, pObj, pObj->signature);

    taosRemoveRef(tscObjRef, pSql->self);
    taosReleaseRef(tscObjRef, pSql->self);
    rpcFreeCont(rpcMsg->pCont);
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
    pSql->cmd.submitSchema = 1;
  }

  if ((cmd == TSDB_SQL_SELECT || cmd == TSDB_SQL_FETCH || cmd == TSDB_SQL_UPDATE_TAGS_VAL) &&
      (rpcMsg->code == TSDB_CODE_TDB_INVALID_TABLE_ID ||
       rpcMsg->code == TSDB_CODE_VND_INVALID_VGROUP_ID ||
       rpcMsg->code == TSDB_CODE_RPC_NETWORK_UNAVAIL ||
       rpcMsg->code == TSDB_CODE_APP_NOT_READY)) {
        
    pSql->retry++;
    tscWarn("%p it shall renew table meta, code:%s, retry:%d", pSql, tstrerror(rpcMsg->code), pSql->retry);

    pSql->res.code = rpcMsg->code;  // keep the previous error code
    if (pSql->retry > pSql->maxRetry) {
      tscError("%p max retry %d reached, give up", pSql, pSql->maxRetry);
    } else {
      // wait for a little bit moment and then retry
      // todo do not sleep in rpc callback thread, add this process into queueu to process
      if (rpcMsg->code == TSDB_CODE_APP_NOT_READY || rpcMsg->code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
        int32_t duration = getWaitingTimeInterval(pSql->retry);
        taosMsleep(duration);
      }

      rpcMsg->code = tscRenewTableMeta(pSql, 0);

      // if there is an error occurring, proceed to the following error handling procedure.
      if (rpcMsg->code == TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
        taosReleaseRef(tscObjRef, pSql->self);
        rpcFreeCont(rpcMsg->pCont);
        return;
      }
    }
  }

  pRes->rspLen = 0;
  
  if (pRes->code == TSDB_CODE_TSC_QUERY_CANCELLED) {
    tscDebug("%p query is cancelled, code:%s", pSql, tstrerror(pRes->code));
  } else {
    pRes->code = rpcMsg->code;
  }

  if (pRes->code == TSDB_CODE_SUCCESS) {
    tscDebug("%p reset retry counter to be 0 due to success rsp, old:%d", pSql, pSql->retry);
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
      tscDebug("%p SQL cmd:%s, code:%s inserted rows:%d rspLen:%d", pSql, sqlCmd[pCmd->command], 
          tstrerror(pRes->code), pMsg->affectedRows, pRes->rspLen);
    } else {
      tscDebug("%p SQL cmd:%s, code:%s rspLen:%d", pSql, sqlCmd[pCmd->command], tstrerror(pRes->code), pRes->rspLen);
    }
  }
  
  if (pRes->code == TSDB_CODE_SUCCESS && tscProcessMsgRsp[pCmd->command]) {
    rpcMsg->code = (*tscProcessMsgRsp[pCmd->command])(pSql);
  }

  bool shouldFree = tscShouldBeFreed(pSql);
  if (rpcMsg->code != TSDB_CODE_TSC_ACTION_IN_PROGRESS) {
    rpcMsg->code = (pRes->code == TSDB_CODE_SUCCESS) ? (int32_t)pRes->numOfRows : pRes->code;
    (*pSql->fp)(pSql->param, pSql, rpcMsg->code);
  }

  taosReleaseRef(tscObjRef, pSql->self);

  if (shouldFree) { // in case of table-meta/vgrouplist query, automatically free it
    taosRemoveRef(tscObjRef, pSql->self);
    tscDebug("%p sqlObj is automatically freed", pSql); 
  }

  rpcFreeCont(rpcMsg->pCont);
}

int doProcessSql(SSqlObj *pSql) {
  SSqlCmd *pCmd = &pSql->cmd;
  SSqlRes *pRes = &pSql->res;

  if (pCmd->command == TSDB_SQL_SELECT ||
      pCmd->command == TSDB_SQL_FETCH ||
      pCmd->command == TSDB_SQL_RETRIEVE ||
      pCmd->command == TSDB_SQL_INSERT ||
      pCmd->command == TSDB_SQL_CONNECT ||
      pCmd->command == TSDB_SQL_HB ||
      pCmd->command == TSDB_SQL_META ||
      pCmd->command == TSDB_SQL_STABLEVGROUP) {
    pRes->code = tscBuildMsg[pCmd->command](pSql, NULL);
  }
  
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return pRes->code;
  }

  int32_t code = tscSendMsgToServer(pSql);

  // NOTE: if code is TSDB_CODE_SUCCESS, pSql may have been released here already by other threads.
  if (code != TSDB_CODE_SUCCESS) {
    pRes->code = code;
    tscAsyncResultOnError(pSql);
    return code;
  }
  
  return TSDB_CODE_SUCCESS;
}

int tscProcessSql(SSqlObj *pSql) {
  char name[TSDB_TABLE_FNAME_LEN] = {0};

  SSqlCmd *pCmd = &pSql->cmd;
  uint32_t type = 0;

  SQueryInfo     *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = NULL;

  if (pQueryInfo != NULL) {
    pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
    if (pTableMetaInfo != NULL) {
      tNameExtractFullName(&pTableMetaInfo->name, name);
    }

    type = pQueryInfo->type;

    // while numOfTables equals to 0, it must be Heartbeat
    assert((pQueryInfo->numOfTables == 0 && pQueryInfo->command == TSDB_SQL_HB) || pQueryInfo->numOfTables > 0);
  }

  tscDebug("%p SQL cmd:%s will be processed, name:%s, type:%d", pSql, sqlCmd[pCmd->command], name, type);
  if (pCmd->command < TSDB_SQL_MGMT) { // the pTableMetaInfo cannot be NULL
    if (pTableMetaInfo == NULL) {
      pSql->res.code = TSDB_CODE_TSC_APP_ERROR;
      return pSql->res.code;
    }
  } else if (pCmd->command >= TSDB_SQL_LOCAL) {
    //pSql->epSet = tscMgmtEpSet;
//  } else {  // local handler
    return (*tscProcessMsgRsp[pCmd->command])(pSql);
  }
  
  return doProcessSql(pSql);
}

int tscBuildFetchMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg *) pSql->cmd.payload;

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, pSql->cmd.clauseIndex);
  pRetrieveMsg->free    = htons(pQueryInfo->type);
  pRetrieveMsg->qhandle = htobe64(pSql->res.qhandle);

  // todo valid the vgroupId at the client side
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    int32_t vgIndex = pTableMetaInfo->vgroupIndex;
    if (pTableMetaInfo->pVgroupTables == NULL) {
      SVgroupsInfo *pVgroupInfo = pTableMetaInfo->vgroupList;
      assert(pVgroupInfo->vgroups[vgIndex].vgId > 0 && vgIndex < pTableMetaInfo->vgroupList->numOfVgroups);

      pRetrieveMsg->header.vgId = htonl(pVgroupInfo->vgroups[vgIndex].vgId);
      tscDebug("%p build fetch msg from vgId:%d, vgIndex:%d", pSql, pVgroupInfo->vgroups[vgIndex].vgId, vgIndex);
    } else {
      int32_t numOfVgroups = (int32_t)taosArrayGetSize(pTableMetaInfo->pVgroupTables);
      assert(vgIndex >= 0 && vgIndex < numOfVgroups);

      SVgroupTableInfo* pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, vgIndex);

      pRetrieveMsg->header.vgId = htonl(pTableIdList->vgInfo.vgId);
      tscDebug("%p build fetch msg from vgId:%d, vgIndex:%d", pSql, pTableIdList->vgInfo.vgId, vgIndex);
    }
  } else {
    STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
    pRetrieveMsg->header.vgId = htonl(pTableMeta->vgId);
    tscDebug("%p build fetch msg from only one vgroup, vgId:%d", pSql, pTableMeta->vgId);
  }

  pSql->cmd.payloadLen = sizeof(SRetrieveTableMsg);
  pSql->cmd.msgType = TSDB_MSG_TYPE_FETCH;

  pRetrieveMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  return TSDB_CODE_SUCCESS;
}

int tscBuildSubmitMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);
  STableMeta* pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;
  
  char* pMsg = pSql->cmd.payload;
  
  // NOTE: shell message size should not include SMsgDesc
  int32_t size = pSql->cmd.payloadLen - sizeof(SMsgDesc);

  SMsgDesc* pMsgDesc = (SMsgDesc*) pMsg;
  pMsgDesc->numOfVnodes = htonl(1); // always one vnode

  pMsg += sizeof(SMsgDesc);
  SSubmitMsg *pShellMsg = (SSubmitMsg *)pMsg;

  pShellMsg->header.vgId = htonl(pTableMeta->vgId);
  pShellMsg->header.contLen = htonl(size);      // the length not includes the size of SMsgDesc
  pShellMsg->length = pShellMsg->header.contLen;
  
  pShellMsg->numOfBlocks = htonl(pSql->cmd.numOfTablesInSubmit);  // number of tables to be inserted

  // pSql->cmd.payloadLen is set during copying data into payload
  pSql->cmd.msgType = TSDB_MSG_TYPE_SUBMIT;

  SNewVgroupInfo vgroupInfo = {0};
  taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo, sizeof(SNewVgroupInfo));
  tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);

  tscDebug("%p build submit msg, vgId:%d numOfTables:%d numberOfEP:%d", pSql, pTableMeta->vgId, pSql->cmd.numOfTablesInSubmit,
      pSql->epSet.numOfEps);
  return TSDB_CODE_SUCCESS;
}

/*
 * for table query, simply return the size <= 1k
 */
static int32_t tscEstimateQueryMsgSize(SSqlObj *pSql, int32_t clauseIndex) {
  const static int32_t MIN_QUERY_MSG_PKT_SIZE = TSDB_MAX_BYTES_PER_ROW * 5;

  SSqlCmd* pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);

  int32_t srcColListSize = (int32_t)(taosArrayGetSize(pQueryInfo->colList) * sizeof(SColumnInfo));

  size_t  numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  int32_t exprSize = (int32_t)(sizeof(SSqlFuncMsg) * numOfExprs * 2);

  int32_t tsBufSize = (pQueryInfo->tsBuf != NULL) ? pQueryInfo->tsBuf->fileSize : 0;
  int32_t sqlLen = (int32_t) strlen(pSql->sqlstr) + 1;


  int32_t tableSerialize = 0;
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pTableMetaInfo->pVgroupTables != NULL) {
    size_t numOfGroups = taosArrayGetSize(pTableMetaInfo->pVgroupTables);

    int32_t totalTables = 0;
    for (int32_t i = 0; i < numOfGroups; ++i) {
      SVgroupTableInfo *pTableInfo = taosArrayGet(pTableMetaInfo->pVgroupTables, i);
      totalTables += (int32_t) taosArrayGetSize(pTableInfo->itemList);
    }

    tableSerialize = totalTables * sizeof(STableIdInfo);
  }

  return MIN_QUERY_MSG_PKT_SIZE + minMsgSize() + sizeof(SQueryTableMsg) + srcColListSize + exprSize + tsBufSize +
         tableSerialize + sqlLen + 4096;
}

static char *doSerializeTableInfo(SQueryTableMsg* pQueryMsg, SSqlObj *pSql, char *pMsg) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, pSql->cmd.clauseIndex, 0);
  TSKEY dfltKey = htobe64(pQueryMsg->window.skey);

  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;
  if (UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo) || pTableMetaInfo->pVgroupTables == NULL) {
    
    int32_t vgId = -1;
    if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
      int32_t index = pTableMetaInfo->vgroupIndex;
      assert(index >= 0);

      SVgroupInfo* pVgroupInfo = NULL;
      if (pTableMetaInfo->vgroupList->numOfVgroups > 0) {
        assert(index < pTableMetaInfo->vgroupList->numOfVgroups);
        pVgroupInfo = &pTableMetaInfo->vgroupList->vgroups[index];
      }

      vgId = pVgroupInfo->vgId;
      tscSetDnodeEpSet(&pSql->epSet, pVgroupInfo);
      tscDebug("%p query on stable, vgIndex:%d, numOfVgroups:%d", pSql, index, pTableMetaInfo->vgroupList->numOfVgroups);
    } else {
      vgId = pTableMeta->vgId;

      SNewVgroupInfo vgroupInfo = {0};
      taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo, sizeof(SNewVgroupInfo));
      tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);
    }

    pSql->epSet.inUse = rand()%pSql->epSet.numOfEps;

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

    tscDebug("%p query on stable, vgIndex:%d, numOfVgroups:%d", pSql, index, numOfVgroups);

    SVgroupTableInfo* pTableIdList = taosArrayGet(pTableMetaInfo->pVgroupTables, index);

    // set the vgroup info 
    tscSetDnodeEpSet(&pSql->epSet, &pTableIdList->vgInfo);
    pQueryMsg->head.vgId = htonl(pTableIdList->vgInfo.vgId);
    
    int32_t numOfTables = (int32_t)taosArrayGetSize(pTableIdList->itemList);
    pQueryMsg->numOfTables = htonl(numOfTables);  // set the number of tables
  
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

  tscDebug("%p vgId:%d, query on table:%s, tid:%d, uid:%" PRIu64, pSql, htonl(pQueryMsg->head.vgId), n, pTableMeta->id.tid, pTableMeta->id.uid);
  return pMsg;
}

int tscBuildQueryMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;

  int32_t size = tscEstimateQueryMsgSize(pSql, pCmd->clauseIndex);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_INVALID_SQL;  // todo add test for this
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableMeta * pTableMeta = pTableMetaInfo->pTableMeta;

  size_t numOfSrcCols = taosArrayGetSize(pQueryInfo->colList);
  if (numOfSrcCols <= 0 && !tscQueryTags(pQueryInfo)) {
    tscError("%p illegal value of numOfCols in query msg: %" PRIu64 ", table cols:%d", pSql, (uint64_t)numOfSrcCols,
        tscGetNumOfColumns(pTableMeta));

    return TSDB_CODE_TSC_INVALID_SQL;
  }
  
  if (pQueryInfo->interval.interval < 0) {
    tscError("%p illegal value of aggregation time interval in query msg: %" PRId64, pSql, (int64_t)pQueryInfo->interval.interval);
    return TSDB_CODE_TSC_INVALID_SQL;
  }
  
  if (pQueryInfo->groupbyExpr.numOfGroupCols < 0) {
    tscError("%p illegal value of numOfGroupCols in query msg: %d", pSql, pQueryInfo->groupbyExpr.numOfGroupCols);
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  SQueryTableMsg *pQueryMsg = (SQueryTableMsg *)pCmd->payload;
  tstrncpy(pQueryMsg->version, version, tListLen(pQueryMsg->version));

  int32_t numOfTags = (int32_t)taosArrayGetSize(pTableMetaInfo->tagColList);
  int32_t sqlLen = (int32_t) strlen(pSql->sqlstr);

  if (pQueryInfo->order.order == TSDB_ORDER_ASC) {
    pQueryMsg->window.skey = htobe64(pQueryInfo->window.skey);
    pQueryMsg->window.ekey = htobe64(pQueryInfo->window.ekey);
  } else {
    pQueryMsg->window.skey = htobe64(pQueryInfo->window.ekey);
    pQueryMsg->window.ekey = htobe64(pQueryInfo->window.skey);
  }

  pQueryMsg->order          = htons(pQueryInfo->order.order);
  pQueryMsg->orderColId     = htons(pQueryInfo->order.orderColId);
  pQueryMsg->fillType       = htons(pQueryInfo->fillType);
  pQueryMsg->limit          = htobe64(pQueryInfo->limit.limit);
  pQueryMsg->offset         = htobe64(pQueryInfo->limit.offset);
  pQueryMsg->numOfCols      = htons((int16_t)taosArrayGetSize(pQueryInfo->colList));
  pQueryMsg->interval.interval = htobe64(pQueryInfo->interval.interval);
  pQueryMsg->interval.sliding  = htobe64(pQueryInfo->interval.sliding);
  pQueryMsg->interval.offset   = htobe64(pQueryInfo->interval.offset);
  pQueryMsg->interval.intervalUnit = pQueryInfo->interval.intervalUnit;
  pQueryMsg->interval.slidingUnit  = pQueryInfo->interval.slidingUnit;
  pQueryMsg->interval.offsetUnit   = pQueryInfo->interval.offsetUnit;
  pQueryMsg->numOfGroupCols = htons(pQueryInfo->groupbyExpr.numOfGroupCols);
  pQueryMsg->tagNameRelType = htons(pQueryInfo->tagCond.relType);
  pQueryMsg->tbnameCondLen  = htonl(pQueryInfo->tagCond.tbnameCond.len);
  pQueryMsg->numOfTags      = htonl(numOfTags);
  pQueryMsg->queryType      = htonl(pQueryInfo->type);
  pQueryMsg->vgroupLimit    = htobe64(pQueryInfo->vgroupLimit);
  pQueryMsg->sqlstrLen      = htonl(sqlLen);

  size_t numOfOutput = tscSqlExprNumOfExprs(pQueryInfo);
  pQueryMsg->numOfOutput = htons((int16_t)numOfOutput);  // this is the stage one output column number

  // set column list ids
  size_t numOfCols = taosArrayGetSize(pQueryInfo->colList);
  char *pMsg = (char *)(pQueryMsg->colList) + numOfCols * sizeof(SColumnInfo);
  SSchema *pSchema = tscGetTableSchema(pTableMeta);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumn *pCol = taosArrayGetP(pQueryInfo->colList, i);
    SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];

    if (pCol->colIndex.columnIndex >= tscGetNumOfColumns(pTableMeta) || !isValidDataType(pColSchema->type)) {
      char n[TSDB_TABLE_FNAME_LEN] = {0};
      tNameExtractFullName(&pTableMetaInfo->name, n);

      tscError("%p tid:%d uid:%" PRIu64" id:%s, column index out of range, numOfColumns:%d, index:%d, column name:%s",
          pSql, pTableMeta->id.tid, pTableMeta->id.uid, n, tscGetNumOfColumns(pTableMeta), pCol->colIndex.columnIndex,
               pColSchema->name);
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    pQueryMsg->colList[i].colId = htons(pColSchema->colId);
    pQueryMsg->colList[i].bytes = htons(pColSchema->bytes);
    pQueryMsg->colList[i].type  = htons(pColSchema->type);
    pQueryMsg->colList[i].numOfFilters = htons(pCol->numOfFilters);

    // append the filter information after the basic column information
    for (int32_t f = 0; f < pCol->numOfFilters; ++f) {
      SColumnFilterInfo *pColFilter = &pCol->filterInfo[f];

      SColumnFilterInfo *pFilterMsg = (SColumnFilterInfo *)pMsg;
      pFilterMsg->filterstr = htons(pColFilter->filterstr);

      pMsg += sizeof(SColumnFilterInfo);

      if (pColFilter->filterstr) {
        pFilterMsg->len = htobe64(pColFilter->len);
        memcpy(pMsg, (void *)pColFilter->pz, (size_t)(pColFilter->len + 1));
        pMsg += (pColFilter->len + 1);  // append the additional filter binary info
      } else {
        pFilterMsg->lowerBndi = htobe64(pColFilter->lowerBndi);
        pFilterMsg->upperBndi = htobe64(pColFilter->upperBndi);
      }

      pFilterMsg->lowerRelOptr = htons(pColFilter->lowerRelOptr);
      pFilterMsg->upperRelOptr = htons(pColFilter->upperRelOptr);

      if (pColFilter->lowerRelOptr == TSDB_RELATION_INVALID && pColFilter->upperRelOptr == TSDB_RELATION_INVALID) {
        tscError("invalid filter info");
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }
  }

  SSqlFuncMsg *pSqlFuncExpr = (SSqlFuncMsg *)pMsg;
  for (int32_t i = 0; i < tscSqlExprNumOfExprs(pQueryInfo); ++i) {
    SSqlExpr *pExpr = tscSqlExprGet(pQueryInfo, i);

    if (!tscValidateColumnId(pTableMetaInfo, pExpr->colInfo.colId, pExpr->numOfParams)) {
      tscError("%p table schema is not matched with parsed sql", pSql);
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    assert(pExpr->resColId < 0);

    pSqlFuncExpr->colInfo.colId    = htons(pExpr->colInfo.colId);
    pSqlFuncExpr->colInfo.colIndex = htons(pExpr->colInfo.colIndex);
    pSqlFuncExpr->colInfo.flag     = htons(pExpr->colInfo.flag);

    pSqlFuncExpr->functionId  = htons(pExpr->functionId);
    pSqlFuncExpr->numOfParams = htons(pExpr->numOfParams);
    pSqlFuncExpr->resColId    = htons(pExpr->resColId);
    pMsg += sizeof(SSqlFuncMsg);

    for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
      // todo add log
      pSqlFuncExpr->arg[j].argType = htons((uint16_t)pExpr->param[j].nType);
      pSqlFuncExpr->arg[j].argBytes = htons(pExpr->param[j].nLen);

      if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
        memcpy(pMsg, pExpr->param[j].pz, pExpr->param[j].nLen);
        pMsg += pExpr->param[j].nLen;
      } else {
        pSqlFuncExpr->arg[j].argValue.i64 = htobe64(pExpr->param[j].i64);
      }
    }

    pSqlFuncExpr = (SSqlFuncMsg *)pMsg;
  }

  size_t output = tscNumOfFields(pQueryInfo);

  if (tscIsSecondStageQuery(pQueryInfo)) {
    pQueryMsg->secondStageOutput = htonl((int32_t) output);

    SSqlFuncMsg *pSqlFuncExpr1 = (SSqlFuncMsg *)pMsg;

    for (int32_t i = 0; i < output; ++i) {
      SInternalField* pField = tscFieldInfoGetInternalField(&pQueryInfo->fieldsInfo, i);
      SSqlExpr *pExpr = pField->pSqlExpr;
      if (pExpr != NULL) {
        if (!tscValidateColumnId(pTableMetaInfo, pExpr->colInfo.colId, pExpr->numOfParams)) {
          tscError("%p table schema is not matched with parsed sql", pSql);
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        pSqlFuncExpr1->colInfo.colId    = htons(pExpr->colInfo.colId);
        pSqlFuncExpr1->colInfo.colIndex = htons(pExpr->colInfo.colIndex);
        pSqlFuncExpr1->colInfo.flag     = htons(pExpr->colInfo.flag);

        pSqlFuncExpr1->functionId  = htons(pExpr->functionId);
        pSqlFuncExpr1->numOfParams = htons(pExpr->numOfParams);
        pMsg += sizeof(SSqlFuncMsg);

        for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
          // todo add log
          pSqlFuncExpr1->arg[j].argType = htons((uint16_t)pExpr->param[j].nType);
          pSqlFuncExpr1->arg[j].argBytes = htons(pExpr->param[j].nLen);

          if (pExpr->param[j].nType == TSDB_DATA_TYPE_BINARY) {
            memcpy(pMsg, pExpr->param[j].pz, pExpr->param[j].nLen);
            pMsg += pExpr->param[j].nLen;
          } else {
            pSqlFuncExpr1->arg[j].argValue.i64 = htobe64(pExpr->param[j].i64);
          }
        }

        pSqlFuncExpr1 = (SSqlFuncMsg *)pMsg;
      } else {
        assert(pField->pArithExprInfo != NULL);
        SExprInfo* pExprInfo = pField->pArithExprInfo;

        pSqlFuncExpr1->colInfo.colId    = htons(pExprInfo->base.colInfo.colId);
        pSqlFuncExpr1->functionId  = htons(pExprInfo->base.functionId);
        pSqlFuncExpr1->numOfParams = htons(pExprInfo->base.numOfParams);
        pMsg += sizeof(SSqlFuncMsg);

        for (int32_t j = 0; j < pExprInfo->base.numOfParams; ++j) {
          // todo add log
          pSqlFuncExpr1->arg[j].argType = htons((uint16_t)pExprInfo->base.arg[j].argType);
          pSqlFuncExpr1->arg[j].argBytes = htons(pExprInfo->base.arg[j].argBytes);

          if (pExprInfo->base.arg[j].argType == TSDB_DATA_TYPE_BINARY) {
            memcpy(pMsg, pExprInfo->base.arg[j].argValue.pz, pExprInfo->base.arg[j].argBytes);
            pMsg += pExprInfo->base.arg[j].argBytes;
          } else {
            pSqlFuncExpr1->arg[j].argValue.i64 = htobe64(pExprInfo->base.arg[j].argValue.i64);
          }
        }

        pSqlFuncExpr1 = (SSqlFuncMsg *)pMsg;
      }
    }
  } else {
    pQueryMsg->secondStageOutput = 0;
  }

  // serialize the table info (sid, uid, tags)
  pMsg = doSerializeTableInfo(pQueryMsg, pSql, pMsg);
  
  SSqlGroupbyExpr *pGroupbyExpr = &pQueryInfo->groupbyExpr;
  if (pGroupbyExpr->numOfGroupCols > 0) {
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

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    for (int32_t i = 0; i < tscSqlExprNumOfExprs(pQueryInfo); ++i) {
      *((int64_t *)pMsg) = htobe64(pQueryInfo->fillVal[i]);
      pMsg += sizeof(pQueryInfo->fillVal[0]);
    }
  }
  
  if (numOfTags != 0) {
    int32_t numOfColumns = tscGetNumOfColumns(pTableMeta);
    int32_t numOfTagColumns = tscGetNumOfTags(pTableMeta);
    int32_t total = numOfTagColumns + numOfColumns;
    
    pSchema = tscGetTableTagSchema(pTableMeta);
    
    for (int32_t i = 0; i < numOfTags; ++i) {
      SColumn *pCol = taosArrayGetP(pTableMetaInfo->tagColList, i);
      SSchema *pColSchema = &pSchema[pCol->colIndex.columnIndex];

      if ((pCol->colIndex.columnIndex >= numOfTagColumns || pCol->colIndex.columnIndex < -1) ||
          (!isValidDataType(pColSchema->type))) {
        char n[TSDB_TABLE_FNAME_LEN] = {0};
        tNameExtractFullName(&pTableMetaInfo->name, n);

        tscError("%p tid:%d uid:%" PRIu64 " id:%s, tag index out of range, totalCols:%d, numOfTags:%d, index:%d, column name:%s",
                 pSql, pTableMeta->id.tid, pTableMeta->id.uid, n, total, numOfTagColumns, pCol->colIndex.columnIndex, pColSchema->name);

        return TSDB_CODE_TSC_INVALID_SQL;
      }
  
      SColumnInfo* pTagCol = (SColumnInfo*) pMsg;
  
      pTagCol->colId = htons(pColSchema->colId);
      pTagCol->bytes = htons(pColSchema->bytes);
      pTagCol->type  = htons(pColSchema->type);
      pTagCol->numOfFilters = 0;
      
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

  SCond* pCond = &pQueryInfo->tagCond.tbnameCond;
  if (pCond->len > 0) {
    strncpy(pMsg, pCond->cond, pCond->len);
    pMsg += pCond->len;
  }

  // compressed ts block
  pQueryMsg->tsOffset = htonl((int32_t)(pMsg - pCmd->payload));

  if (pQueryInfo->tsBuf != NULL) {
    // note: here used the index instead of actual vnode id.
    int32_t vnodeIndex = pTableMetaInfo->vgroupIndex;
    int32_t code = dumpFileBlockByGroupId(pQueryInfo->tsBuf, vnodeIndex, pMsg, &pQueryMsg->tsLen, &pQueryMsg->tsNumOfBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pMsg += pQueryMsg->tsLen;

    pQueryMsg->tsOrder = htonl(pQueryInfo->tsBuf->tsOrder);
    pQueryMsg->tsLen   = htonl(pQueryMsg->tsLen);
    pQueryMsg->tsNumOfBlocks = htonl(pQueryMsg->tsNumOfBlocks);
  }

  memcpy(pMsg, pSql->sqlstr, sqlLen);
  pMsg += sqlLen;

  int32_t msgLen = (int32_t)(pMsg - pCmd->payload);

  tscDebug("%p msg built success, len:%d bytes", pSql, msgLen);
  pCmd->payloadLen = msgLen;
  pSql->cmd.msgType = TSDB_MSG_TYPE_QUERY;
  
  pQueryMsg->head.contLen = htonl(msgLen);
  assert(msgLen + minMsgSize() <= (int32_t)pCmd->allocSize);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateDbMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_CREATE_DB;

  SCreateDbMsg *pCreateDbMsg = (SCreateDbMsg *)pCmd->payload;

  assert(pCmd->numOfClause == 1);
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pCreateDbMsg->db);
  assert(code == TSDB_CODE_SUCCESS);

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildCreateDnodeMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCreateDnodeMsg);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
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
    tscError("%p failed to malloc for query msg", pSql);
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
    tscError("%p failed to malloc for query msg", pSql);
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
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SDropDbMsg *pDropDbMsg = (SDropDbMsg*)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pDropDbMsg->db);
  assert(code == TSDB_CODE_SUCCESS && pTableMetaInfo->name.type == TSDB_DB_NAME_T);

  pDropDbMsg->ignoreNotExists = pInfo->pMiscInfo->existsCheck ? 1 : 0;

  pCmd->msgType = TSDB_MSG_TYPE_CM_DROP_DB;
  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildDropTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SCMDropTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SCMDropTableMsg *pDropTableMsg = (SCMDropTableMsg*)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  tNameExtractFullName(&pTableMetaInfo->name, pDropTableMsg->name);

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
    tscError("%p failed to malloc for query msg", pSql);
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
    tscError("%p failed to malloc for query msg", pSql);
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
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SUseDbMsg *pUseDbMsg = (SUseDbMsg *)pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  tNameExtractFullName(&pTableMetaInfo->name, pUseDbMsg->db);
  pCmd->msgType = TSDB_MSG_TYPE_CM_USE_DB;

  return TSDB_CODE_SUCCESS;
}

int32_t tscBuildShowMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  STscObj *pObj = pSql->pTscObj;
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_SHOW;
  pCmd->payloadLen = sizeof(SShowMsg) + 100;

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SShowMsg *pShowMsg = (SShowMsg *)pCmd->payload;

  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

  if (tNameIsEmpty(&pTableMetaInfo->name)) {
    tstrncpy(pShowMsg->db, pObj->db, sizeof(pShowMsg->db));
  } else {
    tNameGetFullDbName(&pTableMetaInfo->name, pShowMsg->db);
  }

  SShowInfo *pShowInfo = &pInfo->pMiscInfo->showOpt;
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

  SCreateTableSQL *pCreateTableInfo = pInfo->pCreateTableInfo;
  if (pCreateTableInfo->type == TSQL_CREATE_TABLE_FROM_STABLE) {
    int32_t numOfTables = (int32_t)taosArrayGetSize(pInfo->pCreateTableInfo->childTableInfo);
    size += numOfTables * (sizeof(SCreateTableMsg) + TSDB_MAX_TAGS_LEN);
  } else {
    size += sizeof(SSchema) * (pCmd->numOfCols + pCmd->count);
  }

  if (pCreateTableInfo->pSelect != NULL) {
    size += (pCreateTableInfo->pSelect->selectToken.n + 1);
  }

  return size + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildCreateTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  int      msgLen = 0;
  int      size = 0;
  SSchema *pSchema;
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo     *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // Reallocate the payload size
  size = tscEstimateCreateTableMsgLength(pSql, pInfo);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for create table msg", pSql);
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

    SCreateTableSQL *pCreateTable = pInfo->pCreateTableInfo;

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
      SQuerySQL *pQuerySql = pInfo->pCreateTableInfo->pSelect;

      strncpy(pMsg, pQuerySql->selectToken.z, pQuerySql->selectToken.n + 1);
      pCreateMsg->sqlLen = htons(pQuerySql->selectToken.n + 1);
      pMsg += pQuerySql->selectToken.n + 1;
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
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  return minMsgSize() + sizeof(SAlterTableMsg) + sizeof(SSchema) * tscNumOfFields(pQueryInfo) + TSDB_EXTRA_PAYLOAD_SIZE;
}

int tscBuildAlterTableMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  char *pMsg;
  int   msgLen = 0;

  SSqlCmd    *pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  SAlterTableInfo *pAlterInfo = pInfo->pAlterInfo;
  int size = tscEstimateAlterTableMsgLength(pCmd);
  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, size)) {
    tscError("%p failed to malloc for alter table msg", pSql);
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
  memcpy(pMsg, pAlterInfo->tagData.data, pAlterInfo->tagData.dataLen);
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

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMeta *pTableMeta = tscGetMetaInfo(pQueryInfo, 0)->pTableMeta;

  SNewVgroupInfo vgroupInfo = {.vgId = -1};
  taosHashGetClone(tscVgroupMap, &pTableMeta->vgId, sizeof(pTableMeta->vgId), NULL, &vgroupInfo, sizeof(SNewVgroupInfo));
  assert(vgroupInfo.vgId > 0);

  tscDumpEpSetFromVgroupInfo(&pSql->epSet, &vgroupInfo);

  return TSDB_CODE_SUCCESS;
}

int tscAlterDbMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->payloadLen = sizeof(SAlterDbMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_ALTER_DB;

  SAlterDbMsg *pAlterDbMsg = (SAlterDbMsg* )pCmd->payload;
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);
  tNameExtractFullName(&pTableMetaInfo->name, pAlterDbMsg->db);

  return TSDB_CODE_SUCCESS;
}

int tscBuildRetrieveFromMgmtMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  pCmd->msgType = TSDB_MSG_TYPE_CM_RETRIEVE;
  pCmd->payloadLen = sizeof(SRetrieveTableMsg);

  if (TSDB_CODE_SUCCESS != tscAllocPayload(pCmd, pCmd->payloadLen)) {
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  SRetrieveTableMsg *pRetrieveMsg = (SRetrieveTableMsg*)pCmd->payload;
  pRetrieveMsg->qhandle = htobe64(pSql->res.qhandle);
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

    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
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
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, 0);

  STableComInfo tinfo = tscGetTableInfo(pTableMetaInfo->pTableMeta);
  
  int32_t numOfRes = tinfo.numOfColumns + tinfo.numOfTags;
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessLocalRetrieveRsp(SSqlObj *pSql) {
  int32_t numOfRes = 1;
  pSql->res.completed = true;
  return tscLocalResultCommonBuilder(pSql, numOfRes);
}

int tscProcessRetrieveLocalMergeRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd* pCmd = &pSql->cmd;

  int32_t code = pRes->code;
  if (pRes->code != TSDB_CODE_SUCCESS) {
    tscAsyncResultOnError(pSql);
    return code;
  }

  pRes->code = tscDoLocalMerge(pSql);

  if (pRes->code == TSDB_CODE_SUCCESS && pRes->numOfRows > 0) {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
    tscCreateResPointerInfo(pRes, pQueryInfo);
    tscSetResRawPtr(pRes, pQueryInfo);
  }

  pRes->row = 0;
  pRes->completed = (pRes->numOfRows == 0);

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
    tscError("%p failed to malloc for query msg", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  SConnectMsg *pConnect = (SConnectMsg*)pCmd->payload;

  // TODO refactor full_name
  char *db;  // ugly code to move the space
  db = strstr(pObj->db, TS_PATH_DELIMITER);
  db = (db == NULL) ? pObj->db : db + 1;
  tstrncpy(pConnect->db, db, sizeof(pConnect->db));
  tstrncpy(pConnect->clientVersion, version, sizeof(pConnect->clientVersion));
  tstrncpy(pConnect->msgVersion, "", sizeof(pConnect->msgVersion));

  pConnect->pid = htonl(taosGetPId());
  taosGetCurrentAPPName(pConnect->appName, NULL);

  return TSDB_CODE_SUCCESS;
}

int tscBuildTableMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd    *pCmd = &pSql->cmd;
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(&pSql->cmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  STableInfoMsg  *pInfoMsg = (STableInfoMsg *)pCmd->payload;

  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pInfoMsg->tableFname);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  pInfoMsg->createFlag = htons(pSql->cmd.autoCreated ? 1 : 0);

  char *pMsg = (char *)pInfoMsg + sizeof(STableInfoMsg);

  if (pCmd->autoCreated && pCmd->tagData.dataLen != 0) {
    pMsg = serializeTagData(&pCmd->tagData, pMsg);
  }

  pCmd->payloadLen = (int32_t)(pMsg - (char*)pInfoMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLE_META;

  return TSDB_CODE_SUCCESS;
}

/**
 *  multi table meta req pkg format:
 *  | SMgmtHead | SMultiTableInfoMsg | tableId0 | tableId1 | tableId2 | ......
 *      no used         4B
 **/
int tscBuildMultiMeterMetaMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
#if 0
  SSqlCmd *pCmd = &pSql->cmd;

  // copy payload content to temp buff
  char *tmpData = 0;
  if (pCmd->payloadLen > 0) {
    if ((tmpData = calloc(1, pCmd->payloadLen + 1)) == NULL) return -1;
    memcpy(tmpData, pCmd->payload, pCmd->payloadLen);
  }

  // fill head info
  SMgmtHead *pMgmt = (SMgmtHead *)(pCmd->payload + tsRpcHeadSize);
  memset(pMgmt->db, 0, TSDB_TABLE_FNAME_LEN);  // server don't need the db

  SMultiTableInfoMsg *pInfoMsg = (SMultiTableInfoMsg *)(pCmd->payload + tsRpcHeadSize + sizeof(SMgmtHead));
  pInfoMsg->numOfTables = htonl((int32_t)pCmd->count);

  if (pCmd->payloadLen > 0) {
    memcpy(pInfoMsg->tableIds, tmpData, pCmd->payloadLen);
  }

  tfree(tmpData);

  pCmd->payloadLen += sizeof(SMgmtHead) + sizeof(SMultiTableInfoMsg);
  pCmd->msgType = TSDB_MSG_TYPE_CM_TABLES_META;

  assert(pCmd->payloadLen + minMsgSize() <= pCmd->allocSize);

  tscDebug("%p build load multi-metermeta msg completed, numOfTables:%d, msg size:%d", pSql, pCmd->count,
           pCmd->payloadLen);

  return pCmd->payloadLen;
#endif
  return 0;  
}

int tscBuildSTableVgroupMsg(SSqlObj *pSql, SSqlInfo *pInfo) {
  SSqlCmd *pCmd = &pSql->cmd;
  
  char* pMsg = pCmd->payload;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  SSTableVgroupMsg *pStableVgroupMsg = (SSTableVgroupMsg *)pMsg;
  pStableVgroupMsg->numOfTables = htonl(pQueryInfo->numOfTables);
  pMsg += sizeof(SSTableVgroupMsg);

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, i);
    int32_t code = tNameExtractFullName(&pTableMetaInfo->name, pMsg);
    assert(code == TSDB_CODE_SUCCESS);

    pMsg += TSDB_TABLE_FNAME_LEN;
  }

  pCmd->msgType = TSDB_MSG_TYPE_CM_STABLE_VGROUP;
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
    tscError("%p failed to create heartbeat msg", pSql);
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

int tscProcessTableMetaRsp(SSqlObj *pSql) {
  STableMetaMsg *pMetaMsg = (STableMetaMsg *)pSql->res.pRsp;

  pMetaMsg->tid = htonl(pMetaMsg->tid);
  pMetaMsg->sversion = htons(pMetaMsg->sversion);
  pMetaMsg->tversion = htons(pMetaMsg->tversion);
  pMetaMsg->vgroup.vgId = htonl(pMetaMsg->vgroup.vgId);
  
  pMetaMsg->uid = htobe64(pMetaMsg->uid);
  pMetaMsg->contLen = htons(pMetaMsg->contLen);
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
  
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  assert(pTableMetaInfo->pTableMeta == NULL);

  STableMeta* pTableMeta = tscCreateTableMetaFromMsg(pMetaMsg);
  if (!tIsValidSchema(pTableMeta->schema, pTableMeta->tableInfo.numOfColumns, pTableMeta->tableInfo.numOfTags)) {
    tscError("%p invalid table meta from mnode, name:%s", pSql, tNameGetTableName(&pTableMetaInfo->name));
    return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (pTableMeta->tableType == TSDB_CHILD_TABLE) {
    // check if super table hashmap or not
    int32_t len = (int32_t) strnlen(pTableMeta->sTableName, TSDB_TABLE_FNAME_LEN);

    // super tableMeta data alreay exists, create it according to tableMeta and add it to hash map
    STableMeta* pSupTableMeta = createSuperTableMeta(pMetaMsg);

    uint32_t size = tscGetTableMetaSize(pSupTableMeta);
    int32_t code = taosHashPut(tscTableMetaInfo, pTableMeta->sTableName, len, pSupTableMeta, size);
    assert(code == TSDB_CODE_SUCCESS);

    tfree(pSupTableMeta);

    CChildTableMeta* cMeta = tscCreateChildMeta(pTableMeta);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(&pTableMetaInfo->name, name);

    taosHashPut(tscTableMetaInfo, name, strlen(name), cMeta, sizeof(CChildTableMeta));
    tfree(cMeta);
  } else {
    uint32_t s = tscGetTableMetaSize(pTableMeta);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    tNameExtractFullName(&pTableMetaInfo->name, name);

    taosHashPut(tscTableMetaInfo, name, strlen(name), pTableMeta, s);
  }

  // update the vgroupInfo if needed
  if (pTableMeta->vgId > 0) {
    int32_t vgId = pTableMeta->vgId;
    assert(pTableMeta->tableType != TSDB_SUPER_TABLE);

    SNewVgroupInfo vgroupInfo = {.inUse = -1};
    taosHashGetClone(tscVgroupMap, &vgId, sizeof(vgId), NULL, &vgroupInfo, sizeof(SNewVgroupInfo));

    if (((vgroupInfo.inUse >= 0) && !vgroupInfoIdentical(&vgroupInfo, &pMetaMsg->vgroup)) ||
        (vgroupInfo.inUse < 0)) {  // vgroup info exists, compare with it
      vgroupInfo = createNewVgroupInfo(&pMetaMsg->vgroup);
      taosHashPut(tscVgroupMap, &vgId, sizeof(vgId), &vgroupInfo, sizeof(vgroupInfo));
      tscDebug("add new VgroupInfo, vgId:%d, total:%d", vgId, (int32_t) taosHashGetSize(tscVgroupMap));
    }
  }

  tscDebug("%p recv table meta, uid:%" PRIu64 ", tid:%d, name:%s", pSql, pTableMeta->id.uid, pTableMeta->id.tid,
           tNameGetTableName(&pTableMetaInfo->name));

  free(pTableMeta);
  return TSDB_CODE_SUCCESS;
}

/**
 *  multi table meta rsp pkg format:
 *  | STaosRsp | SMultiTableInfoMsg | SMeterMeta0 | SSchema0 | SMeterMeta1 | SSchema1 | SMeterMeta2 | SSchema2
 *  |...... 1B                    4B
 **/
int tscProcessMultiMeterMetaRsp(SSqlObj *pSql) {
#if 0
  char *rsp = pSql->res.pRsp;

  ieType = *rsp;
  if (ieType != TSDB_IE_TYPE_META) {
    tscError("invalid ie type:%d", ieType);
    pSql->res.code = TSDB_CODE_TSC_INVALID_IE;
    pSql->res.numOfTotal = 0;
    return TSDB_CODE_TSC_APP_ERROR;
  }

  rsp++;

  SMultiTableInfoMsg *pInfo = (SMultiTableInfoMsg *)rsp;
  totalNum = htonl(pInfo->numOfTables);
  rsp += sizeof(SMultiTableInfoMsg);

  for (i = 0; i < totalNum; i++) {
    SMultiTableMeta *pMultiMeta = (SMultiTableMeta *)rsp;
    STableMeta *     pMeta = pMultiMeta->metas;

    pMeta->sid = htonl(pMeta->sid);
    pMeta->sversion = htons(pMeta->sversion);
    pMeta->vgId = htonl(pMeta->vgId);
    pMeta->uid = htobe64(pMeta->uid);

    if (pMeta->sid <= 0 || pMeta->vgId < 0) {
      tscError("invalid meter vgId:%d, sid%d", pMeta->vgId, pMeta->sid);
      pSql->res.code = TSDB_CODE_TSC_INVALID_VALUE;
      pSql->res.numOfTotal = i;
      return TSDB_CODE_TSC_APP_ERROR;
    }

    //    pMeta->numOfColumns = htons(pMeta->numOfColumns);
    //
    //    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    //      tscError("invalid tag value count:%d", pMeta->numOfTags);
    //      pSql->res.code = TSDB_CODE_TSC_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_TSC_APP_ERROR;
    //    }
    //
    //    if (pMeta->numOfTags > TSDB_MAX_TAGS || pMeta->numOfTags < 0) {
    //      tscError("invalid numOfTags:%d", pMeta->numOfTags);
    //      pSql->res.code = TSDB_CODE_TSC_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_TSC_APP_ERROR;
    //    }
    //
    //    if (pMeta->numOfColumns > TSDB_MAX_COLUMNS || pMeta->numOfColumns < 0) {
    //      tscError("invalid numOfColumns:%d", pMeta->numOfColumns);
    //      pSql->res.code = TSDB_CODE_TSC_INVALID_VALUE;
    //      pSql->res.numOfTotal = i;
    //      return TSDB_CODE_TSC_APP_ERROR;
    //    }
    //
    //    for (int j = 0; j < TSDB_REPLICA_MAX_NUM; ++j) {
    //      pMeta->vpeerDesc[j].vnode = htonl(pMeta->vpeerDesc[j].vnode);
    //    }
    //
    //    pMeta->rowSize = 0;
    //    rsp += sizeof(SMultiTableMeta);
    //    pSchema = (SSchema *)rsp;
    //
    //    int32_t numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
    //    for (int j = 0; j < numOfTotalCols; ++j) {
    //      pSchema->bytes = htons(pSchema->bytes);
    //      pSchema->colId = htons(pSchema->colId);
    //
    //      // ignore the tags length
    //      if (j < pMeta->numOfColumns) {
    //        pMeta->rowSize += pSchema->bytes;
    //      }
    //      pSchema++;
    //    }
    //
    //    rsp += numOfTotalCols * sizeof(SSchema);
    //
    //    int32_t  tagLen = 0;
    //    SSchema *pTagsSchema = tscGetTableTagSchema(pMeta);
    //
    //    if (pMeta->tableType == TSDB_CHILD_TABLE) {
    //      for (int32_t j = 0; j < pMeta->numOfTags; ++j) {
    //        tagLen += pTagsSchema[j].bytes;
    //      }
    //    }
    //
    //    rsp += tagLen;
    //    int32_t size = (int32_t)(rsp - ((char *)pMeta));  // Consistent with STableMeta in cache
    //  }
  }
  
  pSql->res.code = TSDB_CODE_SUCCESS;
  pSql->res.numOfTotal = i;
  tscDebug("%p load multi-metermeta resp from complete num:%d", pSql, pSql->res.numOfTotal);
#endif
  
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
  for(int32_t i = 0; i < pStableVgroup->numOfTables; ++i) {
    STableMetaInfo *pInfo = tscGetTableMetaInfoFromCmd(pCmd, pCmd->clauseIndex, i);

    SVgroupsMsg *  pVgroupMsg = (SVgroupsMsg *) pMsg;
    pVgroupMsg->numOfVgroups = htonl(pVgroupMsg->numOfVgroups);

    size_t size = sizeof(SVgroupMsg) * pVgroupMsg->numOfVgroups + sizeof(SVgroupsMsg);

    size_t vgroupsz = sizeof(SVgroupInfo) * pVgroupMsg->numOfVgroups + sizeof(SVgroupsInfo);
    pInfo->vgroupList = calloc(1, vgroupsz);
    assert(pInfo->vgroupList != NULL);

    pInfo->vgroupList->numOfVgroups = pVgroupMsg->numOfVgroups;
    for (int32_t j = 0; j < pInfo->vgroupList->numOfVgroups; ++j) {
      //just init, no need to lock
      SVgroupInfo *pVgroups = &pInfo->vgroupList->vgroups[j];

      SVgroupMsg *vmsg = &pVgroupMsg->vgroups[j];
      pVgroups->vgId = htonl(vmsg->vgId);
      pVgroups->numOfEps = vmsg->numOfEps;

      assert(pVgroups->numOfEps >= 1 && pVgroups->vgId >= 1);

      for (int32_t k = 0; k < pVgroups->numOfEps; ++k) {
        pVgroups->epAddr[k].port = htons(vmsg->epAddr[k].port);
        pVgroups->epAddr[k].fqdn = strndup(vmsg->epAddr[k].fqdn, tListLen(vmsg->epAddr[k].fqdn));
      }
    }

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

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  pShow = (SShowRsp *)pRes->pRsp;
  pShow->qhandle = htobe64(pShow->qhandle);
  pRes->qhandle = pShow->qhandle;

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
  
  for (int16_t i = 0; i < pMetaMsg->numOfColumns; ++i, ++pSchema) {
    index.columnIndex = i;
    tscColumnListInsert(pQueryInfo->colList, &index);
    
    TAOS_FIELD f = tscCreateField(pSchema->type, pSchema->name, pSchema->bytes);
    SInternalField* pInfo = tscFieldInfoAppend(pFieldInfo, &f);
    
    pInfo->pSqlExpr = tscSqlExprAppend(pQueryInfo, TSDB_FUNC_TS_DUMMY, &index,
                     pTableSchema[i].type, pTableSchema[i].bytes, getNewResColId(pQueryInfo), pTableSchema[i].bytes, false);
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

  SQueryInfo *pQueryInfo = tscGetQueryInfoDetailSafely(&pSql->cmd, 0);
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
  tscDebug("%p HB is allocated, pObj:%p", pSql, pObj);

  pObj->hbrid = pSql->self;
}

int tscProcessConnectRsp(SSqlObj *pSql) {
  STscObj *pObj = pSql->pTscObj;
  SSqlRes *pRes = &pSql->res;

  char temp[TSDB_TABLE_FNAME_LEN * 2] = {0};

  SConnectRsp *pConnect = (SConnectRsp *)pRes->pRsp;
  tstrncpy(pObj->acctId, pConnect->acctId, sizeof(pObj->acctId));  // copy acctId from response
  int32_t len = sprintf(temp, "%s%s%s", pObj->acctId, TS_PATH_DELIMITER, pObj->db);

  assert(len <= sizeof(pObj->db));
  tstrncpy(pObj->db, temp, sizeof(pObj->db));
  
  if (pConnect->epSet.numOfEps > 0) {
    tscEpSetHtons(&pConnect->epSet);
    tscUpdateMgmtEpSet(pSql, &pConnect->epSet);

    for (int i = 0; i < pConnect->epSet.numOfEps; ++i) {
      tscDebug("%p epSet.fqdn[%d]: %s, pObj:%p", pSql, i, pConnect->epSet.fqdn[i], pObj);
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
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);
  return tNameExtractFullName(&pTableMetaInfo->name, pObj->db);
}

int tscProcessDropDbRsp(SSqlObj *pSql) {
  pSql->pTscObj->db[0] = 0;
  taosHashEmpty(tscTableMetaInfo);
  return 0;
}

int tscProcessDropTableRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  //The cached tableMeta is expired in this case, so clean it in hash table
  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  taosHashRemove(tscTableMetaInfo, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
  tscDebug("%p remove table meta after drop table:%s, numOfRemain:%d", pSql, name, (int32_t) taosHashGetSize(tscTableMetaInfo));

  pTableMetaInfo->pTableMeta = NULL;
  return 0;
}

int tscProcessAlterTableMsgRsp(SSqlObj *pSql) {
  STableMetaInfo *pTableMetaInfo = tscGetTableMetaInfoFromCmd(&pSql->cmd, 0, 0);

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  tscDebug("%p remove tableMeta in hashMap after alter-table: %s", pSql, name);

  bool isSuperTable = UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  taosHashRemove(tscTableMetaInfo, name, strnlen(name, TSDB_TABLE_FNAME_LEN));
  tfree(pTableMetaInfo->pTableMeta);

  if (isSuperTable) {  // if it is a super table, iterate the hashTable and remove all the childTableMeta
    taosHashEmpty(tscTableMetaInfo);
  }

  return 0;
}

int tscProcessAlterDbMsgRsp(SSqlObj *pSql) {
  UNUSED(pSql);
  return 0;
}

int tscProcessShowCreateRsp(SSqlObj *pSql) {
  return tscLocalResultCommonBuilder(pSql, 1);
}

int tscProcessQueryRsp(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;

  SQueryTableRsp *pQuery = (SQueryTableRsp *)pRes->pRsp;
  pQuery->qhandle = htobe64(pQuery->qhandle);
  pRes->qhandle = pQuery->qhandle;

  pRes->data = NULL;
  tscResetForNextRetrieve(pRes);
  return 0;
}

int tscProcessRetrieveRspFromNode(SSqlObj *pSql) {
  SSqlRes *pRes = &pSql->res;
  SSqlCmd *pCmd = &pSql->cmd;

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
  
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  if (tscCreateResPointerInfo(pRes, pQueryInfo) != TSDB_CODE_SUCCESS) {
    return pRes->code;
  }

  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  if (pCmd->command == TSDB_SQL_RETRIEVE) {
    tscSetResRawPtr(pRes, pQueryInfo);
  } else if ((UTIL_TABLE_IS_CHILD_TABLE(pTableMetaInfo) || UTIL_TABLE_IS_NORMAL_TABLE(pTableMetaInfo)) && !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_SUBQUERY)) {
    tscSetResRawPtr(pRes, pQueryInfo);
  } else if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_QUERY) && !TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
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
  tscDebug("%p numOfRows:%d, offset:%" PRId64 ", complete:%d", pSql, pRes->numOfRows, pRes->offset, pRes->completed);

  return 0;
}

void tscTableMetaCallBack(void *param, TAOS_RES *res, int code);

static int32_t getTableMetaFromMnode(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo) {
  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  if (NULL == pNew) {
    tscError("%p malloc failed for new sqlobj to get table meta", pSql);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;
  pNew->cmd.command = TSDB_SQL_META;

  tscAddSubqueryInfo(&pNew->cmd);

  SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetailSafely(&pNew->cmd, 0);

  pNew->cmd.autoCreated = pSql->cmd.autoCreated;  // create table if not exists
  if (TSDB_CODE_SUCCESS != tscAllocPayload(&pNew->cmd, TSDB_DEFAULT_PAYLOAD_SIZE + pSql->cmd.payloadLen)) {
    tscError("%p malloc failed for payload to get table meta", pSql);
    tscFreeSqlObj(pNew);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  STableMetaInfo *pNewMeterMetaInfo = tscAddEmptyMetaInfo(pNewQueryInfo);
  assert(pNew->cmd.numOfClause == 1 && pNewQueryInfo->numOfTables == 1);

  tNameAssign(&pNewMeterMetaInfo->name, &pTableMetaInfo->name);

  if (pSql->cmd.autoCreated) {
    int32_t code = copyTagData(&pNew->cmd.tagData, &pSql->cmd.tagData);
    if (code != TSDB_CODE_SUCCESS) {
      tscError("%p malloc failed for new tag data to get table meta", pSql);
      tscFreeSqlObj(pNew);
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  tscDebug("%p new pSqlObj:%p to get tableMeta, auto create:%d", pSql, pNew, pNew->cmd.autoCreated);

  registerSqlObj(pNew);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = (void *)pSql->self;

  tscDebug("%p metaRid from %" PRId64 " to %" PRId64 , pSql, pSql->metaRid, pNew->self);
  
  pSql->metaRid = pNew->self;

  int32_t code = tscProcessSql(pNew);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TSC_ACTION_IN_PROGRESS;  // notify application that current process needs to be terminated
  }

  return code;
}

int32_t tscGetTableMeta(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo) {
  assert(tIsValidName(&pTableMetaInfo->name));

  tfree(pTableMetaInfo->pTableMeta);

  uint32_t size = tscGetTableMetaMaxSize();
  pTableMetaInfo->pTableMeta = calloc(1, size);

  pTableMetaInfo->pTableMeta->tableType = -1;
  pTableMetaInfo->pTableMeta->tableInfo.numOfColumns  = -1;

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  size_t len = strlen(name);
  taosHashGetClone(tscTableMetaInfo, name, len, NULL, pTableMetaInfo->pTableMeta, -1);

  // TODO resize the tableMeta
  STableMeta* pMeta = pTableMetaInfo->pTableMeta;
  if (pMeta->id.uid > 0) {
    if (pMeta->tableType == TSDB_CHILD_TABLE) {
      int32_t code = tscCreateTableMetaFromCChildMeta(pTableMetaInfo->pTableMeta, name);
      if (code != TSDB_CODE_SUCCESS) {
        return getTableMetaFromMnode(pSql, pTableMetaInfo);
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  return getTableMetaFromMnode(pSql, pTableMetaInfo);
}

int tscGetTableMetaEx(SSqlObj *pSql, STableMetaInfo *pTableMetaInfo, bool createIfNotExists) {
  pSql->cmd.autoCreated = createIfNotExists;
  return tscGetTableMeta(pSql, pTableMetaInfo);
}

/**
 * retrieve table meta from mnode, and update the local table meta hashmap.
 * @param pSql          sql object
 * @param tableIndex    table index
 * @return              status code
 */
int tscRenewTableMeta(SSqlObj *pSql, int32_t tableIndex) {
  SSqlCmd *pCmd = &pSql->cmd;

  SQueryInfo     *pQueryInfo = tscGetQueryInfoDetail(pCmd, 0);
  STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  int32_t code = tNameExtractFullName(&pTableMetaInfo->name, name);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("%p failed to generate the table full name", pSql);
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  STableMeta* pTableMeta = pTableMetaInfo->pTableMeta;
  if (pTableMeta) {
    tscDebug("%p update table meta:%s, old meta numOfTags:%d, numOfCols:%d, uid:%" PRId64, pSql, name,
             tscGetNumOfTags(pTableMeta), tscGetNumOfColumns(pTableMeta), pTableMeta->id.uid);
  }

  // remove stored tableMeta info in hash table
  size_t len = strlen(name);
  taosHashRemove(tscTableMetaInfo, name, len);

  return getTableMetaFromMnode(pSql, pTableMetaInfo);
}

static bool allVgroupInfoRetrieved(SSqlCmd* pCmd, int32_t clauseIndex) {
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo *pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);
    if (pTableMetaInfo->vgroupList == NULL) {
      return false;
    }
  }
  
  // all super tables vgroupinfo are retrieved, no need to retrieve vgroup info anymore
  return true;
}

int tscGetSTableVgroupInfo(SSqlObj *pSql, int32_t clauseIndex) {
  int      code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
  SSqlCmd *pCmd = &pSql->cmd;
  
  if (allVgroupInfoRetrieved(pCmd, clauseIndex)) {
    return TSDB_CODE_SUCCESS;
  }

  SSqlObj *pNew = calloc(1, sizeof(SSqlObj));
  pNew->pTscObj = pSql->pTscObj;
  pNew->signature = pNew;

  pNew->cmd.command = TSDB_SQL_STABLEVGROUP;

  // TODO TEST IT
  SQueryInfo *pNewQueryInfo = tscGetQueryInfoDetailSafely(&pNew->cmd, 0);
  if (pNewQueryInfo == NULL) {
    tscFreeSqlObj(pNew);
    return code;
  }
  
  SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, clauseIndex);
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

  tscDebug("%p svgroupRid from %" PRId64 " to %" PRId64 , pSql, pSql->svgroupRid, pNew->self);
  
  pSql->svgroupRid = pNew->self;
  

  tscDebug("%p new sqlObj:%p to get vgroupInfo, numOfTables:%d", pSql, pNew, pNewQueryInfo->numOfTables);

  pNew->fp = tscTableMetaCallBack;
  pNew->param = (void *)pSql->self;
  code = tscProcessSql(pNew);
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

  tscBuildMsg[TSDB_SQL_CREATE_ACCT] = tscBuildAcctMsg;
  tscBuildMsg[TSDB_SQL_ALTER_ACCT] = tscBuildAcctMsg;

  tscBuildMsg[TSDB_SQL_CREATE_TABLE] = tscBuildCreateTableMsg;
  tscBuildMsg[TSDB_SQL_DROP_USER] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_ACCT] = tscBuildDropUserAcctMsg;
  tscBuildMsg[TSDB_SQL_DROP_DB] = tscBuildDropDbMsg;
  tscBuildMsg[TSDB_SQL_DROP_TABLE] = tscBuildDropTableMsg;
  tscBuildMsg[TSDB_SQL_ALTER_USER] = tscBuildUserMsg;
  tscBuildMsg[TSDB_SQL_CREATE_DNODE] = tscBuildCreateDnodeMsg;
  tscBuildMsg[TSDB_SQL_DROP_DNODE] = tscBuildDropDnodeMsg;
  tscBuildMsg[TSDB_SQL_CFG_DNODE] = tscBuildCfgDnodeMsg;
  tscBuildMsg[TSDB_SQL_ALTER_TABLE] = tscBuildAlterTableMsg;
  tscBuildMsg[TSDB_SQL_UPDATE_TAGS_VAL] = tscBuildUpdateTagMsg;
  tscBuildMsg[TSDB_SQL_ALTER_DB] = tscAlterDbMsg;

  tscBuildMsg[TSDB_SQL_CONNECT] = tscBuildConnectMsg;
  tscBuildMsg[TSDB_SQL_USE_DB] = tscBuildUseDbMsg;
  tscBuildMsg[TSDB_SQL_META] = tscBuildTableMetaMsg;
  tscBuildMsg[TSDB_SQL_STABLEVGROUP] = tscBuildSTableVgroupMsg;
  tscBuildMsg[TSDB_SQL_MULTI_META] = tscBuildMultiMeterMetaMsg;

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
  tscProcessMsgRsp[TSDB_SQL_MULTI_META] = tscProcessMultiMeterMetaRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW] = tscProcessShowRsp;
  tscProcessMsgRsp[TSDB_SQL_RETRIEVE] = tscProcessRetrieveRspFromNode;  // rsp handled by same function.
  tscProcessMsgRsp[TSDB_SQL_DESCRIBE_TABLE] = tscProcessDescribeTableRsp;

  tscProcessMsgRsp[TSDB_SQL_CURRENT_DB]   = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CURRENT_USER] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_VERSION] = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_CLI_VERSION]  = tscProcessLocalRetrieveRsp;
  tscProcessMsgRsp[TSDB_SQL_SERV_STATUS]  = tscProcessLocalRetrieveRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_EMPTY_RESULT] = tscProcessEmptyResultRsp;

  tscProcessMsgRsp[TSDB_SQL_RETRIEVE_LOCALMERGE] = tscProcessRetrieveLocalMergeRsp;

  tscProcessMsgRsp[TSDB_SQL_ALTER_TABLE] = tscProcessAlterTableMsgRsp;
  tscProcessMsgRsp[TSDB_SQL_ALTER_DB] = tscProcessAlterDbMsgRsp;

  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_TABLE] = tscProcessShowCreateRsp;
  tscProcessMsgRsp[TSDB_SQL_SHOW_CREATE_DATABASE] = tscProcessShowCreateRsp;
  

  tscKeepConn[TSDB_SQL_SHOW] = 1;
  tscKeepConn[TSDB_SQL_RETRIEVE] = 1;
  tscKeepConn[TSDB_SQL_SELECT] = 1;
  tscKeepConn[TSDB_SQL_FETCH] = 1;
  tscKeepConn[TSDB_SQL_HB] = 1;
}
