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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosmsg.h"
#include "tglobal.h"
// #include "query.h"
#include "vnodeMain.h"
#include "vnodeRead.h"
#include "vnodeReadMsg.h"
#include "vnodeStatus.h"

static struct {
  SWorkerPool query;
  SWorkerPool fetch;
  int32_t (*msgFp[TSDB_MSG_TYPE_MAX])(SVnode *, struct SReadMsg *);
} tsVread = {0};

void vnodeStartRead(SVnode *pVnode) {}
void vnodeStopRead(SVnode *pVnode) {}

void vnodeWaitReadCompleted(SVnode *pVnode) {
  while (pVnode->queuedRMsg > 0) {
    vTrace("vgId:%d, queued rmsg num:%d", pVnode->vgId, pVnode->queuedRMsg);
    taosMsleep(10);
  }
}

static int32_t vnodeWriteToRQueue(SVnode *pVnode, void *pCont, int32_t contLen, int8_t qtype, SRpcMsg *pRpcMsg) {
  if (pVnode->dropped) {
    return TSDB_CODE_APP_NOT_READY;
  }

#if 0
  if (!((pVnode->role == TAOS_SYNC_ROLE_MASTER) || (tsEnableSlaveQuery && pVnode->role == TAOS_SYNC_ROLE_SLAVE))) {
    return TSDB_CODE_APP_NOT_READY;
  }
#endif  

  if (!vnodeInReadyStatus(pVnode)) {
    vDebug("vgId:%d, failed to write into vread queue, vnode status is %s", pVnode->vgId, vnodeStatus[pVnode->status]);
    return TSDB_CODE_APP_NOT_READY;
  }

  int32_t   size = sizeof(SReadMsg) + contLen;
  SReadMsg *pRead = taosAllocateQitem(size);
  if (pRead == NULL) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  if (pRpcMsg != NULL) {
    pRead->rpcHandle = pRpcMsg->handle;
    pRead->rpcAhandle = pRpcMsg->ahandle;
    pRead->msgType = pRpcMsg->msgType;
    pRead->code = pRpcMsg->code;
  }

  if (contLen != 0) {
    pRead->contLen = contLen;
    memcpy(pRead->pCont, pCont, contLen);
  } else {
    pRead->qhandle = pCont;
  }

  pRead->qtype = qtype;

  atomic_add_fetch_32(&pVnode->refCount, 1);
  atomic_add_fetch_32(&pVnode->queuedRMsg, 1);

  if (pRead->code == TSDB_CODE_RPC_NETWORK_UNAVAIL || pRead->msgType == TSDB_MSG_TYPE_FETCH) {
    return taosWriteQitem(pVnode->fqueue, qtype, pRead);
  } else {
    return taosWriteQitem(pVnode->qqueue, qtype, pRead);
  }
}

static void vnodeFreeFromRQueue(SVnode *pVnode, SReadMsg *pRead) {
  atomic_sub_fetch_32(&pVnode->queuedRMsg, 1);

  taosFreeQitem(pRead);
  vnodeRelease(pVnode);
}

int32_t vnodeReputPutToRQueue(SVnode *pVnode, void **qhandle, void *ahandle) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TSDB_MSG_TYPE_QUERY;
  rpcMsg.ahandle = ahandle;

  int32_t code = vnodeWriteToRQueue(pVnode, qhandle, 0, TAOS_QTYPE_QUERY, &rpcMsg);
  if (code == TSDB_CODE_SUCCESS) {
    vTrace("QInfo:%p add to vread queue for exec query", *qhandle);
  }

  return code;
}

void vnodeProcessReadMsg(SRpcMsg *pMsg) {
  int32_t queuedMsgNum = 0;
  int32_t leftLen = pMsg->contLen;
  int32_t code = TSDB_CODE_VND_INVALID_VGROUP_ID;
  char *  pCont = pMsg->pCont;

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *)pCont;
    pHead->vgId = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    assert(pHead->contLen > 0);
    SVnode *pVnode = vnodeAcquireNotClose(pHead->vgId);
    if (pVnode != NULL) {
      code = vnodeWriteToRQueue(pVnode, pCont, pHead->contLen, TAOS_QTYPE_RPC, pMsg);
      if (code == TSDB_CODE_SUCCESS) queuedMsgNum++;
      vnodeRelease(pVnode);
    }

    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
  }

  if (queuedMsgNum == 0) {
    SRpcMsg rpcRsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rpcRsp);
  }

  rpcFreeCont(pMsg->pCont);
}

static void vnodeInitReadMsgFp() {
  tsVread.msgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessQueryMsg;
  tsVread.msgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessFetchMsg;
}

static int32_t vnodeProcessReadStart(SVnode *pVnode, SReadMsg *pRead, int32_t qtype) {
  int32_t msgType = pRead->msgType;
  if (tsVread.msgFp[msgType] == NULL) {
    vDebug("vgId:%d, msgType:%s not processed, no handle", pVnode->vgId, taosMsg[msgType]);
    return TSDB_CODE_VND_MSG_NOT_PROCESSED;
  } else {
    vTrace("msg:%p, app:%p type:%s will be processed", pRead, pRead->rpcAhandle, taosMsg[msgType]);
  }

  return (*tsVread.msgFp[msgType])(pVnode, pRead);
}

static void vnodeSendReadRsp(SReadMsg *pRead, int32_t code) {
  SRpcMsg rpcRsp = {
      .handle = pRead->rpcHandle,
      .pCont = pRead->rspRet.rsp,
      .contLen = pRead->rspRet.len,
      .code = code,
  };

  rpcSendResponse(&rpcRsp);
}

static void vnodeProcessReadEnd(SVnode *pVnode, SReadMsg *pRead, int32_t qtype, int32_t code) {
  if (qtype == TAOS_QTYPE_RPC && code != TSDB_CODE_QRY_NOT_READY) {
    vnodeSendReadRsp(pRead, code);
  } else {
    if (code == TSDB_CODE_QRY_HAS_RSP) {
      vnodeSendReadRsp(pRead, pRead->code);
    } else {  // code == TSDB_CODE_QRY_NOT_READY, do not return msg to client
      assert(pRead->rpcHandle == NULL || (pRead->rpcHandle != NULL && pRead->msgType == 5));
    }
  }

  vnodeFreeFromRQueue(pVnode, pRead);
}

int32_t vnodeInitRead() {
  vnodeInitReadMsgFp();

  int32_t maxFetchThreads = 4;
  float   threadsForQuery = MAX(tsNumOfCores * tsRatioOfQueryCores, 1);

  SWorkerPool *pPool = &tsVread.query;
  pPool->name = "vquery";
  pPool->startFp = (ProcessStartFp)vnodeProcessReadStart;
  pPool->endFp = (ProcessEndFp)vnodeProcessReadEnd;
  pPool->min = (int32_t)threadsForQuery;
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) return -1;

  pPool = &tsVread.fetch;
  pPool->name = "vfetch";
  pPool->startFp = (ProcessStartFp)vnodeProcessReadStart;
  pPool->endFp = (ProcessEndFp)vnodeProcessReadEnd;
  pPool->min = MIN(maxFetchThreads, tsNumOfCores);
  pPool->max = pPool->min;
  if (tWorkerInit(pPool) != 0) return -1;

  vInfo("vread is initialized, max worker %d", pPool->max);
  return 0;
}

void vnodeCleanupRead() {
  tWorkerCleanup(&tsVread.fetch);
  tWorkerCleanup(&tsVread.query);
  vInfo("vread is closed");
}

taos_queue vnodeAllocQueryQueue(SVnode *pVnode) { return tWorkerAllocQueue(&tsVread.query, pVnode); }

taos_queue vnodeAllocFetchQueue(SVnode *pVnode) { return tWorkerAllocQueue(&tsVread.fetch, pVnode); }

void vnodeFreeQueryQueue(taos_queue pQueue) { tWorkerFreeQueue(&tsVread.query, pQueue); }

void vnodeFreeFetchQueue(taos_queue pQueue) { tWorkerFreeQueue(&tsVread.fetch, pQueue); }
