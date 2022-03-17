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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "vmInt.h"

static void vmProcessQueryQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessQueryMsg(pVnode->pImpl, pMsg); }

static void vmProcessFetchQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessFetchMsg(pVnode->pImpl, pMsg); }

static void vmProcessWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SNodeMsg *));

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    assert(ptr != NULL);
  }

  vnodeProcessWMsgs(pVnode->pImpl, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg  *pRsp = NULL;
    SNodeMsg *pNodeMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    SRpcMsg  *pMsg = &pNodeMsg->rpcMsg;
    int32_t   code = vnodeApplyWMsg(pVnode->pImpl, pMsg, &pRsp);
    if (pRsp != NULL) {
      pRsp->ahandle = pMsg->ahandle;
      rpcSendResponse(pRsp);
      free(pRsp);
    } else {
      if (code != 0) code = terrno;
      SRpcMsg rpcRsp = {.handle = pMsg->handle, .ahandle = pMsg->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);
    }
  }

  for (size_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pNodeMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    rpcFreeCont(pNodeMsg->rpcMsg.pCont);
    taosFreeQitem(pNodeMsg);
  }

  taosArrayDestroy(pArray);
}

static void vmProcessApplyQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeApplyWMsg(pVnode->pImpl, pMsg, &pRsp);
  }
}

static void vmProcessSyncQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SRpcMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeProcessSyncReq(pVnode->pImpl, pMsg, &pRsp);
  }
}

static int32_t vmWriteMsgToQueue(STaosQueue *pQueue, SNodeMsg *pMsg, bool sendRsp) {
  int32_t code = taosWriteQitem(pQueue, pMsg);

  if (code != TSDB_CODE_SUCCESS && sendRsp) {
    if (pMsg->rpcMsg.msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->rpcMsg.handle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->rpcMsg.pCont);
  }

  return code;
}

static SVnodeObj *vmAcquireFromMsg(SVnodesMgmt *pMgmt, SNodeMsg *pNodeMsg) {
  SRpcMsg *pMsg = &pNodeMsg->rpcMsg;

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to acquire vnode while process req", pHead->vgId);
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .code = TSDB_CODE_VND_INVALID_VGROUP_ID};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
  }

  return pVnode;
}

int32_t vmProcessWriteMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode != NULL) {
    (void)vmWriteMsgToQueue(pVnode->pWriteQ, pMsg, true);
    vmReleaseVnode(pMgmt, pVnode);
  }
}

int32_t vmProcessSyncMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode != NULL) {
    (void)vmWriteMsgToQueue(pVnode->pSyncQ, pMsg, true);
    vmReleaseVnode(pMgmt, pVnode);
  }
}

int32_t vmProcessQueryMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode != NULL) {
    (void)vmWriteMsgToQueue(pVnode->pQueryQ, pMsg, true);
    vmReleaseVnode(pMgmt, pVnode);
  }
}

int32_t vmProcessFetchMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode != NULL) {
    (void)vmWriteMsgToQueue(pVnode->pFetchQ, pMsg, true);
    vmReleaseVnode(pMgmt, pVnode);
  }
}

int32_t vmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;

  int32_t   code = -1;
  SMsgHead *pHead = pRpc->pCont;
  // pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) return -1;

  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg != NULL) {
    code = vmWriteMsgToQueue(pVnode->pQueryQ, pMsg, false);
  }
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutMsgToApplyQueue(SMgmtWrapper *pWrapper, int32_t vgId, SRpcMsg *pRpc) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;

  int32_t   code = -1;
  SMsgHead *pHead = pRpc->pCont;
  // pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) return -1;

  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg != NULL) {
    code = vmWriteMsgToQueue(pVnode->pApplyQ, pMsg, false);
  }
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmAllocQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessWriteQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessApplyQueue);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)vmProcessSyncQueue);
  pVnode->pFetchQ = tFWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItem)vmProcessFetchQueue);
  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)vmProcessQueryQueue);

  if (pVnode->pApplyQ == NULL || pVnode->pWriteQ == NULL || pVnode->pSyncQ == NULL || pVnode->pFetchQ == NULL ||
      pVnode->pQueryQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

void vmFreeQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tFWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pApplyQ);
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  pVnode->pWriteQ = NULL;
  pVnode->pApplyQ = NULL;
  pVnode->pSyncQ = NULL;
  pVnode->pFetchQ = NULL;
  pVnode->pQueryQ = NULL;
}

static void vmProcessMgmtQueue(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  int32_t code = -1;
  tmsg_t  msgType = pMsg->rpcMsg.msgType;
  dTrace("msg:%p, will be processed", pMsg);

  switch (msgType) {
    case TDMT_DND_CREATE_VNODE:
      code = vmProcessCreateVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE:
      code = vmProcessAlterVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = vmProcessDropVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_SYNC_VNODE:
      code = vmProcessSyncVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_COMPACT_VNODE:
      code = vmProcessCompactVnodeReq(pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->rpcMsg.handle, TMSG_INFO(msgType));
      break;
  }

  if (msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->rpcMsg.pCont);
  pMsg->rpcMsg.pCont = NULL;
  taosFreeQitem(pMsg);
  dTrace("msg:%p, is freed", pMsg);
}

int32_t vmStartWorker(SVnodesMgmt *pMgmt) {
  int32_t maxFetchThreads = 4;
  int32_t minFetchThreads = TMIN(maxFetchThreads, tsNumOfCores);
  int32_t minQueryThreads = TMAX((int32_t)(tsNumOfCores * tsRatioOfQueryCores), 1);
  int32_t maxQueryThreads = minQueryThreads;
  int32_t maxWriteThreads = TMAX(tsNumOfCores, 1);
  int32_t maxSyncThreads = TMAX(tsNumOfCores / 2, 1);

  SQWorkerPool *pQPool = &pMgmt->queryPool;
  pQPool->name = "vnode-query";
  pQPool->min = minQueryThreads;
  pQPool->max = maxQueryThreads;
  if (tQWorkerInit(pQPool) != 0) return -1;

  SFWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->min = minFetchThreads;
  pFPool->max = maxFetchThreads;
  if (tFWorkerInit(pFPool) != 0) return -1;

  SWWorkerPool *pWPool = &pMgmt->writePool;
  pWPool->name = "vnode-write";
  pWPool->max = maxWriteThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  pWPool = &pMgmt->syncPool;
  pWPool->name = "vnode-sync";
  pWPool->max = maxSyncThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  if (dndInitWorker(pMgmt, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "vnode-mgmt", 1, 1, vmProcessMgmtQueue) != 0) {
    dError("failed to start dnode mgmt worker since %s", terrstr());
    return -1;
  }

  dDebug("vnode workers is initialized");
  return 0;
}

void vmStopWorker(SVnodesMgmt *pMgmt) {
  dndCleanupWorker(&pMgmt->mgmtWorker);
  tFWorkerCleanup(&pMgmt->fetchPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->syncPool);
  dDebug("vnode workers is closed");
}

int32_t vmProcessMgmtMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, will be written to worker %s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg, 0);
}