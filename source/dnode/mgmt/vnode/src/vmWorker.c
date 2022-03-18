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

static void vmProcessQueryQueue(SVnodeObj *pVnode, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed in vnode query queue", pMsg);
  vnodeProcessQueryMsg(pVnode->pImpl, &pMsg->rpcMsg);
}

static void vmProcessFetchQueue(SVnodeObj *pVnode, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed in vnode fetch queue", pMsg);
  vnodeProcessFetchMsg(pVnode->pImpl, &pMsg->rpcMsg);
}

static void vmProcessWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SNodeMsg *));

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    dTrace("msg:%p, will be processed in vnode write queue", pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    assert(ptr != NULL);
  }

  vnodeProcessWMsgs(pVnode->pImpl, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg  *pRsp = NULL;
    SNodeMsg *pMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    SRpcMsg  *pRpc = &pMsg->rpcMsg;
    int32_t   code = vnodeApplyWMsg(pVnode->pImpl, pRpc, &pRsp);
    if (pRsp != NULL) {
      pRsp->ahandle = pRpc->ahandle;
      rpcSendResponse(pRsp);
      free(pRsp);
    } else {
      if (code != 0) code = terrno;
      SRpcMsg rpcRsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = code};
      rpcSendResponse(&rpcRsp);
    }
  }

  for (size_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->rpcMsg.pCont);
    taosFreeQitem(pMsg);
  }

  taosArrayDestroy(pArray);
}

static void vmProcessApplyQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SNodeMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeApplyWMsg(pVnode->pImpl, &pMsg->rpcMsg, &pRsp);
  }
}

static void vmProcessSyncQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SNodeMsg *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    taosGetQitem(qall, (void **)&pMsg);

    // todo
    SRpcMsg *pRsp = NULL;
    (void)vnodeProcessSyncReq(pVnode->pImpl, &pMsg->rpcMsg, &pRsp);
  }
}

static SVnodeObj *vmAcquireFromMsg(SVnodesMgmt *pMgmt, SNodeMsg *pNodeMsg) {
  SRpcMsg *pMsg = &pNodeMsg->rpcMsg;

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to acquire vnode while process req", pHead->vgId);
  }

  return pVnode;
}

int32_t vmProcessWriteMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pWriteQ, pMsg);
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmProcessSyncMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pSyncQ, pMsg);
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmProcessQueryMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pQueryQ, pMsg);
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmProcessFetchMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireFromMsg(pMgmt, pMsg);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pFetchQ, pMsg);
  vmReleaseVnode(pMgmt, pVnode);
  return code;
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
    pMsg->rpcMsg = *pRpc;
    code = taosWriteQitem(pVnode->pQueryQ, pMsg);
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
    pMsg->rpcMsg = *pRpc;
    code = taosWriteQitem(pVnode->pApplyQ, pMsg);
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
  dTrace("msg:%p, will be processed in vnode mgmt queue", pMsg);

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
      dError("msg:%p, not processed in mgmt queue", pMsg);
  }

  if (msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle};
    rpcSendResponse(&rsp);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
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
  return dndWriteMsgToWorker(pWorker, pMsg);
}