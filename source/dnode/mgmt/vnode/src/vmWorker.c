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
#include "vmWorker.h"

static void vmProcessQueryQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessQueryMsg(pVnode->pImpl, pMsg); }

static void vmProcessFetchQueue(SVnodeObj *pVnode, SRpcMsg *pMsg) { vnodeProcessFetchMsg(pVnode->pImpl, pMsg); }

static void vmProcessWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SRpcMsg *));

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);
    void *ptr = taosArrayPush(pArray, &pMsg);
    assert(ptr != NULL);
  }

  vnodeProcessWMsgs(pVnode->pImpl, pArray);

  for (size_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pRsp = NULL;
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    int32_t  code = vnodeApplyWMsg(pVnode->pImpl, pMsg, &pRsp);
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
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
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

static int32_t vmWriteMsgToQueue(STaosQueue *pQueue, SRpcMsg *pRpcMsg, bool sendRsp) {
  int32_t code = 0;

  if (pQueue == NULL) {
    code = TSDB_CODE_MSG_NOT_PROCESSED;
  } else {
    SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg));
    if (pMsg == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      *pMsg = *pRpcMsg;
      if (taosWriteQitem(pQueue, pMsg) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  if (code != TSDB_CODE_SUCCESS && sendRsp) {
    if (pRpcMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pRpcMsg->handle, .code = code};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pRpcMsg->pCont);
  }

  return code;
}

static SVnodeObj *vmAcquireFromMsg(SVnodesMgmt *pMgmt, SRpcMsg *pMsg) {
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

int32_t vmProcessWriteMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
//   SVnodeObj *pVnode = vmAcquireFromMsg(pDnode, pMsg);
//   if (pVnode != NULL) {
//     (void)vmWriteMsgToQueue(pVnode->pWriteQ, pMsg, true);
//     vmReleaseVnode(pMgmt, pVnode);
//   }
return 0;
}

int32_t vmProcessSyncMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
//   SVnodeObj *pVnode = vmAcquireFromMsg(pDnode, pMsg);
//   if (pVnode != NULL) {
//     (void)vmWriteMsgToQueue(pVnode->pSyncQ, pMsg, true);
//     vmReleaseVnode(pMgmt, pVnode);
//   }
return 0;
}

int32_t vmProcessQueryMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
//   SVnodeObj *pVnode = vmAcquireFromMsg(pDnode, pMsg);
//   if (pVnode != NULL) {
//     (void)vmWriteMsgToQueue(pVnode->pQueryQ, pMsg, true);
//     vmReleaseVnode(pMgmt, pVnode);
//   }
return 0;
}

int32_t vmProcessFetchMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg){
//   SVnodeObj *pVnode = vmAcquireFromMsg(pDnode, pMsg);
//   if (pVnode != NULL) {
//     (void)vmWriteMsgToQueue(pVnode->pFetchQ, pMsg, true);
//     vmReleaseVnode(pMgmt, pVnode);
//   }
return 0;
}

int32_t vmPutMsgToQueryQueue(SVnodesMgmt *pMgmt, SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  // pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) return -1;

  int32_t code = vmWriteMsgToQueue(pVnode->pQueryQ, pMsg, false);
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutMsgToApplyQueue(SVnodesMgmt *pMgmt, int32_t vgId, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode == NULL) return -1;

  int32_t code = taosWriteQitem(pVnode->pApplyQ, pMsg);
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

  dDebug("vnode workers is initialized");
  return 0;
}

void vmStopWorker(SVnodesMgmt *pMgmt) {
  tFWorkerCleanup(&pMgmt->fetchPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->syncPool);
  dDebug("vnode workers is closed");
}
