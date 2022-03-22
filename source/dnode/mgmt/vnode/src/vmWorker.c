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

static void vmSendRsp(SMgmtWrapper *pWrapper, SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {.handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle, .code = code};
  dndSendRsp(pWrapper, &rsp);
}

static void vmProcessMgmtQueue(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  int32_t code = -1;
  tmsg_t  msgType = pMsg->rpcMsg.msgType;
  dTrace("msg:%p, will be processed in vnode-mgmt queue", pMsg);

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
      dError("msg:%p, not processed in vnode-mgmt queue", pMsg);
  }

  if (msgType & 1u) {
    if (code != 0 && terrno != 0) code = terrno;
    vmSendRsp(pMgmt->pWrapper, pMsg, code);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessQueryQueue(SVnodeObj *pVnode, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed in vnode-query queue", pMsg);
  int32_t code = vnodeProcessQueryMsg(pVnode->pImpl, &pMsg->rpcMsg);
  if (code != 0) {
    vmSendRsp(pVnode->pWrapper, pMsg, code);
    dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
    rpcFreeCont(pMsg->rpcMsg.pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessFetchQueue(SVnodeObj *pVnode, SNodeMsg *pMsg) {
  dTrace("msg:%p, will be processed in vnode-fetch queue", pMsg);
  int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, &pMsg->rpcMsg);
  if (code != 0) {
    vmSendRsp(pVnode->pWrapper, pMsg, code);
    dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
    rpcFreeCont(pMsg->rpcMsg.pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessWriteQueue(SVnodeObj *pVnode, STaosQall *qall, int32_t numOfMsgs) {
  SArray *pArray = taosArrayInit(numOfMsgs, sizeof(SNodeMsg *));
  if (pArray == NULL) {
    dError("failed to process %d msgs in write-queue since %s", numOfMsgs, terrstr());
    return;
  }

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SNodeMsg *pMsg = NULL;
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;

    dTrace("msg:%p, will be processed in vnode-write queue", pMsg);
    if (taosArrayPush(pArray, &pMsg) == NULL) {
      dTrace("msg:%p, failed to process since %s", pMsg, terrstr());
      vmSendRsp(pVnode->pWrapper, pMsg, TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  vnodeProcessWMsgs(pVnode->pImpl, pArray);

  numOfMsgs = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pMsg = *(SNodeMsg **)taosArrayGet(pArray, i);
    SRpcMsg  *pRpc = &pMsg->rpcMsg;
    SRpcMsg  *pRsp = NULL;

    int32_t code = vnodeApplyWMsg(pVnode->pImpl, pRpc, &pRsp);
    if (pRsp != NULL) {
      pRsp->ahandle = pRpc->ahandle;
      dndSendRsp(pVnode->pWrapper, pRsp);
      free(pRsp);
    } else {
      if (code != 0 && terrno != 0) code = terrno;
      vmSendRsp(pVnode->pWrapper, pMsg, code);
    }
  }

  for (int32_t i = 0; i < numOfMsgs; i++) {
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

static int32_t vmPutNodeMsgToQueue(SVnodesMgmt *pMgmt, SNodeMsg *pMsg, EQueueType qtype) {
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to write msg:%p to vnode-queue since %s", pHead->vgId, pMsg, terrstr());
    return -1;
  }

  switch (qtype) {
    case QUERY_QUEUE:
      dTrace("msg:%p, will be written into vnode-query queue", pMsg);
      code = taosWriteQitem(pVnode->pQueryQ, pMsg);
      break;
    case FETCH_QUEUE:
      dTrace("msg:%p, will be written into vnode-fetch queue", pMsg);
      code = taosWriteQitem(pVnode->pFetchQ, pMsg);
      break;
    case WRITE_QUEUE:
      dTrace("msg:%p, will be written into vnode-write queue", pMsg);
      code = taosWriteQitem(pVnode->pWriteQ, pMsg);
      break;
    case SYNC_QUEUE:
      dTrace("msg:%p, will be written into vnode-sync queue", pMsg);
      code = taosWriteQitem(pVnode->pSyncQ, pMsg);
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      break;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmProcessSyncMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, SYNC_QUEUE);
}

int32_t vmProcessWriteMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, WRITE_QUEUE);
}

int32_t vmProcessQueryMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, QUERY_QUEUE);
}

int32_t vmProcessFetchMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, FETCH_QUEUE);
}

int32_t vmProcessMgmtMsg(SVnodesMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, will be written to vnode-mgmt queue, worker:%s", pMsg, pWorker->name);
  return dndWriteMsgToWorker(pWorker, pMsg);
}

static int32_t vmPutRpcMsgToQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, EQueueType qtype) {
  SVnodesMgmt *pMgmt = pWrapper->pMgmt;
  int32_t      code = -1;
  SMsgHead    *pHead = pRpc->pCont;

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) return -1;

  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg != NULL) {
    dTrace("msg:%p, is created, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
    pMsg->rpcMsg = *pRpc;
    switch (qtype) {
      case QUERY_QUEUE:
        dTrace("msg:%p, will be put into vnode-query queue", pMsg);
        code = taosWriteQitem(pVnode->pQueryQ, pMsg);
        break;
      case FETCH_QUEUE:
        dTrace("msg:%p, will be put into vnode-fetch queue", pMsg);
        code = taosWriteQitem(pVnode->pFetchQ, pMsg);
        break;
      case APPLY_QUEUE:
        dTrace("msg:%p, will be put into vnode-apply queue", pMsg);
        code = taosWriteQitem(pVnode->pApplyQ, pMsg);
        break;
      default:
        terrno = TSDB_CODE_INVALID_PARA;
        break;
    }
  }
  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pWrapper, pRpc, QUERY_QUEUE);
}

int32_t vmPutMsgToFetchQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pWrapper, pRpc, FETCH_QUEUE);
}

int32_t vmPutMsgToApplyQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pWrapper, pRpc, APPLY_QUEUE);
}

int32_t vmAllocQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessWriteQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessApplyQueue);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)vmProcessSyncQueue);
  pVnode->pFetchQ = tQWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItem)vmProcessFetchQueue);
  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)vmProcessQueryQueue);

  if (pVnode->pApplyQ == NULL || pVnode->pWriteQ == NULL || pVnode->pSyncQ == NULL || pVnode->pFetchQ == NULL ||
      pVnode->pQueryQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("vgId:%d, vnode queue is alloced", pVnode->vgId);
  return 0;
}

void vmFreeQueue(SVnodesMgmt *pMgmt, SVnodeObj *pVnode) {
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tQWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pApplyQ);
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  pVnode->pWriteQ = NULL;
  pVnode->pApplyQ = NULL;
  pVnode->pSyncQ = NULL;
  pVnode->pFetchQ = NULL;
  pVnode->pQueryQ = NULL;
  dDebug("vgId:%d, vnode queue is freed", pVnode->vgId);
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

  SQWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->min = minFetchThreads;
  pFPool->max = maxFetchThreads;
  if (tQWorkerInit(pFPool) != 0) return -1;

  SWWorkerPool *pWPool = &pMgmt->writePool;
  pWPool->name = "vnode-write";
  pWPool->max = maxWriteThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  pWPool = &pMgmt->syncPool;
  pWPool->name = "vnode-sync";
  pWPool->max = maxSyncThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  if (dndInitWorker(pMgmt, &pMgmt->mgmtWorker, DND_WORKER_SINGLE, "vnode-mgmt", 1, 1, vmProcessMgmtQueue) != 0) {
    dError("failed to start vnode-mgmt worker since %s", terrstr());
    return -1;
  }

  dDebug("vnode workers is initialized");
  return 0;
}

void vmStopWorker(SVnodesMgmt *pMgmt) {
  dndCleanupWorker(&pMgmt->mgmtWorker);
  tQWorkerCleanup(&pMgmt->fetchPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->syncPool);
  dDebug("vnode workers is closed");
}
