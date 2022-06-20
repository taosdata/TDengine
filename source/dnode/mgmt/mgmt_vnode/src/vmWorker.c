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

static inline void vmSendRsp(SRpcMsg *pMsg, int32_t code) {
  if (pMsg->info.handle == NULL) return;
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void vmProcessMgmtQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;

  STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from vnode-mgmt queue", pMsg);
  switch (pMsg->msgType) {
    case TDMT_MON_VM_INFO:
      code = vmProcessGetMonitorInfoReq(pMgmt, pMsg);
      break;
    case TDMT_MON_VM_LOAD:
      code = vmProcessGetLoadsReq(pMgmt, pMsg);
      break;
    case TDMT_DND_CREATE_VNODE:
      code = vmProcessCreateVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = vmProcessDropVnodeReq(pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      dError("msg:%p, not processed in vnode-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dError("msg:%p, failed to process since %s", pMsg, terrstr());
    }
    vmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessQueryQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = pInfo->ahandle;

  dTrace("vgId:%d, msg:%p get from vnode-query queue", pVnode->vgId, pMsg);
  int32_t code = vnodeProcessQueryMsg(pVnode->pImpl, pMsg);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dError("vgId:%d, msg:%p failed to query since %s", pVnode->vgId, pMsg, terrstr());
    vmSendRsp(pMsg, code);
  }

  dTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessFetchQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = pInfo->ahandle;

  dTrace("vgId:%d, msg:%p get from vnode-fetch queue", pVnode->vgId, pMsg);
  int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dError("vgId:%d, msg:%p failed to fetch since %s", pVnode->vgId, pMsg, terrstr());
    vmSendRsp(pMsg, code);
  }

  dTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessSyncQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;
  SRpcMsg *  pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    dTrace("vgId:%d, msg:%p get from vnode-sync queue", pVnode->vgId, pMsg);

    int32_t code = vnodeProcessSyncReq(pVnode->pImpl, pMsg, NULL);
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dError("vgId:%d, msg:%p failed to sync since %s", pVnode->vgId, pMsg, terrstr());
      vmSendRsp(pMsg, code);
    }

    dTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessMergeQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;
  SRpcMsg *  pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    dTrace("vgId:%d, msg:%p get from vnode-merge queue", pVnode->vgId, pMsg);

    int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dError("vgId:%d, msg:%p failed to merge since %s", pVnode->vgId, pMsg, terrstr());
      vmSendRsp(pMsg, code);
    }

    dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static int32_t vmPutMsgToQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg, EQueueType qtype) {
  SMsgHead *pHead = pMsg->pCont;
  int32_t   code = 0;

  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, msg:%p failed to put into vnode queue since %s, type:%s", pHead->vgId, pMsg, terrstr(),
           TMSG_INFO(pMsg->msgType));
    return terrno != 0 ? terrno : -1;
  }

  switch (qtype) {
    case QUERY_QUEUE:
      vnodePreprocessQueryMsg(pVnode->pImpl, pMsg);
      dTrace("vgId:%d, msg:%p put into vnode-query queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pQueryQ, pMsg);
      break;
    case FETCH_QUEUE:
      dTrace("vgId:%d, msg:%p put into vnode-fetch queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pFetchQ, pMsg);
      break;
    case WRITE_QUEUE:
      dTrace("vgId:%d, msg:%p put into vnode-write queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pWriteQ, pMsg);
      break;
    case SYNC_QUEUE:
      dTrace("vgId:%d, msg:%p put into vnode-sync queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pSyncQ, pMsg);
      break;
    case MERGE_QUEUE:
      dTrace("vgId:%d, msg:%p put into vnode-merge queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pMergeQ, pMsg);
      break;
    case APPLY_QUEUE:
      dTrace("vgId:%d, msg:%p put into vnode-apply queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pApplyQ, pMsg);
      break;
    default:
      code = -1;
      terrno = TSDB_CODE_INVALID_PARA;
      break;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, SYNC_QUEUE); }

int32_t vmPutMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, WRITE_QUEUE); }

int32_t vmPutMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, QUERY_QUEUE); }

int32_t vmPutMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, FETCH_QUEUE); }

int32_t vmPutMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, MERGE_QUEUE); }

int32_t vmPutMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into vnode-mgmt queue", pMsg);
  taosWriteQitem(pMgmt->mgmtWorker.queue, pMsg);
  return 0;
}

int32_t vmPutMsgToMonitorQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into vnode-monitor queue", pMsg);
  taosWriteQitem(pMgmt->monitorWorker.queue, pMsg);
  return 0;
}

int32_t vmPutRpcMsgToQueue(SVnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  if (pMsg == NULL) return -1;

  SMsgHead *pHead = pRpc->pCont;
  dTrace("vgId:%d, msg:%p is created, type:%s", pHead->vgId, pMsg, TMSG_INFO(pRpc->msgType));

  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  return vmPutMsgToQueue(pMgmt, pMsg, qtype);
}

int32_t vmGetQueueSize(SVnodeMgmt *pMgmt, int32_t vgId, EQueueType qtype) {
  int32_t    size = -1;
  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, vgId);
  if (pVnode != NULL) {
    switch (qtype) {
      case WRITE_QUEUE:
        size = taosQueueItemSize(pVnode->pWriteQ);
        break;
      case SYNC_QUEUE:
        size = taosQueueItemSize(pVnode->pSyncQ);
        break;
      case APPLY_QUEUE:
        size = taosQueueItemSize(pVnode->pApplyQ);
        break;
      case QUERY_QUEUE:
        size = taosQueueItemSize(pVnode->pQueryQ);
        break;
      case FETCH_QUEUE:
        size = taosQueueItemSize(pVnode->pFetchQ);
        break;
      case MERGE_QUEUE:
        size = taosQueueItemSize(pVnode->pMergeQ);
        break;
      default:
        break;
    }
  }
  vmReleaseVnode(pMgmt, pVnode);
  return size;
}

int32_t vmAllocQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode->pImpl, (FItems)vnodeProposeMsg);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)vmProcessSyncQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->applyPool, pVnode->pImpl, (FItems)vnodeApplyMsg);
  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)vmProcessQueryQueue);
  pVnode->pFetchQ = tQWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItem)vmProcessFetchQueue);
  pVnode->pMergeQ = tWWorkerAllocQueue(&pMgmt->mergePool, pVnode, (FItems)vmProcessMergeQueue);

  if (pVnode->pWriteQ == NULL || pVnode->pSyncQ == NULL || pVnode->pApplyQ == NULL || pVnode->pQueryQ == NULL ||
      pVnode->pFetchQ == NULL || pVnode->pMergeQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("vgId:%d, queue is alloced", pVnode->vgId);
  return 0;
}

void vmFreeQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  tWWorkerFreeQueue(&pMgmt->applyPool, pVnode->pApplyQ);
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tQWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  tWWorkerFreeQueue(&pMgmt->mergePool, pVnode->pMergeQ);
  pVnode->pWriteQ = NULL;
  pVnode->pSyncQ = NULL;
  pVnode->pApplyQ = NULL;
  pVnode->pQueryQ = NULL;
  pVnode->pFetchQ = NULL;
  pVnode->pMergeQ = NULL;
  dDebug("vgId:%d, queue is freed", pVnode->vgId);
}

int32_t vmStartWorker(SVnodeMgmt *pMgmt) {
  SQWorkerPool *pQPool = &pMgmt->queryPool;
  pQPool->name = "vnode-query";
  pQPool->min = tsNumOfVnodeQueryThreads;
  pQPool->max = tsNumOfVnodeQueryThreads;
  if (tQWorkerInit(pQPool) != 0) return -1;

  SQWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->min = tsNumOfVnodeFetchThreads;
  pFPool->max = tsNumOfVnodeFetchThreads;
  if (tQWorkerInit(pFPool) != 0) return -1;

  SWWorkerPool *pWPool = &pMgmt->writePool;
  pWPool->name = "vnode-write";
  pWPool->max = tsNumOfVnodeWriteThreads;
  if (tWWorkerInit(pWPool) != 0) return -1;

  SWWorkerPool *pAPool = &pMgmt->applyPool;
  pAPool->name = "vnode-apply";
  pAPool->max = tsNumOfVnodeWriteThreads;
  if (tWWorkerInit(pAPool) != 0) return -1;

  SWWorkerPool *pSPool = &pMgmt->syncPool;
  pSPool->name = "vnode-sync";
  pSPool->max = tsNumOfVnodeSyncThreads;
  if (tWWorkerInit(pSPool) != 0) return -1;

  SWWorkerPool *pMPool = &pMgmt->mergePool;
  pMPool->name = "vnode-merge";
  pMPool->max = tsNumOfVnodeMergeThreads;
  if (tWWorkerInit(pMPool) != 0) return -1;

  SSingleWorkerCfg mgmtCfg = {
      .min = 1,
      .max = 1,
      .name = "vnode-mgmt",
      .fp = (FItem)vmProcessMgmtQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &mgmtCfg) != 0) return -1;

  SSingleWorkerCfg monitorCfg = {
      .min = 1,
      .max = 1,
      .name = "vnode-monitor",
      .fp = (FItem)vmProcessMgmtQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->monitorWorker, &monitorCfg) != 0) return -1;

  dDebug("vnode workers are initialized");
  return 0;
}

void vmStopWorker(SVnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->applyPool);
  tWWorkerCleanup(&pMgmt->syncPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tQWorkerCleanup(&pMgmt->fetchPool);
  tWWorkerCleanup(&pMgmt->mergePool);
  dDebug("vnode workers are closed");
}
