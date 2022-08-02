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
  SVnodeMgmt     *pMgmt = pInfo->ahandle;
  int32_t         code = -1;
  const STraceId *trace = &pMsg->info.traceId;

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
      dGError("msg:%p, not processed in vnode-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dGError("msg:%p, failed to process since %s", pMsg, terrstr());
    }
    vmSendRsp(pMsg, code);
  }

  dGTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessQueryQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj      *pVnode = pInfo->ahandle;
  const STraceId *trace = &pMsg->info.traceId;

  dGTrace("vgId:%d, msg:%p get from vnode-query queue", pVnode->vgId, pMsg);
  int32_t code = vnodeProcessQueryMsg(pVnode->pImpl, pMsg);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dGError("vgId:%d, msg:%p failed to query since %s", pVnode->vgId, pMsg, terrstr());
    vmSendRsp(pMsg, code);
  }

  dGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessStreamQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj      *pVnode = pInfo->ahandle;
  const STraceId *trace = &pMsg->info.traceId;

  dGTrace("vgId:%d, msg:%p get from vnode-stream queue", pVnode->vgId, pMsg);
  int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dGError("vgId:%d, msg:%p failed to process stream since %s", pVnode->vgId, pMsg, terrstr());
    vmSendRsp(pMsg, code);
  }

  dGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessFetchQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;
  SRpcMsg   *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    const STraceId *trace = &pMsg->info.traceId;
    dGTrace("vgId:%d, msg:%p get from vnode-fetch queue", pVnode->vgId, pMsg);

    int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dGError("vgId:%d, msg:%p failed to fetch since %s", pVnode->vgId, pMsg, terrstr());
      vmSendRsp(pMsg, code);
    }

    dGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessSyncQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;
  SRpcMsg   *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    const STraceId *trace = &pMsg->info.traceId;
    dGTrace("vgId:%d, msg:%p get from vnode-sync queue", pVnode->vgId, pMsg);

    int32_t code = vnodeProcessSyncMsg(pVnode->pImpl, pMsg, NULL);  // no response here
    dGTrace("vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static int32_t vmPutMsgToQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg, EQueueType qtype) {
  const STraceId *trace = &pMsg->info.traceId;
  SMsgHead       *pHead = pMsg->pCont;
  int32_t         code = 0;

  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dGError("vgId:%d, msg:%p failed to put into vnode queue since %s, type:%s qtype:%d", pHead->vgId, pMsg,
            terrstr(), TMSG_INFO(pMsg->msgType), qtype);
    return terrno != 0 ? terrno : -1;
  }

  switch (qtype) {
    case QUERY_QUEUE:
      if ((pMsg->msgType == TDMT_SCH_QUERY) && (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS)) {
        terrno = TSDB_CODE_GRANT_EXPIRED;
        code = terrno;
        dDebug("vgId:%d, msg:%p put into vnode-query queue failed since %s", pVnode->vgId, pMsg, terrstr());
      } else {
        vnodePreprocessQueryMsg(pVnode->pImpl, pMsg);
        dGTrace("vgId:%d, msg:%p put into vnode-query queue", pVnode->vgId, pMsg);
        taosWriteQitem(pVnode->pQueryQ, pMsg);
      }
      break;
    case STREAM_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-stream queue", pVnode->vgId, pMsg);
      if (pMsg->msgType == TDMT_STREAM_TASK_DISPATCH) {
        vnodeEnqueueStreamMsg(pVnode->pImpl, pMsg);
      } else {
        taosWriteQitem(pVnode->pStreamQ, pMsg);
      }
      break;
    case FETCH_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-fetch queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pFetchQ, pMsg);
      break;
    case WRITE_QUEUE:
      if ((pMsg->msgType == TDMT_VND_SUBMIT) && (grantCheck(TSDB_GRANT_STORAGE) != TSDB_CODE_SUCCESS)) {
        terrno = TSDB_CODE_VND_NO_WRITE_AUTH;
        code = terrno;
        dDebug("vgId:%d, msg:%p put into vnode-write queue failed since %s", pVnode->vgId, pMsg, terrstr());
      } else {
        dGTrace("vgId:%d, msg:%p put into vnode-write queue", pVnode->vgId, pMsg);
        taosWriteQitem(pVnode->pWriteQ, pMsg);
#if 0  // tests for batch writes
        if (pMsg->msgType == TDMT_VND_CREATE_TABLE) {
          SRpcMsg *pDup = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
          memcpy(pDup, pMsg, sizeof(SRpcMsg));
          pDup->pCont = rpcMallocCont(pMsg->contLen);
          memcpy(pDup->pCont, pMsg->pCont, pMsg->contLen);
          pDup->info.handle = NULL;
          taosWriteQitem(pVnode->pWriteQ, pDup);
        }
#endif
      }
      break;
    case SYNC_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-sync queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pSyncQ, pMsg);
      break;
    case APPLY_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-apply queue", pVnode->vgId, pMsg);
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

int32_t vmPutMsgToStreamQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, STREAM_QUEUE); }

int32_t vmPutMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, put into vnode-mgmt queue", pMsg);
  taosWriteQitem(pMgmt->mgmtWorker.queue, pMsg);
  return 0;
}

int32_t vmPutMsgToMonitorQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, put into vnode-monitor queue", pMsg);
  taosWriteQitem(pMgmt->monitorWorker.queue, pMsg);
  return 0;
}

int32_t vmPutRpcMsgToQueue(SVnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  if (pMsg == NULL) {
    rpcFreeCont(pMsg->pCont);
    pRpc->pCont = NULL;
    return -1;
  }

  SMsgHead *pHead = pRpc->pCont;
  dTrace("vgId:%d, msg:%p is created, type:%s", pHead->vgId, pMsg, TMSG_INFO(pRpc->msgType));

  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));

  int32_t code = vmPutMsgToQueue(pMgmt, pMsg, qtype);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
    pRpc->pCont = NULL;
    taosFreeQitem(pMsg);
  }

  return code;
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
      case STREAM_QUEUE:
        size = taosQueueItemSize(pVnode->pStreamQ);
        break;
      default:
        break;
    }
    vmReleaseVnode(pMgmt, pVnode);
  }
  return size;
}

int32_t vmAllocQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode->pImpl, (FItems)vnodeProposeWriteMsg);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)vmProcessSyncQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->applyPool, pVnode->pImpl, (FItems)vnodeApplyWriteMsg);
  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)vmProcessQueryQueue);
  pVnode->pStreamQ = tQWorkerAllocQueue(&pMgmt->streamPool, pVnode, (FItem)vmProcessStreamQueue);
  pVnode->pFetchQ = tWWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItems)vmProcessFetchQueue);

  if (pVnode->pWriteQ == NULL || pVnode->pSyncQ == NULL || pVnode->pApplyQ == NULL || pVnode->pQueryQ == NULL ||
      pVnode->pStreamQ == NULL || pVnode->pFetchQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dDebug("vgId:%d, write-queue:%p is alloced", pVnode->vgId, pVnode->pWriteQ);
  dDebug("vgId:%d, sync-queue:%p is alloced", pVnode->vgId, pVnode->pSyncQ);
  dDebug("vgId:%d, apply-queue:%p is alloced", pVnode->vgId, pVnode->pApplyQ);
  dDebug("vgId:%d, query-queue:%p is alloced", pVnode->vgId, pVnode->pQueryQ);
  dDebug("vgId:%d, stream-queue:%p is alloced", pVnode->vgId, pVnode->pStreamQ);
  dDebug("vgId:%d, fetch-queue:%p is alloced", pVnode->vgId, pVnode->pFetchQ);
  return 0;
}

void vmFreeQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pWriteQ);
  tWWorkerFreeQueue(&pMgmt->applyPool, pVnode->pApplyQ);
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tQWorkerFreeQueue(&pMgmt->streamPool, pVnode->pStreamQ);
  tWWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
  pVnode->pWriteQ = NULL;
  pVnode->pSyncQ = NULL;
  pVnode->pApplyQ = NULL;
  pVnode->pQueryQ = NULL;
  pVnode->pStreamQ = NULL;
  pVnode->pFetchQ = NULL;
  dDebug("vgId:%d, queue is freed", pVnode->vgId);
}

int32_t vmStartWorker(SVnodeMgmt *pMgmt) {
  SQWorkerPool *pQPool = &pMgmt->queryPool;
  pQPool->name = "vnode-query";
  pQPool->min = tsNumOfVnodeQueryThreads;
  pQPool->max = tsNumOfVnodeQueryThreads;
  if (tQWorkerInit(pQPool) != 0) return -1;

  SQWorkerPool *pStreamPool = &pMgmt->streamPool;
  pStreamPool->name = "vnode-stream";
  pStreamPool->min = tsNumOfVnodeStreamThreads;
  pStreamPool->max = tsNumOfVnodeStreamThreads;
  if (tQWorkerInit(pStreamPool) != 0) return -1;

  SWWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->max = tsNumOfVnodeFetchThreads;
  if (tWWorkerInit(pFPool) != 0) return -1;

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
  tQWorkerCleanup(&pMgmt->streamPool);
  tWWorkerCleanup(&pMgmt->fetchPool);
  dDebug("vnode workers are closed");
}
