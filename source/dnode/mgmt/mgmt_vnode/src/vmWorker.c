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

#include "sync.h"
#include "syncTools.h"

static inline void vmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void vmProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  dTrace("msg:%p, get from vnode queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

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
      dError("msg:%p, not processed in vnode queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0 && terrno != 0) code = terrno;
    vmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessQueryQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = pInfo->ahandle;

  dTrace("msg:%p, get from vnode-query queue", pMsg);
  int32_t code = vnodeProcessQueryMsg(pVnode->pImpl, pMsg);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    vmSendRsp(pMsg, code);
  }
  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessFetchQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SVnodeObj *pVnode = pInfo->ahandle;

  dTrace("msg:%p, get from vnode-fetch queue", pMsg);
  int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    vmSendRsp(pMsg, code);
  }
  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void vmProcessWriteQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;
  SArray    *pArray = taosArrayInit(numOfMsgs, sizeof(SRpcMsg *));
  if (pArray == NULL) {
    dError("failed to process %d msgs in write-queue since %s", numOfMsgs, terrstr());
    return;
  }

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;

    dTrace("msg:%p, get from vnode-write queue", pMsg);
    if (taosArrayPush(pArray, &pMsg) == NULL) {
      dTrace("msg:%p, failed to process since %s", pMsg, terrstr());
      vmSendRsp(pMsg, TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  for (int i = 0; i < taosArrayGetSize(pArray); i++) {
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    SRpcMsg  rsp = {.info = pMsg->info};

    vnodePreprocessReq(pVnode->pImpl, pMsg);

    int32_t ret = syncPropose(vnodeGetSyncHandle(pVnode->pImpl), pMsg, false);
    if (ret == TAOS_SYNC_PROPOSE_NOT_LEADER) {
      dTrace("msg:%p, is redirect since not leader, vgId:%d ", pMsg, pVnode->vgId);
      rsp.code = TSDB_CODE_RPC_REDIRECT;
      SEpSet newEpSet;
      syncGetEpSet(vnodeGetSyncHandle(pVnode->pImpl), &newEpSet);
      newEpSet.inUse = (newEpSet.inUse + 1) % newEpSet.numOfEps;
      tmsgSendRedirectRsp(&rsp, &newEpSet);
    } else if (ret == TAOS_SYNC_PROPOSE_OTHER_ERROR) {
      rsp.code = TSDB_CODE_SYN_INTERNAL_ERROR;
      tmsgSendRsp(&rsp);
    } else if (ret == TAOS_SYNC_PROPOSE_SUCCESS) {
      // send response in applyQ
    } else {
      assert(0);
    }
  }

  for (int32_t i = 0; i < numOfMsgs; i++) {
    SRpcMsg *pMsg = *(SRpcMsg **)taosArrayGet(pArray, i);
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }

  taosArrayDestroy(pArray);
}

static void vmProcessApplyQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);

    // init response rpc msg
    SRpcMsg rsp = {0};

    // get original rpc msg
    assert(pMsg->msgType == TDMT_VND_SYNC_APPLY_MSG);
    SyncApplyMsg *pSyncApplyMsg = syncApplyMsgFromRpcMsg2(pMsg);
    syncApplyMsgLog2("==vmProcessApplyQueue==", pSyncApplyMsg);
    SRpcMsg originalRpcMsg;
    syncApplyMsg2OriginalRpcMsg(pSyncApplyMsg, &originalRpcMsg);

    // apply data into tsdb
    if (vnodeProcessWriteReq(pVnode->pImpl, &originalRpcMsg, pSyncApplyMsg->fsmMeta.index, &rsp) < 0) {
      rsp.code = terrno;
      dTrace("msg:%p, process write error since %s", pMsg, terrstr());
    }

    syncApplyMsgDestroy(pSyncApplyMsg);
    rpcFreeCont(originalRpcMsg.pCont);

    // if leader, send response
    if (pMsg->info.handle != NULL) {
      rsp.info = pMsg->info;
      tmsgSendRsp(&rsp);
    }

    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessSyncQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);

    int32_t code = vnodeProcessSyncReq(pVnode->pImpl, pMsg, NULL);
    if (code != 0) {
      if (pMsg->info.handle != NULL) {
        SRpcMsg rsp = {
            .code = (terrno < 0) ? terrno : code,
            .info = pMsg->info,
        };
        dTrace("msg:%p, failed to process sync queue since %s", pMsg, terrstr());
        tmsgSendRsp(&rsp);
      }
    }

    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void vmProcessMergeQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SVnodeObj *pVnode = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    SRpcMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);

    dTrace("msg:%p, get from vnode-merge queue", pMsg);
    int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
    if (code != 0) {
      if (terrno != 0) code = terrno;
      vmSendRsp(pMsg, code);
    }
    dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static int32_t vmPutNodeMsgToQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg, EQueueType qtype) {
  SMsgHead *pHead = pMsg->pCont;
  int32_t   code = 0;

  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dError("vgId:%d, failed to put msg:%p into vnode-queue since %s", pHead->vgId, pMsg, terrstr());
    return terrno != 0 ? terrno : -1;
  }

  switch (qtype) {
    case QUERY_QUEUE:
      dTrace("msg:%p, put into vnode-query worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      taosWriteQitem(pVnode->pQueryQ, pMsg);
      break;
    case FETCH_QUEUE:
      dTrace("msg:%p, put into vnode-fetch worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      taosWriteQitem(pVnode->pFetchQ, pMsg);
      break;
    case WRITE_QUEUE:
      dTrace("msg:%p, put into vnode-write worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      taosWriteQitem(pVnode->pWriteQ, pMsg);
      break;
    case SYNC_QUEUE:
      dTrace("msg:%p, put into vnode-sync worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      taosWriteQitem(pVnode->pSyncQ, pMsg);
      break;
    case MERGE_QUEUE:
      dTrace("msg:%p, put into vnode-merge worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      taosWriteQitem(pVnode->pMergeQ, pMsg);
      break;
    default:
      code = -1;
      terrno = TSDB_CODE_INVALID_PARA;
      break;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutNodeMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, SYNC_QUEUE);
}

int32_t vmPutNodeMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, WRITE_QUEUE);
}

int32_t vmPutNodeMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, QUERY_QUEUE);
}

int32_t vmPutNodeMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, FETCH_QUEUE);
}

int32_t vmPutNodeMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return vmPutNodeMsgToQueue(pMgmt, pMsg, MERGE_QUEUE);
}

int32_t vmPutNodeMsgToMgmtQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->mgmtWorker;
  dTrace("msg:%p, put into vnode-mgmt worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t vmPutNodeMsgToMonitorQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->monitorWorker;
  dTrace("msg:%p, put into vnode-monitor worker, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

static int32_t vmPutRpcMsgToQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc, EQueueType qtype) {
  SMsgHead  *pHead = pRpc->pCont;
  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) return -1;

  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  int32_t  code = 0;

  if (pMsg == NULL) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    code = -1;
  } else {
    memcpy(pMsg, pRpc, sizeof(SRpcMsg));
    switch (qtype) {
      case WRITE_QUEUE:
        dTrace("msg:%p, create and put into vnode-write worker, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pWriteQ, pMsg);
        break;
      case QUERY_QUEUE:
        dTrace("msg:%p, create and put into vnode-query queue, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pQueryQ, pMsg);
        break;
      case FETCH_QUEUE:
        dTrace("msg:%p, create and put into vnode-fetch queue, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pFetchQ, pMsg);
        break;
      case APPLY_QUEUE:
        dTrace("msg:%p, create and put into vnode-apply queue, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pApplyQ, pMsg);
        break;
      case MERGE_QUEUE:
        dTrace("msg:%p, create and put into vnode-merge queue, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pMergeQ, pMsg);
        break;
      case SYNC_QUEUE:
        dTrace("msg:%p, create and put into vnode-sync queue, type:%s", pMsg, TMSG_INFO(pRpc->msgType));
        taosWriteQitem(pVnode->pSyncQ, pMsg);
        break;
      default:
        code = -1;
        terrno = TSDB_CODE_INVALID_PARA;
        break;
    }
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutRpcMsgToWriteQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pMgmt, pRpc, WRITE_QUEUE);
}

int32_t vmPutRpcMsgToSyncQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) { return vmPutRpcMsgToQueue(pMgmt, pRpc, SYNC_QUEUE); }

int32_t vmPutRpcMsgToApplyQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pMgmt, pRpc, APPLY_QUEUE);
}

int32_t vmPutRpcMsgToQueryQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pMgmt, pRpc, QUERY_QUEUE);
}

int32_t vmPutRpcMsgToFetchQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pMgmt, pRpc, FETCH_QUEUE);
}

int32_t vmPutRpcMsgToMergeQueue(SVnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return vmPutRpcMsgToQueue(pMgmt, pRpc, MERGE_QUEUE);
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
  pVnode->pWriteQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessWriteQueue);
  pVnode->pSyncQ = tWWorkerAllocQueue(&pMgmt->syncPool, pVnode, (FItems)vmProcessSyncQueue);
  pVnode->pApplyQ = tWWorkerAllocQueue(&pMgmt->writePool, pVnode, (FItems)vmProcessApplyQueue);
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
  tWWorkerFreeQueue(&pMgmt->syncPool, pVnode->pSyncQ);
  tWWorkerFreeQueue(&pMgmt->writePool, pVnode->pApplyQ);
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

  SWWorkerPool *pSPool = &pMgmt->syncPool;
  pSPool->name = "vnode-sync";
  pSPool->max = tsNumOfVnodeSyncThreads;
  if (tWWorkerInit(pSPool) != 0) return -1;

  SWWorkerPool *pMPool = &pMgmt->mergePool;
  pMPool->name = "vnode-merge";
  pMPool->max = tsNumOfVnodeMergeThreads;
  if (tWWorkerInit(pMPool) != 0) return -1;

  SSingleWorkerCfg cfg = {
      .min = 1,
      .max = 1,
      .name = "vnode-mgmt",
      .fp = (FItem)vmProcessQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &cfg) != 0) {
    dError("failed to start vnode-mgmt worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg mCfg = {
      .min = 1,
      .max = 1,
      .name = "vnode-monitor",
      .fp = (FItem)vmProcessQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
    dError("failed to start vnode-monitor worker since %s", terrstr());
    return -1;
  }

  dDebug("vnode workers are initialized");
  return 0;
}

void vmStopWorker(SVnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  tWWorkerCleanup(&pMgmt->writePool);
  tWWorkerCleanup(&pMgmt->syncPool);
  tQWorkerCleanup(&pMgmt->queryPool);
  tQWorkerCleanup(&pMgmt->fetchPool);
  tWWorkerCleanup(&pMgmt->mergePool);
  dDebug("vnode workers are closed");
}
