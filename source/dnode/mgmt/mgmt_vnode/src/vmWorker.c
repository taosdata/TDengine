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
#include "vnodeInt.h"

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
    case TDMT_DND_CREATE_VNODE:
      code = vmProcessCreateVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = vmProcessDropVnodeReq(pMgmt, pMsg);
      break;
    case TDMT_VND_ALTER_REPLICA:
      code = vmProcessAlterVnodeReplicaReq(pMgmt, pMsg);
      break;
    case TDMT_VND_DISABLE_WRITE:
      code = vmProcessDisableVnodeWriteReq(pMgmt, pMsg);
      break;
    case TDMT_VND_ALTER_HASHRANGE:
      code = vmProcessAlterHashRangeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE_TYPE:
      code = vmProcessAlterVnodeTypeReq(pMgmt, pMsg);
      break;
    case TDMT_DND_CHECK_VNODE_LEARNER_CATCHUP:
      code = vmProcessCheckLearnCatchupReq(pMgmt, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      dGError("msg:%p, not processed in vnode-mgmt queue", pMsg);
  }

  if (IsReq(pMsg)) {
    if (code != 0) {
      if (terrno != 0) code = terrno;
      dGError("msg:%p, failed to process since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
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
    dGError("vgId:%d, msg:%p failed to query since %s", pVnode->vgId, pMsg, tstrerror(code));
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
  int32_t code = vnodeProcessStreamMsg(pVnode->pImpl, pMsg, pInfo);
  if (code != 0) {
    if (terrno != 0) code = terrno;
    dGError("vgId:%d, msg:%p failed to process stream msg %s since %s", pVnode->vgId, pMsg, TMSG_INFO(pMsg->msgType),
            terrstr(code));
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

    terrno = 0;
    int32_t code = vnodeProcessFetchMsg(pVnode->pImpl, pMsg, pInfo);
    if (code != 0) {
      if (code == -1 && terrno != 0) {
        code = terrno;
      }

      if (code == TSDB_CODE_WAL_LOG_NOT_EXIST) {
        dGDebug("vnodeProcessFetchMsg vgId:%d, msg:%p failed to fetch since %s", pVnode->vgId, pMsg, terrstr());
      } else {
        dGError("vnodeProcessFetchMsg vgId:%d, msg:%p failed to fetch since %s", pVnode->vgId, pMsg, terrstr());
      }

      vmSendRsp(pMsg, code);
    }

    dGTrace("vnodeProcessFetchMsg vgId:%d, msg:%p is freed, code:0x%x", pVnode->vgId, pMsg, code);
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

static void vmSendResponse(SRpcMsg *pMsg) {
  if (pMsg->info.handle) {
    SRpcMsg rsp = {.info = pMsg->info, .code = terrno};
    rpcSendResponse(&rsp);
  }
}

static bool vmDataSpaceSufficient(SVnodeObj *pVnode) {
  STfs *pTfs = pVnode->pImpl->pTfs;
  if (pTfs) {
    return tfsDiskSpaceSufficient(pTfs, 0, pVnode->diskPrimary);
  } else {
    return osDataSpaceSufficient();
  }
}

static int32_t vmPutMsgToQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg, EQueueType qtype) {
  const STraceId *trace = &pMsg->info.traceId;
  if (pMsg->contLen < sizeof(SMsgHead)) {
    dGError("invalid rpc msg with no msg head at pCont. pMsg:%p, type:%s, contLen:%d", pMsg, TMSG_INFO(pMsg->msgType),
            pMsg->contLen);
    return -1;
  }

  SMsgHead *pHead = pMsg->pCont;
  int32_t   code = 0;

  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  SVnodeObj *pVnode = vmAcquireVnode(pMgmt, pHead->vgId);
  if (pVnode == NULL) {
    dGDebug("vgId:%d, msg:%p failed to put into vnode queue since %s, type:%s qtype:%d contLen:%d", pHead->vgId, pMsg,
            terrstr(), TMSG_INFO(pMsg->msgType), qtype, pHead->contLen);
    terrno = (terrno != 0) ? terrno : -1;
    return terrno;
  }

  switch (qtype) {
    case QUERY_QUEUE:
      code = vnodePreprocessQueryMsg(pVnode->pImpl, pMsg);
      if (code) {
        dError("vgId:%d, msg:%p preprocess query msg failed since %s", pVnode->vgId, pMsg, terrstr(code));
      } else {
        dGTrace("vgId:%d, msg:%p put into vnode-query queue", pVnode->vgId, pMsg);
        taosWriteQitem(pVnode->pQueryQ, pMsg);
      }
      break;
    case STREAM_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-stream queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pStreamQ, pMsg);
      break;
    case FETCH_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-fetch queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pFetchQ, pMsg);
      break;
    case WRITE_QUEUE:
      if (!vmDataSpaceSufficient(pVnode)) {
        terrno = TSDB_CODE_NO_ENOUGH_DISKSPACE;
        code = terrno;
        dError("vgId:%d, msg:%p put into vnode-write queue failed since %s", pVnode->vgId, pMsg, terrstr(code));
        break;
      }
      if (pMsg->msgType == TDMT_VND_SUBMIT && (grantCheck(TSDB_GRANT_STORAGE) != TSDB_CODE_SUCCESS)) {
        terrno = TSDB_CODE_VND_NO_WRITE_AUTH;
        code = terrno;
        dDebug("vgId:%d, msg:%p put into vnode-write queue failed since %s", pVnode->vgId, pMsg, terrstr(code));
        break;
      }
      if (pMsg->msgType != TDMT_VND_ALTER_CONFIRM && pVnode->disable) {
        dDebug("vgId:%d, msg:%p put into vnode-write queue failed since its disable", pVnode->vgId, pMsg);
        terrno = TSDB_CODE_VND_STOPPED;
        code = terrno;
        break;
      }
      dGTrace("vgId:%d, msg:%p put into vnode-write queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pWriteW.queue, pMsg);
      break;
    case SYNC_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-sync queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pSyncW.queue, pMsg);
      break;
    case SYNC_RD_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-sync-rd queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pSyncRdW.queue, pMsg);
      break;
    case APPLY_QUEUE:
      dGTrace("vgId:%d, msg:%p put into vnode-apply queue", pVnode->vgId, pMsg);
      taosWriteQitem(pVnode->pApplyW.queue, pMsg);
      break;
    default:
      code = -1;
      terrno = TSDB_CODE_INVALID_PARA;
      break;
  }

  vmReleaseVnode(pMgmt, pVnode);
  return code;
}

int32_t vmPutMsgToSyncRdQueue(SVnodeMgmt *pMgmt, SRpcMsg *pMsg) { return vmPutMsgToQueue(pMgmt, pMsg, SYNC_RD_QUEUE); }

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

int32_t vmPutRpcMsgToQueue(SVnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  if (pRpc->contLen < sizeof(SMsgHead)) {
    dError("invalid rpc msg with no msg head at pCont. pRpc:%p, type:%s, len:%d", pRpc, TMSG_INFO(pRpc->msgType),
           pRpc->contLen);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return -1;
  }

  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return -1;
  }

  SMsgHead *pHead = pRpc->pCont;
  dTrace("vgId:%d, msg:%p is created, type:%s len:%d", pHead->vgId, pMsg, TMSG_INFO(pRpc->msgType), pRpc->contLen);

  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = htonl(pHead->vgId);
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  int32_t code = vmPutMsgToQueue(pMgmt, pMsg, qtype);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
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
        size = taosQueueItemSize(pVnode->pWriteW.queue);
        break;
      case SYNC_QUEUE:
        size = taosQueueItemSize(pVnode->pSyncW.queue);
        break;
      case APPLY_QUEUE:
        size = taosQueueItemSize(pVnode->pApplyW.queue);
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
  }
  if (pVnode) vmReleaseVnode(pMgmt, pVnode);
  if (size < 0) {
    dTrace("vgId:%d, can't get size from queue since %s, qtype:%d", vgId, terrstr(), qtype);
    size = 0;
  }
  return size;
}

int32_t vmAllocQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  SMultiWorkerCfg wcfg = {.max = 1, .name = "vnode-write", .fp = (FItems)vnodeProposeWriteMsg, .param = pVnode->pImpl};
  SMultiWorkerCfg scfg = {.max = 1, .name = "vnode-sync", .fp = (FItems)vmProcessSyncQueue, .param = pVnode};
  SMultiWorkerCfg sccfg = {.max = 1, .name = "vnode-sync-rd", .fp = (FItems)vmProcessSyncQueue, .param = pVnode};
  SMultiWorkerCfg acfg = {.max = 1, .name = "vnode-apply", .fp = (FItems)vnodeApplyWriteMsg, .param = pVnode->pImpl};
  (void)tMultiWorkerInit(&pVnode->pWriteW, &wcfg);
  (void)tMultiWorkerInit(&pVnode->pSyncW, &scfg);
  (void)tMultiWorkerInit(&pVnode->pSyncRdW, &sccfg);
  (void)tMultiWorkerInit(&pVnode->pApplyW, &acfg);

  pVnode->pQueryQ = tQWorkerAllocQueue(&pMgmt->queryPool, pVnode, (FItem)vmProcessQueryQueue);
  pVnode->pStreamQ = tAutoQWorkerAllocQueue(&pMgmt->streamPool, pVnode, (FItem)vmProcessStreamQueue);
  pVnode->pFetchQ = tWWorkerAllocQueue(&pMgmt->fetchPool, pVnode, (FItems)vmProcessFetchQueue);

  if (pVnode->pWriteW.queue == NULL || pVnode->pSyncW.queue == NULL || pVnode->pSyncRdW.queue == NULL ||
      pVnode->pApplyW.queue == NULL || pVnode->pQueryQ == NULL || pVnode->pStreamQ == NULL || pVnode->pFetchQ == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  dInfo("vgId:%d, write-queue:%p is alloced, thread:%08" PRId64, pVnode->vgId, pVnode->pWriteW.queue,
        taosQueueGetThreadId(pVnode->pWriteW.queue));
  dInfo("vgId:%d, sync-queue:%p is alloced, thread:%08" PRId64, pVnode->vgId, pVnode->pSyncW.queue,
        taosQueueGetThreadId(pVnode->pSyncW.queue));
  dInfo("vgId:%d, sync-rd-queue:%p is alloced, thread:%08" PRId64, pVnode->vgId, pVnode->pSyncRdW.queue,
        taosQueueGetThreadId(pVnode->pSyncRdW.queue));
  dInfo("vgId:%d, apply-queue:%p is alloced, thread:%08" PRId64, pVnode->vgId, pVnode->pApplyW.queue,
        taosQueueGetThreadId(pVnode->pApplyW.queue));
  dInfo("vgId:%d, query-queue:%p is alloced", pVnode->vgId, pVnode->pQueryQ);
  dInfo("vgId:%d, fetch-queue:%p is alloced, thread:%08" PRId64, pVnode->vgId, pVnode->pFetchQ,
        taosQueueGetThreadId(pVnode->pFetchQ));
  dInfo("vgId:%d, stream-queue:%p is alloced", pVnode->vgId, pVnode->pStreamQ);
  return 0;
}

void vmFreeQueue(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  tQWorkerFreeQueue(&pMgmt->queryPool, pVnode->pQueryQ);
  tAutoQWorkerFreeQueue(&pMgmt->streamPool, pVnode->pStreamQ);
  tWWorkerFreeQueue(&pMgmt->fetchPool, pVnode->pFetchQ);
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

  SAutoQWorkerPool *pStreamPool = &pMgmt->streamPool;
  pStreamPool->name = "vnode-stream";
  pStreamPool->ratio = tsRatioOfVnodeStreamThreads;
  if (tAutoQWorkerInit(pStreamPool) != 0) return -1;

  SWWorkerPool *pFPool = &pMgmt->fetchPool;
  pFPool->name = "vnode-fetch";
  pFPool->max = tsNumOfVnodeFetchThreads;
  if (tWWorkerInit(pFPool) != 0) return -1;

  SSingleWorkerCfg mgmtCfg = {
      .min = 1, .max = 1, .name = "vnode-mgmt", .fp = (FItem)vmProcessMgmtQueue, .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->mgmtWorker, &mgmtCfg) != 0) return -1;

  dDebug("vnode workers are initialized");
  return 0;
}

void vmStopWorker(SVnodeMgmt *pMgmt) {
  tQWorkerCleanup(&pMgmt->queryPool);
  tAutoQWorkerCleanup(&pMgmt->streamPool);
  tWWorkerCleanup(&pMgmt->fetchPool);
  dDebug("vnode workers are closed");
}
