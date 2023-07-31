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
#include "mmInt.h"

static inline int32_t mmAcquire(SMnodeMgmt *pMgmt) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&pMgmt->lock);
  if (pMgmt->stopped) {
    code = -1;
  } else {
    atomic_add_fetch_32(&pMgmt->refCount, 1);
  }
  taosThreadRwlockUnlock(&pMgmt->lock);
  return code;
}

static inline void mmRelease(SMnodeMgmt *pMgmt) {
  taosThreadRwlockRdlock(&pMgmt->lock);
  atomic_sub_fetch_32(&pMgmt->refCount, 1);
  taosThreadRwlockUnlock(&pMgmt->lock);
}

static inline void mmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void mmProcessRpcMsg(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from mnode queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  int32_t code = mndProcessRpcMsg(pMsg);

  if (IsReq(pMsg) && pMsg->info.handle != NULL && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (code != 0 && terrno != 0) code = terrno;
    mmSendRsp(pMsg, code);
  } else {
    rpcFreeCont(pMsg->info.rsp);
    pMsg->info.rsp = NULL;
  }

  if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_RESTORING) {
    mndPostProcessQueryMsg(pMsg);
  }

  dGTrace("msg:%p is freed, code:%s", pMsg, tstrerror(code));
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessSyncMsg(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from mnode-sync queue", pMsg);

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  int32_t code = mndProcessSyncMsg(pMsg);

  dGTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static inline int32_t mmPutMsgToWorker(SMnodeMgmt *pMgmt, SSingleWorker *pWorker, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;

  if (mmAcquire(pMgmt) == 0) {
    dGTrace("msg:%p, put into %s queue, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
    taosWriteQitem(pWorker->queue, pMsg);
    mmRelease(pMgmt);
    return 0;
  } else {
    dGTrace("msg:%p, failed to put into %s queue since %s, type:%s", pMsg, pWorker->name, terrstr(),
            TMSG_INFO(pMsg->msgType));
    return -1;
  }
}

int32_t mmPutMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->writeWorker, pMsg);
}

int32_t mmPutMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->syncWorker, pMsg);
}

int32_t mmPutMsgToSyncRdQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->syncRdWorker, pMsg);
}

int32_t mmPutMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->readWorker, pMsg);
}

int32_t mmPutMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  if (NULL == pMgmt->pMnode) {
    const STraceId *trace = &pMsg->info.traceId;
    dGError("msg:%p, stop to pre-process in mnode since mnode is NULL, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
    return -1;
  }
  pMsg->info.node = pMgmt->pMnode;
  if (mndPreProcessQueryMsg(pMsg) != 0) {
    const STraceId *trace = &pMsg->info.traceId;
    dGError("msg:%p, failed to pre-process in mnode since %s, type:%s", pMsg, terrstr(), TMSG_INFO(pMsg->msgType));
    return -1;
  }
  return mmPutMsgToWorker(pMgmt, &pMgmt->queryWorker, pMsg);
}

int32_t mmPutMsgToFetchQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->fetchWorker, pMsg);
}

int32_t mmPutMsgToQueue(SMnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SSingleWorker *pWorker = NULL;
  switch (qtype) {
    case WRITE_QUEUE:
      pWorker = &pMgmt->writeWorker;
      break;
    case QUERY_QUEUE:
      pWorker = &pMgmt->queryWorker;
      break;
    case FETCH_QUEUE:
      pWorker = &pMgmt->fetchWorker;
      break;
    case READ_QUEUE:
      pWorker = &pMgmt->readWorker;
      break;
    case SYNC_QUEUE:
      pWorker = &pMgmt->syncWorker;
      break;
    case SYNC_RD_QUEUE:
      pWorker = &pMgmt->syncRdWorker;
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
  }

  if (pWorker == NULL) return -1;
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) return -1;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  dTrace("msg:%p, is created and will put into %s queue, type:%s len:%d", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType), pRpc->contLen);
  int32_t code = mmPutMsgToWorker(pMgmt, pWorker, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
  return code;
}

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  SSingleWorkerCfg qCfg = {
      .min = tsNumOfMnodeQueryThreads,
      .max = tsNumOfMnodeQueryThreads,
      .name = "mnode-query",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->queryWorker, &qCfg) != 0) {
    dError("failed to start mnode-query worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg fCfg = {
      .min = tsNumOfMnodeFetchThreads,
      .max = tsNumOfMnodeFetchThreads,
      .name = "mnode-fetch",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->fetchWorker, &fCfg) != 0) {
    dError("failed to start mnode-fetch worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg rCfg = {
      .min = tsNumOfMnodeReadThreads,
      .max = tsNumOfMnodeReadThreads,
      .name = "mnode-read",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->readWorker, &rCfg) != 0) {
    dError("failed to start mnode-read worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg wCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-write",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->writeWorker, &wCfg) != 0) {
    dError("failed to start mnode-write worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg sCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-sync",
      .fp = (FItem)mmProcessSyncMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->syncWorker, &sCfg) != 0) {
    dError("failed to start mnode mnode-sync worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg scCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-sync-rd",
      .fp = (FItem)mmProcessSyncMsg,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->syncRdWorker, &scCfg) != 0) {
    dError("failed to start mnode mnode-sync-rd worker since %s", terrstr());
    return -1;
  }

  dDebug("mnode workers are initialized");
  return 0;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  while (pMgmt->refCount > 0) taosMsleep(10);

  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->fetchWorker);
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  tSingleWorkerCleanup(&pMgmt->syncRdWorker);
  dDebug("mnode workers are closed");
}
