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

static inline void mmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void mmProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  dTrace("msg:%p, get from mnode queue", pMsg);

  switch (pMsg->msgType) {
    case TDMT_MON_MM_INFO:
      code = mmProcessGetMonitorInfoReq(pMgmt, pMsg);
      break;
    case TDMT_MON_MM_LOAD:
      code = mmProcessGetLoadsReq(pMgmt, pMsg);
      break;
    default:
      pMsg->info.node = pMgmt->pMnode;
      code = mndProcessMsg(pMsg);
  }

  if (IsReq(pMsg) && pMsg->info.handle != NULL && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (code != 0 && terrno != 0) code = terrno;
    mmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessSyncQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  dTrace("msg:%p, get from mnode-sync queue", pMsg);

  pMsg->info.node = pMgmt->pMnode;

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  int32_t code = mndProcessSyncMsg(pMsg);

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t mmPutNodeMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into worker %s, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t mmPutNodeMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->writeWorker, pMsg);
}

int32_t mmPutNodeMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->syncWorker, pMsg);
}

int32_t mmPutNodeMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->readWorker, pMsg);
}

int32_t mmPutNodeMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t mmPutNodeMsgToMonitorQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->monitorWorker, pMsg);
}

static inline int32_t mmPutRpcMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  if (pMsg == NULL) return -1;

  dTrace("msg:%p, create and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t mmPutRpcMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutRpcMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t mmPutRpcMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutRpcMsgToWorker(&pMgmt->writeWorker, pMsg);
}

int32_t mmPutRpcMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutRpcMsgToWorker(&pMgmt->readWorker, pMsg);
}

int32_t mmPutRpcMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t code = -1;
  if (mmAcquire(pMgmt) == 0) {
    code = mmPutRpcMsgToWorker(&pMgmt->syncWorker, pMsg);
    mmRelease(pMgmt);
  }

  if (code != 0) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
  }
  return code;
}

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  SSingleWorkerCfg qCfg = {
      .min = tsNumOfMnodeQueryThreads,
      .max = tsNumOfMnodeQueryThreads,
      .name = "mnode-query",
      .fp = (FItem)mmProcessQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->queryWorker, &qCfg) != 0) {
    dError("failed to start mnode-query worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg rCfg = {
      .min = tsNumOfMnodeReadThreads,
      .max = tsNumOfMnodeReadThreads,
      .name = "mnode-read",
      .fp = (FItem)mmProcessQueue,
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
      .fp = (FItem)mmProcessQueue,
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
      .fp = (FItem)mmProcessSyncQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->syncWorker, &sCfg) != 0) {
    dError("failed to start mnode mnode-sync worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg mCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-monitor",
      .fp = (FItem)mmProcessQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
    dError("failed to start mnode mnode-monitor worker since %s", terrstr());
    return -1;
  }

  dDebug("mnode workers are initialized");
  return 0;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  taosThreadRwlockWrlock(&pMgmt->lock);
  pMgmt->stopped = 1;
  taosThreadRwlockUnlock(&pMgmt->lock);
  while (pMgmt->refCount > 0) taosMsleep(10);

  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  dDebug("mnode workers are closed");
}
