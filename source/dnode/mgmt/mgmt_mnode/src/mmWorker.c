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

static inline void mmSendRsp(SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .handle = pMsg->rpcMsg.handle,
      .ahandle = pMsg->rpcMsg.ahandle,
      .refId = pMsg->rpcMsg.refId,
      .code = code,
      .pCont = pMsg->pRsp,
      .contLen = pMsg->rspLen,
  };
  tmsgSendRsp(&rsp);
}

static void mmProcessQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  tmsg_t      msgType = pMsg->rpcMsg.msgType;
  bool        isRequest = msgType & 1U;
  dTrace("msg:%p, get from mnode queue", pMsg);

  switch (msgType) {
    case TDMT_DND_ALTER_MNODE:
      code = mmProcessAlterReq(pMgmt, pMsg);
      break;
    case TDMT_MON_MM_INFO:
      code = mmProcessGetMonitorInfoReq(pMgmt, pMsg);
      break;
    case TDMT_MON_MM_LOAD:
      code = mmProcessGetLoadsReq(pMgmt, pMsg);
      break;
    default:
      pMsg->pNode = pMgmt->pMnode;
      code = mndProcessMsg(pMsg);
  }

  if (isRequest) {
    if (pMsg->rpcMsg.handle != NULL && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      if (code != 0 && terrno != 0) code = terrno;
      mmSendRsp(pMsg, code);
    }
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessQueryQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  tmsg_t      msgType = pMsg->rpcMsg.msgType;
  bool        isRequest = msgType & 1U;
  dTrace("msg:%p, get from mnode-query queue", pMsg);

  pMsg->pNode = pMgmt->pMnode;
  code = mndProcessMsg(pMsg);

  if (isRequest) {
    if (pMsg->rpcMsg.handle != NULL && code != 0) {
      if (code != 0 && terrno != 0) code = terrno;
      mmSendRsp(pMsg, code);
    }
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static int32_t mmPutNodeMsgToWorker(SSingleWorker *pWorker, SNodeMsg *pMsg) {
  dTrace("msg:%p, put into worker %s, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->rpcMsg.msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t mmPutNodeMsgToWriteQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) { return mmPutNodeMsgToWorker(&pMgmt->writeWorker, pMsg); }

int32_t mmPutNodeMsgToSyncQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) { return mmPutNodeMsgToWorker(&pMgmt->syncWorker, pMsg); }

int32_t mmPutNodeMsgToReadQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) { return mmPutNodeMsgToWorker(&pMgmt->readWorker, pMsg); }

int32_t mmPutNodeMsgToQueryQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) { return mmPutNodeMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t mmPutNodeMsgToMonitorQueue(SMnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  return mmPutNodeMsgToWorker(&pMgmt->monitorWorker, pMsg);
}

static inline int32_t mmPutRpcMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) return -1;

  dTrace("msg:%p, is created and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  pMsg->rpcMsg = *pRpc;
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t mmPutRpcMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return mmPutRpcMsgToWorker(&pMgmt->queryWorker, pRpc);
}

int32_t mmPutRpcMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return mmPutRpcMsgToWorker(&pMgmt->writeWorker, pRpc);
}

int32_t mmPutRpcMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return mmPutRpcMsgToWorker(&pMgmt->readWorker, pRpc);
}

int32_t mmPutMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pRpc) { return mmPutRpcMsgToWorker(&pMgmt->syncWorker, pRpc); }

int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  SSingleWorkerCfg qCfg = {
      .min = tsNumOfMnodeQueryThreads,
      .max = tsNumOfMnodeQueryThreads,
      .name = "mnode-query",
      .fp = (FItem)mmProcessQueryQueue,
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
      .fp = (FItem)mmProcessQueue,
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
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  dDebug("mnode workers are closed");
}
