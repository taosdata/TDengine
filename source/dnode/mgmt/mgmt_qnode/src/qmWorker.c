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
#include "qmInt.h"

static inline void qmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void qmProcessQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SQnodeMgmt *pMgmt = pInfo->ahandle;
  int32_t     code = -1;
  dTrace("msg:%p, get from qnode queue", pMsg);

  switch (pMsg->msgType) {
    case TDMT_MON_QM_INFO:
      code = qmProcessGetMonitorInfoReq(pMgmt, pMsg);
      break;
    default:
      code = qndProcessQueryMsg(pMgmt->pQnode, pInfo->timestamp, pMsg);
      break;
  }

  if (IsReq(pMsg) && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (code != 0 && terrno != 0) code = terrno;
    qmSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t qmPutNodeMsgToWorker(SSingleWorker *pWorker, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into worker %s, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t qmPutNodeMsgToQueryQueue(SQnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return qmPutNodeMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t qmPutNodeMsgToFetchQueue(SQnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return qmPutNodeMsgToWorker(&pMgmt->fetchWorker, pMsg);
}

int32_t qmPutNodeMsgToMonitorQueue(SQnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return qmPutNodeMsgToWorker(&pMgmt->monitorWorker, pMsg);
}

static int32_t qmPutRpcMsgToWorker(SQnodeMgmt *pMgmt, SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM);
  if (pMsg == NULL) return -1;

  dTrace("msg:%p, create and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t qmPutRpcMsgToQueryQueue(SQnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return qmPutRpcMsgToWorker(pMgmt, &pMgmt->queryWorker, pRpc);
}

int32_t qmPutRpcMsgToFetchQueue(SQnodeMgmt *pMgmt, SRpcMsg *pRpc) {
  return qmPutRpcMsgToWorker(pMgmt, &pMgmt->fetchWorker, pRpc);
}

int32_t qmGetQueueSize(SQnodeMgmt *pMgmt, int32_t vgId, EQueueType qtype) {
  int32_t size = -1;

  switch (qtype) {
    case QUERY_QUEUE:
      size = taosQueueItemSize(pMgmt->queryWorker.queue);
      break;
    case FETCH_QUEUE:
      size = taosQueueItemSize(pMgmt->fetchWorker.queue);
      break;
    default:
      break;
  }

  return size;
}

int32_t qmStartWorker(SQnodeMgmt *pMgmt) {
  SSingleWorkerCfg queryCfg = {
      .min = tsNumOfVnodeQueryThreads,
      .max = tsNumOfVnodeQueryThreads,
      .name = "qnode-query",
      .fp = (FItem)qmProcessQueue,
      .param = pMgmt,
  };

  if (tSingleWorkerInit(&pMgmt->queryWorker, &queryCfg) != 0) {
    dError("failed to start qnode-query worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg fetchCfg = {
      .min = tsNumOfQnodeFetchThreads,
      .max = tsNumOfQnodeFetchThreads,
      .name = "qnode-fetch",
      .fp = (FItem)qmProcessQueue,
      .param = pMgmt,
  };

  if (tSingleWorkerInit(&pMgmt->fetchWorker, &fetchCfg) != 0) {
    dError("failed to start qnode-fetch worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg mCfg = {
      .min = 1,
      .max = 1,
      .name = "qnode-monitor",
      .fp = (FItem)qmProcessQueue,
      .param = pMgmt,
  };
  if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
    dError("failed to start qnode-monitor worker since %s", terrstr());
    return -1;
  }

  dDebug("qnode workers are initialized");
  return 0;
}

void qmStopWorker(SQnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->fetchWorker);
  dDebug("qnode workers are closed");
}
