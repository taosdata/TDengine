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
  dTrace("msg:%p, get from qnode queue", pMsg);

  int32_t code = qndProcessQueryMsg(pMgmt->pQnode, pInfo->timestamp, pMsg);
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
  qndPreprocessQueryMsg(pMgmt->pQnode, pMsg);

  return qmPutNodeMsgToWorker(&pMgmt->queryWorker, pMsg);
}

int32_t qmPutNodeMsgToFetchQueue(SQnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return qmPutNodeMsgToWorker(&pMgmt->fetchWorker, pMsg);
}

int32_t qmPutRpcMsgToQueue(SQnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  SRpcMsg *pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) return -1;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case QUERY_QUEUE:
      dTrace("msg:%p, is created and will put into qnode-query queue, len:%d", pMsg, pRpc->contLen);
      taosWriteQitem(pMgmt->queryWorker.queue, pMsg);
      return 0;
    case READ_QUEUE:
    case FETCH_QUEUE:
      dTrace("msg:%p, is created and will put into qnode-fetch queue, len:%d", pMsg, pRpc->contLen);
      taosWriteQitem(pMgmt->fetchWorker.queue, pMsg);
      return 0;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return -1;
  }
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

  dDebug("qnode workers are initialized");
  return 0;
}

void qmStopWorker(SQnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->fetchWorker);
  dDebug("qnode workers are closed");
}
