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

static void qmSendRsp(SMgmtWrapper *pWrapper, SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {.handle = pMsg->rpcMsg.handle, .ahandle = pMsg->rpcMsg.ahandle, .code = code};
  dndSendRsp(pWrapper, &rsp);
}

static void qmProcessQueryQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SQnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, will be processed in qnode-query queue", pMsg);
  int32_t code = qndProcessQueryMsg(pMgmt->pQnode, &pMsg->rpcMsg);
  if (code != 0) {
    qmSendRsp(pMgmt->pWrapper, pMsg, code);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static void qmProcessFetchQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SQnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, will be processed in qnode-fetch queue", pMsg);
  int32_t code = qndProcessFetchMsg(pMgmt->pQnode, &pMsg->rpcMsg);
  if (code != 0) {
    qmSendRsp(pMgmt->pWrapper, pMsg, code);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

static int32_t qmPutMsgToWorker(SSingleWorker *pWorker, SNodeMsg *pMsg) {
  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t qmProcessQueryMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg) { return qmPutMsgToWorker(&pMgmt->queryWorker, pMsg); }

int32_t qmProcessFetchMsg(SQnodeMgmt *pMgmt, SNodeMsg *pMsg) { return qmPutMsgToWorker(&pMgmt->fetchWorker, pMsg); }

static int32_t qmPutRpcMsgToWorker(SQnodeMgmt *pMgmt, SSingleWorker *pWorker, SRpcMsg *pRpc) {
  SNodeMsg *pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) {
    return -1;
  }

  dTrace("msg:%p, is created and put into worker:%s, type:%s", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType));
  pMsg->rpcMsg = *pRpc;

  int32_t code = taosWriteQitem(pWorker->queue, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }

  return code;
}

int32_t qmPutMsgToQueryQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  return qmPutRpcMsgToWorker(pMgmt, &pMgmt->queryWorker, pRpc);
}

int32_t qmPutMsgToFetchQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRpc) {
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  return qmPutRpcMsgToWorker(pMgmt, &pMgmt->fetchWorker, pRpc);
}

int32_t qmGetQueueSize(SMgmtWrapper *pWrapper, int32_t vgId, EQueueType qtype) {
  int32_t     size = -1;
  SQnodeMgmt *pMgmt = pWrapper->pMgmt;
  switch (qtype) {
    case QUERY_QUEUE:
      size = taosQueueSize(pMgmt->queryWorker.queue);
      break;
    case FETCH_QUEUE:
      size = taosQueueSize(pMgmt->fetchWorker.queue);
      break;
    default:
      break;
  }

  return size;
}

int32_t qmStartWorker(SQnodeMgmt *pMgmt) {
  int32_t maxFetchThreads = 4;
  int32_t minFetchThreads = TMIN(maxFetchThreads, tsNumOfCores);
  int32_t minQueryThreads = TMAX((int32_t)(tsNumOfCores * tsRatioOfQueryCores), 1);
  int32_t maxQueryThreads = minQueryThreads;

  SSingleWorkerCfg queryCfg = {.minNum = minQueryThreads,
                               .maxNum = maxQueryThreads,
                               .name = "qnode-query",
                               .fp = (FItem)qmProcessQueryQueue,
                               .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->queryWorker, &queryCfg) != 0) {
    dError("failed to start qnode-query worker since %s", terrstr());
    return -1;
  }

  SSingleWorkerCfg fetchCfg = {.minNum = minFetchThreads,
                               .maxNum = maxFetchThreads,
                               .name = "qnode-fetch",
                               .fp = (FItem)qmProcessFetchQueue,
                               .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->fetchWorker, &fetchCfg) != 0) {
    dError("failed to start qnode-fetch worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void qmStopWorker(SQnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->fetchWorker);
}
