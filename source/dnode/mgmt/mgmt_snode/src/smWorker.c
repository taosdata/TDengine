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
#include "smInt.h"

static inline void smSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void smProcessRunnerQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SSnodeMgmt     *pMgmt = pInfo->ahandle;
  const STraceId *trace = &pMsg->info.traceId;

  dTrace("msg:%p, get from snode-stream-runner queue", pMsg);
  int32_t code = sndProcessStreamMsg(pMgmt->pSnode, pMsg);
  if (code < 0) {
    dGError("snd, msg:%p failed to process stream msg %s since %s", pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code));
    smSendRsp(pMsg, terrno);
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t smStartWorker(SSnodeMgmt *pMgmt) {
  int32_t code = 0;

  SSingleWorkerCfg cfg = {
      .min = tsNumOfStreamRunnerThreads,
      .max = tsNumOfStreamRunnerThreads,
      .name = "snode-stream-runner",
      .fp = (FItem)smProcessRunnerQueue,
      .param = pMgmt,
  };

  if ((code = tSingleWorkerInit(&pMgmt->runnerWorker, &cfg)) != 0) {
    dError("failed to start snode runner worker since %s", tstrerror(code));
    return code;
  }

  SDispatchWorkerPool* pTriggerPool = &pMgmt->triggerWorkerPool;
  pTriggerPool->max = tsNumOfStreamTriggerThreads;
  pTriggerPool->name = "snode-stream-trigger";
  code = tDispatchWorkerInit(pTriggerPool);
  if (code != 0) {
    dError("failed to start snode stream-trigger worker since %s", tstrerror(code));
    return code;
  }
  code = tDispatchWorkerAllocQueue(pTriggerPool, pMgmt, NULL, NULL); // TODO wjm set fp
  if (code != 0) {
    dError("failed to start snode stream-trigger worker since %s", tstrerror(code));
    return code;
  }

  dDebug("snode workers are initialized");
  return code;
}

void smStopWorker(SSnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->runnerWorker);
  tDispatchWorkerCleanup(&pMgmt->triggerWorkerPool);
  dDebug("snode workers are closed");
}

int32_t smPutMsgToQueue(SSnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  int32_t  code;
  SRpcMsg *pMsg;

  code = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen, (void **)&pMsg);
  if (code) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code = terrno;
  }

  SSnode *pSnode = pMgmt->pSnode;
  if (pSnode == NULL) {
    code = terrno;
    dError("msg:%p failed to put into snode queue since %s, type:%s qtype:%d len:%d", pMsg, tstrerror(code),
           TMSG_INFO(pMsg->msgType), qtype, pRpc->contLen);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code;
  }

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = SNODE_HANDLE;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case STREAM_RUNNER_QUEUE:
      code = smPutMsgToRunnerQueue(pMgmt, pMsg);
      break;
    case STREAM_TRIGGER_QUEUE:
      code = smPutMsgToTriggerQueue(pMgmt, pMsg);
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return code;
  }
  return code;
}

int32_t smPutMsgToRunnerQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->runnerWorker;

  dTrace("msg:%p, put into worker %s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smPutMsgToTriggerQueue(SSnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  dTrace("msg:%p, put into pool %s", pMsg, pMgmt->triggerWorkerPool.name);
  return tAddTaskIntoDispatchWorkerPool(&pMgmt->triggerWorkerPool, pMsg);
}
