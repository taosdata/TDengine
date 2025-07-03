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
#include "stream.h"

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

  dDebug("msg:%p %d, get from snode-stream-runner queue", pMsg, pMsg->msgType);
  
  int32_t code = sndProcessStreamMsg(pMgmt->pSnode, pInfo->workerCb, pMsg);
  if (code < 0) {
    dGError("snd, msg:%p failed to process stream msg %s since %s", pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code));
    smSendRsp(pMsg, terrno);
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void smProcessStreamTriggerQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;
  STraceId   *trace = &pMsg->info.traceId;
  void       *taskAddr = NULL;
  dGTrace("msg:%p, get from snode-stream-trigger queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
  SSTriggerAHandle* pAhandle = pMsg->info.ahandle;

  int32_t      code = TSDB_CODE_SUCCESS;
  SStreamTask *pTask = NULL;
  switch (pMsg->msgType) {
    case TDMT_STREAM_TRIGGER_PULL_RSP: {
      if (pAhandle == NULL) {
        code = TSDB_CODE_INVALID_PARA;
        dError("msg:%p, invalid pull request in snode-stream-trigger queue", pMsg);
        break;
      }

      code = streamAcquireTask(pAhandle->streamId, pAhandle->taskId, &pTask, &taskAddr);
      break;
    }
    case TDMT_STREAM_TRIGGER_CALC_RSP: {
      if (pAhandle == NULL) {
        code = TSDB_CODE_INVALID_PARA;
        dError("msg:%p, invalid calc request in snode-stream-trigger queue", pMsg);
        break;
      }

      code = streamAcquireTask(pAhandle->streamId, pAhandle->taskId, &pTask, &taskAddr);
      break;
    }
    default: {
      dError("msg:%p, invalid msg type %d in snode-stream-trigger queue", pMsg, pMsg->msgType);
      code = TSDB_CODE_INVALID_PARA;
      break;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    int64_t errTaskId = 0;
    code = stTriggerTaskProcessRsp(pTask, pMsg, &errTaskId);
    if (code != TSDB_CODE_SUCCESS) {
      streamHandleTaskError(pTask->streamId, errTaskId, code);
    }
  }

  streamReleaseTask(taskAddr);
  taosMemoryFree(pAhandle);

  dTrace("msg:%p, is freed, code:%d", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static int32_t smDispatchStreamTriggerRsp(struct SDispatchWorkerPool *pPool, void *pParam, int32_t *pWorkerIdx) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  SRpcMsg *pMsg = (SRpcMsg *)pParam;
  SSTriggerAHandle* pAhandle = pMsg->info.ahandle;
  if (pAhandle == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    dError("empty ahandle for msg %s", TMSG_INFO(pMsg->msgType));
    return code;
  }

  SStreamTask *pTask = NULL;  
  void       *taskAddr = NULL;  
  TAOS_CHECK_EXIT(streamAcquireTask(pAhandle->streamId, pAhandle->taskId, &pTask, &taskAddr));

  switch (pMsg->msgType) {
    case TDMT_STREAM_TRIGGER_PULL_RSP: {
      SSTriggerPullRequest *pReq = pAhandle->param;
      if (pReq == NULL){
        dError("msg:%p, invalid trigger-pull-resp without request ahandle", pMsg);
        TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
        break;
      }
      int64_t               buf[] = {pReq->streamId, pReq->triggerTaskId, pReq->sessionId};
      uint32_t              hashVal = MurmurHash3_32((const char *)buf, sizeof(buf));
      *pWorkerIdx = hashVal % tsNumOfStreamTriggerThreads;
      break;
    }

    case TDMT_STREAM_TRIGGER_CALC_RSP: {
      SSTriggerCalcRequest *pReq = pAhandle->param;
      int64_t               buf[] = {pReq->streamId, pReq->triggerTaskId, pReq->sessionId};
      uint32_t              hashVal = MurmurHash3_32((const char *)buf, sizeof(buf));
      *pWorkerIdx = hashVal % tsNumOfStreamTriggerThreads;
      break;
    }

    default: {
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
      break;
    }
  }

_exit:

  if (code) {
    taosMemoryFree(pAhandle);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  streamReleaseTask(taskAddr);

  return code;
}

int32_t smStartWorker(SSnodeMgmt *pMgmt) {
  int32_t code = 0;

  SSingleWorkerCfg cfg = {
      .min = tsNumOfStreamRunnerThreads,
      .max = tsNumOfStreamRunnerThreads,
      .name = "snode-stream-runner",
      .fp = (FItem)smProcessRunnerQueue,
      .param = pMgmt,
      .poolType = QUERY_AUTO_QWORKER_POOL,
      .stopNoWaitQueue = true,
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
  code = tDispatchWorkerAllocQueue(pTriggerPool, pMgmt, (FItem)smProcessStreamTriggerQueue, smDispatchStreamTriggerRsp);
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
