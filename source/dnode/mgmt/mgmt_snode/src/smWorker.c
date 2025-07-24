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

static void smProcessRunnerQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SSnodeMgmt     *pMgmt = pInfo->ahandle;
  const STraceId *trace = &pMsg->info.traceId;

  dDebug("msg:%p %d, get from snode-stream-runner queue", pMsg, pMsg->msgType);
  
  int32_t code = sndProcessStreamMsg(pMgmt->pSnode, pInfo->workerCb, pMsg);
  if (code < 0) {
    dGError("snd, msg:%p failed to process stream msg %s since %s", pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code));
  }

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void smSendErrorRrsp(SRpcMsg *pMsg, int32_t errCode) {
  SRpcMsg             rspMsg = {0};

  rspMsg.info = pMsg->info;
  rspMsg.pCont = NULL;
  rspMsg.contLen = 0;
  rspMsg.code = errCode;
  rspMsg.msgType = pMsg->msgType;

  tmsgSendRsp(&rspMsg);
}

static void smProcessStreamTriggerQueue(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SBatchReq     batchReq = {0};
  int64_t       streamId = 0, taskId = 0;
  SStreamTask  *pTask = NULL;
  void         *taskAddr = NULL;
  SMsgSendInfo *ahandle = NULL;

  SSnodeMgmt *pMgmt = pInfo->ahandle;
  STraceId   *trace = &pMsg->info.traceId;
  dGDebug("msg:%p, get from snode-stream-trigger queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  if (pMsg->msgType == TDMT_SND_BATCH_META) {
    code = tDeserializeSBatchReq(pMsg->pCont, pMsg->contLen, &batchReq);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, invalid batch meta request in snode-stream-trigger queue", pMsg);
      smSendErrorRrsp(pMsg, TSDB_CODE_INVALID_MSG);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    SBatchMsg         *pReq = TARRAY_DATA(batchReq.pMsgs);
    SStreamProgressReq req = {0};
    code = tDeserializeStreamProgressReq(pReq->msg, pReq->msgLen, &req);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, invalid stream progress request in snode-stream-trigger queue", pMsg);
      smSendErrorRrsp(pMsg, TSDB_CODE_INVALID_MSG);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    streamId = req.streamId;
    taskId = req.taskId;
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_CTRL) {
    SSTriggerCtrlRequest ctrlReq = {0};
    code = tDeserializeSTriggerCtrlRequest(pMsg->pCont, pMsg->contLen, &ctrlReq);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, invalid trigger control request in snode-stream-trigger queue", pMsg);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    streamId = ctrlReq.streamId;
    taskId = ctrlReq.taskId;
  } else {
    ahandle = pMsg->info.ahandle;
    if (ahandle == NULL) {
      dError("empty ahandle for msg %s", TMSG_INFO(pMsg->msgType));
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    SSTriggerAHandle* pAhandle = ahandle->param;
    if (pAhandle == NULL) {
      dError("empty trigger ahandle for msg %s", TMSG_INFO(pMsg->msgType));
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    streamId = pAhandle->streamId;
    taskId = pAhandle->taskId;
  }

  TAOS_CHECK_EXIT(streamAcquireTask(streamId, taskId, &pTask, &taskAddr));

  int64_t errTaskId = 0;
  code = stTriggerTaskProcessRsp(pTask, pMsg, &errTaskId);
  if (code != TSDB_CODE_SUCCESS) {
    streamHandleTaskError(pTask->streamId, errTaskId, code);
    TAOS_CHECK_EXIT(code);
  }

_exit:
  taosArrayDestroyEx(batchReq.pMsgs, tFreeSBatchReqMsg);
  if (taskAddr != NULL) {
    streamReleaseTask(taskAddr);
  }
  if (ahandle != NULL) {
    destroyAhandle(ahandle);
  }
  dTrace("msg:%p, is freed, code:%d", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
}
static int32_t smDispatchStreamTriggerRsp(struct SDispatchWorkerPool *pPool, void *pParam, int32_t *pWorkerIdx) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  SBatchReq     batchReq = {0};
  SRpcMsg      *pMsg = (SRpcMsg *)pParam;
  int64_t       streamId = 0, taskId = 0, sessionId = 0;
  void         *taskAddr = NULL;
  SMsgSendInfo *ahandle = NULL;

  dDebug("dispatch snode %s msg", TMSG_INFO(pMsg->msgType));

  if (pMsg->msgType == TDMT_SND_BATCH_META) {
    code = tDeserializeSBatchReq(pMsg->pCont, pMsg->contLen, &batchReq);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, failed to deserialize batch meta request", pMsg);
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
    }
    SBatchMsg         *pReq = TARRAY_DATA(batchReq.pMsgs);
    SStreamProgressReq req = {0};
    code = tDeserializeStreamProgressReq(pReq->msg, pReq->msgLen, &req);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, failed to deserialize stream progress request", pMsg);
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
    }
    streamId = req.streamId;
    taskId = req.taskId;
    sessionId = 1;
  } else if (pMsg->msgType == TDMT_STREAM_TRIGGER_CTRL) {
    SSTriggerCtrlRequest ctrlReq = {0};
    code = tDeserializeSTriggerCtrlRequest(pMsg->pCont, pMsg->contLen, &ctrlReq);
    if (code != TSDB_CODE_SUCCESS) {
      dError("msg:%p, failed to deserialize trigger control request", pMsg);
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
    }
    streamId = ctrlReq.streamId;
    taskId = ctrlReq.taskId;
    sessionId = ctrlReq.sessionId;
  } else {
    ahandle = pMsg->info.ahandle;
    if (ahandle == NULL) {
      dError("empty ahandle for msg %s", TMSG_INFO(pMsg->msgType));
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
    }
    SSTriggerAHandle* pAhandle = ahandle->param;
    if (pAhandle == NULL) {
      dError("empty trigger ahandle for msg %s", TMSG_INFO(pMsg->msgType));
      TAOS_CHECK_EXIT(TSDB_CODE_MSG_NOT_PROCESSED);
    }
    streamId = pAhandle->streamId;
    taskId = pAhandle->taskId;
    sessionId = pAhandle->sessionId;
  }

  int64_t  buf[] = {streamId, taskId, sessionId};
  uint32_t hashVal = MurmurHash3_32((const char *)buf, sizeof(buf));
  *pWorkerIdx = hashVal % tsNumOfStreamTriggerThreads;

_exit:
  taosArrayDestroyEx(batchReq.pMsgs, tFreeSBatchReqMsg);
  if (taskAddr != NULL) {
    streamReleaseTask(taskAddr);
  }
  if (code) {
    destroyAhandle(ahandle);
    //rpcFreeCont(pMsg->pCont);
    //taosFreeQitem(pMsg);
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

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
  int32_t code = tAddTaskIntoDispatchWorkerPool(&pMgmt->triggerWorkerPool, pMsg);
  stDebug("msg:%p, put into pool %s, code %d", pMsg, pMgmt->triggerWorkerPool.name, code);
  return code;
}
