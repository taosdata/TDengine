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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "executor.h"
#include "sndInt.h"
#include "tstream.h"
#include "tuuid.h"

void sndEnqueueStreamDispatch(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msgStr = pMsg->pCont;
  char   *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t code = 0;

  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    goto FAIL;
  }

  tDecoderClear(&decoder);

  int32_t taskId = req.taskId;

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = {
        .info = pMsg->info,
        .code = 0,
    };
    streamProcessDispatchMsg(pTask, &req, &rsp, false);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    return;
  }

FAIL:
  if (pMsg->info.handle == NULL) return;
  SRpcMsg rsp = {
      .code = code,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t sndExpandTask(SSnode *pSnode, SStreamTask *pTask, int64_t ver) {
  ASSERT(pTask->taskLevel == TASK_LEVEL__AGG && taosArrayGetSize(pTask->childEpInfo) != 0);

  pTask->refCnt = 1;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;

  pTask->inputQueue = streamQueueOpen(0);
  pTask->outputQueue = streamQueueOpen(0);

  if (pTask->inputQueue == NULL || pTask->outputQueue == NULL) {
    return -1;
  }

  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMsgCb = &pSnode->msgCb;
  pTask->chkInfo.version = ver;
  pTask->pMeta = pSnode->pMeta;

  pTask->pState = streamStateOpen(pSnode->path, pTask, false, -1, -1);
  if (pTask->pState == NULL) {
    return -1;
  }

  int32_t numOfChildEp = taosArrayGetSize(pTask->childEpInfo);
  SReadHandle handle = { .vnode = NULL, .numOfVgroups = numOfChildEp, .pStateBackend = pTask->pState };
  initStreamStateAPI(&handle.api);

  pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, 0);
  ASSERT(pTask->exec.pExecutor);

  streamSetupTrigger(pTask);
  return 0;
}

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pSnode->path = taosStrdup(path);
  if (pSnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }
  pSnode->msgCb = pOption->msgCb;

  pSnode->pMeta = streamMetaOpen(path, pSnode, (FTaskExpand *)sndExpandTask, SNODE_HANDLE);
  if (pSnode->pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  return pSnode;

FAIL:
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
  return NULL;
}

void sndClose(SSnode *pSnode) {
  streamMetaCommit(pSnode->pMeta);
  streamMetaClose(pSnode->pMeta);
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
}

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

int32_t sndProcessTaskDeployReq(SSnode *pSnode, char *msg, int32_t msgLen) {
  int32_t code;

  // 1.deserialize msg and build task
  SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t *)msg, msgLen);
  code = tDecodeStreamTask(&decoder, pTask);
  if (code < 0) {
    tDecoderClear(&decoder);
    taosMemoryFree(pTask);
    return -1;
  }
  tDecoderClear(&decoder);

  ASSERT(pTask->taskLevel == TASK_LEVEL__AGG);

  // 2.save task
  taosWLockLatch(&pSnode->pMeta->lock);
  code = streamMetaAddDeployedTask(pSnode->pMeta, -1, pTask);
  if (code < 0) {
    taosWUnLockLatch(&pSnode->pMeta->lock);
    return -1;
  }

  taosWUnLockLatch(&pSnode->pMeta->lock);

  // 3.go through recover steps to fill history
  if (pTask->fillHistory) {
    streamSetParamForRecover(pTask);
    streamAggRecoverPrepare(pTask);
  }

  return 0;
}

int32_t sndProcessTaskDropReq(SSnode *pSnode, char *msg, int32_t msgLen) {
  SVDropStreamTaskReq *pReq = (SVDropStreamTaskReq *)msg;
  streamMetaRemoveTask(pSnode->pMeta, pReq->taskId);
  return 0;
}

int32_t sndProcessTaskRunReq(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamTaskRunReq *pReq = pMsg->pCont;
  int32_t            taskId = pReq->taskId;
  SStreamTask       *pTask = streamMetaAcquireTask(pSnode->pMeta, taskId);
  if (pTask) {
    streamProcessRunReq(pTask);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskDispatchReq(SSnode *pSnode, SRpcMsg *pMsg, bool exec) {
  char              *msgStr = pMsg->pCont;
  char              *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t *)msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);
  int32_t taskId = req.taskId;

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0 };
    streamProcessDispatchMsg(pTask, &req, &rsp, exec);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskRetrieveReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char              *msgStr = pMsg->pCont;
  char              *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamRetrieveReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);
  int32_t      taskId = req.dstTaskId;
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, taskId);

  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0};
    streamProcessRetrieveReq(pTask, &req, &rsp);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    tDeleteStreamRetrieveReq(&req);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskDispatchRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamDispatchRsp *pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t             taskId = ntohl(pRsp->upstreamTaskId);
  SStreamTask        *pTask = streamMetaAcquireTask(pSnode->pMeta, taskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
  return 0;
}

int32_t sndProcessTaskRetrieveRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  //
  return 0;
}

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  void   *pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY:
      return sndProcessTaskDeployReq(pSnode, pReq, len);
    case TDMT_STREAM_TASK_DROP:
      return sndProcessTaskDropReq(pSnode, pReq, len);
    default:
      ASSERT(0);
  }
  return 0;
}

int32_t sndProcessTaskRecoverFinishReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamRecoverFinishReq req;

  SDecoder decoder;
  tDecoderInit(&decoder, msg, msgLen);
  tDecodeSStreamRecoverFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  // find task
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.taskId);
  if (pTask == NULL) {
    return -1;
  }
  // do process request
  if (streamProcessRecoverFinishReq(pTask, req.childId) < 0) {
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return -1;
  }

  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return 0;
}

int32_t sndProcessTaskRecoverFinishRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  //
  return 0;
}

int32_t sndProcessStreamMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_RUN:
      return sndProcessTaskRunReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_DISPATCH:
      return sndProcessTaskDispatchReq(pSnode, pMsg, true);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return sndProcessTaskDispatchRsp(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return sndProcessTaskRetrieveReq(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:
      return sndProcessTaskRetrieveRsp(pSnode, pMsg);
    case TDMT_STREAM_RECOVER_FINISH:
      return sndProcessTaskRecoverFinishReq(pSnode, pMsg);
    case TDMT_STREAM_RECOVER_FINISH_RSP:
      return sndProcessTaskRecoverFinishRsp(pSnode, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}
