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
#include "tuuid.h"
/*SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) { return NULL; }*/
/*void    sndClose(SSnode *pSnode) {}*/
int32_t sndProcessUMsg(SSnode *pSnode, SRpcMsg *pMsg) { return 0; }
int32_t sndProcessSMsg(SSnode *pSnode, SRpcMsg *pMsg) { return 0; }

SSnode *sndOpen(const char *path, const SSnodeOpt *pOption) {
  SSnode *pSnode = taosMemoryCalloc(1, sizeof(SSnode));
  if (pSnode == NULL) {
    return NULL;
  }
  pSnode->msgCb = pOption->msgCb;
#if 0
  pSnode->pMeta = sndMetaNew();
  if (pSnode->pMeta == NULL) {
    taosMemoryFree(pSnode);
    return NULL;
  }
#endif
  return pSnode;
}

void sndClose(SSnode *pSnode) {
  /*sndMetaDelete(pSnode->pMeta);*/
  taosMemoryFree(pSnode);
}

int32_t sndGetLoad(SSnode *pSnode, SSnodeLoad *pLoad) { return 0; }

#if 0
SStreamMeta *sndMetaNew() {
  SStreamMeta *pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    return NULL;
  }
  pMeta->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMeta->pHash == NULL) {
    taosMemoryFree(pMeta);
    return NULL;
  }
  return pMeta;
}

void sndMetaDelete(SStreamMeta *pMeta) {
  taosHashCleanup(pMeta->pHash);
  taosMemoryFree(pMeta);
}

int32_t sndMetaDeployTask(SStreamMeta *pMeta, SStreamTask *pTask) {
  pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, NULL);
  return taosHashPut(pMeta->pHash, &pTask->taskId, sizeof(int32_t), pTask, sizeof(void *));
}

SStreamTask *sndMetaGetTask(SStreamMeta *pMeta, int32_t taskId) {
  return taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
}

int32_t sndMetaRemoveTask(SStreamMeta *pMeta, int32_t taskId) {
  SStreamTask *pTask = taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  if (pTask == NULL) {
    return -1;
  }
  taosMemoryFree(pTask->exec.qmsg);
  // TODO:free executor
  taosMemoryFree(pTask);
  return taosHashRemove(pMeta->pHash, &taskId, sizeof(int32_t));
}

static int32_t sndProcessTaskDeployReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;
  char        *msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t *)msg, msgLen);
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    ASSERT(0);
  }
  tDecoderClear(&decoder);

  pTask->execStatus = TASK_EXEC_STATUS__IDLE;

  pTask->inputQueue = streamQueueOpen();
  pTask->outputQueue = streamQueueOpen();
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_INPUT_STATUS__NORMAL;

  if (pTask->inputQueue == NULL || pTask->outputQueue == NULL) goto FAIL;

  pTask->pMsgCb = &pNode->msgCb;

  pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, NULL);
  ASSERT(pTask->exec.executor);

  streamSetupTrigger(pTask);

  qInfo("deploy stream: stream id %" PRId64 " task id %d child id %d on snode", pTask->streamId, pTask->taskId,
        pTask->selfChildId);

  taosHashPut(pMeta->pHash, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void *));

  return 0;

FAIL:
  if (pTask->inputQueue) streamQueueClose(pTask->inputQueue);
  if (pTask->outputQueue) streamQueueClose(pTask->outputQueue);
  if (pTask) taosMemoryFree(pTask);
  return -1;
}

static int32_t sndProcessTaskRunReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta       *pMeta = pNode->pMeta;
  SStreamTaskRunReq *pReq = pMsg->pCont;
  int32_t            taskId = pReq->taskId;
  SStreamTask       *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  streamProcessRunReq(pTask);
  return 0;
}

static int32_t sndProcessTaskDispatchReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  char   *msgStr = pMsg->pCont;
  char   *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);
  int32_t      taskId = req.taskId;
  SStreamTask *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  SRpcMsg      rsp = {
           .info = pMsg->info,
           .code = 0,
  };
  streamProcessDispatchReq(pTask, &req, &rsp, true);
  return 0;
}

static int32_t sndProcessTaskRecoverReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  SStreamTaskRecoverReq *pReq = pMsg->pCont;
  int32_t                taskId = pReq->taskId;
  SStreamTask           *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  streamProcessRecoverReq(pTask, pReq, pMsg);
  return 0;
}

static int32_t sndProcessTaskDispatchRsp(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  SStreamDispatchRsp *pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t             taskId = pRsp->taskId;
  SStreamTask        *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  streamProcessDispatchRsp(pTask, pRsp);
  return 0;
}

static int32_t sndProcessTaskRecoverRsp(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  SStreamTaskRecoverRsp *pRsp = pMsg->pCont;
  int32_t                taskId = pRsp->rspTaskId;
  SStreamTask           *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  streamProcessRecoverRsp(pTask, pRsp);
  return 0;
}

static int32_t sndProcessTaskDropReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  char                *msg = pMsg->pCont;
  int32_t              msgLen = pMsg->contLen;
  SVDropStreamTaskReq *pReq = (SVDropStreamTaskReq *)msg;
  int32_t              code = taosHashRemove(pMeta->pHash, &pReq->taskId, sizeof(int32_t));
  ASSERT(code == 0);
  if (code == 0) {
    // sendrsp
  }
  return code;
}

static int32_t sndProcessTaskRetrieveReq(SSnode *pNode, SRpcMsg *pMsg) {
  SStreamMeta *pMeta = pNode->pMeta;

  char              *msgStr = pMsg->pCont;
  char              *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamRetrieveReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  int32_t      taskId = req.dstTaskId;
  SStreamTask *pTask = *(SStreamTask **)taosHashGet(pMeta->pHash, &taskId, sizeof(int32_t));
  if (atomic_load_8(&pTask->taskStatus) != TASK_STATUS__NORMAL) {
    return 0;
  }
  SRpcMsg rsp = {
      .info = pMsg->info,
      .code = 0,
  };
  streamProcessRetrieveReq(pTask, &req, &rsp);
  return 0;
}

static int32_t sndProcessTaskRetrieveRsp(SSnode *pNode, SRpcMsg *pMsg) {
  //
  return 0;
}

int32_t sndProcessUMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  // stream deploy
  // stream stop/resume
  // operator exec
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY:
      return sndProcessTaskDeployReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_DROP:
      return sndProcessTaskDropReq(pSnode, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}

int32_t sndProcessSMsg(SSnode *pSnode, SRpcMsg *pMsg) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_RUN:
      return sndProcessTaskRunReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_DISPATCH:
      return sndProcessTaskDispatchReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_RECOVER:
      return sndProcessTaskRecoverReq(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return sndProcessTaskRecoverReq(pSnode, pMsg);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return sndProcessTaskDispatchRsp(pSnode, pMsg);
    case TDMT_STREAM_TASK_RECOVER_RSP:
      return sndProcessTaskRecoverRsp(pSnode, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:
      return sndProcessTaskRetrieveRsp(pSnode, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}
#endif
