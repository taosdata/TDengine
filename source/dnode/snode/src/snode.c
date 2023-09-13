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

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.taskId);
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
  SRpcMsg rsp = { .code = code, .info = pMsg->info};
  tmsgSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

int32_t sndExpandTask(SSnode *pSnode, SStreamTask *pTask, int64_t ver) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG && taosArrayGetSize(pTask->pUpstreamInfoList) != 0);
  int32_t code = streamTaskInit(pTask, pSnode->pMeta, &pSnode->msgCb, ver);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  streamTaskOpenAllUpstreamInput(pTask);

  pTask->pState = streamStateOpen(pSnode->path, pTask, false, -1, -1);
  if (pTask->pState == NULL) {
    qError("s-task:%s failed to open state for task", pTask->id.idStr);
    return -1;
  } else {
    qDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
  }

  int32_t numOfChildEp = taosArrayGetSize(pTask->pUpstreamInfoList);
  SReadHandle handle = { .vnode = NULL, .numOfVgroups = numOfChildEp, .pStateBackend = pTask->pState, .fillHistory = pTask->info.fillHistory };
  initStreamStateAPI(&handle.api);

  pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, 0, pTask->id.taskId);
  ASSERT(pTask->exec.pExecutor);

  streamTaskResetUpstreamStageInfo(pTask);
  streamSetupScheduleTrigger(pTask);

  SCheckpointInfo* pChkInfo = &pTask->chkInfo;
  // checkpoint ver is the kept version, handled data should be the next version.
  if (pTask->chkInfo.checkpointId != 0) {
    pTask->chkInfo.nextProcessVer = pTask->chkInfo.checkpointVer + 1;
    qInfo("s-task:%s restore from the checkpointId:%" PRId64 " ver:%" PRId64 " nextProcessVer:%" PRId64, pTask->id.idStr,
           pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
  } else {
    if (pTask->chkInfo.nextProcessVer == -1) {
      pTask->chkInfo.nextProcessVer = 0;
    }
  }

  qInfo("snode:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64 " nextProcessVer:%" PRId64
         " child id:%d, level:%d, status:%s fill-history:%d, trigger:%" PRId64 " ms",
        SNODE_HANDLE, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
         pTask->info.selfChildId, pTask->info.taskLevel, streamGetTaskStatusStr(pTask->status.taskStatus),
         pTask->info.fillHistory, pTask->info.triggerParam);

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

  pSnode->pMeta = streamMetaOpen(path, pSnode, (FTaskExpand *)sndExpandTask, SNODE_HANDLE, -1);
  if (pSnode->pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  // todo fix it: send msg to mnode to rollback to an existed checkpoint, and broadcast the rollback msg to all other
  // computing nodes.
  pSnode->pMeta->stage = 0;

  return pSnode;

FAIL:
  taosMemoryFree(pSnode->path);
  taosMemoryFree(pSnode);
  return NULL;
}

void sndClose(SSnode *pSnode) {
  streamMetaNotifyClose(pSnode->pMeta);
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

  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG);

  // 2.save task
  taosWLockLatch(&pSnode->pMeta->lock);

  bool added = false;
  code = streamMetaRegisterTask(pSnode->pMeta, -1, pTask, &added);
  if (code < 0) {
    taosWUnLockLatch(&pSnode->pMeta->lock);
    return -1;
  }

  int32_t numOfTasks = streamMetaGetNumOfTasks(pSnode->pMeta);
  taosWUnLockLatch(&pSnode->pMeta->lock);
  qDebug("snode:%d s-task:%s is deployed on snode and add into meta, status:%s, numOfTasks:%d", SNODE_HANDLE, pTask->id.idStr,
         streamGetTaskStatusStr(pTask->status.taskStatus), numOfTasks);

  streamTaskCheckDownstream(pTask);
  return 0;
}

int32_t sndProcessTaskDropReq(SSnode *pSnode, char *msg, int32_t msgLen) {
  SVDropStreamTaskReq *pReq = (SVDropStreamTaskReq *)msg;
  qDebug("snode:%d receive msg to drop stream task:0x%x", pSnode->pMeta->vgId, pReq->taskId);

  SStreamTask* pTask = streamMetaAcquireTask(pSnode->pMeta, pReq->streamId, pReq->taskId);
  if (pTask == NULL) {
    qError("vgId:%d failed to acquire s-task:0x%x when dropping it", pSnode->pMeta->vgId, pReq->taskId);
    return 0;
  }

  streamMetaUnregisterTask(pSnode->pMeta, pReq->streamId, pReq->taskId);
  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return 0;
}

int32_t sndProcessTaskRunReq(SSnode *pSnode, SRpcMsg *pMsg) {
  SStreamTaskRunReq *pReq = pMsg->pCont;

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, pReq->streamId, pReq->taskId);
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

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.taskId);
  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
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
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.dstTaskId);

  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
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

  int32_t taskId = htonl(pRsp->upstreamTaskId);
  int64_t streamId = htobe64(pRsp->streamId);

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, streamId, taskId);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pSnode->pMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t sndProcessTaskRetrieveRsp(SSnode *pSnode, SRpcMsg *pMsg) {
  //
  return 0;
}

int32_t sndProcessWriteMsg(SSnode *pSnode, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_DEPLOY: {
      void   *pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
      int32_t len = pMsg->contLen - sizeof(SMsgHead);
      return sndProcessTaskDeployReq(pSnode, pReq, len);
    }

    case TDMT_STREAM_TASK_DROP:
      return sndProcessTaskDropReq(pSnode, pMsg->pCont, pMsg->contLen);
    default:
      ASSERT(0);
  }
  return 0;
}

int32_t sndProcessStreamTaskScanHistoryFinishReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamScanHistoryFinishReq req;

  SDecoder decoder;
  tDecoderInit(&decoder, msg, msgLen);
  tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  // find task
  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, req.downstreamTaskId);
  if (pTask == NULL) {
    return -1;
  }
  // do process request
  if (streamProcessScanHistoryFinishReq(pTask, &req, &pMsg->info) < 0) {
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

int32_t sndProcessStreamTaskCheckReq(SSnode *pSnode, SRpcMsg *pMsg) {
  char   *msgStr = pMsg->pCont;
  char   *msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SDecoder            decoder;

  tDecoderInit(&decoder, (uint8_t *)msgBody, msgLen);
  tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  int32_t taskId = req.downstreamTaskId;

  SStreamTaskCheckRsp rsp = {
      .reqId = req.reqId,
      .streamId = req.streamId,
      .childId = req.childId,
      .downstreamNodeId = req.downstreamNodeId,
      .downstreamTaskId = req.downstreamTaskId,
      .upstreamNodeId = req.upstreamNodeId,
      .upstreamTaskId = req.upstreamTaskId,
  };

  SStreamTask *pTask = streamMetaAcquireTask(pSnode->pMeta, req.streamId, taskId);

  if (pTask != NULL) {
    rsp.status = streamTaskCheckStatus(pTask, req.upstreamTaskId, req.upstreamNodeId, req.stage);
    streamMetaReleaseTask(pSnode->pMeta, pTask);

    const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
    qDebug("s-task:%s status:%s, recv task check req(reqId:0x%" PRIx64 ") task:0x%x (vgId:%d), ready:%d",
            pTask->id.idStr, pStatus, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  } else {
    rsp.status = 0;
    qDebug("recv task check(taskId:0x%x not built yet) req(reqId:0x%" PRIx64 ") from task:0x%x (vgId:%d), rsp status %d",
           taskId, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  }

  SEncoder encoder;
  int32_t  code;
  int32_t  len;

  tEncodeSize(tEncodeStreamTaskCheckRsp, &rsp, len, code);
  if (code < 0) {
    qError("vgId:%d failed to encode task check rsp, task:0x%x", pSnode->pMeta->vgId, taskId);
    return -1;
  }

  void *buf = rpcMallocCont(sizeof(SMsgHead) + len);
  ((SMsgHead *)buf)->vgId = htonl(req.upstreamNodeId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t *)abuf, len);
  tEncodeStreamTaskCheckRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = {.code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = pMsg->info};

  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t sndProcessStreamTaskCheckRsp(SSnode* pSnode, SRpcMsg* pMsg) {
  char* pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  int32_t             code;
  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  code = tDecodeStreamTaskCheckRsp(&decoder, &rsp);

  if (code < 0) {
    tDecoderClear(&decoder);
    return -1;
  }

  tDecoderClear(&decoder);
  qDebug("tq task:0x%x (vgId:%d) recv check rsp(reqId:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d",
          rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  SStreamTask* pTask = streamMetaAcquireTask(pSnode->pMeta, rsp.streamId, rsp.upstreamTaskId);
  if (pTask == NULL) {
    qError("tq failed to locate the stream task:0x%x (vgId:%d), it may have been destroyed", rsp.upstreamTaskId,
            pSnode->pMeta->vgId);
    return -1;
  }

  code = streamProcessCheckRsp(pTask, &rsp);
  streamMetaReleaseTask(pSnode->pMeta, pTask);
  return code;
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
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH:
      return sndProcessStreamTaskScanHistoryFinishReq(pSnode, pMsg);
    case TDMT_VND_STREAM_SCAN_HISTORY_FINISH_RSP:
      return sndProcessTaskRecoverFinishRsp(pSnode, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK:
      return sndProcessStreamTaskCheckReq(pSnode, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK_RSP:
      return sndProcessStreamTaskCheckRsp(pSnode, pMsg);
    default:
      ASSERT(0);
  }
  return 0;
}
