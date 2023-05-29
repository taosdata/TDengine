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

#include "streamInc.h"

int32_t streamTaskLaunchRecover(SStreamTask* pTask, int64_t version) {
  qDebug("s-task:%s at node %d launch recover", pTask->id.idStr, pTask->nodeId);

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__RECOVER_PREPARE);
    qDebug("s-task:%s set task status:%d and start to recover", pTask->id.idStr, pTask->status.taskStatus);

    streamSetParamForRecover(pTask);
    streamSourceRecoverPrepareStep1(pTask, version);

    SStreamRecoverStep1Req req;
    streamBuildSourceRecover1Req(pTask, &req);
    int32_t len = sizeof(SStreamRecoverStep1Req);

    void* serializedReq = rpcMallocCont(len);
    if (serializedReq == NULL) {
      return -1;
    }

    memcpy(serializedReq, &req, len);

    SRpcMsg rpcMsg = { .contLen = len, .pCont = serializedReq, .msgType = TDMT_VND_STREAM_RECOVER_NONBLOCKING_STAGE };
    if (tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &rpcMsg) < 0) {
      /*ASSERT(0);*/
    }

  } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__NORMAL);
    streamSetParamForRecover(pTask);
    streamAggRecoverPrepare(pTask);
  } else if (pTask->taskLevel == TASK_LEVEL__SINK) {
    // sink nodes has no specified operation for fill history
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__NORMAL);
  }

  return 0;
}

// checkstatus
int32_t streamTaskCheckDownstream(SStreamTask* pTask, int64_t version) {
  qDebug("s-task:%s in fill history stage, ver:%"PRId64, pTask->id.idStr, version);

  SStreamTaskCheckReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->nodeId,
      .childId = pTask->selfChildId,
  };

  // serialize
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {

    req.reqId = tGenIdPI64();
    req.downstreamNodeId = pTask->fixedEpDispatcher.nodeId;
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->checkReqId = req.reqId;

    qDebug("s-task:%s at node %d check downstream task:0x%x at node %d", pTask->id.idStr, pTask->nodeId, req.downstreamTaskId,
           req.downstreamNodeId);
    streamDispatchCheckMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->recoverTryingDownstream = numOfVgs;
    pTask->checkReqIds = taosArrayInit(numOfVgs, sizeof(int64_t));

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.reqId = tGenIdPI64();
      taosArrayPush(pTask->checkReqIds, &req.reqId);
      req.downstreamNodeId = pVgInfo->vgId;
      req.downstreamTaskId = pVgInfo->taskId;
      qDebug("s-task:%s at node %d check downstream task:0x%x at node %d (shuffle)", pTask->id.idStr, pTask->nodeId,
             req.downstreamTaskId, req.downstreamNodeId);
      streamDispatchCheckMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s at node %d direct launch recover since no downstream", pTask->id.idStr, pTask->nodeId);
    streamTaskLaunchRecover(pTask, version);
  }

  return 0;
}

int32_t streamRecheckOneDownstream(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp) {
  SStreamTaskCheckReq req = {
      .reqId = pRsp->reqId,
      .streamId = pRsp->streamId,
      .upstreamTaskId = pRsp->upstreamTaskId,
      .upstreamNodeId = pRsp->upstreamNodeId,
      .downstreamTaskId = pRsp->downstreamTaskId,
      .downstreamNodeId = pRsp->downstreamNodeId,
      .childId = pRsp->childId,
  };

  qDebug("s-task:%s at node %d check downstream task:0x%x at node %d (recheck)", pTask->id.idStr, pTask->nodeId,
         req.downstreamTaskId, req.downstreamNodeId);

  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo->taskId == req.downstreamTaskId) {
        streamDispatchCheckMsg(pTask, &req, pRsp->downstreamNodeId, &pVgInfo->epSet);
      }
    }
  }

  return 0;
}

int32_t streamTaskCheckStatus(SStreamTask* pTask) {
  return atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__NORMAL? 1:0;
}

int32_t streamProcessTaskCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp, int64_t version) {
  ASSERT(pTask->id.taskId == pRsp->upstreamTaskId);

  qDebug("s-task:%s at node %d recv check rsp from task:0x%x at node %d: status %d", pTask->id.idStr,
         pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status);

  if (pRsp->status == 1) {
    if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      bool found = false;

      int32_t numOfReqs = taosArrayGetSize(pTask->checkReqIds);
      for (int32_t i = 0; i < numOfReqs; i++) {
        int64_t reqId = *(int64_t*)taosArrayGet(pTask->checkReqIds, i);
        if (reqId == pRsp->reqId) {
          found = true;
          break;
        }
      }

      if (!found) {
        return -1;
      }

      int32_t left = atomic_sub_fetch_32(&pTask->recoverTryingDownstream, 1);
      ASSERT(left >= 0);

      if (left == 0) {
        taosArrayDestroy(pTask->checkReqIds);
        pTask->checkReqIds = NULL;

        qDebug("s-task:%s all %d downstream tasks are ready, now enter into recover stage", pTask->id.idStr, numOfReqs);
        streamTaskLaunchRecover(pTask, version);
      }
    } else if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
      if (pRsp->reqId != pTask->checkReqId) {
        return -1;
      }

      streamTaskLaunchRecover(pTask, version);
    } else {
      ASSERT(0);
    }
  } else { // not ready, wait for 100ms and retry
    qDebug("s-task:%s downstream taskId:0x%x (vgId:%d) not ready, wait for 100ms and retry", pTask->id.idStr,
        pRsp->downstreamTaskId, pRsp->downstreamNodeId);
    taosMsleep(100);

    streamRecheckOneDownstream(pTask, pRsp);
  }

  return 0;
}

// common
int32_t streamSetParamForRecover(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamSetParamForRecover(exec);
}
int32_t streamRestoreParam(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  return qStreamRestoreParam(exec);
}

int32_t streamSetStatusNormal(SStreamTask* pTask) {
  atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__NORMAL);
  return 0;
}

// source
int32_t streamSourceRecoverPrepareStep1(SStreamTask* pTask, int64_t ver) {
  void* exec = pTask->exec.pExecutor;
  return qStreamSourceRecoverStep1(exec, ver);
}

int32_t streamBuildSourceRecover1Req(SStreamTask* pTask, SStreamRecoverStep1Req* pReq) {
  pReq->msgHead.vgId = pTask->nodeId;
  pReq->streamId = pTask->id.streamId;
  pReq->taskId = pTask->id.taskId;
  return 0;
}

int32_t streamSourceRecoverScanStep1(SStreamTask* pTask) {
  return streamScanExec(pTask, 100);
}

int32_t streamBuildSourceRecover2Req(SStreamTask* pTask, SStreamRecoverStep2Req* pReq) {
  pReq->msgHead.vgId = pTask->nodeId;
  pReq->streamId = pTask->id.streamId;
  pReq->taskId = pTask->id.taskId;
  return 0;
}

int32_t streamSourceRecoverScanStep2(SStreamTask* pTask, int64_t ver) {
  void* exec = pTask->exec.pExecutor;
  const char* id = pTask->id.idStr;

  int64_t st = taosGetTimestampMs();
  qDebug("s-task:%s recover step2(blocking stage) started", id);
  if (qStreamSourceRecoverStep2(exec, ver) < 0) {
  }

  int32_t code = streamScanExec(pTask, 100);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  qDebug("s-task:%s recover step2(blocking stage) ended, elapsed time:%.2fs", id,  el);

  return code;
}

int32_t streamDispatchRecoverFinishReq(SStreamTask* pTask) {
  SStreamRecoverFinishReq req = { .streamId = pTask->id.streamId, .childId = pTask->selfChildId };

  // serialize
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    qDebug("s-task:%s send recover finish msg to downstream (fix-dispatch) to taskId:%d, status:%d", pTask->id.idStr,
           pTask->fixedEpDispatcher.taskId, pTask->status.taskStatus);

    req.taskId = pTask->fixedEpDispatcher.taskId;
    streamDispatchOneRecoverFinishReq(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t vgSz = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < vgSz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.taskId = pVgInfo->taskId;
      streamDispatchOneRecoverFinishReq(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  }
  return 0;
}

// agg
int32_t streamAggRecoverPrepare(SStreamTask* pTask) {
  pTask->recoverWaitingUpstream = taosArrayGetSize(pTask->childEpInfo);
  qDebug("s-task:%s wait for %d upstreams", pTask->id.idStr, pTask->recoverWaitingUpstream);
  return 0;
}

int32_t streamAggChildrenRecoverFinish(SStreamTask* pTask) {
  void* exec = pTask->exec.pExecutor;
  if (qStreamRestoreParam(exec) < 0) {
    return -1;
  }
  if (qStreamRecoverFinish(exec) < 0) {
    return -1;
  }
  streamSetStatusNormal(pTask);
  return 0;
}

int32_t streamProcessRecoverFinishReq(SStreamTask* pTask, int32_t childId) {
  if (pTask->taskLevel == TASK_LEVEL__AGG) {
    int32_t left = atomic_sub_fetch_32(&pTask->recoverWaitingUpstream, 1);
    qDebug("s-task:%s remain unfinished child tasks:%d", pTask->id.idStr, left);
    ASSERT(left >= 0);
    if (left == 0) {
      streamAggChildrenRecoverFinish(pTask);
    }
  }
  return 0;
}

int32_t tEncodeSStreamTaskCheckReq(SEncoder* pEncoder, const SStreamTaskCheckReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamTaskCheckReq(SDecoder* pDecoder, SStreamTaskCheckReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSStreamTaskCheckRsp(SEncoder* pEncoder, const SStreamTaskCheckRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->reqId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->childId) < 0) return -1;
  if (tEncodeI8(pEncoder, pRsp->status) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamTaskCheckRsp(SDecoder* pDecoder, SStreamTaskCheckRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->reqId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  if (tDecodeI8(pDecoder, &pRsp->status) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSStreamRecoverFinishReq(SEncoder* pEncoder, const SStreamRecoverFinishReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}
int32_t tDecodeSStreamRecoverFinishReq(SDecoder* pDecoder, SStreamRecoverFinishReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}
