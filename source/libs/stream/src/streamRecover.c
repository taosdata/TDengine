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
  qDebug("task %d at node %d launch recover", pTask->taskId, pTask->nodeId);
  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__RECOVER_PREPARE);
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

    SRpcMsg rpcMsg = {
        .contLen = len,
        .pCont = serializedReq,
        .msgType = TDMT_VND_STREAM_RECOVER_NONBLOCKING_STAGE,
    };

    if (tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &rpcMsg) < 0) {
      /*ASSERT(0);*/
    }

  } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__NORMAL);
    streamSetParamForRecover(pTask);
    streamAggRecoverPrepare(pTask);
  } else if (pTask->taskLevel == TASK_LEVEL__SINK) {
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__NORMAL);
  }
  return 0;
}

// checkstatus
int32_t streamTaskCheckDownstream(SStreamTask* pTask, int64_t version) {
  SStreamTaskCheckReq req = {
      .streamId = pTask->streamId,
      .upstreamTaskId = pTask->taskId,
      .upstreamNodeId = pTask->nodeId,
      .childId = pTask->selfChildId,
  };
  // serialize
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    req.reqId = tGenIdPI64();
    req.downstreamNodeId = pTask->fixedEpDispatcher.nodeId;
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->checkReqId = req.reqId;

    qDebug("task %d at node %d check downstream task %d at node %d", pTask->taskId, pTask->nodeId, req.downstreamTaskId,
           req.downstreamNodeId);
    streamDispatchOneCheckReq(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t vgSz = taosArrayGetSize(vgInfo);
    pTask->recoverTryingDownstream = vgSz;
    pTask->checkReqIds = taosArrayInit(vgSz, sizeof(int64_t));

    for (int32_t i = 0; i < vgSz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.reqId = tGenIdPI64();
      taosArrayPush(pTask->checkReqIds, &req.reqId);
      req.downstreamNodeId = pVgInfo->vgId;
      req.downstreamTaskId = pVgInfo->taskId;
      qDebug("task %d at node %d check downstream task %d at node %d (shuffle)", pTask->taskId, pTask->nodeId,
             req.downstreamTaskId, req.downstreamNodeId);
      streamDispatchOneCheckReq(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("task %d at node %d direct launch recover since no downstream", pTask->taskId, pTask->nodeId);
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
  qDebug("task %d at node %d check downstream task %d at node %d (recheck)", pTask->taskId, pTask->nodeId,
         req.downstreamTaskId, req.downstreamNodeId);
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    streamDispatchOneCheckReq(pTask, &req, pRsp->downstreamNodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t vgSz = taosArrayGetSize(vgInfo);
    for (int32_t i = 0; i < vgSz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo->taskId == req.downstreamTaskId) {
        streamDispatchOneCheckReq(pTask, &req, pRsp->downstreamNodeId, &pVgInfo->epSet);
      }
    }
  }
  return 0;
}

int32_t streamProcessTaskCheckReq(SStreamTask* pTask, const SStreamTaskCheckReq* pReq) {
  return atomic_load_8(&pTask->taskStatus) == TASK_STATUS__NORMAL;
}

int32_t streamProcessTaskCheckRsp(SStreamTask* pTask, const SStreamTaskCheckRsp* pRsp, int64_t version) {
  qDebug("task %d at node %d recv check rsp from task %d at node %d: status %d", pRsp->upstreamTaskId,
         pRsp->upstreamNodeId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->status);
  if (pRsp->status == 1) {
    if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      bool found = false;
      for (int32_t i = 0; i < taosArrayGetSize(pTask->checkReqIds); i++) {
        int64_t reqId = *(int64_t*)taosArrayGet(pTask->checkReqIds, i);
        if (reqId == pRsp->reqId) {
          found = true;
          break;
        }
      }
      if (!found) return -1;
      int32_t left = atomic_sub_fetch_32(&pTask->recoverTryingDownstream, 1);
      ASSERT(left >= 0);
      if (left == 0) {
        taosArrayDestroy(pTask->checkReqIds);
        pTask->checkReqIds = NULL;
        streamTaskLaunchRecover(pTask, version);
      }
    } else if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
      if (pRsp->reqId != pTask->checkReqId) return -1;
      streamTaskLaunchRecover(pTask, version);
    } else {
      ASSERT(0);
    }
  } else {
    streamRecheckOneDownstream(pTask, pRsp);
  }
  return 0;
}

// common
int32_t streamSetParamForRecover(SStreamTask* pTask) {
  void* exec = pTask->exec.executor;
  return qStreamSetParamForRecover(exec);
}
int32_t streamRestoreParam(SStreamTask* pTask) {
  void* exec = pTask->exec.executor;
  return qStreamRestoreParam(exec);
}
int32_t streamSetStatusNormal(SStreamTask* pTask) {
  pTask->taskStatus = TASK_STATUS__NORMAL;
  return 0;
}

// source
int32_t streamSourceRecoverPrepareStep1(SStreamTask* pTask, int64_t ver) {
  void* exec = pTask->exec.executor;
  return qStreamSourceRecoverStep1(exec, ver);
}

int32_t streamBuildSourceRecover1Req(SStreamTask* pTask, SStreamRecoverStep1Req* pReq) {
  pReq->msgHead.vgId = pTask->nodeId;
  pReq->streamId = pTask->streamId;
  pReq->taskId = pTask->taskId;
  return 0;
}

int32_t streamSourceRecoverScanStep1(SStreamTask* pTask) {
  //
  return streamScanExec(pTask, 100);
}

int32_t streamBuildSourceRecover2Req(SStreamTask* pTask, SStreamRecoverStep2Req* pReq) {
  pReq->msgHead.vgId = pTask->nodeId;
  pReq->streamId = pTask->streamId;
  pReq->taskId = pTask->taskId;
  return 0;
}

int32_t streamSourceRecoverScanStep2(SStreamTask* pTask, int64_t ver) {
  void* exec = pTask->exec.executor;
  if (qStreamSourceRecoverStep2(exec, ver) < 0) {
  }
  return streamScanExec(pTask, 100);
}

int32_t streamDispatchRecoverFinishReq(SStreamTask* pTask) {
  SStreamRecoverFinishReq req = {
      .streamId = pTask->streamId,
      .childId = pTask->selfChildId,
  };
  // serialize
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
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
  void* exec = pTask->exec.executor;
  pTask->recoverWaitingUpstream = taosArrayGetSize(pTask->childEpInfo);
  return 0;
}

int32_t streamAggChildrenRecoverFinish(SStreamTask* pTask) {
  void* exec = pTask->exec.executor;
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
