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

#include "streamInt.h"

int32_t tEncodeStreamCheckpointSourceReq(SEncoder* pEncoder, const SStreamCheckpointSourceReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->nodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->mnodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->expireTime) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointSourceReq(SDecoder* pDecoder, SStreamCheckpointSourceReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->nodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->mnodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->expireTime) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->nodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->expireTime) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointSourceRsp(SDecoder* pDecoder, SStreamCheckpointSourceRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->nodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->expireTime) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointReq(SEncoder* pEncoder, const SStreamCheckpointReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointReq(SDecoder* pDecoder, SStreamCheckpointReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointRsp(SEncoder* pEncoder, const SStreamCheckpointRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->downstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamCheckpointRsp(SDecoder* pDecoder, SStreamCheckpointRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->childId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

static int32_t streamAlignCheckpoint(SStreamTask* pTask, int64_t checkpointId, int32_t childId) {
  int32_t num = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  int64_t old = atomic_val_compare_exchange_32(&pTask->checkpointAlignCnt, 0, num);
  if (old == 0) {
    qDebug("s-task:%s set initial align upstream num:%d", pTask->id.idStr, num);
    pTask->checkpointingId = checkpointId;
  }

  ASSERT(pTask->checkpointingId == checkpointId);
  return atomic_sub_fetch_32(&pTask->checkpointAlignCnt, 1);
}

static int32_t streamTaskDispatchCheckpointMsg(SStreamTask* pTask, uint64_t checkpointId) {
  SStreamCheckpointReq req = {
      .streamId = pTask->id.streamId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->info.nodeId,
      .downstreamNodeId = pTask->info.nodeId,
      .downstreamTaskId = pTask->id.taskId,
      .childId = pTask->info.selfChildId,
      .checkpointId = checkpointId,
  };

  // serialize
  if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH) {
    req.downstreamNodeId = pTask->fixedEpDispatcher.nodeId;
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    streamDispatchCheckpointMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;

    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->notReadyTasks = numOfVgs;
    pTask->checkReqIds = taosArrayInit(numOfVgs, sizeof(int64_t));

    qDebug("s-task:%s dispatch %d checkpoint msg to downstream", pTask->id.idStr, numOfVgs);

    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.downstreamNodeId = pVgInfo->vgId;
      req.downstreamTaskId = pVgInfo->taskId;
      streamDispatchCheckpointMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {  // no need to dispatch msg to downstream task
    qDebug("s-task:%s no down stream task, not dispatch checkpoint msg to downstream", pTask->id.idStr);
    streamProcessCheckpointRsp(NULL, pTask);
  }

  return 0;
}

static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask) {
  SStreamCheckpoint* pChkpoint = taosAllocateQitem(sizeof(SStreamCheckpoint), DEF_QITEM, sizeof(SSDataBlock));
  if (pChkpoint == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChkpoint->type = STREAM_INPUT__CHECKPOINT;
  pChkpoint->pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pChkpoint->pBlock == NULL) {
    taosFreeQitem(pChkpoint);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChkpoint->pBlock->info.type = STREAM_CHECKPOINT;
  if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pChkpoint) < 0) {
    taosFreeQitem(pChkpoint);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  streamSchedExec(pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamProcessCheckpointSourceReq(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  int32_t code = 0;
  int64_t checkpointId = pReq->checkpointId;

  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  // 1. set task status to be prepared for check point, no data are allowed to put into inputQ.
  pTask->status.taskStatus = TASK_STATUS__CK;
  pTask->checkpointNotReadyTasks = 1;
  pTask->checkpointingId = pReq->checkpointId;

  // 2. let's dispatch checkpoint msg to downstream task directly and do nothing else.
  streamTaskDispatchCheckpointMsg(pTask, checkpointId);
  return code;
}

int32_t streamProcessCheckpointReq(SStreamTask* pTask, SStreamCheckpointReq* pReq) {
  int32_t code;
  int64_t checkpointId = pReq->checkpointId;
  int32_t childId = pReq->childId;

  // set the task status
  pTask->status.taskStatus = TASK_STATUS__CK;
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG || pTask->info.taskLevel == TASK_LEVEL__SINK);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    appendCheckpointIntoInputQ(pTask);
    streamSchedExec(pTask);
    qDebug("s-task:%s sink task set to checkpoint ready, start to send rsp to upstream", pTask->id.idStr);
  } else {
    // todo close the inputQ for data from childId, which means data from childId are not allowed to put into intpuQ
    // anymore
    ASSERT(taosArrayGetSize(pTask->pUpstreamEpInfoList) > 0);

    // there are still some upstream tasks not send checkpoint request
    int32_t notReady = streamAlignCheckpoint(pTask, checkpointId, childId);
    if (notReady > 0) {
      int32_t num = taosArrayGetSize(pTask->pUpstreamEpInfoList);
      qDebug("s-task:%s received checkpoint req, %d upstream tasks not send checkpoint info yet, total:%d",
             pTask->id.idStr, notReady, num);
      return 0;
    }

    qDebug("s-task:%s received checkpoint req, all upstream sent checkpoint msg, dispatch checkpoint msg to downstream",
           pTask->id.idStr);
    pTask->checkpointNotReadyTasks = (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH)
                                         ? 1
                                         : taosArrayGetSize(pTask->shuffleDispatcher.dbInfo.pVgroupInfos);

    // if all upstreams are ready for generating checkpoint, set the status to be TASK_STATUS__CK_READY
    // 2. dispatch check point msg to all downstream tasks
    streamTaskDispatchCheckpointMsg(pTask, checkpointId);
  }

  return 0;
}

/**
 * All down stream tasks have successfully completed the check point task.
 * Current stream task is allowed to start to do checkpoint things in ASYNC model.
 */
int32_t streamProcessCheckpointRsp(SStreamMeta* pMeta, SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG);

  // only when all downstream tasks are send checkpoint rsp, we can start the checkpoint procedure for the agg task
  int32_t notReady = atomic_sub_fetch_32(&pTask->checkpointNotReadyTasks, 1);
  if (notReady == 0) {
    qDebug("s-task:%s all downstream tasks have completed the checkpoint, start to do checkpoint for current task",
           pTask->id.idStr);
    appendCheckpointIntoInputQ(pTask);
    streamSchedExec(pTask);
  } else {
    qDebug("s-task:%s %d downstream tasks are not ready, wait", pTask->id.idStr, notReady);
  }

  return 0;
}
