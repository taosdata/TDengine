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
  if (tDecodeI64(pDecoder, &pReq->expireTime) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeSStreamCheckpointSourceRsp(SEncoder* pEncoder, const SStreamCheckpointSourceRsp* pRsp) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pRsp->nodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->expireTime) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamCheckpointSourceRsp(SDecoder* pDecoder, SStreamCheckpointSourceRsp* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->checkpointId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->nodeId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->expireTime) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamTaskCheckpointReq(SEncoder* pEncoder, const SStreamTaskCheckpointReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->checkpointId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNodeId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->childId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskCheckpointReq(SDecoder* pDecoder, SStreamTaskCheckpointReq* pReq) {
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
  if (tEncodeI64(pEncoder, pRsp->upstreamTaskId) < 0) return -1;
  if (tEncodeI64(pEncoder, pRsp->upstreamNodeId) < 0) return -1;
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
  if (pTask->checkpointingId == 0) {
    pTask->checkpointingId = checkpointId;
    pTask->checkpointAlignCnt = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  }

  ASSERT(pTask->checkpointingId == checkpointId);

  return atomic_sub_fetch_32(&pTask->checkpointAlignCnt, 1);
}

static int32_t streamTaskDispatchCheckpointMsg(SStreamTask* pTask, uint64_t checkpointId) {
  SStreamTaskCheckpointReq req = {
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

    qDebug("s-task:%s dispatch checkpoint msg to task:0x%x(vgId:%d)", pTask->id.idStr, req.downstreamTaskId,
           req.downstreamNodeId);

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
      qDebug("s-task:%s (vgId:%d) checkpoint to task:0x%x (vgId:%d) (shuffle), idx:%d", pTask->id.idStr,
             pTask->info.nodeId, req.downstreamTaskId, req.downstreamNodeId, i);
      streamDispatchCheckpointMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s (vgId:%d) sink task set to be ready for checkpointing", pTask->id.idStr, pTask->info.nodeId);
    ASSERT(pTask->info.taskLevel == TASK_LEVEL__SINK);
    streamTaskLaunchScanHistory(pTask);
  }

  return 0;
}

// set status check pointing
// do checkpoint
static int32_t streamDoSourceCheckpoint(SStreamMeta* pMeta, SStreamTask* pTask, uint64_t checkpointId) {
  int  code = 0;
  char buf[256] = {0};

  int64_t ts = taosGetTimestampMs();

  sprintf(buf, "%s/%s", pMeta->path, "checkpoints");
  code = taosMulModeMkDir(buf, 0755);
  if (code != 0) {
    qError("failed to prepare checkpoint %s, checkpointId:%" PRIu64 ", reason:%s", buf, checkpointId, tstrerror(code));
    return code;
  }

  pMeta->checkpointTs = ts;
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  // 1. set task status to be prepared for check point
  pTask->status.taskStatus = TASK_STATUS__CK;

  // 2. put the checkpoint data block into the inputQ, to enable the local status to be flushed to storage backend
  {
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
  }

  // 2. dispatch checkpoint msg to downstream task
  streamTaskDispatchCheckpointMsg(pTask, checkpointId);

//  code = streamBackendDoCheckpoint((void*)pMeta, buf);
  return code;
}

int32_t streamProcessCheckpointSourceReq(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  int32_t code = 0;
  int64_t checkpointId = pReq->checkpointId;

  code = streamDoSourceCheckpoint(pMeta, pTask, checkpointId);
  if (code < 0) {
    // rsp error
    return -1;
  }

  return 0;
}

int32_t streamProcessCheckpointReq(SStreamMeta* pMeta, SStreamTask* pTask, SStreamTaskCheckpointReq* pReq) {
  int32_t code;
  int64_t checkpointId = pReq->checkpointId;
  int32_t childId = pReq->childId;

  if (taosArrayGetSize(pTask->pUpstreamEpInfoList) > 0) {
    code = streamAlignCheckpoint(pTask, checkpointId, childId);
    if (code > 0) {
      return 0;
    }
    if (code < 0) {
      ASSERT(0);
      return -1;
    }
  }

  // code = streamDoCheckpoint(pMeta, pTask, checkpointId);
  if (code < 0) {
    // rsp error
    return -1;
  }

  // send rsp to all children

  return 0;
}

int32_t streamProcessCheckpointRsp(SStreamMeta* pMeta, SStreamTask* pTask, SStreamCheckpointRsp* pRsp) {
  // recover step2, scan from wal
  // unref wal
  // set status normal
  return 0;
}
