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
  if (tEncodeSEpSet(pEncoder, &pReq->mgmtEps) < 0) return -1;
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
  if (tDecodeSEpSet(pDecoder, &pReq->mgmtEps) < 0) return -1;
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
  if (tEncodeI8(pEncoder, pRsp->success) < 0) return -1;
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
  if (tDecodeI8(pDecoder, &pRsp->success) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t tEncodeStreamCheckpointReadyMsg(SEncoder* pEncoder, const SStreamCheckpointReadyMsg* pReq) {
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

int32_t tDecodeStreamCheckpointReadyMsg(SDecoder* pDecoder, SStreamCheckpointReadyMsg* pRsp) {
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

static int32_t streamAlignCheckpoint(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
  int64_t old = atomic_val_compare_exchange_32(&pTask->checkpointAlignCnt, 0, num);
  if (old == 0) {
    stDebug("s-task:%s set initial align upstream num:%d", pTask->id.idStr, num);
  }

  return atomic_sub_fetch_32(&pTask->checkpointAlignCnt, 1);
}

static int32_t appendCheckpointIntoInputQ(SStreamTask* pTask, int32_t checkpointType) {
  SStreamDataBlock* pChkpoint = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, sizeof(SSDataBlock));
  if (pChkpoint == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pChkpoint->type = checkpointType;

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    taosFreeQitem(pChkpoint);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBlock->info.type = STREAM_CHECKPOINT;
  pBlock->info.version = pTask->checkpointingId;
  pBlock->info.rows = 1;
  pBlock->info.childId = pTask->info.selfChildId;

  pChkpoint->blocks = taosArrayInit(4, sizeof(SSDataBlock));//pBlock;
  taosArrayPush(pChkpoint->blocks, pBlock);

  taosMemoryFree(pBlock);
  if (streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pChkpoint) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  streamSchedExec(pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamProcessCheckpointSourceReq(SStreamTask* pTask, SStreamCheckpointSourceReq* pReq) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  // todo this status may not be set here.
  // 1. set task status to be prepared for check point, no data are allowed to put into inputQ.
  streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);

  pTask->checkpointingId = pReq->checkpointId;
  pTask->checkpointNotReadyTasks = streamTaskGetNumOfDownstream(pTask);
  pTask->chkInfo.startTs = taosGetTimestampMs();
  pTask->execInfo.checkpoint += 1;

  // 2. Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
  //    already.
  int32_t code = appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT_TRIGGER);
  return code;
}

static int32_t continueDispatchCheckpointBlock(SStreamDataBlock* pBlock, SStreamTask* pTask) {
  pBlock->srcTaskId = pTask->id.taskId;
  pBlock->srcVgId = pTask->pMeta->vgId;

  int32_t code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
  if (code == 0) {
    streamDispatchStreamBlock(pTask);
  } else {
    stError("s-task:%s failed to put checkpoint into outputQ, code:%s", pTask->id.idStr, tstrerror(code));
    streamFreeQitem((SStreamQueueItem*)pBlock);
  }

  return code;
}

int32_t streamProcessCheckpointBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  SSDataBlock* pDataBlock = taosArrayGet(pBlock->blocks, 0);
  int64_t checkpointId = pDataBlock->info.version;

  const char* id = pTask->id.idStr;
  int32_t code = TSDB_CODE_SUCCESS;

  // set task status
  if (streamTaskGetStatus(pTask, NULL) != TASK_STATUS__CK) {
    pTask->checkpointingId = checkpointId;
    streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_GEN_CHECKPOINT);
  }

  { // todo: remove this when the pipeline checkpoint generating is used.
    SStreamMeta* pMeta = pTask->pMeta;
    taosWLockLatch(&pMeta->lock);

    if (pMeta->chkptNotReadyTasks == 0) {
      pMeta->chkptNotReadyTasks = pMeta->numOfStreamTasks;
    }

    taosWUnLockLatch(&pMeta->lock);
  }

  //todo fix race condition: set the status and append checkpoint block
  int32_t taskLevel = pTask->info.taskLevel;
  if (taskLevel == TASK_LEVEL__SOURCE) {
    if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s set childIdx:%d, and add checkpoint-trigger block into outputQ", id, pTask->info.selfChildId);
      continueDispatchCheckpointBlock(pBlock, pTask);
    } else {  // only one task exists, no need to dispatch downstream info
      atomic_add_fetch_32(&pTask->checkpointNotReadyTasks, 1);
      streamProcessCheckpointReadyMsg(pTask);
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else if (taskLevel == TASK_LEVEL__SINK || taskLevel == TASK_LEVEL__AGG) {
    ASSERT(taosArrayGetSize(pTask->upstreamInfo.pList) > 0);
    if (pTask->chkInfo.startTs == 0) {
      pTask->chkInfo.startTs = taosGetTimestampMs();
      pTask->execInfo.checkpoint += 1;
    }

    // update the child Id for downstream tasks
    streamAddCheckpointReadyMsg(pTask, pBlock->srcTaskId, pTask->info.selfChildId, checkpointId);

    // there are still some upstream tasks not send checkpoint request, do nothing and wait for then
    int32_t notReady = streamAlignCheckpoint(pTask);
    int32_t num = taosArrayGetSize(pTask->upstreamInfo.pList);
    if (notReady > 0) {
      stDebug("s-task:%s received checkpoint block, idx:%d, %d upstream tasks not send checkpoint info yet, total:%d",
             id, pTask->info.selfChildId, notReady, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      return code;
    }

    if (taskLevel == TASK_LEVEL__SINK) {
      stDebug("s-task:%s process checkpoint block, all %d upstreams sent checkpoint msgs, send ready msg to upstream",
             id, num);
      streamFreeQitem((SStreamQueueItem*)pBlock);
      streamTaskBuildCheckpoint(pTask);
    } else {
      stDebug(
          "s-task:%s process checkpoint block, all %d upstreams sent checkpoint msgs, dispatch checkpoint msg "
          "downstream", id, num);

      // set the needed checked downstream tasks, only when all downstream tasks do checkpoint complete, this task
      // can start local checkpoint procedure
      pTask->checkpointNotReadyTasks = streamTaskGetNumOfDownstream(pTask);

      // Put the checkpoint block into inputQ, to make sure all blocks with less version have been handled by this task
      // already. And then, dispatch check point msg to all downstream tasks
      code = continueDispatchCheckpointBlock(pBlock, pTask);
    }
  }

  return code;
}

/**
 * All down stream tasks have successfully completed the check point task.
 * Current stream task is allowed to start to do checkpoint things in ASYNC model.
 */
int32_t streamProcessCheckpointReadyMsg(SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE || pTask->info.taskLevel == TASK_LEVEL__AGG);

  // only when all downstream tasks are send checkpoint rsp, we can start the checkpoint procedure for the agg task
  int32_t notReady = atomic_sub_fetch_32(&pTask->checkpointNotReadyTasks, 1);
  ASSERT(notReady >= 0);

  if (notReady == 0) {
    stDebug("s-task:%s all downstream tasks have completed the checkpoint, start to do checkpoint for current task",
           pTask->id.idStr);
    appendCheckpointIntoInputQ(pTask, STREAM_INPUT__CHECKPOINT);
  } else {
    int32_t total = streamTaskGetNumOfDownstream(pTask);
    stDebug("s-task:%s %d/%d downstream tasks are not ready, wait", pTask->id.idStr, notReady, total);
  }

  return 0;
}

void streamTaskClearCheckInfo(SStreamTask* pTask) {
  pTask->checkpointingId = 0; // clear the checkpoint id
  pTask->chkInfo.failedId = 0;
  pTask->chkInfo.startTs = 0;  // clear the recorded start time
  pTask->checkpointNotReadyTasks = 0;
  pTask->checkpointAlignCnt = 0;
  streamTaskOpenAllUpstreamInput(pTask);   // open inputQ for all upstream tasks
}

int32_t streamSaveAllTaskStatus(SStreamMeta* pMeta, int64_t checkpointId) {
  int32_t vgId = pMeta->vgId;

  taosWLockLatch(&pMeta->lock);

  for (int32_t i = 0; i < taosArrayGetSize(pMeta->pTaskList); ++i) {
    STaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    SStreamTask** ppTask = taosHashGet(pMeta->pTasksMap, pId, sizeof(*pId));
    if (ppTask == NULL) {
      continue;
    }

    SStreamTask* p = *ppTask;
    if (p->info.fillHistory == 1) {
      continue;
    }

    ASSERT(p->chkInfo.checkpointId < p->checkpointingId && p->checkpointingId == checkpointId);

    p->chkInfo.checkpointId = p->checkpointingId;
    streamTaskClearCheckInfo(p);

    char* str = NULL;
    streamTaskGetStatus(p, &str);

    int32_t code = streamTaskHandleEvent(p->status.pSM, TASK_EVENT_CHECKPOINT_DONE);
    if (code != TSDB_CODE_SUCCESS) {
      stDebug("s-task:%s vgId:%d save task status failed, since handle event failed", p->id.idStr, vgId);
      taosWUnLockLatch(&pMeta->lock);
      return -1;
    } else { // save the task
      streamMetaSaveTask(pMeta, p);
    }

    stDebug(
        "vgId:%d s-task:%s level:%d open upstream inputQ, commit task status after checkpoint completed, "
        "checkpointId:%" PRId64 ", Ver(saved):%" PRId64 " currentVer:%" PRId64 ", status to be normal, prev:%s",
        pMeta->vgId, p->id.idStr, p->info.taskLevel, checkpointId, p->chkInfo.checkpointVer, p->chkInfo.nextProcessVer,
        str);
  }

  if (streamMetaCommit(pMeta) < 0) {
    taosWUnLockLatch(&pMeta->lock);
    stError("vgId:%d failed to commit stream meta after do checkpoint, checkpointId:%" PRId64 ", since %s", pMeta->vgId,
            checkpointId, terrstr());
    return -1;
  } else {
    taosWUnLockLatch(&pMeta->lock);
    stInfo("vgId:%d commit stream meta after do checkpoint, checkpointId:%" PRId64 " DONE", pMeta->vgId, checkpointId);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamTaskBuildCheckpoint(SStreamTask* pTask) {
  int32_t code = 0;

  // check for all tasks, and do generate the vnode-wide checkpoint data.
  SStreamMeta* pMeta = pTask->pMeta;
  int32_t      remain = atomic_sub_fetch_32(&pMeta->chkptNotReadyTasks, 1);
  ASSERT(remain >= 0);

  double el = (taosGetTimestampMs() - pTask->chkInfo.startTs) / 1000.0;
  if (remain == 0) {  // all tasks are ready
    stDebug("s-task:%s all downstreams are ready, ready for do checkpoint", pTask->id.idStr);
    streamBackendDoCheckpoint(pMeta, pTask->checkpointingId);
    streamSaveAllTaskStatus(pMeta, pTask->checkpointingId);
    stInfo(
        "vgId:%d vnode wide checkpoint completed, save all tasks status, last:%s, level:%d elapsed time:%.2f Sec "
        "checkpointId:%" PRId64,
        pMeta->vgId, pTask->id.idStr, pTask->info.taskLevel, el, pTask->checkpointingId);
  } else {
    stInfo(
        "vgId:%d vnode wide tasks not reach checkpoint ready status, ready s-task:%s, level:%d elapsed time:%.2f Sec "
        "not ready:%d/%d",
        pMeta->vgId, pTask->id.idStr, pTask->info.taskLevel, el, remain, pMeta->numOfStreamTasks);
  }

  // send check point response to upstream task
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    code = streamTaskSendCheckpointSourceRsp(pTask);
  } else {
    code = streamTaskSendCheckpointReadyMsg(pTask);
  }

  if (code != TSDB_CODE_SUCCESS) {
    // todo: let's retry send rsp to upstream/mnode
    stError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%" PRId64 ", code:%s", pTask->id.idStr,
           pTask->checkpointingId, tstrerror(code));
  }

  return code;
}
