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

static int32_t streamTaskExecImpl(SStreamTask* pTask, const void* data, SArray* pRes) {
  void* exec = pTask->exec.executor;

  // set input
  const SStreamQueueItem* pItem = (const SStreamQueueItem*)data;
  if (pItem->type == STREAM_INPUT__GET_RES) {
    const SStreamTrigger* pTrigger = (const SStreamTrigger*)data;
    qSetMultiStreamInput(exec, pTrigger->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
  } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    ASSERT(pTask->taskLevel == TASK_LEVEL__SOURCE);
    const SStreamDataSubmit* pSubmit = (const SStreamDataSubmit*)data;
    qDebug("task %d %p set submit input %p %p %d 1", pTask->taskId, pTask, pSubmit, pSubmit->data, *pSubmit->dataRef);
    qSetMultiStreamInput(exec, pSubmit->data, 1, STREAM_INPUT__DATA_SUBMIT);
  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    const SStreamDataBlock* pBlock = (const SStreamDataBlock*)data;
    SArray*                 blocks = pBlock->blocks;
    qDebug("task %d %p set ssdata input", pTask->taskId, pTask);
    qSetMultiStreamInput(exec, blocks->pData, blocks->size, STREAM_INPUT__DATA_BLOCK);
  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    const SStreamMergedSubmit* pMerged = (const SStreamMergedSubmit*)data;
    SArray*                    blocks = pMerged->reqs;
    qDebug("task %d %p set submit input (merged), batch num: %d", pTask->taskId, pTask, (int32_t)blocks->size);
    qSetMultiStreamInput(exec, blocks->pData, blocks->size, STREAM_INPUT__MERGED_SUBMIT);
  } else {
    ASSERT(0);
  }

  // exec
  while (1) {
    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if (qExecTask(exec, &output, &ts) < 0) {
      ASSERT(false);
    }
    if (output == NULL) {
      if (pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
        SSDataBlock             block = {0};
        const SStreamDataBlock* pRetrieveBlock = (const SStreamDataBlock*)data;
        ASSERT(taosArrayGetSize(pRetrieveBlock->blocks) == 1);
        assignOneDataBlock(&block, taosArrayGet(pRetrieveBlock->blocks, 0));
        block.info.type = STREAM_PULL_OVER;
        block.info.childId = pTask->selfChildId;
        taosArrayPush(pRes, &block);

        qDebug("task %d(child %d) processed retrieve, reqId %" PRId64, pTask->taskId, pTask->selfChildId,
               pRetrieveBlock->reqId);
      }
      break;
    }

    if (output->info.type == STREAM_RETRIEVE) {
      if (streamBroadcastToChildren(pTask, output) < 0) {
        // TODO
      }
      continue;
    }

    qDebug("task %d(child %d) executed and get block", pTask->taskId, pTask->selfChildId);

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->selfChildId;
    taosArrayPush(pRes, &block);
  }
  return 0;
}

#if 0
static FORCE_INLINE int32_t streamUpdateVer(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK);
  int32_t             childId = pBlock->childId;
  int64_t             ver = pBlock->sourceVer;
  SStreamChildEpInfo* pChildInfo = taosArrayGetP(pTask->childEpInfo, childId);
  /*pChildInfo-> = ver;*/
  return 0;
}
#endif

int32_t streamPipelineExec(SStreamTask* pTask, int32_t batchNum, bool dispatch) {
  ASSERT(pTask->taskLevel != TASK_LEVEL__SINK);

  void* exec = pTask->exec.executor;

  while (1) {
    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    if (pRes == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    int32_t batchCnt = 0;
    while (1) {
      SSDataBlock* output = NULL;
      uint64_t     ts = 0;
      if (qExecTask(exec, &output, &ts) < 0) {
        ASSERT(0);
      }
      if (output == NULL) break;

      SSDataBlock block = {0};
      assignOneDataBlock(&block, output);
      block.info.childId = pTask->selfChildId;
      taosArrayPush(pRes, &block);

      if (++batchCnt >= batchNum) break;
    }
    if (taosArrayGetSize(pRes) == 0) {
      taosArrayDestroy(pRes);
      break;
    }
    if (dispatch) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
      if (qRes == NULL) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        return -1;
      }

      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;
      qRes->childId = pTask->selfChildId;

      if (streamTaskOutput(pTask, qRes) < 0) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        taosFreeQitem(qRes);
        return -1;
      }

      if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
        streamDispatch(pTask);
      }
    }
  }

  return 0;
}
// TODO: handle version
int32_t streamExecForAll(SStreamTask* pTask) {
  while (1) {
    int32_t cnt = 1;
    void*   data = NULL;
    while (1) {
      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
      if (qItem == NULL) {
        qDebug("stream task exec over, queue empty, task: %d", pTask->taskId);
        break;
      }
      if (data == NULL) {
        data = qItem;
        streamQueueProcessSuccess(pTask->inputQueue);
        if (pTask->taskLevel == TASK_LEVEL__SINK) {
          break;
        }
      } else {
        void* newRet;
        if ((newRet = streamAppendQueueItem(data, qItem)) == NULL) {
          streamQueueProcessFail(pTask->inputQueue);
          break;
        } else {
          cnt++;
          data = newRet;
          /*streamUpdateVer(pTask, (SStreamDataBlock*)qItem);*/
          streamQueueProcessSuccess(pTask->inputQueue);
        }
      }
    }

    if (pTask->taskStatus == TASK_STATUS__DROPPING) {
      if (data) streamFreeQitem(data);
      return 0;
    }

    if (data == NULL) {
      break;
    }

    if (pTask->taskLevel == TASK_LEVEL__SINK) {
      ASSERT(((SStreamQueueItem*)data)->type == STREAM_INPUT__DATA_BLOCK);
      streamTaskOutput(pTask, data);
      continue;
    }

    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));

    qDebug("stream task %d exec begin, msg batch: %d", pTask->taskId, cnt);
    streamTaskExecImpl(pTask, data, pRes);
    qDebug("stream task %d exec end", pTask->taskId);

    if (taosArrayGetSize(pRes) != 0) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM);
      if (qRes == NULL) {
        // TODO log failed ver
        streamQueueProcessFail(pTask->inputQueue);
        taosArrayDestroy(pRes);
        streamFreeQitem(data);
        return -1;
      }
      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;

      if (((SStreamQueueItem*)data)->type == STREAM_INPUT__DATA_SUBMIT) {
        SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)data;
        qRes->childId = pTask->selfChildId;
        qRes->sourceVer = pSubmit->ver;
      }

      if (streamTaskOutput(pTask, qRes) < 0) {
        // TODO save failed ver
        /*streamQueueProcessFail(pTask->inputQueue);*/
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        taosFreeQitem(qRes);
        streamFreeQitem(data);
        return -1;
      }
      /*streamQueueProcessSuccess(pTask->inputQueue);*/
    } else {
      taosArrayDestroy(pRes);
    }
    streamFreeQitem(data);
  }
  return 0;
}

int32_t streamTryExec(SStreamTask* pTask) {
  int8_t schedStatus =
      atomic_val_compare_exchange_8(&pTask->schedStatus, TASK_SCHED_STATUS__WAITING, TASK_SCHED_STATUS__ACTIVE);
  if (schedStatus == TASK_SCHED_STATUS__WAITING) {
    int32_t code = streamExecForAll(pTask);
    if (code < 0) {
      atomic_store_8(&pTask->schedStatus, TASK_SCHED_STATUS__FAILED);
      return -1;
    }
    atomic_store_8(&pTask->schedStatus, TASK_SCHED_STATUS__INACTIVE);

    if (!taosQueueEmpty(pTask->inputQueue->queue)) {
      streamSchedExec(pTask);
    }
  }
  return 0;
}
