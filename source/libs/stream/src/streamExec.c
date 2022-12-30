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
  int32_t code;
  void*   exec = pTask->exec.executor;

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
  } else if (pItem->type == STREAM_INPUT__REF_DATA_BLOCK) {
    const SStreamRefDataBlock* pRefBlock = (const SStreamRefDataBlock*)data;
    qSetMultiStreamInput(exec, pRefBlock->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
  } else {
    ASSERT(0);
  }

  // exec
  while (1) {
    if (pTask->taskStatus == TASK_STATUS__DROPPING) {
      return 0;
    }

    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if ((code = qExecTask(exec, &output, &ts)) < 0) {
      /*ASSERT(false);*/
      qError("unexpected stream execution, stream %" PRId64 " task: %d,  since %s", pTask->streamId, pTask->taskId,
             terrstr());
      continue;
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

int32_t streamScanExec(SStreamTask* pTask, int32_t batchSz) {
  ASSERT(pTask->taskLevel == TASK_LEVEL__SOURCE);

  void* exec = pTask->exec.executor;

  qSetStreamOpOpen(exec);
  bool finished = false;

  while (1) {
    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    if (pRes == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    int32_t batchCnt = 0;
    while (1) {
      if (atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING) {
        return 0;
      }

      SSDataBlock* output = NULL;
      uint64_t     ts = 0;
      if (qExecTask(exec, &output, &ts) < 0) {
        return -1;
      }
      if (output == NULL) {
        if (qStreamRecoverScanFinished(exec)) {
          finished = true;
        } else {
          qSetStreamOpOpen(exec);
        }
        break;
      }

      SSDataBlock block = {0};
      assignOneDataBlock(&block, output);
      block.info.childId = pTask->selfChildId;
      taosArrayPush(pRes, &block);

      if (++batchCnt >= batchSz) break;
    }
    if (taosArrayGetSize(pRes) == 0) {
      taosArrayDestroy(pRes);
      break;
    }
    SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
    if (qRes == NULL) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    qRes->type = STREAM_INPUT__DATA_BLOCK;
    qRes->blocks = pRes;
    streamTaskOutput(pTask, qRes);

    if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      streamDispatch(pTask);
    }
    if (finished) break;
  }
  return 0;
}

#if 0
int32_t streamBatchExec(SStreamTask* pTask, int32_t batchLimit) {
  // fetch all queue item, merge according to batchLimit
  int32_t numOfItems = taosReadAllQitems(pTask->inputQueue1, pTask->inputQall);
  if (numOfItems == 0) {
    qDebug("task: %d, stream task exec over, queue empty", pTask->taskId);
    return 0;
  }
  SStreamQueueItem* pMerged = NULL;
  SStreamQueueItem* pItem = NULL;
  taosGetQitem(pTask->inputQall, (void**)&pItem);
  if (pItem == NULL) {
    if (pMerged != NULL) {
      // process merged item
    } else {
      return 0;
    }
  }

  // if drop
  if (pItem->type == STREAM_INPUT__DESTROY) {
    // set status drop
    return -1;
  }

  if (pTask->taskLevel == TASK_LEVEL__SINK) {
    ASSERT(((SStreamQueueItem*)pItem)->type == STREAM_INPUT__DATA_BLOCK);
    streamTaskOutput(pTask, (SStreamDataBlock*)pItem);
  }

  // exec impl

  // output
  // try dispatch
  return 0;
}
#endif

int32_t streamExecForAll(SStreamTask* pTask) {
  while (1) {
    int32_t batchCnt = 1;
    void*   input = NULL;
    while (1) {
      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
      if (qItem == NULL) {
        qDebug("stream task exec over, queue empty, task: %d", pTask->taskId);
        break;
      }
      if (input == NULL) {
        input = qItem;
        streamQueueProcessSuccess(pTask->inputQueue);
        if (pTask->taskLevel == TASK_LEVEL__SINK) {
          break;
        }
      } else {
        void* newRet;
        if ((newRet = streamMergeQueueItem(input, qItem)) == NULL) {
          streamQueueProcessFail(pTask->inputQueue);
          break;
        } else {
          batchCnt++;
          input = newRet;
          streamQueueProcessSuccess(pTask->inputQueue);
        }
      }
    }

    if (pTask->taskStatus == TASK_STATUS__DROPPING) {
      if (input) streamFreeQitem(input);
      return 0;
    }

    if (input == NULL) {
      break;
    }

    if (pTask->taskLevel == TASK_LEVEL__SINK) {
      ASSERT(((SStreamQueueItem*)input)->type == STREAM_INPUT__DATA_BLOCK);
      streamTaskOutput(pTask, input);
      continue;
    }

    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));

    qDebug("stream task %d exec begin, msg batch: %d", pTask->taskId, batchCnt);
    streamTaskExecImpl(pTask, input, pRes);
    qDebug("stream task %d exec end", pTask->taskId);

    if (taosArrayGetSize(pRes) != 0) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
      if (qRes == NULL) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        streamFreeQitem(input);
        return -1;
      }
      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;

      if (((SStreamQueueItem*)input)->type == STREAM_INPUT__DATA_SUBMIT) {
        SStreamDataSubmit* pSubmit = (SStreamDataSubmit*)input;
        qRes->childId = pTask->selfChildId;
        qRes->sourceVer = pSubmit->ver;
      } else if (((SStreamQueueItem*)input)->type == STREAM_INPUT__MERGED_SUBMIT) {
        SStreamMergedSubmit* pMerged = (SStreamMergedSubmit*)input;
        qRes->childId = pTask->selfChildId;
        qRes->sourceVer = pMerged->ver;
      }

      if (streamTaskOutput(pTask, qRes) < 0) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        streamFreeQitem(input);
        taosFreeQitem(qRes);
        return -1;
      }
    } else {
      taosArrayDestroy(pRes);
    }
    streamFreeQitem(input);
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
