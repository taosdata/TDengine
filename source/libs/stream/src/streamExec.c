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

#define MAX_STREAM_EXEC_BATCH_NUM 10240
#define MIN_STREAM_EXEC_BATCH_NUM 16

bool streamTaskShouldStop(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*) &pStatus->taskStatus);
  return (status == TASK_STATUS__STOP) || (status == TASK_STATUS__DROPPING);
}

bool streamTaskShouldPause(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*) &pStatus->taskStatus);
  return (status == TASK_STATUS__PAUSE);
}

static int32_t streamTaskExecImpl(SStreamTask* pTask, const void* data, SArray* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  void*   pExecutor = pTask->exec.pExecutor;

  while (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    int8_t status = atomic_load_8(&pTask->status.taskStatus);
    if (status != TASK_STATUS__NORMAL && status != TASK_STATUS__RESTORE) {
      qError("stream task wait for the end of fill history, s-task:%s, status:%d", pTask->id.idStr,
             atomic_load_8(&pTask->status.taskStatus));
      taosMsleep(2);
    } else {
      break;
    }
  }

  // set input
  const SStreamQueueItem* pItem = (const SStreamQueueItem*)data;
  if (pItem->type == STREAM_INPUT__GET_RES) {
    const SStreamTrigger* pTrigger = (const SStreamTrigger*)data;
    qSetMultiStreamInput(pExecutor, pTrigger->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
  } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    ASSERT(pTask->taskLevel == TASK_LEVEL__SOURCE);
    const SStreamDataSubmit2* pSubmit = (const SStreamDataSubmit2*)data;
    qSetMultiStreamInput(pExecutor, &pSubmit->submit, 1, STREAM_INPUT__DATA_SUBMIT);
    qDebug("s-task:%s set submit blocks as source block completed, %p %p len:%d ver:%" PRId64, pTask->id.idStr, pSubmit, pSubmit->submit.msgStr,
           pSubmit->submit.msgLen, pSubmit->submit.ver);
  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    const SStreamDataBlock* pBlock = (const SStreamDataBlock*)data;

    SArray* pBlockList = pBlock->blocks;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    qDebug("s-task:%s set sdata blocks as input num:%d, ver:%"PRId64, pTask->id.idStr, numOfBlocks, pBlock->sourceVer);
    qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__DATA_BLOCK);
  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    const SStreamMergedSubmit2* pMerged = (const SStreamMergedSubmit2*)data;

    SArray* pBlockList = pMerged->submits;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    qDebug("st-task:%s %p set submit input (merged), batch num:%d", pTask->id.idStr, pTask, numOfBlocks);
    qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__MERGED_SUBMIT);
  } else if (pItem->type == STREAM_INPUT__REF_DATA_BLOCK) {
    const SStreamRefDataBlock* pRefBlock = (const SStreamRefDataBlock*)data;
    qSetMultiStreamInput(pExecutor, pRefBlock->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
  } else {
    ASSERT(0);
  }

  // pExecutor
  while (1) {
    if (streamTaskShouldStop(&pTask->status)) {
      return 0;
    }

    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if ((code = qExecTask(pExecutor, &output, &ts)) < 0) {
      if (code == TSDB_CODE_QRY_IN_EXEC) {
        resetTaskInfo(pExecutor);
      }

      qError("unexpected stream execution, s-task:%s since %s", pTask->id.idStr, terrstr());
      continue;
    }

    if (output == NULL) {
      if (pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
        SSDataBlock block = {0};

        const SStreamDataBlock* pRetrieveBlock = (const SStreamDataBlock*)data;
        ASSERT(taosArrayGetSize(pRetrieveBlock->blocks) == 1);

        assignOneDataBlock(&block, taosArrayGet(pRetrieveBlock->blocks, 0));
        block.info.type = STREAM_PULL_OVER;
        block.info.childId = pTask->selfChildId;
        taosArrayPush(pRes, &block);

        qDebug("task %d(child %d) processed retrieve, reqId %" PRId64, pTask->id.taskId, pTask->selfChildId,
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

    qDebug("s-task:%s (child %d) executed and get block", pTask->id.idStr, pTask->selfChildId);

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->selfChildId;
    taosArrayPush(pRes, &block);
  }

  return 0;
}

int32_t streamScanExec(SStreamTask* pTask, int32_t batchSz) {
  int32_t code = 0;

  ASSERT(pTask->taskLevel == TASK_LEVEL__SOURCE);

  void* exec = pTask->exec.pExecutor;

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
      if (streamTaskShouldStop(&pTask->status) || streamTaskShouldPause(&pTask->status)) {
        taosArrayDestroy(pRes);
        return 0;
      }

      SSDataBlock* output = NULL;
      uint64_t     ts = 0;
      if (qExecTask(exec, &output, &ts) < 0) {
        continue;
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

      batchCnt++;

      qDebug("s-task:%s scan exec block num %d, block limit %d", pTask->id.idStr, batchCnt, batchSz);

      if (batchCnt >= batchSz) {
        break;
      }
    }

    if (taosArrayGetSize(pRes) == 0) {
      if (finished) {
        taosArrayDestroy(pRes);
        qDebug("s-task:%s finish recover exec task ", pTask->id.idStr);
        break;
      } else {
        qDebug("s-task:%s continue recover exec task ", pTask->id.idStr);
        continue;
      }
    }

    SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
    if (qRes == NULL) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    qRes->type = STREAM_INPUT__DATA_BLOCK;
    qRes->blocks = pRes;
    code = streamTaskOutput(pTask, qRes);
    if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY) {
      taosFreeQitem(pRes);
      return code;
    }

    if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      qDebug("task %d scan exec dispatch block num %d", pTask->id.taskId, batchCnt);
      streamDispatch(pTask);
    }

    if (finished) {
      break;
    }
  }
  return 0;
}

#if 0
int32_t streamBatchExec(SStreamTask* pTask, int32_t batchLimit) {
  // fetch all queue item, merge according to batchLimit
  int32_t numOfItems = taosReadAllQitems(pTask->inputQueue1, pTask->inputQall);
  if (numOfItems == 0) {
    qDebug("task: %d, stream task exec over, queue empty", pTask->id.taskId);
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
  int32_t code = 0;
  while (1) {
    int32_t batchSize = 1;
    void*   pInput = NULL;
    int16_t times = 0;

    // merge multiple input data if possible in the input queue.
    while (1) {
      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
      if (qItem == NULL) {
        if (pTask->taskLevel == TASK_LEVEL__SOURCE && batchSize < MIN_STREAM_EXEC_BATCH_NUM && times < 5) {
          times++;
          taosMsleep(1);
          qDebug("===stream===try agian batchSize:%d", batchSize);
          continue;
        }
        break;
      }

      if (pInput == NULL) {
        pInput = qItem;
        streamQueueProcessSuccess(pTask->inputQueue);
        if (pTask->taskLevel == TASK_LEVEL__SINK) {
          break;
        }
      } else {
        void* newRet = NULL;
        if ((newRet = streamMergeQueueItem(pInput, qItem)) == NULL) {
          streamQueueProcessFail(pTask->inputQueue);
          break;
        } else {
          batchSize++;
          pInput = newRet;
          streamQueueProcessSuccess(pTask->inputQueue);
          if (batchSize > MAX_STREAM_EXEC_BATCH_NUM) {
            break;
          }
        }
      }
    }

    if (streamTaskShouldStop(&pTask->status)) {
      if (pInput) {
        streamFreeQitem(pInput);
      }
      return 0;
    }

    if (pInput == NULL) {
      break;
    }

    if (pTask->taskLevel == TASK_LEVEL__SINK) {
      ASSERT(((SStreamQueueItem*)pInput)->type == STREAM_INPUT__DATA_BLOCK);
      qDebug("s-task:%s sink node start to sink result. numOfBlocks:%d", pTask->id.idStr, batchSize);
      streamTaskOutput(pTask, pInput);
      continue;
    }

    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    qDebug("s-task:%s exec begin, numOfBlocks:%d", pTask->id.idStr, batchSize);

    streamTaskExecImpl(pTask, pInput, pRes);

    int64_t ckId = 0;
    int64_t dataVer = 0;
    qGetCheckpointVersion(pTask->exec.pExecutor, &dataVer, &ckId);
    if (ckId > pTask->chkInfo.id) {    // save it since the checkpoint is updated
      qDebug("s-task:%s exec end, start to update check point, ver from %" PRId64 " to %" PRId64
             ", checkPoint id:%" PRId64 " -> %" PRId64,
             pTask->id.idStr, pTask->chkInfo.version, dataVer, pTask->chkInfo.id, ckId);

      pTask->chkInfo = (SCheckpointInfo) {.version = dataVer, .id = ckId, .currentVer = pTask->chkInfo.currentVer};

      taosWLockLatch(&pTask->pMeta->lock);
      streamMetaSaveTask(pTask->pMeta, pTask);
      if (streamMetaCommit(pTask->pMeta) < 0) {
        taosWUnLockLatch(&pTask->pMeta->lock);
        qError("s-task:%s failed to commit stream meta, since %s", pTask->id.idStr, terrstr());
        return -1;
      } else {
        taosWUnLockLatch(&pTask->pMeta->lock);
        qDebug("s-task:%s update checkpoint ver succeed", pTask->id.idStr);
      }
    } else {
      qDebug("s-task:%s exec end", pTask->id.idStr);
    }

    if (taosArrayGetSize(pRes) != 0) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
      if (qRes == NULL) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        streamFreeQitem(pInput);
        return -1;
      }

      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;

      if (((SStreamQueueItem*)pInput)->type == STREAM_INPUT__DATA_SUBMIT) {
        SStreamDataSubmit2* pSubmit = (SStreamDataSubmit2*)pInput;
        qRes->childId = pTask->selfChildId;
        qRes->sourceVer = pSubmit->ver;
      } else if (((SStreamQueueItem*)pInput)->type == STREAM_INPUT__MERGED_SUBMIT) {
        SStreamMergedSubmit2* pMerged = (SStreamMergedSubmit2*)pInput;
        qRes->childId = pTask->selfChildId;
        qRes->sourceVer = pMerged->ver;
      }

      code = streamTaskOutput(pTask, qRes);
      if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY) {
        // backpressure and record position
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        streamFreeQitem(pInput);
        taosFreeQitem(qRes);
        return -1;
      }
    } else {
      taosArrayDestroy(pRes);
    }
    streamFreeQitem(pInput);
  }
  return 0;
}

int32_t streamTryExec(SStreamTask* pTask) {
  // this function may be executed by multi-threads, so status check is required.
  int8_t schedStatus =
      atomic_val_compare_exchange_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__WAITING, TASK_SCHED_STATUS__ACTIVE);

  if (schedStatus == TASK_SCHED_STATUS__WAITING) {
    int32_t code = streamExecForAll(pTask);
    if (code < 0) {
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__FAILED);
      return -1;
    }

    // todo the task should be commit here
    atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
    qDebug("s-task:%s exec completed", pTask->id.idStr);

    if (!taosQueueEmpty(pTask->inputQueue->queue) && (!streamTaskShouldStop(&pTask->status))) {
      streamSchedExec(pTask);
    }
  }

  return 0;
}
