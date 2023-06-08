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

// maximum allowed processed block batches. One block may include several submit blocks
#define MAX_STREAM_EXEC_BATCH_NUM 32
#define MIN_STREAM_EXEC_BATCH_NUM 4
#define MAX_STREAM_RESULT_DUMP_THRESHOLD  100

static int32_t updateCheckPointInfo(SStreamTask* pTask);

bool streamTaskShouldStop(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*)&pStatus->taskStatus);
  return (status == TASK_STATUS__STOP) || (status == TASK_STATUS__DROPPING);
}

bool streamTaskShouldPause(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*)&pStatus->taskStatus);
  return (status == TASK_STATUS__PAUSE);
}

static int32_t doDumpResult(SStreamTask* pTask, SStreamQueueItem* pItem, SArray* pRes, int32_t size, int64_t* totalSize,
                            int32_t* totalBlocks) {
  int32_t code = updateCheckPointInfo(pTask);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
    return code;
  }

  int32_t numOfBlocks = taosArrayGetSize(pRes);
  if (numOfBlocks > 0) {
    SStreamDataBlock* pStreamBlocks = createStreamBlockFromResults(pItem, pTask, size, pRes);
    if (pStreamBlocks == NULL) {
      qError("s-task:%s failed to create result stream data block, code:%s", pTask->id.idStr, tstrerror(terrno));
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      return -1;
    }

    qDebug("s-task:%s dump stream result data blocks, num:%d, size:%.2fMiB", pTask->id.idStr, numOfBlocks,
           size / 1048576.0);

    code = streamTaskOutputResultBlock(pTask, pStreamBlocks);
    if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY) {  // back pressure and record position
      destroyStreamDataBlock(pStreamBlocks);
      return -1;
    }

    *totalSize += size;
    *totalBlocks += numOfBlocks;
  } else {
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t streamTaskExecImpl(SStreamTask* pTask, SStreamQueueItem* pItem, int64_t* totalSize,
                                  int32_t* totalBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  void*   pExecutor = pTask->exec.pExecutor;

  *totalBlocks = 0;
  *totalSize = 0;

  int32_t size = 0;
  int32_t numOfBlocks = 0;
  SArray* pRes = NULL;

  while (1) {
    if (pRes == NULL) {
      pRes = taosArrayInit(4, sizeof(SSDataBlock));
    }

    if (streamTaskShouldStop(&pTask->status)) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
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
        SSDataBlock             block = {0};
        const SStreamDataBlock* pRetrieveBlock = (const SStreamDataBlock*)pItem;
        ASSERT(taosArrayGetSize(pRetrieveBlock->blocks) == 1);

        assignOneDataBlock(&block, taosArrayGet(pRetrieveBlock->blocks, 0));
        block.info.type = STREAM_PULL_OVER;
        block.info.childId = pTask->selfChildId;
        taosArrayPush(pRes, &block);
        numOfBlocks += 1;
        qDebug("s-task:%s(child %d) processed retrieve, reqId:0x%" PRIx64, pTask->id.idStr, pTask->selfChildId,
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

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->selfChildId;

    size += blockDataGetSize(output) + sizeof(SSDataBlock) + sizeof(SColumnInfoData) * blockDataGetNumOfCols(&block);
    numOfBlocks += 1;

    taosArrayPush(pRes, &block);

    qDebug("s-task:%s (child %d) executed and get %d result blocks, size:%.2fMiB", pTask->id.idStr,
           pTask->selfChildId, numOfBlocks, size / 1048576.0);

    // current output should be dispatched to down stream nodes
    if (numOfBlocks >= MAX_STREAM_RESULT_DUMP_THRESHOLD) {
      ASSERT(numOfBlocks == taosArrayGetSize(pRes));
      code = doDumpResult(pTask, pItem, pRes, size, totalSize, totalBlocks);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      pRes = NULL;
      size = 0;
      numOfBlocks = 0;
    }
  }

  if (numOfBlocks > 0) {
    ASSERT(numOfBlocks == taosArrayGetSize(pRes));
    code = doDumpResult(pTask, pItem, pRes, size, totalSize, totalBlocks);
  } else {
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
  }

  return code;
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
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
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

      qDebug("s-task:%s scan exec numOfBlocks:%d, limit:%d", pTask->id.idStr, batchCnt, batchSz);
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
    code = streamTaskOutputResultBlock(pTask, qRes);
    if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      taosFreeQitem(qRes);
      return code;
    }
//
//    if (pTask->outputType == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputType == TASK_OUTPUT__SHUFFLE_DISPATCH) {
//      qDebug("s-task:%s scan exec dispatch blocks:%d", pTask->id.idStr, batchCnt);
//      streamDispatchStreamBlock(pTask);
//    }

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
    streamTaskOutputResultBlock(pTask, (SStreamDataBlock*)pItem);
  }

  // exec impl

  // output
  // try dispatch
  return 0;
}
#endif

int32_t updateCheckPointInfo(SStreamTask* pTask) {
  int64_t ckId = 0;
  int64_t dataVer = 0;
  qGetCheckpointVersion(pTask->exec.pExecutor, &dataVer, &ckId);

  SCheckpointInfo* pCkInfo = &pTask->chkInfo;
  if (ckId > pCkInfo->id) {  // save it since the checkpoint is updated
    qDebug("s-task:%s exec end, start to update check point, ver from %" PRId64 " to %" PRId64
           ", checkPoint id:%" PRId64 " -> %" PRId64,
           pTask->id.idStr, pCkInfo->version, dataVer, pCkInfo->id, ckId);

    pTask->chkInfo = (SCheckpointInfo){.version = dataVer, .id = ckId, .currentVer = pCkInfo->currentVer};

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
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * todo: the batch of blocks should be tuned dynamic, according to the total elapsed time of each batch of blocks, the
 * appropriate batch of blocks should be handled in 5 to 10 sec.
 */
int32_t streamExecForAll(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  while (1) {
    int32_t batchSize = 1;
    int16_t times = 0;

    SStreamQueueItem* pInput = NULL;

    // merge multiple input data if possible in the input queue.
    qDebug("s-task:%s start to extract data block from inputQ", id);

    while (1) {
      if (streamTaskShouldPause(&pTask->status)) {
        if (batchSize > 1) {
          break;
        } else {
          return 0;
        }
      }

      SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
      if (qItem == NULL) {
        if (pTask->taskLevel == TASK_LEVEL__SOURCE && batchSize < MIN_STREAM_EXEC_BATCH_NUM && times < 5) {
          times++;
          taosMsleep(10);
          qDebug("===stream===try again batchSize:%d", batchSize);
          continue;
        }

        qDebug("===stream===break batchSize:%d", batchSize);
        break;
      }

      if (pInput == NULL) {
        pInput = qItem;
        streamQueueProcessSuccess(pTask->inputQueue);
        if (pTask->taskLevel == TASK_LEVEL__SINK) {
          break;
        }
      } else {
        // todo we need to sort the data block, instead of just appending into the array list.
        void* newRet = NULL;
        if ((newRet = streamMergeQueueItem(pInput, qItem)) == NULL) {
          streamQueueProcessFail(pTask->inputQueue);
          break;
        } else {
          batchSize++;
          pInput = newRet;
          streamQueueProcessSuccess(pTask->inputQueue);

          if (batchSize > MAX_STREAM_EXEC_BATCH_NUM) {
            qDebug("s-task:%s batch size limit:%d reached, start to process blocks", id,
                   MAX_STREAM_EXEC_BATCH_NUM);
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
      ASSERT(pInput->type == STREAM_INPUT__DATA_BLOCK);
      qDebug("s-task:%s sink task start to sink %d blocks", id, batchSize);
      streamTaskOutputResultBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    // wait for the task to be ready to go
    while (pTask->taskLevel == TASK_LEVEL__SOURCE) {
      int8_t status = atomic_load_8(&pTask->status.taskStatus);
      if (status != TASK_STATUS__NORMAL && status != TASK_STATUS__PAUSE) {
        qError("stream task wait for the end of fill history, s-task:%s, status:%d", id,
               atomic_load_8(&pTask->status.taskStatus));
        taosMsleep(100);
      } else {
        break;
      }
    }

    int64_t st = taosGetTimestampMs();
    qDebug("s-task:%s start to process batch of blocks, num:%d", id, batchSize);

    {
      // set input
      void* pExecutor = pTask->exec.pExecutor;

      const SStreamQueueItem* pItem = pInput;
      if (pItem->type == STREAM_INPUT__GET_RES) {
        const SStreamTrigger* pTrigger = (const SStreamTrigger*)pInput;
        qSetMultiStreamInput(pExecutor, pTrigger->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
      } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
        ASSERT(pTask->taskLevel == TASK_LEVEL__SOURCE);
        const SStreamDataSubmit* pSubmit = (const SStreamDataSubmit*)pInput;
        qSetMultiStreamInput(pExecutor, &pSubmit->submit, 1, STREAM_INPUT__DATA_SUBMIT);
        qDebug("s-task:%s set submit blocks as source block completed, %p %p len:%d ver:%" PRId64, id, pSubmit,
               pSubmit->submit.msgStr, pSubmit->submit.msgLen, pSubmit->submit.ver);
      } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
        const SStreamDataBlock* pBlock = (const SStreamDataBlock*)pInput;

        SArray* pBlockList = pBlock->blocks;
        int32_t numOfBlocks = taosArrayGetSize(pBlockList);
        qDebug("s-task:%s set sdata blocks as input num:%d, ver:%" PRId64, id, numOfBlocks, pBlock->sourceVer);
        qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__DATA_BLOCK);
      } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
        const SStreamMergedSubmit* pMerged = (const SStreamMergedSubmit*)pInput;

        SArray* pBlockList = pMerged->submits;
        int32_t numOfBlocks = taosArrayGetSize(pBlockList);
        qDebug("s-task:%s %p set (merged) submit blocks as a batch, numOfBlocks:%d", id, pTask, numOfBlocks);
        qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__MERGED_SUBMIT);
      } else if (pItem->type == STREAM_INPUT__REF_DATA_BLOCK) {
        const SStreamRefDataBlock* pRefBlock = (const SStreamRefDataBlock*)pInput;
        qSetMultiStreamInput(pExecutor, pRefBlock->pBlock, 1, STREAM_INPUT__DATA_BLOCK);
      } else {
        ASSERT(0);
      }
    }

    int64_t resSize = 0;
    int32_t totalBlocks = 0;
    streamTaskExecImpl(pTask, pInput, &resSize, &totalBlocks);

    double  el = (taosGetTimestampMs() - st) / 1000.0;
    qDebug("s-task:%s batch of input blocks exec end, elapsed time:%.2fs, result size:%.2fMiB, numOfBlocks:%d",
           id, el, resSize / 1048576.0, totalBlocks);
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

    if (!taosQueueEmpty(pTask->inputQueue->queue) && (!streamTaskShouldStop(&pTask->status)) &&
        (!streamTaskShouldPause(&pTask->status))) {
      streamSchedExec(pTask);
    }
  }

  return 0;
}
