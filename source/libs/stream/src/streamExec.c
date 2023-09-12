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

// maximum allowed processed block batches. One block may include several submit blocks
#define MAX_STREAM_EXEC_BATCH_NUM        32
#define MIN_STREAM_EXEC_BATCH_NUM        4
#define STREAM_RESULT_DUMP_THRESHOLD     100

static int32_t streamDoTransferStateToStreamTask(SStreamTask* pTask);

bool streamTaskShouldStop(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*)&pStatus->taskStatus);
  return (status == TASK_STATUS__STOP) || (status == TASK_STATUS__DROPPING);
}

bool streamTaskShouldPause(const SStreamStatus* pStatus) {
  int32_t status = atomic_load_8((int8_t*)&pStatus->taskStatus);
  return (status == TASK_STATUS__PAUSE);
}

static int32_t doOutputResultBlockImpl(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  int32_t code = 0;
  int32_t type = pTask->outputInfo.type;
  if (type == TASK_OUTPUT__TABLE) {
    pTask->tbSink.tbSinkFunc(pTask, pTask->tbSink.vnode, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else if (type == TASK_OUTPUT__SMA) {
    pTask->smaSink.smaSink(pTask->smaSink.vnode, pTask->smaSink.smaId, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else {
    ASSERT(type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH);
    code = streamTaskPutDataIntoOutputQ(pTask, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    streamDispatchStreamBlock(pTask);
    return code;
  }

  return 0;
}

static int32_t doDumpResult(SStreamTask* pTask, SStreamQueueItem* pItem, SArray* pRes, int32_t size, int64_t* totalSize,
                            int32_t* totalBlocks) {
  int32_t numOfBlocks = taosArrayGetSize(pRes);
  if (numOfBlocks == 0) {
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
    return TSDB_CODE_SUCCESS;
  }

  SStreamDataBlock* pStreamBlocks = createStreamBlockFromResults(pItem, pTask, size, pRes);
  if (pStreamBlocks == NULL) {
    qError("s-task:%s failed to create result stream data block, code:%s", pTask->id.idStr, tstrerror(terrno));
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  qDebug("s-task:%s dump stream result data blocks, num:%d, size:%.2fMiB", pTask->id.idStr, numOfBlocks,
         SIZE_IN_MB(size));

  int32_t code = doOutputResultBlockImpl(pTask, pStreamBlocks);
  if (code != TSDB_CODE_SUCCESS) {  // back pressure and record position
    //code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY
    destroyStreamDataBlock(pStreamBlocks);
    return code;
  }

  *totalSize += size;
  *totalBlocks += numOfBlocks;

  return code;
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

    if (pTask->inputInfo.status == TASK_INPUT_STATUS__BLOCKED) {
      qWarn("s-task:%s downstream task inputQ blocked, idle for 1sec and retry exec task", pTask->id.idStr);
      taosMsleep(1000);
      continue;
    }

    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if ((code = qExecTask(pExecutor, &output, &ts)) < 0) {
      if (code == TSDB_CODE_QRY_IN_EXEC) {
        resetTaskInfo(pExecutor);
      }

      qError("unexpected stream execution, s-task:%s since %s", pTask->id.idStr, tstrerror(code));
      continue;
    }

    if (output == NULL) {
      if (pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
        SSDataBlock             block = {0};
        const SStreamDataBlock* pRetrieveBlock = (const SStreamDataBlock*)pItem;
        ASSERT(taosArrayGetSize(pRetrieveBlock->blocks) == 1);

        assignOneDataBlock(&block, taosArrayGet(pRetrieveBlock->blocks, 0));
        block.info.type = STREAM_PULL_OVER;
        block.info.childId = pTask->info.selfChildId;
        taosArrayPush(pRes, &block);
        numOfBlocks += 1;

        qDebug("s-task:%s(child %d) retrieve process completed, reqId:0x%" PRIx64 " dump results", pTask->id.idStr,
               pTask->info.selfChildId, pRetrieveBlock->reqId);
      }

      break;
    }

    if (output->info.type == STREAM_RETRIEVE) {
      if (streamBroadcastToChildren(pTask, output) < 0) {
        // TODO
      }
      continue;
    } else if (output->info.type == STREAM_CHECKPOINT) {
      continue;  // checkpoint block not dispatch to downstream tasks
    }

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->info.selfChildId;

    size += blockDataGetSize(output) + sizeof(SSDataBlock) + sizeof(SColumnInfoData) * blockDataGetNumOfCols(&block);
    numOfBlocks += 1;

    taosArrayPush(pRes, &block);

    qDebug("s-task:%s (child %d) executed and get %d result blocks, size:%.2fMiB", pTask->id.idStr,
           pTask->info.selfChildId, numOfBlocks, SIZE_IN_MB(size));

    // current output should be dispatched to down stream nodes
    if (numOfBlocks >= STREAM_RESULT_DUMP_THRESHOLD) {
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

int32_t streamScanHistoryData(SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  int32_t code = TSDB_CODE_SUCCESS;
  void*   exec = pTask->exec.pExecutor;
  bool    finished = false;
  int32_t outputBatchSize = 100;

  qSetStreamOpOpen(exec);

  while (!finished) {
    if (streamTaskShouldPause(&pTask->status)) {
      double el = (taosGetTimestampMs() - pTask->tsInfo.step1Start) / 1000.0;
      qDebug("s-task:%s paused from the scan-history task, elapsed time:%.2fsec", pTask->id.idStr, el);
      break;
    }

    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    if (pRes == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    int32_t numOfBlocks = 0;
    while (1) {
      if (streamTaskShouldStop(&pTask->status)) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        return 0;
      }

      if (pTask->inputInfo.status == TASK_INPUT_STATUS__BLOCKED) {
        qDebug("s-task:%s inputQ is blocked, wait for 10sec and retry", pTask->id.idStr);
        taosMsleep(10000);
        continue;
      }

      SSDataBlock* output = NULL;
      uint64_t     ts = 0;
      code = qExecTask(exec, &output, &ts);
      if (code != TSDB_CODE_TSC_QUERY_KILLED && code != TSDB_CODE_SUCCESS) {
        qError("%s scan-history data error occurred code:%s, continue scan", pTask->id.idStr, tstrerror(code));
        continue;
      }

      // the generated results before fill-history task been paused, should be dispatched to sink node
      if (output == NULL) {
        finished = qStreamRecoverScanFinished(exec);
        break;
      }

      SSDataBlock block = {0};
      assignOneDataBlock(&block, output);
      block.info.childId = pTask->info.selfChildId;
      taosArrayPush(pRes, &block);

      if ((++numOfBlocks) >= outputBatchSize) {
        qDebug("s-task:%s scan exec numOfBlocks:%d, output limit:%d reached", pTask->id.idStr, numOfBlocks, outputBatchSize);
        break;
      }
    }

    if (taosArrayGetSize(pRes) > 0) {
      SStreamDataBlock* qRes = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
      if (qRes == NULL) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }

      qRes->type = STREAM_INPUT__DATA_BLOCK;
      qRes->blocks = pRes;

      code = doOutputResultBlockImpl(pTask, qRes);
      if (code == TSDB_CODE_UTIL_QUEUE_OUT_OF_MEMORY) {
        taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
        taosFreeQitem(qRes);
        return code;
      }
    } else {
      taosArrayDestroy(pRes);
    }
  }

  return 0;
}

// wait for the stream task to be idle
static void waitForTaskIdle(SStreamTask* pTask, SStreamTask* pStreamTask) {
  const char* id = pTask->id.idStr;

  int64_t st = taosGetTimestampMs();
  while (!streamTaskIsIdle(pStreamTask)) {
    qDebug("s-task:%s level:%d wait for stream task:%s to be idle, check again in 100ms", id, pTask->info.taskLevel,
           pStreamTask->id.idStr);
    taosMsleep(100);
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  if (el > 0) {
    qDebug("s-task:%s wait for stream task:%s for %.2fs to be idle", id, pStreamTask->id.idStr, el);
  }
}

int32_t streamDoTransferStateToStreamTask(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;

  SStreamTask* pStreamTask = streamMetaAcquireTask(pMeta, pTask->streamTaskId.streamId, pTask->streamTaskId.taskId);
  if (pStreamTask == NULL) {
    qError(
        "s-task:%s failed to find related stream task:0x%x, it may have been destroyed or closed, destroy the related "
        "fill-history task",
        pTask->id.idStr, pTask->streamTaskId.taskId);

    // 1. free it and remove fill-history task from disk meta-store
    streamMetaUnregisterTask(pMeta, pTask->id.streamId, pTask->id.taskId);

    // 2. save to disk
    taosWLockLatch(&pMeta->lock);
    if (streamMetaCommit(pMeta) < 0) {
      // persist to disk
    }
    taosWUnLockLatch(&pMeta->lock);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  } else {
    qDebug("s-task:%s fill-history task end, update related stream task:%s info, transfer exec state", pTask->id.idStr,
           pStreamTask->id.idStr);
  }

  ASSERT(pStreamTask->historyTaskId.taskId == pTask->id.taskId && pTask->status.appendTranstateBlock == true);

  STimeWindow* pTimeWindow = &pStreamTask->dataRange.window;

  // todo. the dropping status should be append to the status after the halt completed.
  // It must be halted for a source stream task, since when the related scan-history-data task start scan the history
  // for the step 2.
  int8_t status = pStreamTask->status.taskStatus;
  if (pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    ASSERT(status == TASK_STATUS__HALT || status == TASK_STATUS__DROPPING);
  } else {
    ASSERT(status == TASK_STATUS__NORMAL);
    pStreamTask->status.taskStatus = TASK_STATUS__HALT;
    qDebug("s-task:%s halt by related fill-history task:%s", pStreamTask->id.idStr, pTask->id.idStr);
  }

  // wait for the stream task to handle all in the inputQ, and to be idle
  waitForTaskIdle(pTask, pStreamTask);

  // In case of sink tasks, no need to halt them.
  // In case of source tasks and agg tasks, we should HALT them, and wait for them to be idle. And then, it's safe to
  // start the task state transfer procedure.
  // When a task is idle with halt status, all data in inputQ are consumed.
  if (pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    // update the scan data range for source task.
    qDebug("s-task:%s level:%d stream task window %" PRId64 " - %" PRId64 " update to %" PRId64 " - %" PRId64
           ", status:%s, sched-status:%d",
           pStreamTask->id.idStr, TASK_LEVEL__SOURCE, pTimeWindow->skey, pTimeWindow->ekey, INT64_MIN,
           pTimeWindow->ekey, streamGetTaskStatusStr(TASK_STATUS__NORMAL), pStreamTask->status.schedStatus);
  } else {
    qDebug("s-task:%s no need to update time window for non-source task", pStreamTask->id.idStr);
  }

  // 1. expand the query time window for stream task of WAL scanner
  pTimeWindow->skey = INT64_MIN;
  qStreamInfoResetTimewindowFilter(pStreamTask->exec.pExecutor);

  // 2. transfer the ownership of executor state
  streamTaskReleaseState(pTask);
  streamTaskReloadState(pStreamTask);

  // 3. clear the link between fill-history task and stream task info
  pStreamTask->historyTaskId.taskId = 0;

  // 4. resume the state of stream task, after this function, the stream task will run immidately. But it can not be
  // pause, since the pause allowed attribute is not set yet.
  streamTaskResumeFromHalt(pStreamTask);

  qDebug("s-task:%s fill-history task set status to be dropping, save the state into disk", pTask->id.idStr);

  // 5. free it and remove fill-history task from disk meta-store
  streamMetaUnregisterTask(pMeta, pTask->id.streamId, pTask->id.taskId);

  // 6. save to disk
  taosWLockLatch(&pMeta->lock);
  streamMetaSaveTask(pMeta, pStreamTask);
  if (streamMetaCommit(pMeta) < 0) {
    // persist to disk
  }
  taosWUnLockLatch(&pMeta->lock);

  // 7. pause allowed.
  streamTaskEnablePause(pStreamTask);
  if (taosQueueEmpty(pStreamTask->inputInfo.queue->pQueue)) {
    SStreamRefDataBlock* pItem = taosAllocateQitem(sizeof(SStreamRefDataBlock), DEF_QITEM, 0);

    SSDataBlock* pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
    pDelBlock->info.rows = 0;
    pDelBlock->info.version = 0;
    pItem->type = STREAM_INPUT__REF_DATA_BLOCK;
    pItem->pBlock = pDelBlock;
    int32_t code = streamTaskPutDataIntoInputQ(pStreamTask, (SStreamQueueItem*)pItem);
    qDebug("s-task:%s append dummy delete block,res:%d", pStreamTask->id.idStr, code);
  }

  streamSchedExec(pStreamTask);
  streamMetaReleaseTask(pMeta, pStreamTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamTransferStateToStreamTask(SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  ASSERT(pTask->status.appendTranstateBlock == 1);

  int32_t level = pTask->info.taskLevel;
  if (level == TASK_LEVEL__SOURCE) {
    streamTaskFillHistoryFinished(pTask);
  }

  if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SOURCE) {  // do transfer task operator states.
    code = streamDoTransferStateToStreamTask(pTask);
  }

  return code;
}

// set input
static void doSetStreamInputBlock(SStreamTask* pTask, const void* pInput, int64_t* pVer, const char* id) {
  void* pExecutor = pTask->exec.pExecutor;

  const SStreamQueueItem* pItem = pInput;
  if (pItem->type == STREAM_INPUT__GET_RES) {
    const SStreamTrigger* pTrigger = (const SStreamTrigger*)pInput;
    qSetMultiStreamInput(pExecutor, pTrigger->pBlock, 1, STREAM_INPUT__DATA_BLOCK);

  } else if (pItem->type == STREAM_INPUT__DATA_SUBMIT) {
    ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);
    const SStreamDataSubmit* pSubmit = (const SStreamDataSubmit*)pInput;
    qSetMultiStreamInput(pExecutor, &pSubmit->submit, 1, STREAM_INPUT__DATA_SUBMIT);
    qDebug("s-task:%s set submit blocks as source block completed, %p %p len:%d ver:%" PRId64, id, pSubmit,
           pSubmit->submit.msgStr, pSubmit->submit.msgLen, pSubmit->submit.ver);
    ASSERT((*pVer) <= pSubmit->submit.ver);
    (*pVer) = pSubmit->submit.ver;

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
    qDebug("s-task:%s %p set (merged) submit blocks as a batch, numOfBlocks:%d, ver:%" PRId64, id, pTask, numOfBlocks,
           pMerged->ver);
    qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__MERGED_SUBMIT);
    ASSERT((*pVer) <= pMerged->ver);
    (*pVer) = pMerged->ver;

  } else if (pItem->type == STREAM_INPUT__REF_DATA_BLOCK) {
    const SStreamRefDataBlock* pRefBlock = (const SStreamRefDataBlock*)pInput;
    qSetMultiStreamInput(pExecutor, pRefBlock->pBlock, 1, STREAM_INPUT__DATA_BLOCK);

  } else if (pItem->type == STREAM_INPUT__CHECKPOINT || pItem->type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
    const SStreamDataBlock* pCheckpoint = (const SStreamDataBlock*)pInput;
    qSetMultiStreamInput(pExecutor, pCheckpoint->blocks, 1, pItem->type);

  } else {
    ASSERT(0);
  }
}

int32_t streamProcessTranstateBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  const char* id = pTask->id.idStr;
  int32_t     code = TSDB_CODE_SUCCESS;

  int32_t level = pTask->info.taskLevel;
  if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SINK) {
    int32_t remain = streamAlignTransferState(pTask);
    if (remain > 0) {
      streamFreeQitem((SStreamQueueItem*)pBlock);
      qDebug("s-task:%s receive upstream transfer state msg, remain:%d", id, remain);
      return 0;
    }
  }

  // dispatch the tran-state block to downstream task immediately
  int32_t type = pTask->outputInfo.type;

  // transfer the ownership of executor state
  if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (level == TASK_LEVEL__SOURCE) {
      qDebug("s-task:%s add transfer-state block into outputQ", id);
    } else {
      qDebug("s-task:%s all upstream tasks send transfer-state block, add transfer-state block into outputQ", id);
      ASSERT(pTask->streamTaskId.taskId != 0 && pTask->info.fillHistory == 1);
    }

    // agg task should dispatch trans-state msg to sink task, to flush all data to sink task.
    if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SOURCE) {
      pBlock->srcVgId = pTask->pMeta->vgId;
      code = taosWriteQitem(pTask->outputInfo.queue->pQueue, pBlock);
      if (code == 0) {
        streamDispatchStreamBlock(pTask);
      } else {
        streamFreeQitem((SStreamQueueItem*)pBlock);
      }
    } else {  // level == TASK_LEVEL__SINK
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else {  // non-dispatch task, do task state transfer directly
    streamFreeQitem((SStreamQueueItem*)pBlock);
    if (level != TASK_LEVEL__SINK) {
      qDebug("s-task:%s non-dispatch task, start to transfer state directly", id);
      ASSERT(pTask->info.fillHistory == 1);
      code = streamTransferStateToStreamTask(pTask);

      if (code != TSDB_CODE_SUCCESS) {
        atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
      }
    } else {
      qDebug("s-task:%s sink task does not transfer state", id);
    }
  }

  return code;
}

/**
 * todo: the batch of blocks should be tuned dynamic, according to the total elapsed time of each batch of blocks, the
 * appropriate batch of blocks should be handled in 5 to 10 sec.
 */
int32_t streamExecForAll(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  while (1) {
    int32_t           numOfBlocks = 0;
    SStreamQueueItem* pInput = NULL;
    if (streamTaskShouldStop(&pTask->status)) {
      qDebug("s-task:%s stream task is stopped", id);
      break;
    }

    // merge multiple input data if possible in the input queue.
    qDebug("s-task:%s start to extract data block from inputQ", id);

    /*int32_t code = */ streamTaskGetDataFromInputQ(pTask, &pInput, &numOfBlocks);
    if (pInput == NULL) {
      ASSERT(numOfBlocks == 0);
      return 0;
    }

    int32_t type = pInput->type;

    // dispatch checkpoint msg to all downstream tasks
    if (type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
      streamProcessCheckpointBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    if (pInput->type == STREAM_INPUT__TRANS_STATE) {
      streamProcessTranstateBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
      ASSERT(type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__CHECKPOINT);

      if (type == STREAM_INPUT__DATA_BLOCK) {
        qDebug("s-task:%s sink task start to sink %d blocks", id, numOfBlocks);
        doOutputResultBlockImpl(pTask, (SStreamDataBlock*)pInput);
        continue;
      }
    }

    int64_t st = taosGetTimestampMs();

    const SStreamQueueItem* pItem = pInput;
    qDebug("s-task:%s start to process batch of blocks, num:%d, type:%d", id, numOfBlocks, pItem->type);

    int64_t ver = pTask->chkInfo.checkpointVer;
    doSetStreamInputBlock(pTask, pInput, &ver, id);

    int64_t resSize = 0;
    int32_t totalBlocks = 0;
    streamTaskExecImpl(pTask, pInput, &resSize, &totalBlocks);

    double el = (taosGetTimestampMs() - st) / 1000.0;
    qDebug("s-task:%s batch of input blocks exec end, elapsed time:%.2fs, result size:%.2fMiB, numOfBlocks:%d", id, el,
           SIZE_IN_MB(resSize), totalBlocks);

    // update the currentVer if processing the submit blocks.
    ASSERT(pTask->chkInfo.checkpointVer <= pTask->chkInfo.nextProcessVer && ver >= pTask->chkInfo.checkpointVer);

    if (ver != pTask->chkInfo.checkpointVer) {
      qDebug("s-task:%s update checkpointVer(unsaved) from %" PRId64 " to %" PRId64 " , currentVer:%" PRId64,
             pTask->id.idStr, pTask->chkInfo.checkpointVer, ver, pTask->chkInfo.nextProcessVer);
      pTask->chkInfo.checkpointVer = ver;
    }

    streamFreeQitem(pInput);

    // todo other thread may change the status
    // do nothing after sync executor state to storage backend, untill the vnode-level checkpoint is completed.
    if (type == STREAM_INPUT__CHECKPOINT) {
      qDebug("s-task:%s checkpoint block received, set the status:%s", pTask->id.idStr,
             streamGetTaskStatusStr(pTask->status.taskStatus));
      streamTaskBuildCheckpoint(pTask);
      return 0;
    }
  }

  return 0;
}

// the task may be set dropping/stopping, while it is still in the task queue, therefore, the sched-status can not
// be updated by tryExec function, therefore, the schedStatus will always be the TASK_SCHED_STATUS__WAITING.
bool streamTaskIsIdle(const SStreamTask* pTask) {
  return (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE || pTask->status.taskStatus == TASK_STATUS__STOP ||
          pTask->status.taskStatus == TASK_STATUS__DROPPING);
}

int32_t streamTryExec(SStreamTask* pTask) {
  // this function may be executed by multi-threads, so status check is required.
  int8_t schedStatus =
      atomic_val_compare_exchange_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__WAITING, TASK_SCHED_STATUS__ACTIVE);

  const char* id = pTask->id.idStr;

  if (schedStatus == TASK_SCHED_STATUS__WAITING) {
    int32_t code = streamExecForAll(pTask);
    if (code < 0) {  // todo this status shoudl be removed
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__FAILED);
      return -1;
    }

    atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
    qDebug("s-task:%s exec completed, status:%s, sched-status:%d", id, streamGetTaskStatusStr(pTask->status.taskStatus),
           pTask->status.schedStatus);

    if (!(taosQueueEmpty(pTask->inputInfo.queue->pQueue) || streamTaskShouldStop(&pTask->status) ||
          streamTaskShouldPause(&pTask->status))) {
      streamSchedExec(pTask);
    }
  } else {
    qDebug("s-task:%s already started to exec by other thread, status:%s, sched-status:%d", id,
           streamGetTaskStatusStr(pTask->status.taskStatus), pTask->status.schedStatus);
  }

  return 0;
}

int32_t streamTaskReleaseState(SStreamTask* pTask) {
  qDebug("s-task:%s release exec state", pTask->id.idStr);
  void* pExecutor = pTask->exec.pExecutor;
  if (pExecutor != NULL) {
    int32_t code = qStreamOperatorReleaseState(pExecutor);
    return code;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t streamTaskReloadState(SStreamTask* pTask) {
  qDebug("s-task:%s reload exec state", pTask->id.idStr);
  void* pExecutor = pTask->exec.pExecutor;
  if (pExecutor != NULL) {
    int32_t code = qStreamOperatorReloadState(pExecutor);
    return code;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t streamAlignTransferState(SStreamTask* pTask) {
  int32_t numOfUpstream = taosArrayGetSize(pTask->pUpstreamInfoList);
  int32_t old = atomic_val_compare_exchange_32(&pTask->transferStateAlignCnt, 0, numOfUpstream);
  if (old == 0) {
    qDebug("s-task:%s set the transfer state aligncnt %d", pTask->id.idStr, numOfUpstream);
  }

  return atomic_sub_fetch_32(&pTask->transferStateAlignCnt, 1);
}
