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
#define MAX_STREAM_EXEC_BATCH_NUM         32
#define STREAM_RESULT_DUMP_THRESHOLD      300
#define STREAM_RESULT_DUMP_SIZE_THRESHOLD (1048576 * 1)   // 1MiB result data
#define STREAM_SCAN_HISTORY_TIMESLICE     1000            // 1000 ms

static int32_t streamDoTransferStateToStreamTask(SStreamTask* pTask);

bool streamTaskShouldStop(const SStreamTask* pTask) {
  SStreamTaskState* pState = streamTaskGetStatus(pTask);
  return (pState->state == TASK_STATUS__STOP) || (pState->state == TASK_STATUS__DROPPING);
}

bool streamTaskShouldPause(const SStreamTask* pTask) {
  return (streamTaskGetStatus(pTask)->state == TASK_STATUS__PAUSE);
}

static int32_t doOutputResultBlockImpl(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  int32_t code = 0;
  int32_t type = pTask->outputInfo.type;
  if (type == TASK_OUTPUT__TABLE) {
    pTask->outputInfo.tbSink.tbSinkFunc(pTask, pTask->outputInfo.tbSink.vnode, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else if (type == TASK_OUTPUT__SMA) {
    pTask->outputInfo.smaSink.smaSink(pTask->outputInfo.smaSink.vnode, pTask->outputInfo.smaSink.smaId, pBlock->blocks);
    destroyStreamDataBlock(pBlock);
  } else {
    ASSERT(type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH);
    code = streamTaskPutDataIntoOutputQ(pTask, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // checkpoint trigger will be checked
    streamDispatchStreamBlock(pTask);
  }

  return code;
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
    stError("s-task:%s failed to create result stream data block, code:%s", pTask->id.idStr, tstrerror(terrno));
    taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  stDebug("s-task:%s dump stream result data blocks, num:%d, size:%.2fMiB", pTask->id.idStr, numOfBlocks,
         SIZE_IN_MiB(size));

  int32_t code = doOutputResultBlockImpl(pTask, pStreamBlocks);
  if (code != TSDB_CODE_SUCCESS) {  // back pressure and record position
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

    if (streamTaskShouldStop(pTask)) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      return 0;
    }

    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    if ((code = qExecTask(pExecutor, &output, &ts)) < 0) {
      if (code == TSDB_CODE_QRY_IN_EXEC) {
        resetTaskInfo(pExecutor);
      }

      stError("unexpected stream execution, s-task:%s since %s", pTask->id.idStr, tstrerror(code));
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

        stDebug("s-task:%s(child %d) retrieve process completed, reqId:0x%" PRIx64 " dump results", pTask->id.idStr,
               pTask->info.selfChildId, pRetrieveBlock->reqId);
      }

      break;
    }

    if (output->info.type == STREAM_RETRIEVE) {
      if (streamBroadcastToUpTasks(pTask, output) < 0) {
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

    stDebug("s-task:%s (child %d) executed and get %d result blocks, size:%.2fMiB", pTask->id.idStr,
           pTask->info.selfChildId, numOfBlocks, SIZE_IN_MiB(size));

    // current output should be dispatched to down stream nodes
    if (numOfBlocks >= STREAM_RESULT_DUMP_THRESHOLD || size >= STREAM_RESULT_DUMP_SIZE_THRESHOLD) {
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

static int32_t handleResultBlocks(SStreamTask* pTask, SArray* pRes, int32_t size) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (taosArrayGetSize(pRes) > 0) {
    SStreamDataBlock* pStreamBlocks = createStreamBlockFromResults(NULL, pTask, size, pRes);
    code = doOutputResultBlockImpl(pTask, pStreamBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      stDebug("s-task:%s dump fill-history results failed, code:%s", pTask->id.idStr, tstrerror(code));
    }
  } else {
    taosArrayDestroy(pRes);
  }
  return code;
}

static void streamScanHistoryDataImpl(SStreamTask* pTask, SArray* pRes, int32_t* pSize, bool* pFinish) {
  int32_t code = TSDB_CODE_SUCCESS;
  void*   exec = pTask->exec.pExecutor;
  int32_t numOfBlocks = 0;

  while (1) {
    if (streamTaskShouldStop(pTask)) {
      break;
    }

    if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED) {
      stDebug("s-task:%s level:%d inputQ is blocked, retry in 5s", pTask->id.idStr, pTask->info.taskLevel);
      break;
    }

    SSDataBlock* output = NULL;
    uint64_t     ts = 0;
    code = qExecTask(exec, &output, &ts);
    if (code != TSDB_CODE_TSC_QUERY_KILLED && code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s scan-history data error occurred code:%s, continue scan-history", pTask->id.idStr,
              tstrerror(code));
      continue;
    }

    // the generated results before fill-history task been paused, should be dispatched to sink node
    if (output == NULL) {
      (*pFinish) = qStreamScanhistoryFinished(exec);
      break;
    }

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->info.selfChildId;
    taosArrayPush(pRes, &block);

    (*pSize) += blockDataGetSize(output) + sizeof(SSDataBlock) + sizeof(SColumnInfoData) * blockDataGetNumOfCols(&block);
    numOfBlocks += 1;

    if (numOfBlocks >= STREAM_RESULT_DUMP_THRESHOLD || (*pSize) >= STREAM_RESULT_DUMP_SIZE_THRESHOLD) {
      stDebug("s-task:%s scan exec numOfBlocks:%d, size:%.2fKiB output num-limit:%d, size-limit:%.2fKiB reached",
              pTask->id.idStr, numOfBlocks, SIZE_IN_KiB(*pSize), STREAM_RESULT_DUMP_THRESHOLD,
              SIZE_IN_KiB(STREAM_RESULT_DUMP_SIZE_THRESHOLD));
      break;
    }
  }
}

SScanhistoryDataInfo streamScanHistoryData(SStreamTask* pTask, int64_t st) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  void*       exec = pTask->exec.pExecutor;
  bool        finished = false;
  const char* id = pTask->id.idStr;

  if (!pTask->hTaskInfo.operatorOpen) {
    qSetStreamOpOpen(exec);
    pTask->hTaskInfo.operatorOpen = true;
  }

  while (1) {
    if (streamTaskShouldPause(pTask)) {
      stDebug("s-task:%s paused from the scan-history task", id);
      // quit from step1, not continue to handle the step2
      return (SScanhistoryDataInfo){TASK_SCANHISTORY_QUIT, 0};
    }

    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    if (pRes == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      stError("s-task:%s scan-history prepare result block failed, code:%s, retry later", id, tstrerror(terrno));
      continue;
    }

    int32_t size = 0;
    streamScanHistoryDataImpl(pTask, pRes, &size, &finished);

    if(streamTaskShouldStop(pTask)) {
      taosArrayDestroyEx(pRes, (FDelete)blockDataFreeRes);
      return (SScanhistoryDataInfo){TASK_SCANHISTORY_QUIT, 0};
    }

    // dispatch the generated results
    /*int32_t code = */handleResultBlocks(pTask, pRes, size);

    int64_t el = taosGetTimestampMs() - st;

    // downstream task input queue is full, try in 5sec
    if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED && (pTask->info.fillHistory == 1)) {
      return (SScanhistoryDataInfo){TASK_SCANHISTORY_REXEC, 5000};
    }

    if (finished) {
      return (SScanhistoryDataInfo){TASK_SCANHISTORY_CONT, 0};
    }

    if (el >= STREAM_SCAN_HISTORY_TIMESLICE && (pTask->info.fillHistory == 1)) {
      stDebug("s-task:%s fill-history:%d time slice exhausted, elapsed time:%.2fs, retry in 100ms", id,
              pTask->info.fillHistory, el / 1000.0);
      return (SScanhistoryDataInfo){TASK_SCANHISTORY_REXEC, 100};
    }
  }
}

// wait for the stream task to be idle
static void waitForTaskIdle(SStreamTask* pTask, SStreamTask* pStreamTask) {
  const char* id = pTask->id.idStr;

  int64_t st = taosGetTimestampMs();
  while (!streamTaskIsIdle(pStreamTask)) {
    stDebug("s-task:%s level:%d wait for stream task:%s to be idle, check again in 100ms", id, pTask->info.taskLevel,
            pStreamTask->id.idStr);
    taosMsleep(100);
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  if (el > 0) {
    stDebug("s-task:%s wait for stream task:%s for %.2fs to be idle", id, pStreamTask->id.idStr, el);
  }
}

int32_t streamDoTransferStateToStreamTask(SStreamTask* pTask) {
  SStreamMeta* pMeta = pTask->pMeta;
  const char*  id = pTask->id.idStr;

  SStreamTask* pStreamTask = streamMetaAcquireTask(pMeta, pTask->streamTaskId.streamId, pTask->streamTaskId.taskId);
  if (pStreamTask == NULL) {
    stError(
        "s-task:%s failed to find related stream task:0x%x, it may have been destroyed or closed, destroy the related "
        "fill-history task",
        id, (int32_t) pTask->streamTaskId.taskId);

    // 1. free it and remove fill-history task from disk meta-store
    streamBuildAndSendDropTaskMsg(pTask->pMsgCb, pMeta->vgId, &pTask->id, 0);

    // 2. save to disk
    streamMetaWLock(pMeta);
    if (streamMetaCommit(pMeta) < 0) {
      // persist to disk
    }
    streamMetaWUnLock(pMeta);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  } else {
    double el = (taosGetTimestampMs() - pTask->execInfo.step2Start) / 1000.;
    stDebug(
        "s-task:%s fill-history task end, scan wal elapsed time:%.2fSec,update related stream task:%s info, transfer "
        "exec state",
        id, el, pStreamTask->id.idStr);
  }

  ETaskStatus status = streamTaskGetStatus(pStreamTask)->state;
  STimeWindow* pTimeWindow = &pStreamTask->dataRange.window;

  // It must be halted for a source stream task, since when the related scan-history-data task start scan the history
  // for the step 2.
  if (pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    ASSERT(status == TASK_STATUS__HALT || status == TASK_STATUS__DROPPING || status == TASK_STATUS__STOP);
  } else {
    ASSERT(status == TASK_STATUS__READY || status == TASK_STATUS__DROPPING || status == TASK_STATUS__STOP);
    int32_t code = streamTaskHandleEvent(pStreamTask->status.pSM, TASK_EVENT_HALT);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s halt stream task:%s failed, code:%s not transfer state to stream task", id,
              pStreamTask->id.idStr, tstrerror(code));
      streamMetaReleaseTask(pMeta, pStreamTask);
      return code;
    } else {
      stDebug("s-task:%s halt by related fill-history task:%s", pStreamTask->id.idStr, id);
    }
  }

  // wait for the stream task to handle all in the inputQ, and to be idle
  waitForTaskIdle(pTask, pStreamTask);

  // In case of sink tasks, no need to halt them.
  // In case of source tasks and agg tasks, we should HALT them, and wait for them to be idle. And then, it's safe to
  // start the task state transfer procedure.
  SStreamTaskState* pState = streamTaskGetStatus(pStreamTask);
  status = pState->state;
  char* p = pState->name;
  if (status == TASK_STATUS__STOP || status == TASK_STATUS__DROPPING) {
    stError("s-task:%s failed to transfer state from fill-history task:%s, status:%s", id, pStreamTask->id.idStr, p);
    streamMetaReleaseTask(pMeta, pStreamTask);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // 1. expand the query time window for stream task of WAL scanner
  if (pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    // update the scan data range for source task.
    stDebug("s-task:%s level:%d stream task window %" PRId64 " - %" PRId64 " update to %" PRId64 " - %" PRId64
            ", status:%s, sched-status:%d",
            pStreamTask->id.idStr, TASK_LEVEL__SOURCE, pTimeWindow->skey, pTimeWindow->ekey, INT64_MIN,
            pTimeWindow->ekey, p, pStreamTask->status.schedStatus);

    streamTaskResetTimewindowFilter(pStreamTask);
  } else {
    stDebug("s-task:%s no need to update/reset filter time window for non-source tasks", pStreamTask->id.idStr);
  }

  // 2. transfer the ownership of executor state
  streamTaskReleaseState(pTask);
  streamTaskReloadState(pStreamTask);

  // 3. send msg to mnode to launch a checkpoint to keep the state for current stream
  streamTaskSendCheckpointReq(pStreamTask);

  // 4. assign the status to the value that will be kept in disk
  pStreamTask->status.taskStatus = streamTaskGetStatus(pStreamTask)->state;

  // 5. open the inputQ for all upstream tasks
  streamTaskOpenAllUpstreamInput(pStreamTask);

  streamMetaReleaseTask(pMeta, pStreamTask);
  return TSDB_CODE_SUCCESS;
}

int32_t streamTransferStateToStreamTask(SStreamTask* pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  SStreamMeta* pMeta = pTask->pMeta;

  ASSERT(pTask->status.appendTranstateBlock == 1);

  int32_t level = pTask->info.taskLevel;
  if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SOURCE) {  // do transfer task operator states.
    code = streamDoTransferStateToStreamTask(pTask);
  } else { // no state transfer for sink tasks, and drop fill-history task, followed by opening inputQ of sink task.
    SStreamTask* pStreamTask = streamMetaAcquireTask(pMeta, pTask->streamTaskId.streamId, pTask->streamTaskId.taskId);
    if (pStreamTask != NULL) {
      // halt the related stream sink task
      code = streamTaskHandleEvent(pStreamTask->status.pSM, TASK_EVENT_HALT);
      if (code != TSDB_CODE_SUCCESS) {
        stError("s-task:%s halt stream task:%s failed, code:%s not transfer state to stream task", pTask->id.idStr,
                pStreamTask->id.idStr, tstrerror(code));
        streamMetaReleaseTask(pMeta, pStreamTask);
        return code;
      } else {
        stDebug("s-task:%s halt by related fill-history task:%s", pStreamTask->id.idStr, pTask->id.idStr);
      }

      streamTaskOpenAllUpstreamInput(pStreamTask);
      streamTaskSendCheckpointReq(pStreamTask);
      streamMetaReleaseTask(pMeta, pStreamTask);
    }
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
    stDebug("s-task:%s set submit blocks as source block completed, %p %p len:%d ver:%" PRId64, id, pSubmit,
           pSubmit->submit.msgStr, pSubmit->submit.msgLen, pSubmit->submit.ver);
    ASSERT((*pVer) <= pSubmit->submit.ver);
    (*pVer) = pSubmit->submit.ver;

  } else if (pItem->type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    const SStreamDataBlock* pBlock = (const SStreamDataBlock*)pInput;

    SArray* pBlockList = pBlock->blocks;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    stDebug("s-task:%s set sdata blocks as input num:%d, ver:%" PRId64, id, numOfBlocks, pBlock->sourceVer);
    qSetMultiStreamInput(pExecutor, pBlockList->pData, numOfBlocks, STREAM_INPUT__DATA_BLOCK);

  } else if (pItem->type == STREAM_INPUT__MERGED_SUBMIT) {
    const SStreamMergedSubmit* pMerged = (const SStreamMergedSubmit*)pInput;

    SArray* pBlockList = pMerged->submits;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    stDebug("s-task:%s %p set (merged) submit blocks as a batch, numOfBlocks:%d, ver:%" PRId64, id, pTask, numOfBlocks,
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

int32_t streamProcessTransstateBlock(SStreamTask* pTask, SStreamDataBlock* pBlock) {
  const char* id = pTask->id.idStr;
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     level = pTask->info.taskLevel;

  if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SINK) {
    int32_t remain = streamAlignTransferState(pTask);

    if (remain > 0) {
      streamFreeQitem((SStreamQueueItem*)pBlock);
      stDebug("s-task:%s receive upstream trans-state msg, not sent remain:%d", id, remain);
      return 0;
    }
  }

  // dispatch the tran-state block to downstream task immediately
  int32_t type = pTask->outputInfo.type;

  // transfer the ownership of executor state
  if (type == TASK_OUTPUT__FIXED_DISPATCH || type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (level == TASK_LEVEL__SOURCE) {
      stDebug("s-task:%s add transfer-state block into outputQ", id);
    } else {
      stDebug("s-task:%s all upstream tasks send transfer-state block, add transfer-state block into outputQ", id);
      ASSERT(pTask->streamTaskId.taskId != 0 && pTask->info.fillHistory == 1);
    }

    // agg task should dispatch trans-state msg to sink task, to flush all data to sink task.
    if (level == TASK_LEVEL__AGG || level == TASK_LEVEL__SOURCE) {
      pBlock->srcVgId = pTask->pMeta->vgId;
      code = taosWriteQitem(pTask->outputq.queue->pQueue, pBlock);
      if (code == 0) {
        streamDispatchStreamBlock(pTask);
      } else {  // todo put into queue failed, retry
        streamFreeQitem((SStreamQueueItem*)pBlock);
      }
    } else {  // level == TASK_LEVEL__SINK
      streamFreeQitem((SStreamQueueItem*)pBlock);
    }
  } else {  // non-dispatch task, do task state transfer directly
    streamFreeQitem((SStreamQueueItem*)pBlock);
    stDebug("s-task:%s non-dispatch task, level:%d start to transfer state directly", id, level);
    ASSERT(pTask->info.fillHistory == 1);

    code = streamTransferStateToStreamTask(pTask);
    if (code != TSDB_CODE_SUCCESS) {
      /*int8_t status = */ streamTaskSetSchedStatusInactive(pTask);
    }
  }

  return code;
}

static void setTaskSchedInfo(SStreamTask* pTask, int32_t idleTime) { pTask->status.schedIdleTime = idleTime; }
static void clearTaskSchedInfo(SStreamTask* pTask) { pTask->status.schedIdleTime = 0; }
static void setLastExecTs(SStreamTask* pTask, int64_t ts) { pTask->status.lastExecTs = ts; }

/**
 * todo: the batch of blocks should be tuned dynamic, according to the total elapsed time of each batch of blocks, the
 * appropriate batch of blocks should be handled in 5 to 10 sec.
 */
int32_t doStreamExecTask(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  // merge multiple input data if possible in the input queue.
  stDebug("s-task:%s start to extract data block from inputQ", id);

  while (1) {
    int32_t           blockSize = 0;
    int32_t           numOfBlocks = 0;
    SStreamQueueItem* pInput = NULL;

    if (streamTaskShouldStop(pTask) || (streamTaskGetStatus(pTask)->state == TASK_STATUS__UNINIT)) {
      stDebug("s-task:%s stream task is stopped", id);
      return 0;
    }

    if (streamQueueIsFull(pTask->outputq.queue)) {
      stWarn("s-task:%s outputQ is full, idle for 500ms and retry", id);
      setTaskSchedInfo(pTask, 500);
      return 0;
    }

    if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED) {
      stWarn("s-task:%s downstream task inputQ blocked, idle for 1sec and retry", id);
      setTaskSchedInfo(pTask, 1000);
      return 0;
    }

    if (taosGetTimestampMs() - pTask->status.lastExecTs < 50) {
      stDebug("s-task:%s invoke with high frequency, idle and retry exec in 50ms", id);
      setTaskSchedInfo(pTask, 50);
      return 0;
    }

    /*int32_t code = */ streamTaskGetDataFromInputQ(pTask, &pInput, &numOfBlocks, &blockSize);
    if (pInput == NULL) {
      ASSERT(numOfBlocks == 0);
      return 0;
    }

    // dispatch checkpoint msg to all downstream tasks
    int32_t type = pInput->type;
    if (type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
      streamProcessCheckpointBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    if (type == STREAM_INPUT__TRANS_STATE) {
      streamProcessTransstateBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
      ASSERT(type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__CHECKPOINT);

      // here only handle the data block sink operation
      if (type == STREAM_INPUT__DATA_BLOCK) {
        pTask->execInfo.sink.dataSize += blockSize;
        stDebug("s-task:%s sink task start to sink %d blocks, size:%.2fKiB", id, numOfBlocks, SIZE_IN_KiB(blockSize));
        doOutputResultBlockImpl(pTask, (SStreamDataBlock*)pInput);
        continue;
      }
    }

    int64_t st = taosGetTimestampMs();

    const SStreamQueueItem* pItem = pInput;
    stDebug("s-task:%s start to process batch of blocks, num:%d, type:%d", id, numOfBlocks, pItem->type);

    int64_t ver = pTask->chkInfo.processedVer;
    doSetStreamInputBlock(pTask, pInput, &ver, id);

    int64_t resSize = 0;
    int32_t totalBlocks = 0;
    streamTaskExecImpl(pTask, pInput, &resSize, &totalBlocks);

    double el = (taosGetTimestampMs() - st) / 1000.0;
    stDebug("s-task:%s batch of input blocks exec end, elapsed time:%.2fs, result size:%.2fMiB, numOfBlocks:%d", id, el,
           SIZE_IN_MiB(resSize), totalBlocks);

    SCheckpointInfo* pInfo = &pTask->chkInfo;

    // update the currentVer if processing the submit blocks.
    ASSERT(pInfo->checkpointVer <= pInfo->nextProcessVer && ver >= pInfo->checkpointVer);

    if (ver != pInfo->processedVer) {
      stDebug("s-task:%s update processedVer(unsaved) from %" PRId64 " to %" PRId64 " nextProcessVer:%" PRId64
              " ckpt:%" PRId64,
              id, pInfo->processedVer, ver, pInfo->nextProcessVer, pInfo->checkpointVer);
      pInfo->processedVer = ver;
    }

    streamFreeQitem(pInput);

    // todo other thread may change the status
    // do nothing after sync executor state to storage backend, untill the vnode-level checkpoint is completed.
    if (type == STREAM_INPUT__CHECKPOINT) {

      // todo add lock
      SStreamTaskState* pState = streamTaskGetStatus(pTask);
      if (pState->state == TASK_STATUS__CK) {
        stDebug("s-task:%s checkpoint block received, set status:%s", id, pState->name);
        streamTaskBuildCheckpoint(pTask);
      } else {
        // todo refactor
        int32_t code = 0;
        if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
          code = streamTaskSendCheckpointSourceRsp(pTask);
        } else {
          code = streamTaskSendCheckpointReadyMsg(pTask);
        }

        if (code != TSDB_CODE_SUCCESS) {
          // todo: let's retry send rsp to upstream/mnode
          stError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%d, code:%s", id, 0,
                  tstrerror(code));
        }
      }

      return 0;
    }
  }

  return 0;
}

// the task may be set dropping/stopping, while it is still in the task queue, therefore, the sched-status can not
// be updated by tryExec function, therefore, the schedStatus will always be the TASK_SCHED_STATUS__WAITING.
bool streamTaskIsIdle(const SStreamTask* pTask) {
  ETaskStatus status = streamTaskGetStatus(pTask)->state;
  return (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE || status == TASK_STATUS__STOP ||
          status == TASK_STATUS__DROPPING);
}

bool streamTaskReadyToRun(const SStreamTask* pTask, char** pStatus) {
  SStreamTaskState* pState = streamTaskGetStatus(pTask);

  ETaskStatus st = pState->state;
  if (pStatus != NULL) {
    *pStatus = pState->name;
  }

  // pause & halt will still run for sink tasks.
  if (streamTaskIsSinkTask(pTask)) {
    return (st == TASK_STATUS__READY || st == TASK_STATUS__SCAN_HISTORY || st == TASK_STATUS__CK ||
            st == TASK_STATUS__PAUSE || st == TASK_STATUS__HALT);
  } else {
    return (st == TASK_STATUS__READY || st == TASK_STATUS__SCAN_HISTORY || st == TASK_STATUS__CK ||
            st == TASK_STATUS__HALT);
  }
}

static void doStreamExecTaskHelper(void* param, void* tmrId) {
  SStreamTask* pTask = (SStreamTask*)param;

  SStreamTaskState* p = streamTaskGetStatus(pTask);
  if (p->state == TASK_STATUS__DROPPING || p->state == TASK_STATUS__STOP) {
    streamTaskSetSchedStatusInactive(pTask);

    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s status:%s not resume task, ref:%d", pTask->id.idStr, p->name, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  // task resume running
  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    /*int8_t status = */streamTaskSetSchedStatusInactive(pTask);

    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stError("failed to create msg to resume s-task:%s, reason out of memory, ref:%d", pTask->id.idStr, ref);

    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  pRunReq->head.vgId = pTask->info.nodeId;
  pRunReq->streamId = pTask->id.streamId;
  pRunReq->taskId = pTask->id.taskId;
  pRunReq->reqType = STREAM_EXEC_T_RESUME_TASK;

  int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
  stDebug("trigger to resume s-task:%s after being idled for %dms, ref:%d", pTask->id.idStr, pTask->status.schedIdleTime, ref);

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(pTask->pMsgCb, STREAM_QUEUE, &msg);

  // release the task ref count
  clearTaskSchedInfo(pTask);
  streamMetaReleaseTask(pTask->pMeta, pTask);
}

static int32_t schedTaskInFuture(SStreamTask* pTask) {
  int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
  stDebug("s-task:%s task should idle, add into timer to retry in %dms, ref:%d", pTask->id.idStr,
          pTask->status.schedIdleTime, ref);

  // add one ref count for task
  /*SStreamTask* pAddRefTask = */streamMetaAcquireOneTask(pTask);

  if (pTask->schedInfo.pIdleTimer == NULL) {
    pTask->schedInfo.pIdleTimer = taosTmrStart(doStreamExecTaskHelper, pTask->status.schedIdleTime, pTask, streamTimer);
  } else {
    taosTmrReset(doStreamExecTaskHelper, pTask->status.schedIdleTime, pTask, streamTimer, &pTask->schedInfo.pIdleTimer);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamResumeTask(SStreamTask* pTask) {
  ASSERT(pTask->status.schedStatus == TASK_SCHED_STATUS__ACTIVE);
  const char* id = pTask->id.idStr;

  while (1) {
    /*int32_t code = */ doStreamExecTask(pTask);

    // check if continue
    taosThreadMutexLock(&pTask->lock);

    int32_t numOfItems = streamQueueGetNumOfItems(pTask->inputq.queue);
    if ((numOfItems == 0) || streamTaskShouldStop(pTask) || streamTaskShouldPause(pTask)) {
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
      clearTaskSchedInfo(pTask);
      taosThreadMutexUnlock(&pTask->lock);

      setLastExecTs(pTask, taosGetTimestampMs());

      char* p = streamTaskGetStatus(pTask)->name;
      stDebug("s-task:%s exec completed, status:%s, sched-status:%d, lastExecTs:%" PRId64, id, p,
              pTask->status.schedStatus, pTask->status.lastExecTs);

      return 0;
    } else {
      // check if this task needs to be idle for a while
      if (pTask->status.schedIdleTime > 0) {
        schedTaskInFuture(pTask);

        taosThreadMutexUnlock(&pTask->lock);
        setLastExecTs(pTask, taosGetTimestampMs());
        return 0;
      }
    }

    taosThreadMutexUnlock(&pTask->lock);
  }
}

int32_t streamExecTask(SStreamTask* pTask) {
  // this function may be executed by multi-threads, so status check is required.
  const char* id = pTask->id.idStr;

  int8_t schedStatus = streamTaskSetSchedStatusActive(pTask);
  if (schedStatus == TASK_SCHED_STATUS__WAITING) {
    streamResumeTask(pTask);
  } else {
    char* p = streamTaskGetStatus(pTask)->name;
    stDebug("s-task:%s already started to exec by other thread, status:%s, sched-status:%d", id, p,
            pTask->status.schedStatus);
  }

  return 0;
}

int32_t streamTaskReleaseState(SStreamTask* pTask) {
  stDebug("s-task:%s release exec state", pTask->id.idStr);
  void* pExecutor = pTask->exec.pExecutor;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pExecutor != NULL) {
    code = qStreamOperatorReleaseState(pExecutor);
  }

  return code;
}

int32_t streamTaskReloadState(SStreamTask* pTask) {
  stDebug("s-task:%s reload exec state", pTask->id.idStr);
  void* pExecutor = pTask->exec.pExecutor;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pExecutor != NULL) {
    code = qStreamOperatorReloadState(pExecutor);
  }

  return code;
}

int32_t streamAlignTransferState(SStreamTask* pTask) {
  int32_t numOfUpstream = taosArrayGetSize(pTask->upstreamInfo.pList);
  int32_t old = atomic_val_compare_exchange_32(&pTask->transferStateAlignCnt, 0, numOfUpstream);
  if (old == 0) {
    stDebug("s-task:%s set the transfer state aligncnt %d", pTask->id.idStr, numOfUpstream);
  }

  return atomic_sub_fetch_32(&pTask->transferStateAlignCnt, 1);
}
