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
#define MAX_STREAM_RESULT_DUMP_THRESHOLD 100

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

    int32_t code = streamTaskOutputResultBlock(pTask, pStreamBlocks);
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

static int32_t streamTaskExecImpl(SStreamTask* pTask, SStreamQueueItem* pItem, int64_t* totalSize, int32_t* totalBlocks) {
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
    }

    SSDataBlock block = {0};
    assignOneDataBlock(&block, output);
    block.info.childId = pTask->info.selfChildId;

    size += blockDataGetSize(output) + sizeof(SSDataBlock) + sizeof(SColumnInfoData) * blockDataGetNumOfCols(&block);
    numOfBlocks += 1;

    taosArrayPush(pRes, &block);

    qDebug("s-task:%s (child %d) executed and get %d result blocks, size:%.2fMiB", pTask->id.idStr,
           pTask->info.selfChildId, numOfBlocks, size / 1048576.0);

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

  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);
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
      if (streamTaskShouldStop(&pTask->status)) {
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
          if (streamTaskShouldPause(&pTask->status)) {
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
            return 0;
          }
        }
        break;
      }

      SSDataBlock block = {0};
      assignOneDataBlock(&block, output);
      block.info.childId = pTask->info.selfChildId;
      taosArrayPush(pRes, &block);

      batchCnt++;

      qDebug("s-task:%s scan exec numOfBlocks:%d, limit:%d", pTask->id.idStr, batchCnt, batchSz);
      if (batchCnt >= batchSz) {
        break;
      }
    }

    if (taosArrayGetSize(pRes) == 0) {
      taosArrayDestroy(pRes);

      if (finished) {
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

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    ASSERT(((SStreamQueueItem*)pItem)->type == STREAM_INPUT__DATA_BLOCK);
    streamTaskOutputResultBlock(pTask, (SStreamDataBlock*)pItem);
  }

  // exec impl

  // output
  // try dispatch
  return 0;
}
#endif

static int32_t getNumOfItemsInputQ(const SStreamTask* pTask) {
  int32_t numOfItems1 = taosQueueItemSize(pTask->inputQueue->queue);
  int32_t numOfItems2 = taosQallItemSize(pTask->inputQueue->qall);

  return numOfItems1 + numOfItems2;
}

static void waitForTaskIdle(SStreamTask* pTask, SStreamTask* pStreamTask) {
  // wait for the stream task to be idle
  int64_t st = taosGetTimestampMs();

  while (!streamTaskIsIdle(pStreamTask)) {
    qDebug("s-task:%s level:%d wait for stream task:%s to be idle, check again in 100ms", pTask->id.idStr,
           pTask->info.taskLevel, pStreamTask->id.idStr);
    taosMsleep(100);
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  if (el > 0) {
    qDebug("s-task:%s wait for stream task:%s for %.2fs to handle all data in inputQ", pTask->id.idStr,
           pStreamTask->id.idStr, el);
  }
}

static int32_t streamTransferStateToStreamTask(SStreamTask* pTask) {
  SStreamTask* pStreamTask = streamMetaAcquireTask(pTask->pMeta, pTask->streamTaskId.taskId);
  if (pStreamTask == NULL) {
    qError("s-task:%s failed to find related stream task:0x%x, it may have been destroyed or closed",
        pTask->id.idStr, pTask->streamTaskId.taskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  } else {
    qDebug("s-task:%s fill-history task end, update related stream task:%s info, transfer exec state", pTask->id.idStr,
           pStreamTask->id.idStr);
  }

  pTask->status.transferState = false;  // reset this value, to avoid transfer state again

  ASSERT(pStreamTask != NULL && pStreamTask->historyTaskId.taskId == pTask->id.taskId);
  STimeWindow* pTimeWindow = &pStreamTask->dataRange.window;

  // It must be halted for a source stream task, since when the related scan-history-data task start scan the history
  // for the step 2. For a agg task
  if (pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    ASSERT(pStreamTask->status.taskStatus == TASK_STATUS__HALT);
  } else {
    ASSERT(pStreamTask->status.taskStatus == TASK_STATUS__NORMAL);
    pStreamTask->status.taskStatus = TASK_STATUS__HALT;
    qDebug("s-task:%s status: halt by related fill history task:%s", pStreamTask->id.idStr, pTask->id.idStr);
  }

  // wait for the stream task to be idle
  waitForTaskIdle(pTask, pStreamTask);

  // In case of sink tasks, no need to be halted for them.
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

  // expand the query time window for stream scanner
  pTimeWindow->skey = INT64_MIN;
  qResetStreamInfoTimeWindow(pStreamTask->exec.pExecutor);

  // transfer the ownership of executor state
  streamTaskReleaseState(pTask);
  streamTaskReloadState(pStreamTask);

  streamSetStatusNormal(pStreamTask);

  streamSchedExec(pStreamTask);
  streamMetaReleaseTask(pTask->pMeta, pStreamTask);
  return TSDB_CODE_SUCCESS;
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

/**
 * todo: the batch of blocks should be tuned dynamic, according to the total elapsed time of each batch of blocks, the
 * appropriate batch of blocks should be handled in 5 to 10 sec.
 */
int32_t streamExecForAll(SStreamTask* pTask) {
  const char* id = pTask->id.idStr;

  while (1) {
    int32_t           numOfBlocks = 0;
    SStreamQueueItem* pInput = NULL;

    // merge multiple input data if possible in the input queue.
    extractBlocksFromInputQ(pTask, &pInput, &numOfBlocks);
    if (pInput == NULL) {
      ASSERT(numOfBlocks == 0);
      if (pTask->info.fillHistory && pTask->status.transferState) {
        int32_t code = streamTransferStateToStreamTask(pTask);
        if (code != TSDB_CODE_SUCCESS) { // todo handle this
          return 0;
        }
      }

      // no data in the inputQ, return now
      return 0;
    }

    int32_t type = pInput->type;

    // dispatch checkpoint msg to all downstream tasks
    if (type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
      streamProcessCheckpointBlock(pTask, (SStreamDataBlock*)pInput);
      continue;
    }

    if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
      ASSERT(type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__CHECKPOINT);

      if (type == STREAM_INPUT__DATA_BLOCK) {
        qDebug("s-task:%s sink task start to sink %d blocks", id, numOfBlocks);
        streamTaskOutputResultBlock(pTask, (SStreamDataBlock*)pInput);
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
           resSize / 1048576.0, totalBlocks);

    // update the currentVer if processing the submit blocks.
    ASSERT(pTask->chkInfo.checkpointVer <= pTask->chkInfo.currentVer && ver >= pTask->chkInfo.checkpointVer);

    if (ver != pTask->chkInfo.checkpointVer) {
      qDebug("s-task:%s update checkpointVer(unsaved) from %" PRId64 " to %" PRId64, pTask->id.idStr,
             pTask->chkInfo.checkpointVer, ver);
      pTask->chkInfo.checkpointVer = ver;
    }

    streamFreeQitem(pInput);

    // do nothing after sync executor state to storage backend, untill the vnode-level checkpoint is completed.
    if (type == STREAM_INPUT__CHECKPOINT) {
      ASSERT(pTask->status.taskStatus == TASK_STATUS__CK);
      pTask->status.taskStatus = TASK_STATUS__CK_READY;
      return 0;
    }
  }
}

bool streamTaskIsIdle(const SStreamTask* pTask) {
  int32_t numOfItems = taosQueueItemSize(pTask->inputQueue->queue);
  if (numOfItems > 0) {
    return false;
  }

  numOfItems = taosQallItemSize(pTask->inputQueue->qall);
  if (numOfItems > 0) {
    return false;
  }

  // blocked by downstream task
  if (pTask->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    return false;
  }

  return (pTask->status.schedStatus == TASK_SCHED_STATUS__INACTIVE);
}

int32_t streamTryExec(SStreamTask* pTask) {
  // this function may be executed by multi-threads, so status check is required.
  int8_t schedStatus =
      atomic_val_compare_exchange_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__WAITING, TASK_SCHED_STATUS__ACTIVE);

  if (schedStatus == TASK_SCHED_STATUS__WAITING) {
    int32_t code = streamExecForAll(pTask);
    if (code < 0) {  // todo this status shoudl be removed
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__FAILED);
      return -1;
    }

    atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
    qDebug("s-task:%s exec completed, status:%s, sched-status:%d", pTask->id.idStr,
           streamGetTaskStatusStr(pTask->status.taskStatus), pTask->status.schedStatus);

    if (pTask->status.taskStatus == TASK_STATUS__CK_READY) {
      // check for all tasks, and do generate the vnode-wide checkpoint data.
      // todo extract method
      SStreamMeta* pMeta = pTask->pMeta;
      int32_t      remain = atomic_sub_fetch_32(&pMeta->chkptNotReadyTasks, 1);
      ASSERT(remain >= 0);

      if (remain == 0) {  // all tasks are in TASK_STATUS__CK_READY state
        streamBackendDoCheckpoint(pMeta, pTask->checkpointingId);
        streamSaveTasks(pMeta, pTask->checkpointingId);
        qDebug("vgId:%d vnode wide checkpoint completed, save all tasks status, checkpointId:%" PRId64, pMeta->vgId,
               pTask->checkpointingId);
      }

      // send check point response to upstream task
      if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
        code = streamTaskSendCheckpointSourceRsp(pTask);
      } else {
        code = streamTaskSendCheckpointReadyMsg(pTask);
      }

      if (code != TSDB_CODE_SUCCESS) {
        // todo: let's retry send rsp to upstream/mnode
        qError("s-task:%s failed to send checkpoint rsp to upstream, checkpointId:%" PRId64 ", code:%s",
               pTask->id.idStr, pTask->checkpointingId, tstrerror(code));
      }
    } else {
      if (!taosQueueEmpty(pTask->inputQueue->queue) && (!streamTaskShouldStop(&pTask->status)) &&
          (!streamTaskShouldPause(&pTask->status))) {
        streamSchedExec(pTask);
      }
    }
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
