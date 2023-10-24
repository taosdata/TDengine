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

#include "tq.h"
#include "vnd.h"

#define MAX_REPEAT_SCAN_THRESHOLD  3
#define SCAN_WAL_IDLE_DURATION     100

static int32_t doScanWalForAllTasks(SStreamMeta* pStreamMeta, bool* pScanIdle);
static int32_t setWalReaderStartOffset(SStreamTask* pTask, int32_t vgId);
static bool    handleFillhistoryScanComplete(SStreamTask* pTask, int64_t ver);

// extract data blocks(submit/delete) from WAL, and add them into the input queue for all the sources tasks.
int32_t tqScanWal(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int64_t      st = taosGetTimestampMs();

  while (1) {
    int32_t scan = pMeta->walScanCounter;
    tqDebug("vgId:%d continue check if data in wal are available, walScanCounter:%d", vgId, scan);

    // check all tasks
    bool shouldIdle = true;
    doScanWalForAllTasks(pTq->pStreamMeta, &shouldIdle);

    if (shouldIdle) {
      taosWLockLatch(&pMeta->lock);

      int32_t times = (--pMeta->walScanCounter);
      ASSERT(pMeta->walScanCounter >= 0);

      if (pMeta->walScanCounter <= 0) {
        taosWUnLockLatch(&pMeta->lock);
        break;
      }

      taosWUnLockLatch(&pMeta->lock);
      tqDebug("vgId:%d scan wal for stream tasks for %d times in %dms", vgId, times, SCAN_WAL_IDLE_DURATION);
    }

    taosMsleep(SCAN_WAL_IDLE_DURATION);
  }

  int64_t el = (taosGetTimestampMs() - st);
  tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%" PRId64 " ms", vgId, el);
  return 0;
}

int32_t tqStartStreamTask(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  tqDebug("vgId:%d start to check all %d stream task(s) downstream status", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pTaskList = NULL;
  taosWLockLatch(&pMeta->lock);
  pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  pMeta->startInfo.startTs = taosGetTimestampMs();
  taosWUnLockLatch(&pMeta->lock);

  // broadcast the check downstream tasks msg
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    // fill-history task can only be launched by related stream tasks.
    if (pTask->info.fillHistory == 1) {
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    if (pTask->status.downstreamReady == 1) {
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        tqDebug("s-task:%s downstream ready, no need to check downstream, check only related fill-history task",
                pTask->id.idStr);
        streamLaunchFillHistoryTask(pTask);
      }

      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    EStreamTaskEvent event = (HAS_RELATED_FILLHISTORY_TASK(pTask)) ? TASK_EVENT_INIT_STREAM_SCANHIST : TASK_EVENT_INIT;
    streamTaskHandleEvent(pTask->status.pSM, event);
    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return 0;
}

int32_t tqCheckAndRunStreamTaskAsync(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    return -1;
  }

  tqDebug("vgId:%d check %d stream task(s) status async", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = STREAM_EXEC_TASK_STATUS_CHECK_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  return 0;
}

int32_t tqScanWalAsync(STQ* pTq, bool ckPause) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  // do not launch the stream tasks, if it is a follower or not restored vnode.
  if (!(vnodeIsRoleLeader(pTq->pVnode) && pTq->pVnode->restored)) {
    return TSDB_CODE_SUCCESS;
  }

  taosWLockLatch(&pMeta->lock);

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  pMeta->walScanCounter += 1;
  if (pMeta->walScanCounter > MAX_REPEAT_SCAN_THRESHOLD) {
    pMeta->walScanCounter = MAX_REPEAT_SCAN_THRESHOLD;
  }

  if (pMeta->walScanCounter > 1) {
    tqDebug("vgId:%d wal read task has been launched, remain scan times:%d", vgId, pMeta->walScanCounter);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  int32_t numOfPauseTasks = pTq->pStreamMeta->numOfPausedTasks;
  if (ckPause && numOfTasks == numOfPauseTasks) {
    tqDebug("vgId:%d ignore all submit, all streams had been paused, reset the walScanCounter", vgId);

    // reset the counter value, since we do not launch the scan wal operation.
    pMeta->walScanCounter = 0;
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    taosWUnLockLatch(&pMeta->lock);
    return -1;
  }

  tqDebug("vgId:%d create msg to start wal scan to launch stream tasks, numOfTasks:%d", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = STREAM_EXEC_EXTRACT_DATA_IN_WAL_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  taosWUnLockLatch(&pMeta->lock);

  return 0;
}

int32_t tqStopStreamTasks(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = TD_VID(pTq->pVnode);
  int32_t      numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  tqDebug("vgId:%d stop all %d stream task(s)", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pTaskList = NULL;
  taosWLockLatch(&pMeta->lock);
  pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosWUnLockLatch(&pMeta->lock);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId*   pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    streamTaskStop(pTask);
    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return 0;
}

int32_t tqStartStreamTasks(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = TD_VID(pTq->pVnode);
  int32_t      numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  tqDebug("vgId:%d start all %d stream task(s)", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);

    STaskId id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));

    if ((*pTask)->info.fillHistory != 1) {
      streamTaskResetStatus(*pTask);
    }
  }

  return 0;
}

int32_t setWalReaderStartOffset(SStreamTask* pTask, int32_t vgId) {
  // seek the stored version and extract data from WAL
  int64_t firstVer = walReaderGetValidFirstVer(pTask->exec.pWalReader);
  if (pTask->chkInfo.nextProcessVer < firstVer) {
    tqWarn("vgId:%d s-task:%s ver:%" PRId64 " earlier than the first ver of wal range %" PRId64 ", forward to %" PRId64,
           vgId, pTask->id.idStr, pTask->chkInfo.nextProcessVer, firstVer, firstVer);

    pTask->chkInfo.nextProcessVer = firstVer;

    // todo need retry if failed
    int32_t code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.nextProcessVer);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // append the data for the stream
    tqDebug("vgId:%d s-task:%s wal reader seek to ver:%" PRId64, vgId, pTask->id.idStr, pTask->chkInfo.nextProcessVer);
  } else {
    int64_t currentVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (currentVer == -1) {  // we only seek the read for the first time
      int32_t code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.nextProcessVer);
      if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
        return code;
      }

      // append the data for the stream
      tqDebug("vgId:%d s-task:%s wal reader initial seek to ver:%" PRId64, vgId, pTask->id.idStr,
              pTask->chkInfo.nextProcessVer);
    }
  }

  int64_t skipToVer = walReaderGetSkipToVersion(pTask->exec.pWalReader);
  if (skipToVer != 0 && skipToVer > pTask->chkInfo.nextProcessVer) {
    int32_t code = walReaderSeekVer(pTask->exec.pWalReader, skipToVer);
    if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
      return code;
    }

    tqDebug("vgId:%d s-task:%s wal reader jump to ver:%" PRId64, vgId, pTask->id.idStr, skipToVer);
  }

  return TSDB_CODE_SUCCESS;
}

// todo handle memory error
bool handleFillhistoryScanComplete(SStreamTask* pTask, int64_t ver) {
  const char* id = pTask->id.idStr;
  int64_t     maxVer = pTask->dataRange.range.maxVer;

  if ((pTask->info.fillHistory == 1) && ver > pTask->dataRange.range.maxVer) {
    if (!pTask->status.appendTranstateBlock) {
      qWarn("s-task:%s fill-history scan WAL, nextProcessVer:%" PRId64 " out of the maximum ver:%" PRId64
            ", not scan wal anymore, add transfer-state block into inputQ",
            id, ver, maxVer);

      double el = (taosGetTimestampMs() - pTask->execInfo.step2Start) / 1000.0;
      qDebug("s-task:%s scan-history from WAL stage(step 2) ended, elapsed time:%.2fs", id, el);
      /*int32_t code = */streamTaskPutTranstateIntoInputQ(pTask);
      return true;
    } else {
      qWarn("s-task:%s fill-history scan WAL, nextProcessVer:%" PRId64 " out of the maximum ver:%" PRId64 ", not scan wal",
            id, ver, maxVer);
    }
  }

  return false;
}

static bool taskReadyForDataFromWal(SStreamTask* pTask) {
  // non-source or fill-history tasks don't need to response the WAL scan action.
  if ((pTask->info.taskLevel != TASK_LEVEL__SOURCE) || (pTask->status.downstreamReady == 0)) {
    return false;
  }

  // not in ready state, do not handle the data from wal
//  int32_t status = pTask->status.taskStatus;
  char* p = NULL;
  int32_t status = streamTaskGetStatus(pTask, &p);
  if (streamTaskGetStatus(pTask, &p) != TASK_STATUS__READY) {
    tqTrace("s-task:%s not ready for submit block in wal, status:%s", pTask->id.idStr, p);
    return false;
  }

  // fill-history task has entered into the last phase, no need to anything
  if ((pTask->info.fillHistory == 1) && pTask->status.appendTranstateBlock) {
    ASSERT(status == TASK_STATUS__READY);
    // the maximum version of data in the WAL has reached already, the step2 is done
    tqDebug("s-task:%s fill-history reach the maximum ver:%" PRId64 ", not scan wal anymore", pTask->id.idStr,
            pTask->dataRange.range.maxVer);
    return false;
  }

  // check if input queue is full or not
  if (streamQueueIsFull(pTask->inputInfo.queue)) {
    tqTrace("s-task:%s input queue is full, do nothing", pTask->id.idStr);
    return false;
  }

  // the input queue of downstream task is full, so the output is blocked, stopped for a while
  if (pTask->inputInfo.status == TASK_INPUT_STATUS__BLOCKED) {
    tqDebug("s-task:%s inputQ is blocked, do nothing", pTask->id.idStr);
    return false;
  }

  return true;
}

static bool doPutDataIntoInputQFromWal(SStreamTask* pTask, int64_t maxVer, int32_t* numOfItems) {
  const char* id = pTask->id.idStr;
  int32_t     numOfNewItems = 0;

  while(1) {
    if ((pTask->info.fillHistory == 1) && pTask->status.appendTranstateBlock) {
      *numOfItems += numOfNewItems;
      return numOfNewItems > 0;
    }

    SStreamQueueItem* pItem = NULL;
    int32_t code = extractMsgFromWal(pTask->exec.pWalReader, (void**)&pItem, maxVer, id);
    if (code != TSDB_CODE_SUCCESS || pItem == NULL) {  // failed, continue
      int64_t currentVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
      bool itemInFillhistory = handleFillhistoryScanComplete(pTask, currentVer);
      if (itemInFillhistory) {
        numOfNewItems += 1;
      }
      break;
    }

    if (pItem != NULL) {
      code = streamTaskPutDataIntoInputQ(pTask, pItem);
      if (code == TSDB_CODE_SUCCESS) {
        numOfNewItems += 1;
        int64_t ver = walReaderGetCurrentVer(pTask->exec.pWalReader);
        pTask->chkInfo.nextProcessVer = ver;
        tqDebug("s-task:%s set the ver:%" PRId64 " from WALReader after extract block from WAL", id, ver);

        bool itemInFillhistory = handleFillhistoryScanComplete(pTask, ver);
        if (itemInFillhistory) {
          break;
        }
      } else {
        tqError("s-task:%s append input queue failed, code: too many items, ver:%" PRId64, id, pTask->chkInfo.nextProcessVer);
        break;
      }
    }
  }

  *numOfItems += numOfNewItems;
  return numOfNewItems > 0;
}

int32_t doScanWalForAllTasks(SStreamMeta* pStreamMeta, bool* pScanIdle) {
  *pScanIdle = true;
  bool    noDataInWal = true;
  int32_t vgId = pStreamMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pStreamMeta->pTaskList);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // clone the task list, to avoid the task update during scan wal files
  SArray* pTaskList = NULL;
  taosWLockLatch(&pStreamMeta->lock);
  pTaskList = taosArrayDup(pStreamMeta->pTaskList, NULL);
  taosWUnLockLatch(&pStreamMeta->lock);

  tqDebug("vgId:%d start to check wal to extract new submit block for %d tasks", vgId, numOfTasks);

  // update the new task number
  numOfTasks = taosArrayGetSize(pTaskList);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId*     pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask* pTask = streamMetaAcquireTask(pStreamMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    if (!taskReadyForDataFromWal(pTask)) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    *pScanIdle = false;

    // seek the stored version and extract data from WAL
    int32_t code = setWalReaderStartOffset(pTask, vgId);
    if (code != TSDB_CODE_SUCCESS) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    int32_t numOfItems = streamQueueGetNumOfItems(pTask->inputInfo.queue);
    int64_t maxVer = (pTask->info.fillHistory == 1) ? pTask->dataRange.range.maxVer : INT64_MAX;

    taosThreadMutexLock(&pTask->lock);

    char* p = NULL;
    ETaskStatus status = streamTaskGetStatus(pTask, &p);
    if (status != TASK_STATUS__READY) {
      tqDebug("s-task:%s not ready for submit block from wal, status:%s", pTask->id.idStr, p);
      taosThreadMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    bool hasNewData = doPutDataIntoInputQFromWal(pTask, maxVer, &numOfItems);
    taosThreadMutexUnlock(&pTask->lock);

    if ((numOfItems > 0) || hasNewData) {
      noDataInWal = false;
      code = streamSchedExec(pTask);
      if (code != TSDB_CODE_SUCCESS) {
        streamMetaReleaseTask(pStreamMeta, pTask);
        return -1;
      }
    }

    streamMetaReleaseTask(pStreamMeta, pTask);
  }

  // all wal are checked, and no new data available in wal.
  if (noDataInWal) {
    *pScanIdle = true;
  }

  taosArrayDestroy(pTaskList);
  return 0;
}
