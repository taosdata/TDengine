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
static bool    taskReadyForDataFromWal(SStreamTask* pTask);
static bool    doPutDataIntoInputQ(SStreamTask* pTask, int64_t maxVer, int32_t* numOfItems);
static int32_t tqScanWalInFuture(STQ* pTq, int32_t numOfTasks, int32_t idleDuration);

// extract data blocks(submit/delete) from WAL, and add them into the input queue for all the sources tasks.
int32_t tqScanWal(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = pMeta->vgId;
  int64_t      st = taosGetTimestampMs();

  tqDebug("vgId:%d continue to check if data in wal are available, scanCounter:%d", vgId, pMeta->scanInfo.scanCounter);

  // check all tasks
  int32_t numOfTasks = 0;
  bool shouldIdle = true;

  int32_t code = doScanWalForAllTasks(pMeta, &shouldIdle);
  if (code) {
    tqError("vgId:%d failed to start all tasks, try next time", vgId);
    return code;
  }

  streamMetaWLock(pMeta);
  int32_t times = (--pMeta->scanInfo.scanCounter);
  if (times < 0) {
    tqError("vgId:%d invalid scan counter:%d, reset to 0", vgId, times);
    times = 0;
  }

  numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  streamMetaWUnLock(pMeta);

  int64_t el = (taosGetTimestampMs() - st);
  tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%" PRId64 " ms", vgId, el);

  if (times > 0) {
    tqDebug("vgId:%d scan wal for stream tasks for %d times in %dms", vgId, times, SCAN_WAL_IDLE_DURATION);
    code = tqScanWalInFuture(pTq, numOfTasks, SCAN_WAL_IDLE_DURATION);
    if (code) {
      tqError("vgId:%d sched scan wal in %dms failed, ignore this failure", vgId, SCAN_WAL_IDLE_DURATION);
    }
  }

  return code;
}

typedef struct SBuildScanWalMsgParam {
  STQ*    pTq;
  int32_t numOfTasks;
} SBuildScanWalMsgParam;

static void doStartScanWal(void* param, void* tmrId) {
  SBuildScanWalMsgParam* pParam = (SBuildScanWalMsgParam*)param;

  STQ*    pTq = pParam->pTq;
  int32_t vgId = pTq->pStreamMeta->vgId;
  tqDebug("vgId:%d create msg to start wal scan, numOfTasks:%d, vnd restored:%d", vgId, pParam->numOfTasks,
          pTq->pVnode->restored);

  int32_t code = streamTaskSchedTask(&pTq->pVnode->msgCb, vgId, 0, 0, STREAM_EXEC_T_EXTRACT_WAL_DATA);
  taosMemoryFree(pParam);

  if (code) {
    tqError("vgId:%d failed sched task to scan wal, code:%s", vgId, tstrerror(code));
  }
}

int32_t tqScanWalInFuture(STQ* pTq, int32_t numOfTasks, int32_t idleDuration) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      code = 0;
  int32_t      vgId = TD_VID(pTq->pVnode);

  SBuildScanWalMsgParam* pParam = taosMemoryMalloc(sizeof(SBuildScanWalMsgParam));
  if (pParam == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pParam->pTq = pTq;
  pParam->numOfTasks = numOfTasks;

  tmr_h pTimer = NULL;
  code = streamTimerGetInstance(&pTimer);
  if (code) {
    tqError("vgId:%d failed to get tmr ctrl during sched scan wal", vgId);
    return code;
  }

  if (pMeta->scanInfo.scanTimer == NULL) {
    pMeta->scanInfo.scanTimer = taosTmrStart(doStartScanWal, idleDuration, pParam, pTimer);
  } else {
    bool ret = taosTmrReset(doStartScanWal, idleDuration, pParam, pTimer, &pMeta->scanInfo.scanTimer);
    if (!ret) {
      tqError("vgId:%d failed to start scan wal in:%dms", vgId, idleDuration);
    }
  }

  return code;
}

int32_t tqScanWalAsync(STQ* pTq, bool ckPause) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  bool alreadyRestored = pTq->pVnode->restored;

  // do not launch the stream tasks, if it is a follower or not restored vnode.
  if (!(vnodeIsRoleLeader(pTq->pVnode) && alreadyRestored)) {
    return TSDB_CODE_SUCCESS;
  }

  streamMetaWLock(pMeta);

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqDebug("vgId:%d no stream tasks existed to run", vgId);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  if (pMeta->startInfo.startAllTasks) {
    tqTrace("vgId:%d in restart procedure, not scan wal", vgId);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  pMeta->scanInfo.scanCounter += 1;
  if (pMeta->scanInfo.scanCounter > MAX_REPEAT_SCAN_THRESHOLD) {
    pMeta->scanInfo.scanCounter = MAX_REPEAT_SCAN_THRESHOLD;
  }

  if (pMeta->scanInfo.scanCounter > 1) {
    tqDebug("vgId:%d wal read task has been launched, remain scan times:%d", vgId, pMeta->scanInfo.scanCounter);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  int32_t numOfPauseTasks = pMeta->numOfPausedTasks;
  if (ckPause && numOfTasks == numOfPauseTasks) {
    tqDebug("vgId:%d ignore all submit, all streams had been paused, reset the walScanCounter", vgId);

    // reset the counter value, since we do not launch the scan wal operation.
    pMeta->scanInfo.scanCounter = 0;
    streamMetaWUnLock(pMeta);
    return 0;
  }

  tqDebug("vgId:%d create msg to start wal scan to launch stream tasks, numOfTasks:%d, vnd restored:%d", vgId,
          numOfTasks, alreadyRestored);

  int32_t code = streamTaskSchedTask(&pTq->pVnode->msgCb, vgId, 0, 0, STREAM_EXEC_T_EXTRACT_WAL_DATA);
  streamMetaWUnLock(pMeta);

  return code;
}

int32_t tqStopStreamTasksAsync(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = pMeta->vgId;
  return streamTaskSchedTask(&pTq->pVnode->msgCb, vgId, 0, 0, STREAM_EXEC_T_STOP_ALL_TASKS);
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
  int64_t     maxVer = pTask->step2Range.maxVer;

  if ((pTask->info.fillHistory == 1) && ver > maxVer) {
    if (!pTask->status.appendTranstateBlock) {
      qWarn("s-task:%s fill-history scan WAL, nextProcessVer:%" PRId64 " out of the maximum ver:%" PRId64
            ", not scan wal anymore, add transfer-state block into inputQ",
            id, ver, maxVer);

      double el = (taosGetTimestampMs() - pTask->execInfo.step2Start) / 1000.0;
      qDebug("s-task:%s scan-history from WAL stage(step 2) ended, range:%" PRId64 "-%" PRId64 ", elapsed time:%.2fs",
             id, pTask->step2Range.minVer, maxVer, el);
      int32_t code = streamTaskPutTranstateIntoInputQ(pTask);
      if (code) {
        qError("s-task:%s failed to put trans-state into inputQ", id);
      }

      return true;
    } else {
      qWarn("s-task:%s fill-history scan WAL, nextProcessVer:%" PRId64 " out of the ver range:%" PRId64 "-%" PRId64
            ", not scan wal",
            id, ver, pTask->step2Range.minVer, maxVer);
    }
  }

  return false;
}

bool taskReadyForDataFromWal(SStreamTask* pTask) {
  // non-source or fill-history tasks don't need to response the WAL scan action.
  if ((pTask->info.taskLevel != TASK_LEVEL__SOURCE) || (pTask->status.downstreamReady == 0)) {
    return false;
  }

  // not in ready state, do not handle the data from wal
  SStreamTaskState pState = streamTaskGetStatus(pTask);
  if (pState.state != TASK_STATUS__READY) {
    tqTrace("s-task:%s not ready for submit block in wal, status:%s", pTask->id.idStr, pState.name);
    return false;
  }

  // fill-history task has entered into the last phase, no need to anything
  if ((pTask->info.fillHistory == 1) && pTask->status.appendTranstateBlock) {
    // the maximum version of data in the WAL has reached already, the step2 is done
    tqDebug("s-task:%s fill-history reach the maximum ver:%" PRId64 ", not scan wal anymore", pTask->id.idStr,
            pTask->dataRange.range.maxVer);
    return false;
  }

  // check if input queue is full or not
  if (streamQueueIsFull(pTask->inputq.queue)) {
    tqTrace("s-task:%s input queue is full, do nothing", pTask->id.idStr);
    return false;
  }

  // the input queue of downstream task is full, so the output is blocked, stopped for a while
  if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED) {
    tqDebug("s-task:%s inputQ is blocked, do nothing", pTask->id.idStr);
    return false;
  }

  return true;
}

bool doPutDataIntoInputQ(SStreamTask* pTask, int64_t maxVer, int32_t* numOfItems) {
  const char* id = pTask->id.idStr;
  int32_t     numOfNewItems = 0;

  while (1) {
    if ((pTask->info.fillHistory == 1) && pTask->status.appendTranstateBlock) {
      *numOfItems += numOfNewItems;
      return numOfNewItems > 0;
    }

    SStreamQueueItem* pItem = NULL;
    int32_t           code = extractMsgFromWal(pTask->exec.pWalReader, (void**)&pItem, maxVer, id);
    if (code != TSDB_CODE_SUCCESS || pItem == NULL) {  // failed, continue
      int64_t currentVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
      bool    itemInFillhistory = handleFillhistoryScanComplete(pTask, currentVer);
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
        tqDebug("s-task:%s set ver:%" PRId64 " for reader after extract data from WAL", id, ver);

        bool itemInFillhistory = handleFillhistoryScanComplete(pTask, ver);
        if (itemInFillhistory) {
          break;
        }
      } else {
        tqTrace("s-task:%s append input queue failed, code:too many items, ver:%" PRId64, id, pTask->chkInfo.nextProcessVer);
        code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.nextProcessVer);
        if (code) {
          tqError("s-task:%s failed to seek ver to:%"PRId64 " in wal", id, pTask->chkInfo.nextProcessVer);
        }

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
  streamMetaWLock(pStreamMeta);
  pTaskList = taosArrayDup(pStreamMeta->pTaskList, NULL);
  streamMetaWUnLock(pStreamMeta);
  if (pTaskList == NULL) {
    return terrno;
  }

  tqDebug("vgId:%d start to check wal to extract new submit block for %d tasks", vgId, numOfTasks);

  // update the new task number
  numOfTasks = taosArrayGetSize(pTaskList);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId*     pTaskId = taosArrayGet(pTaskList, i);
    if (pTaskId == NULL) {
      continue;
    }

    SStreamTask* pTask = NULL;
    int32_t code = streamMetaAcquireTask(pStreamMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if (pTask == NULL || code != 0) {
      continue;
    }

    if (!taskReadyForDataFromWal(pTask)) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    *pScanIdle = false;

    // seek the stored version and extract data from WAL
    code = setWalReaderStartOffset(pTask, vgId);
    if (code != TSDB_CODE_SUCCESS) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    int32_t numOfItems = streamQueueGetNumOfItems(pTask->inputq.queue);
    int64_t maxVer = (pTask->info.fillHistory == 1) ? pTask->step2Range.maxVer : INT64_MAX;

    streamMutexLock(&pTask->lock);

    SStreamTaskState pState = streamTaskGetStatus(pTask);
    if (pState.state != TASK_STATUS__READY) {
      tqDebug("s-task:%s not ready for submit block from wal, status:%s", pTask->id.idStr, pState.name);
      streamMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    bool hasNewData = doPutDataIntoInputQ(pTask, maxVer, &numOfItems);
    streamMutexUnlock(&pTask->lock);

    if ((numOfItems > 0) || hasNewData) {
      noDataInWal = false;
      code = streamTrySchedExec(pTask);
      if (code != TSDB_CODE_SUCCESS) {
        streamMetaReleaseTask(pStreamMeta, pTask);
        taosArrayDestroy(pTaskList);
        return code;
      }
    }

    streamMetaReleaseTask(pStreamMeta, pTask);
  }

  // all wal are checked, and no new data available in wal.
  if (noDataInWal) {
    *pScanIdle = true;
  }

  taosArrayDestroy(pTaskList);
  return TSDB_CODE_SUCCESS;
}
