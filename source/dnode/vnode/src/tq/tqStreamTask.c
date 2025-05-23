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

#define SCAN_WAL_IDLE_DURATION    250  // idle for 500ms to do next wal scan
#define SCAN_WAL_WAIT_COUNT       2

typedef struct SBuildScanWalMsgParam {
  int64_t metaId;
  SMsgCb  msgCb;
} SBuildScanWalMsgParam;

static int32_t doScanWalForAllTasks(SStreamMeta* pStreamMeta, int32_t* pNumOfTasks);
static int32_t setWalReaderStartOffset(SStreamTask* pTask, int32_t vgId);
static bool    handleFillhistoryScanComplete(SStreamTask* pTask, int64_t ver);
static bool    taskReadyForDataFromWal(SStreamTask* pTask);
static int32_t doPutDataIntoInputQ(SStreamTask* pTask, int64_t maxVer, int32_t* numOfItems, bool* pSucc);

// extract data blocks(submit/delete) from WAL, and add them into the input queue for all the sources tasks.
int32_t tqScanWal(STQ* pTq) {
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int32_t      vgId = pMeta->vgId;
  int64_t      st = taosGetTimestampMs();
  int32_t      numOfTasks = 0;
  int64_t      el = 0;
  int32_t      code = 0;

  int32_t old = atomic_val_compare_exchange_32(&pMeta->scanInfo.scanSentinel, 0, 1);
  if (old == 0) {
    tqDebug("vgId:%d try to scan wal to extract data", vgId);
  } else {
    tqDebug("vgId:%d already in wal scan, abort", vgId);
    return code;
  }

  // the scan wal interval less than 200, not scan, actually.
  if ((pMeta->scanInfo.lastScanTs > 0) && (st - pMeta->scanInfo.lastScanTs < 200)) {
    tqDebug("vgId:%d scan wal less than 200ms, do nothing", vgId);
    atomic_store_32(&pMeta->scanInfo.scanSentinel, 0);
    return code;
  }

  // check all tasks
  code = doScanWalForAllTasks(pMeta, &numOfTasks);

  pMeta->scanInfo.lastScanTs = taosGetTimestampMs();
  el = (pMeta->scanInfo.lastScanTs - st);

  if (code) {
    tqError("vgId:%d failed to scan wal for all tasks, try next time, elapsed time:%" PRId64 "ms code:%s", vgId, el,
            tstrerror(code));
  } else {
    tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%" PRId64 "ms", vgId, el);
  }

  atomic_store_32(&pMeta->scanInfo.scanSentinel, 0);
  return code;
}

static bool waitEnoughDuration(SStreamMeta* pMeta) {
  if ((++pMeta->scanInfo.tickCounter) >= SCAN_WAL_WAIT_COUNT) {
    pMeta->scanInfo.tickCounter = 0;
    return true;
  }

  return false;
}

static void doStartScanWal(void* param, void* tmrId) {
  int32_t                vgId = 0;
  int32_t                code = 0;
  int32_t                numOfTasks = 0;
  tmr_h                  pTimer = NULL;
  int32_t                numOfItems = 0;
  STQ*                   pTq = NULL;
  SBuildScanWalMsgParam* pParam = (SBuildScanWalMsgParam*)param;

  tqTrace("start to do scan wal in tmr, metaRid:%" PRId64, pParam->metaId);

  SStreamMeta* pMeta = taosAcquireRef(streamMetaRefPool, pParam->metaId);
  if (pMeta == NULL) {
    tqError("metaRid:%" PRId64 " not valid now, stream meta has been freed", pParam->metaId);
    taosMemoryFree(pParam);
    return;
  }

  pTq = pMeta->ahandle;
  vgId = pMeta->vgId;
  code = streamTimerGetInstance(&pTimer);
  if (code) {
    tqFatal("vgId:%d failed to get tmr ctrl during sched scan wal, not scan wal, code:%s", vgId, tstrerror(code));
    taosMemoryFree(pParam);
    return;
  }

  if (pMeta->closeFlag) {
    code = taosReleaseRef(streamMetaRefPool, pParam->metaId);
    if (code == TSDB_CODE_SUCCESS) {
      tqInfo("vgId:%d jump out of scan wal timer since closed", vgId);
    } else {
      tqError("vgId:%d failed to release ref for streamMeta, rid:%" PRId64 " code:%s", vgId, pParam->metaId,
              tstrerror(code));
    }

    taosMemoryFree(pParam);
    return;
  }

  if (pMeta->role != NODE_ROLE_LEADER) {
    tqDebug("vgId:%d not leader, role:%d not scan wal anymore", vgId, pMeta->role);

    code = taosReleaseRef(streamMetaRefPool, pParam->metaId);
    if (code == TSDB_CODE_SUCCESS) {
      tqDebug("vgId:%d jump out of scan wal timer since not leader", vgId);
    } else {
      tqError("vgId:%d failed to release ref for streamMeta, rid:%" PRId64 " code:%s", vgId, pParam->metaId,
              tstrerror(code));
    }

    taosMemFree(pParam);
    return;
  }

  if (pMeta->startInfo.startAllTasks) {
    tqDebug("vgId:%d in restart procedure, not ready to scan wal", vgId);
    goto _end;
  }

  numOfItems = tmsgGetQueueSize(&pTq->pVnode->msgCb, pMeta->vgId, STREAM_QUEUE);
  bool tooMany = (numOfItems > tsThresholdItemsInStreamQueue);

  if (!waitEnoughDuration(pMeta) || tooMany) {
    if (tooMany) {
      tqDebug("vgId:%d %d items (threshold: %d) in stream_queue, not scan wal now", vgId, numOfItems,
              tsThresholdItemsInStreamQueue);
    }

    streamTmrStart(doStartScanWal, SCAN_WAL_IDLE_DURATION, pParam, pTimer, &pMeta->scanInfo.scanTimer, vgId,
                   "scan-wal");
    code = taosReleaseRef(streamMetaRefPool, pParam->metaId);
    if (code) {
      tqError("vgId:%d failed to release ref for streamMeta, rid:%" PRId64 " code:%s", vgId, pParam->metaId,
              tstrerror(code));
    }
    return;
  }

  // failed to lock, try 500ms later
  code = streamMetaTryRlock(pMeta);
  if (code == 0) {
    numOfTasks = taosArrayGetSize(pMeta->pTaskList);
    streamMetaRUnLock(pMeta);
  } else {
    numOfTasks = 0;
  }

  if (numOfTasks > 0) {
    tqDebug("vgId:%d create msg to start wal scan, numOfTasks:%d", vgId, numOfTasks);

#if 0
  //  wait for the vnode is freed, and invalid read may occur.
  taosMsleep(10000);
#endif

    code = streamTaskSchedTask(&pParam->msgCb, vgId, 0, 0, STREAM_EXEC_T_EXTRACT_WAL_DATA, false);
    if (code) {
      tqError("vgId:%d failed sched task to scan wal, code:%s", vgId, tstrerror(code));
    }
  }

_end:
  streamTmrStart(doStartScanWal, SCAN_WAL_IDLE_DURATION, pParam, pTimer, &pMeta->scanInfo.scanTimer, vgId, "scan-wal");
  tqTrace("vgId:%d try scan-wal will start in %dms", vgId, SCAN_WAL_IDLE_DURATION*SCAN_WAL_WAIT_COUNT);

  code = taosReleaseRef(streamMetaRefPool, pParam->metaId);
  if (code) {
    tqError("vgId:%d failed to release ref for streamMeta, rid:%" PRId64 " code:%s", vgId, pParam->metaId,
            tstrerror(code));
  }
}

void tqScanWalAsync(STQ* pTq) {
  SStreamMeta*           pMeta = pTq->pStreamMeta;
  int32_t                code = 0;
  int32_t                vgId = TD_VID(pTq->pVnode);
  tmr_h                  pTimer = NULL;
  SBuildScanWalMsgParam* pParam = NULL;

  // 1. the vnode should be the leader.
  // 2. the stream isn't disabled
  if ((pMeta->role != NODE_ROLE_LEADER) || tsDisableStream) {
    tqInfo("vgId:%d follower node or stream disabled, not scan wal", vgId);
    return;
  }

  pParam = taosMemoryMalloc(sizeof(SBuildScanWalMsgParam));
  if (pParam == NULL) {
    tqError("vgId:%d failed to start scan wal, stream not executes, code:%s", vgId, tstrerror(code));
    return;
  }

  pParam->metaId = pMeta->rid;
  pParam->msgCb = pTq->pVnode->msgCb;

  code = streamTimerGetInstance(&pTimer);
  if (code) {
    tqFatal("vgId:%d failed to get tmr ctrl during sched scan wal", vgId);
    taosMemoryFree(pParam);
  } else {
    streamTmrStart(doStartScanWal, SCAN_WAL_IDLE_DURATION, pParam, pTimer, &pMeta->scanInfo.scanTimer, vgId,
                   "scan-wal");
  }
}

int32_t tqStopStreamAllTasksAsync(SStreamMeta* pMeta, SMsgCb* pMsgCb) {
  return streamTaskSchedTask(pMsgCb, pMeta->vgId, 0, 0, STREAM_EXEC_T_STOP_ALL_TASKS, false);
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
  SSTaskBasicInfo* pInfo = &pTask->info;
  if ((pInfo->taskLevel != TASK_LEVEL__SOURCE) || (pTask->status.downstreamReady == 0)) {
    return false;
  }

  if (pInfo->trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) {
    return false;
  }

  if (pInfo->fillHistory == STREAM_RECALCUL_TASK) {
    return false;
  }

  // not in ready state, do not handle the data from wal
  SStreamTaskState pState = streamTaskGetStatus(pTask);
  if (pState.state != TASK_STATUS__READY) {
    tqTrace("s-task:%s not ready for submit block in wal, status:%s", pTask->id.idStr, pState.name);
    return false;
  }

  // fill-history task has entered into the last phase, no need to do anything
  if ((pInfo->fillHistory == STREAM_HISTORY_TASK) && pTask->status.appendTranstateBlock) {
    // the maximum version of data in the WAL has reached already, the step2 is done
    tqDebug("s-task:%s fill-history reach the maximum ver:%" PRId64 ", not scan wal anymore", pTask->id.idStr,
            pTask->dataRange.range.maxVer);
    return false;
  }

  // check whether input queue is full or not
  if (streamQueueIsFull(pTask->inputq.queue)) {
    tqTrace("s-task:%s input queue is full, launch task without scanning wal", pTask->id.idStr);
    int32_t code = streamTrySchedExec(pTask, false);
    if (code) {
      tqError("s-task:%s failed to start task while inputQ is full", pTask->id.idStr);
    }
    return false;
  }

  // the input queue of downstream task is full, so the output is blocked, stopped for a while
  if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED) {
    tqDebug("s-task:%s inputQ is blocked, do nothing", pTask->id.idStr);
    return false;
  }

  return true;
}

int32_t doPutDataIntoInputQ(SStreamTask* pTask, int64_t maxVer, int32_t* numOfItems, bool* pSucc) {
  const char* id = pTask->id.idStr;
  int32_t     numOfNewItems = 0;
  int32_t     code = 0;
  *pSucc = false;

  while (1) {
    if ((pTask->info.fillHistory == 1) && pTask->status.appendTranstateBlock) {
      *numOfItems += numOfNewItems;
      return numOfNewItems > 0;
    }

    SStreamQueueItem* pItem = NULL;
    code = extractMsgFromWal(pTask->exec.pWalReader, (void**)&pItem, maxVer, id);
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
        if (code == TSDB_CODE_OUT_OF_MEMORY) {
          tqError("s-task:%s failed to put data into inputQ, since out of memory", id);
        } else {
          tqTrace("s-task:%s append input queue failed, code:inputQ is full, ver:%" PRId64, id,
                  pTask->chkInfo.nextProcessVer);
          code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.nextProcessVer);
          if (code) {
            tqError("s-task:%s failed to seek ver to:%" PRId64 " in wal", id, pTask->chkInfo.nextProcessVer);
          }

          code = 0;  // reset the error code
        }

        break;
      }
    }
  }

  *numOfItems += numOfNewItems;
  *pSucc = (numOfNewItems > 0);
  return code;
}

int32_t doScanWalForAllTasks(SStreamMeta* pStreamMeta, int32_t* pNumOfTasks) {
  int32_t vgId = pStreamMeta->vgId;
  SArray* pTaskList = NULL;
  int32_t numOfTasks = 0;

  // clone the task list, to avoid the task update during scan wal files
  streamMetaWLock(pStreamMeta);
  pTaskList = taosArrayDup(pStreamMeta->pTaskList, NULL);
  streamMetaWUnLock(pStreamMeta);
  if (pTaskList == NULL) {
    tqError("vgId:%d failed to create task list dup, code:%s", vgId, tstrerror(terrno));
    return terrno;
  }

  // update the new task number
  numOfTasks = taosArrayGetSize(pTaskList);
  if (pNumOfTasks != NULL) {
    *pNumOfTasks = numOfTasks;
  }

  tqDebug("vgId:%d start to check wal to extract new submit block for %d tasks", vgId, numOfTasks);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId* pTaskId = taosArrayGet(pTaskList, i);
    if (pTaskId == NULL) {
      continue;
    }

    SStreamTask* pTask = NULL;
    int32_t      code = streamMetaAcquireTask(pStreamMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if (pTask == NULL || code != 0) {
      continue;
    }

    if (!taskReadyForDataFromWal(pTask)) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    // seek the stored version and extract data from WAL
    code = setWalReaderStartOffset(pTask, vgId);
    if (code != TSDB_CODE_SUCCESS) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    int32_t numOfItems = streamQueueGetNumOfItems(pTask->inputq.queue);
    int64_t maxVer = (pTask->info.fillHistory == 1) ? pTask->step2Range.maxVer : INT64_MAX;

    streamMutexLock(&pTask->lock);

    SStreamTaskState state = streamTaskGetStatus(pTask);
    if (state.state != TASK_STATUS__READY) {
      tqDebug("s-task:%s not ready for submit block from wal, status:%s", pTask->id.idStr, state.name);
      streamMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    bool hasNewData = false;
    code = doPutDataIntoInputQ(pTask, maxVer, &numOfItems, &hasNewData);
    streamMutexUnlock(&pTask->lock);

    TAOS_UNUSED(code);

    if ((numOfItems > 0) || hasNewData) {
      code = streamTrySchedExec(pTask, false);
      if (code != TSDB_CODE_SUCCESS) {
        streamMetaReleaseTask(pStreamMeta, pTask);
        taosArrayDestroy(pTaskList);
        return code;
      }
    }

    streamMetaReleaseTask(pStreamMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return TSDB_CODE_SUCCESS;
}

void streamMetaFreeTQDuringScanWalError(STQ* pTq) {
  SBuildScanWalMsgParam* p = taosMemoryCalloc(1, sizeof(SBuildScanWalMsgParam));
  p->metaId = pTq->pStreamMeta->rid;

  doStartScanWal(p, 0);
}