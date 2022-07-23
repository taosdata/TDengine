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

static int32_t createStreamTaskRunReq(SStreamMeta* pStreamMeta, bool* pScanIdle);
static int32_t doSetOffsetForWalReader(SStreamTask *pTask, int32_t vgId);

// this function should be executed by stream threads.
// extract submit block from WAL, and add them into the input queue for the sources tasks.
int32_t tqStreamTasksScanWal(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int64_t      st = taosGetTimestampMs();

  while (1) {
    int32_t scan = pMeta->walScanCounter;
    tqDebug("vgId:%d continue check if data in wal are available, walScanCounter:%d", vgId, scan);

    // check all restore tasks
    bool shouldIdle = true;
    createStreamTaskRunReq(pTq->pStreamMeta, &shouldIdle);

    int32_t times = 0;

    if (shouldIdle) {
      taosWLockLatch(&pMeta->lock);

      pMeta->walScanCounter -= 1;
      times = pMeta->walScanCounter;

      ASSERT(pMeta->walScanCounter >= 0);

      if (pMeta->walScanCounter <= 0) {
        taosWUnLockLatch(&pMeta->lock);
        break;
      }

      taosWUnLockLatch(&pMeta->lock);
      tqDebug("vgId:%d scan wal for stream tasks for %d times", vgId, times);
    }
  }

  int64_t el = (taosGetTimestampMs() - st);
  tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%" PRId64 " ms", vgId, el);
  return 0;
}

int32_t tqStreamTasksStatusCheck(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  tqDebug("vgId:%d start to check all (%d) stream tasks downstream status", vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pTaskList = NULL;
  taosWLockLatch(&pMeta->lock);
  pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosWUnLockLatch(&pMeta->lock);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    int32_t*     pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, *pTaskId);
    if (pTask == NULL) {
      continue;
    }

    if (pTask->info.fillHistory == 1) {
      tqDebug("s-task:%s fill-history task, wait for related stream task:0x%x to launch it", pTask->id.idStr,
              pTask->streamTaskId.taskId);
      continue;
    }

    streamTaskDoCheckDownstreamTasks(pTask);
    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);
  return 0;
}

int32_t tqCheckStreamStatus(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  taosWLockLatch(&pMeta->lock);

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqInfo("vgId:%d no stream tasks exist", vgId);
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

  tqDebug("vgId:%d check for stream tasks status, numOfTasks:%d", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = STREAM_TASK_STATUS_CHECK_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  taosWUnLockLatch(&pMeta->lock);

  return 0;
}

int32_t tqStartStreamTasks(STQ* pTq) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  taosWLockLatch(&pMeta->lock);

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqInfo("vgId:%d no stream tasks exist", vgId);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  pMeta->walScanCounter += 1;

  if (pMeta->walScanCounter > 1) {
    tqDebug("vgId:%d wal read task has been launched, remain scan times:%d", vgId, pMeta->walScanCounter);
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
  pRunReq->taskId = EXTRACT_DATA_FROM_WAL_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  taosWUnLockLatch(&pMeta->lock);

  return 0;
}

int32_t doSetOffsetForWalReader(SStreamTask *pTask, int32_t vgId) {
  // seek the stored version and extract data from WAL
  int64_t firstVer = walReaderGetValidFirstVer(pTask->exec.pWalReader);
  if (pTask->chkInfo.currentVer < firstVer) {
    tqWarn("vgId:%d s-task:%s ver:%"PRId64" earlier than the first ver of wal range %" PRId64 ", forward to %" PRId64, vgId,
           pTask->id.idStr, pTask->chkInfo.currentVer, firstVer, firstVer);

    pTask->chkInfo.currentVer = firstVer;

    // todo need retry if failed
    int32_t code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.currentVer);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // append the data for the stream
    tqDebug("vgId:%d s-task:%s wal reader seek to ver:%" PRId64, vgId, pTask->id.idStr, pTask->chkInfo.currentVer);
  } else {
    int64_t currentVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
    if (currentVer == -1) {  // we only seek the read for the first time
      int32_t code = walReaderSeekVer(pTask->exec.pWalReader, pTask->chkInfo.currentVer);
      if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
        return code;
      }

      // append the data for the stream
      tqDebug("vgId:%d s-task:%s wal reader initial seek to ver:%" PRId64, vgId, pTask->id.idStr, pTask->chkInfo.currentVer);
    }
  }

  int64_t skipToVer = walReaderGetSkipToVersion(pTask->exec.pWalReader);
  if (skipToVer != 0 && skipToVer > pTask->chkInfo.currentVer) {
    int32_t code = walReaderSeekVer(pTask->exec.pWalReader, skipToVer);
    if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
      return code;
    }

    tqDebug("vgId:%d s-task:%s wal reader jump to ver:%" PRId64, vgId, pTask->id.idStr, skipToVer);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createStreamTaskRunReq(SStreamMeta* pStreamMeta, bool* pScanIdle) {
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
    int32_t*     pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask* pTask = streamMetaAcquireTask(pStreamMeta, *pTaskId);
    if (pTask == NULL) {
      continue;
    }

    int32_t status = pTask->status.taskStatus;
    if (pTask->info.taskLevel != TASK_LEVEL__SOURCE) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    if (status != TASK_STATUS__NORMAL) {
      tqDebug("s-task:%s not ready for new submit block from wal, status:%s", pTask->id.idStr, streamGetTaskStatusStr(status));
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    if (tInputQueueIsFull(pTask)) {
      tqTrace("s-task:%s input queue is full, do nothing", pTask->id.idStr);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    *pScanIdle = false;

    // seek the stored version and extract data from WAL
    int32_t code = doSetOffsetForWalReader(pTask, vgId);
    if (code != TSDB_CODE_SUCCESS) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    int32_t numOfItemsInQ = taosQueueItemSize(pTask->inputQueue->queue);

    // append the data for the stream
    SStreamQueueItem* pItem = NULL;
    code = extractMsgFromWal(pTask->exec.pWalReader, (void**) &pItem, pTask->id.idStr);

    if ((code != TSDB_CODE_SUCCESS || pItem == NULL) && (numOfItemsInQ == 0)) {  // failed, continue
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    if (pItem != NULL) {
      noDataInWal = false;
      code = tAppendDataToInputQueue(pTask, pItem);
      if (code == TSDB_CODE_SUCCESS) {
        pTask->chkInfo.currentVer = walReaderGetCurrentVer(pTask->exec.pWalReader);
        tqDebug("s-task:%s set the ver:%" PRId64 " from WALReader after extract block from WAL", pTask->id.idStr,
                pTask->chkInfo.currentVer);
      } else {
        tqError("s-task:%s append input queue failed, too many in inputQ, ver:%" PRId64, pTask->id.idStr,
                pTask->chkInfo.currentVer);
      }
    }

    if ((code == TSDB_CODE_SUCCESS) || (numOfItemsInQ > 0)) {
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

