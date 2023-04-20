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

static int32_t doCreateReqsByScanWal(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, bool* pScanIdle);

// this function should be executed by stream threads.
// there is a case that the WAL increases more fast than the restore procedure, and this restore procedure
// will not stop eventually.
int tqStreamTasksScanWal(STQ* pTq) {
  int32_t vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;
  int64_t st = taosGetTimestampMs();

  while (1) {
    int32_t scan = pMeta->walScan;
    tqDebug("vgId:%d continue check if data in wal are available, scan:%d", vgId, scan);

    // check all restore tasks
    bool shouldIdle = true;
    doCreateReqsByScanWal(pTq->pStreamMeta, pTq->pOffsetStore, &shouldIdle);

    int32_t times = 0;

    if (shouldIdle) {
      taosWLockLatch(&pMeta->lock);
      pMeta->walScan -= 1;
      times = pMeta->walScan;

      ASSERT(pMeta->walScan >= 0);

      if (pMeta->walScan <= 0) {
        taosWUnLockLatch(&pMeta->lock);
        break;
      }

      taosWUnLockLatch(&pMeta->lock);
      tqDebug("vgId:%d scan wal for stream tasks for %d times", vgId, times);
    }
  }

  int64_t el = (taosGetTimestampMs() - st);
  tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%"PRId64" ms", vgId, el);
  return 0;
}

static SArray* extractTaskIdList(SStreamMeta* pStreamMeta, int32_t numOfTasks) {
  SArray* pTaskIdList = taosArrayInit(numOfTasks, sizeof(int32_t));
  void*   pIter = NULL;

  taosWLockLatch(&pStreamMeta->lock);
  while(1) {
    pIter = taosHashIterate(pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    taosArrayPush(pTaskIdList, &pTask->id.taskId);
  }

  taosWUnLockLatch(&pStreamMeta->lock);
  return pTaskIdList;
}

int32_t doCreateReqsByScanWal(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, bool* pScanIdle) {
  *pScanIdle = true;
  bool    noNewDataInWal = true;
  int32_t vgId = pStreamMeta->vgId;

  int32_t numOfTasks = taosHashGetSize(pStreamMeta->pTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("vgId:%d start to check wal to extract new submit block for %d tasks", vgId, numOfTasks);
  SArray* pTaskIdList = extractTaskIdList(pStreamMeta, numOfTasks);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    int32_t*     pTaskId = taosArrayGet(pTaskIdList, i);
    SStreamTask* pTask = streamMetaAcquireTask(pStreamMeta, *pTaskId);
    if (pTask == NULL) {
      continue;
    }

    int32_t status = pTask->status.taskStatus;
    if ((pTask->taskLevel != TASK_LEVEL__SOURCE) || (status == TASK_STATUS__DROPPING)) {
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    if (status == TASK_STATUS__RECOVER_PREPARE || status == TASK_STATUS__WAIT_DOWNSTREAM) {
      tqDebug("s-task:%s skip push data, not ready for processing, status %d", pTask->id.idStr, status);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    // check if offset value exists
    char key[128] = {0};
    createStreamTaskOffsetKey(key, pTask->id.streamId, pTask->id.taskId);

    if (tInputQueueIsFull(pTask)) {
      tqDebug("vgId:%d s-task:%s input queue is full, do nothing", vgId, pTask->id.idStr);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    *pScanIdle = false;

    // check if offset value exists
    STqOffset* pOffset = tqOffsetRead(pOffsetStore, key);
    ASSERT(pOffset != NULL);

    // seek the stored version and extract data from WAL
    int32_t code = walReadSeekVer(pTask->exec.pWalReader, pOffset->val.version);
    if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    // append the data for the stream
    tqDebug("vgId:%d wal reader seek to ver:%" PRId64 " %s", vgId, pOffset->val.version, pTask->id.idStr);

    SPackedData packData = {0};
    code = extractSubmitMsgFromWal(pTask->exec.pWalReader, &packData);
    if (code != TSDB_CODE_SUCCESS) {  // failed, continue
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    SStreamDataSubmit2* p = streamDataSubmitNew(packData, STREAM_INPUT__DATA_SUBMIT);
    if (p == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tqError("%s failed to create data submit for stream since out of memory", pTask->id.idStr);
      streamMetaReleaseTask(pStreamMeta, pTask);
      continue;
    }

    noNewDataInWal = false;

    tqDebug("s-task:%s submit data extracted from WAL", pTask->id.idStr);
    code = tqAddInputBlockNLaunchTask(pTask, (SStreamQueueItem*)p, packData.ver);
    if (code == TSDB_CODE_SUCCESS) {
      pOffset->val.version = walReaderGetCurrentVer(pTask->exec.pWalReader);
      tqDebug("s-task:%s set the ver:%" PRId64 " from WALReader after extract block from WAL", pTask->id.idStr,
              pOffset->val.version);
    } else {
      // do nothing
    }

    streamDataSubmitDestroy(p);
    taosFreeQitem(p);
    streamMetaReleaseTask(pStreamMeta, pTask);
  }

  // all wal are checked, and no new data available in wal.
  if (noNewDataInWal) {
    *pScanIdle = true;
  }

  taosArrayDestroy(pTaskIdList);
  return 0;
}

