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

static int32_t streamTaskReplayWal(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, bool* pScanIdle);
static int32_t transferToNormalTask(SStreamMeta* pStreamMeta, SArray* pTaskList);

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
    ASSERT(scan >= 1);

    // check all restore tasks
    bool shouldIdle = true;
    streamTaskReplayWal(pTq->pStreamMeta, pTq->pOffsetStore, &shouldIdle);

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
    } else {
      tqDebug("vgId:%d no idle, scan wal for stream tasks for %d times", vgId, pMeta->walScan);
      ASSERT(pMeta->walScan >= 1);
    }
  }

  int64_t el = (taosGetTimestampMs() - st);
  tqDebug("vgId:%d scan wal for stream tasks completed, elapsed time:%"PRId64" ms", vgId, el);

  // restore wal scan flag
//  atomic_store_8(&pTq->pStreamMeta->walScan, 0);
  return 0;
}

//int32_t transferToNormalTask(SStreamMeta* pStreamMeta, SArray* pTaskList) {
//  int32_t numOfTask = taosArrayGetSize(pTaskList);
//  if (numOfTask <= 0)  {
//    return TSDB_CODE_SUCCESS;
//  }
//
//  // todo: add lock
//  for (int32_t i = 0; i < numOfTask; ++i) {
//    SStreamTask* pTask = taosArrayGetP(pTaskList, i);
//    tqDebug("vgId:%d transfer s-task:%s state restore -> ready, checkpoint:%" PRId64 " checkpoint id:%" PRId64,
//            pStreamMeta->vgId, pTask->id.idStr, pTask->chkInfo.version, pTask->chkInfo.id);
//    taosHashRemove(pStreamMeta->pWalReadTasks, &pTask->id.taskId, sizeof(pTask->id.taskId));
//
//    // NOTE: do not change the following order
//    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__NORMAL);
//    taosHashPut(pStreamMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId), &pTask, POINTER_BYTES);
//  }
//
//  return TSDB_CODE_SUCCESS;
//}

int32_t streamTaskReplayWal(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, bool* pScanIdle) {
  void*   pIter = NULL;
  int32_t vgId = pStreamMeta->vgId;

  *pScanIdle = true;

  bool allWalChecked = true;
  tqDebug("vgId:%d start to check wal to extract new submit block", vgId);

  while (1) {
    pIter = taosHashIterate(pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    if (pTask->status.taskStatus == TASK_STATUS__RECOVER_PREPARE ||
        pTask->status.taskStatus == TASK_STATUS__WAIT_DOWNSTREAM) {
      tqDebug("s-task:%s skip push data, not ready for processing, status %d", pTask->id.idStr,
              pTask->status.taskStatus);
      continue;
    }

    // check if offset value exists
    char key[128] = {0};
    createStreamTaskOffsetKey(key, pTask->id.streamId, pTask->id.taskId);

    if (tInputQueueIsFull(pTask)) {
      tqDebug("vgId:%d s-task:%s input queue is full, do nothing", vgId, pTask->id.idStr);
      continue;
    }

    *pScanIdle = false;

    // check if offset value exists
    STqOffset* pOffset = tqOffsetRead(pOffsetStore, key);
    ASSERT(pOffset != NULL);

    // seek the stored version and extract data from WAL
    int32_t code = walReadSeekVer(pTask->exec.pWalReader, pOffset->val.version);
    if (code != TSDB_CODE_SUCCESS) {  // no data in wal, quit
      continue;
    }

    // append the data for the stream
    tqDebug("vgId:%d wal reader seek to ver:%" PRId64 " %s", vgId, pOffset->val.version, pTask->id.idStr);

    SPackedData packData = {0};
    code = extractSubmitMsgFromWal(pTask->exec.pWalReader, &packData);
    if (code != TSDB_CODE_SUCCESS) {  // failed, continue
      continue;
    }

    SStreamDataSubmit2* p = streamDataSubmitNew(packData, STREAM_INPUT__DATA_SUBMIT);
    if (p == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tqError("%s failed to create data submit for stream since out of memory", pTask->id.idStr);
      continue;
    }

    allWalChecked = false;

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
  }

  if (allWalChecked) {
    *pScanIdle = true;
  }
  return 0;
}

