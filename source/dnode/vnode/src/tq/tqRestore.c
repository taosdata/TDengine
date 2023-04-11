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

static int32_t restoreStreamTaskImpl(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, SArray* pTaskList);
static int32_t transferToNormalTask(SStreamMeta* pStreamMeta, SArray* pTaskList);

// this function should be executed by stream threads.
// there is a case that the WAL increases more fast than the restore procedure, and this restore procedure
// will not stop eventually.
int tqDoRestoreSourceStreamTasks(STQ* pTq) {
  int64_t st = taosGetTimestampMs();
  while (1) {
    SArray* pTaskList = taosArrayInit(4, POINTER_BYTES);

    // check all restore tasks
    restoreStreamTaskImpl(pTq->pStreamMeta, pTq->pOffsetStore, pTaskList);
    transferToNormalTask(pTq->pStreamMeta, pTaskList);
    taosArrayDestroy(pTaskList);

    int32_t numOfRestored = taosHashGetSize(pTq->pStreamMeta->pRestoreTasks);
    if (numOfRestored <= 0) {
      break;
    }
  }

  int64_t et = taosGetTimestampMs();
  tqInfo("vgId:%d restoring task completed, elapsed time:%" PRId64 " sec.", TD_VID(pTq->pVnode), (et - st));
  return 0;
}

int32_t transferToNormalTask(SStreamMeta* pStreamMeta, SArray* pTaskList) {
  int32_t numOfTask = taosArrayGetSize(pTaskList);
  if (numOfTask <= 0)  {
    return TSDB_CODE_SUCCESS;
  }

  // todo: add lock
  for(int32_t i = 0; i < numOfTask; ++i){
    SStreamTask* pTask = taosArrayGetP(pTaskList, i);
    tqDebug("vgId:%d transfer s-task:%s state restore -> ready", pStreamMeta->vgId, pTask->id.idStr);
    taosHashRemove(pStreamMeta->pRestoreTasks, &pTask->id.taskId, sizeof(pTask->id.taskId));

    // NOTE: do not change the following order
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__NORMAL);
    taosHashPut(pStreamMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId), &pTask, POINTER_BYTES);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t restoreStreamTaskImpl(SStreamMeta* pStreamMeta, STqOffsetStore* pOffsetStore, SArray* pTaskList) {
  // check all restore tasks
  void* pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pStreamMeta->pRestoreTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    if (pTask->taskStatus == TASK_STATUS__RECOVER_PREPARE || pTask->taskStatus == TASK_STATUS__WAIT_DOWNSTREAM) {
      tqDebug("s-task:%s skip push data, not ready for processing, status %d", pTask->id.idStr, pTask->taskStatus);
      continue;
    }

    // check if offset value exists
    char key[128] = {0};
    createStreamTaskOffsetKey(key, pTask->id.streamId, pTask->id.taskId);

    if (tInputQueueIsFull(pTask)) {
      tqDebug("s-task:%s input queue is full, do nothing", pTask->id.idStr);
      taosMsleep(10);
      continue;
    }

    // check if offset value exists
    STqOffset* pOffset = tqOffsetRead(pOffsetStore, key);
    if (pOffset != NULL) {
      // seek the stored version and extract data from WAL
      int32_t code = tqSeekVer(pTask->exec.pTqReader, pOffset->val.version, "");
      if (code == TSDB_CODE_SUCCESS) {  // all data retrieved, abort
        // append the data for the stream
        SFetchRet ret = {.data.info.type = STREAM_NORMAL};
        terrno = 0;

        tqNextBlock(pTask->exec.pTqReader, &ret);
        if (ret.fetchType == FETCH_TYPE__DATA) {
          code = launchTaskForWalBlock(pTask, &ret, pOffset);
          if (code != TSDB_CODE_SUCCESS) {
            continue;
          }
        } else {
          // FETCH_TYPE__NONE: all data has been retrieved from WAL, let's try submit block directly.
          tqDebug("s-task:%s data in WAL are all consumed, transfer this task to be normal state", pTask->id.idStr);
          taosArrayPush(pTaskList, &pTask);
        }
      } else {  // failed to seek to the WAL version
        tqDebug("s-task:%s data in WAL are all consumed, transfer this task to be normal state", pTask->id.idStr);
        taosArrayPush(pTaskList, &pTask);
      }
    } else {
      ASSERT(0);
    }
  }

  return 0;
}

