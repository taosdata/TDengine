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

#include "tsdb.h"
#include "tsdbInt.h"
#include "vnd.h"

bool tsdbRetentionMonHasTask(STsdb *tsdb) { return (TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr) > 0); }

int32_t tsdbOpenRetentionMonitor(STsdb *tsdb) {
  tsdb->pRetentionMonitor = (SRetentionMonitor *)taosMemoryCalloc(1, sizeof(SRetentionMonitor));
  if (tsdb->pRetentionMonitor == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

void tsdbCloseRetentionMonitor(STsdb *tsdb) {
  if (tsdb->pRetentionMonitor) {
    TARRAY2_DESTROY(&tsdb->pRetentionMonitor->stateArr, NULL);
    taosMemoryFreeClear(tsdb->pRetentionMonitor);
  }
}

int32_t tsdbAddRetentionMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t fileSize) {
  if (TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr) == 0) {
    tsdb->pRetentionMonitor->startTimeSec = taosGetTimestampSec();
    tsdb->pRetentionMonitor->totalTasks = 0;
    tsdb->pRetentionMonitor->totalSize = 0;
    tsdb->pRetentionMonitor->finishedSize = 0;
    tsdb->pRetentionMonitor->lastUpdateFinishedSizeTime = tsdb->pRetentionMonitor->startTimeSec;
    tsdb->pRetentionMonitor->killed = 0;
  }

  SRetentionState state = {
      .fid = fid,
      .taskId = *taskId,
      .fileSize = fileSize,
  };

  int32_t code = TARRAY2_APPEND(&tsdb->pRetentionMonitor->stateArr, state);
  if (code) return code;
  tsdb->pRetentionMonitor->totalTasks++;
  tsdb->pRetentionMonitor->totalSize += fileSize;
  tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is added to retention monitor, number of tasks:%d", TD_VID(tsdb->pVnode),
            fid, taskId->id, TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr));
  return 0;
}

int32_t tsdbUpdateRetentionMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId, int64_t fileSize) {
  for (int32_t i = 0; i < TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr); i++) {
    SRetentionState *state = TARRAY2_GET_PTR(&tsdb->pRetentionMonitor->stateArr, i);
    if (state->fid == fid) {
      tsdb->pRetentionMonitor->totalSize -= state->fileSize;
      tsdb->pRetentionMonitor->totalSize += fileSize;
      state->fileSize = fileSize;
      tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is updated in retention monitor, number of tasks:%d",
                TD_VID(tsdb->pVnode), fid, taskId->id, TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr));
      return 0;
    }
  }
  return 0;
}

void tsdbRemoveRetentionMonitorTask(STsdb *tsdb, SVATaskID *taskId) {
  (void)taosThreadMutexLock(&tsdb->mutex);

  for (int32_t i = 0; i < TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr); i++) {
    SRetentionState *state = TARRAY2_GET_PTR(&tsdb->pRetentionMonitor->stateArr, i);
    if (state->taskId.async == taskId->async && state->taskId.id == taskId->id) {
      tsdbInfo("vid:%d, fid:%d, taskId:%" PRId64 " is removed from retention monitor, number of tasks:%d",
               TD_VID(tsdb->pVnode), state->fid, taskId->id, TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr));
      tsdb->pRetentionMonitor->finishedSize += state->fileSize;
      tsdb->pRetentionMonitor->lastUpdateFinishedSizeTime = taosGetTimestampSec();
      TARRAY2_REMOVE(&tsdb->pRetentionMonitor->stateArr, i, NULL);
      break;
    }
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);
}

void tsdbStopAllRetentionTask(STsdb *tsdb) {
  int32_t i;

  (void)taosThreadMutexLock(&tsdb->mutex);

  if (tsdb->pRetentionMonitor == NULL) {
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    return;
  }

  atomic_store_8(&tsdb->pRetentionMonitor->killed, 1);

  i = 0;
  while (i < TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr)) {
    SRetentionState *state = TARRAY2_GET_PTR(&tsdb->pRetentionMonitor->stateArr, i);
    if (vnodeACancel(&state->taskId) == 0) {
      TARRAY2_REMOVE(&tsdb->pRetentionMonitor->stateArr, i, NULL);
    } else {
      i++;
    }
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return;
}

int32_t tsdbRetentionMonitorGetInfo(STsdb *tsdb, SQueryCompactProgressRsp *rsp) {
  (void)taosThreadMutexLock(&tsdb->mutex);
  rsp->compactId = 0;  // TODO
  rsp->vgId = TD_VID(tsdb->pVnode);
  rsp->numberFileset = tsdb->pRetentionMonitor->totalTasks;
  rsp->finished = rsp->numberFileset - TARRAY2_SIZE(&tsdb->pRetentionMonitor->stateArr);
  // calculate progress
  if (tsdb->pRetentionMonitor->totalSize > 0) {
    rsp->progress = tsdb->pRetentionMonitor->finishedSize * 100 / tsdb->pRetentionMonitor->totalSize;
  } else {
    rsp->progress = 0;
  }
  // calculate estimated remaining time
  int64_t elapsed = tsdb->pRetentionMonitor->lastUpdateFinishedSizeTime - tsdb->pRetentionMonitor->startTimeSec;
  if (rsp->progress > 0 && elapsed > 0) {
    rsp->remainingTime = elapsed * (100 - rsp->progress) / rsp->progress;
  } else {
    rsp->remainingTime = tsdb->pRetentionMonitor->totalSize / (20 * 1024 * 1024);  // suppose 20MB/s
  }
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return 0;
}
