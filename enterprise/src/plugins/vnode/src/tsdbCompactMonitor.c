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
#include "vnd.h"

typedef struct SCompState   SCompState;
typedef struct SCompMonInfo SCompMonInfo;

struct SCompState {
  int32_t   fid;
  SVATaskID taskId;
};

struct SCompMonitor {
  int32_t totalCompTasks;
  int64_t startTimeSec;  // start time of seconds
  TARRAY2(SCompState) stateArr;
};

struct SCompMonInfo {
  bool    compactRunning;
  int64_t startTimeSec;  // start time of seconds
  int32_t numTotalFileSet;
  int32_t numRemainFileSet;
};

bool tsdbCompMonHasTask(STsdb *tsdb) { return (TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr) > 0); }

int32_t tsdbOpenCompMonitor(STsdb *tsdb) {
  tsdb->pCompMonitor = (SCompMonitor *)taosMemoryCalloc(1, sizeof(SCompMonitor));
  if (tsdb->pCompMonitor == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t tsdbCloseCompMonitor(STsdb *tsdb) {
  if (tsdb->pCompMonitor) {
    TARRAY2_DESTROY(&tsdb->pCompMonitor->stateArr, NULL);
    taosMemoryFree(tsdb->pCompMonitor);
    tsdb->pCompMonitor = NULL;
  }
  return 0;
}

int32_t tsdbAddCompMonitorTask(STsdb *tsdb, int32_t fid, SVATaskID *taskId) {
  if (TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr) == 0) {
    tsdb->pCompMonitor->startTimeSec = taosGetTimestampSec();
    tsdb->pCompMonitor->totalCompTasks = 0;
  }

  SCompState state = {
      .fid = fid,
      .taskId = *taskId,
  };

  int32_t code = TARRAY2_APPEND(&tsdb->pCompMonitor->stateArr, state);
  if (code) return code;
  tsdb->pCompMonitor->totalCompTasks++;
  tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is added to compact monitor, number of tasks:%d", TD_VID(tsdb->pVnode),
            fid, taskId->id, TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr));
  return 0;
}

int32_t tsdbRemoveCompMonitorTask(STsdb *tsdb, SVATaskID *taskId) {
  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));

  for (int32_t i = 0; i < TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr); i++) {
    SCompState *state = TARRAY2_GET_PTR(&tsdb->pCompMonitor->stateArr, i);
    if (state->taskId.async == taskId->async && state->taskId.id == taskId->id) {
      tsdbInfo("vid:%d, fid:%d, taskId:%" PRId64 " is removed from compact monitor, number of tasks:%d",
               TD_VID(tsdb->pVnode), state->fid, taskId->id, TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr));
      TARRAY2_REMOVE(&tsdb->pCompMonitor->stateArr, i, NULL);
      break;
    }
  }

  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
  return 0;
}

int32_t tsdbStopAllCompTask(STsdb *tsdb) {
  int32_t i;

  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));

  if (tsdb->pCompMonitor == NULL) {
    TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
    return 0;
  }

  i = 0;
  while (i < TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr)) {
    SCompState *state = TARRAY2_GET_PTR(&tsdb->pCompMonitor->stateArr, i);
    if (vnodeACancel(&state->taskId) == 0) {
      TARRAY2_REMOVE(&tsdb->pCompMonitor->stateArr, i, NULL);
    } else {
      i++;
    }
  }

  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
  return 0;
}

int32_t tsdbCompMonitorGetInfo(STsdb *tsdb, SQueryCompactProgressRsp *rsp) {
  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
  rsp->compactId = 0;  // TODO
  rsp->vgId = TD_VID(tsdb->pVnode);
  rsp->numberFileset = tsdb->pCompMonitor->totalCompTasks;
  rsp->finished = rsp->numberFileset - TARRAY2_SIZE(&tsdb->pCompMonitor->stateArr);
  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
  return 0;
}
