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
#include "tsdbFS2.h"

typedef struct {
  STsdb    *tsdb;
  STFileSet *fset;
  SVATaskID taskid;
} SScanArg;

typedef struct {
  int32_t   fid;
  SVATaskID taskId;
  int64_t   scanSize;
} SScanState;

struct SScanMonitor {
  int32_t          totalScanTasks;
  int64_t          startTimeSec;  // start time of seconds
  SArray          *tasks;
  int64_t          totalScanSize;
  int64_t          finishedScanSize;
  int64_t          lastUpdateFinishedSizeTime;
  volatile int32_t killed;
};

static int32_t tsdbScanFileSet(void *arg) {
  int32_t code = 0;
  SScanArg *scanArg = (SScanArg *)arg;

#if 0
  // Prepare to scan
  code = tsdbScanFileSetBegin(arg);

  // Do scan
  code = tsdbDoScanFileSet(arg);

  // Finish scan
  code = tsdbScanFileSetEnd(arg);
#endif

  return code;
}

static void scanArgDestroy(SScanArg *arg) {
  if (arg == NULL) return;

  if (arg->fset) {
    tsdbTFileSetClear(&arg->fset);
  }

  taosMemoryFree(arg);
}

static void tsdbScanFileSetCancel(void *arg) { scanArgDestroy((SScanArg *)arg); }

static int64_t tsdbGetFileSetScanSize(STFileSet *fset) {
  int64_t    size = 0;
  STFileObj *fobj;
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = fset->farr[ftype], 1); ftype++) {
    if (fobj == NULL) continue;
    size += fobj->f[0].size;
  }

  SSttLvl *lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) { size += fobj->f[0].size; }
  }
  return size;
}

static void tsdbAddScanMonitorTask(STsdb *tsdb, SScanArg *arg) {
  if (tsdb->pScanMonitor == NULL) {
    return;
  }

  // Init a new task
  if (taosArrayGetSize(tsdb->pScanMonitor->tasks) == 0) {
    tsdb->pScanMonitor->startTimeSec = taosGetTimestampSec();
    tsdb->pScanMonitor->totalScanTasks = 0;
    tsdb->pScanMonitor->totalScanSize = 0;
    tsdb->pScanMonitor->finishedScanSize = 0;
    tsdb->pScanMonitor->lastUpdateFinishedSizeTime = tsdb->pScanMonitor->startTimeSec;
    tsdb->pScanMonitor->killed = 0;
  }

  if (NULL == tsdb->pScanMonitor->tasks) {
    tsdb->pScanMonitor->tasks = taosArrayInit(64, sizeof(SScanState));
    if (tsdb->pScanMonitor->tasks == NULL) {
      tsdbError("vid:%d, failed to allocate memory for scan monitor tasks", TD_VID(tsdb->pVnode));
      return;
    }
  }

  SScanState state = {
      .fid = arg->fset->fid,
      .taskId = arg->taskid,
      .scanSize = tsdbGetFileSetScanSize(arg->fset),
  };

  if (NULL == taosArrayPush(tsdb->pScanMonitor->tasks, (const void *)&state)) {
    tsdbError("vid:%d, failed to add scan monitor task for fid:%d", TD_VID(tsdb->pVnode), arg->fset->fid);
    return;
  }

  tsdb->pScanMonitor->totalScanTasks++;
  tsdb->pScanMonitor->totalScanSize += state.scanSize;
  tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is added to scan monitor", TD_VID(tsdb->pVnode),
            state.fid, state.taskId.id);
  return;
}

static int32_t tsdbAsyncScanImpl(STsdb *tsdb, const STimeWindow *tw) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip scan", TD_VID(tsdb->pVnode));
    return 0;
  }

  int32_t minFid = tsdbKeyFid(tw->skey, tsdb->keepCfg.days, tsdb->keepCfg.precision);
  int32_t maxFid = tsdbKeyFid(tw->ekey, tsdb->keepCfg.days, tsdb->keepCfg.precision);

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    if (fset->fid < minFid || fset->fid > maxFid) {
      continue;
    }

    SScanArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }

    arg->tsdb = tsdb;
    code = tsdbTFileSetInitRef(tsdb, fset, &arg->fset);
    if (code) {
      scanArgDestroy(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = vnodeAsync(SCAN_TASK_ASYNC, EVA_PRIORITY_NORMAL, tsdbScanFileSet, tsdbScanFileSetCancel, arg, &arg->taskid);
    if (code) {
      scanArgDestroy(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    tsdbAddScanMonitorTask(tsdb, arg);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncScan(STsdb *tsdb, const STimeWindow *tw) {
  int32_t code = 0;
  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncScanImpl(tsdb, tw);
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}

void tsdbCancelScanTask(STsdb *tsdb) {
  if (tsdb->pScanMonitor == NULL) {
    return;
  }

  // TODO: fix here
  return;

  (void)taosThreadMutexLock(&tsdb->mutex);

  atomic_store_32(&tsdb->pScanMonitor->killed, 1);
  int32_t i = 0;
  while (true) {
    if (i < taosArrayGetSize(tsdb->pScanMonitor->tasks)) {
      SScanState *state = (SScanState *)taosArrayGet(tsdb->pScanMonitor->tasks, i);
      if (vnodeACancel(&state->taskId) == 0) {
        taosArrayRemove(tsdb->pScanMonitor->tasks, i);
        tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is cancelled and removed from scan monitor", TD_VID(tsdb->pVnode),
                  state->fid, state->taskId.id);
        continue;
      } else {
        i++;
      }
    }
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);
}

void tsdbScanMonitorGetInfo(STsdb *tsdb, SQueryScanProgressRsp *rsp) {
  (void)taosThreadMutexLock(&tsdb->mutex);
  rsp->scanId = 0;
  rsp->vgId = TD_VID(tsdb->pVnode);
  rsp->numberFileset = tsdb->pScanMonitor->totalScanTasks;
  rsp->finished = rsp->numberFileset - taosArrayGetSize(tsdb->pScanMonitor->tasks);
  // calculate progress
  if (tsdb->pScanMonitor->totalScanSize > 0) {
    rsp->progress = tsdb->pScanMonitor->finishedScanSize * 100 / tsdb->pScanMonitor->totalScanSize;
  } else {
    rsp->progress = 0;
  }
  // calculate estimated remaining time
  int64_t elapsed = tsdb->pScanMonitor->lastUpdateFinishedSizeTime - tsdb->pScanMonitor->startTimeSec;
  if (rsp->progress > 0 && elapsed > 0) {
    rsp->remainingTime = elapsed * (100 - rsp->progress) / rsp->progress;
  } else {
    rsp->remainingTime = tsdb->pScanMonitor->totalScanSize / (20 * 1024 * 1024);  // suppose 20MB/s
  }
  (void)taosThreadMutexUnlock(&tsdb->mutex);
}

int32_t tsdbScanMonitorOpen(STsdb *tsdb) {
  int32_t code = 0;

  tsdb->pScanMonitor = (struct SScanMonitor *)taosMemoryCalloc(1, sizeof(struct SScanMonitor));
  if (NULL == tsdb->pScanMonitor) {
    return terrno;
  }
  return code;
}

void tsdbScanMonitorClose(STsdb *tsdb) {
  if (tsdb->pScanMonitor) {
    tsdbCancelScanTask(tsdb);
    taosArrayDestroy(tsdb->pScanMonitor->tasks);
    taosMemoryFree(tsdb->pScanMonitor);
    tsdb->pScanMonitor = NULL;
  }
}