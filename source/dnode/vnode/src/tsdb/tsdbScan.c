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
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbSttFileRW.h"

typedef struct {
  STsdb    *tsdb;
  int32_t   fid;
  SVATaskID taskid;
  int64_t   scanSize;
} SScanArg;

typedef struct {
  int32_t   fid;
  SVATaskID taskId;
  int64_t   scanSize;
} SScanState;

struct SScanMonitor {
  int32_t          totalScanTasks;
  int64_t          startTimeSec;
  SArray          *tasks;
  int64_t          totalScanSize;
  int64_t          finishedScanSize;
  int64_t          lastUpdateFinishedSizeTime;
  volatile int32_t killed;
};

typedef struct {
  STsdb *tsdb;
  STFileSet *fset;
} SScanContext;

static void tsdbRemoveScanMonitorTask(STsdb *tsdb, SVATaskID *taskId);
static FORCE_INLINE bool tsdbScanTaskCanceled(STsdb *tsdb) { return atomic_load_32(&tsdb->pScanMonitor->killed) != 0; }

static int32_t tsdbScanContextInit(STsdb *tsdb, SScanContext **ppCtx) {
  int32_t       code = 0;
  int32_t       lino;
  SScanContext *ctx = (SScanContext *)taosMemoryCalloc(1, sizeof(SScanContext));
  if (ctx == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  ctx->tsdb = tsdb;
  *ppCtx = ctx;
_exit:
  if (code) {
    if (ctx) {
      taosMemoryFree(ctx);
    }
  }
  return code;
}

static void tsdbScanContextDestroy(SScanContext *ctx) {
  if (ctx == NULL) return;
  taosMemoryFree(ctx);
}

static void tsdbScanSttFile(STFileObj *fobj, SScanContext *pCtx) {
  struct SSttFileReader *reader = NULL;
  int32_t                code = TSDB_CODE_SUCCESS;
  char                   logPrefix[1024] = {0};
  SBlockData             blockData = {0};

  snprintf(logPrefix, sizeof(logPrefix), "[SCAN] fname:%s", fobj->fname);

  tsdbInfo("vgId:%d %s start scan stt file %s", TD_VID(pCtx->tsdb->pVnode), logPrefix, fobj->fname);

  // Open the STT file reader
  SSttFileReaderConfig config = {
      .tsdb = pCtx->tsdb,
      .szPage = pCtx->tsdb->pVnode->config.tsdbPageSize,
      .file[0] = fobj->f[0],
  };
  code = tsdbSttFileReaderOpen(fobj->fname, &config, &reader);
  if (code) {
    tsdbError("vgId:%d %s failed to open stt file %s since %s", TD_VID(pCtx->tsdb->pVnode), logPrefix, fobj->fname,
              tstrerror(code));
    return;
  }

  // Do scan the stt file
  const TSttBlkArray *sttBlkArray = NULL;
  code = tsdbSttFileReadSttBlk(reader, &sttBlkArray);
  if (code) {
    tsdbError("vgId:%d %s failed to read stt blocks from %s since %s", TD_VID(pCtx->tsdb->pVnode), logPrefix,
              fobj->fname, tstrerror(code));
    goto _exit;
  }

  // Loop to scan each stt block
  SSttBlk *sttBlk = NULL;
  TARRAY2_FOREACH_PTR(sttBlkArray, sttBlk) {
    code = tsdbSttFileReadBlockData(reader, sttBlk, &blockData);
    if (code) {
      tsdbError("vgId:%d %s failed to read block data from %s since %s, suid:%" PRId64 " min uid:%" PRId64
                " max uid:%" PRId64 " min ts:%" PRId64 " max ts:%" PRId64 " block offset:%" PRId64 " block size:%d",
                TD_VID(pCtx->tsdb->pVnode), logPrefix, fobj->fname, tstrerror(code), sttBlk->suid, sttBlk->minUid,
                sttBlk->maxUid, sttBlk->minKey, sttBlk->maxKey, sttBlk->bInfo.offset, sttBlk->bInfo.szBlock);
    } else {
      tsdbTrace("vgId:%d %s read block data from %s, suid:%" PRId64 " min uid:%" PRId64 " max uid:%" PRId64
                " min ts:%" PRId64 " max ts:%" PRId64 " block offset:%" PRId64 " block size:%d, nRow:%d",
                TD_VID(pCtx->tsdb->pVnode), logPrefix, fobj->fname, sttBlk->suid, sttBlk->minUid, sttBlk->maxUid,
                sttBlk->minKey, sttBlk->maxKey, sttBlk->bInfo.offset, sttBlk->bInfo.szBlock, blockData.nRow);
    }

    if (tsdbScanTaskCanceled(pCtx->tsdb)) {
      tsdbTrace("vgId:%d %s scan task is canceled, stop scanning stt file %s", TD_VID(pCtx->tsdb->pVnode), logPrefix,
                fobj->fname);
      goto _exit;
    }
  }

_exit:
  tBlockDataDestroy(&blockData);
  tsdbSttFileReaderClose(&reader);
  tsdbInfo("vgId:%d %s, finish scan stt file %s", TD_VID(pCtx->tsdb->pVnode), logPrefix, fobj->fname);
}

static void tsdbScanDataFile(SScanContext *pCtx) {
  SDataFileReader *reader = NULL;
  int32_t          code = TSDB_CODE_SUCCESS;
  SBlockData       blockData = {0};
  STFileObj       *fobj = NULL;
  char             logPrefix[1024] = {0};

  if (pCtx->fset->farr[TSDB_FTYPE_DATA] == NULL) {
    return;
  }

  snprintf(logPrefix, sizeof(logPrefix), "[SCAN] data file:%s", pCtx->fset->farr[TSDB_FTYPE_DATA]->fname);

  tsdbInfo("vgId:%d %s start scan data file", TD_VID(pCtx->tsdb->pVnode), logPrefix);

  // Open the data file reader
  SDataFileReaderConfig config = {
      .tsdb = pCtx->tsdb,
      .szPage = pCtx->tsdb->pVnode->config.tsdbPageSize,
  };
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = pCtx->fset->farr[ftype], 1); ftype++) {
    if (fobj == NULL) {
      continue;
    }

    config.files[ftype].exist = true;
    config.files[ftype].file = fobj->f[0];
  }

  // Open the data file reader
  code = tsdbDataFileReaderOpen(NULL, &config, &reader);
  if (code) {
    tsdbError("vgId:%d %s failed to open data file since %s", TD_VID(pCtx->tsdb->pVnode), logPrefix, tstrerror(code));
    goto _exit;
  }

  // Load BRIN index
  const TBrinBlkArray *brinBlkArray = NULL;
  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code) {
    tsdbError("vgId:%d %s failed to read BRIN blocks since %s", TD_VID(pCtx->tsdb->pVnode), logPrefix, tstrerror(code));
    goto _exit;
  }

  // Loop each BRIN block
  for (int32_t i = 0; i < TARRAY2_SIZE(brinBlkArray); ++i) {
    const SBrinBlk *brinBlk = TARRAY2_GET_PTR(brinBlkArray, i);

    // Load BRIN block
    SBrinBlock brinBlock = {0};
    code = tsdbDataFileReadBrinBlock(reader, brinBlk, &brinBlock);
    if (code) {
      tsdbError("vgId:%d %s failed to read BRIN block since %s, min table id:(%" PRId64 ", %" PRId64
                ") max table id:(%" PRId64 ", %" PRId64 ") BRIN block offset:%" PRId64 " BRIN block size:%" PRId64,
                TD_VID(pCtx->tsdb->pVnode), logPrefix, tstrerror(code), brinBlk->minTbid.suid, brinBlk->minTbid.uid,
                brinBlk->maxTbid.suid, brinBlk->maxTbid.uid, brinBlk->dp[0].offset, brinBlk->dp[0].size);
      continue;
    }

    // Loop each BRIN record
    SBrinRecord record = {0};
    for (int32_t j = 0; j < BRIN_BLOCK_SIZE(&brinBlock); ++j) {
      code = tBrinBlockGet(&brinBlock, j, &record);

      // Load block data
      code = tsdbDataFileReadBlockData(reader, &record, &blockData);
      if (code) {
        tsdbError("vgId:%d %s failed to read block data since %s, suid:%" PRId64 " uid:%" PRId64 " min ts:%" PRId64
                  " max ts:%" PRId64 " min version:%" PRId64 " max version:%" PRId64 " block offset:%" PRId64
                  " block size:%d block key size:%d number rows:%d count:%d",
                  TD_VID(pCtx->tsdb->pVnode), logPrefix, tstrerror(code), record.suid, record.uid,
                  record.firstKey.key.ts, record.lastKey.key.ts, record.minVer, record.maxVer, record.blockOffset,
                  record.blockSize, record.blockKeySize, record.numRow, record.count);
        continue;
      }
    }
  }

_exit:
  tBlockDataDestroy(&blockData);
  tsdbDataFileReaderClose(&reader);
  tsdbInfo("vgId:%d %s finish scan data file", TD_VID(pCtx->tsdb->pVnode), logPrefix);
}

static void tsdbDoScanFileSet(SScanContext *pCtx) {
  // Do scan the STT files
  SSttLvl   *lvl = NULL;
  STFileObj *fobj = NULL;
  TARRAY2_FOREACH(pCtx->fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      if (tsdbScanTaskCanceled(pCtx->tsdb)) {
        return;
      }
      tsdbScanSttFile(fobj, pCtx);
    }
  }

  if (tsdbScanTaskCanceled(pCtx->tsdb)) {
    return;
  }

  // Do scan the data file
  tsdbScanDataFile(pCtx);
}

static void scanArgDestroy(SScanArg *arg);

static int32_t tsdbScanFileSet(void *arg) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SScanArg *scanArg = (SScanArg *)arg;
  STsdb    *tsdb = scanArg->tsdb;
  SScanContext ctx = {
      .tsdb = tsdb,
  };

  tsdbInfo("vgId:%d, start scan file set, fid:%d, taskId:%" PRId64, TD_VID(tsdb->pVnode), scanArg->fid,
           scanArg->taskid.id);

  // Check if task is canceled
  if (tsdbScanTaskCanceled(tsdb)) {
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  // Get the file set
  (void)taosThreadMutexLock(&tsdb->mutex);

  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip scan", TD_VID(tsdb->pVnode));
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  STFileSet *fset = NULL;
  tsdbFSGetFSet(tsdb->pFS, scanArg->fid, &fset);
  if (fset == NULL) {
    tsdbInfo("vgId:%d, fid:%d not found, skip scan", TD_VID(tsdb->pVnode), scanArg->fid);
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  code = tsdbTFileSetInitRef(tsdb, fset, &ctx.fset);
  if (code) {
    tsdbError("vgId:%d, fid:%d, tsdbTFileSetInitRef failed, code:%d", TD_VID(tsdb->pVnode), scanArg->fid, code);
    (void)taosThreadMutexUnlock(&tsdb->mutex);
    goto _exit;
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);

  // Do scan the file set
  tsdbDoScanFileSet(&ctx);

_exit:
  tsdbInfo("vgId:%d, finish scan file set, fid:%d, taskId:%" PRId64 ", code:%d", TD_VID(tsdb->pVnode), scanArg->fid,
           scanArg->taskid.id, code);
  tsdbTFileSetClear(&ctx.fset);
  tsdbRemoveScanMonitorTask(tsdb, &scanArg->taskid);
  scanArgDestroy(scanArg);
  return code;
}

static void scanArgDestroy(SScanArg *arg) {
  if (arg == NULL) return;
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
      .fid = arg->fid,
      .taskId = arg->taskid,
      .scanSize = arg->scanSize,
  };

  if (NULL == taosArrayPush(tsdb->pScanMonitor->tasks, (const void *)&state)) {
    tsdbError("vid:%d, failed to add scan monitor task for fid:%d", TD_VID(tsdb->pVnode), arg->fid);
    return;
  }

  tsdb->pScanMonitor->totalScanTasks++;
  tsdb->pScanMonitor->totalScanSize += state.scanSize;
  tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is added to scan monitor", TD_VID(tsdb->pVnode),
            state.fid, state.taskId.id);
  return;
}

static void tsdbRemoveScanMonitorTask(STsdb *tsdb, SVATaskID *taskId) {
  (void)taosThreadMutexLock(&tsdb->mutex);

  for (int32_t i = 0; i < taosArrayGetSize(tsdb->pScanMonitor->tasks); i++) {
    SScanState *state = (SScanState *)taosArrayGet(tsdb->pScanMonitor->tasks, i);
    if (state->taskId.async == taskId->async && state->taskId.id == taskId->id) {
      tsdbInfo("vid:%d, fid:%d, taskId:%" PRId64 " is removed from scan monitor", TD_VID(tsdb->pVnode), state->fid,
               taskId->id);
      tsdb->pScanMonitor->finishedScanSize += state->scanSize;
      tsdb->pScanMonitor->lastUpdateFinishedSizeTime = taosGetTimestampSec();
      taosArrayRemove(tsdb->pScanMonitor->tasks, i);
      break;
    }
  }

  (void)taosThreadMutexUnlock(&tsdb->mutex);
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

    SScanArg *arg = taosMemoryCalloc(1, sizeof(*arg));
    if (arg == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    arg->tsdb = tsdb;
    arg->fid = fset->fid;
    arg->scanSize = tsdbGetFileSetScanSize(fset);
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

  (void)taosThreadMutexLock(&tsdb->mutex);

  atomic_store_32(&tsdb->pScanMonitor->killed, 1);
  int32_t i = 0;
  while (true) {
    int32_t arraySize = taosArrayGetSize(tsdb->pScanMonitor->tasks);

    if (i >= arraySize) {
      break;
    }

    if (i < arraySize) {
      SScanState *state = (SScanState *)taosArrayGet(tsdb->pScanMonitor->tasks, i);
      if (vnodeACancel(&state->taskId) == 0) {
        tsdbDebug("vid:%d, fid:%d, taskId:%" PRId64 " is cancelled and removed from scan monitor", TD_VID(tsdb->pVnode),
                  state->fid, state->taskId.id);
        taosArrayRemove(tsdb->pScanMonitor->tasks, i);
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
  tsdb->pScanMonitor = (struct SScanMonitor *)taosMemoryCalloc(1, sizeof(struct SScanMonitor));
  if (NULL == tsdb->pScanMonitor) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

void tsdbScanMonitorClose(STsdb *tsdb) {
  if (tsdb->pScanMonitor) {
    taosArrayDestroy(tsdb->pScanMonitor->tasks);
    taosMemoryFree(tsdb->pScanMonitor);
    tsdb->pScanMonitor = NULL;
  }
}