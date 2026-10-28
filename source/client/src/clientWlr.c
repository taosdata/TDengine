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

/*
 * Workload Recorder -- manager and session lifecycle (DS 6.3, 6.4).
 *
 * One manager and at most one session per process. Every native connection in
 * the process writes into that single session, which is what gives a total
 * event order within the process (DS 9.3).
 */

#include "clientWlrInt.h"

#include "tglobal.h"
#include "tlog.h"

SWlrManager wlrMgr = {0};

/* Per-thread public API depth and async parent, DS 6.6 / 6.8. Thread-local
 * because two threads may each be inside their own public API at the same time;
 * a shared counter would read 2 and wrongly suppress both. */
threadlocal int32_t  wlrTlsDepth = 0;
threadlocal uint64_t wlrTlsCallbackParent = 0;

/* ------------------------------------------------------------------- state */

int32_t wlrRecorderState(void) { return atomic_load_32(&wlrMgr.state); }

bool wlrStateIs(int32_t expected) { return atomic_load_32(&wlrMgr.state) == expected; }

/*
 * CAS transition. Returns true when this caller performed it, so exactly one
 * thread runs the side effects of a transition even when several observe the
 * same trigger (e.g. two producers hitting a write error together).
 */
bool wlrTrySetState(int32_t from, int32_t to) {
  return atomic_val_compare_exchange_32(&wlrMgr.state, from, to) == from;
}

/* --------------------------------------------------------------- ID minting */

uint64_t wlrNextConnId(void) { return (uint64_t)atomic_add_fetch_64(&wlrMgr.nextConnId, 1); }
uint64_t wlrNextStmtId(void) { return (uint64_t)atomic_add_fetch_64(&wlrMgr.nextStmtId, 1); }
uint64_t wlrNextResultId(void) { return (uint64_t)atomic_add_fetch_64(&wlrMgr.nextResultId, 1); }

/* ------------------------------------------------------- callback plumbing */

void     wlrSetCallbackParent(uint64_t callId) { wlrTlsCallbackParent = callId; }
uint64_t wlrGetCallbackParent(void) { return wlrTlsCallbackParent; }
int32_t  wlrCallbackBegin(void) {
  int32_t previous = wlrTlsDepth;
  wlrTlsDepth = 0;
  return previous;
}
void wlrCallbackEnd(int32_t previousDepth) { wlrTlsDepth = previousDepth; }

int32_t wlrInternalBegin(void) {
  int32_t previous = wlrTlsDepth;
  wlrTlsDepth = 1;
  return previous;
}

void wlrInternalEnd(int32_t previousDepth) { wlrTlsDepth = previousDepth; }

void wlrCallbackScopeBegin(uint64_t parentCallId, SWlrCallbackScope *scope) {
  if (scope == NULL) return;
  scope->previousParent = wlrGetCallbackParent();
  scope->previousDepth = wlrCallbackBegin();
  wlrSetCallbackParent(parentCallId);
}

void wlrCallbackScopeEnd(const SWlrCallbackScope *scope) {
  if (scope == NULL) return;
  wlrSetCallbackParent(scope->previousParent);
  wlrCallbackEnd(scope->previousDepth);
}

#define WLR_PENDING_ASYNC_MAX_DEPTH 16

typedef struct {
  uint64_t callId;
  int64_t  beginMonoNs;
  uint16_t apiType;
} SWlrPendingAsyncCall;

static threadlocal SWlrPendingAsyncCall wlrTlsPendingAsync[WLR_PENDING_ASYNC_MAX_DEPTH];
static threadlocal int32_t              wlrTlsPendingAsyncDepth = 0;
static threadlocal int32_t              wlrTlsPendingAsyncOverflow = 0;
static threadlocal int32_t              wlrTlsPendingAsyncClearDebt = 0;

void wlrSetPendingAsyncCall(const SWorkloadCallToken *token) {
  if (token == NULL) {
    if (wlrTlsPendingAsyncClearDebt > 0) {
      --wlrTlsPendingAsyncClearDebt;
    } else if (wlrTlsPendingAsyncOverflow > 0) {
      --wlrTlsPendingAsyncOverflow;
    } else if (wlrTlsPendingAsyncDepth > 0) {
      --wlrTlsPendingAsyncDepth;
    }
    return;
  }

  /* A completed top-level handoff whose wrapper omitted its paired clear cannot
   * belong to this new call. Nested handoffs retain debt while an outer frame exists. */
  if (wlrTlsPendingAsyncDepth == 0 && wlrTlsPendingAsyncOverflow == 0) {
    wlrTlsPendingAsyncClearDebt = 0;
  }
  if (wlrTlsPendingAsyncDepth >= WLR_PENDING_ASYNC_MAX_DEPTH) {
    if (wlrTlsPendingAsyncOverflow++ == 0) {
      tscWarn("WLR: pending async call nesting exceeds %d; innermost DONE may be omitted",
              WLR_PENDING_ASYNC_MAX_DEPTH);
    }
    return;
  }

  SWlrPendingAsyncCall *pending = &wlrTlsPendingAsync[wlrTlsPendingAsyncDepth++];
  pending->callId = token->recorded ? token->callId : 0;
  pending->beginMonoNs = token->recorded ? token->beginMonoNs : 0;
  pending->apiType = token->recorded ? token->apiType : 0;
}

void wlrTakePendingAsyncCall(uint64_t *callId, int64_t *beginMonoNs, uint16_t *apiType) {
  SWlrPendingAsyncCall pending = {0};
  if (wlrTlsPendingAsyncOverflow > 0) {
    --wlrTlsPendingAsyncOverflow;
    ++wlrTlsPendingAsyncClearDebt;
  } else if (wlrTlsPendingAsyncDepth > 0) {
    pending = wlrTlsPendingAsync[--wlrTlsPendingAsyncDepth];
    ++wlrTlsPendingAsyncClearDebt;
  }
  if (callId != NULL) *callId = pending.callId;
  if (beginMonoNs != NULL) *beginMonoNs = pending.beginMonoNs;
  if (apiType != NULL) *apiType = pending.apiType;
}

/* ------------------------------------------------------------- session init */

WLR_LAYOUT_CHECK(session_id_bytes, WLR_SESSION_ID_LEN == 8 + 4 + 4);

static int32_t wlrGenSessionId(uint8_t *out) {
  /* Locally generated and allowed to repeat: replay boundaries come from the
   * files the user names, not from this value, and it is never an authorization
   * token (DS 6.5, 8). */
  int64_t wall = taosGetTimestampNs();
  int64_t mono = taosGetMonoTimestampNs();
  int64_t pid = (int64_t)taosGetPId();
  uint32_t rnd = taosRand();

  wlrPutU64(out, (uint64_t)wall);
  wlrPutU32(out + 8, (uint32_t)pid);
  wlrPutU32(out + 12, rnd ^ (uint32_t)(mono & 0xFFFFFFFF));
  return 0;
}

static int32_t wlrPrepareDir(const char *dir) {
  if (dir == NULL || dir[0] == 0) return WLR_E_INIT_DIR;

  if (taosCheckExistFile(dir)) {
    TdDirPtr existing = taosOpenDir(dir);
    if (existing == NULL) {
      tscError("WLR: record dir:%s names an existing non-directory", dir);
      return WLR_E_INIT_DIR;
    }
    (void)taosCloseDir(&existing);
  }

  /*
   * An existing directory that the operator deliberately made unwritable means
   * "do not record here", so that is checked FIRST. It has to be, because the
   * chmod below would otherwise hand write permission back to ourselves and
   * silently override the operator's intent (TS CFG009).
   */
  if (taosDirExist(dir) &&
      !taosCheckAccessFile(dir, TD_FILE_ACCESS_EXIST_OK | TD_FILE_ACCESS_WRITE_OK)) {
    tscError("WLR: record dir:%s is not writable, recording disabled", dir);
    return WLR_E_INIT_DIR;
  }

  /* The directory must exist and be writable before recording can start, then its
   * permissions are narrowed to owner-only because it holds SQL and bind payloads
   * (FS 6, TS CFG011).
   *
   * checkAccess=false is deliberate: with it true, taosMulModeMkDir returns early
   * for an existing accessible directory and skips the chmod, leaving a
   * pre-existing world-readable directory untightened -- the common case, since an
   * operator usually creates the path before enabling recording. */
  if (taosMulModeMkDir(dir, WLR_DIR_PERM, false) != 0) {
    tscError("WLR: failed to prepare record dir:%s since %s", dir, tstrerror(TAOS_SYSTEM_ERROR(ERRNO)));
    return WLR_E_INIT_DIR;
  }
  return WLR_E_OK;
}

int32_t wlrRecorderInit(const SWorkloadRecorderCfg *cfg) {
  int32_t code = WLR_E_OK;
  bool    queueReady = false;
  bool    writerCleanupNeeded = false;

  if (cfg == NULL) return WLR_E_CONFIG;

  /* Mode 0 and reserved modes allocate nothing: no pool, no queue, no thread,
   * no directory. The hot path must stay a single atomic load (DS 6.1, 9.1). */
  if (cfg->mode == WLR_CFG_MODE_OFF) {
    return WLR_E_OK;
  }
  if (cfg->mode != WLR_CFG_MODE_ON) {
    tscWarn("WLR: workloadRecord=%d is reserved, recording stays off", cfg->mode);
    return WLR_E_CONFIG;
  }
  if (cfg->bufferSizeMb < WLR_CFG_BUFFER_MB_MIN || cfg->bufferSizeMb > WLR_CFG_BUFFER_MB_MAX) {
    tscError("WLR: workloadRecordBufferSize=%d out of range", cfg->bufferSizeMb);
    return WLR_E_CONFIG;
  }

  if (!wlrTrySetState(WLR_STATE_OFF, WLR_STATE_STARTING)) {
    tscWarn("WLR: recorder already initialized, state:%d", wlrRecorderState());
    return WLR_E_OK;
  }
  /* OFF is published only after the preceding generation has released all
   * resources. Reset its single-closer guard only after owning STARTING, so a
   * losing concurrent init cannot reopen a close already in progress. */
  atomic_store_32(&wlrMgr.closeStarted, 0);
  tstrncpy(wlrMgr.recordDir, cfg->recordDir, sizeof(wlrMgr.recordDir));
  wlrMgr.bufferBytes = (int64_t)cfg->bufferSizeMb * 1024 * 1024;

  code = wlrPrepareDir(wlrMgr.recordDir);
  if (code != WLR_E_OK) goto _fail;

  if (taosThreadMutexInit(&wlrMgr.lock, NULL) != 0) {
    code = WLR_E_ALLOC;
    goto _fail;
  }
  wlrMgr.lockReady = true;

  (void)wlrGenSessionId(wlrMgr.sessionId);
  memset(wlrMgr.clockDomainId, 0, sizeof(wlrMgr.clockDomainId));
  (void)taosGetMonotonicClockId(wlrMgr.clockDomainId, sizeof(wlrMgr.clockDomainId));
  wlrMgr.processStartWallNs = taosGetTimestampNs();
  wlrMgr.processStartMonoNs = taosGetMonoTimestampNs();
  wlrMgr.pid = (uint64_t)taosGetPId();
  /* A new session must not inherit the previous one's in-flight calls: those IDs
   * belong to a different session and would appear as unfinished in this one's
   * SESSION_STOP. Reset before the queue is created, since that is what makes the
   * list usable. */
  wlrMgr.activeCount = 0;
  wlrMgr.activeOverflow = 0;
  wlrMgr.manifestCollision = 0;

  atomic_store_64(&wlrMgr.nextEventSeq, 0);
  atomic_store_64(&wlrMgr.nextCallId, 0);
  atomic_store_64(&wlrMgr.nextConnId, 0);
  atomic_store_64(&wlrMgr.nextStmtId, 0);
  atomic_store_64(&wlrMgr.nextResultId, 0);
  atomic_store_64(&wlrMgr.eventsWritten, 0);
  atomic_store_64(&wlrMgr.bytesWritten, 0);
  atomic_store_64(&wlrMgr.droppedEvents, 0);
  atomic_store_64(&wlrMgr.backpressureNs, 0);
  atomic_store_32(&wlrMgr.captureIncomplete, 0);
  atomic_store_64(&wlrMgr.lifecycleState, 0);
  atomic_store_32(&wlrMgr.injectWriteFail, 0);
  atomic_store_32(&wlrMgr.injectFsyncFail, 0);
  atomic_store_32(&wlrMgr.stallWriter, 0);

  code = wlrQueueInit(&wlrMgr, wlrMgr.bufferBytes);
  if (code != WLR_E_OK) goto _fail;
  queueReady = true;

  /* Start may open the first segment and then fail before writerReady is set. */
  writerCleanupNeeded = true;
  code = wlrWriterStart(&wlrMgr);
  if (code != WLR_E_OK) goto _fail;

  /* RUNNING only after the first segment header is on disk, so a reader never
   * sees a session directory with no parsable file (DS 6.3). */
  if (!wlrTrySetState(WLR_STATE_STARTING, WLR_STATE_RUNNING)) {
    code = WLR_E_WRITE;
    goto _fail;
  }

  if (atomic_load_32(&wlrMgr.closeRequested) != 0) wlrRecorderClose();

  tscInfo("WLR: recording started, dir:%s buffer:%dMB pid:%" PRIu64, wlrMgr.recordDir, cfg->bufferSizeMb,
          wlrMgr.pid);
  return WLR_E_OK;

_fail:
  atomic_store_32(&wlrMgr.closeRequested, 0);
  atomic_store_32(&wlrMgr.closeStarted, 1);
  atomic_store_32(&wlrMgr.state, WLR_STATE_STOPPING);
  if (writerCleanupNeeded) {
    (void)wlrWriterStop(&wlrMgr, false);
    if (atomic_load_32(&wlrMgr.writerDeferred) != 0) {
      atomic_store_32(&wlrMgr.writerCloseHandedOff, 1);
      return code;
    }
    if (atomic_load_64(&wlrMgr.commitSeq) < atomic_load_64(&wlrMgr.nextEventSeq)) {
      atomic_store_32(&wlrMgr.captureIncomplete, 1);
    }
    wlrCloseSegmentPublic(&wlrMgr);
  }
  if (queueReady) wlrQueueDestroy(&wlrMgr);

  if (wlrMgr.lockReady) {
    (void)taosThreadMutexDestroy(&wlrMgr.lock);
    wlrMgr.lockReady = false;
  }
  atomic_store_32(&wlrMgr.state, WLR_STATE_OFF);

  tscError("WLR: recorder init failed, code:%d, recording disabled, client continues", code);
  return code;
}

void wlrRecorderClose(void) {
  int32_t st = atomic_load_32(&wlrMgr.state);

  if (st == WLR_STATE_OFF) return;
  atomic_store_32(&wlrMgr.closeRequested, 1);
  st = atomic_load_32(&wlrMgr.state);
  if (st == WLR_STATE_OFF) return;
  if (st == WLR_STATE_STARTING) return;
  if (atomic_exchange_32(&wlrMgr.closeStarted, 1) != 0) return;

  /* From RUNNING go through STOPPING so producers stop getting tokens while the
   * queue drains. From FAILED_OPEN there is nothing to drain. */
  if (st == WLR_STATE_RUNNING) {
    (void)wlrTrySetState(WLR_STATE_RUNNING, WLR_STATE_STOPPING);
  }

  /* Wake anyone blocked on buffer space or a begin ack: without this a producer
   * waiting for a writer that is shutting down would never return (DS 6.13). */
  wlrQueueWakeAll(&wlrMgr);
  wlrQueueWaitProducers(&wlrMgr);

  bool wasStopping = atomic_load_32(&wlrMgr.state) == WLR_STATE_STOPPING;
  atomic_store_32(&wlrMgr.writerFinalize, 1);
  (void)wlrWriterStop(&wlrMgr, wasStopping);
  if (atomic_load_32(&wlrMgr.writerDeferred) != 0) {
    atomic_store_32(&wlrMgr.writerCloseHandedOff, 1);
    return;
  }
  wlrQueueDestroy(&wlrMgr);

  if (wlrMgr.lockReady) {
    (void)taosThreadMutexDestroy(&wlrMgr.lock);
    wlrMgr.lockReady = false;
  }

  /* Clear the request before publishing OFF. closeStarted deliberately remains
   * set until the next successful OFF -> STARTING transition, preventing another
   * closer from entering this cleanup while state is still STOPPING. */
  atomic_store_32(&wlrMgr.closeRequested, 0);
  atomic_store_32(&wlrMgr.state, WLR_STATE_OFF);
  tscInfo("WLR: recording stopped, events:%" PRId64 " bytes:%" PRId64,
          atomic_load_64(&wlrMgr.eventsWritten), atomic_load_64(&wlrMgr.bytesWritten));
}

bool wlrRecorderReleaseLog(void) { return wlrWriterReleaseLog(&wlrMgr); }

/*
 * Entry point from taos_init_imp(). Kept here so the caller does not need to
 * know the config globals or assemble SWorkloadRecorderCfg.
 */
int32_t wlrRecorderInitFromCfg(void) {
  SWorkloadRecorderCfg cfg = {0};
  cfg.mode = tsWorkloadRecord;
  cfg.bufferSizeMb = tsWorkloadRecordBufferSize;
  tstrncpy(cfg.recordDir, tsWorkloadRecordDir, sizeof(cfg.recordDir));
  return wlrRecorderInit(&cfg);
}

/* ------------------------------------------------------------- test hooks */

#ifdef WLR_TEST_HOOKS
int32_t  wlrTestCurrentDepth(void) { return wlrTlsDepth; }
uint64_t wlrTestLastEventSeq(void) { return (uint64_t)atomic_load_64(&wlrMgr.nextEventSeq); }
uint64_t wlrTestDroppedEventCount(void) { return (uint64_t)atomic_load_64(&wlrMgr.droppedEvents); }
int64_t  wlrTestEventsWritten(void) { return atomic_load_64(&wlrMgr.eventsWritten); }
int64_t  wlrTestBackpressureNs(void) { return atomic_load_64(&wlrMgr.backpressureNs); }
int32_t  wlrTestAdmissionWaiters(void) { return atomic_load_32(&wlrMgr.admissionWaiters); }

int32_t wlrTestActiveCallCount(void) {
  int32_t count = 0;
  if (!wlrMgr.activeLockReady) return 0;
  (void)taosThreadMutexLock(&wlrMgr.activeLock);
  count = wlrMgr.activeCount;
  (void)taosThreadMutexUnlock(&wlrMgr.activeLock);
  return count;
}

/*
 * Clear this thread's public-API depth and pending asynchronous handoffs.
 *
 * Only for tests that intentionally leave a call unfinished, mimicking a hung API.
 * A real client never needs this: an unreturned call means the process is stuck
 * inside it, and process exit discards the thread anyway.
 */
void wlrTestResetDepth(void) {
  wlrTlsDepth = 0;
  wlrTlsPendingAsyncDepth = 0;
  wlrTlsPendingAsyncOverflow = 0;
  wlrTlsPendingAsyncClearDebt = 0;
  memset(wlrTlsPendingAsync, 0, sizeof(wlrTlsPendingAsync));
}

uint64_t wlrTokenEventSeq(const SWorkloadCallToken *token) { return token == NULL ? 0 : token->eventSeq; }
#endif
