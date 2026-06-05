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

#include "streamInt.h"
#include "osSleep.h"
#include "ttimer.h"

int32_t streamTimerInit(void** ppTimer) {
  *ppTimer = taosTmrInit(1000, 100, 10000, "STREAM");
  if (*ppTimer == NULL) {
    stError("init stream timer failed, code:%s", tstrerror(terrno));
    return -1;
  }

  stInfo("init stream timer, %p", &ppTimer);
  return 0;
}

int32_t streamTimerGetInstance(tmr_h* pTmr) {
  *pTmr = gStreamMgmt.timer;
  return TSDB_CODE_SUCCESS;
}

void streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, const char* pMsg) {
  // Refuse to (re-)arm any stream timer once cleanup has been signaled. Without
  // this gate, the timer callbacks (streamHbStart, stTriggerTaskCheckWaitSession)
  // would keep re-scheduling themselves from their _exit path and race with
  // streamMgmtCleanup() / stTriggerTaskEnvCleanup() that frees the very objects
  // those callbacks dereference, producing an ASan UAF in atomic_store_32 /
  // taosWUnLockLatch (see streamUtil.c stmHbAddStreamStatus).
  if (atomic_load_8(&gStreamMgmt.tmrStopped)) {
    stTrace("stream timer stopped, skip start %s tmr", pMsg);
    return;
  }

  if (*pTmrId == NULL) {
    *pTmrId = taosTmrStart(fp, mseconds, pParam, pHandle);
    if (*pTmrId == NULL) {
      stError("start %s tmr failed, code:%s", pMsg, tstrerror(terrno));
      return;
    }
  } else {
    bool ret = taosTmrReset(fp, mseconds, pParam, pHandle, pTmrId);
    if (ret) {
      stError("start %s tmr failed, code:%s", pMsg, tstrerror(terrno));
      return;
    }
  }

  stTrace("start %s tmr succ", pMsg);
}

void streamTmrStop(tmr_h tmrId) {
  bool stop = taosTmrStop(tmrId);
  if (stop) {
    // todo
  }
}

// Spin-wait until every stream timer callback that may have already been
// dispatched (and is therefore beyond the reach of taosTmrStop) has finished.
//
// Caller MUST set gStreamMgmt.tmrStopped = 1 before invoking this helper,
// otherwise a callback could re-arm itself after we observe zero inflight and
// race with subsequent cleanup.
//
// On hard-cap timeout we log an error and return rather than abort: aborting
// during dnode shutdown would mask the real defect with a SIGABRT crash dump.
// The hard cap is a last-resort safety valve to keep shutdown from hanging
// forever; if it ever trips, the error log is the signal to investigate the
// stuck callback.
void streamTmrWaitAllCallbacks(void) {
  const int32_t poll_ms       = 10;
  const int32_t warn_step_ms  = 1000;
  const int32_t hard_cap_ms   = 30000;

  int32_t waited = 0;
  int32_t inflight;
  while ((inflight = atomic_load_32(&gStreamMgmt.tmrInflight)) > 0) {
    if (waited == 0) {
      stDebug("waiting for stream timer callbacks to drain, inflight:%d", inflight);
    } else if (waited % warn_step_ms == 0) {
      stWarn("still waiting for stream timer callbacks, inflight:%d, waited:%dms",
             inflight, waited);
    }
    if (waited >= hard_cap_ms) {
      stError("stream timer callbacks drain timeout, inflight:%d still running after %dms, giving up "
              "to avoid blocking shutdown indefinitely (subsequent cleanup may race)",
              inflight, waited);
      return;
    }
    taosMsleep(poll_ms);
    waited += poll_ms;
  }
  stDebug("stream timer callbacks drained after %dms", waited);
}


void streamTimerCleanUp() {
  stInfo("cleanup stream timer, %p", gStreamMgmt.timer);

  // NOTE: tmrStopped has already been set by streamCleanup() before any
  // module-level cleanup ran, and streamTmrWaitAllCallbacks() has already
  // drained every in-flight callback.  At this point no callback can be
  // executing or pending in the wheel, so it is safe to tear the module down.
  taosTmrCleanUp(gStreamMgmt.timer);
  gStreamMgmt.timer = NULL;
}

