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

#include "syncEnv.h"
#include <assert.h>

SSyncEnv *gSyncEnv = NULL;

// local function -----------------
static SSyncEnv *doSyncEnvStart();
static int32_t   doSyncEnvStop(SSyncEnv *pSyncEnv);
static int32_t   doSyncEnvStartTimer(SSyncEnv *pSyncEnv);
static int32_t   doSyncEnvStopTimer(SSyncEnv *pSyncEnv);
static void      syncEnvTick(void *param, void *tmrId);
// --------------------------------

int32_t syncEnvStart() {
  int32_t ret = 0;
  taosSeedRand(taosGetTimestampSec());
  gSyncEnv = doSyncEnvStart(gSyncEnv);
  assert(gSyncEnv != NULL);
  return ret;
}

int32_t syncEnvStop() {
  int32_t ret = doSyncEnvStop(gSyncEnv);
  return ret;
}

int32_t syncEnvStartTimer() {
  int32_t ret = doSyncEnvStartTimer(gSyncEnv);
  return ret;
}

int32_t syncEnvStopTimer() {
  int32_t ret = doSyncEnvStopTimer(gSyncEnv);
  return ret;
}

// local function -----------------
static void syncEnvTick(void *param, void *tmrId) {
  SSyncEnv *pSyncEnv = (SSyncEnv *)param;
  if (atomic_load_64(&pSyncEnv->envTickTimerLogicClockUser) <= atomic_load_64(&pSyncEnv->envTickTimerLogicClock)) {
    ++(pSyncEnv->envTickTimerCounter);
    sTrace("syncEnvTick do ... envTickTimerLogicClockUser:%" PRIu64 ", envTickTimerLogicClock:%" PRIu64
           ", envTickTimerCounter:%" PRIu64
           ", "
           "envTickTimerMS:%d, tmrId:%p",
           pSyncEnv->envTickTimerLogicClockUser, pSyncEnv->envTickTimerLogicClock, pSyncEnv->envTickTimerCounter,
           pSyncEnv->envTickTimerMS, tmrId);

    // do something, tick ...
    taosTmrReset(syncEnvTick, pSyncEnv->envTickTimerMS, pSyncEnv, pSyncEnv->pTimerManager, &pSyncEnv->pEnvTickTimer);
  } else {
    sTrace("syncEnvTick pass ... envTickTimerLogicClockUser:%" PRIu64 ", envTickTimerLogicClock:%" PRIu64
           ", envTickTimerCounter:%" PRIu64
           ", "
           "envTickTimerMS:%d, tmrId:%p",
           pSyncEnv->envTickTimerLogicClockUser, pSyncEnv->envTickTimerLogicClock, pSyncEnv->envTickTimerCounter,
           pSyncEnv->envTickTimerMS, tmrId);
  }
}

static SSyncEnv *doSyncEnvStart() {
  SSyncEnv *pSyncEnv = (SSyncEnv *)taosMemoryMalloc(sizeof(SSyncEnv));
  assert(pSyncEnv != NULL);
  memset(pSyncEnv, 0, sizeof(SSyncEnv));

  pSyncEnv->envTickTimerCounter = 0;
  pSyncEnv->envTickTimerMS = ENV_TICK_TIMER_MS;
  pSyncEnv->FpEnvTickTimer = syncEnvTick;
  atomic_store_64(&pSyncEnv->envTickTimerLogicClock, 0);
  atomic_store_64(&pSyncEnv->envTickTimerLogicClockUser, 0);

  // start tmr thread
  pSyncEnv->pTimerManager = taosTmrInit(1000, 50, 10000, "SYNC-ENV");
  return pSyncEnv;
}

static int32_t doSyncEnvStop(SSyncEnv *pSyncEnv) {
  taosTmrCleanUp(pSyncEnv->pTimerManager);
  return 0;
}

static int32_t doSyncEnvStartTimer(SSyncEnv *pSyncEnv) {
  int32_t ret = 0;
  taosTmrReset(pSyncEnv->FpEnvTickTimer, pSyncEnv->envTickTimerMS, pSyncEnv, pSyncEnv->pTimerManager,
               &pSyncEnv->pEnvTickTimer);
  atomic_store_64(&pSyncEnv->envTickTimerLogicClock, pSyncEnv->envTickTimerLogicClockUser);
  return ret;
}

static int32_t doSyncEnvStopTimer(SSyncEnv *pSyncEnv) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncEnv->envTickTimerLogicClockUser, 1);
  taosTmrStop(pSyncEnv->pEnvTickTimer);
  pSyncEnv->pEnvTickTimer = NULL;
  return ret;
}
