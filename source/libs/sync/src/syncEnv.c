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
static void    syncEnvTick(void *param, void *tmrId);
static int32_t doSyncEnvStart(SSyncEnv *pSyncEnv);
static int32_t doSyncEnvStop(SSyncEnv *pSyncEnv);
static tmr_h   doSyncEnvStartTimer(SSyncEnv *pSyncEnv, TAOS_TMR_CALLBACK fp, int mseconds, void *param);
static void    doSyncEnvStopTimer(SSyncEnv *pSyncEnv, tmr_h *pTimer);
// --------------------------------

int32_t syncEnvStart() {
  int32_t ret;
  gSyncEnv = (SSyncEnv *)malloc(sizeof(SSyncEnv));
  assert(gSyncEnv != NULL);
  ret = doSyncEnvStart(gSyncEnv);
  return ret;
}

int32_t syncEnvStop() {
  int32_t ret = doSyncEnvStop(gSyncEnv);
  return ret;
}

tmr_h syncEnvStartTimer(TAOS_TMR_CALLBACK fp, int mseconds, void *param) {
  return doSyncEnvStartTimer(gSyncEnv, fp, mseconds, param);
}

void syncEnvStopTimer(tmr_h *pTimer) { doSyncEnvStopTimer(gSyncEnv, pTimer); }

// local function -----------------
static void syncEnvTick(void *param, void *tmrId) {
  SSyncEnv *pSyncEnv = (SSyncEnv *)param;
  sTrace("syncEnvTick ... name:%s ", pSyncEnv->name);

  pSyncEnv->pEnvTickTimer = taosTmrStart(syncEnvTick, 1000, pSyncEnv, pSyncEnv->pTimerManager);
}

static int32_t doSyncEnvStart(SSyncEnv *pSyncEnv) {
  snprintf(pSyncEnv->name, sizeof(pSyncEnv->name), "SyncEnv_%p", pSyncEnv);

  // start tmr thread
  pSyncEnv->pTimerManager = taosTmrInit(1000, 50, 10000, "SYNC-ENV");

  // pSyncEnv->pEnvTickTimer = taosTmrStart(syncEnvTick, 1000, pSyncEnv, pSyncEnv->pTimerManager);

  sTrace("SyncEnv start ok, name:%s", pSyncEnv->name);

  return 0;
}

static int32_t doSyncEnvStop(SSyncEnv *pSyncEnv) {
  taosTmrCleanUp(pSyncEnv->pTimerManager);
  return 0;
}

static tmr_h doSyncEnvStartTimer(SSyncEnv *pSyncEnv, TAOS_TMR_CALLBACK fp, int mseconds, void *param) {
  return taosTmrStart(fp, mseconds, pSyncEnv, pSyncEnv->pTimerManager);
}

static void doSyncEnvStopTimer(SSyncEnv *pSyncEnv, tmr_h *pTimer) {}
