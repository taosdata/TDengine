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

#define _DEFAULT_SOURCE
#include "syncEnv.h"
#include "syncUtil.h"
#include "tref.h"

static SSyncEnv gSyncEnv = {0};
static int32_t  gNodeRefId = -1;
static int32_t  gHbDataRefId = -1;
static void     syncEnvTick(void *param, void *tmrId);

SSyncEnv *syncEnv() { return &gSyncEnv; }

bool syncIsInit() { return atomic_load_8(&gSyncEnv.isStart); }

int32_t syncInit() {
  if (syncIsInit()) return 0;

  uint32_t seed = (uint32_t)(taosGetTimestampNs() & 0x00000000FFFFFFFF);
  taosSeedRand(seed);

  memset(&gSyncEnv, 0, sizeof(SSyncEnv));
  gSyncEnv.envTickTimerCounter = 0;
  gSyncEnv.envTickTimerMS = ENV_TICK_TIMER_MS;
  gSyncEnv.FpEnvTickTimer = syncEnvTick;
  atomic_store_64(&gSyncEnv.envTickTimerLogicClock, 0);
  atomic_store_64(&gSyncEnv.envTickTimerLogicClockUser, 0);

  // start tmr thread
  gSyncEnv.pTimerManager = taosTmrInit(1000, 50, 10000, "SYNC-ENV");
  atomic_store_8(&gSyncEnv.isStart, 1);

  gNodeRefId = taosOpenRef(200, (RefFp)syncNodeClose);
  if (gNodeRefId < 0) {
    sError("failed to init node ref");
    syncCleanUp();
    return -1;
  }

  gHbDataRefId = taosOpenRef(200, (RefFp)syncHbTimerDataFree);
  if (gHbDataRefId < 0) {
    sError("failed to init hb-data ref");
    syncCleanUp();
    return -1;
  }

  sDebug("sync rsetId:%d is open", gNodeRefId);
  return 0;
}

void syncCleanUp() {
  atomic_store_8(&gSyncEnv.isStart, 0);
  taosTmrCleanUp(gSyncEnv.pTimerManager);
  memset(&gSyncEnv, 0, sizeof(SSyncEnv));

  if (gNodeRefId != -1) {
    sDebug("sync rsetId:%d is closed", gNodeRefId);
    taosCloseRef(gNodeRefId);
    gNodeRefId = -1;
  }

  if (gHbDataRefId != -1) {
    sDebug("sync rsetId:%d is closed", gHbDataRefId);
    taosCloseRef(gHbDataRefId);
    gHbDataRefId = -1;
  }
}

int64_t syncNodeAdd(SSyncNode *pNode) {
  pNode->rid = taosAddRef(gNodeRefId, pNode);
  if (pNode->rid < 0) return -1;

  sDebug("vgId:%d, sync rid:%" PRId64 " is added to rsetId:%d", pNode->vgId, pNode->rid, gNodeRefId);
  return pNode->rid;
}

void syncNodeRemove(int64_t rid) { taosRemoveRef(gNodeRefId, rid); }

SSyncNode *syncNodeAcquire(int64_t rid) {
  SSyncNode *pNode = taosAcquireRef(gNodeRefId, rid);
  if (pNode == NULL) {
    sError("failed to acquire node from refId:%" PRId64, rid);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  return pNode;
}

void syncNodeRelease(SSyncNode *pNode) {
  if (pNode) taosReleaseRef(gNodeRefId, pNode->rid);
}

int64_t syncHbTimerDataAdd(SSyncHbTimerData *pData) {
  pData->rid = taosAddRef(gHbDataRefId, pData);
  if (pData->rid < 0) return -1;
  return pData->rid;
}

void syncHbTimerDataRemove(int64_t rid) { taosRemoveRef(gHbDataRefId, rid); }

SSyncHbTimerData *syncHbTimerDataAcquire(int64_t rid) {
  SSyncHbTimerData *pData = taosAcquireRef(gHbDataRefId, rid);
  if (pData == NULL && rid > 0) {
    sInfo("failed to acquire hb-timer-data from refId:%" PRId64, rid);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  return pData;
}

void syncHbTimerDataRelease(SSyncHbTimerData *pData) { taosReleaseRef(gHbDataRefId, pData->rid); }

#if 0
void syncEnvStartTimer() {
  taosTmrReset(gSyncEnv.FpEnvTickTimer, gSyncEnv.envTickTimerMS, &gSyncEnv, gSyncEnv.pTimerManager,
               &gSyncEnv.pEnvTickTimer);
  atomic_store_64(&gSyncEnv.envTickTimerLogicClock, gSyncEnv.envTickTimerLogicClockUser);
}

void syncEnvStopTimer() {
  int32_t ret = 0;
  atomic_add_fetch_64(&gSyncEnv.envTickTimerLogicClockUser, 1);
  taosTmrStop(gSyncEnv.pEnvTickTimer);
  gSyncEnv.pEnvTickTimer = NULL;
  return ret;
}
#endif

static void syncEnvTick(void *param, void *tmrId) {
#if 0
  SSyncEnv *pSyncEnv = param;
  if (atomic_load_64(&gSyncEnv.envTickTimerLogicClockUser) <= atomic_load_64(&gSyncEnv.envTickTimerLogicClock)) {
    gSyncEnv.envTickTimerCounter++;
    sTrace("syncEnvTick do ... envTickTimerLogicClockUser:%" PRIu64 ", envTickTimerLogicClock:%" PRIu64
           ", envTickTimerCounter:%" PRIu64 ", envTickTimerMS:%d, tmrId:%p",
           gSyncEnv.envTickTimerLogicClockUser, gSyncEnv.envTickTimerLogicClock, gSyncEnv.envTickTimerCounter,
           gSyncEnv.envTickTimerMS, tmrId);

    // do something, tick ...
    taosTmrReset(syncEnvTick, gSyncEnv.envTickTimerMS, pSyncEnv, gSyncEnv.pTimerManager, &gSyncEnv.pEnvTickTimer);
  } else {
    sTrace("syncEnvTick pass ... envTickTimerLogicClockUser:%" PRIu64 ", envTickTimerLogicClock:%" PRIu64
           ", envTickTimerCounter:%" PRIu64 ", envTickTimerMS:%d, tmrId:%p",
           gSyncEnv.envTickTimerLogicClockUser, gSyncEnv.envTickTimerLogicClock, gSyncEnv.envTickTimerCounter,
           gSyncEnv.envTickTimerMS, tmrId);
  }
#endif
}
