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

SSyncEnv *syncEnv() { return &gSyncEnv; }

bool syncIsInit() { return atomic_load_8(&gSyncEnv.isStart); }

int32_t syncInit() {
  if (syncIsInit()) return 0;

  uint32_t seed = (uint32_t)(taosGetTimestampNs() & 0x00000000FFFFFFFF);
  taosSeedRand(seed);

  (void)memset(&gSyncEnv, 0, sizeof(SSyncEnv));
  gSyncEnv.pTimerManager = taosTmrInit(1000, 50, 10000, "SYNC-ENV");

  gNodeRefId = taosOpenRef(200, (RefFp)syncNodeClose);
  if (gNodeRefId < 0) {
    sError("failed to init node rset");
    syncCleanUp();
    return TSDB_CODE_SYN_WRONG_REF;
  }
  sDebug("sync node rset is open, rsetId:%d", gNodeRefId);

  gHbDataRefId = taosOpenRef(200, (RefFp)syncHbTimerDataFree);
  if (gHbDataRefId < 0) {
    sError("failed to init hbdata rset");
    syncCleanUp();
    return TSDB_CODE_SYN_WRONG_REF;
  }

  sDebug("sync hbdata rset is open, rsetId:%d", gHbDataRefId);

  atomic_store_8(&gSyncEnv.isStart, 1);
  return 0;
}

void syncCleanUp() {
  atomic_store_8(&gSyncEnv.isStart, 0);
  taosTmrCleanUp(gSyncEnv.pTimerManager);
  (void)memset(&gSyncEnv, 0, sizeof(SSyncEnv));

  if (gNodeRefId != -1) {
    sDebug("sync node rset is closed, rsetId:%d", gNodeRefId);
    taosCloseRef(gNodeRefId);
    gNodeRefId = -1;
  }

  if (gHbDataRefId != -1) {
    sDebug("sync hbdata rset is closed, rsetId:%d", gHbDataRefId);
    taosCloseRef(gHbDataRefId);
    gHbDataRefId = -1;
  }
}

int64_t syncNodeAdd(SSyncNode *pNode) {
  pNode->rid = taosAddRef(gNodeRefId, pNode);
  if (pNode->rid < 0) {
    return terrno = TSDB_CODE_SYN_WRONG_REF;
  }

  sDebug("vgId:%d, sync node refId:%" PRId64 " is added to rsetId:%d", pNode->vgId, pNode->rid, gNodeRefId);
  return pNode->rid;
}

void syncNodeRemove(int64_t rid) {
  sDebug("sync node refId:%" PRId64 " is removed from rsetId:%d", rid, gNodeRefId);
  if (rid > 0) {
    int32_t code = 0;
    if ((code = taosRemoveRef(gNodeRefId, rid)) != 0)
      sError("failed to remove sync node from refId:%" PRId64 ", rsetId:%d, since %s", rid, gNodeRefId,
             tstrerror(code));
  }
}

SSyncNode *syncNodeAcquire(int64_t rid) {
  SSyncNode *pNode = taosAcquireRef(gNodeRefId, rid);
  if (pNode == NULL) {
    sError("failed to acquire sync node from refId:%" PRId64 ", rsetId:%d", rid, gNodeRefId);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  return pNode;
}

void syncNodeRelease(SSyncNode *pNode) {
  if (pNode) {
    int32_t code = 0;
    if ((code = taosReleaseRef(gNodeRefId, pNode->rid)) != 0)
      sError("failed to release sync node from refId:%" PRId64 ", rsetId:%d, since %s", pNode->rid, gNodeRefId,
             tstrerror(code));
  }
}

int64_t syncHbTimerDataAdd(SSyncHbTimerData *pData) {
  pData->rid = taosAddRef(gHbDataRefId, pData);
  if (pData->rid < 0) {
    return terrno = TSDB_CODE_SYN_WRONG_REF;
  }

  return pData->rid;
}

void syncHbTimerDataRemove(int64_t rid) {
  if (rid > 0) {
    int32_t code = 0;
    if ((code = taosRemoveRef(gHbDataRefId, rid)) != 0)
      sError("failed to remove hbdata from refId:%" PRId64 ", rsetId:%d, since %s", rid, gHbDataRefId, tstrerror(code));
  }
}

SSyncHbTimerData *syncHbTimerDataAcquire(int64_t rid) {
  SSyncHbTimerData *pData = taosAcquireRef(gHbDataRefId, rid);
  if (pData == NULL && rid > 0) {
    sInfo("failed to acquire hbdata from refId:%" PRId64 ", rsetId:%d", rid, gHbDataRefId);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  return pData;
}

void syncHbTimerDataRelease(SSyncHbTimerData *pData) {
  if (pData) {
    int32_t code = 0;
    if ((code = taosReleaseRef(gHbDataRefId, pData->rid)) != 0) {
      sError("failed to release hbdata from refId:%" PRId64 ", rsetId:%d, since %s", pData->rid, gHbDataRefId,
             tstrerror(code));
    }
  }
}
