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

#include "mndReload.h"
#include "mndDb.h"
#include "mndVgroup.h"
#include "tmsgcb.h"
#include "ttime.h"
#include "tuuid.h"

typedef struct SReloadTask {
  int64_t  reloadUid;
  int8_t   cacheType;
  int8_t   scopeType;
  char     dbName[TSDB_DB_NAME_LEN];
  char     tableName[TSDB_TABLE_NAME_LEN];
  char     colName[TSDB_COL_NAME_LEN];
  int64_t  startTimeMs;
  SArray*  pVgIds;  // SArray<int32_t>
} SReloadTask;

static SHashObj*     gReloadTasks = NULL;
static TdThreadMutex gReloadMutex;

static int32_t mndProcessReloadLastCacheReq(SRpcMsg* pReq);  // forward decl

// No-op handlers: reload/cancel are fire-and-forget; the RSP just needs to be routable.
static int32_t mndProcessVnodeReloadRsp(SRpcMsg* pReq)   { return TSDB_CODE_SUCCESS; }
static int32_t mndProcessVnodeCancelRsp(SRpcMsg* pReq)   { return TSDB_CODE_SUCCESS; }

int32_t mndInitReload(SMnode* pMnode) {
  gReloadTasks = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (!gReloadTasks) return TSDB_CODE_OUT_OF_MEMORY;
  taosThreadMutexInit(&gReloadMutex, NULL);
  mndSetMsgHandle(pMnode, TDMT_MND_RELOAD_LAST_CACHE,         mndProcessReloadLastCacheReq);
  mndSetMsgHandle(pMnode, TDMT_VND_RELOAD_LAST_CACHE_RSP,     mndProcessVnodeReloadRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_CANCEL_LAST_CACHE_RELOAD_RSP, mndProcessVnodeCancelRsp);
  return TSDB_CODE_SUCCESS;
}

void mndCleanupReload(SMnode* pMnode) {
  // Free all remaining tasks
  void* pIter = taosHashIterate(gReloadTasks, NULL);
  while (pIter) {
    SReloadTask** ppTask = pIter;
    if (*ppTask) {
      taosArrayDestroy((*ppTask)->pVgIds);
      taosMemoryFree(*ppTask);
    }
    pIter = taosHashIterate(gReloadTasks, pIter);
  }
  taosHashCleanup(gReloadTasks);
  gReloadTasks = NULL;
  taosThreadMutexDestroy(&gReloadMutex);
}

static int32_t mndProcessReloadLastCacheReq(SRpcMsg* pReq) {
  SMnode*                pMnode = pReq->info.node;
  SMndReloadLastCacheReq req = {0};
  int32_t                code = 0;

  code = tDeserializeSMndReloadLastCacheReq(pReq->pCont, pReq->contLen, &req);
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to deserialize SMndReloadLastCacheReq, code:%s", tstrerror(code));
    TAOS_RETURN(code);
  }

  int64_t reloadUid = 0;
  code = mndReloadLastCache(pMnode, pReq, req.cacheType, req.scopeType, req.dbName, req.tableName, req.colName,
                             &reloadUid);
  if (code != TSDB_CODE_SUCCESS) {
    TAOS_RETURN(code);
  }

  // Build and send response with reloadUid
  SMndReloadLastCacheRsp rsp = {.reloadUid = reloadUid};
  int32_t                rspLen = tSerializeSMndReloadLastCacheRsp(NULL, 0, &rsp);
  if (rspLen < 0) {
    TAOS_RETURN(rspLen);
  }
  void* pRsp = rpcMallocCont(rspLen);
  if (!pRsp) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  if ((code = tSerializeSMndReloadLastCacheRsp(pRsp, rspLen, &rsp)) < 0) {
    rpcFreeCont(pRsp);
    TAOS_RETURN(code);
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t mndReloadLastCache(SMnode* pMnode, SRpcMsg* pReq, int8_t cacheType, int8_t scopeType,
                            const char* dbName, const char* tableName, const char* colName,
                            int64_t* pReloadUid) {
  SSdb*        pSdb = pMnode->pSdb;
  int32_t      code = TSDB_CODE_SUCCESS;
  SReloadTask* pTask = NULL;

  SReloadTask* pNewTask = taosMemoryCalloc(1, sizeof(SReloadTask));
  if (!pNewTask) return TSDB_CODE_OUT_OF_MEMORY;

  pNewTask->cacheType   = cacheType;
  pNewTask->scopeType   = scopeType;
  pNewTask->startTimeMs = taosGetTimestampMs();
  pNewTask->pVgIds      = taosArrayInit(4, sizeof(int32_t));
  tstrncpy(pNewTask->dbName,    dbName    ? dbName    : "", sizeof(pNewTask->dbName));
  tstrncpy(pNewTask->tableName, tableName ? tableName : "", sizeof(pNewTask->tableName));
  tstrncpy(pNewTask->colName,   colName   ? colName   : "", sizeof(pNewTask->colName));

  if (!pNewTask->pVgIds) {
    taosMemoryFree(pNewTask);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Collect vgroup IDs for this database
  SDbObj* pDb = mndAcquireDb(pMnode, dbName ? dbName : "");
  if (!pDb) {
    taosArrayDestroy(pNewTask->pVgIds);
    taosMemoryFree(pNewTask);
    return TSDB_CODE_MND_DB_NOT_EXIST;
  }

  void*   pIter = NULL;
  SVgObj* pVgroup = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup)) != NULL) {
    if (pVgroup->dbUid == pDb->uid) {
      if (taosArrayPush(pNewTask->pVgIds, &pVgroup->vgId) == NULL) {
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        mndReleaseDb(pMnode, pDb);
        taosArrayDestroy(pNewTask->pVgIds);
        taosMemoryFree(pNewTask);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    sdbRelease(pSdb, pVgroup);
  }
  mndReleaseDb(pMnode, pDb);

  // Assign UID and store task atomically under mutex
  taosThreadMutexLock(&gReloadMutex);
  int64_t uid = tGenIdPI64();
  pNewTask->reloadUid = uid;
  code = taosHashPut(gReloadTasks, &uid, sizeof(uid), &pNewTask, sizeof(pNewTask));
  taosThreadMutexUnlock(&gReloadMutex);

  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pNewTask->pVgIds);
    taosMemoryFree(pNewTask);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pTask = pNewTask;
  *pReloadUid = uid;

  // Dispatch reload request to each vgroup
  SVReloadLastCacheReq req = {
    .reloadUid = uid,
    .cacheType = cacheType,
    .cid       = -1,
  };

  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pTask->pVgIds); i++) {
    int32_t vgId = *(int32_t*)taosArrayGet(pTask->pVgIds, i);
    SVgObj* pVg  = mndAcquireVgroup(pMnode, vgId);
    if (!pVg) continue;

    int32_t contLen = tSerializeSVReloadLastCacheReq(NULL, 0, &req);
    if (contLen < 0) { mndReleaseVgroup(pMnode, pVg); continue; }
    void* pCont = rpcMallocCont(contLen);
    if (!pCont) { mndReleaseVgroup(pMnode, pVg); continue; }
    if (tSerializeSVReloadLastCacheReq(pCont, contLen, &req) < 0) {
      rpcFreeCont(pCont);
      mndReleaseVgroup(pMnode, pVg);
      continue;
    }

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_RELOAD_LAST_CACHE, .pCont = pCont, .contLen = contLen};
    SEpSet  epSet  = mndGetVgroupEpset(pMnode, pVg);
    int32_t ret    = tmsgSendReq(&epSet, &rpcMsg);
    if (ret != TSDB_CODE_SUCCESS) {
      mWarn("reloadUid:%" PRId64 ", failed to send reload-last-cache to vgId:%d, code:%s",
            uid, vgId, tstrerror(ret));
    }
    mndReleaseVgroup(pMnode, pVg);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mndShowReloads(SMnode* pMnode, SRpcMsg* pReq, SRetrieveTableRsp** ppRsp) {
  *ppRsp = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t mndShowReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid, SRetrieveTableRsp** ppRsp) {
  taosThreadMutexLock(&gReloadMutex);
  SReloadTask** ppTask = taosHashGet(gReloadTasks, &reloadUid, sizeof(reloadUid));
  bool          found  = (ppTask != NULL && *ppTask != NULL);
  taosThreadMutexUnlock(&gReloadMutex);

  if (!found) {
    return TSDB_CODE_MND_INVALID_COMPACT_ID;  // TODO: define TSDB_CODE_MND_INVALID_RELOAD_ID
  }
  *ppRsp = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t mndDropReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid) {
  // Atomically remove the task from the map so concurrent DROP calls cannot
  // both win the race (prevents double-free and use-after-free).
  taosThreadMutexLock(&gReloadMutex);
  SReloadTask** ppTask = taosHashGet(gReloadTasks, &reloadUid, sizeof(reloadUid));
  if (!ppTask || !*ppTask) {
    taosThreadMutexUnlock(&gReloadMutex);
    return TSDB_CODE_MND_INVALID_COMPACT_ID;  // TODO: define TSDB_CODE_MND_INVALID_RELOAD_ID
  }
  SReloadTask* pTask = *ppTask;
  taosHashRemove(gReloadTasks, &reloadUid, sizeof(reloadUid));
  taosThreadMutexUnlock(&gReloadMutex);

  // Send cancel signal to all vnodes
  SVCancelLastCacheReloadReq cancelReq = {.reloadUid = reloadUid};
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pTask->pVgIds); i++) {
    int32_t vgId = *(int32_t*)taosArrayGet(pTask->pVgIds, i);
    SVgObj* pVg  = mndAcquireVgroup(pMnode, vgId);
    if (!pVg) continue;
    int32_t contLen = tSerializeSVCancelLastCacheReloadReq(NULL, 0, &cancelReq);
    if (contLen < 0) { mndReleaseVgroup(pMnode, pVg); continue; }
    void* pCont = rpcMallocCont(contLen);
    if (!pCont) { mndReleaseVgroup(pMnode, pVg); continue; }
    if (tSerializeSVCancelLastCacheReloadReq(pCont, contLen, &cancelReq) < 0) {
      rpcFreeCont(pCont);
      mndReleaseVgroup(pMnode, pVg);
      continue;
    }
    SRpcMsg rpcMsg = {.msgType = TDMT_VND_CANCEL_LAST_CACHE_RELOAD, .pCont = pCont, .contLen = contLen};
    SEpSet  epSet  = mndGetVgroupEpset(pMnode, pVg);
    int32_t ret    = tmsgSendReq(&epSet, &rpcMsg);
    if (ret != TSDB_CODE_SUCCESS) {
      mWarn("reloadUid:%" PRId64 ", failed to send cancel-reload to vgId:%d, code:%s",
            reloadUid, vgId, tstrerror(ret));
    }
    mndReleaseVgroup(pMnode, pVg);
  }

  taosArrayDestroy(pTask->pVgIds);
  taosMemoryFree(pTask);
  return TSDB_CODE_SUCCESS;
}
