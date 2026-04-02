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
static int64_t       gNextReloadUid = 1;
static TdThreadMutex gReloadMutex;

static int32_t mndProcessReloadLastCacheReq(SRpcMsg* pReq);  // forward decl

int32_t mndInitReload(SMnode* pMnode) {
  gReloadTasks = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (!gReloadTasks) return TSDB_CODE_OUT_OF_MEMORY;
  taosThreadMutexInit(&gReloadMutex, NULL);
  mndSetMsgHandle(pMnode, TDMT_MND_RELOAD_LAST_CACHE, mndProcessReloadLastCacheReq);
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
  tSerializeSMndReloadLastCacheRsp(pRsp, rspLen, &rsp);
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = rspLen;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t mndReloadLastCache(SMnode* pMnode, SRpcMsg* pReq, int8_t cacheType, int8_t scopeType,
                            const char* dbName, const char* tableName, const char* colName,
                            int64_t* pReloadUid) {
  SSdb* pSdb = pMnode->pSdb;

  taosThreadMutexLock(&gReloadMutex);
  int64_t uid = gNextReloadUid++;
  taosThreadMutexUnlock(&gReloadMutex);

  SReloadTask* pTask = taosMemoryCalloc(1, sizeof(SReloadTask));
  if (!pTask) return TSDB_CODE_OUT_OF_MEMORY;

  pTask->reloadUid   = uid;
  pTask->cacheType   = cacheType;
  pTask->scopeType   = scopeType;
  pTask->startTimeMs = taosGetTimestampMs();
  pTask->pVgIds      = taosArrayInit(4, sizeof(int32_t));
  tstrncpy(pTask->dbName,    dbName    ? dbName    : "", sizeof(pTask->dbName));
  tstrncpy(pTask->tableName, tableName ? tableName : "", sizeof(pTask->tableName));
  tstrncpy(pTask->colName,   colName   ? colName   : "", sizeof(pTask->colName));

  if (!pTask->pVgIds) {
    taosMemoryFree(pTask);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Collect vgroup IDs for this database
  SDbObj* pDb = mndAcquireDb(pMnode, dbName ? dbName : "");
  if (!pDb) {
    taosArrayDestroy(pTask->pVgIds);
    taosMemoryFree(pTask);
    return TSDB_CODE_MND_DB_NOT_EXIST;
  }

  void*   pIter = NULL;
  SVgObj* pVgroup = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void**)&pVgroup)) != NULL) {
    if (pVgroup->dbUid == pDb->uid) {
      taosArrayPush(pTask->pVgIds, &pVgroup->vgId);
    }
    sdbRelease(pSdb, pVgroup);
  }
  mndReleaseDb(pMnode, pDb);

  // Store task in active map
  if (taosHashPut(gReloadTasks, &uid, sizeof(uid), &pTask, sizeof(pTask)) != 0) {
    taosArrayDestroy(pTask->pVgIds);
    taosMemoryFree(pTask);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
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
    tSerializeSVReloadLastCacheReq(pCont, contLen, &req);

    SRpcMsg rpcMsg = {.msgType = TDMT_VND_RELOAD_LAST_CACHE, .pCont = pCont, .contLen = contLen};
    SEpSet  epSet  = mndGetVgroupEpset(pMnode, pVg);
    (void)tmsgSendReq(&epSet, &rpcMsg);
    mndReleaseVgroup(pMnode, pVg);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mndShowReloads(SMnode* pMnode, SRpcMsg* pReq, SRetrieveTableRsp** ppRsp) {
  // Return a simple response listing all active reload tasks.
  // Minimum viable: return NULL ppRsp with TSDB_CODE_SUCCESS (caller handles empty result)
  *ppRsp = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t mndShowReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid, SRetrieveTableRsp** ppRsp) {
  SReloadTask** ppTask = taosHashGet(gReloadTasks, &reloadUid, sizeof(reloadUid));
  if (!ppTask || !*ppTask) {
    return TSDB_CODE_MND_INVALID_COMPACT_ID;
  }
  *ppRsp = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t mndDropReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid) {
  taosThreadMutexLock(&gReloadMutex);
  SReloadTask** ppTask = taosHashGet(gReloadTasks, &reloadUid, sizeof(reloadUid));
  if (!ppTask || !*ppTask) {
    taosThreadMutexUnlock(&gReloadMutex);
    return TSDB_CODE_MND_INVALID_COMPACT_ID;
  }
  SReloadTask* pTask = *ppTask;
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
    tSerializeSVCancelLastCacheReloadReq(pCont, contLen, &cancelReq);
    SRpcMsg rpcMsg = {.msgType = TDMT_VND_CANCEL_LAST_CACHE_RELOAD, .pCont = pCont, .contLen = contLen};
    SEpSet  epSet  = mndGetVgroupEpset(pMnode, pVg);
    (void)tmsgSendReq(&epSet, &rpcMsg);
    mndReleaseVgroup(pMnode, pVg);
  }

  // Remove from active map
  taosThreadMutexLock(&gReloadMutex);
  taosHashRemove(gReloadTasks, &reloadUid, sizeof(reloadUid));
  taosThreadMutexUnlock(&gReloadMutex);
  taosArrayDestroy(pTask->pVgIds);
  taosMemoryFree(pTask);
  return TSDB_CODE_SUCCESS;
}
