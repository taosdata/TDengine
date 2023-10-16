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

#include "executor.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tmisce.h"
#include "tref.h"
#include "tstream.h"
#include "ttimer.h"

#define META_HB_CHECK_INTERVAL    200
#define META_HB_SEND_IDLE_COUNTER 25  // send hb every 5 sec
#define STREAM_TASK_KEY_LEN       ((sizeof(int64_t)) << 1)

static TdThreadOnce streamMetaModuleInit = PTHREAD_ONCE_INIT;

int32_t streamBackendId = 0;
int32_t streamBackendCfWrapperId = 0;
int32_t streamMetaId = 0;

static int64_t streamGetLatestCheckpointId(SStreamMeta* pMeta);
static void    metaHbToMnode(void* param, void* tmrId);
static void    streamMetaClear(SStreamMeta* pMeta);
static int32_t streamMetaBegin(SStreamMeta* pMeta);
static void    streamMetaCloseImpl(void* arg);
static void    extractStreamTaskKey(int64_t* pKey, const SStreamTask* pTask);

typedef struct {
  TdThreadMutex mutex;
  SHashObj*     pTable;
} SMetaRefMgt;

SMetaRefMgt gMetaRefMgt;

void    metaRefMgtInit();
void    metaRefMgtCleanup();
int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid);

static void streamMetaEnvInit() {
  streamBackendId = taosOpenRef(64, streamBackendCleanup);
  streamBackendCfWrapperId = taosOpenRef(64, streamBackendHandleCleanup);

  streamMetaId = taosOpenRef(64, streamMetaCloseImpl);

  metaRefMgtInit();
}

void streamMetaInit() { taosThreadOnce(&streamMetaModuleInit, streamMetaEnvInit); }
void streamMetaCleanup() {
  taosCloseRef(streamBackendId);
  taosCloseRef(streamBackendCfWrapperId);
  taosCloseRef(streamMetaId);

  metaRefMgtCleanup();
}

void metaRefMgtInit() {
  taosThreadMutexInit(&(gMetaRefMgt.mutex), NULL);
  gMetaRefMgt.pTable = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
}

void metaRefMgtCleanup() {
  void* pIter = taosHashIterate(gMetaRefMgt.pTable, NULL);
  while (pIter) {
    SArray* list = *(SArray**)pIter;
    for (int i = 0; i < taosArrayGetSize(list); i++) {
      void* rid = taosArrayGetP(list, i);
      taosMemoryFree(rid);
    }
    taosArrayDestroy(list);
    pIter = taosHashIterate(gMetaRefMgt.pTable, pIter);
  }
  taosHashCleanup(gMetaRefMgt.pTable);

  taosThreadMutexDestroy(&gMetaRefMgt.mutex);
}

int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid) {
  taosThreadMutexLock(&gMetaRefMgt.mutex);
  void* p = taosHashGet(gMetaRefMgt.pTable, &vgId, sizeof(vgId));
  if (p == NULL) {
    SArray* list = taosArrayInit(8, sizeof(void*));
    taosArrayPush(list, &rid);
    taosHashPut(gMetaRefMgt.pTable, &vgId, sizeof(vgId), &list, sizeof(void*));
  } else {
    SArray* list = *(SArray**)p;
    taosArrayPush(list, &rid);
  }
  taosThreadMutexUnlock(&gMetaRefMgt.mutex);
  return 0;
}

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId, int64_t stage) {
  int32_t      code = -1;
  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    qError("vgId:%d failed to prepare stream meta, alloc size:%" PRIzu ", out of memory", vgId, sizeof(SStreamMeta));
    return NULL;
  }

  int32_t len = strlen(path) + 64;
  char*   tpath = taosMemoryCalloc(1, len);

  sprintf(tpath, "%s%s%s", path, TD_DIRSEP, "stream");
  pMeta->path = tpath;

  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("task.db", STREAM_TASK_KEY_LEN, -1, NULL, pMeta->db, &pMeta->pTaskDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("checkpoint.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pCheckpointDb, 0) < 0) {
    goto _err;
  }

  if (streamMetaBegin(pMeta) < 0) {
    goto _err;
  }

  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);
  pMeta->pTasks = taosHashInit(64, fp, true, HASH_NO_LOCK);
  if (pMeta->pTasks == NULL) {
    goto _err;
  }

  // task list
  pMeta->pTaskList = taosArrayInit(4, sizeof(SStreamTaskId));
  if (pMeta->pTaskList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->walScanCounter = 0;
  pMeta->vgId = vgId;
  pMeta->ahandle = ahandle;
  pMeta->expandFunc = expandFunc;
  pMeta->stage = stage;

  // send heartbeat every 5sec.
  pMeta->rid = taosAddRef(streamMetaId, pMeta);
  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  *pRid = pMeta->rid;

  metaRefMgtAdd(pMeta->vgId, pRid);

  pMeta->hbInfo.hbTmr = taosTmrStart(metaHbToMnode, META_HB_CHECK_INTERVAL, pRid, streamEnv.timer);
  pMeta->hbInfo.tickCounter = 0;
  pMeta->hbInfo.stopFlag = 0;

  pMeta->pTaskBackendUnique =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  pMeta->chkpSaved = taosArrayInit(4, sizeof(int64_t));
  pMeta->chkpInUse = taosArrayInit(4, sizeof(int64_t));
  pMeta->chkpCap = 8;
  taosInitRWLatch(&pMeta->chkpDirLock);

  pMeta->chkpId = streamGetLatestCheckpointId(pMeta);
  pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId);
  while (pMeta->streamBackend == NULL) {
    taosMsleep(2 * 1000);
    pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId);
    if (pMeta->streamBackend == NULL) {
      qError("vgId:%d failed to init stream backend", pMeta->vgId);
      qInfo("vgId:%d retry to init stream backend", pMeta->vgId);
    }
  }
  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);

  code = streamBackendLoadCheckpointInfo(pMeta);

  taosInitRWLatch(&pMeta->lock);
  taosThreadMutexInit(&pMeta->backendMutex, NULL);

  pMeta->pauseTaskNum = 0;

  qInfo("vgId:%d open stream meta successfully, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, pMeta->chkpId,
        stage);
  return pMeta;

_err:
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasks) taosHashCleanup(pMeta->pTasks);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) tdbTbClose(pMeta->pTaskDb);
  if (pMeta->pCheckpointDb) tdbTbClose(pMeta->pCheckpointDb);
  if (pMeta->db) tdbClose(pMeta->db);

  // taosThreadMutexDestroy(&pMeta->backendMutex);
  //  taosThreadRwlockDestroy(&pMeta->lock);

  taosMemoryFree(pMeta);

  qError("failed to open stream meta");
  return NULL;
}

int32_t streamMetaReopen(SStreamMeta* pMeta, int64_t chkpId) {
  streamMetaClear(pMeta);

  pMeta->streamBackendRid = -1;
  pMeta->streamBackend = NULL;

  char* defaultPath = taosMemoryCalloc(1, strlen(pMeta->path) + 128);
  sprintf(defaultPath, "%s%s%s", pMeta->path, TD_DIRSEP, "state");
  taosRemoveDir(defaultPath);

  char* newPath = taosMemoryCalloc(1, strlen(pMeta->path) + 128);
  sprintf(newPath, "%s%s%s", pMeta->path, TD_DIRSEP, "received");

  int32_t code = taosStatFile(newPath, NULL, NULL, NULL);
  if (code == 0) {
    // directory exists
    code = taosRenameFile(newPath, defaultPath);
    if (code != 0) {
      terrno = TAOS_SYSTEM_ERROR(code);
      qError("vgId:%d failed to rename file, from %s to %s, code:%s", pMeta->vgId, newPath, defaultPath,
             tstrerror(terrno));

      taosMemoryFree(defaultPath);
      taosMemoryFree(newPath);
      return -1;
    }
  }

  pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId);
  while (pMeta->streamBackend == NULL) {
    taosMsleep(2 * 1000);
    pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId);
    if (pMeta->streamBackend == NULL) {
      qError("vgId:%d failed to init stream backend", pMeta->vgId);
      qInfo("vgId:%d retry to init stream backend", pMeta->vgId);
      // return -1;
    }
  }
  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);
  streamBackendLoadCheckpointInfo(pMeta);

  taosMemoryFree(defaultPath);
  taosMemoryFree(newPath);
  return 0;
}

void streamMetaClear(SStreamMeta* pMeta) {
  void* pIter = NULL;
  while ((pIter = taosHashIterate(pMeta->pTasks, pIter)) != NULL) {
    SStreamTask* p = *(SStreamTask**)pIter;

    // release the ref by timer
    if (p->info.triggerParam != 0 && p->info.fillHistory == 0) {  // one more ref in timer
      qDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", p->id.idStr, p->refCnt);
      taosTmrStop(p->schedInfo.pTimer);
      p->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, p);
    }

    streamMetaReleaseTask(pMeta, p);
  }

  taosRemoveRef(streamBackendId, pMeta->streamBackendRid);

  taosHashClear(pMeta->pTasks);
  taosHashClear(pMeta->pTaskBackendUnique);

  taosArrayClear(pMeta->pTaskList);
  taosArrayClear(pMeta->chkpSaved);
  taosArrayClear(pMeta->chkpInUse);
}

void streamMetaClose(SStreamMeta* pMeta) {
  qDebug("start to close stream meta");
  if (pMeta == NULL) {
    return;
  }

  // int64_t rid = *(int64_t*)pMeta->pRid;
  // if (taosTmrStop(pMeta->hbInfo.hbTmr)) {
  //   taosMemoryFree(pMeta->pRid);
  // } else {
  //   // do nothing, stop by timer thread
  // }
  taosRemoveRef(streamMetaId, pMeta->rid);
}

void streamMetaCloseImpl(void* arg) {
  SStreamMeta* pMeta = arg;
  qDebug("start to do-close stream meta");
  if (pMeta == NULL) {
    return;
  }

  streamMetaClear(pMeta);

  tdbAbort(pMeta->db, pMeta->txn);
  tdbTbClose(pMeta->pTaskDb);
  tdbTbClose(pMeta->pCheckpointDb);
  tdbClose(pMeta->db);

  taosArrayDestroy(pMeta->pTaskList);
  taosArrayDestroy(pMeta->chkpSaved);
  taosArrayDestroy(pMeta->chkpInUse);

  taosHashCleanup(pMeta->pTasks);
  taosHashCleanup(pMeta->pTaskBackendUnique);

  taosMemoryFree(pMeta->path);
  taosThreadMutexDestroy(&pMeta->backendMutex);

  taosMemoryFree(pMeta);
  qDebug("end to close stream meta");
}

int32_t streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  void*   buf = NULL;
  int32_t len;
  int32_t code;
  pTask->ver = SSTREAM_TASK_VER;
  tEncodeSize(tEncodeStreamTask, pTask, len, code);
  if (code < 0) {
    return -1;
  }
  buf = taosMemoryCalloc(1, len);
  if (buf == NULL) {
    return -1;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  int64_t key[2] = {0};
  extractStreamTaskKey(key, pTask);

  if (tdbTbUpsert(pMeta->pTaskDb, key, STREAM_TASK_KEY_LEN, buf, len, pMeta->txn) < 0) {
    qError("s-task:%s save to disk failed, code:%s", pTask->id.idStr, tstrerror(terrno));
    return -1;
  }

  taosMemoryFree(buf);
  return 0;
}

void extractStreamTaskKey(int64_t* pKey, const SStreamTask* pTask) {
  pKey[0] = pTask->id.streamId;
  pKey[1] = pTask->id.taskId;
}

int32_t streamMetaRemoveTask(SStreamMeta* pMeta, int64_t* pKey) {
  int32_t code = tdbTbDelete(pMeta->pTaskDb, pKey, STREAM_TASK_KEY_LEN, pMeta->txn);
  if (code != 0) {
    qError("vgId:%d failed to remove task:0x%x from metastore, code:%s", pMeta->vgId, (int32_t)pKey[1],
           tstrerror(terrno));
  } else {
    qDebug("vgId:%d remove task:0x%x from metastore", pMeta->vgId, (int32_t)pKey[1]);
  }

  return code;
}

// add to the ready tasks hash map, not the restored tasks hash map
int32_t streamMetaRegisterTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask, bool* pAdded) {
  *pAdded = false;

  int64_t keys[2] = {pTask->id.streamId, pTask->id.taskId};
  void*   p = taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (p == NULL) {
    if (pMeta->expandFunc(pMeta->ahandle, pTask, ver) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }

    taosArrayPush(pMeta->pTaskList, &pTask->id);

    if (streamMetaSaveTask(pMeta, pTask) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }

    if (streamMetaCommit(pMeta) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }
  } else {
    return 0;
  }

  taosHashPut(pMeta->pTasks, keys, sizeof(keys), &pTask, POINTER_BYTES);
  *pAdded = true;
  return 0;
}

int32_t streamMetaGetNumOfTasks(SStreamMeta* pMeta) {
  size_t size = taosHashGetSize(pMeta->pTasks);
  ASSERT(taosArrayGetSize(pMeta->pTaskList) == taosHashGetSize(pMeta->pTasks));
  return (int32_t)size;
}

int32_t streamMetaGetNumOfStreamTasks(SStreamMeta* pMeta) {
  int32_t num = 0;
  size_t  size = taosArrayGetSize(pMeta->pTaskList);
  for (int32_t i = 0; i < size; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    int64_t        keys[2] = {pId->streamId, pId->taskId};

    SStreamTask** p = taosHashGet(pMeta->pTasks, keys, sizeof(keys));
    if (p == NULL) {
      continue;
    }

    if ((*p)->info.fillHistory == 0) {
      num += 1;
    }
  }

  return num;
}

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  taosRLockLatch(&pMeta->lock);

  int64_t       keys[2] = {streamId, taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (ppTask != NULL) {
    if (!streamTaskShouldStop(&(*ppTask)->status)) {
      int32_t ref = atomic_add_fetch_32(&(*ppTask)->refCnt, 1);
      taosRUnLockLatch(&pMeta->lock);
      qTrace("s-task:%s acquire task, ref:%d", (*ppTask)->id.idStr, ref);
      return *ppTask;
    }
  }

  taosRUnLockLatch(&pMeta->lock);
  return NULL;
}

void streamMetaReleaseTask(SStreamMeta* UNUSED_PARAM(pMeta), SStreamTask* pTask) {
  int32_t ref = atomic_sub_fetch_32(&pTask->refCnt, 1);
  if (ref > 0) {
    qTrace("s-task:%s release task, ref:%d", pTask->id.idStr, ref);
  } else if (ref == 0) {
    ASSERT(streamTaskShouldStop(&pTask->status));
    qTrace("s-task:%s all refs are gone, free it", pTask->id.idStr);
    tFreeStreamTask(pTask);
  } else if (ref < 0) {
    qError("task ref is invalid, ref:%d, %s", ref, pTask->id.idStr);
  }
}

static void doRemoveIdFromList(SStreamMeta* pMeta, int32_t num, SStreamTaskId* id) {
  for (int32_t i = 0; i < num; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);
    if (pTaskId->streamId == id->streamId && pTaskId->taskId == id->taskId) {
      taosArrayRemove(pMeta->pTaskList, i);
      break;
    }
  }
}

int32_t streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  SStreamTask* pTask = NULL;

  // pre-delete operation
  taosWLockLatch(&pMeta->lock);

  int64_t       keys[2] = {streamId, taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (ppTask) {
    pTask = *ppTask;
    if (streamTaskShouldPause(&pTask->status)) {
      int32_t num = atomic_sub_fetch_32(&pMeta->pauseTaskNum, 1);
      qInfo("vgId:%d s-task:%s drop stream task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);
    }
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__DROPPING);
  } else {
    qDebug("vgId:%d failed to find the task:0x%x, it may be dropped already", pMeta->vgId, taskId);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }
  taosWUnLockLatch(&pMeta->lock);

  qDebug("s-task:0x%x set task status:%s and start to unregister it", taskId,
         streamGetTaskStatusStr(TASK_STATUS__DROPPING));

  while (1) {
    taosRLockLatch(&pMeta->lock);
    ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, keys, sizeof(keys));

    if (ppTask) {
      if ((*ppTask)->status.timerActive == 0) {
        taosRUnLockLatch(&pMeta->lock);
        break;
      }

      taosMsleep(10);
      qDebug("s-task:%s wait for quit from timer", (*ppTask)->id.idStr);
      taosRUnLockLatch(&pMeta->lock);
    } else {
      taosRUnLockLatch(&pMeta->lock);
      break;
    }
  }

  // let's do delete of stream task
  taosWLockLatch(&pMeta->lock);
  ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, keys, sizeof(keys));
  if (ppTask) {
    taosHashRemove(pMeta->pTasks, keys, sizeof(keys));
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__DROPPING);

    ASSERT(pTask->status.timerActive == 0);
    doRemoveIdFromList(pMeta, (int32_t)taosArrayGetSize(pMeta->pTaskList), &pTask->id);

    if (pTask->info.triggerParam != 0 && pTask->info.fillHistory == 0) {
      qDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", pTask->id.idStr, pTask->refCnt);
      taosTmrStop(pTask->schedInfo.pTimer);
      pTask->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, pTask);
    }

    streamMetaRemoveTask(pMeta, keys);
    streamMetaReleaseTask(pMeta, pTask);
  } else {
    qDebug("vgId:%d failed to find the task:0x%x, it may have been dropped already", pMeta->vgId, taskId);
  }

  taosWUnLockLatch(&pMeta->lock);
  return 0;
}

int32_t streamMetaBegin(SStreamMeta* pMeta) {
  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }
  return 0;
}

// todo add error log
int32_t streamMetaCommit(SStreamMeta* pMeta) {
  if (tdbCommit(pMeta->db, pMeta->txn) < 0) {
    qError("vgId:%d failed to commit stream meta", pMeta->vgId);
    return -1;
  }

  if (tdbPostCommit(pMeta->db, pMeta->txn) < 0) {
    qError("vgId:%d failed to do post-commit stream meta", pMeta->vgId);
    return -1;
  }

  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    qError("vgId:%d failed to begin trans", pMeta->vgId);
    return -1;
  }

  return 0;
}

int64_t streamGetLatestCheckpointId(SStreamMeta* pMeta) {
  int64_t chkpId = 0;

  TBC* pCur = NULL;
  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    return chkpId;
  }

  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;

  tdbTbcMoveToFirst(pCur);
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    if (pVal == NULL || vLen == 0) {
      break;
    }
    SCheckpointInfo info;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTaskChkInfo(&decoder, &info) < 0) {
      continue;
    }
    tDecoderClear(&decoder);

    chkpId = TMAX(chkpId, info.checkpointId);
  }

  qDebug("get max chkp id: %" PRId64 "", chkpId);

  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);

  return chkpId;
}

static void doClear(void* pKey, void* pVal, TBC* pCur, SArray* pRecycleList) {
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  taosArrayDestroy(pRecycleList);
}

int32_t streamMetaLoadAllTasks(SStreamMeta* pMeta) {
  TBC* pCur = NULL;

  qInfo("vgId:%d load stream tasks from meta files", pMeta->vgId);
  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    qError("vgId:%d failed to open stream meta, code:%s", pMeta->vgId, tstrerror(terrno));
    return -1;
  }

  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;
  SArray*  pRecycleList = taosArrayInit(4, STREAM_TASK_KEY_LEN);

  tdbTbcMoveToFirst(pCur);
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      doClear(pKey, pVal, pCur, pRecycleList);
      return -1;
    }

    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTask(&decoder, pTask) < 0) {
      tDecoderClear(&decoder);
      doClear(pKey, pVal, pCur, pRecycleList);
      tFreeStreamTask(pTask);
      qError(
          "stream read incompatible data, rm %s/vnode/vnode*/tq/stream if taosd cannot start, and rebuild stream "
          "manually",
          tsDataDir);
      return -1;
    }
    tDecoderClear(&decoder);

    if (pTask->status.taskStatus == TASK_STATUS__DROPPING) {
      int32_t taskId = pTask->id.taskId;
      tFreeStreamTask(pTask);

      int64_t key[2] = {0};
      extractStreamTaskKey(key, pTask);

      taosArrayPush(pRecycleList, key);
      int32_t total = taosArrayGetSize(pRecycleList);
      qDebug("s-task:0x%x is already dropped, add into recycle list, total:%d", taskId, total);
      continue;
    }

    // do duplicate task check.
    int64_t keys[2] = {pTask->id.streamId, pTask->id.taskId};
    void*   p = taosHashGet(pMeta->pTasks, keys, sizeof(keys));
    if (p == NULL) {
      if (pMeta->expandFunc(pMeta->ahandle, pTask, pTask->chkInfo.checkpointVer) < 0) {
        doClear(pKey, pVal, pCur, pRecycleList);
        tFreeStreamTask(pTask);
        return -1;
      }

      taosArrayPush(pMeta->pTaskList, &pTask->id);
    } else {
      tdbFree(pKey);
      tdbFree(pVal);
      taosMemoryFree(pTask);
      continue;
    }

    streamTaskResetUpstreamStageInfo(pTask);
    if (taosHashPut(pMeta->pTasks, keys, sizeof(keys), &pTask, sizeof(void*)) < 0) {
      doClear(pKey, pVal, pCur, pRecycleList);
      tFreeStreamTask(pTask);
      return -1;
    }

    if (streamTaskShouldPause(&pTask->status)) {
      atomic_add_fetch_32(&pMeta->pauseTaskNum, 1);
    }

    ASSERT(pTask->status.downstreamReady == 0);
  }
  qInfo("vgId:%d pause task num:%d", pMeta->vgId, pMeta->pauseTaskNum);

  tdbFree(pKey);
  tdbFree(pVal);
  if (tdbTbcClose(pCur) < 0) {
    taosArrayDestroy(pRecycleList);
    return -1;
  }

  if (taosArrayGetSize(pRecycleList) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(pRecycleList); ++i) {
      int64_t* pId = taosArrayGet(pRecycleList, i);
      streamMetaRemoveTask(pMeta, pId);
    }
  }

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  qDebug("vgId:%d load %d tasks into meta from disk completed", pMeta->vgId, numOfTasks);
  taosArrayDestroy(pRecycleList);
  return 0;
}

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->numOfTasks) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    STaskStatusEntry* ps = taosArrayGet(pReq->pTaskStatus, i);
    if (tEncodeI64(pEncoder, ps->streamId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->status) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->numOfTasks) < 0) return -1;

  pReq->pTaskStatus = taosArrayInit(pReq->numOfTasks, sizeof(STaskStatusEntry));
  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    STaskStatusEntry hb = {0};
    if (tDecodeI64(pDecoder, &hb.streamId) < 0) return -1;
    if (tDecodeI32(pDecoder, &hb.taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &hb.status) < 0) return -1;

    taosArrayPush(pReq->pTaskStatus, &hb);
  }

  tEndDecode(pDecoder);
  return 0;
}

static bool readyToSendHb(SMetaHbInfo* pInfo) {
  if ((++pInfo->tickCounter) >= META_HB_SEND_IDLE_COUNTER) {
    // reset the counter
    pInfo->tickCounter = 0;
    return true;
  }
  return false;
}

void metaHbToMnode(void* param, void* tmrId) {
  int64_t rid = *(int64_t*)param;

  SStreamHbMsg hbMsg = {0};
  SStreamMeta* pMeta = taosAcquireRef(streamMetaId, rid);
  if (pMeta == NULL) {
    return;
  }

  // need to stop, stop now
  if (pMeta->hbInfo.stopFlag == STREAM_META_WILL_STOP) {
    pMeta->hbInfo.stopFlag = STREAM_META_OK_TO_STOP;
    qDebug("vgId:%d jump out of meta timer", pMeta->vgId);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  if (!readyToSendHb(&pMeta->hbInfo)) {
    taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamEnv.timer, &pMeta->hbInfo.hbTmr);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  taosRLockLatch(&pMeta->lock);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);

  SEpSet epset = {0};
  bool   hasValEpset = false;

  hbMsg.vgId = pMeta->vgId;
  hbMsg.pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    int64_t        keys[2] = {pId->streamId, pId->taskId};
    SStreamTask**  pTask = taosHashGet(pMeta->pTasks, keys, sizeof(keys));

    if ((*pTask)->info.fillHistory == 1) {
      continue;
    }

    STaskStatusEntry entry = {.streamId = pId->streamId, .taskId = pId->taskId, .status = (*pTask)->status.taskStatus};
    taosArrayPush(hbMsg.pTaskStatus, &entry);

    if (i == 0) {
      epsetAssign(&epset, &(*pTask)->info.mnodeEpset);
      hasValEpset = true;
    }
  }

  hbMsg.numOfTasks = taosArrayGetSize(hbMsg.pTaskStatus);
  taosRUnLockLatch(&pMeta->lock);

  if (hasValEpset) {
    int32_t code = 0;
    int32_t tlen = 0;

    tEncodeSize(tEncodeStreamHbMsg, &hbMsg, tlen, code);
    if (code < 0) {
      qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      taosArrayDestroy(hbMsg.pTaskStatus);
      taosReleaseRef(streamMetaId, rid);
      return;
    }

    void* buf = rpcMallocCont(tlen);
    if (buf == NULL) {
      qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      taosArrayDestroy(hbMsg.pTaskStatus);
      taosReleaseRef(streamMetaId, rid);
      return;
    }

    SEncoder encoder;
    tEncoderInit(&encoder, buf, tlen);
    if ((code = tEncodeStreamHbMsg(&encoder, &hbMsg)) < 0) {
      rpcFreeCont(buf);
      qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      taosArrayDestroy(hbMsg.pTaskStatus);
      taosReleaseRef(streamMetaId, rid);
      return;
    }
    tEncoderClear(&encoder);

    SRpcMsg msg = {0};
    initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);
    msg.info.noResp = 1;

    qDebug("vgId:%d, build and send hb to mnode", pMeta->vgId);
    tmsgSendReq(&epset, &msg);
  }

  taosArrayDestroy(hbMsg.pTaskStatus);
  taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamEnv.timer, &pMeta->hbInfo.hbTmr);
  taosReleaseRef(streamMetaId, rid);
}

static bool hasStreamTaskInTimer(SStreamMeta* pMeta) {
  bool inTimer = false;

  taosWLockLatch(&pMeta->lock);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->status.timerActive >= 1) {
      inTimer = true;
    }
  }

  taosWUnLockLatch(&pMeta->lock);
  return inTimer;
}

void streamMetaNotifyClose(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;

  qDebug("vgId:%d notify all stream tasks that the vnode is closing", vgId);
  taosWLockLatch(&pMeta->lock);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    qDebug("vgId:%d s-task:%s set closing flag", vgId, pTask->id.idStr);
    streamTaskStop(pTask);
  }

  taosWUnLockLatch(&pMeta->lock);

  // wait for the stream meta hb function stopping
  pMeta->hbInfo.stopFlag = STREAM_META_WILL_STOP;
  while (pMeta->hbInfo.stopFlag != STREAM_META_OK_TO_STOP) {
    taosMsleep(100);
    qDebug("vgId:%d wait for meta to stop timer", pMeta->vgId);
  }

  qDebug("vgId:%d start to check all tasks", vgId);
  int64_t st = taosGetTimestampMs();

  while (hasStreamTaskInTimer(pMeta)) {
    qDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  int64_t el = taosGetTimestampMs() - st;
  qDebug("vgId:%d all stream tasks are not in timer, continue close, elapsed time:%" PRId64 " ms", pMeta->vgId, el);
}
