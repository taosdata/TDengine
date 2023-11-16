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
#include "wal.h"

static TdThreadOnce streamMetaModuleInit = PTHREAD_ONCE_INIT;

int32_t streamBackendId = 0;
int32_t streamBackendCfWrapperId = 0;
int32_t streamMetaId = 0;

static void    metaHbToMnode(void* param, void* tmrId);
static void    streamMetaClear(SStreamMeta* pMeta);
static int32_t streamMetaBegin(SStreamMeta* pMeta);
static void    streamMetaCloseImpl(void* arg);

typedef struct {
  TdThreadMutex mutex;
  SHashObj*     pTable;
} SMetaRefMgt;

struct SMetaHbInfo {
  tmr_h   hbTmr;
  int32_t stopFlag;
  int32_t tickCounter;
  int32_t hbCount;
  int64_t hbStart;
};

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
    stError("vgId:%d failed to prepare stream meta, alloc size:%" PRIzu ", out of memory", vgId, sizeof(SStreamMeta));
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
  pMeta->pTasksMap = taosHashInit(64, fp, true, HASH_NO_LOCK);
  if (pMeta->pTasksMap == NULL) {
    goto _err;
  }

  pMeta->updateInfo.pTasks = taosHashInit(64, fp, false, HASH_NO_LOCK);
  if (pMeta->updateInfo.pTasks == NULL) {
    goto _err;
  }

  pMeta->startInfo.pReadyTaskSet = taosHashInit(64, fp, false, HASH_NO_LOCK);
  if (pMeta->startInfo.pReadyTaskSet == NULL) {
    goto _err;
  }

  pMeta->startInfo.pFailedTaskSet = taosHashInit(4, fp, false, HASH_NO_LOCK);
  if (pMeta->startInfo.pFailedTaskSet == NULL) {
    goto _err;
  }

  pMeta->pHbInfo = taosMemoryCalloc(1, sizeof(SMetaHbInfo));
  if (pMeta->pHbInfo == NULL) {
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

  pMeta->pHbInfo->hbTmr = taosTmrStart(metaHbToMnode, META_HB_CHECK_INTERVAL, pRid, streamEnv.timer);
  pMeta->pHbInfo->tickCounter = 0;
  pMeta->pHbInfo->stopFlag = 0;

  pMeta->pTaskBackendUnique =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  pMeta->chkpSaved = taosArrayInit(4, sizeof(int64_t));
  pMeta->chkpInUse = taosArrayInit(4, sizeof(int64_t));
  pMeta->chkpCap = 2;
  taosInitRWLatch(&pMeta->chkpDirLock);

  pMeta->chkpId = streamMetaGetLatestCheckpointId(pMeta);
  pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId, pMeta->vgId);
  while (pMeta->streamBackend == NULL) {
    taosMsleep(100);
    pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId, vgId);
    if (pMeta->streamBackend == NULL) {
      stInfo("vgId:%d failed to init stream backend, retry in 100ms", pMeta->vgId);
    }
  }
  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);

  pMeta->role = NODE_ROLE_UNINIT;
  code = streamBackendLoadCheckpointInfo(pMeta);

  taosInitRWLatch(&pMeta->lock);
  taosThreadMutexInit(&pMeta->backendMutex, NULL);

  pMeta->numOfPausedTasks = 0;
  pMeta->numOfStreamTasks = 0;
  stInfo("vgId:%d open stream meta successfully, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, pMeta->chkpId,
         stage);
  return pMeta;

  _err:
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasksMap) taosHashCleanup(pMeta->pTasksMap);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) tdbTbClose(pMeta->pTaskDb);
  if (pMeta->pCheckpointDb) tdbTbClose(pMeta->pCheckpointDb);
  if (pMeta->db) tdbClose(pMeta->db);
  if (pMeta->pHbInfo) taosMemoryFreeClear(pMeta->pHbInfo);
  if (pMeta->updateInfo.pTasks) taosHashCleanup(pMeta->updateInfo.pTasks);
  if (pMeta->startInfo.pReadyTaskSet) taosHashCleanup(pMeta->startInfo.pReadyTaskSet);
  if (pMeta->startInfo.pFailedTaskSet) taosHashCleanup(pMeta->startInfo.pFailedTaskSet);
  taosMemoryFree(pMeta);

  stError("failed to open stream meta");
  return NULL;
}

int32_t streamMetaReopen(SStreamMeta* pMeta) {
  streamMetaClear(pMeta);

  // NOTE: role should not be changed during reopen meta
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
      stError("vgId:%d failed to rename file, from %s to %s, code:%s", pMeta->vgId, newPath, defaultPath,
              tstrerror(terrno));

      taosMemoryFree(defaultPath);
      taosMemoryFree(newPath);
      return -1;
    }
  }

  taosMemoryFree(defaultPath);
  taosMemoryFree(newPath);

  return 0;
}

// todo refactor: the lock shoud be restricted in one function
void streamMetaInitBackend(SStreamMeta* pMeta) {
  pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId, pMeta->vgId);
  if (pMeta->streamBackend == NULL) {
    streamMetaWUnLock(pMeta);

    while (1) {
      streamMetaWLock(pMeta);
      pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId, pMeta->vgId);
      if (pMeta->streamBackend != NULL) {
        break;
      }

      streamMetaWUnLock(pMeta);
      stInfo("vgId:%d failed to init stream backend, retry in 100ms", pMeta->vgId);
      taosMsleep(100);
    }
  }

  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);
  streamBackendLoadCheckpointInfo(pMeta);
}

void streamMetaClear(SStreamMeta* pMeta) {
  // remove all existed tasks in this vnode
  void* pIter = NULL;
  while ((pIter = taosHashIterate(pMeta->pTasksMap, pIter)) != NULL) {
    SStreamTask* p = *(SStreamTask**)pIter;

    // release the ref by timer
    if (p->info.triggerParam != 0 && p->info.fillHistory == 0) {  // one more ref in timer
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", p->id.idStr, p->refCnt);
      taosTmrStop(p->schedInfo.pTimer);
      p->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, p);
    }

    streamMetaReleaseTask(pMeta, p);
  }

  taosRemoveRef(streamBackendId, pMeta->streamBackendRid);

  taosHashClear(pMeta->pTasksMap);
  taosHashClear(pMeta->pTaskBackendUnique);

  taosArrayClear(pMeta->pTaskList);
  taosArrayClear(pMeta->chkpSaved);
  taosArrayClear(pMeta->chkpInUse);
  pMeta->numOfStreamTasks = 0;
  pMeta->numOfPausedTasks = 0;
  pMeta->chkptNotReadyTasks = 0;

  // the willrestart/starting flag can NOT be cleared
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  pMeta->startInfo.readyTs = 0;
}

void streamMetaClose(SStreamMeta* pMeta) {
  stDebug("start to close stream meta");
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
  stDebug("start to do-close stream meta");
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

  taosHashCleanup(pMeta->pTasksMap);
  taosHashCleanup(pMeta->pTaskBackendUnique);
  taosHashCleanup(pMeta->updateInfo.pTasks);
  taosHashCleanup(pMeta->startInfo.pReadyTaskSet);
  taosHashCleanup(pMeta->startInfo.pFailedTaskSet);

  taosMemoryFree(pMeta->pHbInfo);
  taosMemoryFree(pMeta->path);
  taosThreadMutexDestroy(&pMeta->backendMutex);

  pMeta->role = NODE_ROLE_UNINIT;
  taosMemoryFree(pMeta);
  stDebug("end to close stream meta");
}

// todo let's check the status for each task
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

  int64_t id[2] = {pTask->id.streamId, pTask->id.taskId};
  if (tdbTbUpsert(pMeta->pTaskDb, id, STREAM_TASK_KEY_LEN, buf, len, pMeta->txn) < 0) {
    stError("s-task:%s save to disk failed, code:%s", pTask->id.idStr, tstrerror(terrno));
    return -1;
  }

  taosMemoryFree(buf);
  return 0;
}

int32_t streamMetaRemoveTask(SStreamMeta* pMeta, STaskId* pTaskId) {
  int64_t key[2] = {pTaskId->streamId, pTaskId->taskId};
  int32_t code = tdbTbDelete(pMeta->pTaskDb, key, STREAM_TASK_KEY_LEN, pMeta->txn);
  if (code != 0) {
    stError("vgId:%d failed to remove task:0x%x from metastore, code:%s", pMeta->vgId, (int32_t)pTaskId->taskId,
            tstrerror(terrno));
  } else {
    stDebug("vgId:%d remove task:0x%x from metastore", pMeta->vgId, (int32_t)pTaskId->taskId);
  }

  return code;
}

// add to the ready tasks hash map, not the restored tasks hash map
int32_t streamMetaRegisterTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask, bool* pAdded) {
  *pAdded = false;

  STaskId id = streamTaskExtractKey(pTask);
  void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (p != NULL) {
    return 0;
  }

  if (pTask->info.fillHistory == 1) {
    stDebug("s-task:0x%x initial nextProcessVer is set to 1 for fill-history task", pTask->id.taskId);
    ver = 1;
  }

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

  taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask, POINTER_BYTES);
  if (pTask->info.fillHistory == 0) {
    atomic_add_fetch_32(&pMeta->numOfStreamTasks, 1);
  }

  *pAdded = true;
  return 0;
}

int32_t streamMetaGetNumOfTasks(SStreamMeta* pMeta) {
  size_t size = taosHashGetSize(pMeta->pTasksMap);
  ASSERT(taosArrayGetSize(pMeta->pTaskList) == taosHashGetSize(pMeta->pTasksMap));
  return (int32_t)size;
}

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  streamMetaRLock(pMeta);

  STaskId       id = {.streamId = streamId, .taskId = taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask != NULL) {
    if (!streamTaskShouldStop(*ppTask)) {
      int32_t ref = atomic_add_fetch_32(&(*ppTask)->refCnt, 1);
      streamMetaRUnLock(pMeta);
      stTrace("s-task:%s acquire task, ref:%d", (*ppTask)->id.idStr, ref);
      return *ppTask;
    }
  }

  streamMetaRUnLock(pMeta);
  return NULL;
}

void streamMetaReleaseTask(SStreamMeta* UNUSED_PARAM(pMeta), SStreamTask* pTask) {
  int32_t ref = atomic_sub_fetch_32(&pTask->refCnt, 1);
  if (ref > 0) {
    stTrace("s-task:%s release task, ref:%d", pTask->id.idStr, ref);
  } else if (ref == 0) {
    ASSERT(streamTaskShouldStop(pTask));
    stTrace("s-task:%s all refs are gone, free it", pTask->id.idStr);
    tFreeStreamTask(pTask);
  } else if (ref < 0) {
    stError("task ref is invalid, ref:%d, %s", ref, pTask->id.idStr);
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
  streamMetaWLock(pMeta);

  STaskId       id = {.streamId = streamId, .taskId = taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask) {
    pTask = *ppTask;

    // desc the paused task counter
    if (streamTaskShouldPause(pTask)) {
      int32_t num = atomic_sub_fetch_32(&pMeta->numOfPausedTasks, 1);
      stInfo("vgId:%d s-task:%s drop stream task. pause task num:%d", pMeta->vgId, pTask->id.idStr, num);
    }

    // handle the dropping event
    streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_DROPPING);
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may be dropped already", pMeta->vgId, taskId);
    streamMetaWUnLock(pMeta);
    return 0;
  }
  streamMetaWUnLock(pMeta);

  stDebug("s-task:0x%x set task status:dropping and start to unregister it", taskId);

  while (1) {
    streamMetaRLock(pMeta);

    ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (ppTask) {
      if ((*ppTask)->status.timerActive == 0) {
        streamMetaRUnLock(pMeta);
        break;
      }

      taosMsleep(10);
      stDebug("s-task:%s wait for quit from timer", (*ppTask)->id.idStr);
      streamMetaRUnLock(pMeta);
    } else {
      streamMetaRUnLock(pMeta);
      break;
    }
  }

  // let's do delete of stream task
  streamMetaWLock(pMeta);

  ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask) {
    pTask = *ppTask;

    // it is an fill-history task, remove the related stream task's id that points to it
    if (pTask->info.fillHistory == 1) {
      streamTaskClearHTaskAttr(pTask);
    } else {
      atomic_sub_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    doRemoveIdFromList(pMeta, (int32_t)taosArrayGetSize(pMeta->pTaskList), &pTask->id);

    ASSERT(pTask->status.timerActive == 0);

    if (pTask->info.triggerParam != 0 && pTask->info.fillHistory == 0) {
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", pTask->id.idStr, pTask->refCnt);
      taosTmrStop(pTask->schedInfo.pTimer);
      pTask->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, pTask);
    }

    streamMetaRemoveTask(pMeta, &id);
    streamMetaReleaseTask(pMeta, pTask);
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may have been dropped already", pMeta->vgId, taskId);
  }

  streamMetaWUnLock(pMeta);
  return 0;
}

int32_t streamMetaBegin(SStreamMeta* pMeta) {
  streamMetaWLock(pMeta);
  int32_t code = tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
                          TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  streamMetaWUnLock(pMeta);
  return code;
}

// todo add error log
int32_t streamMetaCommit(SStreamMeta* pMeta) {
  if (tdbCommit(pMeta->db, pMeta->txn) < 0) {
    stError("vgId:%d failed to commit stream meta", pMeta->vgId);
    return -1;
  }

  if (tdbPostCommit(pMeta->db, pMeta->txn) < 0) {
    stError("vgId:%d failed to do post-commit stream meta", pMeta->vgId);
    return -1;
  }

  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    stError("vgId:%d failed to begin trans", pMeta->vgId);
    return -1;
  }

  return 0;
}

int64_t streamMetaGetLatestCheckpointId(SStreamMeta* pMeta) {
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

  stDebug("get max chkp id: %" PRId64 "", chkpId);

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
  TBC*    pCur = NULL;
  int32_t vgId = pMeta->vgId;

  stInfo("vgId:%d load stream tasks from meta files", vgId);

  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    stError("vgId:%d failed to open stream meta, code:%s", vgId, tstrerror(terrno));
    return -1;
  }

  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;
  SArray*  pRecycleList = taosArrayInit(4, sizeof(STaskId));

  tdbTbcMoveToFirst(pCur);
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      stError("vgId:%d failed to load stream task from meta-files, code:%s", vgId, tstrerror(terrno));
      doClear(pKey, pVal, pCur, pRecycleList);
      return -1;
    }

    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTask(&decoder, pTask) < 0) {
      tDecoderClear(&decoder);
      doClear(pKey, pVal, pCur, pRecycleList);
      tFreeStreamTask(pTask);
      stError(
          "vgId:%d stream read incompatible data, rm %s/vnode/vnode*/tq/stream if taosd cannot start, and rebuild "
          "stream manually",
          vgId, tsDataDir);
      return -1;
    }
    tDecoderClear(&decoder);

    if (pTask->status.taskStatus == TASK_STATUS__DROPPING) {
      int32_t taskId = pTask->id.taskId;
      tFreeStreamTask(pTask);

      STaskId id = streamTaskExtractKey(pTask);
      taosArrayPush(pRecycleList, &id);

      int32_t total = taosArrayGetSize(pRecycleList);
      stDebug("s-task:0x%x is already dropped, add into recycle list, total:%d", taskId, total);
      continue;
    }

    // do duplicate task check.
    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (p == NULL) {
      if (pMeta->expandFunc(pMeta->ahandle, pTask, pTask->chkInfo.checkpointVer + 1) < 0) {
        doClear(pKey, pVal, pCur, pRecycleList);
        tFreeStreamTask(pTask);
        return -1;
      }

      taosArrayPush(pMeta->pTaskList, &pTask->id);
    } else {
      // todo this should replace the existed object put by replay creating stream task msg from mnode
      stError("s-task:0x%x already added into table meta by replaying WAL, need check", pTask->id.taskId);
      tdbFree(pKey);
      tdbFree(pVal);
      taosMemoryFree(pTask);
      continue;
    }

    if (taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask, POINTER_BYTES) < 0) {
      doClear(pKey, pVal, pCur, pRecycleList);
      tFreeStreamTask(pTask);
      return -1;
    }

    if (pTask->info.fillHistory == 0) {
      atomic_add_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    if (streamTaskShouldPause(pTask)) {
      atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
    }

    ASSERT(pTask->status.downstreamReady == 0);
  }

  tdbFree(pKey);
  tdbFree(pVal);
  if (tdbTbcClose(pCur) < 0) {
    stError("vgId:%d failed to close meta-file cursor", vgId);
    taosArrayDestroy(pRecycleList);
    return -1;
  }

  if (taosArrayGetSize(pRecycleList) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(pRecycleList); ++i) {
      STaskId* pId = taosArrayGet(pRecycleList, i);
      streamMetaRemoveTask(pMeta, pId);
    }
  }

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  ASSERT(pMeta->numOfStreamTasks <= numOfTasks && pMeta->numOfPausedTasks <= numOfTasks);
  stDebug("vgId:%d load %d tasks into meta from disk completed, streamTask:%d, paused:%d", pMeta->vgId, numOfTasks,
          pMeta->numOfStreamTasks, pMeta->numOfPausedTasks);
  taosArrayDestroy(pRecycleList);
  return 0;
}

int32_t tEncodeStreamHbMsg(SEncoder* pEncoder, const SStreamHbMsg* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->vgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->numOfTasks) < 0) return -1;

  for (int32_t i = 0; i < pReq->numOfTasks; ++i) {
    STaskStatusEntry* ps = taosArrayGet(pReq->pTaskStatus, i);
    if (tEncodeI64(pEncoder, ps->id.streamId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->id.taskId) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->status) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->stage) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->nodeId) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputQUsed) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputRate) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkQuota) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkDataSize) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->processedVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verStart) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verEnd) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->activeCheckpointId) < 0) return -1;
    if (tEncodeI8(pEncoder, ps->checkpointFailed) < 0) return -1;
  }

  int32_t numOfVgs = taosArrayGetSize(pReq->pUpdateNodes);
  if (tEncodeI32(pEncoder, numOfVgs) < 0) return -1;

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t* pVgId = taosArrayGet(pReq->pUpdateNodes, j);
    if (tEncodeI32(pEncoder, *pVgId) < 0) return -1;
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
    int32_t          taskId = 0;
    STaskStatusEntry entry = {0};

    if (tDecodeI64(pDecoder, &entry.id.streamId) < 0) return -1;
    if (tDecodeI32(pDecoder, &taskId) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.status) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.stage) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.nodeId) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputQUsed) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputRate) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkQuota) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkDataSize) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.processedVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verStart) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verEnd) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.activeCheckpointId) < 0) return -1;
    if (tDecodeI8(pDecoder, (int8_t*)&entry.checkpointFailed) < 0) return -1;

    entry.id.taskId = taskId;
    taosArrayPush(pReq->pTaskStatus, &entry);
  }

  int32_t numOfVgs = 0;
  if (tDecodeI32(pDecoder, &numOfVgs) < 0) return -1;

  pReq->pUpdateNodes = taosArrayInit(numOfVgs, sizeof(int32_t));

  for (int j = 0; j < numOfVgs; ++j) {
    int32_t vgId = 0;
    if (tDecodeI32(pDecoder, &vgId) < 0) return -1;
    taosArrayPush(pReq->pUpdateNodes, &vgId);
  }

  tEndDecode(pDecoder);
  return 0;
}

static bool waitForEnoughDuration(SMetaHbInfo* pInfo) {
  if ((++pInfo->tickCounter) >= META_HB_SEND_IDLE_COUNTER) {  // reset the counter
    pInfo->tickCounter = 0;
    return true;
  }
  return false;
}

static void clearHbMsg(SStreamHbMsg* pMsg, SArray* pIdList) {
  taosArrayDestroy(pMsg->pTaskStatus);
  taosArrayDestroy(pMsg->pUpdateNodes);
  taosArrayDestroy(pIdList);
}

static bool existInHbMsg(SStreamHbMsg* pMsg, SDownstreamTaskEpset* pTaskEpset) {
  int32_t numOfExisted = taosArrayGetSize(pMsg->pUpdateNodes);
  for (int k = 0; k < numOfExisted; ++k) {
    if (pTaskEpset->nodeId == *(int32_t*)taosArrayGet(pMsg->pUpdateNodes, k)) {
      return true;
    }
  }
  return false;
}

static void addUpdateNodeIntoHbMsg(SStreamTask* pTask, SStreamHbMsg* pMsg) {
  SStreamMeta* pMeta = pTask->pMeta;

  taosThreadMutexLock(&pTask->lock);

  int32_t num = taosArrayGetSize(pTask->outputInfo.pDownstreamUpdateList);
  for (int j = 0; j < num; ++j) {
    SDownstreamTaskEpset* pTaskEpset = taosArrayGet(pTask->outputInfo.pDownstreamUpdateList, j);

    bool exist = existInHbMsg(pMsg, pTaskEpset);
    if (!exist) {
      taosArrayPush(pMsg->pUpdateNodes, &pTaskEpset->nodeId);
      stDebug("vgId:%d nodeId:%d added into hb update list, total:%d", pMeta->vgId, pTaskEpset->nodeId,
              (int32_t)taosArrayGetSize(pMsg->pUpdateNodes));
    }
  }

  taosArrayClear(pTask->outputInfo.pDownstreamUpdateList);
  taosThreadMutexUnlock(&pTask->lock);
}

void metaHbToMnode(void* param, void* tmrId) {
  int64_t rid = *(int64_t*)param;

  SStreamMeta* pMeta = taosAcquireRef(streamMetaId, rid);
  if (pMeta == NULL) {
    return;
  }

  // need to stop, stop now
  if (pMeta->pHbInfo->stopFlag == STREAM_META_WILL_STOP) {
    pMeta->pHbInfo->stopFlag = STREAM_META_OK_TO_STOP;
    stDebug("vgId:%d jump out of meta timer", pMeta->vgId);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  // not leader not send msg
  if (pMeta->role != NODE_ROLE_LEADER) {
    stInfo("vgId:%d role:%d not leader not send hb to mnode", pMeta->vgId, pMeta->role);
    taosReleaseRef(streamMetaId, rid);
    pMeta->pHbInfo->hbStart = 0;
    return;
  }

  // set the hb start time
  if (pMeta->pHbInfo->hbStart == 0) {
    pMeta->pHbInfo->hbStart = taosGetTimestampMs();
  }

  if (!waitForEnoughDuration(pMeta->pHbInfo)) {
    taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamEnv.timer, &pMeta->pHbInfo->hbTmr);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  stDebug("vgId:%d build stream task hb, leader:%d", pMeta->vgId, (pMeta->role == NODE_ROLE_LEADER));

  SStreamHbMsg hbMsg = {0};
  SEpSet       epset = {0};
  bool         hasMnodeEpset = false;
  int32_t      stage = 0;

  streamMetaRLock(pMeta);

  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  hbMsg.vgId = pMeta->vgId;
  stage = pMeta->stage;

  SArray* pIdList = taosArrayDup(pMeta->pTaskList, NULL);

  streamMetaRUnLock(pMeta);

  hbMsg.pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));
  hbMsg.pUpdateNodes = taosArrayInit(numOfTasks, sizeof(int32_t));

  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId* pId = taosArrayGet(pIdList, i);

    streamMetaRLock(pMeta);
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, pId, sizeof(*pId));
    streamMetaRUnLock(pMeta);

    if (pTask == NULL) {
      continue;
    }

    // not report the status of fill-history task
    if ((*pTask)->info.fillHistory == 1) {
      continue;
    }

    STaskStatusEntry entry = {
        .id = *pId,
        .status = streamTaskGetStatus(*pTask, NULL),
        .nodeId = hbMsg.vgId,
        .stage = stage,
        .inputQUsed = SIZE_IN_MiB(streamQueueGetItemSize((*pTask)->inputq.queue)),
    };

    entry.inputRate = entry.inputQUsed * 100.0 / STREAM_TASK_QUEUE_CAPACITY_IN_SIZE;
    if ((*pTask)->info.taskLevel == TASK_LEVEL__SINK) {
      entry.sinkQuota = (*pTask)->outputInfo.pTokenBucket->quotaRate;
      entry.sinkDataSize = SIZE_IN_MiB((*pTask)->execInfo.sink.dataSize);
    }

    if ((*pTask)->checkpointingId != 0) {
      entry.checkpointFailed = ((*pTask)->chkInfo.failedId >= (*pTask)->checkpointingId);
      entry.activeCheckpointId = (*pTask)->checkpointingId;
    }

    if ((*pTask)->exec.pWalReader != NULL) {
      entry.processedVer = (*pTask)->chkInfo.nextProcessVer - 1;
      walReaderValidVersionRange((*pTask)->exec.pWalReader, &entry.verStart, &entry.verEnd);
    }

    addUpdateNodeIntoHbMsg(*pTask, &hbMsg);
    taosArrayPush(hbMsg.pTaskStatus, &entry);
    if (!hasMnodeEpset) {
      epsetAssign(&epset, &(*pTask)->info.mnodeEpset);
      hasMnodeEpset = true;
    }
  }

  hbMsg.numOfTasks = taosArrayGetSize(hbMsg.pTaskStatus);

  if (hasMnodeEpset) {
    int32_t code = 0;
    int32_t tlen = 0;

    tEncodeSize(tEncodeStreamHbMsg, &hbMsg, tlen, code);
    if (code < 0) {
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      goto _end;
    }

    void* buf = rpcMallocCont(tlen);
    if (buf == NULL) {
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      goto _end;
    }

    SEncoder encoder;
    tEncoderInit(&encoder, buf, tlen);
    if ((code = tEncodeStreamHbMsg(&encoder, &hbMsg)) < 0) {
      rpcFreeCont(buf);
      stError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
      goto _end;
    }
    tEncoderClear(&encoder);

    SRpcMsg msg = {.info.noResp = 1,};
    initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);

    pMeta->pHbInfo->hbCount += 1;

    stDebug("vgId:%d build and send hb to mnode, numOfTasks:%d total:%d", pMeta->vgId, hbMsg.numOfTasks,
            pMeta->pHbInfo->hbCount);
    tmsgSendReq(&epset, &msg);
  } else {
    stDebug("vgId:%d no tasks and no mnd epset, not send stream hb to mnode", pMeta->vgId);
  }

  _end:
  clearHbMsg(&hbMsg, pIdList);
  taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamEnv.timer, &pMeta->pHbInfo->hbTmr);
  taosReleaseRef(streamMetaId, rid);
}

bool streamMetaTaskInTimer(SStreamMeta* pMeta) {
  bool inTimer = false;
  streamMetaWLock(pMeta);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasksMap, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->status.timerActive >= 1) {
      inTimer = true;
    }
  }

  streamMetaWUnLock(pMeta);
  return inTimer;
}

void streamMetaNotifyClose(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;

  stDebug("vgId:%d notify all stream tasks that the vnode is closing. isLeader:%d startHb%" PRId64 ", totalHb:%d", vgId,
          (pMeta->role == NODE_ROLE_LEADER), pMeta->pHbInfo->hbStart, pMeta->pHbInfo->hbCount);

  streamMetaWLock(pMeta);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasksMap, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    stDebug("vgId:%d s-task:%s set closing flag", vgId, pTask->id.idStr);
    streamTaskStop(pTask);
  }

  streamMetaWUnLock(pMeta);

  // wait for the stream meta hb function stopping
  if (pMeta->role == NODE_ROLE_LEADER) {
    pMeta->pHbInfo->stopFlag = STREAM_META_WILL_STOP;
    while (pMeta->pHbInfo->stopFlag != STREAM_META_OK_TO_STOP) {
      taosMsleep(100);
      stDebug("vgId:%d wait for meta to stop timer", pMeta->vgId);
    }
  }

  stDebug("vgId:%d start to check all tasks", vgId);
  int64_t st = taosGetTimestampMs();

  while (streamMetaTaskInTimer(pMeta)) {
    stDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  int64_t el = taosGetTimestampMs() - st;
  stDebug("vgId:%d all stream tasks are not in timer, continue close, elapsed time:%" PRId64 " ms", pMeta->vgId, el);
}

void streamMetaStartHb(SStreamMeta* pMeta) {
  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  metaRefMgtAdd(pMeta->vgId, pRid);
  *pRid = pMeta->rid;
  metaHbToMnode(pRid, NULL);
}

void streamMetaInitForSnode(SStreamMeta* pMeta) {
  pMeta->stage = 0;
  pMeta->role = NODE_ROLE_LEADER;
}

void streamMetaResetStartInfo(STaskStartInfo* pStartInfo) {
  taosHashClear(pStartInfo->pReadyTaskSet);
  taosHashClear(pStartInfo->pFailedTaskSet);
  pStartInfo->tasksWillRestart = 0;
  pStartInfo->readyTs = 0;
  // reset the sentinel flag value to be 0
  atomic_store_32(&pStartInfo->taskStarting, 0);
}

void streamMetaRLock(SStreamMeta* pMeta) {
  stTrace("vgId:%d meta-rlock", pMeta->vgId);
  taosRLockLatch(&pMeta->lock);
}
void streamMetaRUnLock(SStreamMeta* pMeta) {
  stTrace("vgId:%d meta-runlock", pMeta->vgId);
  taosRUnLockLatch(&pMeta->lock);
}
void streamMetaWLock(SStreamMeta* pMeta) {
  stTrace("vgId:%d meta-wlock", pMeta->vgId);
  taosWLockLatch(&pMeta->lock);
}
void streamMetaWUnLock(SStreamMeta* pMeta) {
  stTrace("vgId:%d meta-wunlock", pMeta->vgId);
  taosWUnLockLatch(&pMeta->lock);
}

