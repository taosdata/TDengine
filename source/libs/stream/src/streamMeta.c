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
#include "tsched.h"
#include "tstream.h"
#include "ttimer.h"
#include "wal.h"

static TdThreadOnce streamMetaModuleInit = PTHREAD_ONCE_INIT;

int32_t streamBackendId = 0;
int32_t streamBackendCfWrapperId = 0;
int32_t streamMetaId = 0;
int32_t taskDbWrapperId = 0;

static void    metaHbToMnode(void* param, void* tmrId);
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

typedef struct STaskInitTs {
  int64_t start;
  int64_t end;
  bool    success;
} STaskInitTs;

SMetaRefMgt gMetaRefMgt;

void    metaRefMgtInit();
void    metaRefMgtCleanup();
int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid);

static void streamMetaEnvInit() {
  streamBackendId = taosOpenRef(64, streamBackendCleanup);
  streamBackendCfWrapperId = taosOpenRef(64, streamBackendHandleCleanup);
  taskDbWrapperId = taosOpenRef(64, taskDbDestroy2);

  streamMetaId = taosOpenRef(64, streamMetaCloseImpl);

  metaRefMgtInit();
  streamTimerInit();
}

void streamMetaInit() { taosThreadOnce(&streamMetaModuleInit, streamMetaEnvInit);}

void streamMetaCleanup() {
  taosCloseRef(streamBackendId);
  taosCloseRef(streamBackendCfWrapperId);
  taosCloseRef(streamMetaId);

  metaRefMgtCleanup();
  streamTimerCleanUp();
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

typedef struct {
  int64_t chkpId;
  char*   path;
  char*   taskId;

  SArray* pChkpSave;
  SArray* pChkpInUse;
  int8_t  chkpCap;
  void*   backend;

} StreamMetaTaskState;

int32_t streamMetaOpenTdb(SStreamMeta* pMeta) {
  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db, 0) < 0) {
    return -1;
    // goto _err;
  }

  if (tdbTbOpen("task.db", STREAM_TASK_KEY_LEN, -1, NULL, pMeta->db, &pMeta->pTaskDb, 0) < 0) {
    return -1;
  }

  if (tdbTbOpen("checkpoint.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pCheckpointDb, 0) < 0) {
    return -1;
  }
  return 0;
}

//
// impl later
//
enum STREAM_STATE_VER {
  STREAM_STATA_NO_COMPATIBLE,
  STREAM_STATA_COMPATIBLE,
  STREAM_STATA_NEED_CONVERT,
};

int32_t streamMetaCheckBackendCompatible(SStreamMeta* pMeta) {
  int8_t ret = STREAM_STATA_COMPATIBLE;
  TBC*   pCur = NULL;

  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    // no task info, no stream
    return ret;
  }
  void*   pKey = NULL;
  int32_t kLen = 0;
  void*   pVal = NULL;
  int32_t vLen = 0;

  tdbTbcMoveToFirst(pCur);
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    if (pVal == NULL || vLen == 0) {
      break;
    }
    SDecoder        decoder;
    SCheckpointInfo info;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTaskChkInfo(&decoder, &info) < 0) {
      continue;
    }
    if (info.msgVer <= SSTREAM_TASK_INCOMPATIBLE_VER) {
      ret = STREAM_STATA_NO_COMPATIBLE;
    } else if (info.msgVer >= SSTREAM_TASK_NEED_CONVERT_VER) {
      ret = STREAM_STATA_NEED_CONVERT;
    }
    tDecoderClear(&decoder);
    break;
  }
  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);
  return ret;
}

int32_t streamMetaCvtDbFormat(SStreamMeta* pMeta) {
  int32_t code = 0;
  int64_t chkpId = streamMetaGetLatestCheckpointId(pMeta);

  bool exist = streamBackendDataIsExist(pMeta->path, chkpId, pMeta->vgId);
  if (exist == false) {
    return code;
  }
  SBackendWrapper* pBackend = streamBackendInit(pMeta->path, chkpId, pMeta->vgId);

  void* pIter = taosHashIterate(pBackend->cfInst, NULL);
  while (pIter) {
    void* key = taosHashGetKey(pIter, NULL);
    code = streamStateCvtDataFormat(pMeta->path, key, *(void**)pIter);
    if (code != 0) {
      stError("failed to cvt data");
      goto _EXIT;
    }

    pIter = taosHashIterate(pBackend->cfInst, pIter);
  }

_EXIT:
  streamBackendCleanup((void*)pBackend);

  if (code == 0) {
    char* state = taosMemoryCalloc(1, strlen(pMeta->path) + 32);
    sprintf(state, "%s%s%s", pMeta->path, TD_DIRSEP, "state");
    taosRemoveDir(state);
    taosMemoryFree(state);
  }

  return code;
}
int32_t streamMetaMayCvtDbFormat(SStreamMeta* pMeta) {
  int8_t compatible = streamMetaCheckBackendCompatible(pMeta);
  if (compatible == STREAM_STATA_COMPATIBLE) {
    return 0;
  } else if (compatible == STREAM_STATA_NEED_CONVERT) {
    stInfo("stream state need covert backend format");

    return streamMetaCvtDbFormat(pMeta);
  } else if (compatible == STREAM_STATA_NO_COMPATIBLE) {
    stError(
        "stream read incompatible data, rm %s/vnode/vnode*/tq/stream if taosd cannot start, and rebuild stream "
        "manually",
        tsDataDir);

    return -1;
  }
  return 0;
}

int32_t streamTaskSetDb(SStreamMeta* pMeta, void* arg, char* key) {
  SStreamTask* pTask = arg;

  int64_t chkpId = pTask->chkInfo.checkpointId;

  taosThreadMutexLock(&pMeta->backendMutex);
  void** ppBackend = taosHashGet(pMeta->pTaskDbUnique, key, strlen(key));
  if (ppBackend != NULL && *ppBackend != NULL) {
    taskDbAddRef(*ppBackend);

    STaskDbWrapper* pBackend = *ppBackend;
    pBackend->pMeta = pMeta;
    pTask->pBackend = pBackend;

    taosThreadMutexUnlock(&pMeta->backendMutex);

    stDebug("s-task:0x%x set backend %p", pTask->id.taskId, pBackend);
    return 0;
  }

  STaskDbWrapper* pBackend = taskDbOpen(pMeta->path, key, chkpId);
  while (1) {
    if (pBackend == NULL) {
      taosThreadMutexUnlock(&pMeta->backendMutex);
      taosMsleep(1000);
      stDebug("backend held by other task, restart later, path:%s, key:%s", pMeta->path, key);
    } else {
      taosThreadMutexUnlock(&pMeta->backendMutex);
      break;
    }

    taosThreadMutexLock(&pMeta->backendMutex);
    pBackend = taskDbOpen(pMeta->path, key, chkpId);
  }

  int64_t tref = taosAddRef(taskDbWrapperId, pBackend);
  pTask->pBackend = pBackend;
  pBackend->refId = tref;
  pBackend->pTask = pTask;
  pBackend->pMeta = pMeta;

  taosHashPut(pMeta->pTaskDbUnique, key, strlen(key), &pBackend, sizeof(void*));
  taosThreadMutexUnlock(&pMeta->backendMutex);

  stDebug("s-task:0x%x set backend %p", pTask->id.taskId, pBackend);
  return 0;
}
void streamMetaRemoveDB(void* arg, char* key) {
  if (arg == NULL || key == NULL) return;

  SStreamMeta* pMeta = arg;
  taosThreadMutexLock(&pMeta->backendMutex);
  taosHashRemove(pMeta->pTaskDbUnique, key, strlen(key));

  taosThreadMutexUnlock(&pMeta->backendMutex);
}

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId, int64_t stage,
                            startComplete_fn_t fn) {
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

  if (streamMetaOpenTdb(pMeta) < 0) {
    goto _err;
  }

  if (streamMetaMayCvtDbFormat(pMeta) < 0) {
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

  pMeta->scanInfo.scanCounter = 0;
  pMeta->vgId = vgId;
  pMeta->ahandle = ahandle;
  pMeta->expandFunc = expandFunc;
  pMeta->stage = stage;
  pMeta->role = (vgId == SNODE_HANDLE) ? NODE_ROLE_LEADER : NODE_ROLE_UNINIT;

  pMeta->startInfo.completeFn = fn;
  pMeta->pTaskDbUnique = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);

  pMeta->numOfPausedTasks = 0;
  pMeta->numOfStreamTasks = 0;
  stInfo("vgId:%d open stream meta successfully, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, pMeta->chkpId,
         stage);

  pMeta->rid = taosAddRef(streamMetaId, pMeta);

  // set the attribute when running on Linux OS
  TdThreadRwlockAttr attr;
  taosThreadRwlockAttrInit(&attr);

#ifdef LINUX
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif

  taosThreadRwlockInit(&pMeta->lock, &attr);
  taosThreadRwlockAttrDestroy(&attr);

  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  memcpy(pRid, &pMeta->rid, sizeof(pMeta->rid));
  metaRefMgtAdd(pMeta->vgId, pRid);

  pMeta->pHbInfo->hbTmr = taosTmrStart(metaHbToMnode, META_HB_CHECK_INTERVAL, pRid, streamTimer);
  pMeta->pHbInfo->tickCounter = 0;
  pMeta->pHbInfo->stopFlag = 0;
  pMeta->qHandle = taosInitScheduler(32, 1, "stream-chkp", NULL);

  pMeta->bkdChkptMgt = bkdMgtCreate(tpath);
  taosThreadMutexInit(&pMeta->backendMutex, NULL);

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

// todo refactor: the lock shoud be restricted in one function
#ifdef BUILD_NO_CALL
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
#endif

void streamMetaClear(SStreamMeta* pMeta) {
  // remove all existed tasks in this vnode
  void* pIter = NULL;
  while ((pIter = taosHashIterate(pMeta->pTasksMap, pIter)) != NULL) {
    SStreamTask* p = *(SStreamTask**)pIter;

    // release the ref by timer
    if (p->info.triggerParam != 0 && p->info.fillHistory == 0) {  // one more ref in timer
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", p->id.idStr, p->refCnt);
      taosTmrStop(p->schedInfo.pDelayTimer);
      p->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, p);
    }

    streamMetaReleaseTask(pMeta, p);
  }

  taosRemoveRef(streamBackendId, pMeta->streamBackendRid);
  taosHashClear(pMeta->pTasksMap);

  taosArrayClear(pMeta->pTaskList);
  taosArrayClear(pMeta->chkpSaved);
  taosArrayClear(pMeta->chkpInUse);

  pMeta->numOfStreamTasks = 0;
  pMeta->numOfPausedTasks = 0;

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

  streamMetaWLock(pMeta);
  streamMetaClear(pMeta);
  streamMetaWUnLock(pMeta);

  tdbAbort(pMeta->db, pMeta->txn);
  tdbTbClose(pMeta->pTaskDb);
  tdbTbClose(pMeta->pCheckpointDb);
  tdbClose(pMeta->db);

  taosArrayDestroy(pMeta->pTaskList);
  taosArrayDestroy(pMeta->chkpSaved);
  taosArrayDestroy(pMeta->chkpInUse);

  taosHashCleanup(pMeta->pTasksMap);
  taosHashCleanup(pMeta->pTaskDbUnique);
  taosHashCleanup(pMeta->pUpdateTaskSet);
  taosHashCleanup(pMeta->updateInfo.pTasks);
  taosHashCleanup(pMeta->startInfo.pReadyTaskSet);
  taosHashCleanup(pMeta->startInfo.pFailedTaskSet);

  taosMemoryFree(pMeta->pHbInfo);
  taosMemoryFree(pMeta->path);
  taosThreadMutexDestroy(&pMeta->backendMutex);

  taosCleanUpScheduler(pMeta->qHandle);
  taosMemoryFree(pMeta->qHandle);

  bkdMgtDestroy(pMeta->bkdChkptMgt);

  pMeta->role = NODE_ROLE_UNINIT;
  taosThreadRwlockDestroy(&pMeta->lock);

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

  STaskId id = streamTaskGetTaskId(pTask);
  void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (p != NULL) {
    return 0;
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

SStreamTask* streamMetaAcquireTaskNoLock(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  STaskId       id = {.streamId = streamId, .taskId = taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask == NULL || streamTaskShouldStop(*ppTask)) {
    return NULL;
  }

  int32_t ref = atomic_add_fetch_32(&(*ppTask)->refCnt, 1);
  stTrace("s-task:%s acquire task, ref:%d", (*ppTask)->id.idStr, ref);
  return *ppTask;
}

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  streamMetaRLock(pMeta);
  SStreamTask* p = streamMetaAcquireTaskNoLock(pMeta, streamId, taskId);
  streamMetaRUnLock(pMeta);
  return p;
}

SStreamTask* streamMetaAcquireOneTask(SStreamTask* pTask) {
  int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
  stTrace("s-task:%s acquire task, ref:%d", pTask->id.idStr, ref);
  return pTask;
}

void streamMetaReleaseTask(SStreamMeta* UNUSED_PARAM(pMeta), SStreamTask* pTask) {
  int32_t ref = atomic_sub_fetch_32(&pTask->refCnt, 1);
  if (ref > 0) {
    stTrace("s-task:%s release task, ref:%d", pTask->id.idStr, ref);
  } else if (ref == 0) {
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
    atomic_sub_fetch_32(&pMeta->numOfStreamTasks, 1);

    taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    doRemoveIdFromList(pMeta, (int32_t)taosArrayGetSize(pMeta->pTaskList), &pTask->id);
    streamMetaRemoveTask(pMeta, &id);

    streamMetaWUnLock(pMeta);

    ASSERT(pTask->status.timerActive == 0);

    if (pTask->info.triggerParam != 0 && pTask->info.fillHistory == 0) {
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", pTask->id.idStr, pTask->refCnt);
      taosTmrStop(pTask->schedInfo.pDelayTimer);
      pTask->info.triggerParam = 0;
      streamMetaReleaseTask(pMeta, pTask);
    }

    streamMetaReleaseTask(pMeta, pTask);
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may have been dropped already", pMeta->vgId, taskId);
    streamMetaWUnLock(pMeta);
  }

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
  TBC*     pCur = NULL;
  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;

  if (pMeta == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SArray* pRecycleList = taosArrayInit(4, sizeof(STaskId));
  int32_t vgId = pMeta->vgId;
  stInfo("vgId:%d load stream tasks from meta files", vgId);

  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    stError("vgId:%d failed to open stream meta, code:%s", vgId, tstrerror(terrno));
    taosArrayDestroy(pRecycleList);
    return -1;
  }

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

      STaskId id = streamTaskGetTaskId(pTask);
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
    if (tEncodeI64(pEncoder, ps->stage) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->nodeId) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputQUsed) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->inputRate) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkQuota) < 0) return -1;
    if (tEncodeDouble(pEncoder, ps->sinkDataSize) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->processedVer) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verStart) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->verEnd) < 0) return -1;
    if (tEncodeI64(pEncoder, ps->checkpointId) < 0) return -1;
    if (tEncodeI8(pEncoder, ps->checkpointFailed) < 0) return -1;
    if (tEncodeI32(pEncoder, ps->chkpointTransId) < 0) return -1;
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
    if (tDecodeI64(pDecoder, &entry.stage) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.nodeId) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputQUsed) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.inputRate) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkQuota) < 0) return -1;
    if (tDecodeDouble(pDecoder, &entry.sinkDataSize) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.processedVer) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verStart) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.verEnd) < 0) return -1;
    if (tDecodeI64(pDecoder, &entry.checkpointId) < 0) return -1;
    if (tDecodeI8(pDecoder, &entry.checkpointFailed) < 0) return -1;
    if (tDecodeI32(pDecoder, &entry.chkpointTransId) < 0) return -1;

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

void streamMetaClearHbMsg(SStreamHbMsg* pMsg) {
  if (pMsg == NULL) {
    return;
  }

  if (pMsg->pUpdateNodes != NULL) {
    taosArrayDestroy(pMsg->pUpdateNodes);
  }

  if (pMsg->pTaskStatus != NULL) {
    taosArrayDestroy(pMsg->pTaskStatus);
  }
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

static int32_t metaHeartbeatToMnodeImpl(SStreamMeta* pMeta) {
  SStreamHbMsg hbMsg = {0};
  SEpSet       epset = {0};
  bool         hasMnodeEpset = false;
  int32_t      numOfTasks = streamMetaGetNumOfTasks(pMeta);

  hbMsg.vgId = pMeta->vgId;
  hbMsg.pTaskStatus = taosArrayInit(numOfTasks, sizeof(STaskStatusEntry));
  hbMsg.pUpdateNodes = taosArrayInit(numOfTasks, sizeof(int32_t));

  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId* pId = taosArrayGet(pMeta->pTaskList, i);

    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, pId, sizeof(*pId));
    if (pTask == NULL) {
      continue;
    }

    // not report the status of fill-history task
    if ((*pTask)->info.fillHistory == 1) {
      continue;
    }

    STaskStatusEntry entry = {
        .id = *pId,
        .status = streamTaskGetStatus(*pTask)->state,
        .nodeId = hbMsg.vgId,
        .stage = pMeta->stage,
        .inputQUsed = SIZE_IN_MiB(streamQueueGetItemSize((*pTask)->inputq.queue)),
    };

    entry.inputRate = entry.inputQUsed * 100.0 / (2*STREAM_TASK_QUEUE_CAPACITY_IN_SIZE);
    if ((*pTask)->info.taskLevel == TASK_LEVEL__SINK) {
      entry.sinkQuota = (*pTask)->outputInfo.pTokenBucket->quotaRate;
      entry.sinkDataSize = SIZE_IN_MiB((*pTask)->execInfo.sink.dataSize);
    }

    if ((*pTask)->chkInfo.checkpointingId != 0) {
      entry.checkpointFailed = ((*pTask)->chkInfo.failedId >= (*pTask)->chkInfo.checkpointingId)? 1:0;
      entry.checkpointId = (*pTask)->chkInfo.checkpointingId;
      entry.chkpointTransId = (*pTask)->chkInfo.transId;

      if (entry.checkpointFailed) {
        stInfo("s-task:%s send kill checkpoint trans info, transId:%d", (*pTask)->id.idStr, (*pTask)->chkInfo.transId);
      }
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

    SRpcMsg msg = {0};
    initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen);

    pMeta->pHbInfo->hbCount += 1;
    stDebug("vgId:%d build and send hb to mnode, numOfTasks:%d total:%d", pMeta->vgId, hbMsg.numOfTasks,
            pMeta->pHbInfo->hbCount);

    tmsgSendReq(&epset, &msg);
  } else {
    stDebug("vgId:%d no tasks and no mnd epset, not send stream hb to mnode", pMeta->vgId);
  }

  _end:
  streamMetaClearHbMsg(&hbMsg);
  return TSDB_CODE_SUCCESS;
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
    taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr);
    taosReleaseRef(streamMetaId, rid);
    return;
  }

  stDebug("vgId:%d build stream task hb, leader:%d", pMeta->vgId, (pMeta->role == NODE_ROLE_LEADER));
  streamMetaRLock(pMeta);
  metaHeartbeatToMnodeImpl(pMeta);
  streamMetaRUnLock(pMeta);

  taosTmrReset(metaHbToMnode, META_HB_CHECK_INTERVAL, param, streamTimer, &pMeta->pHbInfo->hbTmr);
  taosReleaseRef(streamMetaId, rid);
}

bool streamMetaTaskInTimer(SStreamMeta* pMeta) {
  bool inTimer = false;
  streamMetaRLock(pMeta);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasksMap, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->status.timerActive >= 1) {
      stDebug("s-task:%s in timer, blocking tasks in vgId:%d restart", pTask->id.idStr, pMeta->vgId);
      inTimer = true;
    }
  }

  streamMetaRUnLock(pMeta);
  return inTimer;
}

void streamMetaNotifyClose(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;

  stDebug("vgId:%d notify all stream tasks that the vnode is closing. isLeader:%d startHb:%" PRId64 ", totalHb:%d",
          vgId, (pMeta->role == NODE_ROLE_LEADER), pMeta->pHbInfo->hbStart, pMeta->pHbInfo->hbCount);

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

void streamMetaResetStartInfo(STaskStartInfo* pStartInfo) {
  taosHashClear(pStartInfo->pReadyTaskSet);
  taosHashClear(pStartInfo->pFailedTaskSet);
  pStartInfo->tasksWillRestart = 0;
  pStartInfo->readyTs = 0;

  // reset the sentinel flag value to be 0
  pStartInfo->taskStarting = 0;
}

void streamMetaRLock(SStreamMeta* pMeta) {
//  stTrace("vgId:%d meta-rlock", pMeta->vgId);
  taosThreadRwlockRdlock(&pMeta->lock);
}

void streamMetaRUnLock(SStreamMeta* pMeta) {
//  stTrace("vgId:%d meta-runlock", pMeta->vgId);
  int32_t code = taosThreadRwlockUnlock(&pMeta->lock);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d meta-runlock failed, code:%d", pMeta->vgId, code);
  } else {
//    stTrace("vgId:%d meta-runlock completed", pMeta->vgId);
  }
}

void streamMetaWLock(SStreamMeta* pMeta) {
//  stTrace("vgId:%d meta-wlock", pMeta->vgId);
  taosThreadRwlockWrlock(&pMeta->lock);
//  stTrace("vgId:%d meta-wlock completed", pMeta->vgId);
}

void streamMetaWUnLock(SStreamMeta* pMeta) {
//  stTrace("vgId:%d meta-wunlock", pMeta->vgId);
  taosThreadRwlockUnlock(&pMeta->lock);
}

static void execHelper(struct SSchedMsg* pSchedMsg) {
  __async_exec_fn_t execFn = (__async_exec_fn_t)pSchedMsg->ahandle;
  int32_t           code = execFn(pSchedMsg->thandle);
  if (code != 0 && pSchedMsg->msg != NULL) {
    *(int32_t*)pSchedMsg->msg = code;
  }
}

int32_t streamMetaAsyncExec(SStreamMeta* pMeta, __stream_async_exec_fn_t fn, void* param, int32_t* code) {
  SSchedMsg schedMsg = {0};
  schedMsg.fp = execHelper;
  schedMsg.ahandle = fn;
  schedMsg.thandle = param;
  schedMsg.msg = code;
  return taosScheduleTask(pMeta->qHandle, &schedMsg);
}

SArray* streamMetaSendMsgBeforeCloseTasks(SStreamMeta* pMeta) {
  SArray* pTaskList = taosArrayDup(pMeta->pTaskList, NULL);

  bool sendMsg = pMeta->sendMsgBeforeClosing;
  if (!sendMsg) {
    stDebug("vgId:%d no need to send msg to mnode before closing tasks", pMeta->vgId);
    return pTaskList;
  }

  stDebug("vgId:%d send msg to mnode before closing all tasks", pMeta->vgId);

  // send hb msg to mnode before closing all tasks.
  int32_t numOfTasks = taosArrayGetSize(pTaskList);
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = streamMetaAcquireTaskNoLock(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    taosThreadMutexLock(&pTask->lock);

    SStreamTaskState* pState = streamTaskGetStatus(pTask);
    if (pState->state == TASK_STATUS__CK) {
      streamTaskSetCheckpointFailedId(pTask);
    } else {
      stDebug("s-task:%s status:%s not reset the checkpoint", pTask->id.idStr, pState->name);
    }

    taosThreadMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pMeta, pTask);
  }

  metaHeartbeatToMnodeImpl(pMeta);
  pMeta->sendMsgBeforeClosing = false;
  return pTaskList;
}

void streamMetaUpdateStageRole(SStreamMeta* pMeta, int64_t stage, bool isLeader) {
  streamMetaWLock(pMeta);

  int64_t prevStage = pMeta->stage;
  pMeta->stage = stage;

  // mark the sign to send msg before close all tasks
  if ((!isLeader) && (pMeta->role == NODE_ROLE_LEADER)) {
    pMeta->sendMsgBeforeClosing = true;
  }

  pMeta->role = (isLeader)? NODE_ROLE_LEADER:NODE_ROLE_FOLLOWER;
  streamMetaWUnLock(pMeta);

  if (isLeader) {
    stInfo("vgId:%d update meta stage:%" PRId64 ", prev:%" PRId64 " leader:%d, start to send Hb", pMeta->vgId,
           prevStage, stage, isLeader);
    streamMetaStartHb(pMeta);
  } else {
    stInfo("vgId:%d update meta stage:%" PRId64 " prev:%" PRId64 " leader:%d sendMsg beforeClosing:%d", pMeta->vgId,
           prevStage, stage, isLeader, pMeta->sendMsgBeforeClosing);
  }
}

static SArray* prepareBeforeStartTasks(SStreamMeta* pMeta) {
  streamMetaWLock(pMeta);

  SArray* pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  pMeta->startInfo.startTs = taosGetTimestampMs();

  streamMetaResetTaskStatus(pMeta);
  streamMetaWUnLock(pMeta);

  return pTaskList;
}

int32_t streamMetaStartAllTasks(SStreamMeta* pMeta) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t vgId = pMeta->vgId;

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  stInfo("vgId:%d start to check all %d stream task(s) downstream status", vgId, numOfTasks);

  if (numOfTasks == 0) {
    stInfo("vgId:%d start tasks completed", pMeta->vgId);
    return TSDB_CODE_SUCCESS;
  }

  int64_t now = taosGetTimestampMs();

  SArray* pTaskList = prepareBeforeStartTasks(pMeta);
  numOfTasks = taosArrayGetSize(pTaskList);

  // broadcast the check downstream tasks msg
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);

    // todo: may be we should find the related fill-history task and set it failed.
    // todo: use hashTable instead
    SStreamTask* pTask = streamMetaAcquireTask(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      stError("vgId:%d failed to acquire task:0x%x during start tasks", pMeta->vgId, pTaskId->taskId);
      streamMetaAddTaskLaunchResult(pMeta, pTaskId->streamId, pTaskId->taskId, 0, now, false);
      continue;
    }

    // fill-history task can only be launched by related stream tasks.
    STaskExecStatisInfo* pInfo = &pTask->execInfo;
    if (pTask->info.fillHistory == 1) {
      stDebug("s-task:%s fill-history task wait related stream task start", pTask->id.idStr);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    if (pTask->status.downstreamReady == 1) {
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        stDebug("s-task:%s downstream ready, no need to check downstream, check only related fill-history task",
                pTask->id.idStr);
        streamLaunchFillHistoryTask(pTask);
      }

      streamMetaAddTaskLaunchResult(pMeta, pTaskId->streamId, pTaskId->taskId, pInfo->init, pInfo->start, true);
      streamMetaReleaseTask(pMeta, pTask);
      continue;
    }

    int32_t ret = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_INIT);
    if (ret != TSDB_CODE_SUCCESS) {
      stError("vgId:%d failed to handle event:%d", pMeta->vgId, TASK_EVENT_INIT);
      code = ret;

      streamMetaAddTaskLaunchResult(pMeta, pTaskId->streamId, pTaskId->taskId, pInfo->init, pInfo->start, false);
      if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
        STaskId* pId = &pTask->hTaskInfo.id;
        streamMetaAddTaskLaunchResult(pMeta, pId->streamId, pId->taskId, pInfo->init, pInfo->start, false);
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
  }

  stInfo("vgId:%d start tasks completed", pMeta->vgId);
  taosArrayDestroy(pTaskList);
  return code;
}

int32_t streamMetaStopAllTasks(SStreamMeta* pMeta) {
  streamMetaRLock(pMeta);

  int32_t num = taosArrayGetSize(pMeta->pTaskList);
  stDebug("vgId:%d stop all %d stream task(s)", pMeta->vgId, num);
  if (num == 0) {
    stDebug("vgId:%d stop all %d task(s) completed, elapsed time:0 Sec.", pMeta->vgId, num);
    streamMetaRUnLock(pMeta);
    return TSDB_CODE_SUCCESS;
  }

  int64_t st = taosGetTimestampMs();

  // send hb msg to mnode before closing all tasks.
  SArray* pTaskList = streamMetaSendMsgBeforeCloseTasks(pMeta);
  int32_t numOfTasks = taosArrayGetSize(pTaskList);

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = streamMetaAcquireTaskNoLock(pMeta, pTaskId->streamId, pTaskId->taskId);
    if (pTask == NULL) {
      continue;
    }

    streamTaskStop(pTask);
    streamMetaReleaseTask(pMeta, pTask);
  }

  taosArrayDestroy(pTaskList);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  stDebug("vgId:%d stop all %d task(s) completed, elapsed time:%.2f Sec.", pMeta->vgId, num, el);

  streamMetaRUnLock(pMeta);
  return 0;
}

bool streamMetaAllTasksReady(const SStreamMeta* pMeta) {
  int32_t num = taosArrayGetSize(pMeta->pTaskList);
  for(int32_t i = 0; i < num; ++i) {
    STaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);
    SStreamTask** ppTask = taosHashGet(pMeta->pTasksMap, pTaskId, sizeof(*pTaskId));
    if (ppTask == NULL) {
      continue;
    }

    if ((*ppTask)->status.downstreamReady == 0) {
      return false;
    }
  }

  return true;
}

int32_t streamMetaStartOneTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  int32_t vgId = pMeta->vgId;
  stInfo("vgId:%d start to task:0x%x by checking downstream status", vgId, taskId);

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, streamId, taskId);
  if (pTask == NULL) {
    stError("vgId:%d failed to acquire task:0x%x during start tasks", pMeta->vgId, taskId);
    streamMetaAddTaskLaunchResult(pMeta, streamId, taskId, 0, taosGetTimestampMs(), false);
    return TSDB_CODE_STREAM_TASK_IVLD_STATUS;
  }

  // todo: may be we should find the related fill-history task and set it failed.

  // fill-history task can only be launched by related stream tasks.
  STaskExecStatisInfo* pInfo = &pTask->execInfo;
  if (pTask->info.fillHistory == 1) {
    streamMetaReleaseTask(pMeta, pTask);
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(pTask->status.downstreamReady == 0);

  int32_t ret = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_INIT);
  if (ret != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to handle event:%d", pMeta->vgId, TASK_EVENT_INIT);

    streamMetaAddTaskLaunchResult(pMeta, streamId, taskId, pInfo->init, pInfo->start, false);
    if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
      STaskId* pId = &pTask->hTaskInfo.id;
      streamMetaAddTaskLaunchResult(pMeta, pId->streamId, pId->taskId, pInfo->init, pInfo->start, false);
    }
  }

  streamMetaReleaseTask(pMeta, pTask);
  return ret;
}

static void displayStatusInfo(SStreamMeta* pMeta, SHashObj* pTaskSet, bool succ) {
  int32_t vgId = pMeta->vgId;
  void*   pIter = NULL;
  size_t  keyLen = 0;

  stInfo("vgId:%d %d tasks check-downstream completed %s", vgId, taosHashGetSize(pTaskSet),
         succ ? "success" : "failed");

  while ((pIter = taosHashIterate(pTaskSet, pIter)) != NULL) {
    STaskInitTs* pInfo = pIter;
    void*        key = taosHashGetKey(pIter, &keyLen);

    SStreamTask** pTask1 = taosHashGet(pMeta->pTasksMap, key, sizeof(STaskId));
    if (pTask1 == NULL) {
      stInfo("s-task:0x%x is dropped already, %s", (int32_t)((STaskId*)key)->taskId, succ ? "success" : "failed");
    } else {
      stInfo("s-task:%s level:%d vgId:%d, init:%" PRId64 ", initEnd:%" PRId64 ", %s", (*pTask1)->id.idStr,
             (*pTask1)->info.taskLevel, vgId, pInfo->start, pInfo->end, pInfo->success ? "success" : "failed");
    }
  }
}

int32_t streamMetaAddTaskLaunchResult(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, int64_t startTs,
                                      int64_t endTs, bool ready) {
  STaskStartInfo* pStartInfo = &pMeta->startInfo;
  STaskId         id = {.streamId = streamId, .taskId = taskId};

  streamMetaWLock(pMeta);

  if (pStartInfo->taskStarting != 1) {
    int64_t el = endTs - startTs;
    qDebug("vgId:%d not start all task(s), not record status, s-task:0x%x launch succ:%d elapsed time:%" PRId64 "ms",
           pMeta->vgId, taskId, ready, el);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  SHashObj* pDst = ready ? pStartInfo->pReadyTaskSet : pStartInfo->pFailedTaskSet;

  STaskInitTs initTs = {.start = startTs, .end = endTs, .success = ready};
  taosHashPut(pDst, &id, sizeof(id), &initTs, sizeof(STaskInitTs));

  int32_t numOfTotal = streamMetaGetNumOfTasks(pMeta);
  int32_t numOfRecv = taosHashGetSize(pStartInfo->pReadyTaskSet) + taosHashGetSize(pStartInfo->pFailedTaskSet);

  if (numOfRecv == numOfTotal) {
    pStartInfo->readyTs = taosGetTimestampMs();
    pStartInfo->elapsedTime = (pStartInfo->startTs != 0) ? pStartInfo->readyTs - pStartInfo->startTs : 0;

    stDebug("vgId:%d all %d task(s) check downstream completed, last completed task:0x%x (succ:%d) startTs:%" PRId64
                ", readyTs:%" PRId64 " total elapsed time:%.2fs",
            pMeta->vgId, numOfTotal, taskId, ready, pStartInfo->startTs, pStartInfo->readyTs,
            pStartInfo->elapsedTime / 1000.0);

    // print the initialization elapsed time and info
    displayStatusInfo(pMeta, pStartInfo->pReadyTaskSet, true);
    displayStatusInfo(pMeta, pStartInfo->pFailedTaskSet, false);
    streamMetaResetStartInfo(pStartInfo);
    streamMetaWUnLock(pMeta);

    pStartInfo->completeFn(pMeta);
  } else {
    streamMetaWUnLock(pMeta);
    stDebug("vgId:%d recv check downstream results, s-task:0x%x succ:%d, received:%d, total:%d", pMeta->vgId, taskId,
            ready, numOfRecv, numOfTotal);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t streamMetaResetTaskStatus(SStreamMeta* pMeta) {
  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);

  stDebug("vgId:%d reset all %d stream task(s) status to be uninit", pMeta->vgId, numOfTasks);
  if (numOfTasks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);

    STaskId       id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask** pTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    streamTaskResetStatus(*pTask);
  }

  return 0;
}