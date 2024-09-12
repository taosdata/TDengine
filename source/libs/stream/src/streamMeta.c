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
int32_t taskDbWrapperId = 0;

static int32_t streamMetaBegin(SStreamMeta* pMeta);
static void    streamMetaCloseImpl(void* arg);

typedef struct {
  TdThreadMutex mutex;
  SHashObj*     pTable;
} SMetaRefMgt;

SMetaRefMgt gMetaRefMgt;

int32_t metaRefMgtInit();
void    metaRefMgtCleanup();
int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid);

static void streamMetaEnvInit() {
  streamBackendId = taosOpenRef(64, streamBackendCleanup);
  streamBackendCfWrapperId = taosOpenRef(64, streamBackendHandleCleanup);
  taskDbWrapperId = taosOpenRef(64, taskDbDestroy2);

  streamMetaId = taosOpenRef(64, streamMetaCloseImpl);

  int32_t code = metaRefMgtInit();
  if (code) {
    stError("failed to init stream meta mgmt env, start failed");
    return;
  }

  code = streamTimerInit();
  if (code) {
    stError("failed to init stream meta env, start failed");
  }
}

void streamMetaInit() {
  int32_t code = taosThreadOnce(&streamMetaModuleInit, streamMetaEnvInit);
  if (code) {
    stError("failed to init stream Meta model, code:%s", tstrerror(code));
  }
}

void streamMetaCleanup() {
  taosCloseRef(streamBackendId);
  taosCloseRef(streamBackendCfWrapperId);
  taosCloseRef(streamMetaId);

  metaRefMgtCleanup();
  streamTimerCleanUp();
}

int32_t metaRefMgtInit() {
  int32_t code = taosThreadMutexInit(&(gMetaRefMgt.mutex), NULL);
  if (code) {
    return code;
  }

  if (code == 0) {
    gMetaRefMgt.pTable = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  }

  if (gMetaRefMgt.pTable == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  } else {
    return code;
  }
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

  streamMutexDestroy(&gMetaRefMgt.mutex);
}

int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid) {
  int32_t code = 0;
  void*   p = NULL;

  streamMutexLock(&gMetaRefMgt.mutex);

  p = taosHashGet(gMetaRefMgt.pTable, &vgId, sizeof(vgId));
  if (p == NULL) {
    SArray* pList = taosArrayInit(8, POINTER_BYTES);
    if (pList == NULL) {
      return terrno;
    }

    p = taosArrayPush(pList, &rid);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    code = taosHashPut(gMetaRefMgt.pTable, &vgId, sizeof(vgId), &pList, sizeof(void*));
    if (code) {
      stError("vgId:%d failed to put into metaRef table, rid:%" PRId64, (int32_t)vgId, *rid);
      return code;
    }
  } else {
    SArray* list = *(SArray**)p;
    void*   px = taosArrayPush(list, &rid);
    if (px == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  streamMutexUnlock(&gMetaRefMgt.mutex);
  return code;
}

int32_t streamMetaOpenTdb(SStreamMeta* pMeta) {
  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db, 0, 0, NULL) < 0) {
    stError("vgId:%d open file:%s failed, stream meta open failed", pMeta->vgId, pMeta->path);
    return -1;
  }

  if (tdbTbOpen("task.db", STREAM_TASK_KEY_LEN, -1, NULL, pMeta->db, &pMeta->pTaskDb, 0) < 0) {
    stError("vgId:%d, open task.db failed, stream meta open failed", pMeta->vgId);
    return -1;
  }

  if (tdbTbOpen("checkpoint.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pCheckpointDb, 0) < 0) {
    stError("vgId:%d, open checkpoint.db failed, stream meta open failed", pMeta->vgId);
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
  int8_t  ret = STREAM_STATA_COMPATIBLE;
  TBC*    pCur = NULL;
  int32_t code = 0;
  void*   pKey = NULL;
  int32_t kLen = 0;
  void*   pVal = NULL;
  int32_t vLen = 0;

  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {  // no task info, no stream
    return ret;
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    stError("vgId:%d failed to open stream meta file cursor, not perform compatible check, code:%s", pMeta->vgId,
            tstrerror(code));
    code = tdbTbcClose(pCur);
    if (code) {
      stError("vgId:%d failed to close meta file cursor, code:%s", pMeta->vgId, tstrerror(code));
    }

    return ret;
  }

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
  code = tdbTbcClose(pCur);
  if (code != 0) {
    stError("vgId:%d failed to close meta file cursor, code:%s", pMeta->vgId, tstrerror(code));
  }
  return ret;
}

int32_t streamMetaCvtDbFormat(SStreamMeta* pMeta) {
  int32_t code = 0;
  int64_t chkpId = streamMetaGetLatestCheckpointId(pMeta);
  terrno = 0;
  bool exist = streamBackendDataIsExist(pMeta->path, chkpId);
  if (exist == false) {
    code = terrno;
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
    stInfo("vgId:%d stream state need covert backend format", pMeta->vgId);
    return streamMetaCvtDbFormat(pMeta);
  } else if (compatible == STREAM_STATA_NO_COMPATIBLE) {
    stError(
        "vgId:%d stream read incompatible data, rm %s/vnode/vnode*/tq/stream if taosd cannot start, and rebuild stream "
        "manually",
        pMeta->vgId, tsDataDir);

    return -1;
  }

  return 0;
}

int32_t streamTaskSetDb(SStreamMeta* pMeta, SStreamTask* pTask, const char* key) {
  int32_t code = 0;
  int64_t chkpId = pTask->chkInfo.checkpointId;

  streamMutexLock(&pMeta->backendMutex);
  void** ppBackend = taosHashGet(pMeta->pTaskDbUnique, key, strlen(key));
  if ((ppBackend != NULL) && (*ppBackend != NULL)) {
    void* p = taskDbAddRef(*ppBackend);
    if (p == NULL) {
      stError("s-task:0x%x failed to ref backend", pTask->id.taskId);
      return TSDB_CODE_FAILED;
    }

    STaskDbWrapper* pBackend = *ppBackend;
    pBackend->pMeta = pMeta;
    pTask->pBackend = pBackend;

    streamMutexUnlock(&pMeta->backendMutex);
    stDebug("s-task:0x%x set backend %p", pTask->id.taskId, pBackend);
    return 0;
  }

  STaskDbWrapper* pBackend = NULL;
  int64_t         processVer = -1;
  while (1) {
    code = taskDbOpen(pMeta->path, key, chkpId, &processVer, &pBackend);
    if (code == 0) {
      break;
    }

    streamMutexUnlock(&pMeta->backendMutex);
    taosMsleep(1000);

    stDebug("backend held by other task, restart later, path:%s, key:%s", pMeta->path, key);
    streamMutexLock(&pMeta->backendMutex);
  }

  int64_t tref = taosAddRef(taskDbWrapperId, pBackend);
  pTask->pBackend = pBackend;
  pBackend->refId = tref;
  pBackend->pTask = pTask;
  pBackend->pMeta = pMeta;

  if (processVer != -1) {
    if (pTask->chkInfo.processedVer != processVer) {
      stWarn("s-task:%s vgId:%d update checkpointVer:%" PRId64 "->%" PRId64 " for checkpointId:%" PRId64,
             pTask->id.idStr, pTask->pMeta->vgId, pTask->chkInfo.processedVer, processVer, pTask->chkInfo.checkpointId);
      pTask->chkInfo.processedVer = processVer;
      pTask->chkInfo.checkpointVer = processVer;
      pTask->chkInfo.nextProcessVer = processVer + 1;
    } else {
      stInfo("s-task:%s vgId:%d processedVer:%" PRId64
             " in task meta equals to data in checkpoint data for checkpointId:%" PRId64,
             pTask->id.idStr, pTask->pMeta->vgId, pTask->chkInfo.processedVer, pTask->chkInfo.checkpointId);
    }
  }

  code = taosHashPut(pMeta->pTaskDbUnique, key, strlen(key), &pBackend, sizeof(void*));
  if (code) {
    stError("s-task:0x%x failed to put taskDb backend, code:out of memory", pTask->id.taskId);
  }
  streamMutexUnlock(&pMeta->backendMutex);

  stDebug("s-task:0x%x set backend %p", pTask->id.taskId, pBackend);
  return 0;
}

void streamMetaRemoveDB(void* arg, char* key) {
  if (arg == NULL || key == NULL) return;

  SStreamMeta* pMeta = arg;
  streamMutexLock(&pMeta->backendMutex);
  int32_t code = taosHashRemove(pMeta->pTaskDbUnique, key, strlen(key));
  if (code) {
    stError("vgId:%d failed to remove key:%s in taskDbUnique map", pMeta->vgId, key);
  }

  streamMutexUnlock(&pMeta->backendMutex);
}

int32_t streamMetaOpen(const char* path, void* ahandle, FTaskBuild buildTaskFn, FTaskExpand expandTaskFn, int32_t vgId,
                       int64_t stage, startComplete_fn_t fn, SStreamMeta** p) {
  int32_t code = 0;
  QRY_PARAM_CHECK(p);

  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    stError("vgId:%d failed to prepare stream meta, alloc size:%" PRIzu ", out of memory", vgId, sizeof(SStreamMeta));
    return terrno;
  }

  int32_t len = strlen(path) + 64;
  char*   tpath = taosMemoryCalloc(1, len);
  if (tpath == NULL) {
    code = terrno;
    goto _err;
  }

  sprintf(tpath, "%s%s%s", path, TD_DIRSEP, "stream");
  pMeta->path = tpath;

  code = streamMetaOpenTdb(pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  if ((code = streamMetaMayCvtDbFormat(pMeta)) < 0) {
    stError("vgId:%d convert sub info format failed, open stream meta failed, reason: %s", pMeta->vgId,
            tstrerror(terrno));
    goto _err;
  }

  if ((code = streamMetaBegin(pMeta) < 0)) {
    stError("vgId:%d begin trans for stream meta failed", pMeta->vgId);
    goto _err;
  }

  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);
  pMeta->pTasksMap = taosHashInit(64, fp, true, HASH_NO_LOCK);
  if (pMeta->pTasksMap == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->updateInfo.pTasks = taosHashInit(64, fp, false, HASH_NO_LOCK);
  if (pMeta->updateInfo.pTasks == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  code = streamMetaInitStartInfo(&pMeta->startInfo);
  if (code) {
    goto _err;
  }

  // task list
  pMeta->pTaskList = taosArrayInit(4, sizeof(SStreamTaskId));
  if (pMeta->pTaskList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->scanInfo.scanCounter = 0;
  pMeta->vgId = vgId;
  pMeta->ahandle = ahandle;
  pMeta->buildTaskFn = buildTaskFn;
  pMeta->expandTaskFn = expandTaskFn;
  pMeta->stage = stage;
  pMeta->role = (vgId == SNODE_HANDLE) ? NODE_ROLE_LEADER : NODE_ROLE_UNINIT;
  pMeta->updateInfo.activeTransId = -1;
  pMeta->updateInfo.completeTransId = -1;

  pMeta->startInfo.completeFn = fn;
  pMeta->pTaskDbUnique = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);

  pMeta->numOfPausedTasks = 0;
  pMeta->numOfStreamTasks = 0;
  pMeta->closeFlag = false;

  stInfo("vgId:%d open stream meta succ, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, pMeta->chkpId, stage);

  pMeta->rid = taosAddRef(streamMetaId, pMeta);

  // set the attribute when running on Linux OS
  TdThreadRwlockAttr attr;
  code = taosThreadRwlockAttrInit(&attr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

#ifdef LINUX
  code = pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }
#endif

  code = taosThreadRwlockInit(&pMeta->lock, &attr);
  if (code) {
    goto _err;
  }

  code = taosThreadRwlockAttrDestroy(&attr);
  if (code) {
    goto _err;
  }

  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  memcpy(pRid, &pMeta->rid, sizeof(pMeta->rid));
  code = metaRefMgtAdd(pMeta->vgId, pRid);
  if (code) {
    goto _err;
  }

  code = createMetaHbInfo(pRid, &pMeta->pHbInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  pMeta->qHandle = taosInitScheduler(32, 1, "stream-chkp", NULL);

  code = bkdMgtCreate(tpath, (SBkdMgt**)&pMeta->bkdChkptMgt);
  if (code != 0) {
    goto _err;
  }

  code = taosThreadMutexInit(&pMeta->backendMutex, NULL);

  *p = pMeta;
  return code;

_err:
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasksMap) taosHashCleanup(pMeta->pTasksMap);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) {
    int32_t ret = tdbTbClose(pMeta->pTaskDb);
    if (ret) {
      stError("vgId:%d tdb failed close task db, code:%s", pMeta->vgId, tstrerror(ret));
    }
    pMeta->pTaskDb = NULL;
  }
  if (pMeta->pCheckpointDb) {
    int32_t ret = tdbTbClose(pMeta->pCheckpointDb);
    if (ret) {
      stError("vgId:%d tdb failed close task checkpointDb, code:%s", pMeta->vgId, tstrerror(ret));
    }
  }
  if (pMeta->db) {
    int32_t ret = tdbClose(pMeta->db);
    if (ret) {
      stError("vgId:%d tdb failed close meta db, code:%s", pMeta->vgId, tstrerror(ret));
    }
  }

  if (pMeta->pHbInfo) taosMemoryFreeClear(pMeta->pHbInfo);
  if (pMeta->updateInfo.pTasks) taosHashCleanup(pMeta->updateInfo.pTasks);
  if (pMeta->startInfo.pReadyTaskSet) taosHashCleanup(pMeta->startInfo.pReadyTaskSet);
  if (pMeta->startInfo.pFailedTaskSet) taosHashCleanup(pMeta->startInfo.pFailedTaskSet);
  if (pMeta->bkdChkptMgt) bkdMgtDestroy(pMeta->bkdChkptMgt);
  taosMemoryFree(pMeta);

  stError("failed to open stream meta, reason:%s", tstrerror(terrno));
  return code;
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
    if (p->info.delaySchedParam != 0 && p->info.fillHistory == 0) {  // one more ref in timer
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", p->id.idStr, p->refCnt);
      streamTmrStop(p->schedInfo.pDelayTimer);
      p->info.delaySchedParam = 0;
      streamMetaReleaseTask(pMeta, p);
    }

    streamMetaReleaseTask(pMeta, p);
  }

  if (pMeta->streamBackendRid != 0) {
    int32_t code = taosRemoveRef(streamBackendId, pMeta->streamBackendRid);
    if (code) {
      stError("vgId:%d remove stream backend Ref failed, rid:%" PRId64, pMeta->vgId, pMeta->streamBackendRid);
    }
  }

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
  stDebug("vgId:%d start to close stream meta", pMeta->vgId);
  if (pMeta == NULL) {
    return;
  }
  int32_t code = taosRemoveRef(streamMetaId, pMeta->rid);
  if (code) {
    stError("vgId:%d failed to remove ref:%d, code:%s", pMeta->vgId, pMeta->rid, tstrerror(code));
  }
}

void streamMetaCloseImpl(void* arg) {
  SStreamMeta* pMeta = arg;
  if (pMeta == NULL) {
    return;
  }

  int32_t code = 0;
  int32_t vgId = pMeta->vgId;
  stDebug("vgId:%d start to do-close stream meta", vgId);

  streamMetaWLock(pMeta);
  streamMetaClear(pMeta);
  streamMetaWUnLock(pMeta);

  // already log the error, ignore here
  code = tdbAbort(pMeta->db, pMeta->txn);
  if (code) {
    stError("vgId:%d failed to jump of trans for tdb, code:%s", vgId, tstrerror(code));
  }
  code = tdbTbClose(pMeta->pTaskDb);
  if (code) {
    stError("vgId:%d failed to close taskDb, code:%s", vgId, tstrerror(code));
  }
  code = tdbTbClose(pMeta->pCheckpointDb);
  if (code) {
    stError("vgId:%d failed to close checkpointDb, code:%s", vgId, tstrerror(code));
  }
  code = tdbClose(pMeta->db);
  if (code) {
    stError("vgId:%d failed to close db, code:%s", vgId, tstrerror(code));
  }

  taosArrayDestroy(pMeta->pTaskList);
  taosArrayDestroy(pMeta->chkpSaved);
  taosArrayDestroy(pMeta->chkpInUse);

  taosHashCleanup(pMeta->pTasksMap);
  taosHashCleanup(pMeta->pTaskDbUnique);
  taosHashCleanup(pMeta->updateInfo.pTasks);

  streamMetaClearStartInfo(&pMeta->startInfo);

  destroyMetaHbInfo(pMeta->pHbInfo);
  pMeta->pHbInfo = NULL;

  taosMemoryFree(pMeta->path);
  streamMutexDestroy(&pMeta->backendMutex);

  taosCleanUpScheduler(pMeta->qHandle);
  taosMemoryFree(pMeta->qHandle);

  bkdMgtDestroy(pMeta->bkdChkptMgt);

  pMeta->role = NODE_ROLE_UNINIT;
  code = taosThreadRwlockDestroy(&pMeta->lock);
  if (code) {
    stError("vgId:%d destroy rwlock, code:%s", vgId, tstrerror(code));
  }

  taosMemoryFree(pMeta);
  stDebug("vgId:%d end to close stream meta", vgId);
}

// todo let's check the status for each task
int32_t streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  int32_t vgId = pTask->pMeta->vgId;
  void*   buf = NULL;
  int32_t len;
  int32_t code;
  tEncodeSize(tEncodeStreamTask, pTask, len, code);
  if (code < 0) {
    return -1;
  }

  buf = taosMemoryCalloc(1, len);
  if (buf == NULL) {
    return terrno;
  }

  if (pTask->ver < SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    pTask->ver = SSTREAM_TASK_VER;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  code = tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  if (code == -1) {
    stError("s-task:%s vgId:%d task meta encode failed, code:%s", pTask->id.idStr, vgId, tstrerror(code));
    return TSDB_CODE_INVALID_MSG;
  }

  int64_t id[2] = {pTask->id.streamId, pTask->id.taskId};

  code = tdbTbUpsert(pMeta->pTaskDb, id, STREAM_TASK_KEY_LEN, buf, len, pMeta->txn);
  if (code != TSDB_CODE_SUCCESS) {
    code = terrno;
    stError("s-task:%s vgId:%d task meta save to disk failed, code:%s", pTask->id.idStr, vgId, tstrerror(terrno));
  } else {
    stDebug("s-task:%s vgId:%d task meta save to disk", pTask->id.idStr, vgId);
  }

  taosMemoryFree(buf);
  return code;
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

  int32_t code = 0;
  STaskId id = streamTaskGetTaskId(pTask);
  void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (p != NULL) {
    stDebug("s-task:%" PRIx64 " already exist in meta, no need to register", id.taskId);
    return code;
  }

  if ((code = pMeta->buildTaskFn(pMeta->ahandle, pTask, ver)) != 0) {
    return code;
  }

  p = taosArrayPush(pMeta->pTaskList, &pTask->id);
  if (p == NULL) {
    stError("s-task:0x%" PRIx64 " failed to register task into meta-list, code: out of memory", id.taskId);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask, POINTER_BYTES);
  if (code) {
    stError("s-task:0x%" PRIx64 " failed to register task into meta-list, code: out of memory", id.taskId);
    return code;
  }

  if ((code = streamMetaSaveTask(pMeta, pTask)) != 0) {
    return code;
  }

  if ((code = streamMetaCommit(pMeta)) != 0) {
    return code;
  }

  if (pTask->info.fillHistory == 0) {
    int32_t val = atomic_add_fetch_32(&pMeta->numOfStreamTasks, 1);
  }

  *pAdded = true;
  return code;
}

int32_t streamMetaGetNumOfTasks(SStreamMeta* pMeta) {
  int32_t size = (int32_t)taosHashGetSize(pMeta->pTasksMap);
  int32_t sizeInList = taosArrayGetSize(pMeta->pTaskList);
  if (sizeInList != size) {
    stError("vgId:%d tasks number not consistent in list:%d and map:%d, ", pMeta->vgId, sizeInList, size);
  }

  return size;
}

int32_t streamMetaAcquireTaskNoLock(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, SStreamTask** pTask) {
  STaskId       id = {.streamId = streamId, .taskId = taskId};
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask == NULL || streamTaskShouldStop(*ppTask)) {
    *pTask = NULL;
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  int32_t ref = atomic_add_fetch_32(&(*ppTask)->refCnt, 1);
  stTrace("s-task:%s acquire task, ref:%d", (*ppTask)->id.idStr, ref);
  *pTask = *ppTask;
  return TSDB_CODE_SUCCESS;
}

int32_t streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, SStreamTask** pTask) {
  streamMetaRLock(pMeta);
  int32_t code = streamMetaAcquireTaskNoLock(pMeta, streamId, taskId, pTask);
  streamMetaRUnLock(pMeta);
  return code;
}

void streamMetaAcquireOneTask(SStreamTask* pTask) {
  int32_t ref = atomic_add_fetch_32(&pTask->refCnt, 1);
  stTrace("s-task:%s acquire task, ref:%d", pTask->id.idStr, ref);
}

void streamMetaReleaseTask(SStreamMeta* UNUSED_PARAM(pMeta), SStreamTask* pTask) {
  int32_t taskId = pTask->id.taskId;
  int32_t ref = atomic_sub_fetch_32(&pTask->refCnt, 1);

  // not safe to use the pTask->id.idStr, since pTask may be released by other threads when print logs.
  if (ref > 0) {
    stTrace("s-task:0x%x release task, ref:%d", taskId, ref);
  } else if (ref == 0) {
    stTrace("s-task:0x%x all refs are gone, free it", taskId);
    tFreeStreamTask(pTask);
  } else if (ref < 0) {
    stError("task ref is invalid, ref:%d, 0x%x", ref, taskId);
  }
}

static void doRemoveIdFromList(SArray* pTaskList, int32_t num, SStreamTaskId* id) {
  bool remove = false;
  for (int32_t i = 0; i < num; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    if (pTaskId->streamId == id->streamId && pTaskId->taskId == id->taskId) {
      taosArrayRemove(pTaskList, i);
      remove = true;
      break;
    }
  }

  if (!remove) {
    stError("s-task:0x%x not in meta task list, internal error", id->taskId);
  }
}

static int32_t streamTaskSendTransSuccessMsg(SStreamTask* pTask, void* param) {
  int32_t code = 0;
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    code = streamTaskSendCheckpointSourceRsp(pTask);
    if (code) {
      stError("s-task:%s vgId:%d failed to send checkpoint-source rsp, code:%s", pTask->id.idStr, pTask->pMeta->vgId,
              tstrerror(code));
    }
  }
  return code;
}

int32_t streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  SStreamTask* pTask = NULL;
  int32_t      vgId = pMeta->vgId;
  int32_t      code = 0;
  STaskId      id = {.streamId = streamId, .taskId = taskId};

  // pre-delete operation
  streamMetaWLock(pMeta);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask) {
    pTask = *ppTask;

    // desc the paused task counter
    if (streamTaskShouldPause(pTask)) {
      int32_t num = atomic_sub_fetch_32(&pMeta->numOfPausedTasks, 1);
      stInfo("vgId:%d s-task:%s drop stream task. pause task num:%d", vgId, pTask->id.idStr, num);
    }

    // handle the dropping event
    code = streamTaskHandleEventAsync(pTask->status.pSM, TASK_EVENT_DROPPING, streamTaskSendTransSuccessMsg, NULL);
    if (code) {
      stError("s-task:0x%" PRIx64 " failed to handle dropping event async, code:%s", id.taskId, tstrerror(code));
    }
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may be dropped already", vgId, taskId);
    streamMetaWUnLock(pMeta);
    return 0;
  }

  streamMetaWUnLock(pMeta);

  stDebug("s-task:0x%x vgId:%d set task status:dropping and start to unregister it", taskId, vgId);

  while (1) {
    int32_t timerActive = 0;

    streamMetaRLock(pMeta);
    ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (ppTask) {
      // to make sure check status will not start the check downstream status when we start to check timerActive count.
      streamMutexLock(&pTask->taskCheckInfo.checkInfoLock);
      timerActive = (*ppTask)->status.timerActive;
      streamMutexUnlock(&pTask->taskCheckInfo.checkInfoLock);
    }
    streamMetaRUnLock(pMeta);

    if (timerActive > 0) {
      taosMsleep(100);
      stDebug("s-task:0x%" PRIx64 " wait for quit from timer", id.taskId);
    } else {
      break;
    }
  }

  // let's do delete of stream task
  streamMetaWLock(pMeta);

  ppTask = (SStreamTask**)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (ppTask) {
    pTask = *ppTask;
    // it is an fill-history task, remove the related stream task's id that points to it
    if (pTask->info.fillHistory == 0) {
      int32_t ret = atomic_sub_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    code = taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    doRemoveIdFromList(pMeta->pTaskList, (int32_t)taosArrayGetSize(pMeta->pTaskList), &pTask->id);
    code = streamMetaRemoveTask(pMeta, &id);
    if (code) {
      stError("vgId:%d failed to remove task:0x%" PRIx64 ", code:%s", pMeta->vgId, id.taskId, tstrerror(code));
    }

    int32_t size = (int32_t) taosHashGetSize(pMeta->pTasksMap);
    int32_t sizeInList = taosArrayGetSize(pMeta->pTaskList);
    if (sizeInList != size) {
      stError("vgId:%d tasks number not consistent in list:%d and map:%d, ", vgId, sizeInList, size);
    }
    streamMetaWUnLock(pMeta);

    int32_t numOfTmr = pTask->status.timerActive;
    if (numOfTmr != 0) {
      stError("s-task:%s vgId:%d invalid timer Active record:%d, internal error", pTask->id.idStr, vgId, numOfTmr);
    }

    if (pTask->info.delaySchedParam != 0 && pTask->info.fillHistory == 0) {
      stDebug("s-task:%s stop schedTimer, and (before) desc ref:%d", pTask->id.idStr, pTask->refCnt);
      streamTmrStop(pTask->schedInfo.pDelayTimer);
      pTask->info.delaySchedParam = 0;
      streamMetaReleaseTask(pMeta, pTask);
    }

    streamMetaReleaseTask(pMeta, pTask);
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may have been dropped already", vgId, taskId);
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

int32_t streamMetaCommit(SStreamMeta* pMeta) {
  int32_t code = 0;
  code = tdbCommit(pMeta->db, pMeta->txn);
  if (code != 0) {
    stError("vgId:%d failed to commit stream meta", pMeta->vgId);
    return code;
  }

  code = tdbPostCommit(pMeta->db, pMeta->txn);
  if (code != 0) {
    stError("vgId:%d failed to do post-commit stream meta", pMeta->vgId);
    return code;
  }

  code = tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
                  TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (code != 0) {
    stError("vgId:%d failed to begin trans", pMeta->vgId);
    return code;
  }

  stDebug("vgId:%d stream meta file commit completed", pMeta->vgId);
  return code;
}

int64_t streamMetaGetLatestCheckpointId(SStreamMeta* pMeta) {
  int64_t checkpointId = 0;
  int32_t code = 0;

  TBC* pCur = NULL;
  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    stError("failed to open stream meta file, the latest checkpointId is 0, vgId:%d", pMeta->vgId);
    return checkpointId;
  }

  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    stError("failed to move stream meta file cursor, the latest checkpointId is 0, vgId:%d", pMeta->vgId);
    int32_t ret = tdbTbcClose(pCur);
    if (ret != 0) {
      stError("vgId:%d failed to close meta file cursor, code:%s", pMeta->vgId, tstrerror(ret));
    }
    return checkpointId;
  }

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

    checkpointId = TMAX(checkpointId, info.checkpointId);
  }

  stDebug("vgId:%d get max checkpointId:%" PRId64, pMeta->vgId, checkpointId);

  tdbFree(pKey);
  tdbFree(pVal);

  int32_t ret = tdbTbcClose(pCur);
  if (ret != 0) {
    stError("vgId:%d failed to close meta file cursor, code:%s", pMeta->vgId, tstrerror(ret));
  }

  return checkpointId;
}

// not allowed to return error code
void streamMetaLoadAllTasks(SStreamMeta* pMeta) {
  TBC*     pCur = NULL;
  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;
  int32_t  vgId = 0;
  int32_t  code = 0;
  SArray*  pRecycleList = NULL;

  if (pMeta == NULL) {
    return;
  }

  pRecycleList = taosArrayInit(4, sizeof(STaskId));
  if (pRecycleList == NULL) {
    stError("vgId:%d failed prepare load all tasks, code:out of memory", vgId);
    return;
  }

  vgId = pMeta->vgId;
  stInfo("vgId:%d load stream tasks from meta files", vgId);

  code = tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d failed to open stream meta, code:%s, not load any stream tasks", vgId, tstrerror(terrno));
    taosArrayDestroy(pRecycleList);
    return;
  }

  code = tdbTbcMoveToFirst(pCur);
  if (code) {
    stError("vgId:%d failed to open stream meta cursor, code:%s, not load any stream tasks", vgId, tstrerror(terrno));
    taosArrayDestroy(pRecycleList);
    int32_t ret = tdbTbcClose(pCur);
    if (ret != 0) {
      stError("vgId:%d failed to close meta file cursor, code:%s", pMeta->vgId, tstrerror(ret));
    }
    return;
  }

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    if (pVal == NULL || vLen == 0) {
      break;
    }

    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      stError("vgId:%d failed to load stream task from meta-files, code:%s", vgId, tstrerror(terrno));
      break;
    }

    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTask(&decoder, pTask) < 0) {
      tDecoderClear(&decoder);
      tFreeStreamTask(pTask);
      stError(
          "vgId:%d stream read incompatible data, rm %s/vnode/vnode*/tq/stream if taosd cannot start, and rebuild "
          "stream manually",
          vgId, tsDataDir);
      break;
    }
    tDecoderClear(&decoder);

    if (pTask->status.taskStatus == TASK_STATUS__DROPPING) {
      int32_t taskId = pTask->id.taskId;
      tFreeStreamTask(pTask);

      STaskId id = streamTaskGetTaskId(pTask);
      void* px = taosArrayPush(pRecycleList, &id);
      if (px == NULL) {
        stError("s-task:0x%x failed record the task into recycle list due to out of memory", taskId);
      }

      int32_t total = taosArrayGetSize(pRecycleList);
      stDebug("s-task:0x%x is already dropped, add into recycle list, total:%d", taskId, total);
      continue;
    }

    stDebug("s-task:0x%" PRIx64 "-0x%x vgId:%d loaded from meta file, checkpointId:%" PRId64 " checkpointVer:%" PRId64,
            pTask->id.streamId, pTask->id.taskId, vgId, pTask->chkInfo.checkpointId, pTask->chkInfo.checkpointVer);

    // do duplicate task check.
    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (p == NULL) {
      code = pMeta->buildTaskFn(pMeta->ahandle, pTask, pTask->chkInfo.checkpointVer + 1);
      if (code < 0) {
        stError("failed to load s-task:0x%" PRIx64 ", code:%s, continue", id.taskId, tstrerror(terrno));
        tFreeStreamTask(pTask);
        continue;
      }

      void* px = taosArrayPush(pMeta->pTaskList, &pTask->id);
      if (px == NULL) {
        stFatal("s-task:0x%x failed to add into task list due to out of memory", pTask->id.taskId);
      }
    } else {
      // todo this should replace the existed object put by replay creating stream task msg from mnode
      stError("s-task:0x%x already added into table meta by replaying WAL, need check", pTask->id.taskId);
      tFreeStreamTask(pTask);
      continue;
    }

    if (taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask, POINTER_BYTES) != 0) {
      stError("s-task:0x%x failed to put into hashTable, code:%s, continue", pTask->id.taskId, tstrerror(terrno));
      void* px = taosArrayPop(pMeta->pTaskList);
      tFreeStreamTask(pTask);
      continue;
    }

    if (pTask->info.fillHistory == 0) {
      int32_t val = atomic_add_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    if (streamTaskShouldPause(pTask)) {
      int32_t val = atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
    }
  }

  tdbFree(pKey);
  tdbFree(pVal);

  if (tdbTbcClose(pCur) < 0) {
    stError("vgId:%d failed to close meta-file cursor, code:%s, continue", vgId, tstrerror(terrno));
  }

  if (taosArrayGetSize(pRecycleList) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(pRecycleList); ++i) {
      STaskId* pId = taosArrayGet(pRecycleList, i);
      code = streamMetaRemoveTask(pMeta, pId);
      if (code) {
        stError("s-task:0x%" PRIx64 " failed to remove task, code:%s", pId->taskId, tstrerror(code));
      }
    }
  }

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  stDebug("vgId:%d load %d tasks into meta from disk completed, streamTask:%d, paused:%d", pMeta->vgId, numOfTasks,
          pMeta->numOfStreamTasks, pMeta->numOfPausedTasks);

  taosArrayDestroy(pRecycleList);
  code = streamMetaCommit(pMeta);
  if (code) {
    stError("vgId:%d failed to commit, code:%s", pMeta->vgId, tstrerror(code));
  }
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
      stDebug("s-task:%s in timer, blocking tasks in vgId:%d restart, set closing again", pTask->id.idStr, pMeta->vgId);
      int32_t code = streamTaskStop(pTask);
      if (code) {
        stError("s-task:%s failed to stop task, code:%s", pTask->id.idStr, tstrerror(code));
      }
      inTimer = true;
    }
  }

  streamMetaRUnLock(pMeta);
  return inTimer;
}

void streamMetaNotifyClose(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;
  int64_t startTs = 0;
  int32_t sendCount = 0;
  streamMetaGetHbSendInfo(pMeta->pHbInfo, &startTs, &sendCount);

  stInfo("vgId:%d notify all stream tasks that current vnode is closing. isLeader:%d startHb:%" PRId64 ", totalHb:%d",
         vgId, (pMeta->role == NODE_ROLE_LEADER), startTs, sendCount);

  // wait for the stream meta hb function stopping
  streamMetaWaitForHbTmrQuit(pMeta);

  streamMetaWLock(pMeta);

  pMeta->closeFlag = true;
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasksMap, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    stDebug("vgId:%d s-task:%s set task closing flag", vgId, pTask->id.idStr);
    int32_t code = streamTaskStop(pTask);
    if (code) {
      stError("vgId:%d failed to stop task:0x%x, code:%s", vgId, pTask->id.taskId, tstrerror(code));
    }
  }

  streamMetaWUnLock(pMeta);

  stDebug("vgId:%d start to check all tasks for closing", vgId);
  int64_t st = taosGetTimestampMs();

  while (streamMetaTaskInTimer(pMeta)) {
    stDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pMeta->vgId);
    taosMsleep(100);
  }

  streamMetaRLock(pMeta);

  SArray* pTaskList = NULL;
  int32_t code = streamMetaSendMsgBeforeCloseTasks(pMeta, &pTaskList);
  if (code != TSDB_CODE_SUCCESS) {
  }

  streamMetaRUnLock(pMeta);

  if (pTaskList != NULL) {
    taosArrayDestroy(pTaskList);
  }

  int64_t el = taosGetTimestampMs() - st;
  stDebug("vgId:%d all stream tasks are not in timer, continue close, elapsed time:%" PRId64 " ms", pMeta->vgId, el);
}

void streamMetaStartHb(SStreamMeta* pMeta) {
  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  if (pRid == NULL) {
    stError("vgId:%d failed to prepare the metaHb to mnode, hbMsg will not started, code: out of memory", pMeta->vgId);
    return;
  }

  int32_t code = metaRefMgtAdd(pMeta->vgId, pRid);
  if (code) {
    return;
  }

  *pRid = pMeta->rid;
  streamMetaHbToMnode(pRid, NULL);
}

void streamMetaRLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-rlock", pMeta->vgId);
  int32_t code = taosThreadRwlockRdlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d meta-rlock failed, code:%s", pMeta->vgId, tstrerror(code));
  }
}

void streamMetaRUnLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-runlock", pMeta->vgId);
  int32_t code = taosThreadRwlockUnlock(&pMeta->lock);
  if (code != TSDB_CODE_SUCCESS) {
    stError("vgId:%d meta-runlock failed, code:%s", pMeta->vgId, tstrerror(code));
  } else {
    //    stTrace("vgId:%d meta-runlock completed", pMeta->vgId);
  }
}

void streamMetaWLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-wlock", pMeta->vgId);
  int32_t code = taosThreadRwlockWrlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d failed to apply wlock, code:%s", pMeta->vgId, tstrerror(code));
  }
}

void streamMetaWUnLock(SStreamMeta* pMeta) {
  //  stTrace("vgId:%d meta-wunlock", pMeta->vgId);
  int32_t code = taosThreadRwlockUnlock(&pMeta->lock);
  if (code) {
    stError("vgId:%d failed to apply wunlock, code:%s", pMeta->vgId, tstrerror(code));
  }
}

int32_t streamMetaSendMsgBeforeCloseTasks(SStreamMeta* pMeta, SArray** pList) {
  QRY_PARAM_CHECK(pList);

  int32_t code = 0;
  SArray* pTaskList = taosArrayDup(pMeta->pTaskList, NULL);
  if (pTaskList == NULL) {
    stError("failed to generate the task list during send hbMsg to mnode, vgId:%d, code: out of memory", pMeta->vgId);
    return terrno;
  }

  *pList = pTaskList;

  bool sendMsg = pMeta->sendMsgBeforeClosing;
  if (!sendMsg) {
    stDebug("vgId:%d no need to send msg to mnode before closing tasks", pMeta->vgId);
    return TSDB_CODE_SUCCESS;
  }

  stDebug("vgId:%d send msg to mnode before closing all tasks", pMeta->vgId);

  // send hb msg to mnode before closing all tasks.
  int32_t numOfTasks = taosArrayGetSize(pTaskList);
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = NULL;

    code = streamMetaAcquireTaskNoLock(pMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if (code != TSDB_CODE_SUCCESS) {  // this error is ignored
      continue;
    }

    streamTaskSetCheckpointFailed(pTask);
    streamMetaReleaseTask(pMeta, pTask);
  }

  code = streamMetaSendHbHelper(pMeta);
  pMeta->sendMsgBeforeClosing = false;
  return TSDB_CODE_SUCCESS;  // always return true
}

void streamMetaUpdateStageRole(SStreamMeta* pMeta, int64_t stage, bool isLeader) {
  streamMetaWLock(pMeta);

  int64_t prevStage = pMeta->stage;
  pMeta->stage = stage;

  // mark the sign to send msg before close all tasks
  // 1. for leader vnode, always send msg before closing
  // 2. for follower vnode, if it's is changed from leader, also sending msg before closing.
  if (pMeta->role == NODE_ROLE_LEADER) {
    pMeta->sendMsgBeforeClosing = true;
  }

  pMeta->role = (isLeader) ? NODE_ROLE_LEADER : NODE_ROLE_FOLLOWER;
  streamMetaWUnLock(pMeta);

  if (isLeader) {
    stInfo("vgId:%d update meta stage:%" PRId64 ", prev:%" PRId64 " leader:%d, start to send Hb, rid:%" PRId64,
           pMeta->vgId, prevStage, stage, isLeader, pMeta->rid);
    streamMetaStartHb(pMeta);
  } else {
    stInfo("vgId:%d update meta stage:%" PRId64 " prev:%" PRId64 " leader:%d sendMsg beforeClosing:%d", pMeta->vgId,
           prevStage, stage, isLeader, pMeta->sendMsgBeforeClosing);
  }
}

bool streamMetaAllTasksReady(const SStreamMeta* pMeta) {
  int32_t num = taosArrayGetSize(pMeta->pTaskList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    STaskId        id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask**  ppTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
    if (ppTask == NULL) {
      continue;
    }

    if ((*ppTask)->status.downstreamReady == 0) {
      return false;
    }
  }

  return true;
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

int32_t streamMetaAddFailedTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t now = taosGetTimestampMs();
  int64_t startTs = 0;
  bool    hasFillhistoryTask = false;
  STaskId hId = {0};

  stDebug("vgId:%d add start failed task:0x%x", pMeta->vgId, taskId);

  streamMetaRLock(pMeta);

  STaskId       id = {.streamId = streamId, .taskId = taskId};
  SStreamTask** ppTask = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));

  if (ppTask != NULL) {
    startTs = (*ppTask)->taskCheckInfo.startTs;
    hasFillhistoryTask = HAS_RELATED_FILLHISTORY_TASK(*ppTask);
    hId = (*ppTask)->hTaskInfo.id;

    streamMetaRUnLock(pMeta);

    // add the failed task info, along with the related fill-history task info into tasks list.
    code = streamMetaAddTaskLaunchResult(pMeta, streamId, taskId, startTs, now, false);
    if (hasFillhistoryTask) {
      code = streamMetaAddTaskLaunchResult(pMeta, hId.streamId, hId.taskId, startTs, now, false);
    }
  } else {
    streamMetaRUnLock(pMeta);

    stError("failed to locate the stream task:0x%" PRIx64 "-0x%x (vgId:%d), it may have been destroyed or stopped",
            streamId, taskId, pMeta->vgId);
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  return code;
}

void streamMetaAddFailedTaskSelf(SStreamTask* pTask, int64_t failedTs) {
  int32_t startTs = pTask->execInfo.checkTs;
  int32_t code = streamMetaAddTaskLaunchResult(pTask->pMeta, pTask->id.streamId, pTask->id.taskId, startTs, failedTs, false);
  if (code) {
    stError("s-task:%s failed to add self task failed to start", pTask->id.idStr, tstrerror(code));
  }

  // automatically set the related fill-history task to be failed.
  if (HAS_RELATED_FILLHISTORY_TASK(pTask)) {
    STaskId* pId = &pTask->hTaskInfo.id;
    code = streamMetaAddTaskLaunchResult(pTask->pMeta, pId->streamId, pId->taskId, startTs, failedTs, false);
    if (code) {
      stError("s-task:0x%" PRIx64 " failed to add self task failed to start", pId->taskId, tstrerror(code));
    }
  }
}

void streamMetaAddIntoUpdateTaskList(SStreamMeta* pMeta, SStreamTask* pTask, SStreamTask* pHTask, int32_t transId,
                                     int64_t startTs) {
  const char* id = pTask->id.idStr;
  int32_t     vgId = pTask->pMeta->vgId;
  int32_t     code = 0;

  // keep the already updated info
  STaskUpdateEntry entry = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId, .transId = transId};
  code = taosHashPut(pMeta->updateInfo.pTasks, &entry, sizeof(entry), NULL, 0);
  if (code != 0) {
    stError("s-task:%s failed to put updateTask into update list", id);
  }

  int64_t el = taosGetTimestampMs() - startTs;
  if (pHTask != NULL) {
    STaskUpdateEntry hEntry = {.streamId = pHTask->id.streamId, .taskId = pHTask->id.taskId, .transId = transId};
    code = taosHashPut(pMeta->updateInfo.pTasks, &hEntry, sizeof(hEntry), NULL, 0);
    if (code != 0) {
      stError("s-task:%s failed to put updateTask into update list", id);
    } else {
      stDebug("s-task:%s vgId:%d transId:%d task nodeEp update completed, streamTask/hTask closed, elapsed:%" PRId64
              " ms",
              id, vgId, transId, el);
    }
  } else {
    stDebug("s-task:%s vgId:%d transId:%d task nodeEp update completed, streamTask closed, elapsed time:%" PRId64 "ms",
            id, vgId, transId, el);
  }
}

void streamMetaClearSetUpdateTaskListComplete(SStreamMeta* pMeta) {
  STaskUpdateInfo* pInfo = &pMeta->updateInfo;

  taosHashClear(pInfo->pTasks);

  int32_t prev = pInfo->completeTransId;
  pInfo->completeTransId = pInfo->activeTransId;
  pInfo->activeTransId = -1;
  pInfo->completeTs = taosGetTimestampMs();

  stDebug("vgId:%d set the nodeEp update complete, ts:%" PRId64 ", complete transId:%d->%d, reset active transId",
          pMeta->vgId, pInfo->completeTs, prev, pInfo->completeTransId);
}

bool streamMetaInitUpdateTaskList(SStreamMeta* pMeta, int32_t transId) {
  STaskUpdateInfo* pInfo = &pMeta->updateInfo;

  if (transId > pInfo->completeTransId) {
    if (pInfo->activeTransId == -1) {
      taosHashClear(pInfo->pTasks);
      pInfo->activeTransId = transId;

      stInfo("vgId:%d set the active epset update transId:%d, prev complete transId:%d", pMeta->vgId, transId,
             pInfo->completeTransId);
      return true;
    } else {
      if (pInfo->activeTransId == transId) {
        // do nothing
        return true;
      } else if (transId < pInfo->activeTransId) {
        stError("vgId:%d invalid(out of order)epset update transId:%d, active transId:%d, complete transId:%d, discard",
                pMeta->vgId, transId, pInfo->activeTransId, pInfo->completeTransId);
        return false;
      } else {  // transId > pInfo->activeTransId
        taosHashClear(pInfo->pTasks);
        int32_t prev = pInfo->activeTransId;
        pInfo->activeTransId = transId;

        stInfo("vgId:%d active epset update transId updated from:%d to %d, prev complete transId:%d", pMeta->vgId,
               transId, prev, pInfo->completeTransId);
        return true;
      }
    }
  } else if (transId == pInfo->completeTransId) {
    stError("vgId:%d already handled epset update transId:%d, completeTs:%" PRId64 " ignore", pMeta->vgId, transId,
            pInfo->completeTs);
    return false;
  } else {  // pInfo->completeTransId > transId
    stError("vgId:%d disorder update nodeEp msg recv, prev completed epset update transId:%d, recv:%d, discard",
            pMeta->vgId, pInfo->activeTransId, transId);
    return false;
  }
}
