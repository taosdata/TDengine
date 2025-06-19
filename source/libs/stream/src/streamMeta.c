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

#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tglobal.h"
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
int32_t streamTaskRefPool = 0;

static int32_t streamMetaBegin(SStreamMeta* pMeta);
static void    streamMetaCloseImpl(void* arg);

typedef struct {
  TdThreadMutex mutex;
  SHashObj*     pTable;
} SMetaRefMgt;

SMetaRefMgt gMetaRefMgt;

int32_t metaRefMgtInit();
void    metaRefMgtCleanup();

static void streamMetaEnvInit() {
  streamBackendId = taosOpenRef(64, streamBackendCleanup);
  streamBackendCfWrapperId = taosOpenRef(64, streamBackendHandleCleanup);
  taskDbWrapperId = taosOpenRef(64, taskDbDestroy2);

  streamMetaRefPool = taosOpenRef(64, streamMetaCloseImpl);
  streamTaskRefPool = taosOpenRef(64, tFreeStreamTask);

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
  taosCloseRef(streamMetaRefPool);
  taosCloseRef(streamTaskRefPool);

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
    return terrno;
  } else {
    return code;
  }
}

void metaRefMgtCleanup() {
  void* pIter = taosHashIterate(gMetaRefMgt.pTable, NULL);
  while (pIter) {
    int64_t* p = *(int64_t**)pIter;
    taosMemoryFree(p);
    pIter = taosHashIterate(gMetaRefMgt.pTable, pIter);
  }

  taosHashCleanup(gMetaRefMgt.pTable);
  streamMutexDestroy(&gMetaRefMgt.mutex);
}

int32_t metaRefMgtAdd(int64_t vgId, int64_t* rid) {
  int32_t code = 0;
  void*   p = NULL;

  streamMutexLock(&gMetaRefMgt.mutex);

  p = taosHashGet(gMetaRefMgt.pTable, &rid, sizeof(rid));
  if (p == NULL) {
    code = taosHashPut(gMetaRefMgt.pTable, &rid, sizeof(rid), &rid, sizeof(void*));
    if (code) {
      stError("vgId:%d failed to put into refId mgt, refId:%" PRId64 " %p, code:%s", (int32_t)vgId, *rid, rid,
              tstrerror(code));
      return code;
    } else {  // not
              //      stInfo("add refId:%"PRId64" vgId:%d, %p", *rid, (int32_t)vgId, rid);
    }
  } else {
    stFatal("try to add refId:%" PRId64 " vgId:%d, %p that already added into mgt", *rid, (int32_t)vgId, rid);
  }

  streamMutexUnlock(&gMetaRefMgt.mutex);
  return code;
}

void metaRefMgtRemove(int64_t* pRefId) {
  streamMutexLock(&gMetaRefMgt.mutex);

  int32_t code = taosHashRemove(gMetaRefMgt.pTable, &pRefId, sizeof(pRefId));
  taosMemoryFree(pRefId);
  streamMutexUnlock(&gMetaRefMgt.mutex);
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
    tdbTbcClose(pCur);
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
      tDecoderClear(&decoder);
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
  int32_t          code = 0;
  SBackendWrapper* pBackend = NULL;
  int64_t          chkpId = streamMetaGetLatestCheckpointId(pMeta);

  terrno = 0;
  bool exist = streamBackendDataIsExist(pMeta->path, chkpId);
  if (exist == false) {
    code = terrno;
    return code;
  }

  code = streamBackendInit(pMeta->path, chkpId, pMeta->vgId, &pBackend);
  if (code) {
    return code;
  }

  void* pIter = taosHashIterate(pBackend->cfInst, NULL);
  while (pIter) {
    void* key = taosHashGetKey(pIter, NULL);
    code = streamStateCvtDataFormat(pMeta->path, key, *(void**)pIter);
    if (code != 0) {
      stError("vgId:%d failed to cvt data", pMeta->vgId);
      goto _EXIT;
    }

    pIter = taosHashIterate(pBackend->cfInst, pIter);
  }

_EXIT:
  streamBackendCleanup((void*)pBackend);

  if (code == 0) {
    int32_t len = strlen(pMeta->path) + 32;
    char*   state = taosMemoryCalloc(1, len);
    if (state != NULL) {
      (void)snprintf(state, len, "%s%s%s", pMeta->path, TD_DIRSEP, "state");
      taosRemoveDir(state);
      taosMemoryFree(state);
    } else {
      stError("vgId:%d, failed to remove file dir:%s, since:%s", pMeta->vgId, pMeta->path, tstrerror(code));
    }
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

int8_t streamTaskShouldRecalated(SStreamTask* pTask) { return pTask->info.fillHistory == 2 ? 1 : 0; }

int32_t streamTaskSetDb(SStreamMeta* pMeta, SStreamTask* pTask, const char* key, uint8_t recalated) {
  int32_t code = 0;
  int64_t chkpId = pTask->chkInfo.checkpointId;

  // int8_t recalated = streamTaskShouldRecalated(pTask);

  streamMutexLock(&pMeta->backendMutex);
  // streamId--taskId
  void** ppBackend = taosHashGet(pMeta->pTaskDbUnique, key, strlen(key));
  if ((ppBackend != NULL) && (*ppBackend != NULL)) {
    void* p = taskDbAddRef(*ppBackend);
    if (p == NULL) {
      stError("s-task:0x%x failed to ref backend", pTask->id.taskId);
      streamMutexUnlock(&pMeta->backendMutex);
      return TSDB_CODE_FAILED;
    }

    STaskDbWrapper* pBackend = *ppBackend;
    pBackend->pMeta = pMeta;
    if (recalated) {
      pTask->pRecalBackend = pBackend;
    } else {
      pTask->pBackend = pBackend;
    }

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
  if (recalated) {
    pTask->pRecalBackend = pBackend;
  } else {
    pTask->pBackend = pBackend;
  }
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

  if (recalated) {
    stDebug("s-task:0x%x set recalated backend %p", pTask->id.taskId, pBackend);
  } else {
    stDebug("s-task:0x%x set backend %p", pTask->id.taskId, pBackend);
  }
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

int32_t streamMetaUpdateInfoInit(STaskUpdateInfo* pInfo) {
  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);

  pInfo->pTasks = taosHashInit(64, fp, false, HASH_NO_LOCK);
  if (pInfo->pTasks == NULL) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

void streamMetaUpdateInfoCleanup(STaskUpdateInfo* pInfo) {
  taosHashCleanup(pInfo->pTasks);
  pInfo->pTasks = NULL;
}

int32_t streamMetaOpen(const char* path, void* ahandle, FTaskBuild buildTaskFn, FTaskExpand expandTaskFn, int32_t vgId,
                       int64_t stage, startComplete_fn_t fn, SStreamMeta** p) {
  QRY_PARAM_CHECK(p);
  int32_t code = 0;
  int32_t lino = 0;

  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    stError("vgId:%d failed to prepare stream meta, alloc size:%" PRIzu ", out of memory", vgId, sizeof(SStreamMeta));
    return terrno;
  }

  int32_t len = strlen(path) + 64;
  char*   tpath = taosMemoryCalloc(1, len);
  TSDB_CHECK_NULL(tpath, code, lino, _err, terrno);

  (void)snprintf(tpath, len, "%s%s%s", path, TD_DIRSEP, "stream");
  pMeta->path = tpath;

  code = streamMetaOpenTdb(pMeta);
  TSDB_CHECK_CODE(code, lino, _err);

  if ((code = streamMetaMayCvtDbFormat(pMeta)) < 0) {
    stError("vgId:%d convert sub info format failed, open stream meta failed, reason: %s", pMeta->vgId,
            tstrerror(terrno));
    TSDB_CHECK_CODE(code, lino, _err);
  }

  // set the attribute when running on Linux OS
  TdThreadRwlockAttr attr;
  code = taosThreadRwlockAttrInit(&attr);
  TSDB_CHECK_CODE(code, lino, _err);

#ifdef LINUX
  code = pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  TSDB_CHECK_CODE(code, lino, _err);
#endif

  code = taosThreadRwlockInit(&pMeta->lock, &attr);
  TSDB_CHECK_CODE(code, lino, _err);

  code = taosThreadRwlockAttrDestroy(&attr);
  TSDB_CHECK_CODE(code, lino, _err);

  if ((code = streamMetaBegin(pMeta) < 0)) {
    stError("vgId:%d begin trans for stream meta failed", pMeta->vgId);
    goto _err;
  }

  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);
  pMeta->pTasksMap = taosHashInit(64, fp, true, HASH_NO_LOCK);
  TSDB_CHECK_NULL(pMeta->pTasksMap, code, lino, _err, terrno);

  code = streamMetaUpdateInfoInit(&pMeta->updateInfo);
  TSDB_CHECK_CODE(code, lino, _err);

  code = streamMetaInitStartInfo(&pMeta->startInfo);
  TSDB_CHECK_CODE(code, lino, _err);

  // task list
  pMeta->pTaskList = taosArrayInit(4, sizeof(SStreamTaskId));
  TSDB_CHECK_NULL(pMeta->pTaskList, code, lino, _err, terrno);

  pMeta->scanInfo.scanSentinel = 0;
  pMeta->scanInfo.lastScanTs = 0;
  pMeta->scanInfo.tickCounter = 0;

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
  TSDB_CHECK_NULL(pMeta->pTaskDbUnique, code, lino, _err, terrno);

  pMeta->numOfPausedTasks = 0;
  pMeta->numOfStreamTasks = 0;
  pMeta->closeFlag = false;

  stInfo("vgId:%d open stream meta succ, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, pMeta->chkpId, stage);

  code = bkdMgtCreate(tpath, (SBkdMgt**)&pMeta->bkdChkptMgt);
  TSDB_CHECK_CODE(code, lino, _err);

  code = taosThreadMutexInit(&pMeta->backendMutex, NULL);
  TSDB_CHECK_CODE(code, lino, _err);

  // add refId at the end of initialization function
  pMeta->rid = taosAddRef(streamMetaRefPool, pMeta);

  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  TSDB_CHECK_NULL(pRid, code, lino, _err, terrno);

  memcpy(pRid, &pMeta->rid, sizeof(pMeta->rid));

  code = metaRefMgtAdd(pMeta->vgId, pRid);
  TSDB_CHECK_CODE(code, lino, _err);

  code = createMetaHbInfo(pRid, &pMeta->pHbInfo);

  TSDB_CHECK_CODE(code, lino, _err);

  *p = pMeta;
  return code;

_err:
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasksMap) taosHashCleanup(pMeta->pTasksMap);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) {
    tdbTbClose(pMeta->pTaskDb);
    pMeta->pTaskDb = NULL;
  }
  if (pMeta->pCheckpointDb) {
    tdbTbClose(pMeta->pCheckpointDb);
  }
  if (pMeta->db) {
    tdbClose(pMeta->db);
  }

  if (pMeta->pHbInfo) taosMemoryFreeClear(pMeta->pHbInfo);
  if (pMeta->updateInfo.pTasks) taosHashCleanup(pMeta->updateInfo.pTasks);
  if (pMeta->startInfo.pReadyTaskSet) taosHashCleanup(pMeta->startInfo.pReadyTaskSet);
  if (pMeta->startInfo.pFailedTaskSet) taosHashCleanup(pMeta->startInfo.pFailedTaskSet);
  if (pMeta->bkdChkptMgt) bkdMgtDestroy(pMeta->bkdChkptMgt);

  if (pMeta->startInfo.pStagesList) taosArrayDestroy(pMeta->startInfo.pStagesList);
  taosMemoryFree(pMeta);

  stError("vgId:%d failed to open stream meta, at line:%d reason:%s", vgId, lino, tstrerror(code));
  return code;
}

// todo refactor: the lock shoud be restricted in one function
#ifdef BUILD_NO_CALL
void streamMetaInitBackend(SStreamMeta* pMeta) {
  pMeta->streamBackend = streamBackendInit(pMeta->path, pMeta->chkpId, pMeta->vgId);
  if (pMeta->streamBackend == NULL) {
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
  int64_t st = taosGetTimestampMs();
  void*   pIter = NULL;

  while ((pIter = taosHashIterate(pMeta->pTasksMap, pIter)) != NULL) {
    int64_t      refId = *(int64_t*)pIter;
    SStreamTask* p = taosAcquireRef(streamTaskRefPool, refId);
    if (p == NULL) {
      continue;
    }

    // release the ref by timer
    if (p->info.delaySchedParam != 0 && p->info.fillHistory == 0) {  // one more ref in timer
      stDebug("s-task:%s stop schedTimer", p->id.idStr);
      streamTmrStop(p->schedInfo.pDelayTimer);
      p->info.delaySchedParam = 0;
    }

    int32_t code = taosRemoveRef(streamTaskRefPool, refId);
    if (code) {
      stError("vgId:%d remove task refId failed, refId:%" PRId64, pMeta->vgId, refId);
    }

    code = taosReleaseRef(streamTaskRefPool, refId);
    if (code) {
      stError("vgId:%d failed to release refId:%" PRId64, pMeta->vgId, refId);
    }
  }

  int64_t et = taosGetTimestampMs();
  stDebug("vgId:%d clear task map, elapsed time:%.2fs", pMeta->vgId, (et - st)/1000.0);

  if (pMeta->streamBackendRid != 0) {
    int32_t code = taosRemoveRef(streamBackendId, pMeta->streamBackendRid);
    if (code) {
      stError("vgId:%d remove stream backend Ref failed, rid:%" PRId64, pMeta->vgId, pMeta->streamBackendRid);
    }
  }

  int64_t et1 = taosGetTimestampMs();
  stDebug("vgId:%d clear backend completed, elapsed time:%.2fs", pMeta->vgId, (et1 - et)/1000.0);

  taosHashClear(pMeta->pTasksMap);

  taosArrayClear(pMeta->pTaskList);
  taosArrayClear(pMeta->chkpSaved);
  taosArrayClear(pMeta->chkpInUse);

  pMeta->numOfStreamTasks = 0;
  pMeta->numOfPausedTasks = 0;

  // NOTE: the willrestart/starting flag can NOT be cleared
  taosHashClear(pMeta->startInfo.pReadyTaskSet);
  taosHashClear(pMeta->startInfo.pFailedTaskSet);
  taosArrayClear(pMeta->startInfo.pStagesList);
  taosArrayClear(pMeta->startInfo.pRecvChkptIdTasks);

  pMeta->startInfo.readyTs = 0;
  pMeta->startInfo.elapsedTime = 0;
  pMeta->startInfo.startTs = 0;
}

void streamMetaClose(SStreamMeta* pMeta) {
  if (pMeta == NULL) {
    return;
  }

  stDebug("vgId:%d start to close stream meta", pMeta->vgId);
  int32_t code = taosRemoveRef(streamMetaRefPool, pMeta->rid);
  if (code) {
    stError("vgId:%d failed to remove meta ref:%" PRId64 ", code:%s", pMeta->vgId, pMeta->rid, tstrerror(code));
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
  tdbAbort(pMeta->db, pMeta->txn);
  tdbTbClose(pMeta->pTaskDb);
  tdbTbClose(pMeta->pCheckpointDb);
  tdbClose(pMeta->db);

  taosArrayDestroy(pMeta->pTaskList);
  taosArrayDestroy(pMeta->chkpSaved);
  taosArrayDestroy(pMeta->chkpInUse);

  taosHashCleanup(pMeta->pTasksMap);
  taosHashCleanup(pMeta->pTaskDbUnique);

  streamMetaUpdateInfoCleanup(&pMeta->updateInfo);
  streamMetaClearStartInfo(&pMeta->startInfo);

  destroyMetaHbInfo(pMeta->pHbInfo);
  pMeta->pHbInfo = NULL;

  taosMemoryFree(pMeta->path);
  streamMutexDestroy(&pMeta->backendMutex);

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
int32_t streamMetaSaveTaskInMeta(SStreamMeta* pMeta, SStreamTask* pTask) {
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
    stError("s-task:%s vgId:%d refId:%" PRId64 " task meta save to disk failed, remove ref, code:%s", pTask->id.idStr,
            vgId, pTask->id.refId, tstrerror(code));

    int64_t refId = pTask->id.refId;
    int32_t ret = taosRemoveRef(streamTaskRefPool, pTask->id.refId);
    if (ret != 0) {
      stError("s-task:0x%x failed to remove ref, refId:%" PRId64, (int32_t)id[1], refId);
    }
  } else {
    stDebug("s-task:%s vgId:%d refId:%" PRId64 " task meta save to disk", pTask->id.idStr, vgId, pTask->id.refId);
  }

  taosMemoryFree(buf);
  return code;
}

int32_t streamMetaRemoveTaskInMeta(SStreamMeta* pMeta, STaskId* pTaskId) {
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
  int64_t refId = 0;
  STaskId id = streamTaskGetTaskId(pTask);
  void*   p = taosHashGet(pMeta->pTasksMap, &id, sizeof(id));

  if (p != NULL) {
    stDebug("s-task:0x%" PRIx64 " already exist in meta, no need to register", id.taskId);
    tFreeStreamTask(pTask);
    return code;
  }

  if ((code = pMeta->buildTaskFn(pMeta->ahandle, pTask, ver)) != 0) {
    tFreeStreamTask(pTask);
    return code;
  }

  p = taosArrayPush(pMeta->pTaskList, &pTask->id);
  if (p == NULL) {
    stError("s-task:0x%" PRIx64 " failed to register task into meta-list, code: out of memory", id.taskId);
    tFreeStreamTask(pTask);
    return terrno;
  }

  pTask->id.refId = refId = taosAddRef(streamTaskRefPool, pTask);
  code = taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask->id.refId, sizeof(int64_t));
  if (code) {
    stError("s-task:0x%" PRIx64 " failed to register task into meta-list, code: out of memory", id.taskId);
    void* pUnused = taosArrayPop(pMeta->pTaskList);

    int32_t ret = taosRemoveRef(streamTaskRefPool, refId);
    if (ret != 0) {
      stError("s-task:0x%x failed to remove ref, refId:%" PRId64, (int32_t)id.taskId, refId);
    }
    return code;
  }

  if ((code = streamMetaSaveTaskInMeta(pMeta, pTask)) != 0) {
    int32_t unused = taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    void*   pUnused = taosArrayPop(pMeta->pTaskList);

    int32_t ret = taosRemoveRef(streamTaskRefPool, refId);
    if (ret) {
      stError("vgId:%d remove task refId failed, refId:%" PRId64, pMeta->vgId, refId);
    }
    return code;
  }

  if ((code = streamMetaCommit(pMeta)) != 0) {
    int32_t unused = taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    void*   pUnused = taosArrayPop(pMeta->pTaskList);

    int32_t ret = taosRemoveRef(streamTaskRefPool, refId);
    if (ret) {
      stError("vgId:%d remove task refId failed, refId:%" PRId64, pMeta->vgId, refId);
    }

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
  QRY_PARAM_CHECK(pTask);
  STaskId  id = {.streamId = streamId, .taskId = taskId};
  int64_t* pTaskRefId = (int64_t*)taosHashGet(pMeta->pTasksMap, &id, sizeof(id));
  if (pTaskRefId == NULL) {
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  SStreamTask* p = taosAcquireRef(streamTaskRefPool, *pTaskRefId);
  if (p == NULL) {
    stDebug("s-task:%x failed to acquire task refId:%" PRId64 ", may have been destoried", taskId, *pTaskRefId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  if (p->id.refId != *pTaskRefId) {
    stFatal("s-task:%x inconsistent refId, task refId:%" PRId64 " try acquire:%" PRId64, taskId, *pTaskRefId,
            p->id.refId);
    int32_t ret = taosReleaseRef(streamTaskRefPool, *pTaskRefId);
    if (ret) {
      stError("s-task:0x%x failed to release task refId:%" PRId64, taskId, *pTaskRefId);
    }

    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  if (streamTaskShouldStop(p)) {
    stDebug("s-task:%s is stopped, failed to acquire it now", p->id.idStr);
    int32_t ret = taosReleaseRef(streamTaskRefPool, *pTaskRefId);
    if (ret) {
      stError("s-task:0x%x failed to release task refId:%" PRId64, taskId, *pTaskRefId);
    }
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  stTrace("s-task:%s acquire task, refId:%" PRId64, p->id.idStr, p->id.refId);
  *pTask = p;
  return TSDB_CODE_SUCCESS;
}

int32_t streamMetaAcquireTaskUnsafe(SStreamMeta* pMeta, STaskId* pId, SStreamTask** pTask) {
  QRY_PARAM_CHECK(pTask);
  int64_t* pTaskRefId = (int64_t*)taosHashGet(pMeta->pTasksMap, pId, sizeof(*pId));

  if (pTaskRefId == NULL) {
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  SStreamTask* p = taosAcquireRef(streamTaskRefPool, *pTaskRefId);
  if (p == NULL) {
    stDebug("s-task:%" PRIx64 " failed to acquire task refId:%" PRId64 ", may have been destoried", pId->taskId,
            *pTaskRefId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  if (p->id.refId != *pTaskRefId) {
    stFatal("s-task:%" PRIx64 " inconsistent refId, task refId:%" PRId64 " try acquire:%" PRId64, pId->taskId,
            *pTaskRefId, p->id.refId);
    int32_t ret = taosReleaseRef(streamTaskRefPool, *pTaskRefId);
    if (ret) {
      stError("s-task:0x%" PRIx64 " failed to release task refId:%" PRId64, pId->taskId, *pTaskRefId);
    }

    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  stTrace("s-task:%s acquire task, refId:%" PRId64, p->id.idStr, p->id.refId);
  *pTask = p;
  return TSDB_CODE_SUCCESS;
}

int32_t streamMetaAcquireTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId, SStreamTask** pTask) {
  streamMetaRLock(pMeta);
  int32_t code = streamMetaAcquireTaskNoLock(pMeta, streamId, taskId, pTask);
  streamMetaRUnLock(pMeta);
  return code;
}

void streamMetaReleaseTask(SStreamMeta* UNUSED_PARAM(pMeta), SStreamTask* pTask) {
  if (pTask == NULL) {
    return;
  }

  int32_t taskId = pTask->id.taskId;
  int64_t refId = pTask->id.refId;
  stTrace("s-task:0x%x release task, refId:%" PRId64, taskId, pTask->id.refId);
  int32_t ret = taosReleaseRef(streamTaskRefPool, pTask->id.refId);
  if (ret) {
    stError("s-task:0x%x failed to release task refId:%" PRId64, taskId, refId);
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

  // let's kill the query procedure within stream, to end it ASAP.
  if (pTask->info.taskLevel != TASK_LEVEL__SINK && pTask->exec.pExecutor != NULL) {
    code = qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS, -1);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to kill task related query handle, code:%s", pTask->id.idStr, tstrerror(code));
    }
  }
  return code;
}

int32_t streamMetaUnregisterTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  SStreamTask* pTask = NULL;
  int32_t      vgId = pMeta->vgId;
  int32_t      code = 0;
  STaskId      id = {.streamId = streamId, .taskId = taskId};

  code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
  if (code == 0) {
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

    stDebug("s-task:0x%x vgId:%d set task status:dropping and start to unregister it", taskId, vgId);

    // it is a fill-history task, remove the related stream task's id that points to it
    if (pTask->info.fillHistory == 0) {
      int32_t ret = atomic_sub_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    code = taosHashRemove(pMeta->pTasksMap, &id, sizeof(id));
    doRemoveIdFromList(pMeta->pTaskList, (int32_t)taosArrayGetSize(pMeta->pTaskList), &pTask->id);
    code = streamMetaRemoveTaskInMeta(pMeta, &id);
    if (code) {
      stError("vgId:%d failed to remove task:0x%" PRIx64 ", code:%s", pMeta->vgId, id.taskId, tstrerror(code));
    }

    int32_t size = (int32_t)taosHashGetSize(pMeta->pTasksMap);
    int32_t sizeInList = taosArrayGetSize(pMeta->pTaskList);
    if (sizeInList != size) {
      stError("vgId:%d tasks number not consistent in list:%d and map:%d, ", vgId, sizeInList, size);
    }

    if (pTask->info.delaySchedParam != 0 && pTask->info.fillHistory == 0) {
      stDebug("s-task:%s stop schedTimer", pTask->id.idStr);
      streamTmrStop(pTask->schedInfo.pDelayTimer);
      pTask->info.delaySchedParam = 0;
    }

    int64_t refId = pTask->id.refId;
    int32_t ret = taosRemoveRef(streamTaskRefPool, refId);
    if (ret != 0) {
      stError("s-task:0x%x failed to remove ref, refId:%" PRId64, (int32_t)id.taskId, refId);
    }

    streamMetaReleaseTask(pMeta, pTask);
  } else {
    stDebug("vgId:%d failed to find the task:0x%x, it may have been dropped already", vgId, taskId);
  }

  return 0;
}

int32_t streamMetaStopOneTask(SStreamMeta* pMeta, int64_t streamId, int32_t taskId) {
  SStreamTask* pTask = NULL;
  int32_t      code = 0;
  int32_t      vgId = pMeta->vgId;
  int32_t      numOfTasks = 0;

  streamMetaWLock(pMeta);

//  code = streamMetaUnregisterTask(pMeta, streamId, taskId);
//  numOfTasks = streamMetaGetNumOfTasks(pMeta);
//  if (code) {
//    stError("vgId:%d failed to drop task:0x%x, code:%s", vgId, taskId, tstrerror(code));
//  }
//
//  code = streamMetaCommit(pMeta);
//  if (code) {
//    stError("vgId:%d failed to commit after drop task:0x%x, code:%s", vgId, taskId, tstrerror(code));
//  } else {
//    stDebug("s-task:0x%"PRIx64"-0x%x vgId:%d dropped, remain tasks:%d", streamId, taskId, pMeta->vgId, numOfTasks);
//  }

  streamMetaWUnLock(pMeta);

  return code;
}

int32_t streamMetaBegin(SStreamMeta* pMeta) {
  streamMetaWLock(pMeta);
  int32_t code = tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
                          TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (code) {
    streamSetFatalError(pMeta, code, __func__, __LINE__);
  }
  streamMetaWUnLock(pMeta);
  return code;
}

int32_t streamMetaCommit(SStreamMeta* pMeta) {
  int32_t code = tdbCommit(pMeta->db, pMeta->txn);
  if (code != 0) {
    streamSetFatalError(pMeta, code, __func__, __LINE__);
    stFatal("vgId:%d failed to commit stream meta, code:%s, line:%d", pMeta->vgId, tstrerror(code),
            pMeta->fatalInfo.line);
  }

  code = tdbPostCommit(pMeta->db, pMeta->txn);
  if (code != 0) {
    streamSetFatalError(pMeta, code, __func__, __LINE__);
    stFatal("vgId:%d failed to do post-commit stream meta, code:%s, line:%d", pMeta->vgId, tstrerror(code),
            pMeta->fatalInfo.line);
    return code;
  }

  code = tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
                  TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (code != 0) {
    streamSetFatalError(pMeta, code, __func__, __LINE__);
    stFatal("vgId:%d failed to begin trans, code:%s, line:%d", pMeta->vgId, tstrerror(code), pMeta->fatalInfo.line);
  } else {
    stDebug("vgId:%d stream meta file commit completed", pMeta->vgId);
  }

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
    tdbTbcClose(pCur);
    return checkpointId;
  }

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    if (pVal == NULL || vLen == 0) {
      break;
    }
    SCheckpointInfo info;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTaskChkInfo(&decoder, &info) < 0) {
      tDecoderClear(&decoder);
      continue;
    }
    tDecoderClear(&decoder);

    checkpointId = TMAX(checkpointId, info.checkpointId);
  }

  stDebug("vgId:%d get max checkpointId:%" PRId64, pMeta->vgId, checkpointId);

  tdbFree(pKey);
  tdbFree(pVal);

  tdbTbcClose(pCur);
  return checkpointId;
}

static void dropHistoryTaskIfNoStreamTask(SStreamMeta* pMeta, SArray*  pRecycleList) {
  int32_t i = 0;
  while (i < taosArrayGetSize(pMeta->pTaskList)) {
    SStreamTaskId* pTaskId = taosArrayGet(pMeta->pTaskList, i);
    if (pTaskId == NULL) {
      i++;
      continue;
    }
    SStreamTask* task = taosAcquireRef(streamTaskRefPool, pTaskId->refId);
    if (task != NULL && task->info.fillHistory == 1) {
      if (taosHashGet(pMeta->pTasksMap, &task->streamTaskId, sizeof(STaskId)) == NULL &&
        task->status.taskStatus != TASK_STATUS__DROPPING) {
        STaskId id = streamTaskGetTaskId(task);
        if (taosArrayPush(pRecycleList, &id) == NULL) {
          stError("%s s-task:0x%x failed to add into pRecycleList list due to:%d", __FUNCTION__, task->id.taskId, terrno);
        } else {
          int32_t total = taosArrayGetSize(pRecycleList);
          stInfo("%s s-task:0x%x is already dropped, add into recycle list, total:%d", __FUNCTION__, task->id.taskId, total);
        }
        int32_t code = taosHashRemove(pMeta->pTasksMap, &id, sizeof(STaskId));
        if (code == 0) {
          taosArrayRemove(pMeta->pTaskList, i);
        } else {
          i++;
          stError("%s s-task:0x%x failed to remove task from taskmap, code:%d", __FUNCTION__, task->id.taskId, code);
        }
        if (taosReleaseRef(streamTaskRefPool, pTaskId->refId) != 0) {
          stError("%s s-task:0x%x failed to release refId:%" PRId64, __FUNCTION__, task->id.taskId, pTaskId->refId);
        }
        continue;
      }
    }
    if (task != NULL && taosReleaseRef(streamTaskRefPool, pTaskId->refId) != 0) {
      stError("%s s-task:0x%x failed to release refId:%" PRId64, __FUNCTION__, task->id.taskId, pTaskId->refId);
    }
    i++;
  }
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

  vgId = pMeta->vgId;
  pRecycleList = taosArrayInit(4, sizeof(STaskId));
  if (pRecycleList == NULL) {
    stError("vgId:%d failed prepare load all tasks, code:out of memory", vgId);
    return;
  }

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
    tdbTbcClose(pCur);
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
      STaskId id = streamTaskGetTaskId(pTask);

      tFreeStreamTask(pTask);
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

    pTask->id.refId = taosAddRef(streamTaskRefPool, pTask);

    if (taosHashPut(pMeta->pTasksMap, &id, sizeof(id), &pTask->id.refId, sizeof(int64_t)) != 0) {
      int64_t refId = pTask->id.refId;
      stError("s-task:0x%x failed to put into hashTable, code:%s, remove task ref, refId:%" PRId64 " continue",
              pTask->id.taskId, tstrerror(terrno), refId);

      void*   px = taosArrayPop(pMeta->pTaskList);
      int32_t ret = taosRemoveRef(streamTaskRefPool, refId);
      if (ret != 0) {
        stError("s-task:0x%x failed to remove ref, refId:%" PRId64, (int32_t)id.taskId, refId);
      }
      continue;
    }

    stInfo("s-task:0x%x vgId:%d set refId:%" PRId64, (int32_t)id.taskId, vgId, pTask->id.refId);
    if (pTask->info.fillHistory == 0) {
      int32_t val = atomic_add_fetch_32(&pMeta->numOfStreamTasks, 1);
    }

    if (streamTaskShouldPause(pTask)) {
      int32_t val = atomic_add_fetch_32(&pMeta->numOfPausedTasks, 1);
    }
  }

  tdbFree(pKey);
  tdbFree(pVal);

  tdbTbcClose(pCur);

  dropHistoryTaskIfNoStreamTask(pMeta, pRecycleList);

  if (taosArrayGetSize(pRecycleList) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(pRecycleList); ++i) {
      STaskId* pId = taosArrayGet(pRecycleList, i);
      code = streamMetaRemoveTaskInMeta(pMeta, pId);
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

void streamMetaNotifyClose(SStreamMeta* pMeta) {
  int32_t vgId = pMeta->vgId;
  int64_t startTs = 0;
  int32_t sendCount = 0;
  int32_t numOfTasks = 0;

  streamMetaGetHbSendInfo(pMeta->pHbInfo, &startTs, &sendCount);
  stInfo("vgId:%d notify all stream tasks that current vnode is closing. isLeader:%d startHb:%" PRId64 ", totalHb:%d",
         vgId, (pMeta->role == NODE_ROLE_LEADER), startTs, sendCount);

  // wait for the stream meta hb function stopping
  pMeta->closeFlag = true;

  if (!tsDisableStream) { // stream is disabled, no need to wait for the timer out
    streamMetaWaitForHbTmrQuit(pMeta);
  }

  stDebug("vgId:%d start to check all tasks for closing", vgId);
  int64_t st = taosGetTimestampMs();

  streamMetaRLock(pMeta);

  SArray* pTaskList = NULL;
  int32_t code = streamMetaSendMsgBeforeCloseTasks(pMeta, &pTaskList);
  if (code != TSDB_CODE_SUCCESS) {
  }

  numOfTasks = taosArrayGetSize(pTaskList);
  for (int32_t i = 0; i < numOfTasks; ++i) {
    SStreamTaskId* pTaskId = taosArrayGet(pTaskList, i);
    SStreamTask*   pTask = NULL;

    code = streamMetaAcquireTaskNoLock(pMeta, pTaskId->streamId, pTaskId->taskId, &pTask);
    if (code != TSDB_CODE_SUCCESS) {
      continue;
    }

    int64_t refId = pTask->id.refId;
    int32_t ret = streamTaskStop(pTask);
    if (ret) {
      stError("s-task:0x%x failed to stop task, code:%s", pTaskId->taskId, tstrerror(ret));
    }

    streamMetaReleaseTask(pMeta, pTask);
    ret = taosRemoveRef(streamTaskRefPool, refId);
    if (ret) {
      stError("vgId:%d failed to remove task:0x%x, refId:%" PRId64, pMeta->vgId, pTaskId->taskId, refId);
    }
  }

  taosArrayDestroy(pTaskList);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  stDebug("vgId:%d stop all %d task(s) completed, elapsed time:%.2f Sec.", pMeta->vgId, numOfTasks, el);

  if (pMeta->scanInfo.scanTimer != NULL) {
    streamTmrStop(pMeta->scanInfo.scanTimer);
    pMeta->scanInfo.scanTimer = NULL;
  }

  streamMetaRUnLock(pMeta);
}

void streamMetaStartHb(SStreamMeta* pMeta) {
  int64_t* pRid = taosMemoryMalloc(sizeof(int64_t));
  if (pRid == NULL) {
    stFatal("vgId:%d failed to prepare the metaHb to mnode, hbMsg will not started, code: out of memory", pMeta->vgId);
    return;
  }

  *pRid = pMeta->rid;
  int32_t code = metaRefMgtAdd(pMeta->vgId, pRid);
  if (code) {
    return;
  }

  streamMetaHbToMnode(pRid, NULL);
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

void streamMetaUpdateStageRole(SStreamMeta* pMeta, int64_t term, bool isLeader) {
  streamMetaWLock(pMeta);

  int64_t prevTerm = pMeta->stage;
  int32_t prevRole = pMeta->role;

  pMeta->stage = term;
  pMeta->role = (isLeader) ? NODE_ROLE_LEADER : NODE_ROLE_FOLLOWER;

  // mark the sign to send msg before close all tasks
  // 1. for a leader vnode, always send msg before closing itself
  // 2. for a follower vnode, if it's changed from a leader, also sending msg before closing.
  if (prevRole == NODE_ROLE_LEADER) {
    pMeta->sendMsgBeforeClosing = true;
  }

  if ((prevRole == NODE_ROLE_FOLLOWER || prevRole == NODE_ROLE_LEADER) && (prevRole != pMeta->role) &&
      (taosArrayGetSize(pMeta->pTaskList) > 0)) {
    SStreamTask* pTask = NULL;
    STaskId*     pId = taosArrayGet(pMeta->pTaskList, 0);

    int32_t code = streamMetaAcquireTaskUnsafe(pMeta, pId, &pTask);
    if (code == 0) {
      stInfo("vgId:%d role changed, added into nodeUpdate list, use s-task:0x%s", pMeta->vgId, pTask->id.idStr);
      int32_t unused = streamTaskAddIntoNodeUpdateList(pTask, pMeta->vgId);
      streamMetaReleaseTask(pMeta, pTask);
    }
  }

  if (!isLeader) {
    streamMetaResetStartInfo(&pMeta->startInfo, pMeta->vgId);
  } else {  // wait for nodeep update if become leader from follower
    if (prevRole == NODE_ROLE_FOLLOWER) {
      pMeta->startInfo.tasksWillRestart = 1;
    }
  }

  streamMetaWUnLock(pMeta);

  if (!tsDisableStream) {
    if (isLeader) {
      if (prevRole == NODE_ROLE_FOLLOWER) {
        stInfo("vgId:%d update term:%" PRId64 ", prevTerm:%" PRId64
               " prevRole:%d leader:%d, start to send Hb, rid:%" PRId64 " restart after nodeEp being updated",
               pMeta->vgId, term, prevTerm, prevRole, isLeader, pMeta->rid);
      } else {
        stInfo("vgId:%d update term:%" PRId64 ", prevTerm:%" PRId64
               " prevRole:%d leader:%d, start to send Hb, rid:%" PRId64,
               pMeta->vgId, term, prevTerm, prevRole, isLeader, pMeta->rid);
      }
      streamMetaStartHb(pMeta);
    } else {
      stInfo("vgId:%d update term:%" PRId64 " prevTerm:%" PRId64 " prevRole:%d leader:%d sendMsg beforeClosing:%d",
             pMeta->vgId, term, prevTerm, prevRole, isLeader, pMeta->sendMsgBeforeClosing);
    }
  } else {
    stInfo("vgId:%d stream is disabled, not start the Hb", pMeta->vgId);
  }
}

bool streamMetaAllTasksReady(const SStreamMeta* pMeta) {
  int32_t num = taosArrayGetSize(pMeta->pTaskList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamTaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    STaskId        id = {.streamId = pId->streamId, .taskId = pId->taskId};
    SStreamTask*   pTask = NULL;
    int32_t        code = streamMetaAcquireTaskUnsafe((SStreamMeta*)pMeta, &id, &pTask);

    if (code == 0) {
      if (pTask->status.downstreamReady == 0) {
        streamMetaReleaseTask((SStreamMeta*)pMeta, pTask);
        return false;
      }
      streamMetaReleaseTask((SStreamMeta*)pMeta, pTask);
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
    STaskId        id = {.streamId = pTaskId->streamId, .taskId = pTaskId->taskId};
    SStreamTask*   pTask = NULL;
    int32_t        code = streamMetaAcquireTaskUnsafe(pMeta, &id, &pTask);
    if (code == 0) {
      streamTaskResetStatus(pTask);
      streamMetaReleaseTask(pMeta, pTask);
    }
  }

  return 0;
}

void streamMetaAddIntoUpdateTaskList(SStreamMeta* pMeta, SStreamTask* pTask, int32_t transId, int64_t startTs) {
  const char* id = pTask->id.idStr;
  int32_t     vgId = pMeta->vgId;
  int32_t     code = 0;

  // keep the already updated info
  STaskUpdateEntry entry = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId, .transId = transId};
  code = taosHashPut(pMeta->updateInfo.pTasks, &entry, sizeof(entry), NULL, 0);
  if (code != 0) {
    stError("s-task:%s failed to put updateTask into update list", id);
  }

  int64_t el = taosGetTimestampMs() - startTs;
  stDebug("s-task:%s vgId:%d transId:%d task nodeEp update completed, streamTask closed, elapsed time:%" PRId64 "ms",
          id, vgId, transId, el);
}

void streamMetaClearSetUpdateTaskListComplete(SStreamMeta* pMeta) {
  STaskUpdateInfo* pInfo = &pMeta->updateInfo;
  int32_t          num = taosHashGetSize(pInfo->pTasks);

  taosHashClear(pInfo->pTasks);

  int32_t prev = pInfo->completeTransId;
  pInfo->completeTransId = pInfo->activeTransId;
  pInfo->activeTransId = -1;
  pInfo->completeTs = taosGetTimestampMs();

  stInfo("vgId:%d set the nodeEp update complete, ts:%" PRId64
          ", complete transId:%d->%d, update Tasks:%d reset active transId",
          pMeta->vgId, pInfo->completeTs, prev, pInfo->completeTransId, num);
}

bool streamMetaInitUpdateTaskList(SStreamMeta* pMeta, int32_t transId, SArray* pUpdateTaskList) {
  STaskUpdateInfo* pInfo = &pMeta->updateInfo;
  int32_t          numOfTasks = taosArrayGetSize(pUpdateTaskList);

  if (transId > pInfo->completeTransId) {
    if (pInfo->activeTransId == -1) {
      taosHashClear(pInfo->pTasks);
      pInfo->activeTransId = transId;

      // interrupt the start all tasks procedure, only partial tasks will be started
      // the completion of this processed is based on the partial started tasks.
      if (pMeta->startInfo.startAllTasks == 1) {
        int32_t num = taosArrayGetSize(pMeta->startInfo.pRecvChkptIdTasks);
        pMeta->startInfo.partialTasksStarted = true;
        stInfo(
            "vgId:%d set the active epset update transId:%d for %d tasks, prev complete transId:%d, start all "
            "interrupted, only %d tasks were started",
            pMeta->vgId, transId, numOfTasks, pInfo->completeTransId, num);
      } else {
        stInfo("vgId:%d set the active epset update transId:%d for %d tasks, prev complete transId:%d", pMeta->vgId,
               transId, numOfTasks, pInfo->completeTransId);
      }
      return true;
    } else {
      if (pInfo->activeTransId == transId) {
        // do nothing
        return true;
      } else if (transId < pInfo->activeTransId) {
        stError("vgId:%d invalid(out of order) epset update transId:%d, active transId:%d, complete transId:%d, discard",
                pMeta->vgId, transId, pInfo->activeTransId, pInfo->completeTransId);
        return false;
      } else {  // transId > pInfo->activeTransId
        taosHashClear(pInfo->pTasks);
        int32_t prev = pInfo->activeTransId;
        pInfo->activeTransId = transId;

        stInfo(
            "vgId:%d active epset update transId updated from:%d to %d, prev complete transId:%d, reqUpdate tasks:%d",
            pMeta->vgId, prev, transId, pInfo->completeTransId, numOfTasks);
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
