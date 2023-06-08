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
#include "streamInc.h"
#include "tref.h"
#include "ttimer.h"

static TdThreadOnce streamMetaModuleInit = PTHREAD_ONCE_INIT;
static int32_t      streamBackendId = 0;
static void         streamMetaEnvInit() { streamBackendId = taosOpenRef(20, streamBackendCleanup); }

void streamMetaInit() { taosThreadOnce(&streamMetaModuleInit, streamMetaEnvInit); }
void streamMetaCleanup() { taosCloseRef(streamBackendId); }

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId) {
  int32_t      code = -1;
  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t len = strlen(path) + 20;
  char*   streamPath = taosMemoryCalloc(1, len);
  sprintf(streamPath, "%s/%s", path, "stream");
  pMeta->path = taosStrdup(streamPath);
  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db, 0) < 0) {
    goto _err;
  }
  memset(streamPath, 0, len);

  sprintf(streamPath, "%s/%s", pMeta->path, "checkpoints");
  code = taosMulModeMkDir(streamPath, 0755);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  if (tdbTbOpen("task.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pTaskDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("checkpoint.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pCheckpointDb, 0) < 0) {
    goto _err;
  }

  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  pMeta->pTasks = taosHashInit(64, fp, true, HASH_NO_LOCK);
  if (pMeta->pTasks == NULL) {
    goto _err;
  }

  // task list
  pMeta->pTaskList = taosArrayInit(4, sizeof(int32_t));
  if (pMeta->pTaskList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (streamMetaBegin(pMeta) < 0) {
    goto _err;
  }

  pMeta->walScanCounter = 0;
  pMeta->vgId = vgId;
  pMeta->ahandle = ahandle;
  pMeta->expandFunc = expandFunc;
  pMeta->streamBackendId = streamBackendId;

  memset(streamPath, 0, len);
  sprintf(streamPath, "%s/%s", pMeta->path, "state");
  code = taosMulModeMkDir(streamPath, 0755);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pMeta->streamBackend = streamBackendInit(streamPath);
  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);

  taosMemoryFree(streamPath);

  taosInitRWLatch(&pMeta->lock);
  return pMeta;

_err:
  taosMemoryFree(streamPath);
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasks) taosHashCleanup(pMeta->pTasks);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) tdbTbClose(pMeta->pTaskDb);
  if (pMeta->pCheckpointDb) tdbTbClose(pMeta->pCheckpointDb);
  if (pMeta->db) tdbClose(pMeta->db);
  // if (pMeta->streamBackend) streamBackendCleanup(pMeta->streamBackend);
  taosMemoryFree(pMeta);
  qError("failed to open stream meta");
  return NULL;
}

void streamMetaClose(SStreamMeta* pMeta) {
  tdbAbort(pMeta->db, pMeta->txn);
  tdbTbClose(pMeta->pTaskDb);
  tdbTbClose(pMeta->pCheckpointDb);
  tdbClose(pMeta->db);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->timer) {
      taosTmrStop(pTask->timer);
      pTask->timer = NULL;
    }

    tFreeStreamTask(pTask);
  }

  taosHashCleanup(pMeta->pTasks);
  taosRemoveRef(streamBackendId, pMeta->streamBackendRid);
  pMeta->pTaskList = taosArrayDestroy(pMeta->pTaskList);
  taosMemoryFree(pMeta->path);
  taosMemoryFree(pMeta);
}

#if 0
int32_t streamMetaAddSerializedTask(SStreamMeta* pMeta, int64_t ver, char* msg, int32_t msgLen) {
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeStreamTask(&decoder, pTask) < 0) {
    tDecoderClear(&decoder);
    goto FAIL;
  }
  tDecoderClear(&decoder);

  if (pMeta->expandFunc(pMeta->ahandle, pTask, ver) < 0) {
    ASSERT(0);
    goto FAIL;
  }

  if (taosHashPut(pMeta->pTasks, &pTask->id.taskId, sizeof(int32_t), &pTask, sizeof(void*)) < 0) {
    goto FAIL;
  }

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->id.taskId, sizeof(int32_t), msg, msgLen, pMeta->txn) < 0) {
    taosHashRemove(pMeta->pTasks, &pTask->id.taskId, sizeof(int32_t));
    ASSERT(0);
    goto FAIL;
  }

  return 0;

FAIL:
  if (pTask) tFreeStreamTask(pTask);
  return -1;
}
#endif

int32_t streamMetaSaveTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  void*   buf = NULL;
  int32_t len;
  int32_t code;
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

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->id.taskId, sizeof(int32_t), buf, len, pMeta->txn) < 0) {
    return -1;
  }

  taosMemoryFree(buf);
  return 0;
}

// add to the ready tasks hash map, not the restored tasks hash map
int32_t streamMetaAddDeployedTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask) {
  void* p = taosHashGet(pMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId));
  if (p == NULL) {
    if (pMeta->expandFunc(pMeta->ahandle, pTask, ver) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }

    if (streamMetaSaveTask(pMeta, pTask) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }

    if (streamMetaCommit(pMeta) < 0) {
      tFreeStreamTask(pTask);
      return -1;
    }
    taosArrayPush(pMeta->pTaskList, &pTask->id.taskId);
  } else {
    return 0;
  }

  taosHashPut(pMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId), &pTask, POINTER_BYTES);
  return 0;
}

int32_t streamMetaGetNumOfTasks(const SStreamMeta* pMeta) {
  size_t size = taosHashGetSize(pMeta->pTasks);
  ASSERT(taosArrayGetSize(pMeta->pTaskList) == taosHashGetSize(pMeta->pTasks));

  return (int32_t)size;
}

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int32_t taskId) {
  taosRLockLatch(&pMeta->lock);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask != NULL) {
    if (!streamTaskShouldStop(&(*ppTask)->status)) {
      atomic_add_fetch_32(&(*ppTask)->refCnt, 1);
      taosRUnLockLatch(&pMeta->lock);
      return *ppTask;
    }
  }

  taosRUnLockLatch(&pMeta->lock);
  return NULL;
}

void streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  int32_t left = atomic_sub_fetch_32(&pTask->refCnt, 1);
  if (left < 0) {
    qError("task ref is invalid, ref:%d, %s", left, pTask->id.idStr);
  } else if (left == 0) {
    ASSERT(streamTaskShouldStop(&pTask->status));
    tFreeStreamTask(pTask);
  }
}

void streamMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId) {
  taosWLockLatch(&pMeta->lock);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask) {
    SStreamTask* pTask = *ppTask;

    taosHashRemove(pMeta->pTasks, &taskId, sizeof(int32_t));
    tdbTbDelete(pMeta->pTaskDb, &taskId, sizeof(int32_t), pMeta->txn);

    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__DROPPING);
    int32_t num = taosArrayGetSize(pMeta->pTaskList);

    qDebug("s-task:%s set the drop task flag, remain running s-task:%d", pTask->id.idStr, num - 1);
    for (int32_t i = 0; i < num; ++i) {
      int32_t* pTaskId = taosArrayGet(pMeta->pTaskList, i);
      if (*pTaskId == taskId) {
        taosArrayRemove(pMeta->pTaskList, i);
        break;
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
  } else {
    qDebug("vgId:%d failed to find the task:0x%x, it may be dropped already", pMeta->vgId, taskId);
  }

  taosWUnLockLatch(&pMeta->lock);
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
    qError("failed to commit stream meta");
    return -1;
  }

  if (tdbPostCommit(pMeta->db, pMeta->txn) < 0) {
    qError("failed to commit stream meta");
    return -1;
  }

  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }

  return 0;
}

int32_t streamMetaAbort(SStreamMeta* pMeta) {
  if (tdbAbort(pMeta->db, pMeta->txn) < 0) {
    return -1;
  }

  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamLoadTasks(SStreamMeta* pMeta, int64_t ver) {
  TBC* pCur = NULL;
  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    return -1;
  }

  void*    pKey = NULL;
  int32_t  kLen = 0;
  void*    pVal = NULL;
  int32_t  vLen = 0;
  SDecoder decoder;

  tdbTbcMoveToFirst(pCur);

  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      return -1;
    }

    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeStreamTask(&decoder, pTask);
    tDecoderClear(&decoder);

    // remove duplicate
    void* p = taosHashGet(pMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId));
    if (p == NULL) {
      if (pMeta->expandFunc(pMeta->ahandle, pTask, pTask->chkInfo.version) < 0) {
        tdbFree(pKey);
        tdbFree(pVal);
        tdbTbcClose(pCur);
        taosMemoryFree(pTask);
        return -1;
      }
      taosArrayPush(pMeta->pTaskList, &pTask->id.taskId);
    } else {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      taosMemoryFree(pTask);
      continue;
    }
    if (taosHashPut(pMeta->pTasks, &pTask->id.taskId, sizeof(pTask->id.taskId), &pTask, sizeof(void*)) < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      taosMemoryFree(pTask);
      return -1;
    }

    if (pTask->fillHistory) {
      ASSERT(pTask->status.taskStatus == TASK_STATUS__WAIT_DOWNSTREAM);
      streamTaskCheckDownstream(pTask, ver);
    }
  }

  tdbFree(pKey);
  tdbFree(pVal);
  if (tdbTbcClose(pCur) < 0) {
    return -1;
  }

  return 0;
}
