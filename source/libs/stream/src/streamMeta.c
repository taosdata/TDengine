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
#include "streamInc.h"
#include "ttimer.h"

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId) {
  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  int32_t len = strlen(path) + 20;
  char*   streamPath = taosMemoryCalloc(1, len);
  sprintf(streamPath, "%s/%s", path, "stream");
  pMeta->path = strdup(streamPath);
  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db, 0) < 0) {
    taosMemoryFree(streamPath);
    goto _err;
  }

  sprintf(streamPath, "%s/%s", pMeta->path, "checkpoints");
  taosMulModeMkDir(streamPath, 0755);
  taosMemoryFree(streamPath);

  if (tdbTbOpen("task.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pTaskDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("checkpoint.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pCheckpointDb, 0) < 0) {
    goto _err;
  }

  pMeta->pTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMeta->pTasks == NULL) {
    goto _err;
  }

  if (streamMetaBegin(pMeta) < 0) {
    goto _err;
  }

  pMeta->ahandle = ahandle;
  pMeta->expandFunc = expandFunc;

  return pMeta;

_err:
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasks) taosHashCleanup(pMeta->pTasks);
  if (pMeta->pTaskDb) tdbTbClose(pMeta->pTaskDb);
  if (pMeta->pCheckpointDb) tdbTbClose(pMeta->pCheckpointDb);
  if (pMeta->db) tdbClose(pMeta->db);
  taosMemoryFree(pMeta);
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
    if (pIter == NULL) break;
    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->timer) {
      taosTmrStop(pTask->timer);
      pTask->timer = NULL;
    }
    tFreeSStreamTask(pTask);
    /*streamMetaReleaseTask(pMeta, pTask);*/
  }
  taosHashCleanup(pMeta->pTasks);
  taosHashCleanup(pMeta->pRecoverStatus);
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
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    tDecoderClear(&decoder);
    goto FAIL;
  }
  tDecoderClear(&decoder);

  if (pMeta->expandFunc(pMeta->ahandle, pTask, ver) < 0) {
    ASSERT(0);
    goto FAIL;
  }

  if (taosHashPut(pMeta->pTasks, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void*)) < 0) {
    goto FAIL;
  }

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->taskId, sizeof(int32_t), msg, msgLen, pMeta->txn) < 0) {
    taosHashRemove(pMeta->pTasks, &pTask->taskId, sizeof(int32_t));
    ASSERT(0);
    goto FAIL;
  }

  return 0;

FAIL:
  if (pTask) tFreeSStreamTask(pTask);
  return -1;
}
#endif

#if 1
int32_t streamMetaAddTask(SStreamMeta* pMeta, int64_t ver, SStreamTask* pTask) {
  void* buf = NULL;
  if (pMeta->expandFunc(pMeta->ahandle, pTask, ver) < 0) {
    return -1;
  }

  int32_t len;
  int32_t code;
  tEncodeSize(tEncodeSStreamTask, pTask, len, code);
  if (code < 0) {
    return -1;
  }
  buf = taosMemoryCalloc(1, len);
  if (buf == NULL) {
    return -1;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  tEncodeSStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->taskId, sizeof(int32_t), buf, len, pMeta->txn) < 0) {
    ASSERT(0);
    return -1;
  }

  taosMemoryFree(buf);
  taosHashPut(pMeta->pTasks, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void*));

  return 0;
}
#endif

SStreamTask* streamMetaGetTask(SStreamMeta* pMeta, int32_t taskId) {
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask) {
    ASSERT((*ppTask)->taskId == taskId);
    return *ppTask;
  } else {
    return NULL;
  }
}

SStreamTask* streamMetaAcquireTask(SStreamMeta* pMeta, int32_t taskId) {
  taosRLockLatch(&pMeta->lock);

  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask) {
    SStreamTask* pTask = *ppTask;
    if (atomic_load_8(&pTask->taskStatus) != TASK_STATUS__DROPPING) {
      atomic_add_fetch_32(&pTask->refCnt, 1);
      taosRUnLockLatch(&pMeta->lock);
      return pTask;
    } else {
      taosRUnLockLatch(&pMeta->lock);
      return NULL;
    }
  }
  taosRUnLockLatch(&pMeta->lock);
  return NULL;
}

void streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  int32_t left = atomic_sub_fetch_32(&pTask->refCnt, 1);
  ASSERT(left >= 0);
  if (left == 0) {
    ASSERT(atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING);
    tFreeSStreamTask(pTask);
  }
}

void streamMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId) {
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask) {
    SStreamTask* pTask = *ppTask;
    taosHashRemove(pMeta->pTasks, &taskId, sizeof(int32_t));
    tdbTbDelete(pMeta->pTaskDb, &taskId, sizeof(int32_t), pMeta->txn);
    /*if (pTask->timer) {
     * taosTmrStop(pTask->timer);*/
    /*pTask->timer = NULL;*/
    /*}*/
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__DROPPING);

    taosWLockLatch(&pMeta->lock);
    streamMetaReleaseTask(pMeta, pTask);
    taosWUnLockLatch(&pMeta->lock);
  }
}

int32_t streamMetaBegin(SStreamMeta* pMeta) {
  if (tdbBegin(pMeta->db, &pMeta->txn, tdbDefaultMalloc, tdbDefaultFree, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamMetaCommit(SStreamMeta* pMeta) {
  if (tdbCommit(pMeta->db, pMeta->txn) < 0) {
    return -1;
  }
  if (tdbPostCommit(pMeta->db, pMeta->txn) < 0) {
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

int32_t streamLoadTasks(SStreamMeta* pMeta) {
  TBC* pCur = NULL;
  if (tdbTbcOpen(pMeta->pTaskDb, &pCur, NULL) < 0) {
    ASSERT(0);
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
    tDecodeSStreamTask(&decoder, pTask);
    tDecoderClear(&decoder);

    if (pMeta->expandFunc(pMeta->ahandle, pTask, -1) < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      return -1;
    }

    if (taosHashPut(pMeta->pTasks, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void*)) < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      return -1;
    }
    pTask->taskStatus = TASK_STATUS__NORMAL;
  }

  tdbFree(pKey);
  tdbFree(pVal);
  if (tdbTbcClose(pCur) < 0) {
    return -1;
  }

  return 0;
}
