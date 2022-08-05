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
#include "tdbInt.h"
#include "tstream.h"

typedef int32_t FTaskExpand(void* ahandle, SStreamTask* pTask);

typedef struct SStreamMeta {
  char*        path;
  TDB*         db;
  TTB*         pTaskDb;
  TTB*         pStateDb;
  SHashObj*    pTasks;
  void*        ahandle;
  TXN          txn;
  FTaskExpand* expandFunc;
} SStreamMeta;

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc) {
  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pMeta->path = strdup(path);
  if (tdbOpen(pMeta->path, 16 * 1024, 1, &pMeta->db) < 0) {
    goto _err;
  }

  if (tdbTbOpen("task.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pTaskDb) < 0) {
    goto _err;
  }

  // open state storage backend
  if (tdbTbOpen("state.db", sizeof(int32_t), -1, NULL, pMeta->db, &pMeta->pStateDb) < 0) {
    goto _err;
  }

  pMeta->ahandle = ahandle;
  pMeta->expandFunc = expandFunc;

_err:

  return NULL;
}

void streamMetaClose(SStreamMeta* pMeta) {
  //
  return;
}

int32_t streamMetaAddTask(SStreamMeta* pMeta, SStreamTask* pTask) {
  void* buf = NULL;
  if (pMeta->expandFunc(pMeta->ahandle, pTask) < 0) {
    return -1;
  }
  taosHashPut(pMeta->pTasks, &pTask->taskId, sizeof(int32_t), &pTask, sizeof(void*));

  int32_t len;
  int32_t code;
  tEncodeSize(tEncodeSStreamTask, pTask, len, code);
  if (code < 0) {
    return -1;
  }
  buf = taosMemoryCalloc(1, sizeof(len));
  if (buf == NULL) {
    return -1;
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, len);
  tEncodeSStreamTask(&encoder, pTask);

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->taskId, sizeof(int32_t), buf, len, &pMeta->txn) < 0) {
    ASSERT(0);
    return -1;
  }

  return 0;
}

int32_t streamMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId) {
  SStreamTask** ppTask = (SStreamTask**)taosHashGet(pMeta->pTasks, &taskId, sizeof(int32_t));
  if (ppTask) {
    SStreamTask* pTask = *ppTask;
    taosHashRemove(pMeta->pTasks, &taskId, sizeof(int32_t));
    atomic_store_8(&pTask->taskStatus, TASK_STATUS__DROPPING);
  }

  if (tdbTbDelete(pMeta->pTaskDb, &taskId, sizeof(int32_t), &pMeta->txn) < 0) {
    /*return -1;*/
  }
  return 0;
}

int32_t streamMetaBegin(SStreamMeta* pMeta) {
  if (tdbTxnOpen(&pMeta->txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) <
      0) {
    return -1;
  }

  if (tdbBegin(pMeta->db, &pMeta->txn) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamMetaCommit(SStreamMeta* pMeta) {
  if (tdbCommit(pMeta->db, &pMeta->txn) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamMetaRollBack(SStreamMeta* pMeta) {
  // TODO tdb rollback
  return 0;
}
int32_t streamRestoreTask(SStreamMeta* pMeta) {
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
      return -1;
    }
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeSStreamTask(&decoder, pTask);
    tDecoderClear(&decoder);
  }

  return 0;
}
