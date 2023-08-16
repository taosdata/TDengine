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

#include <libs/sync/sync.h>
#include "executor.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tref.h"
#include "ttimer.h"

static TdThreadOnce streamMetaModuleInit = PTHREAD_ONCE_INIT;
int32_t             streamBackendId = 0;
int32_t             streamBackendCfWrapperId = 0;

int64_t streamGetLatestCheckpointId(SStreamMeta* pMeta);

static void metaHbToMnode(void* param, void* tmrId);

static void streamMetaEnvInit() {
  streamBackendId = taosOpenRef(64, streamBackendCleanup);
  streamBackendCfWrapperId = taosOpenRef(64, streamBackendHandleCleanup);
}

void streamMetaInit() { taosThreadOnce(&streamMetaModuleInit, streamMetaEnvInit); }
void streamMetaCleanup() {
  taosCloseRef(streamBackendId);
  taosCloseRef(streamBackendCfWrapperId);
}

SStreamMeta* streamMetaOpen(const char* path, void* ahandle, FTaskExpand expandFunc, int32_t vgId, int64_t stage) {
  int32_t      code = -1;
  SStreamMeta* pMeta = taosMemoryCalloc(1, sizeof(SStreamMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    qError("vgId:%d failed to prepare stream meta, alloc size:%" PRIzu ", out of memory", vgId, sizeof(SStreamMeta));
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

  _hash_fn_t fp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR);
  pMeta->pTasks = taosHashInit(64, fp, true, HASH_NO_LOCK);
  if (pMeta->pTasks == NULL) {
    goto _err;
  }

  // task list
  pMeta->pTaskList = taosArrayInit(4, sizeof(SStreamId));
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
  pMeta->stage = stage;

  // send heartbeat every 20sec.
  pMeta->hbTmr = taosTmrStart(metaHbToMnode, 20000, pMeta, streamEnv.timer);

  pMeta->pTaskBackendUnique =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  pMeta->checkpointSaved = taosArrayInit(4, sizeof(int64_t));
  pMeta->checkpointInUse = taosArrayInit(4, sizeof(int64_t));
  pMeta->checkpointCap = 8;
  taosInitRWLatch(&pMeta->checkpointDirLock);

  int64_t chkpId = streamGetLatestCheckpointId(pMeta);

  pMeta->streamBackend = streamBackendInit(pMeta->path, chkpId);
  if (pMeta->streamBackend == NULL) {
    goto _err;
  }
  pMeta->streamBackendRid = taosAddRef(streamBackendId, pMeta->streamBackend);

  code = streamBackendLoadCheckpointInfo(pMeta);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  taosMemoryFree(streamPath);

  taosInitRWLatch(&pMeta->lock);
  taosThreadMutexInit(&pMeta->backendMutex, NULL);

  qInfo("vgId:%d open stream meta successfully, latest checkpoint:%" PRId64 ", stage:%" PRId64, vgId, chkpId, stage);
  return pMeta;

_err:
  taosMemoryFree(streamPath);
  taosMemoryFree(pMeta->path);
  if (pMeta->pTasks) taosHashCleanup(pMeta->pTasks);
  if (pMeta->pTaskList) taosArrayDestroy(pMeta->pTaskList);
  if (pMeta->pTaskDb) tdbTbClose(pMeta->pTaskDb);
  if (pMeta->pCheckpointDb) tdbTbClose(pMeta->pCheckpointDb);
  if (pMeta->db) tdbClose(pMeta->db);
  taosMemoryFree(pMeta);

  qError("failed to open stream meta");
  return NULL;
}

void streamMetaClose(SStreamMeta* pMeta) {
  qDebug("start to close stream meta");
  if (pMeta == NULL) {
    return;
  }

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
    tFreeStreamTask(*(SStreamTask**)pIter);
  }

  taosHashCleanup(pMeta->pTasks);
  taosRemoveRef(streamBackendId, pMeta->streamBackendRid);
  pMeta->pTaskList = taosArrayDestroy(pMeta->pTaskList);
  taosMemoryFree(pMeta->path);
  taosThreadMutexDestroy(&pMeta->backendMutex);
  taosHashCleanup(pMeta->pTaskBackendUnique);

  taosArrayDestroy(pMeta->checkpointSaved);
  taosArrayDestroy(pMeta->checkpointInUse);

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

  if (tdbTbUpsert(pMeta->pTaskDb, &pTask->id.taskId, sizeof(int32_t), buf, len, pMeta->txn) < 0) {
    qError("s-task:%s save to disk failed, code:%s", pTask->id.idStr, tstrerror(terrno));
    return -1;
  }

  taosMemoryFree(buf);
  return 0;
}

int32_t streamMetaRemoveTask(SStreamMeta* pMeta, int32_t taskId) {
  int32_t code = tdbTbDelete(pMeta->pTaskDb, &taskId, sizeof(taskId), pMeta->txn);
  if (code != 0) {
    qError("vgId:%d failed to remove task:0x%x from metastore, code:%s", pMeta->vgId, taskId, tstrerror(terrno));
  } else {
    qDebug("vgId:%d remove task:0x%x from metastore", pMeta->vgId, taskId);
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

void streamMetaReleaseTask(SStreamMeta* pMeta, SStreamTask* pTask) {
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

static void doRemoveIdFromList(SStreamMeta* pMeta, int32_t num, SStreamId* id) {
  for (int32_t i = 0; i < num; ++i) {
    SStreamId* pTaskId = taosArrayGet(pMeta->pTaskList, i);
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

    streamMetaRemoveTask(pMeta, taskId);
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
    if (pVal != NULL && vLen != 0) {
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

  tdbFree(pKey);
  tdbFree(pVal);
  tdbTbcClose(pCur);

  return chkpId;
}

int32_t streamLoadTasks(SStreamMeta* pMeta) {
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
  SArray*  pRecycleList = taosArrayInit(4, sizeof(int32_t));

  tdbTbcMoveToFirst(pCur);
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
    if (pTask == NULL) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      taosArrayDestroy(pRecycleList);
      return -1;
    }
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    if (tDecodeStreamTask(&decoder, pTask) < 0) {
      tDecoderClear(&decoder);
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      taosArrayDestroy(pRecycleList);
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

      taosArrayPush(pRecycleList, &taskId);

      int32_t total = taosArrayGetSize(pRecycleList);
      qDebug("s-task:0x%x is already dropped, add into recycle list, total:%d", taskId, total);
      continue;
    }

    // do duplicate task check.
    int64_t keys[2] = {pTask->id.streamId, pTask->id.taskId};
    void*   p = taosHashGet(pMeta->pTasks, keys, sizeof(keys));
    if (p == NULL) {
      if (pMeta->expandFunc(pMeta->ahandle, pTask, pTask->chkInfo.checkpointVer) < 0) {
        tdbFree(pKey);
        tdbFree(pVal);
        tdbTbcClose(pCur);
        tFreeStreamTask(pTask);
        taosArrayDestroy(pRecycleList);
        return -1;
      }

      taosArrayPush(pMeta->pTaskList, &pTask->id);
    } else {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      taosMemoryFree(pTask);
      continue;
    }

    if (taosHashPut(pMeta->pTasks, keys, sizeof(keys), &pTask, sizeof(void*)) < 0) {
      tdbFree(pKey);
      tdbFree(pVal);
      tdbTbcClose(pCur);
      tFreeStreamTask(pTask);
      taosArrayDestroy(pRecycleList);
      return -1;
    }

    ASSERT(pTask->status.downstreamReady == 0);
  }

  tdbFree(pKey);
  tdbFree(pVal);
  if (tdbTbcClose(pCur) < 0) {
    taosArrayDestroy(pRecycleList);
    return -1;
  }

  if (taosArrayGetSize(pRecycleList) > 0) {
    for (int32_t i = 0; i < taosArrayGetSize(pRecycleList); ++i) {
      int32_t taskId = *(int32_t*)taosArrayGet(pRecycleList, i);
      streamMetaRemoveTask(pMeta, taskId);
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
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamHbMsg(SDecoder* pDecoder, SStreamHbMsg* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->vgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->numOfTasks) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

void metaHbToMnode(void* param, void* tmrId) {
#if 0
  SStreamMeta* pMeta = param;
  SStreamHbMsg hbMsg = {0};

  taosRLockLatch(&pMeta->lock);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pMeta);
  taosRUnLockLatch(&pMeta->lock);

  hbMsg.numOfTasks = numOfTasks;
  hbMsg.vgId = pMeta->vgId;

  int32_t code = 0;
  int32_t tlen = 0;

  tEncodeSize(tEncodeStreamHbMsg, &hbMsg, tlen, code);
  if (code < 0) {
    qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
    return;
  }

  void* buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return;
  }

  ((SMsgHead*)buf)->vgId = htonl(pMeta->mgmtInfo.mnodeId);
  void* pBuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, pBuf, tlen);
  if ((code = tEncodeStreamHbMsg(&encoder, &hbMsg)) < 0) {
    rpcFreeCont(buf);
    qError("vgId:%d encode stream hb msg failed, code:%s", pMeta->vgId, tstrerror(code));
    return;
  }
  tEncoderClear(&encoder);

  SRpcMsg msg = {0};
  initRpcMsg(&msg, TDMT_MND_STREAM_HEARTBEAT, buf, tlen + sizeof(SMsgHead));
  qDebug("vgId:%d, build and send hb to mnode", pMeta->mgmtInfo.mnodeId);

  tmsgSendReq(&pMeta->mgmtInfo.epset, &msg);

  // next hb will be issued in 20sec.
  taosTmrReset(metaHbToMnode, 20000, pMeta, streamEnv.timer, pMeta->hbTmr);
#endif
}