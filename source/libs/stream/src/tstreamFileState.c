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

#include "tstreamFileState.h"

#include "query.h"
#include "streamBackendRocksdb.h"
#include "taos.h"
#include "tcommon.h"
#include "thash.h"
#include "tsimplehash.h"

#define FLUSH_RATIO                    0.5
#define FLUSH_NUM                      4
#define DEFAULT_MAX_STREAM_BUFFER_SIZE (128 * 1024 * 1024)
#define MIN_NUM_OF_ROW_BUFF            10240
#define MIN_NUM_OF_RECOVER_ROW_BUFF    128

#define TASK_KEY                       "streamFileState"
#define STREAM_STATE_INFO_NAME         "StreamStateCheckPoint"

struct SStreamFileState {
  SList*   usedBuffs;
  SList*   freeBuffs;
  void*    rowStateBuff;
  void*    pFileStore;
  int32_t  rowSize;
  int32_t  selectivityRowSize;
  int32_t  keyLen;
  uint64_t preCheckPointVersion;
  uint64_t checkPointVersion;
  TSKEY    maxTs;
  TSKEY    deleteMark;
  TSKEY    flushMark;
  uint64_t maxRowCount;
  uint64_t curRowCount;
  GetTsFun getTs;
  char*    id;
  char*    cfName;

  _state_buff_cleanup_fn         stateBuffCleanupFn;
  _state_buff_remove_fn          stateBuffRemoveFn;
  _state_buff_remove_by_pos_fn   stateBuffRemoveByPosFn;
  _state_buff_create_statekey_fn stateBuffCreateStateKeyFn;

  _state_file_remove_fn stateFileRemoveFn;
  _state_file_get_fn    stateFileGetFn;
  _state_file_clear_fn  stateFileClearFn;
};

typedef SRowBuffPos SRowBuffInfo;

int32_t stateHashBuffRemoveFn(void* pBuff, const void* pKey, size_t keyLen) {
  SRowBuffPos** pos = tSimpleHashGet(pBuff, pKey, keyLen);
  if (pos) {
    (*pos)->beFlushed = true;
  }
  return tSimpleHashRemove(pBuff, pKey, keyLen);
}

int32_t stateHashBuffRemoveByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  size_t        keyLen = pFileState->keyLen;
  SRowBuffPos** ppPos = tSimpleHashGet(pFileState->rowStateBuff, pPos->pKey, keyLen);
  if (ppPos) {
    if ((*ppPos) == pPos) {
      return tSimpleHashRemove(pFileState->rowStateBuff, pPos->pKey, keyLen);
    }
  }
  return TSDB_CODE_SUCCESS;
}

void stateHashBuffClearFn(void* pBuff) { tSimpleHashClear(pBuff); }

void stateHashBuffCleanupFn(void* pBuff) { tSimpleHashCleanup(pBuff); }

int32_t intervalFileRemoveFn(SStreamFileState* pFileState, const void* pKey) {
  return streamStateDel_rocksdb(pFileState->pFileStore, pKey);
}

int32_t intervalFileGetFn(SStreamFileState* pFileState, void* pKey, void* data, int32_t* pDataLen) {
  return streamStateGet_rocksdb(pFileState->pFileStore, pKey, data, pDataLen);
}

void* intervalCreateStateKey(SRowBuffPos* pPos, int64_t num) {
  SStateKey* pStateKey = taosMemoryCalloc(1, sizeof(SStateKey));
  SWinKey*   pWinKey = pPos->pKey;
  pStateKey->key = *pWinKey;
  pStateKey->opNum = num;
  return pStateKey;
}

int32_t sessionFileRemoveFn(SStreamFileState* pFileState, const void* pKey) {
  return streamStateSessionDel_rocksdb(pFileState->pFileStore, pKey);
}

int32_t sessionFileGetFn(SStreamFileState* pFileState, void* pKey, void* data, int32_t* pDataLen) {
  return streamStateSessionGet_rocksdb(pFileState->pFileStore, pKey, data, pDataLen);
}

void* sessionCreateStateKey(SRowBuffPos* pPos, int64_t num) {
  SStateSessionKey* pStateKey = taosMemoryCalloc(1, sizeof(SStateSessionKey));
  SSessionKey*      pWinKey = pPos->pKey;
  pStateKey->key = *pWinKey;
  pStateKey->opNum = num;
  return pStateKey;
}

static void streamFileStateDecode(TSKEY* pKey, void* pBuff, int32_t len) { pBuff = taosDecodeFixedI64(pBuff, pKey); }

static void streamFileStateEncode(TSKEY* pKey, void** pVal, int32_t* pLen) {
  *pLen = sizeof(TSKEY);
  (*pVal) = taosMemoryCalloc(1, *pLen);
  void* buff = *pVal;
  taosEncodeFixedI64(&buff, *pKey);
}

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize,
                                      GetTsFun fp, void* pFile, TSKEY delMark, const char* taskId, int64_t checkpointId,
                                      int8_t type) {
  if (memSize <= 0) {
    memSize = DEFAULT_MAX_STREAM_BUFFER_SIZE;
  }
  if (rowSize == 0) {
    goto _error;
  }

  SStreamFileState* pFileState = taosMemoryCalloc(1, sizeof(SStreamFileState));
  if (!pFileState) {
    goto _error;
  }
  rowSize += selectRowSize;
  pFileState->maxRowCount = TMAX((uint64_t)memSize / rowSize, FLUSH_NUM * 2);
  pFileState->usedBuffs = tdListNew(POINTER_BYTES);
  pFileState->freeBuffs = tdListNew(POINTER_BYTES);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  int32_t    cap = TMIN(MIN_NUM_OF_ROW_BUFF, pFileState->maxRowCount);
  if (type == STREAM_STATE_BUFF_HASH) {
    pFileState->rowStateBuff = tSimpleHashInit(cap, hashFn);
    pFileState->stateBuffCleanupFn = stateHashBuffCleanupFn;
    pFileState->stateBuffRemoveFn = stateHashBuffRemoveFn;
    pFileState->stateBuffRemoveByPosFn = stateHashBuffRemoveByPosFn;
    pFileState->stateBuffCreateStateKeyFn = intervalCreateStateKey;

    pFileState->stateFileRemoveFn = intervalFileRemoveFn;
    pFileState->stateFileGetFn = intervalFileGetFn;
    pFileState->stateFileClearFn = streamStateClear_rocksdb;
    pFileState->cfName = taosStrdup("state");
  } else {
    pFileState->rowStateBuff = tSimpleHashInit(cap, hashFn);
    pFileState->stateBuffCleanupFn = sessionWinStateCleanup;
    pFileState->stateBuffRemoveFn = deleteSessionWinStateBuffFn;
    pFileState->stateBuffRemoveByPosFn = deleteSessionWinStateBuffByPosFn;
    pFileState->stateBuffCreateStateKeyFn = sessionCreateStateKey;

    pFileState->stateFileRemoveFn = sessionFileRemoveFn;
    pFileState->stateFileGetFn = sessionFileGetFn;
    pFileState->stateFileClearFn = streamStateSessionClear_rocksdb;
    pFileState->cfName = taosStrdup("sess");
  }

  if (!pFileState->usedBuffs || !pFileState->freeBuffs || !pFileState->rowStateBuff) {
    goto _error;
  }

  pFileState->keyLen = keySize;
  pFileState->rowSize = rowSize;
  pFileState->selectivityRowSize = selectRowSize;
  pFileState->preCheckPointVersion = 0;
  pFileState->checkPointVersion = 1;
  pFileState->pFileStore = pFile;
  pFileState->getTs = fp;
  pFileState->curRowCount = 0;
  pFileState->deleteMark = delMark;
  pFileState->flushMark = INT64_MIN;
  pFileState->maxTs = INT64_MIN;
  pFileState->id = taosStrdup(taskId);

  // todo(liuyao) optimize
  if (type == STREAM_STATE_BUFF_HASH) {
    recoverSnapshot(pFileState, checkpointId);
  } else {
    recoverSesssion(pFileState, checkpointId);
  }

  void*   valBuf = NULL;
  int32_t len = 0;
  int32_t code = streamDefaultGet_rocksdb(pFileState->pFileStore, STREAM_STATE_INFO_NAME, &valBuf, &len);
  if (code == TSDB_CODE_SUCCESS) {
    ASSERT(len == sizeof(TSKEY));
    streamFileStateDecode(&pFileState->flushMark, valBuf, len);
    qDebug("===stream===flushMark  read:%" PRId64, pFileState->flushMark);
  }
  taosMemoryFreeClear(valBuf);
  return pFileState;

_error:
  streamFileStateDestroy(pFileState);
  return NULL;
}

void destroyRowBuffPos(SRowBuffPos* pPos) {
  taosMemoryFreeClear(pPos->pKey);
  taosMemoryFreeClear(pPos->pRowBuff);
  taosMemoryFree(pPos);
}

void destroyRowBuffPosPtr(void* ptr) {
  if (!ptr) {
    return;
  }
  SRowBuffPos* pPos = *(SRowBuffPos**)ptr;
  if (!pPos->beUsed) {
    destroyRowBuffPos(pPos);
  }
}

void destroyRowBuffAllPosPtr(void* ptr) {
  if (!ptr) {
    return;
  }
  SRowBuffPos* pPos = *(SRowBuffPos**)ptr;
  destroyRowBuffPos(pPos);
}

void destroyRowBuff(void* ptr) {
  if (!ptr) {
    return;
  }
  taosMemoryFree(*(void**)ptr);
}

void streamFileStateDestroy(SStreamFileState* pFileState) {
  if (!pFileState) {
    return;
  }

  taosMemoryFree(pFileState->id);
  taosMemoryFree(pFileState->cfName);
  tdListFreeP(pFileState->usedBuffs, destroyRowBuffAllPosPtr);
  tdListFreeP(pFileState->freeBuffs, destroyRowBuff);
  pFileState->stateBuffCleanupFn(pFileState->rowStateBuff);
  taosMemoryFree(pFileState);
}

void putFreeBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  if (pPos->pRowBuff) {
    tdListAppend(pFileState->freeBuffs, &(pPos->pRowBuff));
    pPos->pRowBuff = NULL;
  }
}

void clearExpiredRowBuff(SStreamFileState* pFileState, TSKEY ts, bool all) {
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)(pNode->data);
    if (all || (pFileState->getTs(pPos->pKey) < ts && !pPos->beUsed)) {
      putFreeBuff(pFileState, pPos);

      if (!all) {
        pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
      }
      destroyRowBuffPos(pPos);
      tdListPopNode(pFileState->usedBuffs, pNode);
      taosMemoryFreeClear(pNode);
    }
  }
}

void clearFlushedRowBuff(SStreamFileState* pFileState, SStreamSnapshot* pFlushList, uint64_t max) {
  uint64_t  i = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && i < max) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (isFlushedState(pFileState, pFileState->getTs(pPos->pKey), 0) && !pPos->beUsed) {
      tdListAppend(pFlushList, &pPos);
      pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));
      pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
      tdListPopNode(pFileState->usedBuffs, pNode);
      taosMemoryFreeClear(pNode);
      if (pPos->pRowBuff) {
        i++;
      }
    }
  }
}

void streamFileStateClear(SStreamFileState* pFileState) {
  pFileState->flushMark = INT64_MIN;
  pFileState->maxTs = INT64_MIN;
  tSimpleHashClear(pFileState->rowStateBuff);
  clearExpiredRowBuff(pFileState, 0, true);
}

bool needClearDiskBuff(SStreamFileState* pFileState) { return pFileState->flushMark > 0; }

void streamFileStateReleaseBuff(SStreamFileState* pFileState, SRowBuffPos* pPos, bool used) { pPos->beUsed = used; }

void popUsedBuffs(SStreamFileState* pFileState, SStreamSnapshot* pFlushList, uint64_t max, bool used) {
  uint64_t  i = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && i < max) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (pPos->beUsed == used) {
      if (used && !pPos->pRowBuff) {
        ASSERT(pPos->needFree == true);
        continue;
      }
      tdListAppend(pFlushList, &pPos);
      pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));
      pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
      tdListPopNode(pFileState->usedBuffs, pNode);
      taosMemoryFreeClear(pNode);
      if (pPos->pRowBuff) {
        i++;
      }
    }
  }

  qInfo("stream state flush %d rows to disk. is used:%d", listNEles(pFlushList), used);
}

int32_t flushRowBuff(SStreamFileState* pFileState) {
  SStreamSnapshot* pFlushList = tdListNew(POINTER_BYTES);
  if (!pFlushList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  uint64_t num = (uint64_t)(pFileState->curRowCount * FLUSH_RATIO);
  num = TMAX(num, FLUSH_NUM);
  clearFlushedRowBuff(pFileState, pFlushList, num);
  if (isListEmpty(pFlushList)) {
    popUsedBuffs(pFileState, pFlushList, num, false);

    if (isListEmpty(pFlushList)) {
      popUsedBuffs(pFileState, pFlushList, num, true);
    }
  }

  flushSnapshot(pFileState, pFlushList, false);

  SListIter fIter = {0};
  tdListInitIter(pFlushList, &fIter, TD_LIST_FORWARD);
  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&fIter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    putFreeBuff(pFileState, pPos);
  }

  tdListFreeP(pFlushList, destroyRowBuffPosPtr);
  return TSDB_CODE_SUCCESS;
}

int32_t clearRowBuff(SStreamFileState* pFileState) {
  clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark, false);
  if (isListEmpty(pFileState->freeBuffs)) {
    return flushRowBuff(pFileState);
  }
  return TSDB_CODE_SUCCESS;
}

void* getFreeBuff(SStreamFileState* pFileState) {
  SList*     lists = pFileState->freeBuffs;
  int32_t    buffSize = pFileState->rowSize;
  SListNode* pNode = tdListPopHead(lists);
  if (!pNode) {
    return NULL;
  }
  void* ptr = *(void**)pNode->data;
  memset(ptr, 0, buffSize);
  taosMemoryFree(pNode);
  return ptr;
}

int32_t streamFileStateClearBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  if (pPos->pRowBuff) {
    memset(pPos->pRowBuff, 0, pFileState->rowSize);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_FAILED;
}

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState) {
  SRowBuffPos* pPos = taosMemoryCalloc(1, sizeof(SRowBuffPos));
  pPos->pKey = taosMemoryCalloc(1, pFileState->keyLen);
  void* pBuff = getFreeBuff(pFileState);
  if (pBuff) {
    pPos->pRowBuff = pBuff;
    goto _end;
  }

  if (pFileState->curRowCount < pFileState->maxRowCount) {
    pBuff = taosMemoryCalloc(1, pFileState->rowSize);
    if (pBuff) {
      pPos->pRowBuff = pBuff;
      pFileState->curRowCount++;
      goto _end;
    }
  }

  int32_t code = clearRowBuff(pFileState);
  ASSERT(code == 0);
  pPos->pRowBuff = getFreeBuff(pFileState);

_end:
  tdListAppend(pFileState->usedBuffs, &pPos);
  ASSERT(pPos->pRowBuff != NULL);
  return pPos;
}

SRowBuffPos* getNewRowPosForWrite(SStreamFileState* pFileState) {
  SRowBuffPos* newPos = getNewRowPos(pFileState);
  newPos->beUsed = true;
  newPos->beFlushed = false;
  newPos->needFree = false;
  newPos->beUpdated = true;
  return newPos;
}

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen) {
  pFileState->maxTs = TMAX(pFileState->maxTs, pFileState->getTs(pKey));
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowStateBuff, pKey, keyLen);
  if (pos) {
    *pVLen = pFileState->rowSize;
    *pVal = *pos;
    (*pos)->beUsed = true;
    (*pos)->beFlushed = false;
    return TSDB_CODE_SUCCESS;
  }
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  ASSERT(pNewPos->pRowBuff);
  memcpy(pNewPos->pKey, pKey, keyLen);

  TSKEY ts = pFileState->getTs(pKey);
  if (!isDeteled(pFileState, ts) && isFlushedState(pFileState, ts, 0)) {
    int32_t len = 0;
    void*   p = NULL;
    int32_t code = streamStateGet_rocksdb(pFileState->pFileStore, pKey, &p, &len);
    qDebug("===stream===get %" PRId64 " from disc, res %d", ts, code);
    if (code == TSDB_CODE_SUCCESS) {
      memcpy(pNewPos->pRowBuff, p, len);
    }
    taosMemoryFree(p);
  }

  tSimpleHashPut(pFileState->rowStateBuff, pKey, keyLen, &pNewPos, POINTER_BYTES);
  if (pVal) {
    *pVLen = pFileState->rowSize;
    *pVal = pNewPos;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen) {
  int32_t code_buff = pFileState->stateBuffRemoveFn(pFileState->rowStateBuff, pKey, keyLen);
  int32_t code_file = pFileState->stateFileRemoveFn(pFileState, pKey);
  if (code_buff == TSDB_CODE_SUCCESS || code_file == TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_FAILED;
}

int32_t resetRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen) {
  int32_t code_buff = pFileState->stateBuffRemoveFn(pFileState->rowStateBuff, pKey, keyLen);
  int32_t code_file = pFileState->stateFileRemoveFn(pFileState, pKey);
  if (code_buff == TSDB_CODE_SUCCESS || code_file == TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_FAILED;
}

static void recoverSessionRowBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  int32_t len = 0;
  void*   pBuff = NULL;
  pFileState->stateFileGetFn(pFileState, pPos->pKey, &pBuff, &len);
  memcpy(pPos->pRowBuff, pBuff, len);
  taosMemoryFree(pBuff);
}

int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal) {
  if (pPos->pRowBuff) {
    if (pPos->needFree) {
      recoverSessionRowBuff(pFileState, pPos);
    }
    (*pVal) = pPos->pRowBuff;
    return TSDB_CODE_SUCCESS;
  }

  pPos->pRowBuff = getFreeBuff(pFileState);
  if (!pPos->pRowBuff) {
    if (pFileState->curRowCount < pFileState->maxRowCount) {
      pPos->pRowBuff = taosMemoryCalloc(1, pFileState->rowSize);
      pFileState->curRowCount++;
    } else {
      int32_t code = clearRowBuff(pFileState);
      ASSERT(code == 0);
      pPos->pRowBuff = getFreeBuff(pFileState);
    }
    ASSERT(pPos->pRowBuff);
  }

  recoverSessionRowBuff(pFileState, pPos);
  (*pVal) = pPos->pRowBuff;
  if (!pPos->needFree) {
    tdListPrepend(pFileState->usedBuffs, &pPos);
  }
  return TSDB_CODE_SUCCESS;
}

bool hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen) {
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowStateBuff, pKey, keyLen);
  if (pos) {
    return true;
  }
  return false;
}

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState) {
  int64_t mark = (pFileState->deleteMark == INT64_MAX) ? INT64_MIN : pFileState->maxTs - pFileState->deleteMark;
  clearExpiredRowBuff(pFileState, mark, false);
  return pFileState->usedBuffs;
}

int32_t flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState) {
  int32_t   code = TSDB_CODE_SUCCESS;
  SListIter iter = {0};
  tdListInitIter(pSnapshot, &iter, TD_LIST_FORWARD);

  const int32_t BATCH_LIMIT = 256;

  int64_t    st = taosGetTimestampMs();
  int32_t    numOfElems = listNEles(pSnapshot);
  SListNode* pNode = NULL;

  int idx = streamStateGetCfIdx(pFileState->pFileStore, pFileState->cfName);

  int32_t len = pFileState->rowSize + sizeof(uint64_t) + sizeof(int32_t) + 1;
  char*   buf = taosMemoryCalloc(1, len);

  void* batch = streamStateCreateBatch();
  while ((pNode = tdListNext(&iter)) != NULL && code == TSDB_CODE_SUCCESS) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (pPos->beFlushed || !pPos->pRowBuff) {
      continue;
    }
    pPos->beFlushed = true;
    pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));

    qDebug("===stream===flushed start:%" PRId64, pFileState->getTs(pPos->pKey));
    if (streamStateGetBatchSize(batch) >= BATCH_LIMIT) {
      streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
      streamStateClearBatch(batch);
    }

    void* pSKey = pFileState->stateBuffCreateStateKeyFn(pPos, ((SStreamState*)pFileState->pFileStore)->number);
    code = streamStatePutBatchOptimize(pFileState->pFileStore, idx, batch, pSKey, pPos->pRowBuff, pFileState->rowSize,
                                       0, buf);
    taosMemoryFreeClear(pSKey);
    // todo handle failure
    memset(buf, 0, len);
  }
  taosMemoryFree(buf);

  if (streamStateGetBatchSize(batch) > 0) {
    streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
  }

  streamStateClearBatch(batch);

  int64_t elapsed = taosGetTimestampMs() - st;
  qDebug("%s flush to disk in batch model completed, rows:%d, batch size:%d, elapsed time:%" PRId64 "ms",
         pFileState->id, numOfElems, BATCH_LIMIT, elapsed);

  if (flushState) {
    void*   valBuf = NULL;
    int32_t len = 0;
    streamFileStateEncode(&pFileState->flushMark, &valBuf, &len);
    qDebug("===stream===flushMark write:%" PRId64, pFileState->flushMark);
    streamStatePutBatch(pFileState->pFileStore, "default", batch, STREAM_STATE_INFO_NAME, valBuf, len, 0);
    taosMemoryFree(valBuf);
    streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
  }

  streamStateDestroyBatch(batch);
  return code;
}

int32_t forceRemoveCheckpoint(SStreamFileState* pFileState, int64_t checkpointId) {
  char        keyBuf[128] = {0};
  sprintf(keyBuf, "%s:%" PRId64 "", TASK_KEY, checkpointId);
  return streamDefaultDel_rocksdb(pFileState->pFileStore, keyBuf);
}

int32_t getSnapshotIdList(SStreamFileState* pFileState, SArray* list) {
  return streamDefaultIterGet_rocksdb(pFileState->pFileStore, TASK_KEY, NULL, list);
}

int32_t deleteExpiredCheckPoint(SStreamFileState* pFileState, TSKEY mark) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int64_t     maxCheckPointId = 0;
  {
    char    buf[128] = {0};
    void*   val = NULL;
    int32_t len = 0;
    memcpy(buf, TASK_KEY, strlen(TASK_KEY));
    code = streamDefaultGet_rocksdb(pFileState->pFileStore, buf, &val, &len);
    if (code != 0 || len == 0 || val == NULL) {
      return TSDB_CODE_FAILED;
    }
    memcpy(buf, val, len);
    buf[len] = 0;
    maxCheckPointId = atol((char*)buf);
    taosMemoryFree(val);
  }
  for (int64_t i = maxCheckPointId; i > 0; i--) {
    char    buf[128] = {0};
    void*   val = 0;
    int32_t len = 0;
    sprintf(buf, "%s:%" PRId64 "", TASK_KEY, i);
    code = streamDefaultGet_rocksdb(pFileState->pFileStore, buf, &val, &len);
    if (code != 0) {
      return TSDB_CODE_FAILED;
    }
    memcpy(buf, val, len);
    buf[len] = 0;
    taosMemoryFree(val);

    TSKEY ts;
    ts = atol((char*)buf);
    if (ts < mark) {
      // statekey winkey.ts < mark
      forceRemoveCheckpoint(pFileState, i);
      break;
    }
  }
  return code;
}

int32_t recoverSesssion(SStreamFileState* pFileState, int64_t ckId) {
  int code = TSDB_CODE_SUCCESS;
  if (pFileState->maxTs != INT64_MIN) {
    int64_t mark = (INT64_MIN + pFileState->deleteMark >= pFileState->maxTs)
                       ? INT64_MIN
                       : pFileState->maxTs - pFileState->deleteMark;
    deleteExpiredCheckPoint(pFileState, mark);
  }

  SStreamStateCur* pCur = streamStateSessionSeekToLast_rocksdb(pFileState->pFileStore, INT64_MAX);
  if (pCur == NULL) {
    return -1;
  }
  int32_t recoverNum = TMIN(MIN_NUM_OF_RECOVER_ROW_BUFF, pFileState->maxRowCount);
  while (code == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount >= recoverNum) {
      break;
    }

    void*       pVal = NULL;
    int32_t     vlen = 0;
    SSessionKey key = {0};
    code = streamStateSessionGetKVByCur_rocksdb(pCur, &key, &pVal, &vlen);
    if (code != 0) {
      break;
    }
    SRowBuffPos* pPos = createSessionWinBuff(pFileState, &key, pVal, &vlen);
    putSessionWinResultBuff(pFileState, pPos);
    code = streamStateSessionCurPrev_rocksdb(pCur);
  }
  streamStateFreeCur(pCur);
  return code;
}

int32_t recoverSnapshot(SStreamFileState* pFileState, int64_t ckId) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pFileState->maxTs != INT64_MIN) {
    int64_t mark = (INT64_MIN + pFileState->deleteMark >= pFileState->maxTs)
                       ? INT64_MIN
                       : pFileState->maxTs - pFileState->deleteMark;
    deleteExpiredCheckPoint(pFileState, mark);
  }

  SStreamStateCur* pCur = streamStateSeekToLast_rocksdb(pFileState->pFileStore);
  if (pCur == NULL) {
    return -1;
  }
  int32_t recoverNum = TMIN(MIN_NUM_OF_RECOVER_ROW_BUFF, pFileState->maxRowCount);
  while (code == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount >= recoverNum) {
      break;
    }

    void*        pVal = NULL;
    int32_t      vlen = 0;
    SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
    code = streamStateGetKVByCur_rocksdb(pCur, pNewPos->pKey, (const void**)&pVal, &vlen);
    if (code != TSDB_CODE_SUCCESS || pFileState->getTs(pNewPos->pKey) < pFileState->flushMark) {
      destroyRowBuffPos(pNewPos);
      SListNode* pNode = tdListPopTail(pFileState->usedBuffs);
      taosMemoryFreeClear(pNode);
      taosMemoryFreeClear(pVal);
      break;
    }
    ASSERT(vlen == pFileState->rowSize);
    memcpy(pNewPos->pRowBuff, pVal, vlen);
    taosMemoryFreeClear(pVal);
    pNewPos->beFlushed = true;
    code = tSimpleHashPut(pFileState->rowStateBuff, pNewPos->pKey, pFileState->keyLen, &pNewPos, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      destroyRowBuffPos(pNewPos);
      break;
    }
    code = streamStateCurPrev_rocksdb(pCur);
  }
  streamStateFreeCur(pCur);

  return TSDB_CODE_SUCCESS;
}

int32_t streamFileStateGeSelectRowSize(SStreamFileState* pFileState) { return pFileState->selectivityRowSize; }

void streamFileStateReloadInfo(SStreamFileState* pFileState, TSKEY ts) {
  pFileState->flushMark = TMAX(pFileState->flushMark, ts);
  pFileState->maxTs = TMAX(pFileState->maxTs, ts);
}

void* getRowStateBuff(SStreamFileState* pFileState) { return pFileState->rowStateBuff; }

void* getStateFileStore(SStreamFileState* pFileState) { return pFileState->pFileStore; }

bool isDeteled(SStreamFileState* pFileState, TSKEY ts) {
  return pFileState->deleteMark > 0 && ts < (pFileState->maxTs - pFileState->deleteMark);
}

bool isFlushedState(SStreamFileState* pFileState, TSKEY ts, TSKEY gap) { return ts <= (pFileState->flushMark + gap); }

int32_t getRowStateRowSize(SStreamFileState* pFileState) { return pFileState->rowSize; }
