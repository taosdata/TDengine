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
#define MIN_NUM_SEARCH_BUCKET          128
#define MAX_ARRAY_SIZE                 1024
#define MAX_GROUP_ID_NUM               200000
#define NUM_OF_CACHE_WIN               64
#define MAX_NUM_OF_CACHE_WIN           128

#define TASK_KEY               "streamFileState"
#define STREAM_STATE_INFO_NAME "StreamStateCheckPoint"

struct SStreamFileState {
  SList*     usedBuffs;
  SList*     freeBuffs;
  void*      rowStateBuff;
  void*      pFileStore;
  int32_t    rowSize;
  int32_t    selectivityRowSize;
  int32_t    keyLen;
  uint64_t   preCheckPointVersion;
  uint64_t   checkPointVersion;
  TSKEY      maxTs;
  TSKEY      deleteMark;
  TSKEY      flushMark;
  uint64_t   maxRowCount;
  uint64_t   curRowCount;
  GetTsFun   getTs;
  char*      id;
  char*      cfName;
  void*      searchBuff;
  SSHashObj* pGroupIdMap;
  bool       hasFillCatch;

  _state_buff_cleanup_fn         stateBuffCleanupFn;
  _state_buff_remove_fn          stateBuffRemoveFn;
  _state_buff_remove_by_pos_fn   stateBuffRemoveByPosFn;
  _state_buff_create_statekey_fn stateBuffCreateStateKeyFn;

  _state_file_remove_fn stateFileRemoveFn;
  _state_file_get_fn    stateFileGetFn;

  _state_fun_get_fn stateFunctionGetFn;
};

typedef SRowBuffPos SRowBuffInfo;

int fillStateKeyCompare(const void* pWin1, const void* pDatas, int pos) {
  SWinKey* pWin2 = taosArrayGet(pDatas, pos);
  return winKeyCmprImpl((SWinKey*)pWin1, pWin2);
}

int32_t stateHashBuffRemoveFn(void* pBuff, const void* pKey, size_t keyLen) {
  SRowBuffPos** pos = tSimpleHashGet(pBuff, pKey, keyLen);
  if (pos) {
    (*pos)->beFlushed = true;
  }
  return tSimpleHashRemove(pBuff, pKey, keyLen);
}

void stateHashBuffRemoveByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  size_t        keyLen = pFileState->keyLen;
  SRowBuffPos** ppPos = tSimpleHashGet(pFileState->rowStateBuff, pPos->pKey, keyLen);
  if (ppPos) {
    if ((*ppPos) == pPos) {
      int32_t tmpRes = tSimpleHashRemove(pFileState->rowStateBuff, pPos->pKey, keyLen);
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
    }
  }
}

void stateHashBuffClearFn(void* pBuff) { tSimpleHashClear(pBuff); }

void stateHashBuffCleanupFn(void* pBuff) { tSimpleHashCleanup(pBuff); }

int32_t intervalFileRemoveFn(SStreamFileState* pFileState, const void* pKey) {
  return streamStateDel_rocksdb(pFileState->pFileStore, pKey);
}

int32_t intervalFileGetFn(SStreamFileState* pFileState, void* pKey, void** data, int32_t* pDataLen) {
  return streamStateGet_rocksdb(pFileState->pFileStore, pKey, data, pDataLen);
}

void* intervalCreateStateKey(SRowBuffPos* pPos, int64_t num) {
  SStateKey* pStateKey = taosMemoryCalloc(1, sizeof(SStateKey));
  if (pStateKey == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return NULL;
  }
  SWinKey* pWinKey = pPos->pKey;
  pStateKey->key = *pWinKey;
  pStateKey->opNum = num;
  return pStateKey;
}

void* defaultCreateStateKey(SRowBuffPos* pPos, int64_t num) {
  SWinKey* pStateKey = taosMemoryCalloc(1, sizeof(SWinKey));
  if (pStateKey == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return NULL;
  }
  SWinKey* pWinKey = pPos->pKey;
  *pStateKey = *pWinKey;
  return pStateKey;
}

int32_t sessionFileRemoveFn(SStreamFileState* pFileState, const void* pKey) {
  return streamStateSessionDel_rocksdb(pFileState->pFileStore, pKey);
}

int32_t sessionFileGetFn(SStreamFileState* pFileState, void* pKey, void** data, int32_t* pDataLen) {
  return streamStateSessionGet_rocksdb(pFileState->pFileStore, pKey, data, pDataLen);
}

void* sessionCreateStateKey(SRowBuffPos* pPos, int64_t num) {
  SStateSessionKey* pStateKey = taosMemoryCalloc(1, sizeof(SStateSessionKey));
  if (pStateKey == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return NULL;
  }
  SSessionKey* pWinKey = pPos->pKey;
  pStateKey->key = *pWinKey;
  pStateKey->opNum = num;
  return pStateKey;
}

static void streamFileStateDecode(TSKEY* pKey, void* pBuff, int32_t len) { pBuff = taosDecodeFixedI64(pBuff, pKey); }

static int32_t streamFileStateEncode(TSKEY* pKey, void** pVal, int32_t* pLen) {
  *pLen = sizeof(TSKEY);
  (*pVal) = taosMemoryCalloc(1, *pLen);
  if ((*pVal) == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  void*   buff = *pVal;
  int32_t tmp = taosEncodeFixedI64(&buff, *pKey);
  return TSDB_CODE_SUCCESS;
}

int32_t streamFileStateInit(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize, GetTsFun fp,
                            void* pFile, TSKEY delMark, const char* taskId, int64_t checkpointId, int8_t type,
                            SStreamFileState** ppFileState) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (memSize <= 0) {
    memSize = DEFAULT_MAX_STREAM_BUFFER_SIZE;
  }
  if (rowSize == 0) {
    code = TSDB_CODE_INVALID_PARA;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SStreamFileState* pFileState = taosMemoryCalloc(1, sizeof(SStreamFileState));
  QUERY_CHECK_NULL(pFileState, code, lino, _end, terrno);

  rowSize += selectRowSize;
  pFileState->maxRowCount = TMAX((uint64_t)memSize / rowSize, FLUSH_NUM * 2);
  pFileState->usedBuffs = tdListNew(POINTER_BYTES);
  QUERY_CHECK_NULL(pFileState->usedBuffs, code, lino, _end, terrno);

  pFileState->freeBuffs = tdListNew(POINTER_BYTES);
  QUERY_CHECK_NULL(pFileState->freeBuffs, code, lino, _end, terrno);

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  int32_t    cap = TMIN(MIN_NUM_OF_ROW_BUFF, pFileState->maxRowCount);
  if (type == STREAM_STATE_BUFF_HASH || type == STREAM_STATE_BUFF_HASH_SEARCH) {
    pFileState->rowStateBuff = tSimpleHashInit(cap, hashFn);
    pFileState->stateBuffCleanupFn = stateHashBuffCleanupFn;
    pFileState->stateBuffRemoveFn = stateHashBuffRemoveFn;
    pFileState->stateBuffRemoveByPosFn = stateHashBuffRemoveByPosFn;
    pFileState->stateBuffCreateStateKeyFn = intervalCreateStateKey;

    pFileState->stateFileRemoveFn = intervalFileRemoveFn;
    pFileState->stateFileGetFn = intervalFileGetFn;
    pFileState->cfName = taosStrdup("state");
    pFileState->stateFunctionGetFn = addRowBuffIfNotExist;
  } else if (type == STREAM_STATE_BUFF_SORT) {
    pFileState->rowStateBuff = tSimpleHashInit(cap, hashFn);
    pFileState->stateBuffCleanupFn = sessionWinStateCleanup;
    pFileState->stateBuffRemoveFn = deleteSessionWinStateBuffFn;
    pFileState->stateBuffRemoveByPosFn = deleteSessionWinStateBuffByPosFn;
    pFileState->stateBuffCreateStateKeyFn = sessionCreateStateKey;

    pFileState->stateFileRemoveFn = sessionFileRemoveFn;
    pFileState->stateFileGetFn = sessionFileGetFn;
    pFileState->cfName = taosStrdup("sess");
    pFileState->stateFunctionGetFn = getSessionRowBuff;
  } else if (type == STREAM_STATE_BUFF_HASH_SORT) {
    pFileState->rowStateBuff = tSimpleHashInit(cap, hashFn);
    pFileState->searchBuff = tSimpleHashInit(MIN_NUM_SEARCH_BUCKET, hashFn);
    QUERY_CHECK_NULL(pFileState->searchBuff, code, lino, _end, terrno);
    pFileState->stateBuffCleanupFn = stateHashBuffCleanupFn;
    pFileState->stateBuffRemoveFn = stateHashBuffRemoveFn;
    pFileState->stateBuffRemoveByPosFn = stateHashBuffRemoveByPosFn;
    pFileState->stateBuffCreateStateKeyFn = defaultCreateStateKey;

    pFileState->stateFileRemoveFn = hashSortFileRemoveFn;
    pFileState->stateFileGetFn = hashSortFileGetFn;
    pFileState->cfName = taosStrdup("fill");
    pFileState->stateFunctionGetFn = NULL;
  }

  QUERY_CHECK_NULL(pFileState->usedBuffs, code, lino, _end, terrno);
  QUERY_CHECK_NULL(pFileState->freeBuffs, code, lino, _end, terrno);
  QUERY_CHECK_NULL(pFileState->rowStateBuff, code, lino, _end, terrno);
  QUERY_CHECK_NULL(pFileState->cfName, code, lino, _end, terrno);

  if (type == STREAM_STATE_BUFF_HASH_SEARCH) {
    pFileState->searchBuff = tSimpleHashInit(MIN_NUM_SEARCH_BUCKET, hashFn);
    QUERY_CHECK_NULL(pFileState->searchBuff, code, lino, _end, terrno);
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
  QUERY_CHECK_NULL(pFileState->id, code, lino, _end, terrno);

  pFileState->pGroupIdMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pFileState->pGroupIdMap, code, lino, _end, terrno);

  pFileState->hasFillCatch = true;

  if (type == STREAM_STATE_BUFF_HASH || type == STREAM_STATE_BUFF_HASH_SEARCH) {
    code = recoverSnapshot(pFileState, checkpointId);
  } else if (type == STREAM_STATE_BUFF_SORT) {
    code = recoverSession(pFileState, checkpointId);
  } else if (type == STREAM_STATE_BUFF_HASH_SORT) {
    code = recoverFillSnapshot(pFileState, checkpointId);
  }
  QUERY_CHECK_CODE(code, lino, _end);

  void*   valBuf = NULL;
  int32_t len = 0;
  int32_t tmpRes = streamDefaultGet_rocksdb(pFileState->pFileStore, STREAM_STATE_INFO_NAME, &valBuf, &len);
  if (tmpRes == TSDB_CODE_SUCCESS) {
    QUERY_CHECK_CONDITION((len == sizeof(TSKEY)), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    streamFileStateDecode(&pFileState->flushMark, valBuf, len);
    qDebug("===stream===flushMark  read:%" PRId64, pFileState->flushMark);
  }
  taosMemoryFreeClear(valBuf);
  (*ppFileState) = pFileState;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    streamFileStateDestroy(pFileState);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  sessionWinStateCleanup(pFileState->searchBuff);
  tSimpleHashCleanup(pFileState->pGroupIdMap);
  taosMemoryFree(pFileState);
}

int32_t putFreeBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pPos->pRowBuff) {
    code = tdListAppend(pFileState->freeBuffs, &(pPos->pRowBuff));
    QUERY_CHECK_CODE(code, lino, _end);
    pPos->pRowBuff = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void clearExpiredRowBuff(SStreamFileState* pFileState, TSKEY ts, bool all) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)(pNode->data);
    if (all || (pFileState->getTs(pPos->pKey) < ts && !pPos->beUsed)) {
      code = putFreeBuff(pFileState, pPos);
      QUERY_CHECK_CODE(code, lino, _end);

      if (!all) {
        pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
      }
      destroyRowBuffPos(pPos);
      SListNode* tmp = tdListPopNode(pFileState->usedBuffs, pNode);
      taosMemoryFreeClear(tmp);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

int32_t clearFlushedRowBuff(SStreamFileState* pFileState, SStreamSnapshot* pFlushList, uint64_t max, bool all) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  uint64_t  i = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && i < max) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (isFlushedState(pFileState, pFileState->getTs(pPos->pKey), 0)) {
      if (all || !pPos->beUsed) {
        if (all && !pPos->pRowBuff) {
          continue;
        }
        code = tdListAppend(pFlushList, &pPos);
        QUERY_CHECK_CODE(code, lino, _end);

        pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));
        pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
        if (pPos->beUsed == false) {
          SListNode* tmp = tdListPopNode(pFileState->usedBuffs, pNode);
          taosMemoryFreeClear(tmp);
        }
        if (pPos->pRowBuff) {
          i++;
        }
      }
    }
  }
  qDebug("clear flushed row buff. %d rows to disk. is all:%d", listNEles(pFlushList), all);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void streamFileStateClear(SStreamFileState* pFileState) {
  pFileState->flushMark = INT64_MIN;
  pFileState->maxTs = INT64_MIN;
  tSimpleHashClear(pFileState->rowStateBuff);
  clearExpiredRowBuff(pFileState, 0, true);
}

bool needClearDiskBuff(SStreamFileState* pFileState) { return pFileState->flushMark > 0; }

void streamFileStateReleaseBuff(SStreamFileState* pFileState, SRowBuffPos* pPos, bool used) { pPos->beUsed = used; }

int32_t popUsedBuffs(SStreamFileState* pFileState, SStreamSnapshot* pFlushList, uint64_t max, bool used) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  uint64_t  i = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && i < max) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (pPos->beUsed == used) {
      if (used && !pPos->pRowBuff) {
        continue;
      }
      code = tdListAppend(pFlushList, &pPos);
      QUERY_CHECK_CODE(code, lino, _end);

      pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));
      pFileState->stateBuffRemoveByPosFn(pFileState, pPos);
      if (pPos->beUsed == false) {
        SListNode* tmp = tdListPopNode(pFileState->usedBuffs, pNode);
        taosMemoryFreeClear(tmp);
      }
      if (pPos->pRowBuff) {
        i++;
      }
    }
  }

  qInfo("stream state flush %d rows to disk. is used:%d", listNEles(pFlushList), used);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t flushRowBuff(SStreamFileState* pFileState) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamSnapshot* pFlushList = tdListNew(POINTER_BYTES);
  if (!pFlushList) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  uint64_t num = (uint64_t)(pFileState->curRowCount * FLUSH_RATIO);
  num = TMAX(num, FLUSH_NUM);
  code = clearFlushedRowBuff(pFileState, pFlushList, num, false);
  QUERY_CHECK_CODE(code, lino, _end);

  if (isListEmpty(pFlushList)) {
    code = popUsedBuffs(pFileState, pFlushList, num, false);
    QUERY_CHECK_CODE(code, lino, _end);

    if (isListEmpty(pFlushList)) {
      code = popUsedBuffs(pFileState, pFlushList, num, true);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pFileState->searchBuff) {
    code = clearFlushedRowBuff(pFileState, pFlushList, pFileState->curRowCount, true);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  flushSnapshot(pFileState, pFlushList, false);

  SListIter fIter = {0};
  tdListInitIter(pFlushList, &fIter, TD_LIST_FORWARD);
  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&fIter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    code = putFreeBuff(pFileState, pPos);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  tdListFreeP(pFlushList, destroyRowBuffPosPtr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t clearRowBuff(SStreamFileState* pFileState) {
  if (pFileState->deleteMark != INT64_MAX) {
    clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark, false);
  }
  do {
    int32_t code = flushRowBuff(pFileState);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } while (isListEmpty(pFileState->freeBuffs) && pFileState->curRowCount == pFileState->maxRowCount);
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

void streamFileStateClearBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  if (pPos->pRowBuff) {
    memset(pPos->pRowBuff, 0, pFileState->rowSize);
  }
}

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pPos = taosMemoryCalloc(1, sizeof(SRowBuffPos));
  if (!pPos) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pPos->pKey = taosMemoryCalloc(1, pFileState->keyLen);
  if (!pPos->pKey) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  void* pBuff = getFreeBuff(pFileState);
  if (pBuff) {
    pPos->pRowBuff = pBuff;
    goto _end;
  }

  if (pFileState->curRowCount < pFileState->maxRowCount) {
    pBuff = taosMemoryCalloc(1, pFileState->rowSize);
    QUERY_CHECK_NULL(pBuff, code, lino, _error, terrno);
    pPos->pRowBuff = pBuff;
    pFileState->curRowCount++;
    goto _end;
  }

  code = clearRowBuff(pFileState);
  QUERY_CHECK_CODE(code, lino, _error);

  pPos->pRowBuff = getFreeBuff(pFileState);

_end:
  code = tdListAppend(pFileState->usedBuffs, &pPos);
  QUERY_CHECK_CODE(code, lino, _error);

  QUERY_CHECK_CONDITION((pPos->pRowBuff != NULL), code, lino, _error, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return NULL;
  }

  return pPos;
}

SRowBuffPos* getNewRowPosForWrite(SStreamFileState* pFileState) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* newPos = getNewRowPos(pFileState);
  if (!newPos) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  newPos->beUsed = true;
  newPos->beFlushed = false;
  newPos->needFree = false;
  newPos->beUpdated = true;
  return newPos;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return NULL;
}

int32_t addRowBuffIfNotExist(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                             int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  (*pWinCode) = TSDB_CODE_SUCCESS;
  pFileState->maxTs = TMAX(pFileState->maxTs, pFileState->getTs(pKey));
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowStateBuff, pKey, keyLen);
  if (pos) {
    if (pVal != NULL) {
      *pVLen = pFileState->rowSize;
      *pVal = *pos;
      (*pos)->beUsed = true;
      (*pos)->beFlushed = false;
    }
    goto _end;
  }
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  if (!pNewPos || !pNewPos->pRowBuff) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  memcpy(pNewPos->pKey, pKey, keyLen);
  (*pWinCode) = TSDB_CODE_FAILED;

  TSKEY ts = pFileState->getTs(pKey);
  if (!isDeteled(pFileState, ts) && isFlushedState(pFileState, ts, 0)) {
    int32_t len = 0;
    void*   p = NULL;
    (*pWinCode) = pFileState->stateFileGetFn(pFileState, pKey, &p, &len);
    qDebug("===stream===get %" PRId64 " from disc, res %d", ts, (*pWinCode));
    if ((*pWinCode) == TSDB_CODE_SUCCESS) {
      memcpy(pNewPos->pRowBuff, p, len);
    }
    taosMemoryFree(p);
  }

  code = tSimpleHashPut(pFileState->rowStateBuff, pKey, keyLen, &pNewPos, POINTER_BYTES);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pVal) {
    *pVLen = pFileState->rowSize;
    *pVal = pNewPos;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen) {
  int32_t code_buff = pFileState->stateBuffRemoveFn(pFileState->rowStateBuff, pKey, keyLen);
  qTrace("%s at line %d res:%d", __func__, __LINE__, code_buff);
  int32_t code_file = pFileState->stateFileRemoveFn(pFileState, pKey);
  qTrace("%s at line %d res:%d", __func__, __LINE__, code_file);
  if (pFileState->searchBuff != NULL) {
    deleteHashSortRowBuff(pFileState, pKey);
  }
}

void deleteRowBuffByGroupId(SStreamFileState* pFileState, uint64_t groupId) {
  SSHashObj* pRowMap = pFileState->rowStateBuff;
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pRowMap, pIte, &iter)) != NULL) {
    size_t keyLen = 0;
    SWinKey* pKey = tSimpleHashGetKey(pIte, &keyLen);
    if (pKey->groupId == groupId) {
      int32_t tmpRes = tSimpleHashIterateRemove(pRowMap, pKey, keyLen, &pIte, &iter);
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
    }
  }

  while (1) {
    SWinKey tmp = {.ts = INT64_MIN, .groupId = groupId};
    SStreamStateCur* pCur = streamStateSeekKeyNext_rocksdb(pFileState->pFileStore, &tmp);
    SWinKey delKey = {.groupId = groupId};
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pFileState->pFileStore, pCur, &delKey, NULL, 0);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    code = streamStateDel_rocksdb(pFileState->pFileStore, &delKey);
    qTrace("%s at line %d res:%d", __func__, __LINE__, code);
  }
}

static int32_t recoverSessionRowBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t len = 0;
  void*   pBuff = NULL;
  code = pFileState->stateFileGetFn(pFileState, pPos->pKey, &pBuff, &len);
  QUERY_CHECK_CODE(code, lino, _end);
  memcpy(pPos->pRowBuff, pBuff, len);
  taosMemoryFree(pBuff);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t recoverStateRowBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  pPos->pRowBuff = getFreeBuff(pFileState);
  if (!pPos->pRowBuff) {
    if (pFileState->curRowCount < pFileState->maxRowCount) {
      pPos->pRowBuff = taosMemoryCalloc(1, pFileState->rowSize);
      if (!pPos->pRowBuff) {
        code = terrno;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      pFileState->curRowCount++;
    } else {
      code = clearRowBuff(pFileState);
      QUERY_CHECK_CODE(code, lino, _end);
      pPos->pRowBuff = getFreeBuff(pFileState);
    }
    QUERY_CHECK_CONDITION((pPos->pRowBuff != NULL), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  code = recoverSessionRowBuff(pFileState, pPos);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pPos->pRowBuff) {
    if (pPos->needFree) {
      code = recoverSessionRowBuff(pFileState, pPos);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    (*pVal) = pPos->pRowBuff;
    goto _end;
  }

  code = recoverStateRowBuff(pFileState, pPos);
  QUERY_CHECK_CODE(code, lino, _end);

  (*pVal) = pPos->pRowBuff;
  // if (!pPos->needFree) {
  //   code = tdListPrepend(pFileState->usedBuffs, &pPos);
  //   QUERY_CHECK_CODE(code, lino, _end);
  // }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

bool hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen) {
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowStateBuff, pKey, keyLen);
  if (pos) {
    return true;
  }
  return false;
}

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState) {
  int64_t mark = (pFileState->deleteMark == INT64_MAX || pFileState->maxTs == INT64_MIN)
                     ? INT64_MIN
                     : pFileState->maxTs - pFileState->deleteMark;
  clearExpiredRowBuff(pFileState, mark, false);
  return pFileState->usedBuffs;
}

void flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SListIter iter = {0};
  tdListInitIter(pSnapshot, &iter, TD_LIST_FORWARD);

  const int32_t BATCH_LIMIT = 256;

  int64_t    st = taosGetTimestampMs();
  SListNode* pNode = NULL;

  int idx = streamStateGetCfIdx(pFileState->pFileStore, pFileState->cfName);

  int32_t len = (pFileState->rowSize + sizeof(uint64_t) + sizeof(int32_t) + 64) * 2;
  char*   buf = taosMemoryCalloc(1, len);
  if (!buf) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  void* batch = streamStateCreateBatch();
  if (!batch) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  while ((pNode = tdListNext(&iter)) != NULL && code == TSDB_CODE_SUCCESS) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (pPos->beFlushed || !pPos->pRowBuff) {
      continue;
    }
    pPos->beFlushed = true;
    pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));

    qDebug("===stream===flushed start:%" PRId64, pFileState->getTs(pPos->pKey));
    if (streamStateGetBatchSize(batch) >= BATCH_LIMIT) {
      code = streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
      streamStateClearBatch(batch);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    void* pSKey = pFileState->stateBuffCreateStateKeyFn(pPos, ((SStreamState*)pFileState->pFileStore)->number);
    QUERY_CHECK_NULL(pSKey, code, lino, _end, terrno);

    code = streamStatePutBatchOptimize(pFileState->pFileStore, idx, batch, pSKey, pPos->pRowBuff, pFileState->rowSize,
                                       0, buf);
    taosMemoryFreeClear(pSKey);
    QUERY_CHECK_CODE(code, lino, _end);
    // todo handle failure
    memset(buf, 0, len);
  }
  taosMemoryFreeClear(buf);

  int32_t numOfElems = streamStateGetBatchSize(batch);
  if (numOfElems > 0) {
    code = streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    goto _end;
  }

  streamStateClearBatch(batch);

  clearSearchBuff(pFileState);

  int64_t elapsed = taosGetTimestampMs() - st;
  qDebug("%s flush to disk in batch model completed, rows:%d, batch size:%d, elapsed time:%" PRId64 "ms",
         pFileState->id, numOfElems, BATCH_LIMIT, elapsed);

  if (flushState) {
    void*   valBuf = NULL;
    int32_t len = 0;
    code = streamFileStateEncode(&pFileState->flushMark, &valBuf, &len);
    QUERY_CHECK_CODE(code, lino, _end);

    qDebug("===stream===flushMark write:%" PRId64, pFileState->flushMark);
    code = streamStatePutBatch(pFileState->pFileStore, "default", batch, STREAM_STATE_INFO_NAME, valBuf, len, 0);
    taosMemoryFree(valBuf);
    QUERY_CHECK_CODE(code, lino, _end);

    code = streamStatePutBatch_rocksdb(pFileState->pFileStore, batch);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(buf);
  streamStateDestroyBatch(batch);
}

int32_t forceRemoveCheckpoint(SStreamFileState* pFileState, int64_t checkpointId) {
  char keyBuf[128] = {0};
  TAOS_UNUSED(tsnprintf(keyBuf, sizeof(keyBuf), "%s:%" PRId64 "", TASK_KEY, checkpointId));
  return streamDefaultDel_rocksdb(pFileState->pFileStore, keyBuf);
}

int32_t deleteExpiredCheckPoint(SStreamFileState* pFileState, TSKEY mark) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t maxCheckPointId = 0;
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
    maxCheckPointId = taosStr2Int64((char*)buf, NULL, 10);
    taosMemoryFree(val);
  }
  for (int64_t i = maxCheckPointId; i > 0; i--) {
    char    buf[128] = {0};
    void*   val = 0;
    int32_t len = 0;
    TAOS_UNUSED(tsnprintf(buf, sizeof(buf), "%s:%" PRId64 "", TASK_KEY, i));
    code = streamDefaultGet_rocksdb(pFileState->pFileStore, buf, &val, &len);
    if (code != 0) {
      return TSDB_CODE_FAILED;
    }
    memcpy(buf, val, len);
    buf[len] = 0;
    taosMemoryFree(val);

    TSKEY ts;
    ts = taosStr2Int64((char*)buf, NULL, 10);
    if (ts < mark) {
      // statekey winkey.ts < mark
      int32_t tmpRes = forceRemoveCheckpoint(pFileState, i);
      qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
      break;
    }
  }
  return code;
}

int32_t recoverSession(SStreamFileState* pFileState, int64_t ckId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t winRes = TSDB_CODE_SUCCESS;
  if (pFileState->maxTs != INT64_MIN) {
    int64_t mark = (INT64_MIN + pFileState->deleteMark >= pFileState->maxTs)
                       ? INT64_MIN
                       : pFileState->maxTs - pFileState->deleteMark;
    int32_t tmpRes = deleteExpiredCheckPoint(pFileState, mark);
    qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
  }

  SStreamStateCur* pCur = streamStateSessionSeekToLast_rocksdb(pFileState->pFileStore, INT64_MAX);
  int32_t          recoverNum = TMIN(MIN_NUM_OF_RECOVER_ROW_BUFF, pFileState->maxRowCount);
  while (winRes == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount >= recoverNum) {
      break;
    }

    void*       pVal = NULL;
    int32_t     vlen = 0;
    SSessionKey key = {0};
    winRes = streamStateSessionGetKVByCur_rocksdb(getStateFileStore(pFileState), pCur, &key, &pVal, &vlen);
    if (winRes != TSDB_CODE_SUCCESS) {
      break;
    }

    if (vlen != pFileState->rowSize) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    SRowBuffPos* pPos = createSessionWinBuff(pFileState, &key, pVal, &vlen);
    pPos->beUsed = false;
    winRes = putSessionWinResultBuff(pFileState, pPos);
    if (winRes != TSDB_CODE_SUCCESS) {
      break;
    }

    winRes = streamStateSessionCurPrev_rocksdb(pCur);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  streamStateFreeCur(pCur);
  return code;
}

int32_t recoverSnapshot(SStreamFileState* pFileState, int64_t ckId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t winCode = TSDB_CODE_SUCCESS;
  if (pFileState->maxTs != INT64_MIN) {
    int64_t mark = (INT64_MIN + pFileState->deleteMark >= pFileState->maxTs)
                       ? INT64_MIN
                       : pFileState->maxTs - pFileState->deleteMark;
    int32_t tmpRes = deleteExpiredCheckPoint(pFileState, mark);
    qTrace("%s at line %d res:%d", __func__, __LINE__, tmpRes);
  }

  SStreamStateCur* pCur = streamStateSeekToLast_rocksdb(pFileState->pFileStore);
  int32_t          recoverNum = TMIN(MIN_NUM_OF_RECOVER_ROW_BUFF, pFileState->maxRowCount);
  while (winCode == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount >= recoverNum) {
      break;
    }

    void*        pVal = NULL;
    int32_t      vlen = 0;
    SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
    if (!pNewPos || !pNewPos->pRowBuff) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    winCode = streamStateGetKVByCur_rocksdb(getStateFileStore(pFileState), pCur, pNewPos->pKey, (const void**)&pVal, &vlen);
    qDebug("===stream=== get state by cur winres:%d. %s", winCode, __func__);
    if (winCode != TSDB_CODE_SUCCESS || pFileState->getTs(pNewPos->pKey) < pFileState->flushMark) {
      destroyRowBuffPos(pNewPos);
      SListNode* pNode = tdListPopTail(pFileState->usedBuffs);
      taosMemoryFreeClear(pNode);
      taosMemoryFreeClear(pVal);
      break;
    }
    if (vlen != pFileState->rowSize) {
      qError("row size mismatch, expect:%d, actual:%d", pFileState->rowSize, vlen);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      taosMemoryFreeClear(pVal);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    memcpy(pNewPos->pRowBuff, pVal, vlen);
    taosMemoryFreeClear(pVal);
    pNewPos->beFlushed = true;
    pNewPos->beUsed = false;
    qDebug("===stream=== read checkpoint state from disc. %s", __func__);
    code = tSimpleHashPut(pFileState->rowStateBuff, pNewPos->pKey, pFileState->keyLen, &pNewPos, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      destroyRowBuffPos(pNewPos);
      break;
    }
    streamStateCurPrev_rocksdb(pCur);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  streamStateFreeCur(pCur);
  return code;
}

int32_t streamFileStateGetSelectRowSize(SStreamFileState* pFileState) { return pFileState->selectivityRowSize; }

void streamFileStateReloadInfo(SStreamFileState* pFileState, TSKEY ts) {
  pFileState->flushMark = TMAX(pFileState->flushMark, ts);
  pFileState->maxTs = TMAX(pFileState->maxTs, ts);
}

void* getRowStateBuff(SStreamFileState* pFileState) { return pFileState->rowStateBuff; }
void* getSearchBuff(SStreamFileState* pFileState) { return pFileState->searchBuff; }

void* getStateFileStore(SStreamFileState* pFileState) { return pFileState->pFileStore; }

bool isDeteled(SStreamFileState* pFileState, TSKEY ts) {
  return pFileState->deleteMark != INT64_MAX && pFileState->maxTs > 0 &&
         ts < (pFileState->maxTs - pFileState->deleteMark);
}

bool isFlushedState(SStreamFileState* pFileState, TSKEY ts, TSKEY gap) { return ts <= (pFileState->flushMark + gap); }

TSKEY getFlushMark(SStreamFileState* pFileState) { return pFileState->flushMark; };

int32_t getRowStateRowSize(SStreamFileState* pFileState) { return pFileState->rowSize; }

int32_t getFunctionRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen) {
  int32_t winCode = TSDB_CODE_SUCCESS;
  return pFileState->stateFunctionGetFn(pFileState, pKey, keyLen, pVal, pVLen, &winCode);
}

int32_t recoverFillSnapshot(SStreamFileState* pFileState, int64_t ckId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pFileState->maxTs != INT64_MIN) {
    int64_t mark = (INT64_MIN + pFileState->deleteMark >= pFileState->maxTs)
                       ? INT64_MIN
                       : pFileState->maxTs - pFileState->deleteMark;
    code = deleteExpiredCheckPoint(pFileState, mark);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SStreamStateCur* pCur = streamStateFillSeekToLast_rocksdb(pFileState->pFileStore);
  if (pCur == NULL) {
    return code;
  }
  int32_t recoverNum = TMIN(MIN_NUM_OF_RECOVER_ROW_BUFF, pFileState->maxRowCount);
  int32_t winRes = TSDB_CODE_SUCCESS;
  while (winRes == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount >= recoverNum) {
      break;
    }

    void*        pVal = NULL;
    int32_t      vlen = 0;
    SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
    winRes = streamStateFillGetKVByCur_rocksdb(pCur, pNewPos->pKey, (const void**)&pVal, &vlen);
    qDebug("===stream=== get state by cur winres:%d. %s", winRes, __func__);
    if (winRes != TSDB_CODE_SUCCESS || isFlushedState(pFileState, pFileState->getTs(pNewPos->pKey), 0)) {
      destroyRowBuffPos(pNewPos);
      SListNode* pNode = tdListPopTail(pFileState->usedBuffs);
      taosMemoryFreeClear(pNode);
      taosMemoryFreeClear(pVal);
      break;
    }

    if (vlen != pFileState->rowSize) {
      qError("row size mismatch, expect:%d, actual:%d", pFileState->rowSize, vlen);
      destroyRowBuffPos(pNewPos);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      taosMemoryFreeClear(pVal);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    memcpy(pNewPos->pRowBuff, pVal, vlen);
    taosMemoryFreeClear(pVal);
    pNewPos->beFlushed = true;
    pNewPos->beUsed = false;
    qDebug("===stream=== read checkpoint state from disc. %s", __func__);
    winRes = tSimpleHashPut(pFileState->rowStateBuff, pNewPos->pKey, pFileState->keyLen, &pNewPos, POINTER_BYTES);
    if (winRes != TSDB_CODE_SUCCESS) {
      destroyRowBuffPos(pNewPos);
      break;
    }
    streamStateCurPrev_rocksdb(pCur);
  }
  streamStateFreeCur(pCur);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                   int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  (*pWinCode) = TSDB_CODE_FAILED;
  pFileState->maxTs = TMAX(pFileState->maxTs, pFileState->getTs(pKey));
  SRowBuffPos** ppPos = tSimpleHashGet(pFileState->rowStateBuff, pKey, keyLen);
  if (ppPos) {
    *pVLen = pFileState->rowSize;
    *pVal = *ppPos;
    (*ppPos)->beUsed = true;
    (*ppPos)->beFlushed = false;
    (*pWinCode) = TSDB_CODE_SUCCESS;
    if ((*ppPos)->pRowBuff == NULL) {
      code = recoverStateRowBuff(pFileState, *ppPos);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }
  TSKEY ts = pFileState->getTs(pKey);
  if (!isDeteled(pFileState, ts) && isFlushedState(pFileState, ts, 0)) {
    int32_t len = 0;
    void*   p = NULL;
    (*pWinCode) = pFileState->stateFileGetFn(pFileState, pKey, &p, &len);
    qDebug("===stream===get %" PRId64 " from disc, res %d", ts, (*pWinCode));
    if ((*pWinCode) == TSDB_CODE_SUCCESS) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
      if (!pNewPos || !pNewPos->pRowBuff) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      memcpy(pNewPos->pKey, pKey, keyLen);
      memcpy(pNewPos->pRowBuff, p, len);
      code = tSimpleHashPut(pFileState->rowStateBuff, pKey, keyLen, &pNewPos, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pVal) {
        *pVLen = pFileState->rowSize;
        *pVal = pNewPos;
      }
    }
    taosMemoryFree(p);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamFileStateGroupPut(SStreamFileState* pFileState, int64_t groupId, void* value, int32_t vLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (value != NULL) {
    code = TSDB_CODE_INVALID_PARA;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (tSimpleHashGet(pFileState->pGroupIdMap, &groupId, sizeof(int64_t)) == NULL) {
    if (tSimpleHashGetSize(pFileState->pGroupIdMap) <= MAX_GROUP_ID_NUM) {
      code = tSimpleHashPut(pFileState->pGroupIdMap, &groupId, sizeof(int64_t), NULL, 0);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = streamStatePutParTag_rocksdb(pFileState->pFileStore, groupId, value, vLen);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void streamFileStateGroupCurNext(SStreamStateCur* pCur) {
  SStreamFileState* pFileState = (SStreamFileState*)pCur->pStreamFileState;
  if (pCur->hashIter == -1) {
    streamStateCurNext(pFileState->pFileStore, pCur);
    return;
  }

  int64_t gpId = *(int64_t*)tSimpleHashGetKey(pCur->pHashData, NULL);
  pCur->minGpId = TMAX(pCur->minGpId, gpId);

  SSHashObj* pHash = pFileState->pGroupIdMap;
  pCur->pHashData = tSimpleHashIterate(pHash, pCur->pHashData, &pCur->hashIter);
  if (!pCur->pHashData) {
    pCur->hashIter = -1;
    streamStateParTagSeekKeyNext_rocksdb(pFileState->pFileStore, pCur->minGpId, pCur);
    return;
  }
}

int32_t streamFileStateGroupGetKVByCur(SStreamStateCur* pCur, int64_t* pKey, void** pVal, int32_t* pVLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pCur->pHashData) {
    *pKey = *(int64_t*)tSimpleHashGetKey(pCur->pHashData, NULL);
    return code;
  }
  return streamStateParTagGetKVByCur_rocksdb(pCur, pKey, NULL, NULL);
}

SSHashObj* getGroupIdCache(SStreamFileState* pFileState) {
  return pFileState->pGroupIdMap;
}

void clearExpiredState(SStreamFileState* pFileState) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SSHashObj* pSearchBuff = pFileState->searchBuff;
  void*      pIte = NULL;
  int32_t    iter = 0;
  while ((pIte = tSimpleHashIterate(pSearchBuff, pIte, &iter)) != NULL) {
    SArray* pWinStates = *((void**)pIte);
    int32_t size = taosArrayGetSize(pWinStates);
    for (int32_t i = 0; i < size - 1; i++) {
      SWinKey* pKey = taosArrayGet(pWinStates, i);
      int32_t  code_buff = pFileState->stateBuffRemoveFn(pFileState->rowStateBuff, pKey, sizeof(SWinKey));
      qTrace("clear expired buff, ts:%" PRId64 ". %s at line %d res:%d", pKey->ts, __func__, __LINE__, code_buff);

      if (isFlushedState(pFileState, pKey->ts, 0)) {
        int32_t code_file = pFileState->stateFileRemoveFn(pFileState, pKey);
        qTrace("clear expired file, ts:%" PRId64 ". %s at line %d res:%d", pKey->ts, __func__, __LINE__, code_file);
      }
    }
    taosArrayRemoveBatch(pWinStates, 0, size - 1, NULL);
  }
  code = clearRowBuff(pFileState);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
}

#ifdef BUILD_NO_CALL
int32_t getStateSearchRowBuff(SStreamFileState* pFileState, const SWinKey* pKey, void** pVal, int32_t* pVLen,
                           int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  code = addRowBuffIfNotExist(pFileState, (void*)pKey, sizeof(SWinKey), pVal, pVLen, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, sizeof(SWinKey));
    QUERY_CHECK_NULL(pWinStates, code, lino, _end, terrno);

    code = tSimpleHashPut(pSearchBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  // recover
  if (taosArrayGetSize(pWinStates) == 0 && needClearDiskBuff(pFileState)) {
    TSKEY            ts = getFlushMark(pFileState);
    SWinKey          start = {.groupId = pKey->groupId, .ts = INT64_MAX};
    void*            pState = getStateFileStore(pFileState);
    SStreamStateCur* pCur = streamStateSeekKeyPrev_rocksdb(pState, &start);
    for (int32_t i = 0; i < NUM_OF_CACHE_WIN; i++) {
      SWinKey tmpKey = {.groupId = pKey->groupId};
      int32_t tmpRes = streamStateGetGroupKVByCur_rocksdb(pState, pCur, &tmpKey, NULL, 0);
      if (tmpRes != TSDB_CODE_SUCCESS) {
        break;
      }
      void* tmp = taosArrayPush(pWinStates, &tmpKey);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      streamStateCurPrev_rocksdb(pCur);
    }
    taosArraySort(pWinStates, winKeyCmprImpl);
    streamStateFreeCur(pCur);
  }

  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (!isFlushedState(pFileState, pKey->ts, 0)|| index >= 0) {
    // find the first position which is smaller than the pKey
    if (index >= 0) {
      SWinKey* pTmpKey = taosArrayGet(pWinStates, index);
      if (winKeyCmprImpl(pTmpKey, pKey) == 0) {
        goto _end;
      }
    }
    index++;
    void* tmp = taosArrayInsert(pWinStates, index, pKey);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  if (size >= MAX_NUM_OF_CACHE_WIN) {
    int32_t num = size - NUM_OF_CACHE_WIN;
    taosArrayRemoveBatch(pWinStates, 0, num, NULL);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
#endif

int32_t getRowStatePrevRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** ppVal,
                           int32_t* pVLen, int32_t* pWinCode) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void*      pState = getStateFileStore(pFileState);
  void**     ppBuff = (void**) tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    qDebug("===stream=== search buff is empty.group id:%" PRId64, pKey->groupId);
    SStreamStateCur* pCur = streamStateSeekKeyPrev_rocksdb(pState, pKey);
    void*            tmpVal = NULL;
    int32_t          len = 0;
    (*pWinCode) = streamStateGetGroupKVByCur_rocksdb(pState, pCur, pResKey, (const void**)&tmpVal, &len);
    if ((*pWinCode) == TSDB_CODE_SUCCESS) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
      if (!pNewPos || !pNewPos->pRowBuff) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      memcpy(pNewPos->pRowBuff, tmpVal, len);
      taosMemoryFreeClear(tmpVal);
      *pVLen = getRowStateRowSize(pFileState);
      (*ppVal) = pNewPos;
    }
    streamStateFreeCur(pCur);
    return code;
  }
  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (index >= 0) {
    SWinKey* pCurKey = taosArrayGet(pWinStates, index);
    if (winKeyCmprImpl(pCurKey, pKey) == 0) {
      index--;
    } else {
      qDebug("%s failed at line %d since do not find cur SWinKey. trigger may be force window close", __func__, __LINE__);
    }
  }
  if (index == -1) {
    SStreamStateCur* pCur = streamStateSeekKeyPrev_rocksdb(pState, pKey);
    void*            tmpVal = NULL;
    int32_t          len = 0;
    (*pWinCode) = streamStateGetGroupKVByCur_rocksdb(pState, pCur, pResKey, (const void**)&tmpVal, &len);
    if ((*pWinCode) == TSDB_CODE_SUCCESS) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
      if (!pNewPos || !pNewPos->pRowBuff) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      memcpy(pNewPos->pRowBuff, tmpVal, len);
      taosMemoryFreeClear(tmpVal);
      *pVLen = getRowStateRowSize(pFileState);
      (*ppVal) = pNewPos;
    }
    streamStateFreeCur(pCur);
    return code;
  } else {
    SWinKey* pPrevKey = taosArrayGet(pWinStates, index);
    *pResKey = *pPrevKey;
    return addRowBuffIfNotExist(pFileState, (void*)pPrevKey, sizeof(SWinKey), ppVal, pVLen, pWinCode);
  }
  (*pWinCode) = TSDB_CODE_FAILED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t addSearchItem(SStreamFileState* pFileState, SArray* pWinStates, const SWinKey* pKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (!isFlushedState(pFileState, pKey->ts, 0) || index >= 0 || size == 0) {
    if (index >= 0) {
      SWinKey* pTmpKey = taosArrayGet(pWinStates, index);
      if (winKeyCmprImpl(pTmpKey, pKey) == 0) {
        goto _end;
      }
    }
    index++;
    void* tmp = taosArrayInsert(pWinStates, index, pKey);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  if (size >= MAX_NUM_OF_CACHE_WIN) {
    int32_t num = size - NUM_OF_CACHE_WIN;
    taosArrayRemoveBatch(pWinStates, 0, num, NULL);
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t addArrayBuffIfNotExist(SSHashObj* pSearchBuff, uint64_t groupId, SArray** ppResStates) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0; 
  SArray*    pWinStates = NULL;
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, sizeof(SWinKey));
    QUERY_CHECK_NULL(pWinStates, code, lino, _end, terrno);

    code = tSimpleHashPut(pSearchBuff, &groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  (*ppResStates) = pWinStates;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
