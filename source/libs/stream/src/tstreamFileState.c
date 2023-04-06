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

#include "taos.h"
#include "thash.h"
#include "tsimplehash.h"
#include "streamBackendRocksdb.h"


#define FLUSH_RATIO 0.2
#define DEFAULT_MAX_STREAM_BUFFER_SIZE (128 * 1024 * 1024);

struct SStreamFileState {
  SList*     usedBuffs;
  SList*     freeBuffs;
  SSHashObj* rowBuffMap;
  void*      pFileStore;
  int32_t    rowSize;
  int32_t    keyLen;
  uint64_t   preCheckPointVersion;
  uint64_t   checkPointVersion;
  TSKEY      maxTs;
  TSKEY      deleteMark;
  TSKEY      flushMark;
  uint64_t   maxRowCount;
  uint64_t   curRowCount;
  GetTsFun   getTs;
};

typedef SRowBuffPos SRowBuffInfo;

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t rowSize, GetTsFun fp, void* pFile, TSKEY delMark) {
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
  pFileState->usedBuffs = tdListNew(POINTER_BYTES);
  pFileState->freeBuffs = tdListNew(POINTER_BYTES);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pFileState->rowBuffMap = tSimpleHashInit(1024, hashFn);
  if (!pFileState->usedBuffs || !pFileState->freeBuffs || !pFileState->rowBuffMap) {
    goto _error;
  }
  pFileState->rowSize = rowSize;
  pFileState->preCheckPointVersion = 0;
  pFileState->checkPointVersion = 1;
  pFileState->pFileStore = pFile;
  pFileState->getTs = fp;
  pFileState->maxRowCount = memSize / rowSize;
  pFileState->curRowCount = 0;
  pFileState->deleteMark = delMark;
  pFileState->flushMark = -1;
  recoverSnapshot(pFileState);
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
  tdListFreeP(pFileState->usedBuffs, destroyRowBuffPosPtr);
  tdListFreeP(pFileState->freeBuffs, destroyRowBuff);
  tSimpleHashCleanup(pFileState->rowBuffMap);
  taosMemoryFree(pFileState);
}

void clearExpiredRowBuff(SStreamFileState* pFileState, TSKEY ts, bool all) {
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)(pNode->data);
    if (all || (pFileState->getTs(pPos->pKey) <ts) ) {
      tdListPopNode(pFileState->usedBuffs, pNode);
      taosMemoryFreeClear(pNode);
      tdListAppend(pFileState->freeBuffs, &(pPos->pRowBuff));
      pPos->pRowBuff = NULL;
      tSimpleHashRemove(pFileState->rowBuffMap, pPos->pKey, pFileState->keyLen);
      destroyRowBuffPos(pPos);
    }
  }
}

void streamFileStateClear(SStreamFileState* pFileState) {
  tSimpleHashClear(pFileState->rowBuffMap);
  clearExpiredRowBuff(pFileState, 0, true);
}

int32_t flushRowBuff(SStreamFileState* pFileState) {
  SStreamSnapshot* pFlushList = tdListNew(POINTER_BYTES);
  if (!pFlushList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  uint64_t num = (uint64_t)(pFileState->curRowCount * FLUSH_RATIO);
  uint64_t i = 0;
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && i < num) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (!pPos->beUsed) {
      tdListAppend(pFlushList, &pPos);
      pFileState->flushMark = TMAX(pFileState->flushMark, pFileState->getTs(pPos->pKey));
      tSimpleHashRemove(pFileState->rowBuffMap, pPos->pKey, pFileState->keyLen);
      i++;
    }
  }
  flushSnapshot(pFileState, pFlushList, false);
  return TSDB_CODE_SUCCESS;
}

int32_t clearRowBuff(SStreamFileState* pFileState) {
  clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark, false);
  if (isListEmpty(pFileState->freeBuffs)) {
    return flushRowBuff(pFileState);
  }
  return TSDB_CODE_SUCCESS;
}

void* getFreeBuff(SList* lists, int32_t buffSize) {
  SListNode* pNode = tdListPopHead(lists);
  if (!pNode) {
    return NULL;
  }
  void* ptr = *(void**)pNode->data;
  memset(ptr, 0, buffSize);
  taosMemoryFree(pNode);
  return ptr;
}

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState) {
  SRowBuffPos* pPos = taosMemoryCalloc(1, sizeof(SRowBuffPos));
  tdListAppend(pFileState->usedBuffs, &pPos);
  void* pBuff = getFreeBuff(pFileState->freeBuffs, pFileState->rowSize);
  if (pBuff) {
    pPos->pRowBuff = pBuff;
    return pPos;
  }

  if (pFileState->curRowCount < pFileState->maxRowCount) {
    pBuff = taosMemoryCalloc(1, pFileState->rowSize);
    if (pBuff) {
      pPos->pRowBuff = pBuff;
      pFileState->curRowCount++;
      return pPos;
    }
  }

  int32_t code = clearRowBuff(pFileState);
  ASSERT(code == 0);
  pPos->pRowBuff = getFreeBuff(pFileState->freeBuffs, pFileState->rowSize);
  return pPos;
}

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen) {
  pFileState->maxTs = TMAX(pFileState->maxTs, pFileState->getTs(pKey));
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowBuffMap, pKey, keyLen);
  if (pos) {
    if (pVal) {
      *pVLen = pFileState->rowSize;
      *pVal = *pos;
    }
    return TSDB_CODE_SUCCESS;
  }
  SRowBuffPos* pNewPos = getNewRowPos(pFileState);
  ASSERT(pNewPos);// todo(liuyao) delete
  pNewPos->pKey = taosMemoryCalloc(1, keyLen);
  memcpy(pNewPos->pKey, pKey, keyLen);

  TSKEY ts = pFileState->getTs(pKey);
  if (ts > pFileState->maxTs - pFileState->deleteMark && ts < pFileState->flushMark) {
    int32_t len = 0;
    void *pVal = NULL;
    streamStateGet_rocksdb(pFileState->pFileStore, pKey, pVal, &len);
    memcpy(pNewPos->pRowBuff, pVal, len);
    taosMemoryFree(pVal);
  }

  tSimpleHashPut(pFileState->rowBuffMap, pKey, keyLen, &pNewPos, POINTER_BYTES);
  if (pVal) {
    *pVLen = pFileState->rowSize;
    *pVal = pNewPos;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen) {
  int32_t code_buff = tSimpleHashRemove(pFileState->rowBuffMap, pKey, keyLen);
  int32_t code_rocks = streamStateDel_rocksdb(pFileState->pFileStore, pKey);
  return code_buff == TSDB_CODE_SUCCESS ? code_buff : code_rocks;
}

int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal) {
  if (pPos->pRowBuff) {
    (*pVal) = pPos->pRowBuff;
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = clearRowBuff(pFileState);
  ASSERT(code == 0);
  pPos->pRowBuff = getFreeBuff(pFileState->freeBuffs, pFileState->rowSize);

  int32_t len = 0;
  streamStateGet_rocksdb(pFileState->pFileStore, pPos->pKey, pVal, &len);
  memcpy(pPos->pRowBuff, pVal, len);
  taosMemoryFree(pVal);
  (*pVal) = pPos->pRowBuff;
  return TSDB_CODE_SUCCESS;
}

bool hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen) {
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowBuffMap, pKey, keyLen);
  if (pos) {
    return true;
  }
  return false;
}

void releaseRowBuffPos(SRowBuffPos* pBuff) {
  pBuff->beUsed = false;
}

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState) {
  clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark, false);
  return pFileState->usedBuffs;
}

void streamFileStateDecode(SStreamFileState* pFileState, void* pBuff, int32_t len) {
  pBuff = taosDecodeFixedI64(pBuff, &pFileState->flushMark);
}

void streamFileStateEncode(SStreamFileState* pFileState, void** pVal, int32_t* pLen) {
  *pLen = sizeof(TSKEY);
  *pVal = taosMemoryCalloc(1, *pLen);
  void** buff = pVal; 
  taosEncodeFixedI64(buff, pFileState->flushMark);
}

int32_t flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState) {
  int32_t code = TSDB_CODE_SUCCESS;
  SListIter iter = {0};
  tdListInitIter(pSnapshot, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && code == TSDB_CODE_SUCCESS) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    code = streamStatePut_rocksdb(pFileState->pFileStore, pPos->pKey, pPos->pRowBuff, pFileState->rowSize);
  }
  if (flushState) {
    int32_t len = 0;
    void* buff = NULL;
    streamFileStateEncode(pFileState, &buff, &len);
    SWinKey key = {.ts = -1, .groupId = 0}; // dengyihao
    streamStatePut_rocksdb(pFileState->pFileStore, &key, buff, len);
  }
  return code;
}

int32_t recoverSnapshot(SStreamFileState* pFileState) {
  int32_t code = TSDB_CODE_SUCCESS;
  SWinKey stkey = {.ts = -1, .groupId = 0}; // dengyihao
  void* pStVal = NULL;
  int32_t len = 0;
  code = streamStateGet_rocksdb(pFileState->pFileStore, &stkey, &pStVal, &len);
  if (code == TSDB_CODE_SUCCESS) {
    streamFileStateDecode(pFileState, pStVal, len);
  } else {
    return TSDB_CODE_FAILED;
  }

  SWinKey key = {.groupId = 0, .ts = 0};
  SStreamStateCur* pCur = streamStateGetCur_rocksdb(pFileState->pFileStore, &key);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }
  code = streamStateSeekLast(pFileState->pFileStore, pCur);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  while (code == TSDB_CODE_SUCCESS) {
    if (pFileState->curRowCount == pFileState->maxRowCount) {
      break;
    }
    void* pVal = NULL;
    int32_t pVLen = 0;
    SRowBuffPos* pNewPos = getNewRowPos(pFileState);
    code = streamStateGetKVByCur_rocksdb(pCur, pNewPos->pKey, (const void **)&pVal, &pVLen);
    if (code != TSDB_CODE_SUCCESS || pFileState->getTs(pNewPos->pKey) < pFileState->flushMark) {
      destroyRowBuffPos(pNewPos);
      break;
    }
    memcpy(pNewPos->pRowBuff, pVal, pVLen);
    taosMemoryFree(pVal);
    code = tSimpleHashPut(pFileState->rowBuffMap, pNewPos->pKey, pFileState->rowSize, &pNewPos, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      destroyRowBuffPos(pNewPos);
      break;
    }
    code = streamStateCurPrev_rocksdb(pFileState->pFileStore, pCur);
  }

  return TSDB_CODE_SUCCESS;
}