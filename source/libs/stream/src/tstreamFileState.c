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
  uint64_t   maxRowCount;
  uint64_t   curRowCount;
  ExpiredFun expFunc;
};

typedef SRowBuffPos SRowBuffInfo;

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t rowSize, ExpiredFun fp, void* pFile) {
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
  pFileState->expFunc = fp;
  pFileState->maxRowCount = memSize / rowSize;
  pFileState->curRowCount = 0;
  return pFileState;

_error:
  destroyStreamFileState(pFileState);
  return NULL;
}

void destroyRowBuffPos(SRowBuffPos* pPos) {
  taosMemoryFreeClear(pPos->pRowBuff);
  taosMemoryFree(pPos);
}

void destroyRowBuffPosPtr(void* ptr) {
  if (!ptr) {
    return;
  }
  void* tmp = *(void**)ptr;
  SRowBuffPos* pPos = (SRowBuffPos*)tmp;
  destroyRowBuffPos(pPos);
}

void destroyStreamFileState(SStreamFileState* pFileState) {
  tdListFreeP(pFileState->usedBuffs, destroyRowBuffPosPtr);
  tdListFreeP(pFileState->freeBuffs, taosMemoryFree);
  tSimpleHashCleanup(pFileState->rowBuffMap);
}

void clearExpiredRowBuff(SStreamFileState* pFileState, TSKEY ts) {
  SListIter iter = {0};
  tdListInitIter(pFileState->usedBuffs, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    if (pFileState->expFunc(pPos->pKey, ts)) {
      tdListAppend(pFileState->freeBuffs, &pPos->pRowBuff);
      pPos->pRowBuff = NULL;
      destroyRowBuffPos(pPos);
    }
  }
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
      i++;
    }
  }
  flushSnapshot(pFileState->pFileStore, pFlushList, pFileState->rowSize);
  return TSDB_CODE_SUCCESS;
}

int32_t clearRowBuff(SStreamFileState* pFileState) {
  clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark);
  if (isListEmpty(pFileState->freeBuffs)) {
    return flushRowBuff(pFileState);
  }
  return TSDB_CODE_SUCCESS;
}

void* getFreeBuff(SList* lists) {
  SListNode* pNode = tdListPopHead(lists);
  if (!pNode) {
    return NULL;
  }
  void* ptr = *(void**)pNode->data;
  taosMemoryFree(pNode);
  return ptr;
}

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState) {
  SRowBuffPos* pPos = taosMemoryCalloc(1, sizeof(SRowBuffPos));
  tdListAppend(pFileState->usedBuffs, &pPos);
  void* pBuff = getFreeBuff(pFileState->freeBuffs);
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
  pPos->pRowBuff = getFreeBuff(pFileState->freeBuffs);
  return pPos;
}

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen) {
  SRowBuffPos** pos = tSimpleHashGet(pFileState->rowBuffMap, pKey, keyLen);
  if (pos) {
    *pVLen = pFileState->rowSize;
    *pVal = *pos;
    return TSDB_CODE_SUCCESS;
  }
  SRowBuffPos* pNewPos = getNewRowPos(pFileState);
  ASSERT(pNewPos);// todo(liuyao) delete
  tSimpleHashPut(pFileState->rowBuffMap, pKey, keyLen, &pNewPos, POINTER_BYTES);
  *pVLen = pFileState->rowSize;
  *pVal = pNewPos;
  return TSDB_CODE_SUCCESS;
}

void* getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  if (pPos->pRowBuff) {
    return pPos->pRowBuff;
  }

  int32_t code = clearRowBuff(pFileState);
  ASSERT(code == 0);
  pPos->pRowBuff = getFreeBuff(pFileState->freeBuffs);

  void* pVal = NULL;
  int32_t len = 0;
  streamStateGet_rocksdb(pFileState->pFileStore, pPos->pKey, &pVal, &len);
  memcpy(pPos->pRowBuff, pVal, len);
  taosMemoryFree(pVal);
  return pPos->pRowBuff;
}

void releaseRowBuffPos(SRowBuffPos* pBuff) {
  pBuff->beUsed = false;
}

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState) {
  clearExpiredRowBuff(pFileState, pFileState->maxTs - pFileState->deleteMark);
  return pFileState->usedBuffs;
}

int32_t flushSnapshot(void* pFile, SStreamSnapshot* pSnapshot, int32_t rowSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  SListIter iter = {0};
  tdListInitIter(pSnapshot, &iter, TD_LIST_FORWARD);

  SListNode* pNode = NULL;
  while ((pNode = tdListNext(&iter)) != NULL && code == TSDB_CODE_SUCCESS) {
    SRowBuffPos* pPos = *(SRowBuffPos**)pNode->data;
    code = streamStatePut_rocksdb(pFile, pPos->pKey, pPos->pRowBuff, rowSize);
  }
  return code;
}

int32_t recoverSnapshot(SStreamFileState* pFileState) {
  // 设置一个时间戳标记，小于这个时间戳的，如果缓存里没有，需要从rocks db里读取状态，大于这个时间戳的，不需要
  // 这个还需要考虑一下，如果rocks db中也没有，说明真的是新的，那么这次读取是冗余的。
  return TSDB_CODE_SUCCESS;
}