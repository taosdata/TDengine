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
#include "tcommon.h"
#include "tsimplehash.h"

typedef int (*__session_compare_fn_t)(const SSessionKey* pWin, const void* pDatas, int pos);

int sessionStateKeyCompare(const SSessionKey* pWin1, const void* pDatas, int pos) {
  SRowBuffPos* pPos2 = taosArrayGetP(pDatas, pos);
  SSessionKey* pWin2 = (SSessionKey*)pPos2->pKey;
  return sessionWinKeyCmpr(pWin1, pWin2);
}

int sessionStateRangeKeyCompare(const SSessionKey* pWin1, const void* pDatas, int pos) {
  SRowBuffPos* pPos2 = taosArrayGetP(pDatas, pos);
  SSessionKey* pWin2 = (SSessionKey*)pPos2->pKey;
  return sessionRangeKeyCmpr(pWin1, pWin2);
}

int32_t binarySearch(void* keyList, int num, const void* key, __session_compare_fn_t cmpFn) {
  int firstPos = 0, lastPos = num - 1, midPos = -1;
  int numOfRows = 0;

  if (num <= 0) return -1;
  // find the first position which is smaller or equal than the key.
  // if all data is bigger than the key return -1
  while (1) {
    if (cmpFn(key, keyList, lastPos) >= 0) return lastPos;
    if (cmpFn(key, keyList, firstPos) == 0) return firstPos;
    if (cmpFn(key, keyList, firstPos) < 0) return firstPos - 1;

    numOfRows = lastPos - firstPos + 1;
    midPos = (numOfRows >> 1) + firstPos;

    if (cmpFn(key, keyList, midPos) < 0) {
      lastPos = midPos - 1;
    } else if (cmpFn(key, keyList, midPos) > 0) {
      firstPos = midPos + 1;
    } else {
      break;
    }
  }

  return midPos;
}

int64_t getSessionWindowEndkey(void* data, int32_t index) {
  SArray*       pWinInfos = (SArray*)data;
  SRowBuffPos** ppos = taosArrayGet(pWinInfos, index);
  SSessionKey*  pWin = (SSessionKey*)((*ppos)->pKey);
  return pWin->win.ekey;
}

bool inSessionWindow(SSessionKey* pKey, TSKEY ts, int64_t gap) {
  if (ts + gap >= pKey->win.skey && ts - gap <= pKey->win.ekey) {
    return true;
  }
  return false;
}

SStreamStateCur* createSessionStateCursor(SStreamFileState* pFileState) {
  SStreamStateCur* pCur = createStreamStateCursor();
  pCur->pStreamFileState = pFileState;
  return pCur;
}

static SRowBuffPos* addNewSessionWindow(SStreamFileState* pFileState, SArray* pWinInfos, const SSessionKey* pKey) {
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  ASSERT(pNewPos->pRowBuff);
  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  taosArrayPush(pWinInfos, &pNewPos);
  return pNewPos;
}

static SRowBuffPos* insertNewSessionWindow(SStreamFileState* pFileState, SArray* pWinInfos, const SSessionKey* pKey,
                                           int32_t index) {
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  ASSERT(pNewPos->pRowBuff);
  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  taosArrayInsert(pWinInfos, index, &pNewPos);
  return pNewPos;
}

SRowBuffPos* createSessionWinBuff(SStreamFileState* pFileState, SSessionKey* pKey, void* p, int32_t* pVLen) {
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  pNewPos->needFree = true;
  pNewPos->beFlushed = true;
  if(p) {
    memcpy(pNewPos->pRowBuff, p, *pVLen);
  } else {
    int32_t len = getRowStateRowSize(pFileState);
    memset(pNewPos->pRowBuff, 0, len);
  }
  taosMemoryFree(p);
  return pNewPos;
}

int32_t getSessionWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, TSKEY gap, void** pVal,
                                int32_t* pVLen) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SSHashObj* pSessionBuff = getRowStateBuff(pFileState);
  SArray*    pWinStates = NULL;
  void**     ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  TSKEY startTs = pKey->win.skey;
  TSKEY endTs = pKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void*   pFileStore = getStateFileStore(pFileState);
    void*   p = NULL;
    int32_t code_file = streamStateSessionAddIfNotExist_rocksdb(pFileStore, pKey, gap, &p, pVLen);
    if (code_file == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
      (*pVal) = createSessionWinBuff(pFileState, pKey, p, pVLen);
      code = code_file;
      qDebug("===stream===0 get session win:%" PRId64 ",%" PRId64 " from disc, res %d", startTs, endTs, code_file);
    } else {
      (*pVal) = addNewSessionWindow(pFileState, pWinStates, pKey);
      code = TSDB_CODE_FAILED;
      taosMemoryFree(p);
    }
    goto _end;
  }

  // find the first position which is smaller than the pKey
  int32_t      index = binarySearch(pWinStates, size, pKey, sessionStateKeyCompare);
  SRowBuffPos* pPos = NULL;

  if (index >= 0) {
    pPos = taosArrayGetP(pWinStates, index);
    if (inSessionWindow(pPos->pKey, startTs, gap)) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      *pKey = *pDestWinKey;
      goto _end;
    }
  }

  if (index + 1 < size) {
    pPos = taosArrayGetP(pWinStates, index + 1);
    if (inSessionWindow(pPos->pKey, startTs, gap) || (endTs != INT64_MIN && inSessionWindow(pPos->pKey, endTs, gap))) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      *pKey = *pDestWinKey;
      goto _end;
    }
  }

  if (index + 1 == 0) {
    if (!isDeteled(pFileState, endTs) && isFlushedState(pFileState, endTs, gap)) {
      void*   p = NULL;
      void*   pFileStore = getStateFileStore(pFileState);
      int32_t code_file = streamStateSessionAddIfNotExist_rocksdb(pFileStore, pKey, gap, &p, pVLen);
      if (code_file == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
        (*pVal) = createSessionWinBuff(pFileState, pKey, p, pVLen);
        code = code_file;
        qDebug("===stream===1 get session win:%" PRId64 ",%" PRId64 " from disc, res %d", startTs, endTs, code_file);
        goto _end;
      } else {
        taosMemoryFree(p);
      }
    }
  }

  if (index == size - 1) {
    (*pVal) = addNewSessionWindow(pFileState, pWinStates, pKey);
    code = TSDB_CODE_FAILED;
    goto _end;
  }
  (*pVal) = insertNewSessionWindow(pFileState, pWinStates, pKey, index + 1);
  code = TSDB_CODE_FAILED;

_end:
  return code;
}

int32_t putSessionWinResultBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SSessionKey* pKey = pPos->pKey;
  SArray*      pWinStates = NULL;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    taosArrayPush(pWinStates, &pPos);
    goto _end;
  }

  // find the first position which is smaller than the pKey
  int32_t index = binarySearch(pWinStates, size, pKey, sessionStateKeyCompare);
  if (index >= 0) {
    taosArrayInsert(pWinStates, index, &pPos);
  } else {
    taosArrayInsert(pWinStates, 0, &pPos);
  }

_end:
  pPos->needFree = false;
  return TSDB_CODE_SUCCESS;
}

int32_t getSessionFlushedBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  pNewPos->needFree = true;
  pNewPos->beFlushed = true;
  void*   pBuff = NULL;
  int32_t code = streamStateSessionGet_rocksdb(getStateFileStore(pFileState), pKey, &pBuff, pVLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  memcpy(pNewPos->pRowBuff, pBuff, *pVLen);
  taosMemoryFreeClear(pBuff);
  (*pVal) = pNewPos;
  return TSDB_CODE_SUCCESS;
}

int32_t deleteSessionWinStateBuffFn(void* pBuff, const void* key, size_t keyLen) {
  SSHashObj*   pSessionBuff = (SSHashObj*)pBuff;
  SSessionKey* pWinKey = (SSessionKey*)key;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return TSDB_CODE_SUCCESS;
  }
  SArray* pWinStates = (SArray*)(*ppBuff);
  int32_t size = taosArrayGetSize(pWinStates);
  TSKEY   gap = 0;
  int32_t index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);
  if (index >= 0) {
    SRowBuffPos* pPos = taosArrayGetP(pWinStates, index);
    if (inSessionWindow(pPos->pKey, pWinKey->win.skey, gap)) {
      pPos->beFlushed = true;
      taosArrayRemove(pWinStates, index);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t deleteSessionWinStateBuffByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SSessionKey* pWinKey = (SSessionKey*)pPos->pKey;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return TSDB_CODE_SUCCESS;
  }
  SArray* pWinStates = (SArray*)(*ppBuff);
  int32_t size = taosArrayGetSize(pWinStates);
  TSKEY   gap = 0;
  int32_t index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);
  if (index >= 0) {
    SRowBuffPos* pItemPos = taosArrayGetP(pWinStates, index);
    if (pItemPos == pPos) {
      taosArrayRemove(pWinStates, index);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t allocSessioncWinBuffByNextPosition(SStreamFileState* pFileState, SStreamStateCur* pCur,
                                           const SSessionKey* pWinKey, void** ppVal, int32_t* pVLen) {
  SRowBuffPos* pNewPos = NULL;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  SArray*      pWinStates = NULL;
  if (!ppBuff) {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  } else {
    pWinStates = (SArray*)(*ppBuff);
  }
  if (!pCur) {
    pNewPos = addNewSessionWindow(pFileState, pWinStates, pWinKey);
    goto _end;
  }

  int32_t size = taosArrayGetSize(pWinStates);
  if (pCur->buffIndex >= 0) {
    if (pCur->buffIndex >= size) {
      pNewPos = insertNewSessionWindow(pFileState, pWinStates, pWinKey, size);
      goto _end;
    }
    pNewPos = insertNewSessionWindow(pFileState, pWinStates, pWinKey, pCur->buffIndex);
    goto _end;
  } else {
    if (size > 0) {
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, 0);
      if (sessionWinKeyCmpr(pWinKey, pPos->pKey) >= 0) {
        // pCur is invalid
        SSessionKey pTmpKey = *pWinKey;
        int32_t     code = getSessionWinResultBuff(pFileState, &pTmpKey, 0, (void**)&pNewPos, pVLen);
        ASSERT(code == TSDB_CODE_FAILED);
        goto _end;
      }
    }
    pNewPos = getNewRowPosForWrite(pFileState);
    pNewPos->needFree = true;
    pNewPos->beFlushed = true;
  }

_end:
  memcpy(pNewPos->pKey, pWinKey, sizeof(SSessionKey));
  (*ppVal) = pNewPos;
  return TSDB_CODE_SUCCESS;
}

void sessionWinStateClear(SStreamFileState* pFileState) {
  int32_t buffSize = getRowStateRowSize(pFileState);
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  void*   pBuff = getRowStateBuff(pFileState);
  while ((pIte = tSimpleHashIterate(pBuff, pIte, &iter)) != NULL) {
    SArray* pWinStates = *((void**)pIte);
    int32_t size = taosArrayGetSize(pWinStates);
    for (int32_t i = 0; i < size; i++) {
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, i);
      memset(pPos->pRowBuff, 0, buffSize);
    }
  }
}

void sessionWinStateCleanup(void* pBuff) {
  void*   pIte = NULL;
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pBuff, pIte, &iter)) != NULL) {
    SArray* pWinStates = (SArray*)(*(void**)pIte);
    taosArrayDestroy(pWinStates);
  }
  tSimpleHashCleanup(pBuff);
}

static SStreamStateCur* seekKeyCurrentPrev_buff(SStreamFileState* pFileState, const SSessionKey* pWinKey,
                                                SArray** pWins, int32_t* pIndex) {
  SStreamStateCur* pCur = NULL;
  SSHashObj*       pSessionBuff = getRowStateBuff(pFileState);
  void**           ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return NULL;
  }

  SArray* pWinStates = (SArray*)(*ppBuff);
  int32_t size = taosArrayGetSize(pWinStates);
  TSKEY   gap = 0;
  int32_t index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);

  if (pWins) {
    (*pWins) = pWinStates;
  }

  if (index >= 0) {
    pCur = createSessionStateCursor(pFileState);
    pCur->buffIndex = index;
    if (pIndex) {
      *pIndex = index;
    }
  }
  return pCur;
}

SStreamStateCur* sessionWinStateSeekKeyCurrentPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey) {
  SStreamStateCur* pCur = seekKeyCurrentPrev_buff(pFileState, pWinKey, NULL, NULL);
  if (pCur) {
    return pCur;
  }

  void* pFileStore = getStateFileStore(pFileState);
  pCur = streamStateSessionSeekKeyCurrentPrev_rocksdb(pFileStore, pWinKey);
  if (!pCur) {
    return NULL;
  }
  pCur->buffIndex = -1;
  pCur->pStreamFileState = pFileState;
  return pCur;
}
static void transformCursor(SStreamFileState* pFileState, SStreamStateCur* pCur) {
  if (!pCur) {
    return;
  }
  streamStateResetCur(pCur);
  pCur->buffIndex = 0;
  pCur->pStreamFileState = pFileState;
}

static void checkAndTransformCursor(SStreamFileState* pFileState, const uint64_t groupId, SArray* pWinStates,
                                    SStreamStateCur** ppCur) {
  SSessionKey key = {.groupId = groupId};
  int32_t     code = streamStateSessionGetKVByCur_rocksdb(*ppCur, &key, NULL, NULL);
  if (taosArrayGetSize(pWinStates) > 0 &&
      (code == TSDB_CODE_FAILED || sessionStateKeyCompare(&key, pWinStates, 0) >= 0)) {
    if (!(*ppCur)) {
      (*ppCur) = createSessionStateCursor(pFileState);
    }
    transformCursor(pFileState, *ppCur);
  } else if (*ppCur) {
    (*ppCur)->buffIndex = -1;
    (*ppCur)->pStreamFileState = pFileState;
  }
}

SStreamStateCur* sessionWinStateSeekKeyCurrentNext(SStreamFileState* pFileState, const SSessionKey* pWinKey) {
  SArray*          pWinStates = NULL;
  int32_t          index = -1;
  SStreamStateCur* pCur = seekKeyCurrentPrev_buff(pFileState, pWinKey, &pWinStates, &index);
  if (pCur) {
    if (sessionStateRangeKeyCompare(pWinKey, pWinStates, index) > 0) {
      sessionWinStateMoveToNext(pCur);
    }
    return pCur;
  }

  void* pFileStore = getStateFileStore(pFileState);
  pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pFileStore, (SSessionKey*)pWinKey);
  checkAndTransformCursor(pFileState, pWinKey->groupId, pWinStates, &pCur);
  return pCur;
}

SStreamStateCur* sessionWinStateSeekKeyNext(SStreamFileState* pFileState, const SSessionKey* pWinKey) {
  SArray*          pWinStates = NULL;
  int32_t          index = -1;
  SStreamStateCur* pCur = seekKeyCurrentPrev_buff(pFileState, pWinKey, &pWinStates, &index);
  if (pCur) {
    sessionWinStateMoveToNext(pCur);
    return pCur;
  }

  void* pFileStore = getStateFileStore(pFileState);
  pCur = streamStateSessionSeekKeyNext_rocksdb(pFileStore, pWinKey);
  checkAndTransformCursor(pFileState, pWinKey->groupId, pWinStates, &pCur);
  return pCur;
}

SStreamStateCur* countWinStateSeekKeyPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey, COUNT_TYPE count) {
  SArray*          pWinStates = NULL;
  int32_t          index = -1;
  SStreamStateCur* pBuffCur = seekKeyCurrentPrev_buff(pFileState, pWinKey, &pWinStates, &index);
  int32_t resSize = getRowStateRowSize(pFileState);
  COUNT_TYPE winCount = 0;
  if (pBuffCur) {
    while (index >= 0) {
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, index);
      winCount = *((COUNT_TYPE*) ((char*)pPos->pRowBuff + (resSize - sizeof(COUNT_TYPE))));
      if (sessionStateRangeKeyCompare(pWinKey, pWinStates, index) == 0 || winCount < count) {
        index--;
      } else if (index >= 0) {
        pBuffCur->buffIndex = index + 1;
        return pBuffCur;
      }
    }
    pBuffCur->buffIndex = 0;
  } else if (taosArrayGetSize(pWinStates) > 0) {
    pBuffCur = createSessionStateCursor(pFileState);
    pBuffCur->buffIndex = 0;
  }

  void* pFileStore = getStateFileStore(pFileState);
  SStreamStateCur* pCur = streamStateSessionSeekKeyPrev_rocksdb(pFileStore, pWinKey);
  if (pCur) {
    pCur->pStreamFileState = pFileState;
    SSessionKey key = {0};
    void* pVal = NULL;
    int len = 0;
    int32_t code = streamStateSessionGetKVByCur_rocksdb(pCur, &key, &pVal, &len);
    if (code == TSDB_CODE_FAILED) {
      streamStateFreeCur(pCur);
      return pBuffCur;
    }
    winCount = *((COUNT_TYPE*) ((char*)pVal + (resSize - sizeof(COUNT_TYPE))));
    if (sessionRangeKeyCmpr(pWinKey, &key) != 0 && winCount == count) {
      streamStateFreeCur(pCur);
      return pBuffCur;
    }
    streamStateCurPrev(pFileStore, pCur);
    while (1) {
      code = streamStateSessionGetKVByCur_rocksdb(pCur, &key, &pVal, &len);
      if (code == TSDB_CODE_FAILED) {
        streamStateCurNext(pFileStore, pCur);
        streamStateFreeCur(pBuffCur);
        return pCur;
      }
      winCount = *((COUNT_TYPE*) ((char*)pVal + (resSize - sizeof(COUNT_TYPE))));
      if (sessionRangeKeyCmpr(pWinKey, &key) == 0 || winCount < count) {
        streamStateCurPrev(pFileStore, pCur);
      } else {
        streamStateCurNext(pFileStore, pCur);
        streamStateFreeCur(pBuffCur);
        return pCur;
      }
    }
  }
  return pBuffCur;
}

int32_t sessionWinStateGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }
  int32_t code = TSDB_CODE_SUCCESS;

  SSHashObj* pSessionBuff = getRowStateBuff(pCur->pStreamFileState);
  void**     ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  SArray*    pWinStates = NULL;
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  }

  if (pCur->buffIndex >= 0) {
    int32_t size = taosArrayGetSize(pWinStates);
    if (pCur->buffIndex >= size) {
      return TSDB_CODE_FAILED;
    }
    SRowBuffPos* pPos = taosArrayGetP(pWinStates, pCur->buffIndex);
    if (pVal) {
      *pVal = pPos;
    }
    *pKey = *(SSessionKey*)(pPos->pKey);
  } else {
    void* pData = NULL;
    code = streamStateSessionGetKVByCur_rocksdb(pCur, pKey, &pData, pVLen);
    if (taosArrayGetSize(pWinStates) > 0 &&
        (code == TSDB_CODE_FAILED || sessionStateKeyCompare(pKey, pWinStates, 0) >= 0)) {
      transformCursor(pCur->pStreamFileState, pCur);
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, pCur->buffIndex);
      if (pVal) {
        *pVal = pPos;
      }
      *pKey = *(SSessionKey*)(pPos->pKey);
      code = TSDB_CODE_SUCCESS;
    } else if (code == TSDB_CODE_SUCCESS && pVal) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pCur->pStreamFileState);
      memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
      pNewPos->needFree = true;
      pNewPos->beFlushed = true;
      memcpy(pNewPos->pRowBuff, pData, *pVLen);
      (*pVal) = pNewPos;
    }
    taosMemoryFreeClear(pData);
  }
  return code;
}

int32_t sessionWinStateMoveToNext(SStreamStateCur* pCur) {
  if (pCur && pCur->buffIndex >= 0) {
    pCur->buffIndex++;
  } else {
    streamStateCurNext_rocksdb(NULL, pCur);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sessionWinStateGetKeyByRange(SStreamFileState* pFileState, const SSessionKey* key, SSessionKey* curKey, range_cmpr_fn cmpFn) {
  SStreamStateCur* pCur = sessionWinStateSeekKeyCurrentPrev(pFileState, key);
  SSessionKey      tmpKey = *key;
  int32_t          code = sessionWinStateGetKVByCur(pCur, &tmpKey, NULL, NULL);
  bool             hasCurrentPrev = true;
  if (code == TSDB_CODE_FAILED) {
    streamStateFreeCur(pCur);
    pCur = sessionWinStateSeekKeyNext(pFileState, key);
    code = sessionWinStateGetKVByCur(pCur, &tmpKey, NULL, NULL);
    hasCurrentPrev = false;
  }

  if (code == TSDB_CODE_FAILED) {
    code = TSDB_CODE_FAILED;
    goto _end;
  }

  if (cmpFn(key, &tmpKey) == 0) {
    *curKey = tmpKey;
    goto _end;
  } else if (!hasCurrentPrev) {
    code = TSDB_CODE_FAILED;
    goto _end;
  }

  sessionWinStateMoveToNext(pCur);
  code = sessionWinStateGetKVByCur(pCur, &tmpKey, NULL, NULL);
  if (code == TSDB_CODE_SUCCESS && cmpFn(key, &tmpKey) == 0) {
    *curKey = tmpKey;
  } else {
    code = TSDB_CODE_FAILED;
  }

_end:
  streamStateFreeCur(pCur);
  return code;
}

int32_t getStateWinResultBuff(SStreamFileState* pFileState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                              state_key_cmpr_fn fn, void** pVal, int32_t* pVLen) {
  SSessionKey* pWinKey = key;
  TSKEY        gap = 0;
  int32_t      code = TSDB_CODE_SUCCESS;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SArray*      pWinStates = NULL;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  TSKEY startTs = pWinKey->win.skey;
  TSKEY endTs = pWinKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void*   pFileStore = getStateFileStore(pFileState);
    void*   p = NULL;
    int32_t code_file = streamStateStateAddIfNotExist_rocksdb(pFileStore, pWinKey, pKeyData, keyDataLen, fn, &p, pVLen);
    if (code_file == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
      (*pVal) = createSessionWinBuff(pFileState, pWinKey, p, pVLen);
      code = code_file;
      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
             pWinKey->win.ekey, code_file);
    } else {
      (*pVal) = addNewSessionWindow(pFileState, pWinStates, key);
      code = TSDB_CODE_FAILED;
      taosMemoryFree(p);
    }
    goto _end;
  }

  // find the first position which is smaller than the pWinKey
  int32_t      index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);
  SRowBuffPos* pPos = NULL;
  int32_t      valSize = *pVLen;

  if (index >= 0) {
    pPos = taosArrayGetP(pWinStates, index);
    void* stateKey = (char*)(pPos->pRowBuff) + (valSize - keyDataLen);
    if (inSessionWindow(pPos->pKey, startTs, gap) || fn(pKeyData, stateKey) == true) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      *key = *pDestWinKey;
      goto _end;
    }
  }

  if (index + 1 < size) {
    pPos = taosArrayGetP(pWinStates, index + 1);
    void* stateKey = (char*)(pPos->pRowBuff) + (valSize - keyDataLen);
    if (inSessionWindow(pPos->pKey, startTs, gap) || (endTs != INT64_MIN && inSessionWindow(pPos->pKey, endTs, gap)) ||
        fn(pKeyData, stateKey) == true) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      *key = *pDestWinKey;
      goto _end;
    }
  }

  if (index + 1 == 0) {
    if (!isDeteled(pFileState, endTs)) {
      void*   p = NULL;
      void*   pFileStore = getStateFileStore(pFileState);
      int32_t code_file =
          streamStateStateAddIfNotExist_rocksdb(pFileStore, pWinKey, pKeyData, keyDataLen, fn, &p, pVLen);
      if (code_file == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
        (*pVal) = createSessionWinBuff(pFileState, pWinKey, p, pVLen);
        code = code_file;
        qDebug("===stream===1 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
               pWinKey->win.ekey, code_file);
        goto _end;
      } else {
        taosMemoryFree(p);
      }
    }
  }

  if (index == size - 1) {
    (*pVal) = addNewSessionWindow(pFileState, pWinStates, key);
    code = TSDB_CODE_FAILED;
    goto _end;
  }
  (*pVal) = insertNewSessionWindow(pFileState, pWinStates, key, index + 1);
  code = TSDB_CODE_FAILED;

_end:
  return code;
}

int32_t getCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal, int32_t* pVLen) {
  SSessionKey* pWinKey = pKey;
  const TSKEY gap = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  SSHashObj* pSessionBuff = getRowStateBuff(pFileState);
  SArray* pWinStates = NULL;
  void** ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  TSKEY startTs = pWinKey->win.skey;
  TSKEY endTs = pWinKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void* pFileStore = getStateFileStore(pFileState);
    void* pRockVal = NULL;
    SStreamStateCur* pCur = streamStateSessionSeekToLast_rocksdb(pFileStore, pKey->groupId);
    code = streamStateSessionGetKVByCur_rocksdb(pCur, pWinKey, &pRockVal, pVLen);
    streamStateFreeCur(pCur);
    if (code == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey, pWinKey->win.ekey, code);
      if (code == TSDB_CODE_SUCCESS) {
        int32_t     valSize = *pVLen;
        COUNT_TYPE* pWinStateCout = (COUNT_TYPE*)( (char*)(pRockVal) + (valSize - sizeof(COUNT_TYPE)) );
        if (inSessionWindow(pWinKey, startTs, gap) || (*pWinStateCout) < winCount) {
          (*pVal) = createSessionWinBuff(pFileState, pWinKey, pRockVal, pVLen);
          goto _end;
        }
      }
      pWinKey->win.skey = startTs;
      pWinKey->win.ekey = endTs;
      (*pVal) = createSessionWinBuff(pFileState, pWinKey, NULL, NULL);
      taosMemoryFree(pRockVal);
    } else {
      (*pVal) = addNewSessionWindow(pFileState, pWinStates, pWinKey);
      code = TSDB_CODE_FAILED;
    }
    goto _end;
  }

  // find the first position which is smaller than the pWinKey
  int32_t      index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);
  SRowBuffPos* pPos = NULL;
  int32_t      valSize = *pVLen;

  if (index >= 0) {
    pPos = taosArrayGetP(pWinStates, index);
    COUNT_TYPE* pWinStateCout = (COUNT_TYPE*)( (char*)(pPos->pRowBuff) + (valSize - sizeof(COUNT_TYPE)) );
    if (inSessionWindow(pPos->pKey, startTs, gap) || (index == size - 1 && (*pWinStateCout) < winCount) ) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      *pWinKey = *pDestWinKey;
      goto _end;
    }
  }

  if (index == -1) {
    if (!isDeteled(pFileState, endTs)) {
      void*   p = NULL;
      void*   pFileStore = getStateFileStore(pFileState);
      SStreamStateCur* pCur = streamStateSessionSeekToLast_rocksdb(pFileStore, pKey->groupId);
      int32_t code_file = streamStateSessionGetKVByCur_rocksdb(pCur, pWinKey, &p, pVLen);
      if (code_file == TSDB_CODE_SUCCESS) {
        (*pVal) = createSessionWinBuff(pFileState, pWinKey, p, pVLen);
        code = code_file;
        qDebug("===stream===1 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey, pWinKey->win.ekey, code_file);
        streamStateFreeCur(pCur);
        goto _end;
      }
      taosMemoryFree(p);
      streamStateFreeCur(pCur);
    }
  }

  if (index + 1 < size) {
    pPos = taosArrayGetP(pWinStates, index + 1);
    (*pVal) = pPos;
    SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
    pPos->beUsed = true;
    *pWinKey = *pDestWinKey;
    goto _end;
  }

  (*pVal) = addNewSessionWindow(pFileState, pWinStates, pWinKey);
  code = TSDB_CODE_FAILED;

_end:
  return code;
}

int32_t createCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  SSessionKey* pWinKey = pKey;
  const TSKEY gap = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  SSHashObj* pSessionBuff = getRowStateBuff(pFileState);
  SArray* pWinStates = NULL;
  void** ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  TSKEY startTs = pWinKey->win.skey;
  TSKEY endTs = pWinKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void*   pFileStore = getStateFileStore(pFileState);
    void*   p = NULL;

    SStreamStateCur* pCur = streamStateSessionSeekToLast_rocksdb(pFileStore, pKey->groupId);
    int32_t code_file = streamStateSessionGetKVByCur_rocksdb(pCur, pWinKey, &p, pVLen);
    if (code_file == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
      (*pVal) = createSessionWinBuff(pFileState, pWinKey, p, pVLen);
      code = code_file;
      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey, pWinKey->win.ekey, code_file);
    } else {
      (*pVal) = addNewSessionWindow(pFileState, pWinStates, pWinKey);
      code = TSDB_CODE_FAILED;
      taosMemoryFree(p);
    }
    streamStateFreeCur(pCur);
    goto _end;
  } else {
    (*pVal) = addNewSessionWindow(pFileState, pWinStates, pWinKey);
  }

_end:
  return code;
}
