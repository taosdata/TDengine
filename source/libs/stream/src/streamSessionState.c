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
  if (ppos != NULL) {
    SSessionKey* pWin = (SSessionKey*)((*ppos)->pKey);
    return pWin->win.ekey;
  } else {
    return 0;
  }
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

static int32_t addNewSessionWindow(SStreamFileState* pFileState, SArray* pWinInfos, const SSessionKey* pKey,
                                   SRowBuffPos** ppPos) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  if (!pNewPos || !pNewPos->pRowBuff) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  void* tmp = taosArrayPush(pWinInfos, &pNewPos);
  if (!tmp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  (*ppPos) = pNewPos;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t insertNewSessionWindow(SStreamFileState* pFileState, SArray* pWinInfos, const SSessionKey* pKey,
                                      int32_t index, SRowBuffPos** ppPos) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  if (!pNewPos || !pNewPos->pRowBuff) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  void* tmp = taosArrayInsert(pWinInfos, index, &pNewPos);
  if (!tmp) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  *ppPos = pNewPos;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

SRowBuffPos* createSessionWinBuff(SStreamFileState* pFileState, SSessionKey* pKey, void* p, int32_t* pVLen) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  if (!pNewPos || !pNewPos->pRowBuff) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  pNewPos->needFree = true;
  pNewPos->beFlushed = true;
  if (p) {
    memcpy(pNewPos->pRowBuff, p, *pVLen);
  } else {
    int32_t len = getRowStateRowSize(pFileState);
    memset(pNewPos->pRowBuff, 0, len);
  }

_end:
  taosMemoryFree(p);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return NULL;
  }
  return pNewPos;
}

int32_t getSessionWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, TSKEY gap, void** pVal, int32_t* pVLen,
                                int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  (*pWinCode) = TSDB_CODE_SUCCESS;
  SSHashObj* pSessionBuff = getRowStateBuff(pFileState);
  SArray*    pWinStates = NULL;
  void**     ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    if (!pWinStates) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = tSimpleHashPut(pSessionBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
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
      if (!(*pVal)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      (*pWinCode) = code_file;
      qDebug("===stream===0 get session win:%" PRId64 ",%" PRId64 " from disc, res %d", startTs, endTs, code_file);
    } else {
      code = addNewSessionWindow(pFileState, pWinStates, pKey, (SRowBuffPos**)pVal);
      (*pWinCode) = TSDB_CODE_FAILED;
      taosMemoryFree(p);
      QUERY_CHECK_CODE(code, lino, _end);
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
      pPos->beFlushed = false;
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
      pPos->beFlushed = false;
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
        if (!(*pVal)) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          QUERY_CHECK_CODE(code, lino, _end);
        }

        (*pWinCode) = code_file;
        qDebug("===stream===1 get session win:%" PRId64 ",%" PRId64 " from disc, res %d", startTs, endTs, code_file);
        goto _end;
      } else {
        taosMemoryFree(p);
      }
    }
  }

  if (index == size - 1) {
    code = addNewSessionWindow(pFileState, pWinStates, pKey, (SRowBuffPos**)pVal);
    QUERY_CHECK_CODE(code, lino, _end);

    (*pWinCode) = TSDB_CODE_FAILED;
    goto _end;
  }

  code = insertNewSessionWindow(pFileState, pWinStates, pKey, index + 1, (SRowBuffPos**)pVal);
  QUERY_CHECK_CODE(code, lino, _end);

  (*pWinCode) = TSDB_CODE_FAILED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getSessionRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                          int32_t* pWinCode) {
  SWinKey* pTmpkey = pKey;
  SSessionKey pWinKey = {.groupId = pTmpkey->groupId, .win.skey = pTmpkey->ts, .win.ekey = pTmpkey->ts};
  return getSessionWinResultBuff(pFileState, &pWinKey, 0, pVal, pVLen, pWinCode);
}

int32_t putSessionWinResultBuff(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SSessionKey* pKey = pPos->pKey;
  SArray*      pWinStates = NULL;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    if (!pWinStates) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = tSimpleHashPut(pSessionBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void* tmp = taosArrayPush(pWinStates, &pPos);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }

  // find the first position which is smaller than the pKey
  int32_t index = binarySearch(pWinStates, size, pKey, sessionStateKeyCompare);
  if (index >= 0) {
    void* tmp = taosArrayInsert(pWinStates, index, &pPos);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    void* tmp = taosArrayInsert(pWinStates, 0, &pPos);
    if (!tmp) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  pPos->needFree = false;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getSessionFlushedBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
  if (!pNewPos || !pNewPos->pRowBuff) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pNewPos->needFree = true;
  pNewPos->beFlushed = true;
  void*   pBuff = NULL;
  (*pWinCode) = streamStateSessionGet_rocksdb(getStateFileStore(pFileState), pKey, &pBuff, pVLen);
  if ((*pWinCode) != TSDB_CODE_SUCCESS) {
    goto _end;
  }
  memcpy(pNewPos->pKey, pKey, sizeof(SSessionKey));
  memcpy(pNewPos->pRowBuff, pBuff, *pVLen);
  taosMemoryFreeClear(pBuff);
  (*pVal) = pNewPos;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

void deleteSessionWinStateBuffByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SSessionKey* pWinKey = (SSessionKey*)pPos->pKey;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return;
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
}

int32_t allocSessioncWinBuffByNextPosition(SStreamFileState* pFileState, SStreamStateCur* pCur,
                                           const SSessionKey* pWinKey, void** ppVal, int32_t* pVLen) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pNewPos = NULL;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  SArray*      pWinStates = NULL;
  if (!ppBuff) {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    if (!pWinStates) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    pWinStates = (SArray*)(*ppBuff);
  }
  if (!pCur) {
    code = addNewSessionWindow(pFileState, pWinStates, pWinKey, &pNewPos);
    QUERY_CHECK_CODE(code, lino, _end);

    goto _end;
  }

  int32_t size = taosArrayGetSize(pWinStates);
  if (pCur->buffIndex >= 0) {
    if (pCur->buffIndex >= size) {
      code = addNewSessionWindow(pFileState, pWinStates, pWinKey, &pNewPos);
      QUERY_CHECK_CODE(code, lino, _end);

      goto _end;
    }
    code = insertNewSessionWindow(pFileState, pWinStates, pWinKey, pCur->buffIndex, &pNewPos);
    QUERY_CHECK_CODE(code, lino, _end);

    goto _end;
  } else {
    if (size > 0) {
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, 0);
      if (sessionWinKeyCmpr(pWinKey, pPos->pKey) >= 0) {
        // pCur is invalid
        SSessionKey pTmpKey = *pWinKey;
        int32_t     winCode = TSDB_CODE_SUCCESS;
        code = getSessionWinResultBuff(pFileState, &pTmpKey, 0, (void**)&pNewPos, pVLen, &winCode);
        QUERY_CHECK_CONDITION((winCode == TSDB_CODE_FAILED), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
        QUERY_CHECK_CODE(code, lino, _end);
        goto _end;
      }
    }
    pNewPos = getNewRowPosForWrite(pFileState);
    if (!pNewPos || !pNewPos->pRowBuff) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    memcpy(pNewPos->pKey, pWinKey, sizeof(SSessionKey));
    pNewPos->needFree = true;
    pNewPos->beFlushed = true;
  }

_end:
  (*ppVal) = pNewPos;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

  if (size > 0 && index == -1) {
    SRowBuffPos* pPos = taosArrayGetP(pWinStates, 0);
    SSessionKey* pWin = (SSessionKey*)pPos->pKey;
    if (pWinKey->win.skey == pWin->win.skey) {
      index = 0;
    }
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
  int32_t          resSize = getRowStateRowSize(pFileState);
  COUNT_TYPE       winCount = 0;
  if (pBuffCur) {
    while (index >= 0) {
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, index);
      winCount = *((COUNT_TYPE*)((char*)pPos->pRowBuff + (resSize - sizeof(COUNT_TYPE))));
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

  void*            pFileStore = getStateFileStore(pFileState);
  SStreamStateCur* pCur = streamStateSessionSeekKeyPrev_rocksdb(pFileStore, pWinKey);
  if (pCur) {
    pCur->pStreamFileState = pFileState;
    SSessionKey key = {0};
    void*       pVal = NULL;
    int         len = 0;
    int32_t     code = streamStateSessionGetKVByCur_rocksdb(pCur, &key, &pVal, &len);
    if (code == TSDB_CODE_FAILED) {
      streamStateFreeCur(pCur);
      return pBuffCur;
    }
    winCount = *((COUNT_TYPE*)((char*)pVal + (resSize - sizeof(COUNT_TYPE))));
    taosMemoryFreeClear(pVal);
    streamStateFreeCur(pBuffCur);
    if (sessionRangeKeyCmpr(pWinKey, &key) != 0 && winCount == count) {
      streamStateCurNext(pFileStore, pCur);
      return pCur;
    }
    streamStateCurPrev(pFileStore, pCur);
    while (1) {
      code = streamStateSessionGetKVByCur_rocksdb(pCur, &key, &pVal, &len);
      if (code == TSDB_CODE_FAILED) {
        streamStateCurNext(pFileStore, pCur);
        return pCur;
      }
      winCount = *((COUNT_TYPE*)((char*)pVal + (resSize - sizeof(COUNT_TYPE))));
      taosMemoryFreeClear(pVal);
      if (sessionRangeKeyCmpr(pWinKey, &key) == 0 || winCount < count) {
        streamStateCurPrev(pFileStore, pCur);
      } else {
        streamStateCurNext(pFileStore, pCur);
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
        (code == TSDB_CODE_FAILED || sessionStateRangeKeyCompare(pKey, pWinStates, 0) >= 0)) {
      transformCursor(pCur->pStreamFileState, pCur);
      SRowBuffPos* pPos = taosArrayGetP(pWinStates, pCur->buffIndex);
      if (pVal) {
        *pVal = pPos;
      }
      *pKey = *(SSessionKey*)(pPos->pKey);
      code = TSDB_CODE_SUCCESS;
    } else if (code == TSDB_CODE_SUCCESS && pVal) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pCur->pStreamFileState);
      if (!pNewPos || !pNewPos->pRowBuff) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        taosMemoryFreeClear(pData);
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return code;
      }
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

void sessionWinStateMoveToNext(SStreamStateCur* pCur) {
  qTrace("move cursor to next");
  if (pCur && pCur->buffIndex >= 0) {
    pCur->buffIndex++;
  } else {
    streamStateCurNext_rocksdb(pCur);
  }
}

int32_t sessionWinStateGetKeyByRange(SStreamFileState* pFileState, const SSessionKey* key, SSessionKey* curKey,
                                     range_cmpr_fn cmpFn) {
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
                              state_key_cmpr_fn fn, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  (*pWinCode) = TSDB_CODE_SUCCESS;

  SSessionKey* pWinKey = key;
  TSKEY        gap = 0;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SArray*      pWinStates = NULL;

  void** ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    if (!pWinStates) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
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
      if (!(*pVal)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      (*pWinCode) = code_file;
      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
             pWinKey->win.ekey, code_file);
    } else {
      code = addNewSessionWindow(pFileState, pWinStates, key, (SRowBuffPos**)pVal);
      (*pWinCode) = TSDB_CODE_FAILED;
      taosMemoryFree(p);
      QUERY_CHECK_CODE(code, lino, _end);
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
      pPos->beFlushed = false;
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
      pPos->beFlushed = false;
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
        if (!(*pVal)) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          QUERY_CHECK_CODE(code, lino, _end);
        }

        (*pWinCode) = code_file;
        qDebug("===stream===1 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
               pWinKey->win.ekey, code_file);
        goto _end;
      } else {
        taosMemoryFree(p);
      }
    }
  }

  if (index == size - 1) {
    code = addNewSessionWindow(pFileState, pWinStates, key, (SRowBuffPos**)pVal);
    QUERY_CHECK_CODE(code, lino, _end);

    (*pWinCode) = TSDB_CODE_FAILED;
    goto _end;
  }
  code = insertNewSessionWindow(pFileState, pWinStates, key, index + 1, (SRowBuffPos**)pVal);
  QUERY_CHECK_CODE(code, lino, _end);

  (*pWinCode) = TSDB_CODE_FAILED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getCountWinStateFromDisc(SStreamState* pState, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, pKey);
  int32_t          code = streamStateSessionGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
  streamStateFreeCur(pCur);
  if (code == TSDB_CODE_SUCCESS) {
    return code;
  } else {
    pCur = streamStateSessionSeekKeyPrev_rocksdb(pState, pKey);
  }

  code = streamStateSessionGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
  streamStateFreeCur(pCur);
  return code;
}

int32_t getCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal,
                              int32_t* pVLen, int32_t* pWinCount) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  (*pWinCount) = TSDB_CODE_SUCCESS;

  SSessionKey* pWinKey = pKey;
  const TSKEY  gap = 0;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SArray*      pWinStates = NULL;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    if (!pWinStates) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  TSKEY startTs = pWinKey->win.skey;
  TSKEY endTs = pWinKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void* pFileStore = getStateFileStore(pFileState);
    void* pRockVal = NULL;
    (*pWinCount) = getCountWinStateFromDisc(pFileStore, pWinKey, &pRockVal, pVLen);
    if ((*pWinCount) == TSDB_CODE_SUCCESS || isFlushedState(pFileState, endTs, 0)) {
      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
             pWinKey->win.ekey, (*pWinCount));
      if ((*pWinCount) == TSDB_CODE_SUCCESS) {
        int32_t     valSize = *pVLen;
        COUNT_TYPE* pWinStateCout = (COUNT_TYPE*)((char*)(pRockVal) + (valSize - sizeof(COUNT_TYPE)));
        if (inSessionWindow(pWinKey, startTs, gap) || (*pWinStateCout) < winCount) {
          (*pVal) = createSessionWinBuff(pFileState, pWinKey, pRockVal, pVLen);
          if (!(*pVal)) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            QUERY_CHECK_CODE(code, lino, _end);
          }

          goto _end;
        }
      }
      pWinKey->win.skey = startTs;
      pWinKey->win.ekey = endTs;
      (*pVal) = createSessionWinBuff(pFileState, pWinKey, NULL, NULL);
      taosMemoryFree(pRockVal);
      if (!(*pVal)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      code = addNewSessionWindow(pFileState, pWinStates, pWinKey, (SRowBuffPos**)pVal);
      QUERY_CHECK_CODE(code, lino, _end);

      (*pWinCount) = TSDB_CODE_FAILED;
    }
    goto _end;
  }

  // find the first position which is smaller than the pWinKey
  int32_t      index = binarySearch(pWinStates, size, pWinKey, sessionStateKeyCompare);
  SRowBuffPos* pPos = NULL;
  int32_t      valSize = *pVLen;

  if (index >= 0) {
    pPos = taosArrayGetP(pWinStates, index);
    COUNT_TYPE* pWinStateCout = (COUNT_TYPE*)((char*)(pPos->pRowBuff) + (valSize - sizeof(COUNT_TYPE)));
    if (inSessionWindow(pPos->pKey, startTs, gap) || (index == size - 1 && (*pWinStateCout) < winCount)) {
      (*pVal) = pPos;
      SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
      pPos->beUsed = true;
      pPos->beFlushed = false;
      *pWinKey = *pDestWinKey;
      goto _end;
    }
  }

  if (index == -1) {
    if (!isDeteled(pFileState, endTs) && isFlushedState(pFileState, endTs, 0)) {
      SSessionKey tmpKey = *pWinKey;
      void*       pRockVal = NULL;
      void*       pFileStore = getStateFileStore(pFileState);
      int32_t     code_file = getCountWinStateFromDisc(pFileStore, &tmpKey, &pRockVal, pVLen);
      if (code_file == TSDB_CODE_SUCCESS) {
        SRowBuffPos* pFirstPos = taosArrayGetP(pWinStates, 0);
        SSessionKey* pFirstWinKey = (SSessionKey*)pFirstPos->pKey;
        if (tmpKey.win.ekey < pFirstWinKey->win.skey) {
          *pWinKey = tmpKey;
          (*pVal) = createSessionWinBuff(pFileState, pWinKey, pRockVal, pVLen);
          if (!(*pVal)) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            QUERY_CHECK_CODE(code, lino, _end);
          }

          (*pWinCount) = code_file;
          qDebug("===stream===1 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
                 pWinKey->win.ekey, code_file);
          goto _end;
        }
      }
      taosMemoryFree(pRockVal);
    }
  }

  if (index + 1 < size) {
    pPos = taosArrayGetP(pWinStates, index + 1);
    (*pVal) = pPos;
    SSessionKey* pDestWinKey = (SSessionKey*)pPos->pKey;
    pPos->beUsed = true;
    pPos->beFlushed = false;
    *pWinKey = *pDestWinKey;
    goto _end;
  }

  code = addNewSessionWindow(pFileState, pWinStates, pWinKey, (SRowBuffPos**)pVal);
  QUERY_CHECK_CODE(code, lino, _end);

  (*pWinCount) = TSDB_CODE_FAILED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  SSessionKey* pWinKey = pKey;
  const TSKEY  gap = 0;
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSHashObj*   pSessionBuff = getRowStateBuff(pFileState);
  SArray*      pWinStates = NULL;
  void**       ppBuff = tSimpleHashGet(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, POINTER_BYTES);
    code = tSimpleHashPut(pSessionBuff, &pWinKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  TSKEY startTs = pWinKey->win.skey;
  TSKEY endTs = pWinKey->win.ekey;

  int32_t size = taosArrayGetSize(pWinStates);
  if (size == 0) {
    void* pFileStore = getStateFileStore(pFileState);
    void* p = NULL;

    int32_t code_file = getCountWinStateFromDisc(pFileStore, pWinKey, &p, pVLen);
    if (code_file == TSDB_CODE_SUCCESS && isFlushedState(pFileState, endTs, 0)) {
      (*pVal) = createSessionWinBuff(pFileState, pWinKey, p, pVLen);
      if (!(*pVal)) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }

      qDebug("===stream===0 get state win:%" PRId64 ",%" PRId64 " from disc, res %d", pWinKey->win.skey,
             pWinKey->win.ekey, code_file);
    } else {
      code = addNewSessionWindow(pFileState, pWinStates, pWinKey, (SRowBuffPos**)pVal);
      taosMemoryFree(p);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = addNewSessionWindow(pFileState, pWinStates, pWinKey, (SRowBuffPos**)pVal);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
