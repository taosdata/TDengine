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

#define NUM_OF_CACHE_WIN 64
#define MAX_NUM_OF_CACHE_WIN 128

int32_t recoverSearchBuff(SStreamFileState* pFileState, SArray* pWinStates, uint64_t groupId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SWinKey          start = {.groupId = groupId, .ts = INT64_MAX};
  void*            pState = getStateFileStore(pFileState);
  SStreamStateCur* pCur = streamStateFillSeekKeyPrev_rocksdb(pState, &start);
  for (int32_t i = 0; i < NUM_OF_CACHE_WIN; i++) {
    SWinKey tmpKey = {.groupId = groupId};
    int32_t tmpRes = streamStateFillGetGroupKVByCur_rocksdb(pCur, &tmpKey, NULL, 0);
    if (tmpRes != TSDB_CODE_SUCCESS) {
      break;
    }
    void* tmp = taosArrayPush(pWinStates, &tmpKey);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    streamStateCurPrev_rocksdb(pCur);
  }
  taosArraySort(pWinStates, winKeyCmprImpl);
  streamStateFreeCur(pCur);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey, void** pVal, int32_t* pVLen,
                           int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  code = addRowBuffIfNotExist(pFileState, (void*)pKey, sizeof(SWinKey), pVal, pVLen, pWinCode);
  QUERY_CHECK_CODE(code, lino, _end);

  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  code = addArrayBuffIfNotExist(pSearchBuff, pKey->groupId, &pWinStates);
  QUERY_CHECK_CODE(code, lino, _end);

  // recover
  if (taosArrayGetSize(pWinStates) == 0 && needClearDiskBuff(pFileState)) {
    code = recoverSearchBuff(pFileState, pWinStates, pKey->groupId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  bool isEnd = false;
  code = addSearchItem(pFileState, pWinStates, pKey, &isEnd);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t hashSortFileGetFn(SStreamFileState* pFileState, void* pKey, void** data, int32_t* pDataLen) {
  void* pState = getStateFileStore(pFileState);
  return streamStateFillGet_rocksdb(pState, pKey, data, pDataLen);
}

int32_t hashSortFileRemoveFn(SStreamFileState* pFileState, const void* pKey) {
  void* pState = getStateFileStore(pFileState);
  return streamStateFillDel_rocksdb(pState, pKey);
}

void clearSearchBuff(SStreamFileState* pFileState) {
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  if (!pSearchBuff) {
    return;
  }
  TSKEY   flushMark = getFlushMark(pFileState);
  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pSearchBuff, pIte, &iter)) != NULL) {
    SArray* pWinStates = *((void**)pIte);
    int32_t size = taosArrayGetSize(pWinStates);
    if (size > 0) {
      int64_t gpId = *(int64_t*)tSimpleHashGetKey(pIte, NULL);
      SWinKey key = {.ts = flushMark, .groupId = gpId};
      int32_t num = binarySearch(pWinStates, size, &key, fillStateKeyCompare);
      if (size > NUM_OF_CACHE_WIN) {
        num = TMIN(num, size - NUM_OF_CACHE_WIN);
        taosArrayRemoveBatch(pWinStates, 0, num, NULL);
      }
    }
  }
}

#ifdef BUILD_NO_CALL
int32_t getStateFromRocksdbByCur(SStreamFileState* pFileState, SStreamStateCur* pCur, SWinKey* pResKey, SRowBuffPos** ppPos, int32_t* pVLen, int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   tmpVal = NULL;
  int32_t len = 0;
  (*pWinCode) = streamStateFillGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)&tmpVal, &len);
  if ((*pWinCode) == TSDB_CODE_SUCCESS) {
    SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
    if (!pNewPos || !pNewPos->pRowBuff) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    memcpy(pNewPos->pRowBuff, tmpVal, len);
    taosMemoryFreeClear(tmpVal);
    *pVLen = getRowStateRowSize(pFileState);
    (*ppPos) = pNewPos;
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
#endif

int32_t getHashSortNextRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** ppVal,
                           int32_t* pVLen, int32_t* pWinCode) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void*      pState = getStateFileStore(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    SStreamStateCur* pCur = streamStateFillSeekKeyNext_rocksdb(pState, pKey);
    void*            tmpVal = NULL;
    int32_t          len = 0;
    (*pWinCode) = streamStateFillGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)&tmpVal, &len);
    if ((*pWinCode) == TSDB_CODE_SUCCESS && ppVal != NULL) {
      SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
      if (!pNewPos || !pNewPos->pRowBuff) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      memcpy(pNewPos->pRowBuff, tmpVal, len);
      *pVLen = getRowStateRowSize(pFileState);
      (*ppVal) = pNewPos;
    }
    taosMemoryFreeClear(tmpVal);
    streamStateFreeCur(pCur);
    return code;
  }
  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (index == -1) {
    SStreamStateCur* pCur = streamStateFillSeekKeyNext_rocksdb(pState, pKey);
    void*            tmpVal = NULL;
    int32_t          len = 0;
    (*pWinCode) = streamStateFillGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)&tmpVal, &len);
    if ((*pWinCode) == TSDB_CODE_SUCCESS) {
      if (ppVal != NULL) {
        SRowBuffPos* pNewPos = getNewRowPosForWrite(pFileState);
        if (!pNewPos || !pNewPos->pRowBuff) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        memcpy(pNewPos->pRowBuff, tmpVal, len);
        *pVLen = getRowStateRowSize(pFileState);
        (*ppVal) = pNewPos;
      }
      taosMemoryFreeClear(tmpVal);
      streamStateFreeCur(pCur);
      return code;
    }
    streamStateFreeCur(pCur);
  }

  if (index == size - 1) {
    (*pWinCode) = TSDB_CODE_FAILED;
    return code;
  }
  SWinKey* pNext = taosArrayGet(pWinStates, index + 1);
  *pResKey = *pNext;
  if (ppVal == NULL) {
    (*pWinCode) = TSDB_CODE_SUCCESS;
    return code;
  }
  return getHashSortRowBuff(pFileState, pResKey, ppVal, pVLen, pWinCode);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getHashSortPrevRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** ppVal,
                           int32_t* pVLen, int32_t* pWinCode) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void*      pState = getStateFileStore(pFileState);
  // void**     ppBuff = (void**) tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  
  code = addArrayBuffIfNotExist(pSearchBuff, pKey->groupId, &pWinStates);
  QUERY_CHECK_CODE(code, lino, _end);
  
  // recover
  if (taosArrayGetSize(pWinStates) == 0 && needClearDiskBuff(pFileState)) {
    code = recoverSearchBuff(pFileState, pWinStates, pKey->groupId);
    QUERY_CHECK_CODE(code, lino, _end);
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
    SStreamStateCur* pCur = streamStateFillSeekKeyPrev_rocksdb(pState, pKey);
    void*            tmpVal = NULL;
    int32_t          len = 0;
    (*pWinCode) = streamStateFillGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)&tmpVal, &len);
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
    return getHashSortRowBuff(pFileState, pResKey, ppVal, pVLen, pWinCode);
  }
  (*pWinCode) = TSDB_CODE_FAILED;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void deleteHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey) {
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (!ppBuff) {
    return;
  }
  SArray* pWinStates = *ppBuff;
  int32_t size = taosArrayGetSize(pWinStates);
  if (!isFlushedState(pFileState, pKey->ts, 0)) {
    // find the first position which is smaller than the pKey
    int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
    if (index == -1) {
      index = 0;
    }
    SWinKey* pTmpKey = taosArrayGet(pWinStates, index);
    if (winKeyCmprImpl(pTmpKey, pKey) == 0) {
      taosArrayRemove(pWinStates, index);
    }
  }
}
