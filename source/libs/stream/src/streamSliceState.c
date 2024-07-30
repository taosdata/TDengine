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

#define NUM_OF_FLUSED_WIN 64

int fillStateKeyCompare(const void* pWin1, const void* pDatas, int pos) {
  SWinKey* pWin2 = taosArrayGet(pDatas, pos);
  return winKeyCmprImpl((SWinKey*)pWin1, pWin2);
}

int32_t getHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey, void** pVal, int32_t* pVLen) {
  int32_t winCode = TSDB_CODE_SUCCESS;
  int32_t code = getRowBuff(pFileState, (void*)pKey, sizeof(SWinKey), pVal, pVLen, &winCode);

  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    pWinStates = taosArrayInit(16, sizeof(SWinKey));
    tSimpleHashPut(pSearchBuff, &pKey->groupId, sizeof(uint64_t), &pWinStates, POINTER_BYTES);
  }

  //recover
  if (taosArrayGetSize(pWinStates) == 0 && needClearDiskBuff(pFileState)) {
    TSKEY ts = getFlushMark(pFileState);
    SWinKey start = {.groupId = pKey->groupId, .ts = INT64_MAX};
    void* pState = getStateFileStore(pFileState);
    SStreamStateCur* pCur = streamStateFillSeekKeyPrev_rocksdb(pState, &start);
    int32_t winCode = TSDB_CODE_SUCCESS;
    for(int32_t i = 0; i < NUM_OF_FLUSED_WIN && winCode == TSDB_CODE_SUCCESS; i++) {
      SWinKey tmp = {.groupId = pKey->groupId};
      winCode = streamStateGetGroupKVByCur_rocksdb(pCur, &tmp, NULL, 0);
      if (winCode != TSDB_CODE_SUCCESS) {
        break;
      }
      taosArrayPush(pWinStates, &tmp);
      streamStateCurPrev_rocksdb(pCur);
    }
    taosArraySort(pWinStates, winKeyCmprImpl);
    streamStateFreeCur(pCur);
  }

  int32_t size = taosArrayGetSize(pWinStates);
  if (!isFlushedState(pFileState, pKey->ts, 0)) {
    // find the first position which is smaller than the pKey
    int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
    if (index == -1) {
      index = 0;
    }
    SWinKey* pTmpKey = taosArrayGet(pWinStates, index);
    if (winKeyCmprImpl(pTmpKey, pKey) != 0) {
      taosArrayInsert(pWinStates, index, pKey);
    }
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
  void*   pIte = NULL;
  int32_t iter = 0;
  void*   pBuff = getRowStateBuff(pFileState);
  while ((pIte = tSimpleHashIterate(pBuff, pIte, &iter)) != NULL) {
    SArray* pWinStates = *((void**)pIte);
    int32_t size = taosArrayGetSize(pWinStates);
    if (size > 0) {
      TSKEY ts = getFlushMark(pFileState);
      SWinKey key = *(SWinKey*)taosArrayGet(pWinStates, 0);
      key.ts = ts;
      int32_t num = binarySearch(pWinStates, size, &key, fillStateKeyCompare);
      if (size > NUM_OF_FLUSED_WIN) {
        num = TMIN(num, size - NUM_OF_FLUSED_WIN);
        taosArrayRemoveBatch(pWinStates, 0, num, NULL);
      }
    }
  }
}

int32_t getHashSortNextRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen) {
  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void*      pState = getStateFileStore(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    SStreamStateCur* pCur = streamStateFillSeekKeyNext_rocksdb(pState, pKey);
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pCur, pResKey, (const void **)pVal, pVLen);
    streamStateFreeCur(pCur);
    return code;
  }
  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (index == -1) {
    SStreamStateCur* pCur = streamStateFillSeekKeyNext_rocksdb(pState, pKey);
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pCur, pResKey, (const void **)pVal, pVLen);
    streamStateFreeCur(pCur);
    return code;
  } else {
    if (index == size - 1) {
      return TSDB_CODE_FAILED;
    }
    SWinKey* pNext = taosArrayGet(pWinStates, index + 1);
    *pResKey = *pNext;
    return getHashSortRowBuff(pFileState, pResKey, pVal, pVLen);
  }
  return TSDB_CODE_FAILED;
}

int32_t getHashSortPrevRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen) {
  SArray*    pWinStates = NULL;
  SSHashObj* pSearchBuff = getSearchBuff(pFileState);
  void*      pState = getStateFileStore(pFileState);
  void**     ppBuff = tSimpleHashGet(pSearchBuff, &pKey->groupId, sizeof(uint64_t));
  if (ppBuff) {
    pWinStates = (SArray*)(*ppBuff);
  } else {
    SStreamStateCur* pCur = streamStateFillSeekKeyPrev_rocksdb(pState, pKey);
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)pVal, pVLen);
    streamStateFreeCur(pCur);
    return code;
  }
  int32_t size = taosArrayGetSize(pWinStates);
  int32_t index = binarySearch(pWinStates, size, pKey, fillStateKeyCompare);
  if (index == -1 || index == 0) {
    SStreamStateCur* pCur = streamStateFillSeekKeyPrev_rocksdb(pState, pKey);
    int32_t code = streamStateGetGroupKVByCur_rocksdb(pCur, pResKey, (const void**)pVal, pVLen);
    streamStateFreeCur(pCur);
    return code;
  } else {
    SWinKey* pNext = taosArrayGet(pWinStates, index - 1);
    *pResKey = *pNext;
    return getHashSortRowBuff(pFileState, pResKey, pVal, pVLen);
  }
  return TSDB_CODE_FAILED;
}

void deleteHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey) {
  deleteRowBuff(pFileState, pKey, sizeof(SWinKey));
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

