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

#include "streamState.h"
#include "executor.h"
#include "osMemory.h"
#include "rocksdb/c.h"
#include "streamBackendRocksdb.h"
#include "streamInt.h"
#include "tcoding.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tref.h"

#define MAX_TABLE_NAME_NUM 200000

int sessionRangeKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2) {
  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->win.skey > pWin2->win.ekey) {
    return 1;
  } else if (pWin1->win.ekey < pWin2->win.skey) {
    return -1;
  }

  return 0;
}

int countRangeKeyEqual(const SSessionKey* pWin1, const SSessionKey* pWin2) {
  if (pWin1->groupId == pWin2->groupId && pWin1->win.skey <= pWin2->win.skey && pWin2->win.skey <= pWin1->win.ekey) {
    return 0;
  }

  return 1;
}

int sessionWinKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2) {
  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->win.skey > pWin2->win.skey) {
    return 1;
  } else if (pWin1->win.skey < pWin2->win.skey) {
    return -1;
  }

  if (pWin1->win.ekey > pWin2->win.ekey) {
    return 1;
  } else if (pWin1->win.ekey < pWin2->win.ekey) {
    return -1;
  }

  return 0;
}

int stateSessionKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SStateSessionKey* pWin1 = (SStateSessionKey*)pKey1;
  SStateSessionKey* pWin2 = (SStateSessionKey*)pKey2;

  if (pWin1->opNum > pWin2->opNum) {
    return 1;
  } else if (pWin1->opNum < pWin2->opNum) {
    return -1;
  }

  return sessionWinKeyCmpr(&pWin1->key, &pWin2->key);
}

int stateKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SStateKey* pWin1 = (SStateKey*)pKey1;
  SStateKey* pWin2 = (SStateKey*)pKey2;

  if (pWin1->opNum > pWin2->opNum) {
    return 1;
  } else if (pWin1->opNum < pWin2->opNum) {
    return -1;
  }

  return winKeyCmprImpl(&pWin1->key, &pWin2->key);
}

SStreamState* streamStateRecalatedOpen(const char* path, void* pTask, int64_t streamId, int32_t taskId) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SStreamTask* pStreamTask = pTask;
  if (!streamTaskShouldRecalated(pStreamTask)) {
    stError("failed to open recalation stream state %s", path);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  SStreamState* pState = taosMemoryCalloc(1, sizeof(SStreamState));
  stDebug("open stream state %p, %s", pState, path);

  if (pState == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pState->pTdbState = taosMemoryCalloc(1, sizeof(STdbState));
  if (pState->pTdbState == NULL) {
    streamStateDestroy(pState, true);
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pState->streamId = streamId;
  pState->taskId = taskId;

  pState->pTdbState->recalc = 1;

  TAOS_UNUSED(tsnprintf(pState->pTdbState->idstr, sizeof(pState->pTdbState->idstr), "0x%" PRIx64 "-0x%x-%s",
                        pState->streamId, pState->taskId, "recalc"));

  code = streamTaskSetDb(pStreamTask->pMeta, pTask, pState->pTdbState->idstr, 1);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamMeta* pMeta = pStreamTask->pMeta;
  pState->pTdbState->pOwner = pTask;
  pState->pFileState = NULL;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT);
  pState->parNameMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pState->parNameMap, code, lino, _end, terrno);

  stInfo("open state %p on backend %p 0x%" PRIx64 "-%d succ", pState, pMeta->streamBackend, pState->streamId,
         pState->taskId);
  return pState;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("0x%x %s recalated failed at line %d since %s", taskId, __func__, lino, tstrerror(code));
  }

  return NULL;
}
SStreamState* streamStateOpen(const char* path, void* pTask, int64_t streamId, int32_t taskId) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SStreamTask* pStreamTask = pTask;

  SStreamState* pState = taosMemoryCalloc(1, sizeof(SStreamState));
  stDebug("s-task:%s open stream state %p, %s", pStreamTask->id.idStr, pState, path);

  TAOS_UNUSED(tsnprintf(pState->pTaskIdStr, sizeof(pState->pTaskIdStr), "TID:0x%x QID:0x%" PRIx64,
                        taskId, streamId));

  if (pState == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pState->pTdbState = taosMemoryCalloc(1, sizeof(STdbState));
  if (pState->pTdbState == NULL) {
    streamStateDestroy(pState, true);
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pState->streamId = streamId;
  pState->taskId = taskId;
  TAOS_UNUSED(tsnprintf(pState->pTdbState->idstr, sizeof(pState->pTdbState->idstr), "0x%" PRIx64 "-0x%x",
                        pState->streamId, pState->taskId));
  // recal id + cal
  code = streamTaskSetDb(pStreamTask->pMeta, pTask, pState->pTdbState->idstr, 0);
  QUERY_CHECK_CODE(code, lino, _end);

  SStreamMeta* pMeta = pStreamTask->pMeta;
  pState->pTdbState->pOwner = pTask;
  pState->pFileState = NULL;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT);
  pState->parNameMap = tSimpleHashInit(1024, hashFn);
  QUERY_CHECK_NULL(pState->parNameMap, code, lino, _end, terrno);

  stInfo("s-task:%s open state %p on backend %p 0x%" PRIx64 "-%d succ", pStreamTask->id.idStr, pState,
         pMeta->streamBackend, pState->streamId, pState->taskId);
  return pState;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("0x%x %s failed at line %d since %s", taskId, __func__, lino, tstrerror(code));
  }
  return NULL;
}

int32_t streamStateDelTaskDb(SStreamState* pState) {
  SStreamTask* pTask = pState->pTdbState->pOwner;
  taskDbRemoveRef(pTask->pBackend);
  taosMemoryFree(pTask);
  return 0;
}
void streamStateClose(SStreamState* pState, bool remove) {
  SStreamTask* pTask = pState->pTdbState->pOwner;
  streamStateDestroy(pState, remove);
}

int32_t streamStateBegin(SStreamState* pState) { return 0; }

void streamStateCommit(SStreamState* pState) {
  if (pState->pFileState) {
    SStreamSnapshot* pShot = getSnapshot(pState->pFileState);
    flushSnapshot(pState->pFileState, pShot, true);
  }
}

int32_t streamStateFuncPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pVal = NULL;
  int32_t len = getRowStateRowSize(pState->pFileState);
  int32_t tmpLen = len;
  code = getFunctionRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), &pVal, &tmpLen);
  QUERY_CHECK_CODE(code, lino, _end);

  char*   buf = ((SRowBuffPos*)pVal)->pRowBuff;
  int32_t rowSize = streamFileStateGetSelectRowSize(pState->pFileState);
  memcpy(buf + len - rowSize, value, vLen);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
int32_t streamStateFuncGet(SStreamState* pState, const SWinKey* key, void** ppVal, int32_t* pVLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  void*   pVal = NULL;
  int32_t len = getRowStateRowSize(pState->pFileState);
  int32_t tmpLen = len;
  code = getFunctionRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), (void**)(&pVal), &tmpLen);
  QUERY_CHECK_CODE(code, lino, _end);

  char*   buf = ((SRowBuffPos*)pVal)->pRowBuff;
  int32_t rowSize = streamFileStateGetSelectRowSize(pState->pFileState);
  *ppVal = buf + len - rowSize;
  streamStateReleaseBuf(pState, pVal, false);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) { return 0; }

int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  return addRowBuffIfNotExist(pState->pFileState, (void*)key, sizeof(SWinKey), pVal, pVLen, pWinCode);
}

bool streamStateCheck(SStreamState* pState, const SWinKey* pKey, bool hasLimit, bool* pIsLast) {
  return hasRowBuff(pState->pFileState, pKey, hasLimit, pIsLast);
}

int32_t streamStateGetByPos(SStreamState* pState, void* pos, void** pVal) {
  int32_t code = getRowBuffByPos(pState->pFileState, pos, pVal);
  streamStateReleaseBuf(pState, pos, false);
  return code;
}

void streamStateDel(SStreamState* pState, const SWinKey* key) {
  deleteRowBuff(pState->pFileState, key, sizeof(SWinKey));
}

void streamStateDelByGroupId(SStreamState* pState, uint64_t groupId) {
  deleteRowBuffByGroupId(pState->pFileState, groupId);
}

int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  return streamStateFillPut_rocksdb(pState, key, value, vLen);
}

int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  if (pState->pFileState) {
    return getRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), pVal, pVLen, pWinCode);
  }
  return streamStateFillGet_rocksdb(pState, key, pVal, pVLen);
}

int32_t streamStateFillAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen,
                                     int32_t* pWinCode) {
  return getHashSortRowBuff(pState->pFileState, key, pVal, pVLen, pWinCode);
}

int32_t streamStateFillGetNext(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                               int32_t* pWinCode) {
  return getHashSortNextRow(pState->pFileState, pKey, pResKey, pVal, pVLen, pWinCode);
}

int32_t streamStateFillGetPrev(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                               int32_t* pWinCode) {
  return getHashSortPrevRow(pState->pFileState, pKey, pResKey, pVal, pVLen, pWinCode);
}

void streamStateFillDel(SStreamState* pState, const SWinKey* key) {
  int32_t code = streamStateFillDel_rocksdb(pState, key);
  qTrace("%s at line %d res %d", __func__, __LINE__, code);
}

void streamStateClear(SStreamState* pState) { streamFileStateClear(pState->pFileState); }

void streamStateSetNumber(SStreamState* pState, int32_t number, int32_t tsIdex) {
  pState->number = number;
  pState->tsIndex = tsIdex;
}

void streamStateSaveInfo(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  char* cfName = "default";
  void* batch = streamStateCreateBatch();
  code = streamStatePutBatch(pState, cfName, batch, pKey, pVal, vLen, 0);
  QUERY_CHECK_CODE(code, lino, _end);

  code = streamStatePutBatch_rocksdb(pState, batch);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  streamStateDestroyBatch(batch);
}

int32_t streamStateGetInfo(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = streamDefaultGet_rocksdb(pState, pKey, pVal, pLen);
  return code;
}

int32_t streamStateCreate(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  return createRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), pVal, pVLen);
}

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* pKey, void** pVal, int32_t* pVLen,
                                 int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  bool       isEnd = false;
  SSHashObj* pSearchBuff = getSearchBuff(pState->pFileState);
  if (pSearchBuff != NULL) {
    SArray* pWinStates = NULL;
    code = addArrayBuffIfNotExist(pSearchBuff, pKey->groupId, &pWinStates);
    QUERY_CHECK_CODE(code, lino, _end);

    // recover
    if (taosArrayGetSize(pWinStates) == 0 && needClearDiskBuff(pState->pFileState)) {
      code = recoverHashSortBuff(pState->pFileState, pWinStates, pKey->groupId);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = addSearchItem(pState->pFileState, pWinStates, pKey, &isEnd);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (isEnd) {
    code = streamStateCreate(pState, pKey, pVal, pVLen);
    QUERY_CHECK_CODE(code, lino, _end);
    (*pWinCode) = TSDB_CODE_FAILED;
  } else {
    code = streamStateGet(pState, pKey, pVal, pVLen, pWinCode);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void streamStateReleaseBuf(SStreamState* pState, void* pVal, bool used) {
  if (!pVal) {
    return;
  }
  streamFileStateReleaseBuff(pState->pFileState, pVal, used);
}

void streamStateClearBuff(SStreamState* pState, void* pVal) { streamFileStateClearBuff(pState->pFileState, pVal); }

SStreamStateCur* streamStateFillGetCur(SStreamState* pState, const SWinKey* key) {
  return streamStateFillGetCur_rocksdb(pState, key);
}

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key) {
  return streamStateGetAndCheckCur_rocksdb(pState, key);
}

int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  return streamStateGetKVByCur_rocksdb(getStateFileStore(pCur->pStreamFileState), pCur, pKey, pVal, pVLen);
}

int32_t streamStateFillGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  return streamStateFillGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
}

int32_t streamStateFillGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  return streamStateFillGetGroupKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
}

SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key) {
  return streamStateSeekKeyNext_rocksdb(pState, key);
}

SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key) {
  return streamStateFillSeekKeyNext_rocksdb(pState, key);
}

SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key) {
  return streamStateFillSeekKeyPrev_rocksdb(pState, key);
}

void streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur) { sessionWinStateMoveToNext(pCur); }

void streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur) {
  qTrace("move cursor to next");
  streamStateCurPrev_rocksdb(pCur);
}

void streamStateResetCur(SStreamStateCur* pCur) {
  if (!pCur) {
    return;
  }
  if (pCur->iter) rocksdb_iter_destroy(pCur->iter);
  if (pCur->snapshot) rocksdb_release_snapshot(pCur->db, pCur->snapshot);
  if (pCur->readOpt) rocksdb_readoptions_destroy(pCur->readOpt);

  memset(pCur, 0, sizeof(SStreamStateCur));

  pCur->buffIndex = -1;
}

void streamStateFreeCur(SStreamStateCur* pCur) {
  if (!pCur) {
    return;
  }
  streamStateResetCur(pCur);
  taosMemoryFree(pCur);
}

void streamStateFreeVal(void* val) { taosMemoryFree(val); }

int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, void* value, int32_t vLen) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SRowBuffPos* pos = (SRowBuffPos*)value;
  if (pos->needFree) {
    if (isFlushedState(pState->pFileState, key->win.ekey, 0)) {
      if (!pos->pRowBuff) {
        goto _end;
      }
      code = streamStateSessionPut_rocksdb(pState, key, pos->pRowBuff, getFileStateRowSize(pState->pFileState));
      QUERY_CHECK_CODE(code, lino, _end);

      streamStateReleaseBuf(pState, pos, true);
      code = putFreeBuff(pState->pFileState, pos);
      QUERY_CHECK_CODE(code, lino, _end);

      stDebug("===stream===save skey:%" PRId64 ", ekey:%" PRId64 ", groupId:%" PRIu64 ".code:%d", key->win.skey,
              key->win.ekey, key->groupId, code);
    } else {
      pos->beFlushed = false;
      code = putSessionWinResultBuff(pState->pFileState, value);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamStateSessionAllocWinBuffByNextPosition(SStreamState* pState, SStreamStateCur* pCur,
                                                     const SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  return allocSessioncWinBuffByNextPosition(pState->pFileState, pCur, pKey, pVal, pVLen);
}

int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  return getSessionFlushedBuff(pState->pFileState, key, pVal, pVLen, pWinCode);
}

void streamStateSessionDel(SStreamState* pState, const SSessionKey* key) {
  qDebug("===stream===delete skey:%" PRId64 ", ekey:%" PRId64 ", groupId:%" PRIu64, key->win.skey, key->win.ekey,
         key->groupId);
  deleteRowBuff(pState->pFileState, key, sizeof(SSessionKey));
}

void streamStateSessionReset(SStreamState* pState, void* pVal) {
  int32_t len = getRowStateRowSize(pState->pFileState);
  memset(pVal, 0, len);
}

SStreamStateCur* streamStateSessionSeekKeyCurrentPrev(SStreamState* pState, const SSessionKey* key) {
  return sessionWinStateSeekKeyCurrentPrev(pState->pFileState, key);
}

SStreamStateCur* streamStateSessionSeekKeyCurrentNext(SStreamState* pState, const SSessionKey* key) {
  if (pState->pFileState != NULL) {
    return sessionWinStateSeekKeyCurrentNext(pState->pFileState, key);
  }
  return streamStateSessionSeekKeyCurrentNext_rocksdb(pState, key);
}

SStreamStateCur *streamStateSessionSeekKeyPrev(SStreamState *pState, const SSessionKey *key) {
  return sessionWinStateSeekKeyPrev(pState->pFileState, key);
}

SStreamStateCur* streamStateSessionSeekKeyNext(SStreamState* pState, const SSessionKey* key) {
  return sessionWinStateSeekKeyNext(pState->pFileState, key);
}

SStreamStateCur* streamStateCountSeekKeyPrev(SStreamState* pState, const SSessionKey* key, COUNT_TYPE count) {
  return countWinStateSeekKeyPrev(pState->pFileState, key, count);
}

int32_t streamStateSessionGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  if (pCur != NULL && pCur->pStreamFileState != NULL) {
    return sessionWinStateGetKVByCur(pCur, pKey, pVal, pVLen);
  }
  return streamStateSessionGetKVByCur_rocksdb(NULL, pCur, pKey, pVal, pVLen);
}

void streamStateSessionClear(SStreamState* pState) {
  sessionWinStateClear(pState->pFileState);
  streamStateSessionClear_rocksdb(pState);
}

int32_t streamStateSessionGetKeyByRange(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
  return sessionWinStateGetKeyByRange(pState->pFileState, key, curKey, sessionRangeKeyCmpr);
}

int32_t streamStateCountGetKeyByRange(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
  return sessionWinStateGetKeyByRange(pState->pFileState, key, curKey, countRangeKeyEqual);
}

int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal, int32_t* pVLen,
                                        int32_t* pWinCode) {
  return getSessionWinResultBuff(pState->pFileState, key, gap, pVal, pVLen, pWinCode);
}

int32_t streamStateStateAddIfNotExist(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen, int32_t* pWinCode) {
  return getStateWinResultBuff(pState->pFileState, key, pKeyData, keyDataLen, fn, pVal, pVLen, pWinCode);
}

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pState->parNameMap == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (tSimpleHashGet(pState->parNameMap, &groupId, sizeof(int64_t)) == NULL) {
    if (tSimpleHashGetSize(pState->parNameMap) < MAX_TABLE_NAME_NUM) {
      code = tSimpleHashPut(pState->parNameMap, &groupId, sizeof(int64_t), tbname, TSDB_TABLE_NAME_LEN);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    code = streamStatePutParName_rocksdb(pState, groupId, tbname);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  qTrace("%s tbname:%s, groupId:%"PRId64, __func__, tbname, groupId);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal, bool onlyCache, int32_t* pWinCode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pState->parNameMap == NULL) {
    (*pWinCode) = TSDB_CODE_FAILED;
    return code;
  }

  void* pStr = tSimpleHashGet(pState->parNameMap, &groupId, sizeof(int64_t));
  if (!pStr) {
    if (onlyCache && tSimpleHashGetSize(pState->parNameMap) < MAX_TABLE_NAME_NUM) {
      (*pWinCode) = TSDB_CODE_FAILED;
      goto _end;
    }
    (*pWinCode) = streamStateGetParName_rocksdb(pState, groupId, pVal);
    if ((*pWinCode) == TSDB_CODE_SUCCESS && tSimpleHashGetSize(pState->parNameMap) < MAX_TABLE_NAME_NUM) {
      code = tSimpleHashPut(pState->parNameMap, &groupId, sizeof(int64_t), *pVal, TSDB_TABLE_NAME_LEN);
      qDebug("put into parNameMap, total size:%d, groupId:%" PRId64 ", name:%s", tSimpleHashGetSize(pState->parNameMap),
             groupId, (char*) (*pVal));

      QUERY_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }

  *pVal = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
  if (!(*pVal)) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  memcpy(*pVal, pStr, TSDB_TABLE_NAME_LEN);
  (*pWinCode) = TSDB_CODE_SUCCESS;

_end:
  qTrace("%s tbname:%s, groupId:%"PRId64, __func__, (char*)(*pVal), groupId);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t streamStateDeleteParName(SStreamState* pState, int64_t groupId) {
  if (pState->parNameMap == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = tSimpleHashRemove(pState->parNameMap, &groupId, sizeof(int64_t));
  if (TSDB_CODE_SUCCESS != code) {
    qWarn("failed to remove parname from cache, code:%d", code);
  }
  code = streamStateDeleteParName_rocksdb(pState, groupId);
  if (TSDB_CODE_SUCCESS != code) {
    qWarn("failed to remove parname from rocksdb, code:%d", code);
  }
  return TSDB_CODE_SUCCESS;
}

void streamStateSetParNameInvalid(SStreamState* pState) {
  if (pState != NULL) {
    tSimpleHashCleanup(pState->parNameMap);
    pState->parNameMap = NULL;
  }
}

void streamStateDestroy(SStreamState* pState, bool remove) {
  streamFileStateDestroy(pState->pFileState);
  tSimpleHashCleanup(pState->parNameMap);
  // do nothong
  taosMemoryFreeClear(pState->pTdbState);
  taosMemoryFreeClear(pState);
}

void streamStateReloadInfo(SStreamState* pState, TSKEY ts) { streamFileStateReloadInfo(pState->pFileState, ts); }

void streamStateCopyBackend(SStreamState* src, SStreamState* dst) {
  dst->pFileState = src->pFileState;
  dst->parNameMap = src->parNameMap;
  dst->number = src->number;
  dst->taskId = src->taskId;
  dst->streamId = src->streamId;
  if (dst->pTdbState == NULL) {
    dst->pTdbState = taosMemoryCalloc(1, sizeof(STdbState));
    dst->pTdbState->pOwner = taosMemoryCalloc(1, sizeof(SStreamTask));
  }
  dst->dump = 1;
  dst->pTdbState->pOwner->pBackend = src->pTdbState->pOwner->pBackend;
  dst->pResultRowStore.resultRowPut = src->pResultRowStore.resultRowPut;
  dst->pResultRowStore.resultRowGet = src->pResultRowStore.resultRowGet;
  dst->pExprSupp = src->pExprSupp;
  return;
}
SStreamStateCur* createStreamStateCursor() {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->buffIndex = -1;
  return pCur;
}

// count window
int32_t streamStateCountWinAddIfNotExist(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** ppVal,
                                         int32_t* pVLen, int32_t* pWinCode) {
  return getCountWinResultBuff(pState->pFileState, pKey, winCount, ppVal, pVLen, pWinCode);
}

int32_t streamStateCountWinAdd(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal,
                               int32_t* pVLen) {
  return createCountWinResultBuff(pState->pFileState, pKey, winCount, pVal, pVLen);
}

int32_t streamStateGroupPut(SStreamState* pState, int64_t groupId, void* value, int32_t vLen) {
  return streamFileStateGroupPut(pState->pFileState, groupId, value, vLen);
}

SStreamStateCur* streamStateGroupGetCur(SStreamState* pState) {
  SStreamStateCur* pCur = createStateCursor(pState->pFileState);
  pCur->hashIter = 0;
  pCur->pHashData = NULL;
  SSHashObj* pMap = getGroupIdCache(pState->pFileState);
  pCur->pHashData = tSimpleHashIterate(pMap, pCur->pHashData, &pCur->hashIter);
  if (pCur->pHashData == NULL) {
    pCur->hashIter = -1;
    streamStateParTagSeekKeyNext_rocksdb(pState, INT64_MIN, pCur);
  }
  return pCur;
}

void streamStateGroupCurNext(SStreamStateCur* pCur) { streamFileStateGroupCurNext(pCur); }

int32_t streamStateGroupGetKVByCur(SStreamStateCur* pCur, int64_t* pKey, void** pVal, int32_t* pVLen) {
  if (pVal != NULL) {
    return -1;
  }
  return streamFileStateGroupGetKVByCur(pCur, pKey, pVal, pVLen);
}

void streamStateClearExpiredState(SStreamState* pState, int32_t numOfKeep, TSKEY minTs) {
  qDebug("===stream=== clear stream state. keep:%d, ts:%" PRId64, numOfKeep, minTs);
  if (numOfKeep == 0) {
    streamFileStateClear(pState->pFileState);
    SSHashObj* pSearchBuff = getSearchBuff(pState->pFileState);
    void*      pIte = NULL;
    int32_t    iter = 0;
    while ((pIte = tSimpleHashIterate(pSearchBuff, pIte, &iter)) != NULL) {
      SArray* pWinStates = *((void**)pIte);
      taosArrayClear(pWinStates);
    }
    return;
  }
  clearExpiredState(pState->pFileState, numOfKeep, minTs);
}

int32_t streamStateGetPrev(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                           int32_t* pWinCode) {
  return getRowStatePrevRow(pState->pFileState, pKey, pResKey, pVal, pVLen, pWinCode);
}

int32_t streamStateGetAllPrev(SStreamState* pState, const SWinKey* pKey, SArray* pResArray, int32_t maxNum) {
  return getRowStateAllPrevRow(pState->pFileState, pKey, pResArray, maxNum);
}

int32_t streamStateGetAndSetTsData(STableTsDataState* pState, uint64_t tableUid, TSKEY* pCurTs, void** ppCurPkVal,
                                   TSKEY lastTs, void* pLastPkVal, int32_t lastPkLen, int32_t* pWinCode) {
  return getAndSetTsData(pState, tableUid, pCurTs, ppCurPkVal, lastTs, pLastPkVal, lastPkLen, pWinCode);
}

int32_t streamStateTsDataCommit(STableTsDataState* pState) {
  int32_t code = doTsDataCommit(pState);
  if (code != TSDB_CODE_SUCCESS) return code;
  return doRangeDataCommit(pState);
}

int32_t streamStateInitTsDataState(STableTsDataState** ppTsDataState, int8_t pkType, int32_t pkLen, void* pState,
                                   void* pOtherState) {
  return initTsDataState(ppTsDataState, pkType, pkLen, pState, pOtherState);
}

void streamStateDestroyTsDataState(STableTsDataState* pTsDataState) { destroyTsDataState(pTsDataState); }

int32_t streamStateRecoverTsData(STableTsDataState* pTsDataState) { return recoverTsData(pTsDataState); }

SStreamStateCur* streamStateGetLastStateCur(SStreamState* pState) { return getLastStateCur(pState->pFileState, getSearchBuff); }

void streamStateLastStateCurNext(SStreamStateCur* pCur) { moveLastStateCurNext(pCur, getSearchBuff); }

int32_t streamStateNLastStateGetKVByCur(SStreamStateCur* pCur, int32_t num, SArray* pRes) {
  return getNLastStateKVByCur(pCur, num, pRes);
}

SStreamStateCur* streamStateGetLastSessionStateCur(SStreamState* pState) { return getLastStateCur(pState->pFileState, getRowStateBuff); }

void streamStateLastSessionStateCurNext(SStreamStateCur* pCur) { moveLastStateCurNext(pCur, getRowStateBuff); }

int32_t streamStateNLastSessionStateGetKVByCur(SStreamStateCur* pCur, int32_t num, SArray* pRes) {
  return getNLastSessionStateKVByCur(pCur, num, pRes);
}

int32_t streamStateReloadTsDataState(STableTsDataState* pTsDataState) { return reloadTsDataState(pTsDataState); }

int32_t streamStateMergeAndSaveScanRange(STableTsDataState* pTsDataState, STimeWindow* pWin, uint64_t gpId,
                                         SRecDataInfo* pRecData, int32_t len) {
  return mergeAndSaveScanRange(pTsDataState, pWin, gpId, pRecData, len);
}

int32_t streamStateMergeAllScanRange(STableTsDataState* pTsDataState) { return mergeAllScanRange(pTsDataState); }

int32_t streamStatePopScanRange(STableTsDataState* pTsDataState, SScanRange* pRange) {
  return popScanRange(pTsDataState, pRange);
}

int32_t streamStateGetNumber(SStreamState* pState) { return pState->number; }

int32_t streamStateDeleteInfo(SStreamState* pState, void* pKey, int32_t keyLen) {
  return streamDefaultDel_rocksdb(pState, pKey);
}

int32_t streamStateSessionSaveToDisk(STableTsDataState* pTblState, SSessionKey* pKey, SRecDataInfo* pVal,
                                     int32_t vLen) {
  SStreamState* pState = pTblState->pState;
  qDebug("===stream===%s save recalculate range.recId:%d. start:%" PRId64 ",end:%" PRId64 ",groupId:%" PRIu64
         ". cal start:%" PRId64 ",cal end:%" PRId64 ",tbl uid:%" PRIu64 ",data version:%" PRId64 ",mode:%d",
         pState->pTaskIdStr, pState->number, pKey->win.skey, pKey->win.ekey, pKey->groupId, pVal->calWin.skey,
         pVal->calWin.ekey, pVal->tableUid, pVal->dataVersion, pVal->mode);
  return saveRecInfoToDisk(pTblState, pKey, pVal, vLen);
}

int32_t streamStateFlushReaminInfoToDisk(STableTsDataState* pTblState) {
  return flushRemainRecInfoToDisk(pTblState);
}

int32_t streamStateSessionDeleteAll(SStreamState* pState) {
  SSessionKey key = {.win.skey = INT64_MIN, .win.ekey = INT64_MIN, .groupId = 0};
  while (1) {
    SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, &key);
    SSessionKey      delKey = {0};
    int32_t          winRes = streamStateSessionGetKVByCur_rocksdb(pState, pCur, &delKey, NULL, 0);
    if (winRes != TSDB_CODE_SUCCESS) {
      streamStateFreeCur(pCur);
      break;
    }
    streamStateSessionDel_rocksdb(pState, &delKey);
    streamStateFreeCur(pCur);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t streamStateSetRecFlag(SStreamState* pState, const void* pKey, int32_t keyLen, int32_t mode) {
  return setStateRecFlag(pState->pFileState, pKey, keyLen, mode);
}

int32_t streamStateGetRecFlag(SStreamState* pState, const void* pKey, int32_t keyLen, int32_t* pMode) {
  return getStateRecFlag(pState->pFileState, pKey, keyLen, pMode);
}

void streamStateClearExpiredSessionState(SStreamState* pState, int32_t numOfKeep, TSKEY minTs, SSHashObj* pFlushGroup) {
  if (numOfKeep == 0) {
    void* pBuff = getRowStateBuff(pState->pFileState);
    tSimpleHashSetFreeFp(pBuff, freeArrayPtr);
    streamFileStateClear(pState->pFileState);
    return;
  }
  clearExpiredSessionState(pState->pFileState, numOfKeep, minTs, pFlushGroup);
}

bool streamStateCheckSessionState(SStreamState* pState, SSessionKey* pKey, TSKEY gap, bool* pIsLast) {
  return hasSessionState(pState->pFileState, pKey, gap, pIsLast);
}

