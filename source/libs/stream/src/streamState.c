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

#include "executor.h"
#include "streamInc.h"
#include "tcommon.h"
#include "tcompare.h"
#include "ttimer.h"

// todo refactor
typedef struct SStateKey {
  SWinKey key;
  int64_t opNum;
} SStateKey;

typedef struct SStateSessionKey {
  SSessionKey key;
  int64_t     opNum;
} SStateSessionKey;

static inline int sessionRangeKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2) {
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

static inline int sessionWinKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2) {
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

static inline int stateSessionKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SStateSessionKey* pWin1 = (SStateSessionKey*)pKey1;
  SStateSessionKey* pWin2 = (SStateSessionKey*)pKey2;

  if (pWin1->opNum > pWin2->opNum) {
    return 1;
  } else if (pWin1->opNum < pWin2->opNum) {
    return -1;
  }

  return sessionWinKeyCmpr(&pWin1->key, &pWin2->key);
}

static inline int stateKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  SStateKey* pWin1 = (SStateKey*)pKey1;
  SStateKey* pWin2 = (SStateKey*)pKey2;

  if (pWin1->opNum > pWin2->opNum) {
    return 1;
  } else if (pWin1->opNum < pWin2->opNum) {
    return -1;
  }

  if (pWin1->key.ts > pWin2->key.ts) {
    return 1;
  } else if (pWin1->key.ts < pWin2->key.ts) {
    return -1;
  }

  if (pWin1->key.groupId > pWin2->key.groupId) {
    return 1;
  } else if (pWin1->key.groupId < pWin2->key.groupId) {
    return -1;
  }

  return 0;
}

SStreamState* streamStateOpen(char* path, SStreamTask* pTask, bool specPath, int32_t szPage, int32_t pages) {
  szPage = szPage < 0 ? 4096 : szPage;
  pages = pages < 0 ? 256 : pages;
  SStreamState* pState = taosMemoryCalloc(1, sizeof(SStreamState));
  if (pState == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pState->pTdbState = taosMemoryCalloc(1, sizeof(STdbState));
  if (pState->pTdbState == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    streamStateDestroy(pState);
    return NULL;
  }

  char statePath[1024];
  if (!specPath) {
    sprintf(statePath, "%s/%d", path, pTask->taskId);
  } else {
    memset(statePath, 0, 1024);
    tstrncpy(statePath, path, 1024);
  }
  if (tdbOpen(statePath, szPage, pages, &pState->pTdbState->db, 1) < 0) {
    goto _err;
  }

  // open state storage backend
  if (tdbTbOpen("state.db", sizeof(SStateKey), -1, stateKeyCmpr, pState->pTdbState->db, &pState->pTdbState->pStateDb,
                0) < 0) {
    goto _err;
  }

  // todo refactor
  if (tdbTbOpen("fill.state.db", sizeof(SWinKey), -1, winKeyCmpr, pState->pTdbState->db,
                &pState->pTdbState->pFillStateDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("session.state.db", sizeof(SStateSessionKey), -1, stateSessionKeyCmpr, pState->pTdbState->db,
                &pState->pTdbState->pSessionStateDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("func.state.db", sizeof(STupleKey), -1, STupleKeyCmpr, pState->pTdbState->db,
                &pState->pTdbState->pFuncStateDb, 0) < 0) {
    goto _err;
  }

  if (tdbTbOpen("parname.state.db", sizeof(int64_t), TSDB_TABLE_NAME_LEN, NULL, pState->pTdbState->db,
                &pState->pTdbState->pParNameDb, 0) < 0) {
    goto _err;
  }

  if (streamStateBegin(pState) < 0) {
    goto _err;
  }

  pState->pTdbState->pOwner = pTask;

  return pState;

_err:
  tdbTbClose(pState->pTdbState->pStateDb);
  tdbTbClose(pState->pTdbState->pFuncStateDb);
  tdbTbClose(pState->pTdbState->pFillStateDb);
  tdbTbClose(pState->pTdbState->pSessionStateDb);
  tdbTbClose(pState->pTdbState->pParNameDb);
  tdbClose(pState->pTdbState->db);
  streamStateDestroy(pState);
  return NULL;
}

void streamStateClose(SStreamState* pState) {
  tdbCommit(pState->pTdbState->db, pState->pTdbState->txn);
  tdbPostCommit(pState->pTdbState->db, pState->pTdbState->txn);
  tdbTbClose(pState->pTdbState->pStateDb);
  tdbTbClose(pState->pTdbState->pFuncStateDb);
  tdbTbClose(pState->pTdbState->pFillStateDb);
  tdbTbClose(pState->pTdbState->pSessionStateDb);
  tdbTbClose(pState->pTdbState->pParNameDb);
  tdbClose(pState->pTdbState->db);

  streamStateDestroy(pState);
}

int32_t streamStateBegin(SStreamState* pState) {
  if (tdbBegin(pState->pTdbState->db, &pState->pTdbState->txn, NULL, NULL, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    tdbAbort(pState->pTdbState->db, pState->pTdbState->txn);
    return -1;
  }
  return 0;
}

int32_t streamStateCommit(SStreamState* pState) {
  if (tdbCommit(pState->pTdbState->db, pState->pTdbState->txn) < 0) {
    return -1;
  }
  if (tdbPostCommit(pState->pTdbState->db, pState->pTdbState->txn) < 0) {
    return -1;
  }

  if (tdbBegin(pState->pTdbState->db, &pState->pTdbState->txn, NULL, NULL, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamStateAbort(SStreamState* pState) {
  if (tdbAbort(pState->pTdbState->db, pState->pTdbState->txn) < 0) {
    return -1;
  }

  if (tdbBegin(pState->pTdbState->db, &pState->pTdbState->txn, NULL, NULL, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    return -1;
  }
  return 0;
}

int32_t streamStateFuncPut(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen) {
  return tdbTbUpsert(pState->pTdbState->pFuncStateDb, key, sizeof(STupleKey), value, vLen, pState->pTdbState->txn);
}
int32_t streamStateFuncGet(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen) {
  return tdbTbGet(pState->pTdbState->pFuncStateDb, key, sizeof(STupleKey), pVal, pVLen);
}

int32_t streamStateFuncDel(SStreamState* pState, const STupleKey* key) {
  return tdbTbDelete(pState->pTdbState->pFuncStateDb, key, sizeof(STupleKey), pState->pTdbState->txn);
}

// todo refactor
int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbUpsert(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), value, vLen, pState->pTdbState->txn);
}

// todo refactor
int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  return tdbTbUpsert(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), value, vLen, pState->pTdbState->txn);
}

// todo refactor
int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbGet(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), pVal, pVLen);
}

// todo refactor
int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  return tdbTbGet(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), pVal, pVLen);
}

// todo refactor
int32_t streamStateDel(SStreamState* pState, const SWinKey* key) {
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbDelete(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), pState->pTdbState->txn);
}

int32_t streamStateClear(SStreamState* pState) {
  SWinKey key = {.ts = 0, .groupId = 0};
  streamStatePut(pState, &key, NULL, 0);
  while (1) {
    SStreamStateCur* pCur = streamStateSeekKeyNext(pState, &key);
    SWinKey          delKey = {0};
    int32_t          code = streamStateGetKVByCur(pCur, &delKey, NULL, 0);
    streamStateFreeCur(pCur);
    if (code == 0) {
      streamStateDel(pState, &delKey);
    } else {
      break;
    }
  }
  return 0;
}

void streamStateSetNumber(SStreamState* pState, int32_t number) { pState->number = number; }

// todo refactor
int32_t streamStateFillDel(SStreamState* pState, const SWinKey* key) {
  return tdbTbDelete(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), pState->pTdbState->txn);
}

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  // todo refactor
  int32_t size = *pVLen;
  if (streamStateGet(pState, key, pVal, pVLen) == 0) {
    return 0;
  }
  *pVal = tdbRealloc(NULL, size);
  memset(*pVal, 0, size);
  return 0;
}

int32_t streamStateReleaseBuf(SStreamState* pState, const SWinKey* key, void* pVal) {
  // todo refactor
  if (!pVal) {
    return 0;
  }
  streamFreeVal(pVal);
  return 0;
}

SStreamStateCur* streamStateGetCur(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) return NULL;
  tdbTbcOpen(pState->pTdbState->pStateDb, &pCur->pCur, NULL);

  int32_t   c = 0;
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateKey), &c);
  if (c != 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  pCur->number = pState->number;
  return pCur;
}

SStreamStateCur* streamStateFillGetCur(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) return NULL;
  tdbTbcOpen(pState->pTdbState->pFillStateDb, &pCur->pCur, NULL);

  int32_t c = 0;
  tdbTbcMoveTo(pCur->pCur, key, sizeof(SWinKey), &c);
  if (c != 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key) {
  SStreamStateCur* pCur = streamStateFillGetCur(pState, key);
  if (pCur) {
    int32_t code = streamStateGetGroupKVByCur(pCur, key, NULL, 0);
    if (code == 0) {
      return pCur;
    }
    streamStateFreeCur(pCur);
  }
  return NULL;
}

int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return -1;
  }
  const SStateKey* pKTmp = NULL;
  int32_t          kLen;
  if (tdbTbcGet(pCur->pCur, (const void**)&pKTmp, &kLen, pVal, pVLen) < 0) {
    return -1;
  }
  if (pKTmp->opNum != pCur->number) {
    return -1;
  }
  *pKey = pKTmp->key;
  return 0;
}

int32_t streamStateFillGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return -1;
  }
  const SWinKey* pKTmp = NULL;
  int32_t        kLen;
  if (tdbTbcGet(pCur->pCur, (const void**)&pKTmp, &kLen, pVal, pVLen) < 0) {
    return -1;
  }
  *pKey = *pKTmp;
  return 0;
}

int32_t streamStateGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return -1;
  }
  uint64_t groupId = pKey->groupId;
  int32_t  code = streamStateFillGetKVByCur(pCur, pKey, pVal, pVLen);
  if (code == 0) {
    if (pKey->groupId == groupId) {
      return 0;
    }
  }
  return -1;
}

int32_t streamStateGetFirst(SStreamState* pState, SWinKey* key) {
  // todo refactor
  SWinKey tmp = {.ts = 0, .groupId = 0};
  streamStatePut(pState, &tmp, NULL, 0);
  SStreamStateCur* pCur = streamStateSeekKeyNext(pState, &tmp);
  int32_t          code = streamStateGetKVByCur(pCur, key, NULL, 0);
  streamStateFreeCur(pCur);
  streamStateDel(pState, &tmp);
  return code;
}

int32_t streamStateSeekFirst(SStreamState* pState, SStreamStateCur* pCur) {
  //
  return tdbTbcMoveToFirst(pCur->pCur);
}

int32_t streamStateSeekLast(SStreamState* pState, SStreamStateCur* pCur) {
  //
  return tdbTbcMoveToLast(pCur->pCur);
}

SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  SStateKey sKey = {.key = *key, .opNum = pState->number};
  int32_t   c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (c > 0) return pCur;

  if (tdbTbcMoveToNext(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (!pCur) {
    return NULL;
  }
  if (tdbTbcOpen(pState->pTdbState->pFillStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  int32_t c = 0;
  if (tdbTbcMoveTo(pCur->pCur, key, sizeof(SWinKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (c > 0) return pCur;

  if (tdbTbcMoveToNext(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  if (tdbTbcOpen(pState->pTdbState->pFillStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  int32_t c = 0;
  if (tdbTbcMoveTo(pCur->pCur, key, sizeof(SWinKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (c < 0) return pCur;

  if (tdbTbcMoveToPrev(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

int32_t streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur) {
  if (!pCur) {
    return -1;
  }
  //
  return tdbTbcMoveToNext(pCur->pCur);
}

int32_t streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur) {
  //
  if (!pCur) {
    return -1;
  }
  return tdbTbcMoveToPrev(pCur->pCur);
}
void streamStateFreeCur(SStreamStateCur* pCur) {
  if (!pCur) {
    return;
  }
  tdbTbcClose(pCur->pCur);
  taosMemoryFree(pCur);
}

void streamFreeVal(void* val) { tdbFree(val); }

int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen) {
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbUpsert(pState->pTdbState->pSessionStateDb, &sKey, sizeof(SStateSessionKey), value, vLen,
                     pState->pTdbState->txn);
}

int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen) {
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext(pState, key);
  SSessionKey      resKey = *key;
  void*            tmp = NULL;
  int32_t          code = streamStateSessionGetKVByCur(pCur, &resKey, &tmp, pVLen);
  if (code == 0) {
    if (key->win.skey != resKey.win.skey) {
      code = -1;
    } else {
      *key = resKey;
      *pVal = tdbRealloc(NULL, *pVLen);
      memcpy(*pVal, tmp, *pVLen);
    }
  }
  streamStateFreeCur(pCur);
  return code;
}

int32_t streamStateSessionDel(SStreamState* pState, const SSessionKey* key) {
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbDelete(pState->pTdbState->pSessionStateDb, &sKey, sizeof(SStateSessionKey), pState->pTdbState->txn);
}

SStreamStateCur* streamStateSessionSeekKeyCurrentPrev(SStreamState* pState, const SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pSessionStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateSessionKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (c >= 0) return pCur;

  if (tdbTbcMoveToPrev(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

SStreamStateCur* streamStateSessionSeekKeyCurrentNext(SStreamState* pState, const SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pSessionStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateSessionKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  if (c <= 0) return pCur;

  if (tdbTbcMoveToNext(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

SStreamStateCur* streamStateSessionSeekKeyNext(SStreamState* pState, const SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pSessionStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateSessionKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  if (c < 0) return pCur;

  if (tdbTbcMoveToNext(pCur->pCur) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  return pCur;
}

int32_t streamStateSessionGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return -1;
  }
  SStateSessionKey* pKTmp = NULL;
  int32_t           kLen;
  if (tdbTbcGet(pCur->pCur, (const void**)&pKTmp, &kLen, (const void**)pVal, pVLen) < 0) {
    return -1;
  }
  if (pKTmp->opNum != pCur->number) {
    return -1;
  }
  if (pKey->groupId != 0 && pKey->groupId != pKTmp->key.groupId) {
    return -1;
  }
  *pKey = pKTmp->key;
  return 0;
}

int32_t streamStateSessionClear(SStreamState* pState) {
  SSessionKey      key = {.win.skey = 0, .win.ekey = 0, .groupId = 0};
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext(pState, &key);
  while (1) {
    SSessionKey delKey = {0};
    void*       buf = NULL;
    int32_t     size = 0;
    int32_t     code = streamStateSessionGetKVByCur(pCur, &delKey, &buf, &size);
    if (code == 0 && size > 0) {
      memset(buf, 0, size);
      streamStateSessionPut(pState, &delKey, buf, size);
    } else {
      break;
    }
    streamStateCurNext(pState, pCur);
  }
  streamStateFreeCur(pCur);
  return 0;
}

int32_t streamStateSessionGetKeyByRange(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return -1;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pSessionStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return -1;
  }

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateSessionKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return -1;
  }

  SSessionKey resKey = *key;
  int32_t     code = streamStateSessionGetKVByCur(pCur, &resKey, NULL, 0);
  if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
    *curKey = resKey;
    streamStateFreeCur(pCur);
    return code;
  }

  if (c > 0) {
    streamStateCurNext(pState, pCur);
    code = streamStateSessionGetKVByCur(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  } else if (c < 0) {
    streamStateCurPrev(pState, pCur);
    code = streamStateSessionGetKVByCur(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  }

  streamStateFreeCur(pCur);
  return -1;
}

int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                        int32_t* pVLen) {
  // todo refactor
  int32_t     res = 0;
  SSessionKey originKey = *key;
  SSessionKey searchKey = *key;
  searchKey.win.skey = key->win.skey - gap;
  searchKey.win.ekey = key->win.ekey + gap;
  int32_t valSize = *pVLen;
  void*   tmp = tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev(pState, key);
  int32_t          code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel(pState, key);
      goto _end;
    }
    streamStateCurNext(pState, pCur);
  } else {
    *key = originKey;
    streamStateFreeCur(pCur);
    pCur = streamStateSessionSeekKeyNext(pState, key);
  }

  code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel(pState, key);
      goto _end;
    }
  }

  *key = originKey;
  res = 1;
  memset(tmp, 0, valSize);

_end:

  *pVal = tmp;
  streamStateFreeCur(pCur);
  return res;
}

int32_t streamStateStateAddIfNotExist(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen) {
  // todo refactor
  int32_t     res = 0;
  SSessionKey tmpKey = *key;
  int32_t     valSize = *pVLen;
  void*       tmp = tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev(pState, key);
  int32_t          code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (key->win.skey <= tmpKey.win.skey && tmpKey.win.ekey <= key->win.ekey) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel(pState, key);
      goto _end;
    }

    void* stateKey = (char*)(*pVal) + (valSize - keyDataLen);
    if (fn(pKeyData, stateKey) == true) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel(pState, key);
      goto _end;
    }

    streamStateCurNext(pState, pCur);
  } else {
    *key = tmpKey;
    streamStateFreeCur(pCur);
    pCur = streamStateSessionSeekKeyNext(pState, key);
  }

  code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
  if (code == 0) {
    void* stateKey = (char*)(*pVal) + (valSize - keyDataLen);
    if (fn(pKeyData, stateKey) == true) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel(pState, key);
      goto _end;
    }
  }

  *key = tmpKey;
  res = 1;
  memset(tmp, 0, valSize);

_end:

  *pVal = tmp;
  streamStateFreeCur(pCur);
  return res;
}

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]) {
  tdbTbUpsert(pState->pTdbState->pParNameDb, &groupId, sizeof(int64_t), tbname, TSDB_TABLE_NAME_LEN,
              pState->pTdbState->txn);
  return 0;
}

int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal) {
  int32_t len;
  return tdbTbGet(pState->pTdbState->pParNameDb, &groupId, sizeof(int64_t), pVal, &len);
}

void streamStateDestroy(SStreamState* pState) {
  taosMemoryFreeClear(pState->pTdbState);
  taosMemoryFreeClear(pState);
}

#if 0
char* streamStateSessionDump(SStreamState* pState) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;
  if (tdbTbcOpen(pState->pTdbState->pSessionStateDb, &pCur->pCur, NULL) < 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  tdbTbcMoveToFirst(pCur->pCur);

  SSessionKey key = {0};
  void*       buf = NULL;
  int32_t     bufSize = 0;
  int32_t     code = streamStateSessionGetKVByCur(pCur, &key, &buf, &bufSize);
  if (code != 0) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  int32_t size = 2048;
  char*   dumpBuf = taosMemoryCalloc(size, 1);
  int64_t len = 0;
  len += snprintf(dumpBuf + len, size - len, "||s:%15" PRId64 ",", key.win.skey);
  len += snprintf(dumpBuf + len, size - len, "e:%15" PRId64 ",", key.win.ekey);
  len += snprintf(dumpBuf + len, size - len, "g:%15" PRId64 "||", key.groupId);
  while (1) {
    tdbTbcMoveToNext(pCur->pCur);
    key = (SSessionKey){0};
    code = streamStateSessionGetKVByCur(pCur, &key, NULL, 0);
    if (code != 0) {
      streamStateFreeCur(pCur);
      return dumpBuf;
    }
    len += snprintf(dumpBuf + len, size - len, "||s:%15" PRId64 ",", key.win.skey);
    len += snprintf(dumpBuf + len, size - len, "e:%15" PRId64 ",", key.win.ekey);
    len += snprintf(dumpBuf + len, size - len, "g:%15" PRId64 "||", key.groupId);
  }
  streamStateFreeCur(pCur);
  return dumpBuf;
}
#endif
