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

SStreamState* streamStateOpen(char* path, void* pTask, bool specPath, int32_t szPage, int32_t pages) {
  SStreamState* pState = taosMemoryCalloc(1, sizeof(SStreamState));
  stDebug("open stream state %p, %s", pState, path);
  if (pState == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pState->pTdbState = taosMemoryCalloc(1, sizeof(STdbState));
  if (pState->pTdbState == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    streamStateDestroy(pState, true);
    return NULL;
  }

  SStreamTask* pStreamTask = pTask;
  pState->taskId = pStreamTask->id.taskId;
  pState->streamId = pStreamTask->id.streamId;
  sprintf(pState->pTdbState->idstr, "0x%" PRIx64 "-0x%x", pState->streamId, pState->taskId);

  streamTaskSetDb(pStreamTask->pMeta, pTask, pState->pTdbState->idstr);

#ifdef USE_ROCKSDB
  SStreamMeta* pMeta = pStreamTask->pMeta;
  pState->pTdbState->pOwner = pTask;
  pState->pFileState = NULL;
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT);

  pState->parNameMap = tSimpleHashInit(1024, hashFn);
  stInfo("succ to open state %p on backend %p 0x%" PRIx64 "-%d", pState, pMeta->streamBackend, pState->streamId,
         pState->taskId);
  return pState;

#else

  char cfgPath[1030];
  sprintf(cfgPath, "%s/cfg", statePath);

  szPage = szPage < 0 ? 4096 : szPage;
  pages = pages < 0 ? 256 : pages;
  char cfg[1024];
  memset(cfg, 0, 1024);
  TdFilePtr pCfgFile = taosOpenFile(cfgPath, TD_FILE_READ);
  if (pCfgFile != NULL) {
    int64_t size = 0;
    taosFStatFile(pCfgFile, &size, NULL);
    if (size > 0) {
      taosReadFile(pCfgFile, cfg, size);
      sscanf(cfg, "%d\n%d\n", &szPage, &pages);
    }
  } else {
    int32_t code = taosMulModeMkDir(statePath, 0755, false);
    if (code == 0) {
      pCfgFile = taosOpenFile(cfgPath, TD_FILE_WRITE | TD_FILE_CREATE);
      sprintf(cfg, "%d\n%d\n", szPage, pages);
      taosWriteFile(pCfgFile, cfg, strlen(cfg));
    }
  }
  taosCloseFile(&pCfgFile);

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

  if (tdbTbOpen("partag.state.db", sizeof(int64_t), -1, NULL, pState->pTdbState->db, &pState->pTdbState->pParTagDb, 0) <
      0) {
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
  tdbTbClose(pState->pTdbState->pParTagDb);
  tdbClose(pState->pTdbState->db);
  streamStateDestroy(pState, false);
  return NULL;
#endif
}

int32_t streamStateDelTaskDb(SStreamState* pState) {
  SStreamTask* pTask = pState->pTdbState->pOwner;
  taskDbRemoveRef(pTask->pBackend);
  taosMemoryFree(pTask);
  return 0;
}
void streamStateClose(SStreamState* pState, bool remove) {
  SStreamTask* pTask = pState->pTdbState->pOwner;
#ifdef USE_ROCKSDB
  streamStateDestroy(pState, remove);
#else
  tdbCommit(pState->pTdbState->db, pState->pTdbState->txn);
  tdbPostCommit(pState->pTdbState->db, pState->pTdbState->txn);
  tdbTbClose(pState->pTdbState->pStateDb);
  tdbTbClose(pState->pTdbState->pFuncStateDb);
  tdbTbClose(pState->pTdbState->pFillStateDb);
  tdbTbClose(pState->pTdbState->pSessionStateDb);
  tdbTbClose(pState->pTdbState->pParNameDb);
  tdbTbClose(pState->pTdbState->pParTagDb);
  tdbClose(pState->pTdbState->db);
#endif
}

int32_t streamStateBegin(SStreamState* pState) {
#ifdef USE_ROCKSDB
  return 0;
#else
  if (tdbBegin(pState->pTdbState->db, &pState->pTdbState->txn, NULL, NULL, NULL,
               TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    tdbAbort(pState->pTdbState->db, pState->pTdbState->txn);
    return -1;
  }
  return 0;
#endif
}

int32_t streamStateCommit(SStreamState* pState) {
#ifdef USE_ROCKSDB
  if (pState->pFileState) {
    SStreamSnapshot* pShot = getSnapshot(pState->pFileState);
    flushSnapshot(pState->pFileState, pShot, true);
  }
  return 0;
#else
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
#endif
}

int32_t streamStateFuncPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
#ifdef USE_ROCKSDB
  void*    pVal = NULL;
  int32_t  len = 0;
  int32_t  code = getRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), &pVal, &len);
  char*    buf = ((SRowBuffPos*)pVal)->pRowBuff;
  uint32_t rowSize = streamFileStateGeSelectRowSize(pState->pFileState);
  memcpy(buf + len - rowSize, value, vLen);
  return code;
#else
  return tdbTbUpsert(pState->pTdbState->pFuncStateDb, key, sizeof(STupleKey), value, vLen, pState->pTdbState->txn);
#endif
}
int32_t streamStateFuncGet(SStreamState* pState, const SWinKey* key, void** ppVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  void*    pVal = NULL;
  int32_t  len = 0;
  int32_t  code = getRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), (void**)(&pVal), &len);
  char*    buf = ((SRowBuffPos*)pVal)->pRowBuff;
  uint32_t rowSize = streamFileStateGeSelectRowSize(pState->pFileState);
  *ppVal = buf + len - rowSize;
  return code;
#else
  return tdbTbGet(pState->pTdbState->pFuncStateDb, key, sizeof(STupleKey), ppVal, pVLen);
#endif
}

// todo refactor
int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
#ifdef USE_ROCKSDB
  return 0;
  // return streamStatePut_rocksdb(pState, key, value, vLen);
#else
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbUpsert(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), value, vLen, pState->pTdbState->txn);
#endif
}

int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return getRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey), pVal, pVLen);
#else
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbGet(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), pVal, pVLen);
#endif
}

bool streamStateCheck(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return hasRowBuff(pState->pFileState, (void*)key, sizeof(SWinKey));
#else
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbGet(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), NULL, NULL);
#endif
}

int32_t streamStateGetByPos(SStreamState* pState, void* pos, void** pVal) {
  int32_t code = getRowBuffByPos(pState->pFileState, pos, pVal);
  streamFileStateReleaseBuff(pState->pFileState, pos, false);
  return code;
}

// todo refactor
int32_t streamStateDel(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return deleteRowBuff(pState->pFileState, key, sizeof(SWinKey));
#else
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbDelete(pState->pTdbState->pStateDb, &sKey, sizeof(SStateKey), pState->pTdbState->txn);
#endif
}

// todo refactor
int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
#ifdef USE_ROCKSDB
  return streamStateFillPut_rocksdb(pState, key, value, vLen);
#else
  return tdbTbUpsert(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), value, vLen, pState->pTdbState->txn);
#endif
}

// todo refactor
int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return streamStateFillGet_rocksdb(pState, key, pVal, pVLen);
#else
  return tdbTbGet(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), pVal, pVLen);
#endif
}

// todo refactor
int32_t streamStateFillDel(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateFillDel_rocksdb(pState, key);
#else
  return tdbTbDelete(pState->pTdbState->pFillStateDb, key, sizeof(SWinKey), pState->pTdbState->txn);
#endif
}

int32_t streamStateClear(SStreamState* pState) {
#ifdef USE_ROCKSDB
  streamFileStateClear(pState->pFileState);
  return 0;
#else
  SWinKey key = {.ts = 0, .groupId = 0};
  streamStatePut(pState, &key, NULL, 0);
  while (1) {
    SStreamStateCur* pCur = streamStateSeekKeyNext(pState, &key);
    SWinKey delKey = {0};
    int32_t code = streamStateGetKVByCur(pCur, &delKey, NULL, 0);
    streamStateFreeCur(pCur);
    if (code == 0) {
      streamStateDel(pState, &delKey);
    } else {
      break;
    }
  }
  return 0;
#endif
}

void streamStateSetNumber(SStreamState* pState, int32_t number) { pState->number = number; }

int32_t streamStateSaveInfo(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen) {
#ifdef USE_ROCKSDB
  int32_t code = 0;

  char* cfName = "default";
  void* batch = streamStateCreateBatch();
  code = streamStatePutBatch(pState, cfName, batch, pKey, pVal, vLen, 0);
  if (code != 0) {
    streamStateDestroyBatch(batch);
    return code;
  }
  code = streamStatePutBatch_rocksdb(pState, batch);
  streamStateDestroyBatch(batch);
  return code;
#else
  return 0;
#endif
}

int32_t streamStateGetInfo(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen) {
#ifdef USE_ROCKSDB
  int32_t code = 0;
  code = streamDefaultGet_rocksdb(pState, pKey, pVal, pLen);
  return code;
#else
  return 0;
#endif
}

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return streamStateGet(pState, key, pVal, pVLen);
#else
  // todo refactor
  int32_t size = *pVLen;
  if (streamStateGet(pState, key, pVal, pVLen) == 0) {
    return 0;
  }
  *pVal = tdbRealloc(NULL, size);
  memset(*pVal, 0, size);
  return 0;
#endif
}

int32_t streamStateReleaseBuf(SStreamState* pState, void* pVal, bool used) {
  // todo refactor
  if (!pVal) {
    return 0;
  }
#ifdef USE_ROCKSDB
  streamFileStateReleaseBuff(pState->pFileState, pVal, used);
#else
  streamStateFreeVal(pVal);
#endif
  return 0;
}

int32_t streamStateClearBuff(SStreamState* pState, void* pVal) {
  return streamFileStateClearBuff(pState->pFileState, pVal);
}

SStreamStateCur* streamStateFillGetCur(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateFillGetCur_rocksdb(pState, key);
#else
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
#endif
}

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateGetAndCheckCur_rocksdb(pState, key);
#else
  SStreamStateCur* pCur = streamStateFillGetCur(pState, key);
  if (pCur) {
    int32_t code = streamStateGetGroupKVByCur(pCur, key, NULL, 0);
    if (code == 0) {
      return pCur;
    }
    streamStateFreeCur(pCur);
  }
  return NULL;
#endif
}

int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return streamStateGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
#else
  if (!pCur) {
    return -1;
  }
  const SStateKey* pKTmp = NULL;
  int32_t kLen;
  if (tdbTbcGet(pCur->pCur, (const void**)&pKTmp, &kLen, pVal, pVLen) < 0) {
    return -1;
  }
  if (pKTmp->opNum != pCur->number) {
    return -1;
  }
  *pKey = pKTmp->key;
  return 0;
#endif
}

int32_t streamStateFillGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return streamStateFillGetKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
#else
  if (!pCur) {
    return -1;
  }
  const SWinKey* pKTmp = NULL;
  int32_t kLen;
  if (tdbTbcGet(pCur->pCur, (const void**)&pKTmp, &kLen, pVal, pVLen) < 0) {
    return -1;
  }
  *pKey = *pKTmp;
  return 0;
#endif
}

int32_t streamStateGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return streamStateGetGroupKVByCur_rocksdb(pCur, pKey, pVal, pVLen);
#else
  if (!pCur) {
    return -1;
  }
  uint64_t groupId = pKey->groupId;
  int32_t code = streamStateFillGetKVByCur(pCur, pKey, pVal, pVLen);
  if (code == 0) {
    if (pKey->groupId == groupId) {
      return 0;
    }
  }
  return -1;
#endif
}

SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateSeekKeyNext_rocksdb(pState, key);
#else
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
  int32_t c = 0;
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
#endif
}

SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateFillSeekKeyNext_rocksdb(pState, key);
#else
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
#endif
}

SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key) {
#ifdef USE_ROCKSDB
  return streamStateFillSeekKeyPrev_rocksdb(pState, key);
#else
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
#endif
}

int32_t streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur) {
#ifdef USE_ROCKSDB
  return sessionWinStateMoveToNext(pCur);
#else
  if (!pCur) {
    return -1;
  }
  //
  return tdbTbcMoveToNext(pCur->pCur);
#endif
}

int32_t streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur) {
#ifdef USE_ROCKSDB
  return streamStateCurPrev_rocksdb(pCur);
#else
  if (!pCur) {
    return -1;
  }
  return tdbTbcMoveToPrev(pCur->pCur);
#endif
}

void streamStateResetCur(SStreamStateCur* pCur) {
  if (!pCur) {
    return;
  }
  if (pCur->iter) rocksdb_iter_destroy(pCur->iter);
  if (pCur->snapshot) rocksdb_release_snapshot(pCur->db, pCur->snapshot);
  if (pCur->readOpt) rocksdb_readoptions_destroy(pCur->readOpt);

  tdbTbcClose(pCur->pCur);

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

void streamStateFreeVal(void* val) {
#ifdef USE_ROCKSDB
  taosMemoryFree(val);
#else
  tdbFree(val);
#endif
}

int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, void* value, int32_t vLen) {
#ifdef USE_ROCKSDB
  int32_t      code = TSDB_CODE_SUCCESS;
  SRowBuffPos* pos = (SRowBuffPos*)value;
  if (pos->needFree) {
    if (isFlushedState(pState->pFileState, key->win.ekey, 0)) {
      if (!pos->pRowBuff) {
        return code;
      }
      code = streamStateSessionPut_rocksdb(pState, key, pos->pRowBuff, vLen);
      streamStateReleaseBuf(pState, pos, true);
      putFreeBuff(pState->pFileState, pos);
      stDebug("===stream===save skey:%" PRId64 ", ekey:%" PRId64 ", groupId:%" PRIu64 ".code:%d", key->win.skey,
              key->win.ekey, key->groupId, code);
    } else {
      pos->beFlushed = false;
      code = putSessionWinResultBuff(pState->pFileState, value);
    }
  }
  return code;
#else
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbUpsert(pState->pTdbState->pSessionStateDb, &sKey, sizeof(SStateSessionKey), value, vLen,
                     pState->pTdbState->txn);
#endif
}

int32_t streamStateSessionAllocWinBuffByNextPosition(SStreamState* pState, SStreamStateCur* pCur,
                                                     const SSessionKey* pKey, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return allocSessioncWinBuffByNextPosition(pState->pFileState, pCur, pKey, pVal, pVLen);
#else
  return TSDB_CODE_FAILED;
#endif
}

int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return getSessionFlushedBuff(pState->pFileState, key, pVal, pVLen);
#else

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext(pState, key);
  SSessionKey resKey = *key;
  void* tmp = NULL;
  int32_t code = streamStateSessionGetKVByCur(pCur, &resKey, &tmp, pVLen);
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
#endif
}

int32_t streamStateSessionDel(SStreamState* pState, const SSessionKey* key) {
#ifdef USE_ROCKSDB
  qDebug("===stream===delete skey:%" PRId64 ", ekey:%" PRId64 ", groupId:%" PRIu64, key->win.skey, key->win.ekey,
         key->groupId);
  return deleteRowBuff(pState->pFileState, key, sizeof(SSessionKey));
#else
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  return tdbTbDelete(pState->pTdbState->pSessionStateDb, &sKey, sizeof(SStateSessionKey), pState->pTdbState->txn);
#endif
}

int32_t streamStateSessionReset(SStreamState* pState, void* pVal) {
  int32_t len = getRowStateRowSize(pState->pFileState);
  memset(pVal, 0, len);
  return TSDB_CODE_SUCCESS;
}

SStreamStateCur* streamStateSessionSeekKeyCurrentPrev(SStreamState* pState, const SSessionKey* key) {
#ifdef USE_ROCKSDB
  return sessionWinStateSeekKeyCurrentPrev(pState->pFileState, key);
#else
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
  int32_t c = 0;
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
#endif
}

SStreamStateCur* streamStateSessionSeekKeyCurrentNext(SStreamState* pState, const SSessionKey* key) {
#ifdef USE_ROCKSDB
  return sessionWinStateSeekKeyCurrentNext(pState->pFileState, key);
#else
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
  int32_t c = 0;
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
#endif
}

SStreamStateCur* streamStateSessionSeekKeyNext(SStreamState* pState, const SSessionKey* key) {
#ifdef USE_ROCKSDB
  return sessionWinStateSeekKeyNext(pState->pFileState, key);
#else
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
  int32_t c = 0;
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
#endif
}

SStreamStateCur* streamStateCountSeekKeyPrev(SStreamState* pState, const SSessionKey* key, COUNT_TYPE count) {
#ifdef USE_ROCKSDB
  return countWinStateSeekKeyPrev(pState->pFileState, key, count);
#else
#endif
}

int32_t streamStateSessionGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return sessionWinStateGetKVByCur(pCur, pKey, pVal, pVLen);
#else
  if (!pCur) {
    return -1;
  }
  SStateSessionKey* pKTmp = NULL;
  int32_t kLen;
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
#endif
}

int32_t streamStateSessionClear(SStreamState* pState) {
#ifdef USE_ROCKSDB
  sessionWinStateClear(pState->pFileState);
  return streamStateSessionClear_rocksdb(pState);
#else
  SSessionKey key = {.win.skey = 0, .win.ekey = 0, .groupId = 0};
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext(pState, &key);
  while (1) {
    SSessionKey delKey = {0};
    void* buf = NULL;
    int32_t size = 0;
    int32_t code = streamStateSessionGetKVByCur(pCur, &delKey, &buf, &size);
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
#endif
}

int32_t streamStateSessionGetKeyByRange(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
#ifdef USE_ROCKSDB
  return sessionWinStateGetKeyByRange(pState->pFileState, key, curKey, sessionRangeKeyCmpr);
#else
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
  int32_t c = 0;
  if (tdbTbcMoveTo(pCur->pCur, &sKey, sizeof(SStateSessionKey), &c) < 0) {
    streamStateFreeCur(pCur);
    return -1;
  }

  SSessionKey resKey = *key;
  int32_t code = streamStateSessionGetKVByCur(pCur, &resKey, NULL, 0);
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
#endif
}

int32_t streamStateCountGetKeyByRange(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
#ifdef USE_ROCKSDB
  return sessionWinStateGetKeyByRange(pState->pFileState, key, curKey, countRangeKeyEqual);
#else
#endif
}

int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                        int32_t* pVLen) {
#ifdef USE_ROCKSDB
  return getSessionWinResultBuff(pState->pFileState, key, gap, pVal, pVLen);
#else
  // todo refactor
  int32_t res = 0;
  SSessionKey originKey = *key;
  SSessionKey searchKey = *key;
  searchKey.win.skey = key->win.skey - gap;
  searchKey.win.ekey = key->win.ekey + gap;
  int32_t valSize = *pVLen;
  void* tmp = tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev(pState, key);
  int32_t code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
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

#endif
}

int32_t streamStateStateAddIfNotExist(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen) {
  // todo refactor

#ifdef USE_ROCKSDB
  return getStateWinResultBuff(pState->pFileState, key, pKeyData, keyDataLen, fn, pVal, pVLen);
#else
  int32_t res = 0;
  SSessionKey tmpKey = *key;
  int32_t valSize = *pVLen;
  void* tmp = tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev(pState, key);
  int32_t code = streamStateSessionGetKVByCur(pCur, key, pVal, pVLen);
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
#endif
}

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]) {
#ifdef USE_ROCKSDB
  if (tSimpleHashGetSize(pState->parNameMap) > MAX_TABLE_NAME_NUM) {
    if (tSimpleHashGet(pState->parNameMap, &groupId, sizeof(int64_t)) == NULL) {
      streamStatePutParName_rocksdb(pState, groupId, tbname);
    }
    return TSDB_CODE_SUCCESS;
  }
  tSimpleHashPut(pState->parNameMap, &groupId, sizeof(int64_t), tbname, TSDB_TABLE_NAME_LEN);
  return TSDB_CODE_SUCCESS;
#else
  return tdbTbUpsert(pState->pTdbState->pParNameDb, &groupId, sizeof(int64_t), tbname, TSDB_TABLE_NAME_LEN,
                     pState->pTdbState->txn);
#endif
}

int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal) {
#ifdef USE_ROCKSDB
  void* pStr = tSimpleHashGet(pState->parNameMap, &groupId, sizeof(int64_t));
  if (!pStr) {
    if (tSimpleHashGetSize(pState->parNameMap) > MAX_TABLE_NAME_NUM) {
      return streamStateGetParName_rocksdb(pState, groupId, pVal);
    }
    return TSDB_CODE_FAILED;
  }
  *pVal = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN);
  memcpy(*pVal, pStr, TSDB_TABLE_NAME_LEN);
  return TSDB_CODE_SUCCESS;
#else
  int32_t len;
  return tdbTbGet(pState->pTdbState->pParNameDb, &groupId, sizeof(int64_t), pVal, &len);
#endif
}

void streamStateDestroy(SStreamState* pState, bool remove) {
#ifdef USE_ROCKSDB
  streamFileStateDestroy(pState->pFileState);
  // streamStateDestroy_rocksdb(pState, remove);
  tSimpleHashCleanup(pState->parNameMap);
  // do nothong
#endif
  taosMemoryFreeClear(pState->pTdbState);
  taosMemoryFreeClear(pState);
}

int32_t streamStateDeleteCheckPoint(SStreamState* pState, TSKEY mark) {
#ifdef USE_ROCKSDB
  return deleteExpiredCheckPoint(pState->pFileState, mark);
#else
  return 0;
#endif
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
  return;
}
SStreamStateCur* createStreamStateCursor() {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  pCur->buffIndex = -1;
  return pCur;
}

// count window
int32_t streamStateCountWinAddIfNotExist(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** ppVal, int32_t* pVLen) {
  return getCountWinResultBuff(pState->pFileState, pKey, winCount, ppVal, pVLen);
}

int32_t streamStateCountWinAdd(SStreamState* pState, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  return createCountWinResultBuff(pState->pFileState, pKey, pVal, pVLen);
}
