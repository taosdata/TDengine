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

#include <bits/stdint-uintn.h>
#include <string.h>
#include "executor.h"
#include "osMemory.h"
#include "rocksdb/c.h"
#include "streamInc.h"
#include "streamState.h"
#include "tcoding.h"
#include "tcommon.h"
#include "tcompare.h"
#include "ttimer.h"

//
//  SStateKey
//  |--groupid--|---ts------|--opNum----|
//  |--uint64_t-|-uint64_t--|--int64_t--|
//
//
//
int stateKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SStateKey key1, key2;
  memset(&key1, 0, sizeof(key1));
  memset(&key2, 0, sizeof(key2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &key1.key.groupId);
  p2 = taosDecodeFixedU64(p2, &key2.key.groupId);

  p1 = taosDecodeFixedI64(p1, &key1.key.ts);
  p2 = taosDecodeFixedI64(p2, &key2.key.ts);

  taosDecodeFixedI64(p1, &key1.opNum);
  taosDecodeFixedI64(p2, &key2.opNum);

  return stateKeyCmpr(&key1, sizeof(key1), &key2, sizeof(key2));
}

int stateKeyEncode(void* k, char* buf) {
  SStateKey* key = k;
  int        len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->key.groupId);
  len += taosEncodeFixedI64((void**)&buf, key->key.ts);
  len += taosEncodeFixedU64((void**)&buf, key->opNum);
  return len;
}
int stateKeyDecode(void* k, char* buf) {
  SStateKey* key = k;
  int        len = 0;
  char*      p = buf;
  p = taosDecodeFixedU64(p, &key->key.groupId);
  p = taosDecodeFixedI64(p, &key->key.ts);
  p = taosDecodeFixedI64(p, &key->opNum);
  return p - buf;
}

//
// SStateSessionKey
//  |-----------SSessionKey----------|
//  |-----STimeWindow-----|
//  |---skey--|---ekey----|--groupId-|--opNum--|
//  |---int64-|--int64_t--|--uint64--|--int64_t|
// |
//
int stateSessionKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SStateSessionKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedI64(p1, &w1.key.win.skey);
  p2 = taosDecodeFixedI64(p2, &w2.key.win.skey);

  p1 = taosDecodeFixedI64(p1, &w1.key.win.ekey);
  p2 = taosDecodeFixedI64(p2, &w2.key.win.ekey);

  p1 = taosDecodeFixedU64(p1, &w1.key.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.key.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.opNum);
  p2 = taosDecodeFixedI64(p2, &w2.opNum);

  return stateSessionKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}
int stateSessionKeyEncode(void* ses, char* buf) {
  SStateSessionKey* sess = ses;
  int               len = 0;
  len += taosEncodeFixedI64((void**)&buf, sess->key.win.skey);
  len += taosEncodeFixedI64((void**)&buf, sess->key.win.ekey);
  len += taosEncodeFixedU64((void**)&buf, sess->key.groupId);
  len += taosEncodeFixedI64((void**)&buf, sess->opNum);
  return len;
}
int stateSessionKeyDecode(void* ses, char* buf) {
  SStateSessionKey* sess = ses;
  int               len = 0;

  char* p = buf;
  p = taosDecodeFixedI64(p, &sess->key.win.skey);
  p = taosDecodeFixedI64(p, &sess->key.win.ekey);
  p = taosDecodeFixedU64(p, &sess->key.groupId);
  p = taosDecodeFixedI64(p, &sess->opNum);
  return p - buf;
}

/**
 *  SWinKey
 *  |------groupId------|-----ts------|
 *  |------uint64-------|----int64----|
 */
int winKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  SWinKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &w1.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.ts);
  p2 = taosDecodeFixedI64(p2, &w2.ts);

  return winKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}

int winKeyEncode(void* k, char* buf) {
  SWinKey* key = k;
  int      len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->groupId);
  len += taosEncodeFixedI64((void**)&buf, key->ts);
  return len;
}

int winKeyDecode(void* k, char* buf) {
  SWinKey* key = k;
  int      len = 0;
  char*    p = buf;
  p = taosDecodeFixedU64(p, &key->groupId);
  p = taosDecodeFixedI64(p, &key->ts);
  return len;
}
/*
 * STupleKey
 * |---groupId---|---ts---|---exprIdx---|
 * |---uint64--|---int64--|---int32-----|
 */
int tupleKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  STupleKey w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));

  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  p1 = taosDecodeFixedU64(p1, &w1.groupId);
  p2 = taosDecodeFixedU64(p2, &w2.groupId);

  p1 = taosDecodeFixedI64(p1, &w1.ts);
  p2 = taosDecodeFixedI64(p2, &w2.ts);

  p1 = taosDecodeFixedI32(p1, &w1.exprIdx);
  p2 = taosDecodeFixedI32(p2, &w2.exprIdx);

  return STupleKeyCmpr(&w1, sizeof(w1), &w2, sizeof(w2));
}

int tupleKeyEncode(void* k, char* buf) {
  STupleKey* key = k;
  int        len = 0;
  len += taosEncodeFixedU64((void**)&buf, key->groupId);
  len += taosEncodeFixedI64((void**)&buf, key->ts);
  len += taosEncodeFixedI32((void**)&buf, key->exprIdx);
  return len;
}
int tupleKeyDecode(void* k, char* buf) {
  STupleKey* key = k;
  int        len = 0;
  char*      p = buf;
  p = taosDecodeFixedU64(p, &key->groupId);
  p = taosDecodeFixedI64(p, &key->ts);
  p = taosDecodeFixedI32(p, &key->exprIdx);
  return len;
}

int parKeyDBComp(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen) {
  int64_t w1, w2;
  memset(&w1, 0, sizeof(w1));
  memset(&w2, 0, sizeof(w2));
  char* p1 = (char*)aBuf;
  char* p2 = (char*)bBuf;

  taosDecodeFixedI64(p1, &w1);
  taosDecodeFixedI64(p2, &w2);
  if (w1 == w2) {
    return 0;
  } else {
    return w1 < w2 ? -1 : 1;
  }
}
int parKeyEncode(void* k, char* buf) {
  int64_t* groupid = k;
  int      len = taosEncodeFixedI64((void**)&buf, *groupid);
  return len;
}
int parKeyDecode(void* k, char* buf) {
  char*    p = buf;
  int64_t* groupid = k;

  p = taosDecodeFixedI64(p, groupid);
  return p - buf;
}

const char* cfName[] = {"default", "fill", "sess", "func", "parname", "partag"};
typedef int (*EncodeFunc)(void* key, char* buf);
typedef int (*DecodeFunc)(void* key, char* buf);
////typedef int32_t (*BackendCmpFunc)(void* state, const char* aBuf, size_t aLen, const char* bBuf, size_t bLen);
////typedef const char* (*BackendCmpNameFunc)(void* statue);

typedef struct {
  const char* key;
  int         idx;
  EncodeFunc  enFunc;
  DecodeFunc  deFunc;
} SCfInit;

SCfInit ginitDict[] = {
    {"default", 0, stateKeyEncode, stateKeyDecode},
    {"fill", 1, winKeyEncode, winKeyDecode},
    {"sess", 2, stateSessionKeyEncode, stateSessionKeyDecode},
    {"func", 3, tupleKeyEncode, tupleKeyDecode},
    {"parname", 4, parKeyEncode, parKeyDecode},
    {"partag", 5, parKeyEncode, parKeyDecode},
};

const char* compareStateName(void* name) { return cfName[0]; }
const char* compareWinKey(void* name) { return cfName[1]; }
const char* compareSessionKey(void* name) { return cfName[2]; }
const char* compareFuncKey(void* name) { return cfName[3]; }
const char* compareParKey(void* name) { return cfName[4]; }
const char* comparePartagKey(void* name) { return cfName[5]; }

int streamInitBackend(SStreamState* pState, char* path) {
  rocksdb_options_t* opts = rocksdb_options_create();
  rocksdb_options_increase_parallelism(opts, 4);
  rocksdb_options_optimize_level_style_compaction(opts, 0);
  // create the DB if it's not already present
  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_create_missing_column_families(opts, 1);

  char* err = NULL;
  int   cfLen = sizeof(cfName) / sizeof(cfName[0]);

  const rocksdb_options_t** cfOpt = taosMemoryCalloc(cfLen, sizeof(rocksdb_options_t*));
  for (int i = 0; i < cfLen; i++) {
    cfOpt[i] = rocksdb_options_create_copy(opts);
  }

  rocksdb_comparator_t* stateCompare = rocksdb_comparator_create(NULL, NULL, stateKeyDBComp, compareStateName);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[0], stateCompare);

  rocksdb_comparator_t* fillCompare = rocksdb_comparator_create(NULL, NULL, winKeyDBComp, compareWinKey);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[1], fillCompare);

  rocksdb_comparator_t* sessCompare = rocksdb_comparator_create(NULL, NULL, stateSessionKeyDBComp, compareSessionKey);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[2], sessCompare);

  rocksdb_comparator_t* funcCompare = rocksdb_comparator_create(NULL, NULL, tupleKeyDBComp, compareFuncKey);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[3], funcCompare);

  rocksdb_comparator_t* parnameCompare = rocksdb_comparator_create(NULL, NULL, parKeyDBComp, compareParKey);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[4], parnameCompare);

  rocksdb_comparator_t* partagCompare = rocksdb_comparator_create(NULL, NULL, parKeyDBComp, comparePartagKey);
  rocksdb_options_set_comparator((rocksdb_options_t*)cfOpt[5], partagCompare);

  rocksdb_column_family_handle_t** cfHandle = taosMemoryMalloc(cfLen * sizeof(rocksdb_column_family_handle_t*));
  rocksdb_t* db = rocksdb_open_column_families(opts, "rocksdb", cfLen, cfName, cfOpt, cfHandle, &err);

  pState->pTdbState->rocksdb = db;
  pState->pTdbState->pHandle = cfHandle;
  pState->pTdbState->wopts = rocksdb_writeoptions_create();
  pState->pTdbState->ropts = rocksdb_readoptions_create();
  return 0;
}
void streamCleanBackend(SStreamState* pState) {
  int cfLen = sizeof(cfName) / sizeof(cfName[0]);
  for (int i = 0; i < cfLen; i++) {
    rocksdb_column_family_handle_destroy(pState->pTdbState->pHandle[i]);
  }
  rocksdb_close(pState->pTdbState->rocksdb);
}

int streamGetInit(const char* funcName) {
  for (int i = 0; i < sizeof(ginitDict) / sizeof(ginitDict[0]); i++) {
    if (strncmp(funcName, ginitDict[i].key, strlen(funcName)) == 0) {
      return i;
    }
  }
  return -1;
}

#define STREAM_STATE_PUT_ROCKSDB(pState, funcname, key, value, vLen)                                           \
  do {                                                                                                         \
    char  buf[128] = {0};                                                                                      \
    char* err = NULL;                                                                                          \
    int   i = streamGetInit(funcname);                                                                         \
    if (i < 0) {                                                                                               \
      return -1;                                                                                               \
    }                                                                                                          \
    ginitDict[i].enFunc((void*)key, buf);                                                                      \
    rocksdb_column_family_handle_t* pHandle = pState->pTdbState->pHandle[ginitDict[i].idx];                    \
    rocksdb_t*                      db = pState->pTdbState->rocksdb;                                           \
    rocksdb_writeoptions_t*         opts = pState->pTdbState->wopts;                                           \
    rocksdb_put_cf(db, opts, pHandle, (const char*)buf, sizeof(*key), (const char*)value, (size_t)vLen, &err); \
    if (err != NULL) {                                                                                         \
      taosMemoryFree(err);                                                                                     \
      code = -1;                                                                                               \
    }                                                                                                          \
    code = 0;                                                                                                  \
  } while (0);

#define STREAM_STATE_GET_ROCKSDB(pState, funcname, key, pVal, vLen)                                     \
  do {                                                                                                  \
    char  buf[128] = {0};                                                                               \
    char* err = NULL;                                                                                   \
    int   i = streamGetInit(funcname);                                                                  \
    if (i < 0) {                                                                                        \
      return -1;                                                                                        \
    }                                                                                                   \
    ginitDict[i].enFunc((void*)key, buf);                                                               \
    rocksdb_column_family_handle_t* pHandle = pState->pTdbState->pHandle[ginitDict[i].idx];             \
    rocksdb_t*                      db = pState->pTdbState->rocksdb;                                    \
    rocksdb_readoptions_t*          opts = pState->pTdbState->ropts;                                    \
    char* val = rocksdb_get_cf(db, opts, pHandle, (const char*)buf, sizeof(*key), (size_t*)vLen, &err); \
    *pVal = val;                                                                                        \
    if (err != NULL) {                                                                                  \
      taosMemoryFree(err);                                                                              \
      code = -1;                                                                                        \
    }                                                                                                   \
    code = 0;                                                                                           \
  } while (0);

#define STREAM_STATE_DEL_ROCKSDB(pState, funcname, key)                                     \
  do {                                                                                      \
    char  buf[128] = {0};                                                                   \
    char* err = NULL;                                                                       \
    int   i = streamGetInit(funcname);                                                      \
    if (i < 0) {                                                                            \
      return -1;                                                                            \
    }                                                                                       \
    ginitDict[i].enFunc((void*)key, buf);                                                   \
    rocksdb_column_family_handle_t* pHandle = pState->pTdbState->pHandle[ginitDict[i].idx]; \
    rocksdb_t*                      db = pState->pTdbState->rocksdb;                        \
    rocksdb_writeoptions_t*         opts = pState->pTdbState->wopts;                        \
    rocksdb_delete_cf(db, opts, pHandle, (const char*)buf, sizeof(*key), &err);             \
    if (err != NULL) {                                                                      \
      taosMemoryFree(err);                                                                  \
      code = -1;                                                                            \
    }                                                                                       \
    code = 0;                                                                               \
  } while (0);

int32_t streamStateFuncPut_rocksdb(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "func", key, value, vLen);
  return code;
}
int32_t streamStateFuncGet_rocksdb(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "func", key, pVal, pVLen);
  return 0;
}
int32_t streamStateFuncDel_rocksdb(SStreamState* pState, const STupleKey* key) {
  int code = 0;
  STREAM_STATE_DEL_ROCKSDB(pState, "func", key);
  return 0;
}

int32_t streamStatePut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  int code = 0;

  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_PUT_ROCKSDB(pState, "default", key, value, vLen);
  return code;
}
int32_t streamStateGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  int       code = 0;
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_GET_ROCKSDB(pState, "default", key, pVal, pVLen);
  return code;
}
// todo refactor
int32_t streamStateDel_rocksdb(SStreamState* pState, const SWinKey* key) {
  int       code = 0;
  SStateKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_DEL_ROCKSDB(pState, "default", key);
  return code;
}

// todo refactor
int32_t streamStateFillPut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen) {
  int code = 0;

  STREAM_STATE_PUT_ROCKSDB(pState, "fill", key, value, vLen);
  return code;
}

// todo refactor
int32_t streamStateFillGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "fill", key, pVal, pVLen);
  return code;
}

int32_t streamStateSessionPut_rocksdb(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen) {
  int              code = 0;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_PUT_ROCKSDB(pState, "sess", key, value, vLen);
  return code;
}
SStreamStateCur* streamStateSessionSeekKeyCurrentPrev_rocksdb(SStreamState* pState, const SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->number = pState->number;

  pCur->iter =
      rocksdb_create_iterator_cf(pState->pTdbState->rocksdb, pState->pTdbState->ropts, pState->pTdbState->pHandle[2]);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) >= 0) return pCur;

  rocksdb_iter_prev(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}
SStreamStateCur* streamStateSessionSeekKeyCurrentNext_rocksdb(SStreamState* pState, SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->iter =
      rocksdb_create_iterator_cf(pState->pTdbState->rocksdb, pState->pTdbState->ropts, pState->pTdbState->pHandle[2]);
  pCur->number = pState->number;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  char             buf[128] = {0};
  stateSessionKeyEncode(&sKey, buf);
  rocksdb_iter_seek(pCur->iter, (const char*)buf, sizeof(sKey));
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) <= 0) return pCur;

  rocksdb_iter_next(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

SStreamStateCur* streamStateSessionSeekKeyNext_rocksdb(SStreamState* pState, const SSessionKey* key) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return NULL;
  }
  pCur->iter =
      rocksdb_create_iterator_cf(pState->pTdbState->rocksdb, pState->pTdbState->ropts, pState->pTdbState->pHandle[2]);
  pCur->number = pState->number;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  char             buf[128] = {0};
  stateSessionKeyEncode(&sKey, buf);
  rocksdb_iter_seek(pCur->iter, (const char*)buf, sizeof(sKey));
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  size_t           klen;
  const char*      iKey = rocksdb_iter_key(pCur->iter, &klen);
  SStateSessionKey curKey = {0};
  stateSessionKeyDecode(&curKey, (char*)iKey);
  if (stateSessionKeyCmpr(&sKey, sizeof(sKey), &curKey, sizeof(curKey)) < 0) return pCur;

  rocksdb_iter_next(pCur->iter);
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return NULL;
  }
  return pCur;
}

int32_t streamStateSessionGetKVByCur_rocksdb(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen) {
  if (!pCur) {
    return -1;
  }
  SStateSessionKey  ktmp = {0};
  SStateSessionKey* pKTmp = &ktmp;
  int32_t           kLen, vLen;

  if (!rocksdb_iter_valid(pCur->iter)) {
    return -1;
  }
  const char* curKey = rocksdb_iter_key(pCur->iter, (size_t*)&kLen);
  stateSessionKeyDecode((void*)&ktmp, (char*)curKey);

  const char* val = rocksdb_iter_value(pCur->iter, (size_t*)&vLen);
  if (pVal != NULL) *pVal = (char*)val;
  if (pVLen != NULL) *pVLen = vLen;

  if (pKTmp->opNum != pCur->number) {
    return -1;
  }
  if (pKey->groupId != 0 && pKey->groupId != pKTmp->key.groupId) {
    return -1;
  }
  *pKey = pKTmp->key;
  return 0;
}

int32_t streamStateCurNext_rocksdb(SStreamState* pState, SStreamStateCur* pCur) {
  if (!pCur) {
    return -1;
  }
  rocksdb_iter_next(pCur->iter);
  return 0;
}
int32_t streamStateSessionGetKeyByRange_rocksdb(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey) {
  SStreamStateCur* pCur = taosMemoryCalloc(1, sizeof(SStreamStateCur));
  if (pCur == NULL) {
    return -1;
  }
  pCur->number = pState->number;
  pCur->iter =
      rocksdb_create_iterator_cf(pState->pTdbState->rocksdb, pState->pTdbState->ropts, pState->pTdbState->pHandle[2]);

  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  int32_t          c = 0;
  char             buf[128] = {0};
  stateSessionKeyEncode(&sKey, buf);
  rocksdb_iter_seek(pCur->iter, buf, sizeof(sKey));
  if (!rocksdb_iter_valid(pCur->iter)) {
    streamStateFreeCur(pCur);
    return -1;
  }

  int32_t          kLen;
  const char*      iKeyStr = rocksdb_iter_key(pCur->iter, (size_t*)&kLen);
  SStateSessionKey iKey = {0};
  stateSessionKeyDecode(&iKey, (char*)iKeyStr);

  c = stateSessionKeyCmpr(&sKey, sizeof(sKey), &iKey, sizeof(iKey));

  SSessionKey resKey = *key;
  int32_t     code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
  if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
    *curKey = resKey;
    streamStateFreeCur(pCur);
    return code;
  }

  if (c > 0) {
    streamStateCurNext_rocksdb(pState, pCur);
    code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  } else if (c < 0) {
    streamStateCurPrev(pState, pCur);
    code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, NULL, 0);
    if (code == 0 && sessionRangeKeyCmpr(key, &resKey) == 0) {
      *curKey = resKey;
      streamStateFreeCur(pCur);
      return code;
    }
  }

  streamStateFreeCur(pCur);
  return -1;
}

int32_t streamStateSessionGet_rocksdb(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen) {
  int              code = 0;
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, key);
  SSessionKey      resKey = *key;
  void*            tmp = NULL;
  code = streamStateSessionGetKVByCur_rocksdb(pCur, &resKey, &tmp, pVLen);
  if (code == 0) {
    if (key->win.skey != resKey.win.skey) {
      code = -1;
    } else {
      *key = resKey;
      *pVal = tmp;
    }
  }
  streamStateFreeCur(pCur);

  // impl later
  return code;
}

int32_t streamStateSessionDel_rocksdb(SStreamState* pState, const SSessionKey* key) {
  int              code = 0;
  SStateSessionKey sKey = {.key = *key, .opNum = pState->number};
  STREAM_STATE_DEL_ROCKSDB(pState, "sess", key);
  return code;
}
int32_t streamStateSessionAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                                int32_t* pVLen) {
  // todo refactor
  int32_t     res = 0;
  SSessionKey originKey = *key;
  SSessionKey searchKey = *key;
  searchKey.win.skey = key->win.skey - gap;
  searchKey.win.ekey = key->win.ekey + gap;
  int32_t valSize = *pVLen;

  void* tmp = taosMemoryMalloc(valSize);

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev_rocksdb(pState, key);
  int32_t          code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel_rocksdb(pState, key);
      goto _end;
    }
    streamStateCurNext_rocksdb(pState, pCur);
  } else {
    *key = originKey;
    streamStateFreeCur(pCur);
    pCur = streamStateSessionSeekKeyNext_rocksdb(pState, key);
  }

  code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    if (sessionRangeKeyCmpr(&searchKey, key) == 0) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel_rocksdb(pState, key);
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
int32_t streamStateStateAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, char* pKeyData,
                                              int32_t keyDataLen, state_key_cmpr_fn fn, void** pVal, int32_t* pVLen) {
  // todo refactor
  int32_t     res = 0;
  SSessionKey tmpKey = *key;
  int32_t     valSize = *pVLen;
  void*       tmp = taosMemoryMalloc(valSize);
  // tdbRealloc(NULL, valSize);
  if (!tmp) {
    return -1;
  }

  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentPrev_rocksdb(pState, key);
  int32_t          code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
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

    streamStateCurNext_rocksdb(pState, pCur);
  } else {
    *key = tmpKey;
    streamStateFreeCur(pCur);
    pCur = streamStateSessionSeekKeyNext_rocksdb(pState, key);
  }

  code = streamStateSessionGetKVByCur_rocksdb(pCur, key, pVal, pVLen);
  if (code == 0) {
    void* stateKey = (char*)(*pVal) + (valSize - keyDataLen);
    if (fn(pKeyData, stateKey) == true) {
      memcpy(tmp, *pVal, valSize);
      streamStateSessionDel_rocksdb(pState, key);
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

int32_t streamStateSessionClear(SStreamState* pState) {
  SSessionKey      key = {.win.skey = 0, .win.ekey = 0, .groupId = 0};
  SStreamStateCur* pCur = streamStateSessionSeekKeyCurrentNext_rocksdb(pState, &key);

  while (1) {
    SSessionKey delKey = {0};
    void*       buf = NULL;
    int32_t     size = 0;
    int32_t     code = streamStateSessionGetKVByCur_rocksdb(pCur, &delKey, &buf, &size);
    if (code == 0 && size > 0) {
      memset(buf, 0, size);
      streamStateSessionPut_rocksdb(pState, &delKey, buf, size);
    } else {
      break;
    }
    streamStateCurNext_rocksdb(pState, pCur);
  }
  streamStateFreeCur(pCur);
  return -1;
}
int32_t streamStatePutParTag_rocksdb(SStreamState* pState, int64_t groupId, const void* tag, int32_t tagLen) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "partag", &groupId, tag, tagLen);
  return code;
}

int32_t streamStateGetParTag_rocksdb(SStreamState* pState, int64_t groupId, void** tagVal, int32_t* tagLen) {
  int code = 0;
  STREAM_STATE_GET_ROCKSDB(pState, "partag", &groupId, tagVal, tagLen);
  return code;
}

int32_t streamStatePutParName_rocksdb(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]) {
  int code = 0;
  STREAM_STATE_PUT_ROCKSDB(pState, "parname", &groupId, tbname, TSDB_TABLE_NAME_LEN);
  return code;
}
int32_t streamStateGetParName_rocksdb(SStreamState* pState, int64_t groupId, void** pVal) {
  int    code = 0;
  size_t tagLen;
  STREAM_STATE_GET_ROCKSDB(pState, "parname", &groupId, pVal, &tagLen);
  return code;
}

void streamStateDestroy_rocksdb(SStreamState* pState) {
  // only close db
  streamCleanBackend(pState);
}