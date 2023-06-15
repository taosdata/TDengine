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

#include "tdatablock.h"

#include "rocksdb/c.h"
#include "tdbInt.h"
#include "tsimplehash.h"
#include "tstreamFileState.h"

#ifndef _STREAM_STATE_H_
#define _STREAM_STATE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "storageapi.h"

// void*      streamBackendInit(const char* path);
// void       streamBackendCleanup(void* arg);
// SListNode* streamBackendAddCompare(void* backend, void* arg);
// void       streamBackendDelCompare(void* backend, void* arg);

// <<<<<<< HEAD
// typedef struct STdbState {
//   rocksdb_t*                       rocksdb;
//   rocksdb_column_family_handle_t** pHandle;
//   rocksdb_writeoptions_t*          writeOpts;
//   rocksdb_readoptions_t*           readOpts;
//   rocksdb_options_t**              cfOpts;
//   rocksdb_options_t*               dbOpt;
//   struct SStreamTask*              pOwner;
//   void*                            param;
//   void*                            env;
//   SListNode*                       pComparNode;
//   void*                            pBackend;
//   char                             idstr[64];
//   void*                            compactFactory;
//   TdThreadRwlock                   rwLock;
// =======
// typedef struct STdbState {
//  rocksdb_t*                       rocksdb;
//  rocksdb_column_family_handle_t** pHandle;
//  rocksdb_writeoptions_t*          writeOpts;
//  rocksdb_readoptions_t*           readOpts;
//  rocksdb_options_t**              cfOpts;
//  rocksdb_options_t*               dbOpt;
//  struct SStreamTask*              pOwner;
//  void*                            param;
//  void*                            env;
//  SListNode*                       pComparNode;
//  void*                            pBackendHandle;
//  char                             idstr[64];
//  void*                            compactFactory;
//
//  TDB* db;
//  TTB* pStateDb;
//  TTB* pFuncStateDb;
//  TTB* pFillStateDb;  // todo refactor
//  TTB* pSessionStateDb;
//  TTB* pParNameDb;
//  TTB* pParTagDb;
//  TXN* txn;
//} STdbState;
//>>>>>>> enh/dev3.0

SStreamState* streamStateOpen(char* path, void* pTask, bool specPath, int32_t szPage, int32_t pages);
void          streamStateClose(SStreamState* pState, bool remove);
int32_t       streamStateBegin(SStreamState* pState);
int32_t       streamStateCommit(SStreamState* pState);
void          streamStateDestroy(SStreamState* pState, bool remove);
int32_t       streamStateDeleteCheckPoint(SStreamState* pState, TSKEY mark);

int32_t streamStateFuncPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet(SStreamState* pState, const SWinKey* key, void** ppVal, int32_t* pVLen);

int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
bool    streamStateCheck(SStreamState* pState, const SWinKey* key);
int32_t streamStateGetByPos(SStreamState* pState, void* pos, void** pVal);
int32_t streamStateDel(SStreamState* pState, const SWinKey* key);
int32_t streamStateClear(SStreamState* pState);
void    streamStateSetNumber(SStreamState* pState, int32_t number);
int32_t streamStateSaveInfo(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen);
int32_t streamStateGetInfo(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen);

int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal, int32_t* pVLen);
int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen);
int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateSessionDel(SStreamState* pState, const SSessionKey* key);
int32_t streamStateSessionClear(SStreamState* pState);
int32_t streamStateSessionGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t streamStateStateAddIfNotExist(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);
int32_t streamStateSessionGetKeyByRange(SStreamState* pState, const SSessionKey* range, SSessionKey* curKey);

SStreamStateCur* streamStateSessionSeekKeyNext(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentPrev(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentNext(SStreamState* pState, const SSessionKey* key);

int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFillDel(SStreamState* pState, const SWinKey* key);

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateReleaseBuf(SStreamState* pState, const SWinKey* key, void* pVal);
void    streamStateFreeVal(void* val);

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key);
SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key);
void             streamStateFreeCur(SStreamStateCur* pCur);

int32_t streamStateGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

int32_t streamStateGetFirst(SStreamState* pState, SWinKey* key);
int32_t streamStateSeekFirst(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateSeekLast(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char* tbname);
int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal);

void streamStateReloadInfo(SStreamState* pState, TSKEY ts);

/***compare func **/

typedef struct SStateChekpoint {
  char*   taskName;
  int64_t checkpointId;
} SStateChekpoint;
// todo refactor
typedef struct SStateKey {
  SWinKey key;
  int64_t opNum;
} SStateKey;

typedef struct SStateSessionKey {
  SSessionKey key;
  int64_t     opNum;
} SStateSessionKey;

typedef struct SStreamValue {
  int64_t unixTimestamp;
  int32_t len;
  char*   data;
} SStreamValue;

int sessionRangeKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2);
int sessionWinKeyCmpr(const SSessionKey* pWin1, const SSessionKey* pWin2);
int stateSessionKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
int stateKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2);
#if 0
char* streamStateSessionDump(SStreamState* pState);
char* streamStateIntervalDump(SStreamState* pState);
#endif

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_STATE_H_ */
