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
#include "tdbInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef _STREAM_STATE_H_
#define _STREAM_STATE_H_

typedef struct SStreamTask SStreamTask;

typedef bool (*state_key_cmpr_fn)(void* pKey1, void* pKey2);

typedef struct STdbState {
  SStreamTask* pOwner;
  TDB*         db;
  TTB*         pStateDb;
  TTB*         pFuncStateDb;
  TTB*         pFillStateDb;  // todo refactor
  TTB*         pSessionStateDb;
  TTB*         pParNameDb;
  TXN*         txn;
} STdbState;

// incremental state storage
typedef struct {
  STdbState* pTdbState;
  int32_t    number;
} SStreamState;

SStreamState* streamStateOpen(char* path, SStreamTask* pTask, bool specPath, int32_t szPage, int32_t pages);
void          streamStateClose(SStreamState* pState);
int32_t       streamStateBegin(SStreamState* pState);
int32_t       streamStateCommit(SStreamState* pState);
int32_t       streamStateAbort(SStreamState* pState);
void          streamStateDestroy(SStreamState* pState);

typedef struct {
  TBC*    pCur;
  int64_t number;
} SStreamStateCur;

int32_t streamStateFuncPut(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFuncDel(SStreamState* pState, const STupleKey* key);

int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateDel(SStreamState* pState, const SWinKey* key);
int32_t streamStateClear(SStreamState* pState);
void    streamStateSetNumber(SStreamState* pState, int32_t number);

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
void    streamFreeVal(void* val);

SStreamStateCur* streamStateGetCur(SStreamState* pState, const SWinKey* key);
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

#if 0
char* streamStateSessionDump(SStreamState* pState);
#endif

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_STATE_H_ */
