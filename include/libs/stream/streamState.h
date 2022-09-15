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

// incremental state storage
typedef struct {
  SStreamTask* pOwner;
  TDB*         db;
  TTB*         pStateDb;
  TTB*         pFuncStateDb;
  TXN          txn;
} SStreamState;

SStreamState* streamStateOpen(char* path, SStreamTask* pTask);
void          streamStateClose(SStreamState* pState);
int32_t       streamStateBegin(SStreamState* pState);
int32_t       streamStateCommit(SStreamState* pState);
int32_t       streamStateAbort(SStreamState* pState);

typedef struct {
  TBC* pCur;
} SStreamStateCur;

#if 1
int32_t streamStateFuncPut(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFuncDel(SStreamState* pState, const STupleKey* key);

int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateDel(SStreamState* pState, const SWinKey* key);
int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateReleaseBuf(SStreamState* pState, const SWinKey* key, void* pVal);
void    streamFreeVal(void* val);

SStreamStateCur* streamStateGetCur(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateSeekKeyPrev(SStreamState* pState, const SWinKey* key);
void             streamStateFreeCur(SStreamStateCur* pCur);

int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

int32_t streamStateSeekFirst(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateSeekLast(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur);

#endif

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_STATE_H_ */
