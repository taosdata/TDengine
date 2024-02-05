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

#ifndef _STREAM_STATE_H_
#define _STREAM_STATE_H_

#include "tdatablock.h"

#include "rocksdb/c.h"
#include "tdbInt.h"
#include "tsimplehash.h"
#include "tstreamFileState.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "storageapi.h"

SStreamState* streamStateOpen(char* path, void* pTask, bool specPath, int32_t szPage, int32_t pages);
void          streamStateClose(SStreamState* pState, bool remove);
int32_t       streamStateBegin(SStreamState* pState);
int32_t       streamStateCommit(SStreamState* pState);
void          streamStateDestroy(SStreamState* pState, bool remove);
int32_t       streamStateDeleteCheckPoint(SStreamState* pState, TSKEY mark);
int32_t       streamStateDelTaskDb(SStreamState* pState);

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

// session window
int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal, int32_t* pVLen);
int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, void* value, int32_t vLen);
int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateSessionDel(SStreamState* pState, const SSessionKey* key);
int32_t streamStateSessionReset(SStreamState* pState, void* pVal);
int32_t streamStateSessionClear(SStreamState* pState);
int32_t streamStateSessionGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t streamStateSessionGetKeyByRange(SStreamState* pState, const SSessionKey* range, SSessionKey* curKey);
int32_t streamStateCountGetKeyByRange(SStreamState* pState, const SSessionKey* range, SSessionKey* curKey);
int32_t streamStateSessionAllocWinBuffByNextPosition(SStreamState* pState, SStreamStateCur* pCur,
                                                     const SSessionKey* pKey, void** pVal, int32_t* pVLen);

SStreamStateCur* streamStateSessionSeekKeyNext(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateCountSeekKeyPrev(SStreamState* pState, const SSessionKey* pKey, COUNT_TYPE count);
SStreamStateCur* streamStateSessionSeekKeyCurrentPrev(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentNext(SStreamState* pState, const SSessionKey* key);

// state window
int32_t streamStateStateAddIfNotExist(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);

// fill
int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFillDel(SStreamState* pState, const SWinKey* key);

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateReleaseBuf(SStreamState* pState, void* pVal, bool used);
int32_t streamStateClearBuff(SStreamState* pState, void* pVal);
void    streamStateFreeVal(void* val);

// count window
int32_t streamStateCountWinAddIfNotExist(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** ppVal, int32_t* pVLen);
int32_t streamStateCountWinAdd(SStreamState* pState, SSessionKey* pKey, void** pVal, int32_t* pVLen);

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key);
SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key);
void             streamStateFreeCur(SStreamStateCur* pCur);
void             streamStateResetCur(SStreamStateCur* pCur);

int32_t streamStateGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

int32_t streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char* tbname);
int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal);

void streamStateReloadInfo(SStreamState* pState, TSKEY ts);

void streamStateCopyBackend(SStreamState* src, SStreamState* dst);

SStreamStateCur* createStreamStateCursor();

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

#ifdef __cplusplus
}
#endif

#endif /* ifndef _STREAM_STATE_H_ */