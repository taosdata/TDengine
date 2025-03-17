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

SStreamState* streamStateOpen(const char* path, void* pTask, int64_t streamId, int32_t taskId);
void          streamStateClose(SStreamState* pState, bool remove);
int32_t       streamStateBegin(SStreamState* pState);
void          streamStateCommit(SStreamState* pState);
void          streamStateDestroy(SStreamState* pState, bool remove);
int32_t       streamStateDelTaskDb(SStreamState* pState);

int32_t streamStateFuncPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet(SStreamState* pState, const SWinKey* key, void** ppVal, int32_t* pVLen);

int32_t streamStatePut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode);
bool    streamStateCheck(SStreamState* pState, const SWinKey* key);
int32_t streamStateGetByPos(SStreamState* pState, void* pos, void** pVal);
void    streamStateDel(SStreamState* pState, const SWinKey* key);
void    streamStateDelByGroupId(SStreamState* pState, uint64_t groupId);
void    streamStateClear(SStreamState* pState);
void    streamStateSetNumber(SStreamState* pState, int32_t number, int32_t tsIdex);
void    streamStateSaveInfo(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen);
int32_t streamStateGetInfo(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen);
int32_t streamStateGetPrev(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                           int32_t* pWinCode);

// session window
int32_t streamStateSessionAddIfNotExist(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal, int32_t* pVLen,
                                        int32_t* pWinCode);
int32_t streamStateSessionPut(SStreamState* pState, const SSessionKey* key, void* value, int32_t vLen);
int32_t streamStateSessionGet(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode);
void    streamStateSessionDel(SStreamState* pState, const SSessionKey* key);
void    streamStateSessionReset(SStreamState* pState, void* pVal);
void    streamStateSessionClear(SStreamState* pState);
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
                                      state_key_cmpr_fn fn, void** pVal, int32_t* pVLen, int32_t* pWinCode);

// fill
int32_t streamStateFillPut(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFillGet(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode);
int32_t streamStateFillAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen,
                                        int32_t* pWinCode);
void    streamStateFillDel(SStreamState* pState, const SWinKey* key);
int32_t streamStateFillGetNext(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                               int32_t* pWinCode);
int32_t streamStateFillGetPrev(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal, int32_t* pVLen,
                               int32_t* pWinCode);

int32_t streamStateAddIfNotExist(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen,
                                 int32_t* pWinCode);
void    streamStateReleaseBuf(SStreamState* pState, void* pVal, bool used);
void    streamStateClearBuff(SStreamState* pState, void* pVal);
void    streamStateFreeVal(void* val);

// count window
int32_t streamStateCountWinAddIfNotExist(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** ppVal,
                                         int32_t* pVLen, int32_t* pWinCode);
int32_t streamStateCountWinAdd(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal, int32_t* pVLen);

SStreamStateCur* streamStateGetAndCheckCur(SStreamState* pState, SWinKey* key);
SStreamStateCur* streamStateSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyNext(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyPrev(SStreamState* pState, const SWinKey* key);
void             streamStateFreeCur(SStreamStateCur* pCur);
void             streamStateResetCur(SStreamStateCur* pCur);

int32_t streamStateFillGetGroupKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
int32_t streamStateGetKVByCur(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

// twa
void streamStateClearExpiredState(SStreamState* pState);

void streamStateCurNext(SStreamState* pState, SStreamStateCur* pCur);
void streamStateCurPrev(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStatePutParName(SStreamState* pState, int64_t groupId, const char* tbname);
int32_t streamStateGetParName(SStreamState* pState, int64_t groupId, void** pVal, bool onlyCache, int32_t* pWinCode);
int32_t streamStateDeleteParName(SStreamState* pState, int64_t groupId);

// group id
int32_t streamStateGroupPut(SStreamState* pState, int64_t groupId, void* value, int32_t vLen);
SStreamStateCur* streamStateGroupGetCur(SStreamState* pState);
void streamStateGroupCurNext(SStreamStateCur* pCur);
int32_t streamStateGroupGetKVByCur(SStreamStateCur* pCur, int64_t* pKey, void** pVal, int32_t* pVLen);

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
  int32_t rawLen;
  int8_t  compress;
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