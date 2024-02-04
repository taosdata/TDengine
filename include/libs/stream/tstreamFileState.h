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

#ifndef _STREAM_FILE_STATE_H_
#define _STREAM_FILE_STATE_H_

#include "os.h"

#include "tarray.h"
#include "tdef.h"
#include "tlist.h"
#include "storageapi.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamFileState SStreamFileState;
typedef SList                   SStreamSnapshot;

typedef void*   (*_state_buff_get_fn)(void* pRowBuff, const void* pKey, size_t keyLen);
typedef int32_t (*_state_buff_put_fn)(void* pRowBuff, const void* pKey, size_t keyLen, const void* data, size_t dataLen);
typedef int32_t (*_state_buff_remove_fn)(void* pRowBuff, const void* pKey, size_t keyLen);
typedef int32_t (*_state_buff_remove_by_pos_fn)(SStreamFileState* pState, SRowBuffPos* pPos);
typedef void    (*_state_buff_cleanup_fn)(void* pRowBuff);
typedef void*   (*_state_buff_create_statekey_fn)(SRowBuffPos* pPos, int64_t num);

typedef int32_t (*_state_file_remove_fn)(SStreamFileState* pFileState, const void* pKey);
typedef int32_t (*_state_file_get_fn)(SStreamFileState* pFileState, void* pKey, void* data, int32_t* pDataLen);
typedef int32_t (*_state_file_clear_fn)(SStreamState* pState);

typedef int32_t (*range_cmpr_fn)(const SSessionKey* pWin1, const SSessionKey* pWin2);

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize,
                                      GetTsFun fp, void* pFile, TSKEY delMark, const char* taskId,
                                      int64_t checkpointId, int8_t type);
void              streamFileStateDestroy(SStreamFileState* pFileState);
void              streamFileStateClear(SStreamFileState* pFileState);
bool              needClearDiskBuff(SStreamFileState* pFileState);
void              streamFileStateReleaseBuff(SStreamFileState* pFileState, SRowBuffPos* pPos, bool used);
int32_t           streamFileStateClearBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen);
int32_t deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen);
int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal);
bool    hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen);
void    putFreeBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState);
int32_t          flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState);
int32_t          recoverSnapshot(SStreamFileState* pFileState, int64_t ckId);

int32_t getSnapshotIdList(SStreamFileState* pFileState, SArray* list);
int32_t deleteExpiredCheckPoint(SStreamFileState* pFileState, TSKEY mark);
int32_t streamFileStateGeSelectRowSize(SStreamFileState* pFileState);
void    streamFileStateReloadInfo(SStreamFileState* pFileState, TSKEY ts);

void* getRowStateBuff(SStreamFileState* pFileState);
void* getStateFileStore(SStreamFileState* pFileState);
bool isDeteled(SStreamFileState* pFileState, TSKEY ts);
bool isFlushedState(SStreamFileState* pFileState, TSKEY ts, TSKEY gap);
SRowBuffPos* getNewRowPosForWrite(SStreamFileState* pFileState);
int32_t getRowStateRowSize(SStreamFileState* pFileState);

// session window
int32_t getSessionWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, TSKEY gap, void** pVal, int32_t* pVLen);
int32_t putSessionWinResultBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);
int32_t getSessionFlushedBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t deleteSessionWinStateBuffFn(void* pBuff, const void* key, size_t keyLen);
int32_t deleteSessionWinStateBuffByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos);
int32_t allocSessioncWinBuffByNextPosition(SStreamFileState* pFileState, SStreamStateCur* pCur,
                                           const SSessionKey* pWinKey, void** ppVal, int32_t* pVLen);

SRowBuffPos* createSessionWinBuff(SStreamFileState* pFileState, SSessionKey* pKey, void* p, int32_t* pVLen);
int32_t      recoverSesssion(SStreamFileState* pFileState, int64_t ckId);

void sessionWinStateClear(SStreamFileState* pFileState);
void sessionWinStateCleanup(void* pBuff);

SStreamStateCur* sessionWinStateSeekKeyCurrentPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* sessionWinStateSeekKeyCurrentNext(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* sessionWinStateSeekKeyNext(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* countWinStateSeekKeyPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey, COUNT_TYPE count);
int32_t sessionWinStateGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t sessionWinStateMoveToNext(SStreamStateCur* pCur);
int32_t sessionWinStateGetKeyByRange(SStreamFileState* pFileState, const SSessionKey* key, SSessionKey* curKey, range_cmpr_fn cmpFn);

// state window
int32_t getStateWinResultBuff(SStreamFileState* pFileState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                             state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);

// count window
int32_t getCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal, int32_t* pVLen);
int32_t createCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen);

#ifdef __cplusplus
}
#endif

#endif  // _STREAM_FILE_STATE_H_
