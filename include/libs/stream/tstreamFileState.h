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

#include "storageapi.h"
#include "tarray.h"
#include "tdef.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamFileState SStreamFileState;
typedef SList                   SStreamSnapshot;

typedef void* (*_state_buff_get_fn)(void* pRowBuff, const void* pKey, size_t keyLen);
typedef int32_t (*_state_buff_remove_fn)(void* pRowBuff, const void* pKey, size_t keyLen);
typedef void (*_state_buff_remove_by_pos_fn)(SStreamFileState* pState, SRowBuffPos* pPos);
typedef void (*_state_buff_cleanup_fn)(void* pRowBuff);
typedef void* (*_state_buff_create_statekey_fn)(SRowBuffPos* pPos, int64_t num);

typedef int32_t (*_state_file_remove_fn)(SStreamFileState* pFileState, const void* pKey);
typedef int32_t (*_state_file_get_fn)(SStreamFileState* pFileState, void* pKey, void** data, int32_t* pDataLen);
typedef int32_t (*_state_file_clear_fn)(SStreamState* pState);

typedef int32_t (*_state_fun_get_fn)(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal,
                                     int32_t* pVLen, int32_t* pWinCode);

typedef int32_t (*range_cmpr_fn)(const SSessionKey* pWin1, const SSessionKey* pWin2);

typedef int (*__session_compare_fn_t)(const void* pWin, const void* pDatas, int pos);

int32_t streamFileStateInit(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize, GetTsFun fp,
                            void* pFile, TSKEY delMark, const char* taskId, int64_t checkpointId, int8_t type,
                            struct SStreamFileState** ppFileState);
void              streamFileStateDestroy(SStreamFileState* pFileState);
void              streamFileStateClear(SStreamFileState* pFileState);
bool              needClearDiskBuff(SStreamFileState* pFileState);
void              streamFileStateReleaseBuff(SStreamFileState* pFileState, SRowBuffPos* pPos, bool used);
void              streamFileStateClearBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);

int32_t addRowBuffIfNotExist(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                             int32_t* pWinCode);
int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                   int32_t* pWinCode);
void    deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen);
int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal);
bool    hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen);
int32_t putFreeBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState);
void             flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState);
int32_t          recoverSnapshot(SStreamFileState* pFileState, int64_t ckId);

int32_t getSnapshotIdList(SStreamFileState* pFileState, SArray* list);
int32_t deleteExpiredCheckPoint(SStreamFileState* pFileState, TSKEY mark);
int32_t streamFileStateGetSelectRowSize(SStreamFileState* pFileState);
void    streamFileStateReloadInfo(SStreamFileState* pFileState, TSKEY ts);

void*        getRowStateBuff(SStreamFileState* pFileState);
void*        getSearchBuff(SStreamFileState* pFileState);
void*        getStateFileStore(SStreamFileState* pFileState);
bool         isDeteled(SStreamFileState* pFileState, TSKEY ts);
bool         isFlushedState(SStreamFileState* pFileState, TSKEY ts, TSKEY gap);
TSKEY        getFlushMark(SStreamFileState* pFileState);
SRowBuffPos* getNewRowPosForWrite(SStreamFileState* pFileState);
int32_t      getRowStateRowSize(SStreamFileState* pFileState);

// session window
int32_t getSessionWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, TSKEY gap, void** pVal, int32_t* pVLen,
                                int32_t* pWinCode);
int32_t putSessionWinResultBuff(SStreamFileState* pFileState, SRowBuffPos* pPos);
int32_t getSessionFlushedBuff(SStreamFileState* pFileState, SSessionKey* pKey, void** pVal, int32_t* pVLen,
                              int32_t* pWinCode);
int32_t deleteSessionWinStateBuffFn(void* pBuff, const void* key, size_t keyLen);
void    deleteSessionWinStateBuffByPosFn(SStreamFileState* pFileState, SRowBuffPos* pPos);
int32_t allocSessioncWinBuffByNextPosition(SStreamFileState* pFileState, SStreamStateCur* pCur,
                                           const SSessionKey* pWinKey, void** ppVal, int32_t* pVLen);

SRowBuffPos* createSessionWinBuff(SStreamFileState* pFileState, SSessionKey* pKey, void* p, int32_t* pVLen);
int32_t      recoverSesssion(SStreamFileState* pFileState, int64_t ckId);

void sessionWinStateClear(SStreamFileState* pFileState);
void sessionWinStateCleanup(void* pBuff);

SStreamStateCur* createStateCursor(SStreamFileState* pFileState);
SStreamStateCur* sessionWinStateSeekKeyCurrentPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* sessionWinStateSeekKeyCurrentNext(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* sessionWinStateSeekKeyNext(SStreamFileState* pFileState, const SSessionKey* pWinKey);
SStreamStateCur* countWinStateSeekKeyPrev(SStreamFileState* pFileState, const SSessionKey* pWinKey, COUNT_TYPE count);
int32_t          sessionWinStateGetKVByCur(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
void             sessionWinStateMoveToNext(SStreamStateCur* pCur);
int32_t          sessionWinStateGetKeyByRange(SStreamFileState* pFileState, const SSessionKey* key, SSessionKey* curKey,
                                              range_cmpr_fn cmpFn);

int32_t binarySearch(void* keyList, int num, const void* key, __session_compare_fn_t cmpFn);

// state window
int32_t getStateWinResultBuff(SStreamFileState* pFileState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                              state_key_cmpr_fn fn, void** pVal, int32_t* pVLen, int32_t* pWinCode);

// count window
int32_t getCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal,
                              int32_t* pVLen, int32_t* pWinCode);
int32_t createCountWinResultBuff(SStreamFileState* pFileState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal, int32_t* pVLen);

// function
int32_t getSessionRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen,
                          int32_t* pWinCode);
int32_t getFunctionRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen);

// time slice
int32_t getHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey, void** pVal, int32_t* pVLen,
                           int32_t* pWinCode);
int32_t hashSortFileGetFn(SStreamFileState* pFileState, void* pKey, void** data, int32_t* pDataLen);
int32_t hashSortFileRemoveFn(SStreamFileState* pFileState, const void* pKey);
void    clearSearchBuff(SStreamFileState* pFileState);
int32_t getHashSortNextRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** pVal,
                           int32_t* pVLen, int32_t* pWinCode);
int32_t getHashSortPrevRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** ppVal,
                           int32_t* pVLen, int32_t* pWinCode);
int32_t recoverFillSnapshot(SStreamFileState* pFileState, int64_t ckId);
void    deleteHashSortRowBuff(SStreamFileState* pFileState, const SWinKey* pKey);

//group
int32_t streamFileStateGroupPut(SStreamFileState* pFileState, int64_t groupId, void* value, int32_t vLen);
void streamFileStateGroupCurNext(SStreamStateCur* pCur);
int32_t streamFileStateGroupGetKVByCur(SStreamStateCur* pCur, int64_t* pKey, void** pVal, int32_t* pVLen);
SSHashObj* getGroupIdCache(SStreamFileState* pFileState);
int fillStateKeyCompare(const void* pWin1, const void* pDatas, int pos);
int32_t getRowStatePrevRow(SStreamFileState* pFileState, const SWinKey* pKey, SWinKey* pResKey, void** ppVal,
                           int32_t* pVLen, int32_t* pWinCode);
int32_t addSearchItem(SStreamFileState* pFileState, SArray* pWinStates, const SWinKey* pKey);

//twa
void setFillInfo(SStreamFileState* pFileState);
void clearExpiredState(SStreamFileState* pFileState);
int32_t addArrayBuffIfNotExist(SSHashObj* pSearchBuff, uint64_t groupId, SArray** ppResStates);

#ifdef __cplusplus
}
#endif

#endif  // _STREAM_FILE_STATE_H_
