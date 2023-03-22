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

#ifndef _STREAM_BACKEDN_ROCKSDB_H_
#define _STREAM_BACKEDN_ROCKSDB_H_

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

int  streamInitBackend(SStreamState* pState, char* path);
void streamCleanBackend(SStreamState* pState);

int32_t streamStateFuncPut_rocksdb(SStreamState* pState, const STupleKey* key, const void* value, int32_t vLen);
int32_t streamStateFuncGet_rocksdb(SStreamState* pState, const STupleKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFuncDel_rocksdb(SStreamState* pState, const STupleKey* key);
int32_t streamStatePut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateDel_rocksdb(SStreamState* pState, const SWinKey* key);
int32_t streamStateFillPut_rocksdb(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
int32_t streamStateFillGet_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateFillDel_rocksdb(SStreamState* pState, const SWinKey* key);
int32_t streamStateClear_rocksdb(SStreamState* pState);

int32_t streamStateSessionPut_rocksdb(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen);
SStreamStateCur* streamStateSessionSeekKeyCurrentPrev_rocksdb(SStreamState* pState, const SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyCurrentNext_rocksdb(SStreamState* pState, SSessionKey* key);
SStreamStateCur* streamStateSessionSeekKeyNext_rocksdb(SStreamState* pState, const SSessionKey* key);
int32_t streamStateSessionGetKVByCur_rocksdb(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
int32_t streamStateCurNext_rocksdb(SStreamState* pState, SStreamStateCur* pCur);
int32_t streamStateSessionGetKeyByRange_rocksdb(SStreamState* pState, const SSessionKey* key, SSessionKey* curKey);
int32_t streamStateSessionGet_rocksdb(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen);
int32_t streamStateSessionDel_rocksdb(SStreamState* pState, const SSessionKey* key);
int32_t streamStateSessionAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                                int32_t* pVLen);

int32_t streamStateStateAddIfNotExist_rocksdb(SStreamState* pState, SSessionKey* key, char* pKeyData,
                                              int32_t keyDataLen, state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);

int32_t streamStateGetFirst_rocksdb(SStreamState* pState, SWinKey* key);
int32_t streamStateSessionClear_rocksdb(SStreamState* pState);
int32_t streamStateCurPrev_rocksdb(SStreamState* pState, SStreamStateCur* pCur);

int32_t streamStateGetGroupKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

int32_t streamStateAddIfNotExist_rocksdb(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
SStreamStateCur* streamStateGetCur_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillGetCur_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateGetAndCheckCur_rocksdb(SStreamState* pState, SWinKey* key);
int32_t          streamStateGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
int32_t streamStateFillGetKVByCur_rocksdb(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
SStreamStateCur* streamStateSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyPrev_rocksdb(SStreamState* pState, const SWinKey* key);
SStreamStateCur* streamStateFillSeekKeyNext_rocksdb(SStreamState* pState, const SWinKey* key);
int32_t          streamStatePutParTag_rocksdb(SStreamState* pState, int64_t groupId, const void* tag, int32_t tagLen);
int32_t          streamStateGetParTag_rocksdb(SStreamState* pState, int64_t groupId, void** tagVal, int32_t* tagLen);
int32_t streamStatePutParName_rocksdb(SStreamState* pState, int64_t groupId, const char tbname[TSDB_TABLE_NAME_LEN]);
int32_t streamStateGetParName_rocksdb(SStreamState* pState, int64_t groupId, void** pVal);
void    streamStateDestroy_rocksdb(SStreamState* pState);
#endif