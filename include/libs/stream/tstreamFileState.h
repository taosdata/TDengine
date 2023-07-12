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
typedef SList SStreamSnapshot;

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize,
                                      GetTsFun fp, void* pFile, TSKEY delMark, const char* taskId,
                                      int64_t checkpointId);
void              streamFileStateDestroy(SStreamFileState* pFileState);
void              streamFileStateClear(SStreamFileState* pFileState);
bool              needClearDiskBuff(SStreamFileState* pFileState);

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen);
int32_t deleteRowBuff(SStreamFileState* pFileState, const void* pKey, int32_t keyLen);
int32_t getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos, void** pVal);
void    releaseRowBuffPos(SRowBuffPos* pBuff);
bool    hasRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen);

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState);
int32_t          flushSnapshot(SStreamFileState* pFileState, SStreamSnapshot* pSnapshot, bool flushState);
int32_t          recoverSnapshot(SStreamFileState* pFileState, int64_t ckId);

int32_t getSnapshotIdList(SStreamFileState* pFileState, SArray* list);
int32_t deleteExpiredCheckPoint(SStreamFileState* pFileState, TSKEY mark);
int32_t streamFileStateGeSelectRowSize(SStreamFileState* pFileState);
void    streamFileStateReloadInfo(SStreamFileState* pFileState, TSKEY ts);

#ifdef __cplusplus
}
#endif

#endif  // _STREAM_FILE_STATE_H_
