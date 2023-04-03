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

#include "tdef.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamFileState SStreamFileState;
typedef struct SRowBuffPos {
  void*    pRowBuff;
  void*    pKey;
  bool     beFlushed;
  bool     beUsed;
} SRowBuffPos;

typedef SList SStreamSnapshot;

typedef bool (*ExpiredFun)(void*, TSKEY);

SStreamFileState* streamFileStateInit(int64_t memSize, uint32_t rowSize, ExpiredFun fp, void* pFile);
void              destroyStreamFileState(SStreamFileState* pFileState);

int32_t getRowBuff(SStreamFileState* pFileState, void* pKey, int32_t keyLen, void** pVal, int32_t* pVLen);
void* getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos);

SStreamSnapshot* getSnapshot(SStreamFileState* pFileState);
int32_t flushSnapshot(void* pFile, SStreamSnapshot* pSnapshot, int32_t rowSize);
int32_t recoverSnapshot(SStreamFileState* pFileState);

#ifdef __cplusplus
}
#endif

#endif  // _STREAM_FILE_STATE_H_
