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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamFileState SStreamFileState;
typedef struct SRowBuffPos {
  SStatePage* pPage;
  void*       pRow;
} SRowBuffPos;

SStreamFileState* streamFileStateInit(int64_t memSize, const char* path);
void              destroyStreamFileState(SStreamFileState* pFileState);

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState, void* key, int32_t keyLen);
void* getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos);
// void* getRowBuff(SStreamFileState* pFileState, void* pPos);
void  releaseRowBuf(SStreamFileState* pFileState, void* buff);

void clearExpiredRow(SStreamFileState* pFileState, TSKEY ts);
SStreamFileState* getSnapshot(SStreamFileState* pFileState);

int32_t flushSnapshot(SStreamFileState* pFileState);
int32_t recoverSnapshot(SStreamFileState* pFileState);

#ifdef __cplusplus
}
#endif

#endif  // _STREAM_FILE_STATE_H_
