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

#ifndef TDENGINE_STREAM_VTABLE_MERGE_H
#define TDENGINE_STREAM_VTABLE_MERGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tcommon.h"
#include "tpagedbuf.h"

typedef struct SStreamVtableMergeHandle SStreamVtableMergeHandle;

typedef enum {
  SVM_NEXT_NOT_READY = 0,
  SVM_NEXT_FOUND = 1,
} SVM_NEXT_RESULT;

int32_t streamVtableMergeCreateHandle(SStreamVtableMergeHandle **ppHandle, int64_t vuid, int32_t nSrcTbls,
                                      int32_t numPageLimit, int32_t primaryTsIndex, SDiskbasedBuf *pBuf,
                                      SSDataBlock *pResBlock, const char *idstr);

void streamVtableMergeDestroyHandle(void *ppHandle);

int64_t streamVtableMergeHandleGetVuid(SStreamVtableMergeHandle *pHandle);

int32_t streamVtableMergeAddBlock(SStreamVtableMergeHandle *pHandle, SSDataBlock *pDataBlock, const char *idstr);

int32_t streamVtableMergeMoveNext(SStreamVtableMergeHandle *pHandle, SVM_NEXT_RESULT *pRes, const char *idstr);

int32_t streamVtableMergeCurrent(SStreamVtableMergeHandle *pHandle, SSDataBlock **ppDataBlock, int32_t *pRowIdx,
                                 const char *idstr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_VTABLE_MERGE_H
