/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef TDENGINE_WINDOWFUNC_H
#define TDENGINE_WINDOWFUNC_H

#ifdef __cplusplus
extern "C" {
#endif

#include "querynodes.h"
#include "tcommon.h"
#include "tpagedbuf.h"

typedef struct SSqlWindowFrameRange {
  int64_t start;
  int64_t end;
} SSqlWindowFrameRange;

typedef struct SWindowInputPool  SWindowInputPool;
typedef struct SWindowInputStore SWindowInputStore;

int32_t winCalcRowsFrame(int64_t rowIndex, int64_t partitionRows, const SWindowFrameNode *pFrame,
                         SSqlWindowFrameRange *pRange);
int32_t winCalcRangeFrameForInt64(const int64_t *values, int64_t rows, int64_t rowIndex, int64_t preceding,
                                  int64_t following, SSqlWindowFrameRange *pRange);
int32_t winCalcRangeFrameForDouble(const double *values, int64_t rows, int64_t rowIndex, double preceding,
                                   double following, SSqlWindowFrameRange *pRange);
int32_t winCalcRankValue(int64_t rowIndex, int64_t peerStart, int64_t denseRank, int64_t *pRank);
int32_t winCalcPercentRank(int64_t rank, int64_t partitionRows, double *pValue);
int32_t winCalcCumeDist(int64_t peerEnd, int64_t partitionRows, double *pValue);
int32_t winFuncCheckDedicatedFallback(const char *pFuncName);
int32_t winCalcOutputBatchEnd(int64_t totalRows, int64_t startRow, int64_t capacity, int64_t *pEndRow);
int32_t winInputPoolCreate(int32_t pageSize, int64_t inMemBufSize, const char *id, SWindowInputPool **ppPool);
void    winInputPoolDestroy(SWindowInputPool *pPool);
int32_t winInputPoolGetPageSize(const SWindowInputPool *pPool);
SDiskbasedBufStatis winInputPoolGetStatis(const SWindowInputPool *pPool);
int32_t winInputStoreCreate(const SSDataBlock *pTemplate, SWindowInputPool *pPool, SWindowInputStore **ppStore);
void    winInputStoreDestroy(SWindowInputStore *pStore);
int32_t winInputStoreAppendBlock(SWindowInputStore *pStore, SSDataBlock *pBlock);
int32_t winInputStoreGetRows(const SWindowInputStore *pStore);
int32_t winInputStoreGetPageCount(const SWindowInputStore *pStore);
int32_t winInputStoreGetBlock(SWindowInputStore *pStore, int32_t pageIndex, SSDataBlock **ppBlock);

#if defined(BUILD_TEST)
int32_t winInputPoolTestGetRegisteredPageCount(const SWindowInputPool *pPool);
int32_t winInputStoreTestGetPageId(const SWindowInputStore *pStore, int32_t pageIndex, int32_t *pPageId);
void    winInputStoreTestFailNextDirectoryCommit(SWindowInputStore *pStore);
int32_t winFuncTestSameOrderKeys(SWindowInputStore *pStore,
                                 const SNodeList *pOrderKeys, int32_t lhs,
                                 int32_t rhs, bool *pSame);
#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_WINDOWFUNC_H
