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

#ifndef TDENGINE_HISTOGRAM_H
#define TDENGINE_HISTOGRAM_H

#include "functionResInfoInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define USE_ARRAYLIST

#define MAX_HISTOGRAM_BIN 500

typedef struct SHeapEntry {
  void*  pData;
  double val;
} SHeapEntry;

struct SHistogramInfo;
struct SHistBin;

int32_t tHistogramCreate(int32_t numOfEntries, struct SHistogramInfo** pHisto);
struct SHistogramInfo* tHistogramCreateFrom(void* pBuf, int32_t numOfBins);

int32_t tHistogramAdd(struct SHistogramInfo** pHisto, double val);
int32_t tHistogramSum(struct SHistogramInfo* pHisto, double v, int64_t *res);

int32_t         tHistogramUniform(struct SHistogramInfo* pHisto, double* ratio, int32_t num, double** pVal);
int32_t         tHistogramMerge(struct SHistogramInfo* pHisto1, struct SHistogramInfo* pHisto2, int32_t numOfEntries,
                                struct SHistogramInfo** pResHistogram);
void            tHistogramDestroy(struct SHistogramInfo** pHisto);

void tHistogramPrint(struct SHistogramInfo* pHisto);

int32_t histoBinarySearch(struct SHistBin* pEntry, int32_t len, double val);

SHeapEntry* tHeapCreate(int32_t numOfEntries);
void        tHeapSort(SHeapEntry* pEntry, int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HISTOGRAM_H
