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

#ifdef __cplusplus
extern "C" {
#endif

#define USE_ARRAYLIST

#define MAX_HISTOGRAM_BIN 500

typedef struct SHistBin {
  double  val;
  int64_t num;

#if !defined(USE_ARRAYLIST)
  double  delta;
  int32_t index;  // index in min-heap list
#endif
} SHistBin;

typedef struct SHeapEntry {
  void*  pData;
  double val;
} SHeapEntry;

typedef struct SHistogramInfo {
  int32_t numOfElems;
  int32_t numOfEntries;
  int32_t maxEntries;
  double min;
  double max;

#if defined(USE_ARRAYLIST)
  SHistBin* elems;
#else
  tSkipList*      pList;
  SLoserTreeInfo* pLoserTree;
  int32_t         maxIndex;
  bool            ordered;
#endif
} SHistogramInfo;

SHistogramInfo* tHistogramCreate(int32_t numOfBins);
SHistogramInfo* tHistogramCreateFrom(void* pBuf, int32_t numOfBins);

int32_t tHistogramAdd(SHistogramInfo** pHisto, double val);
int64_t tHistogramSum(SHistogramInfo* pHisto, double v);

double* tHistogramUniform(SHistogramInfo* pHisto, double* ratio, int32_t num);
SHistogramInfo* tHistogramMerge(SHistogramInfo* pHisto1, SHistogramInfo* pHisto2, int32_t numOfEntries);
void tHistogramDestroy(SHistogramInfo** pHisto);

void tHistogramPrint(SHistogramInfo* pHisto);

int32_t vnodeHistobinarySearch(SHistBin* pEntry, int32_t len, double val);

SHeapEntry* tHeapCreate(int32_t numOfEntries);
void tHeapSort(SHeapEntry* pEntry, int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HISTOGRAM_H
