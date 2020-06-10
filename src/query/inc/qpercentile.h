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

#ifndef TDENGINE_QPERCENTILE_H
#define TDENGINE_QPERCENTILE_H

#include "qextbuffer.h"

typedef struct MinMaxEntry {
  union {
    double  dMinVal;
    int32_t iMinVal;
    int64_t i64MinVal;
  };
  union {
    double  dMaxVal;
    int32_t iMaxVal;
    int64_t i64MaxVal;
  };
} MinMaxEntry;

typedef struct tMemBucketSegment {
  int32_t         numOfSlots;
  MinMaxEntry *   pBoundingEntries;
  tExtMemBuffer **pBuffer;
} tMemBucketSegment;

typedef struct tMemBucket {
  int16_t numOfSegs;
  int16_t nTotalSlots;
  int16_t nSlotsOfSeg;
  int16_t dataType;
  
  int16_t nElemSize;
  int32_t numOfElems;
  
  int32_t nTotalBufferSize;
  int32_t maxElemsCapacity;
  
  int32_t pageSize;
  int16_t numOfTotalPages;
  int16_t numOfAvailPages; /* remain available buffer pages */
  
  tMemBucketSegment *pSegs;
  tOrderDescriptor * pOrderDesc;
  
  MinMaxEntry nRange;
  
  void (*HashFunc)(struct tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);
} tMemBucket;

tMemBucket *tMemBucketCreate(int32_t totalSlots, int32_t nBufferSize, int16_t nElemSize, int16_t dataType,
                             tOrderDescriptor *pDesc);

void tMemBucketDestroy(tMemBucket *pBucket);

void tMemBucketPut(tMemBucket *pBucket, void *data, int32_t numOfRows);

double getPercentile(tMemBucket *pMemBucket, double percent);

void tBucketIntHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);

void tBucketDoubleHash(tMemBucket *pBucket, void *value, int16_t *segIdx, int16_t *slotIdx);

#endif  // TDENGINE_QPERCENTILE_H
