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

#ifndef TDENGINE_TPERCENTILE_H
#define TDENGINE_TPERCENTILE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tpagedbuf.h"
#include "ttszip.h"

typedef struct MinMaxEntry {
  union {
    double   dMinVal;
    //double   i64MinVal;
    uint64_t u64MinVal;
  };
  union {
    double  dMaxVal;
    //double  i64MaxVal;
    int64_t u64MaxVal;
  };
} MinMaxEntry;

typedef struct {
  int32_t    size;
  int32_t    pageId;
  SFilePage *data;
} SSlotInfo;

typedef struct tMemBucketSlot {
  SSlotInfo   info;
  MinMaxEntry range;
} tMemBucketSlot;

struct tMemBucket;
typedef int32_t (*__perc_hash_func_t)(struct tMemBucket *pBucket, const void *value);

typedef struct tMemBucket {
  int16_t            numOfSlots;
  int16_t            type;
  int32_t            bytes;
  int32_t            total;
  int32_t            elemPerPage;  // number of elements for each object
  int32_t            maxCapacity;  // maximum allowed number of elements that can be sort directly to get the result
  int32_t            bufPageSize;  // disk page size
  MinMaxEntry        range;        // value range
  int32_t            times;        // count that has been checked for deciding the correct data value buckets.
  __compar_fn_t      comparFn;
  tMemBucketSlot    *pSlots;
  SDiskbasedBuf     *pBuffer;
  __perc_hash_func_t hashFunc;
  SHashObj          *groupPagesMap;  // disk page map for different groups;
} tMemBucket;

tMemBucket *tMemBucketCreate(int32_t nElemSize, int16_t dataType, double minval, double maxval);

void tMemBucketDestroy(tMemBucket *pBucket);

int32_t tMemBucketPut(tMemBucket *pBucket, const void *data, size_t size);

int32_t getPercentile(tMemBucket *pMemBucket, double percent, double *result);

#endif  // TDENGINE_TPERCENTILE_H

#ifdef __cplusplus
}
#endif
