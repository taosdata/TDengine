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

#ifndef TDENGINE_FUNCTIONRESINFOINT_H
#define TDENGINE_FUNCTIONRESINFOINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "thistogram.h"
#include "tdigest.h"
#include "functionResInfo.h"
#include "tpercentile.h"

#define USE_ARRAYLIST

#define HLL_BUCKET_BITS 14  // The bits of the bucket
#define HLL_DATA_BITS   (64 - HLL_BUCKET_BITS)
#define HLL_BUCKETS     (1 << HLL_BUCKET_BITS)
#define HLL_BUCKET_MASK (HLL_BUCKETS - 1)
#define HLL_ALPHA_INF   0.721347520444481703680  // constant for 0.5/ln(2)

typedef struct SSumRes {
  union {
    int64_t  isum;
    uint64_t usum;
    double   dsum;
  };
  int16_t type;
  int64_t prevTs;
  bool    isPrevTsSet;
  bool    overflow;  // if overflow is true, dsum to be used for any type;
} SSumRes;

typedef struct SMinmaxResInfo {
  bool      assign;  // assign the first value or not
  int64_t   v;
  char      *str;
  STuplePos tuplePos;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;
  int16_t   type;
} SMinmaxResInfo;

typedef struct SStdRes {
  double  result;
  int64_t count;
  union {
    double   quadraticDSum;
    int64_t  quadraticISum;
    uint64_t quadraticUSum;
  };
  union {
    double   dsum;
    int64_t  isum;
    uint64_t usum;
  };
  int16_t type;
} SStdRes;

typedef struct SHistBin {
  double  val;
  int64_t num;

#if !defined(USE_ARRAYLIST)
  double  delta;
  int32_t index;  // index in min-heap list
#endif
} SHistBin;

typedef struct SHistogramInfo {
  int64_t numOfElems;
  int32_t numOfEntries;
  int32_t maxEntries;
  double  min;
  double  max;
#if defined(USE_ARRAYLIST)
  SHistBin* elems;
#else
  tSkipList*              pList;
  SMultiwayMergeTreeInfo* pLoserTree;
  int32_t                 maxIndex;
  bool                    ordered;
#endif
} SHistogramInfo;

typedef struct SAPercentileInfo {
  double          result;
  double          percent;
  int8_t          algo;
  SHistogramInfo* pHisto;
  TDigest*        pTDigest;
} SAPercentileInfo;

typedef struct SSpreadInfo {
  double result;
  bool   hasResult;
  double min;
  double max;
} SSpreadInfo;

typedef struct SHLLFuncInfo {
  uint64_t result;
  uint64_t totalCount;
  uint8_t  buckets[HLL_BUCKETS];
} SHLLInfo;

typedef struct SGroupKeyInfo {
  bool hasResult;
  bool isNull;
  char data[];
} SGroupKeyInfo;

typedef struct SAvgRes {
  double  result;
  SSumRes sum;
  int64_t count;
  int16_t type;  // store the original input type, used in merge function
} SAvgRes;


// structs above are used in stream

#define HISTOGRAM_MAX_BINS_NUM 1000
#define MAVG_MAX_POINTS_NUM    1000
#define TAIL_MAX_POINTS_NUM    100
#define TAIL_MAX_OFFSET        100

typedef struct STopBotResItem {
  SVariant  v;
  uint64_t  uid;  // it is a table uid, used to extract tag data during building of the final result for the tag data
  STuplePos tuplePos;  // tuple data of this chosen row
} STopBotResItem;

typedef struct STopBotRes {
  int32_t maxSize;
  int16_t type;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;

  STopBotResItem* pItems;
} STopBotRes;

typedef struct SLeastSQRInfo {
  double  matrix[2][3];
  double  startVal;
  double  stepVal;
  int64_t num;
} SLeastSQRInfo;

typedef struct MinMaxEntry {
  union {
    double dMinVal;
    // double   i64MinVal;
    uint64_t u64MinVal;
  };
  union {
    double dMaxVal;
    // double  i64MaxVal;
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
typedef int32_t (*__perc_hash_func_t)(struct tMemBucket *pBucket, const void *value, int32_t *index);

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

typedef struct SPercentileInfo {
  double      result;
  tMemBucket* pMemBucket;
  int32_t     stage;
  double      minval;
  double      maxval;
  int64_t     numOfElems;
} SPercentileInfo;

typedef struct SDiffInfo {
  bool   hasPrev;
  bool   isFirstRow;
  int8_t ignoreOption;  // replace the ignore with case when
  union {
    int64_t i64;
    double  d64;
  } prev;

  int64_t prevTs;
} SDiffInfo;

typedef struct SElapsedInfo {
  double  result;
  TSKEY   min;
  TSKEY   max;
  int64_t timeUnit;
} SElapsedInfo;

typedef struct STwaInfo {
  double      dOutput;
  int64_t     numOfElems;
  SPoint1     p;
  STimeWindow win;
} STwaInfo;

typedef struct SHistoFuncBin {
  double  lower;
  double  upper;
  int64_t count;
  double  percentage;
} SHistoFuncBin;

typedef struct SHistoFuncInfo {
  int32_t       numOfBins;
  int32_t       totalCount;
  bool          normalized;
  SHistoFuncBin bins[];
} SHistoFuncInfo;

typedef struct SStateInfo {
  union {
    int64_t count;
    int64_t durationStart;
  };
  int64_t prevTs;
  bool    isPrevTsSet;
} SStateInfo;

typedef struct SMavgInfo {
  int32_t pos;
  double  sum;
  int64_t prevTs;
  bool    isPrevTsSet;
  int32_t numOfPoints;
  bool    pointsMeet;
  double  points[];
} SMavgInfo;

typedef struct SSampleInfo {
  int32_t  samples;
  int32_t  totalPoints;
  int32_t  numSampled;
  uint8_t  colType;
  uint16_t colBytes;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;

  char*      data;
  STuplePos* tuplePos;
} SSampleInfo;

typedef struct STailItem {
  int64_t timestamp;
  bool    isNull;
  char    data[];
} STailItem;

typedef struct STailInfo {
  int32_t     numOfPoints;
  int32_t     numAdded;
  int32_t     offset;
  uint8_t     colType;
  uint16_t    colBytes;
  STailItem** pItems;
} STailInfo;

typedef struct SUniqueItem {
  int64_t timestamp;
  bool    isNull;
  char    data[];
} SUniqueItem;

typedef struct SUniqueInfo {
  int32_t   numOfPoints;
  uint8_t   colType;
  uint16_t  colBytes;
  bool      hasNull;  // null is not hashable, handle separately
  SHashObj* pHash;
  char      pItems[];
} SUniqueInfo;

typedef struct SModeItem {
  int64_t   count;
  STuplePos dataPos;
  STuplePos tuplePos;
} SModeItem;

typedef struct SModeInfo {
  uint8_t   colType;
  uint16_t  colBytes;
  SHashObj* pHash;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;

  char* buf;  // serialize data buffer
} SModeInfo;

typedef struct SDerivInfo {
  double  prevValue;       // previous value
  TSKEY   prevTs;          // previous timestamp
  bool    ignoreNegative;  // ignore the negative value
  int64_t tsWindow;        // time window for derivative
  bool    valueSet;        // the value has been set already
} SDerivInfo;

typedef struct SRateInfo {
  double firstValue;
  TSKEY  firstKey;
  double lastValue;
  TSKEY  lastKey;
  int8_t hasResult;  // flag to denote has value

  char* firstPk;
  char* lastPk;
  int8_t pkType;
  int32_t pkBytes;
  char pkData[];
} SRateInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTIONRESINFOINT_H
