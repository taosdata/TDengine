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
#include "decimal.h"

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

typedef struct SDecimalSumRes {
  Decimal128 sum;
  int16_t    type;
  int64_t    prevTs;
  bool       isPrevTsSet;
  bool       overflow;
  uint32_t   flag; // currently not used
} SDecimalSumRes;

#define SUM_RES_GET_RES(pSumRes) ((SSumRes*)pSumRes)
#define SUM_RES_GET_DECIMAL_RES(pSumRes) ((SDecimalSumRes*)pSumRes)

#define SUM_RES_GET_SIZE(type) IS_DECIMAL_TYPE(type) ? sizeof(SDecimalSumRes) : sizeof(SSumRes)

#define SUM_RES_SET_TYPE(pSumRes, inputType, _type)   \
  do {                                                \
    if (IS_DECIMAL_TYPE(inputType))                   \
      SUM_RES_GET_DECIMAL_RES(pSumRes)->type = _type; \
    else                                              \
      SUM_RES_GET_RES(pSumRes)->type = _type;         \
  } while (0)

#define SUM_RES_GET_TYPE(pSumRes, inputType) \
  (IS_DECIMAL_TYPE(inputType) ? SUM_RES_GET_DECIMAL_RES(pSumRes)->type : SUM_RES_GET_RES(pSumRes)->type)
#define SUM_RES_GET_PREV_TS(pSumRes, inputType) \
  (IS_DECIMAL_TYPE(inputType) ? SUM_RES_GET_DECIMAL_RES(pSumRes)->prevTs : SUM_RES_GET_RES(pSumRes)->prevTs)
#define SUM_RES_GET_OVERFLOW(pSumRes, checkInputType, inputType)                             \
  (checkInputType && IS_DECIMAL_TYPE(inputType) ? SUM_RES_GET_DECIMAL_RES(pSumRes)->overflow \
                                                : SUM_RES_GET_RES(pSumRes)->overflow)

#define SUM_RES_GET_ISUM(pSumRes) (((SSumRes*)(pSumRes))->isum)
#define SUM_RES_GET_USUM(pSumRes) (((SSumRes*)(pSumRes))->usum)
#define SUM_RES_GET_DSUM(pSumRes) (((SSumRes*)(pSumRes))->dsum)
#define SUM_RES_INC_ISUM(pSumRes, val) ((SSumRes*)(pSumRes))->isum += val
#define SUM_RES_INC_USUM(pSumRes, val) ((SSumRes*)(pSumRes))->usum += val
#define SUM_RES_INC_DSUM(pSumRes, val) ((SSumRes*)(pSumRes))->dsum += val

#define SUM_RES_GET_DECIMAL_SUM(pSumRes) ((SDecimalSumRes*)(pSumRes))->sum
#define SUM_RES_INC_DECIMAL_SUM(pSumRes, pVal, type)                                           \
  do {                                                                                         \
    const SDecimalOps* pOps = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);                           \
    int32_t            wordNum = 0;                                                            \
    if (type == TSDB_DATA_TYPE_DECIMAL64) {                                                    \
      wordNum = DECIMAL_WORD_NUM(Decimal64);                                                           \
      overflow = decimal128AddCheckOverflow(&SUM_RES_GET_DECIMAL_SUM(pSumRes), pVal, wordNum); \
    } else {                                                                                   \
      wordNum = DECIMAL_WORD_NUM(Decimal);                                                             \
      overflow = decimal128AddCheckOverflow(&SUM_RES_GET_DECIMAL_SUM(pSumRes), pVal, wordNum); \
    }                                                                                          \
    pOps->add(&SUM_RES_GET_DECIMAL_SUM(pSumRes), pVal, wordNum);                               \
    if (overflow) break;                                                                       \
  } while (0)

typedef struct SMinmaxResInfo {
  bool      assign;  // assign the first value or not
  union {
    struct {
      int64_t v;
      char*   str;
    };
    int64_t dec[2]; // for decimal types
  };
  STuplePos tuplePos;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;
  int16_t   type;
} SMinmaxResInfo;

typedef struct SOldMinMaxResInfo {
  bool      assign;  // assign the first value or not
  int64_t   v;
  char*     str;
  STuplePos tuplePos;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;
  int16_t   type;
} SOldMinMaxResInfo;

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

typedef struct SDecimalAvgRes {
  Decimal128     avg;
  SDecimalSumRes sum;
  int64_t        count;
  int16_t        type; // store the original input type and scale, used in merge function
  uint8_t        scale;
} SDecimalAvgRes;

#define AVG_RES_GET_RES(pAvgRes) ((SAvgRes*)pAvgRes)
#define AVG_RES_GET_DECIMAL_RES(pAvgRes) ((SDecimalAvgRes*)pAvgRes)
#define AVG_RES_SET_TYPE(pAvgRes, inputType, _type)   \
  do {                                                \
    if (IS_DECIMAL_TYPE(inputType))                   \
      AVG_RES_GET_DECIMAL_RES(pAvgRes)->type = _type; \
    else                                              \
      AVG_RES_GET_RES(pAvgRes)->type = _type;          \
  } while (0)

#define AVG_RES_SET_INPUT_SCALE(pAvgRes, _scale)      \
  do {                                                \
    AVG_RES_GET_DECIMAL_RES(pAvgRes)->scale = _scale; \
  } while (0)

#define AVG_RES_GET_INPUT_SCALE(pAvgRes) (AVG_RES_GET_DECIMAL_RES(pAvgRes)->scale)

#define AVG_RES_GET_TYPE(pAvgRes, inputType) \
  (IS_DECIMAL_TYPE(inputType) ? AVG_RES_GET_DECIMAL_RES(pAvgRes)->type : AVG_RES_GET_RES(pAvgRes)->type)

#define AVG_RES_GET_SIZE(inputType) (IS_DECIMAL_TYPE(inputType) ? sizeof(SDecimalAvgRes) : sizeof(SAvgRes))
#define AVG_RES_GET_AVG(pAvgRes)    (AVG_RES_GET_RES(pAvgRes)->result)
#define AVG_RES_GET_SUM(pAvgRes) (AVG_RES_GET_RES(pAvgRes)->sum)
#define AVG_RES_GET_COUNT(pAvgRes, checkInputType, inputType)                              \
  (checkInputType && IS_DECIMAL_TYPE(inputType) ? AVG_RES_GET_DECIMAL_RES(pAvgRes)->count \
                                                : AVG_RES_GET_RES(pAvgRes)->count)
#define AVG_RES_INC_COUNT(pAvgRes, inputType, val)    \
  do {                                                \
    if (IS_DECIMAL_TYPE(inputType))                   \
      AVG_RES_GET_DECIMAL_RES(pAvgRes)->count += val; \
    else                                              \
      AVG_RES_GET_RES(pAvgRes)->count += val;         \
  } while (0)

#define AVG_RES_GET_DECIMAL_AVG(pAvgRes) (((SDecimalAvgRes*)(pAvgRes))->avg)
#define AVG_RES_GET_DECIMAL_SUM(pAvgRes) (((SDecimalAvgRes*)(pAvgRes))->sum)

#define AVG_RES_GET_SUM_OVERFLOW(pAvgRes, checkInputType, inputType)             \
  checkInputType&& IS_DECIMAL_TYPE(inputType)                                    \
      ? SUM_RES_GET_OVERFLOW(&AVG_RES_GET_DECIMAL_SUM(pAvgRes), true, inputType) \
      : SUM_RES_GET_OVERFLOW(&AVG_RES_GET_SUM(pAvgRes), false, inputType)

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
  STypeMod           typeMod;
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
  double      dTwaRes;
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

typedef struct SValueChangeInfo {
  int64_t total;
  bool   preIsNull;
  bool   isFirstRow;
  int8_t ignoreOption;  // replace the ignore with case when

  union {
    int64_t i64; // 其他类型转为hash值
    double  d64;
  } prev;

  int64_t prevTs;
} SValueChangeInfo;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTIONRESINFOINT_H
