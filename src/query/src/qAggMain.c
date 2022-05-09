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

#include "os.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "texpr.h"
#include "tdigest.h"
#include "ttype.h"
#include "tsdb.h"

#include "qAggMain.h"
#include "qFill.h"
#include "qHistogram.h"
#include "qPercentile.h"
#include "qTsbuf.h"
#include "queryLog.h"
#include "qUdf.h"
#include "tcompare.h"
#include "hashfunc.h"

#define GET_INPUT_DATA_LIST(x) ((char *)((x)->pInput))
#define GET_INPUT_DATA(x, y) (GET_INPUT_DATA_LIST(x) + (y) * (x)->inputBytes)

#define GET_TS_LIST(x)    ((TSKEY*)((x)->ptsList))
#define GET_TS_DATA(x, y) (GET_TS_LIST(x)[(y)])

#define GET_TRUE_DATA_TYPE()                          \
  int32_t type = 0;                                   \
  if (pCtx->currentStage == MERGE_STAGE) {  \
    type = pCtx->outputType;                          \
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY); \
  } else {                                            \
    type = pCtx->inputType;                           \
  }

#define SET_VAL(ctx, numOfElem, res)     \
  do {                                   \
    if ((numOfElem) <= 0) {              \
      break;                             \
    }                                    \
    GET_RES_INFO(ctx)->numOfRes = (res); \
  } while (0)

#define INC_INIT_VAL(ctx, res) (GET_RES_INFO(ctx)->numOfRes += (res));

#define DO_UPDATE_TAG_COLUMNS(ctx, ts)                             \
  do {                                                             \
    for (int32_t _i = 0; _i < (ctx)->tagInfo.numOfTagCols; ++_i) { \
      SQLFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[_i];      \
      if (__ctx->functionId == TSDB_FUNC_TS_DUMMY) {               \
        __ctx->tag.i64 = (ts);                                     \
        __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;                  \
      }                                                            \
      aAggs[TSDB_FUNC_TAG].xFunction(__ctx);                       \
    }                                                              \
  } while (0)

#define DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx)                   \
  do {                                                          \
    for (int32_t _i = 0; _i < (ctx)->tagInfo.numOfTagCols; ++_i) { \
      SQLFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[_i];    \
      aAggs[TSDB_FUNC_TAG].xFunction(__ctx);                    \
    }                                                           \
  } while (0);

void noop1(SQLFunctionCtx *UNUSED_PARAM(pCtx)) {}

void doFinalizer(SQLFunctionCtx *pCtx) { RESET_RESULT_INFO(GET_RES_INFO(pCtx)); }

typedef struct tValuePair {
  tVariant v;
  int64_t  timestamp;
  char *   pTags;  // the corresponding tags of each record in the final result
} tValuePair;

typedef struct SSpreadInfo {
  double min;
  double max;
  int8_t hasResult;
} SSpreadInfo;

typedef struct SSumInfo {
  union {
    int64_t  isum;
    uint64_t usum;
    double   dsum;
  };
  int8_t hasResult;
} SSumInfo;

// the attribute of hasResult is not needed since the num attribute would server as this purpose
typedef struct SAvgInfo {
  double  sum;
  int64_t num;
} SAvgInfo;

typedef struct SStddevInfo {
  double  avg;
  int64_t num;
  double  res;
  int8_t  stage;
} SStddevInfo;

typedef struct SStddevdstInfo {
  int64_t num;
  double  res;
} SStddevdstInfo;

typedef struct SFirstLastInfo {
  int8_t hasResult;
  TSKEY  ts;
} SFirstLastInfo;

typedef struct SFirstLastInfo SLastrowInfo;
typedef struct SPercentileInfo {
  tMemBucket *pMemBucket;
  int32_t     stage;
  double      minval;
  double      maxval;
  int64_t     numOfElems;
} SPercentileInfo;

typedef struct STopBotInfo {
  int32_t      num;
  tValuePair **res;
} STopBotInfo;

// leastsquares do not apply to super table
typedef struct SLeastsquaresInfo {
  double  mat[2][3];
  double  startVal;
  int64_t num;
} SLeastsquaresInfo;

typedef struct SAPercentileInfo {
  SHistogramInfo *pHisto;
  TDigest* pTDigest;
} SAPercentileInfo;

typedef struct STSCompInfo {
  STSBuf *pTSBuf;
} STSCompInfo;

typedef struct SRateInfo {
  double  correctionValue;
  double  firstValue;
  TSKEY   firstKey;
  double  lastValue;
  TSKEY   lastKey;
  int8_t  hasResult;  // flag to denote has value
  bool    isIRate;    // true for IRate functions, false for Rate functions
} SRateInfo;

typedef struct SDerivInfo {
  double   prevValue;     // previous value
  TSKEY    prevTs;        // previous timestamp
  bool     ignoreNegative;// ignore the negative value
  int64_t  tsWindow;      // time window for derivative
  bool     valueSet;      // the value has been set already
} SDerivInfo;

typedef struct {
  union {
    double d64CumSum;
    int64_t i64CumSum;
    uint64_t u64CumSum;
  };
} SCumSumInfo;

typedef struct {
  int32_t pos;
  double sum;
  int32_t numPointsK;
  double* points;
  bool    kPointsMeet;
} SMovingAvgInfo;

typedef struct  {
  int32_t totalPoints;
  int32_t numSampled;
  int16_t colBytes;
  char *values;
  int64_t *timeStamps;
  char  *taglists;
} SSampleFuncInfo;

typedef struct SElapsedInfo {
  int8_t hasResult;
  TSKEY min;
  TSKEY max;
} SElapsedInfo;

typedef struct {
  bool valueAssigned;
  bool ignoreNegative;
  union {
    int64_t i64Prev;
    double d64Prev;
  };
} SDiffFuncInfo;


typedef struct {
  union {
    int64_t countPrev;
    int64_t durationStart;
  };
} SStateInfo;

typedef struct {
  double lower; // >lower
  double upper; // <=upper
  double count;
} SHistogramFuncBin;

typedef struct{
  int32_t numOfBins;
  int32_t normalized;
  SHistogramFuncBin* orderedBins;
} SHistogramFuncInfo;

typedef struct {
  int64_t  timestamp;
  char     data[];
} UniqueUnit;

typedef struct {
  int32_t      num;
  char         res[];
} SUniqueFuncInfo;

typedef struct {
  int64_t  count;
  char     data[];
} ModeUnit;

typedef struct {
  int32_t      num;
  char         res[];
} SModeFuncInfo;

typedef struct {
  int64_t  timestamp;
  char     data[];
} TailUnit;

typedef struct {
  int32_t      num;
  TailUnit     **res;
} STailInfo;

static void *getOutputInfo(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  // only the first_stage_merge is directly written data into final output buffer
  if (pCtx->stableQuery && pCtx->currentStage != MERGE_STAGE) {
    return pCtx->pOutput;
  } else { // during normal table query and super table at the secondary_stage, result is written to intermediate buffer
    return GET_ROWCELL_INTERBUF(pResInfo);
  }
}

/* hyperloglog start */
#define HLL_BUCKET_BITS 14 // The bits of the bucket
#define HLL_DATA_BITS (64-HLL_BUCKET_BITS)
#define HLL_BUCKETS (1<<HLL_BUCKET_BITS)
#define HLL_BUCKET_MASK (HLL_BUCKETS-1)
#define HLL_ALPHA_INF 0.721347520444481703680 // constant for 0.5/ln(2)

typedef struct {
  uint8_t buckets[HLL_BUCKETS]; // Data bytes.
} SHLLInfo;

static void hllBucketHisto(uint8_t *buckets, int32_t* bucketHisto) {
  uint64_t *word = (uint64_t*) buckets;
  uint8_t *bytes;

  for (int32_t j = 0; j < HLL_BUCKETS>>3; j++) {
    if (*word == 0) {
      bucketHisto[0] += 8;
    } else {
      bytes = (uint8_t*) word;
      bucketHisto[bytes[0]]++;
      bucketHisto[bytes[1]]++;
      bucketHisto[bytes[2]]++;
      bucketHisto[bytes[3]]++;
      bucketHisto[bytes[4]]++;
      bucketHisto[bytes[5]]++;
      bucketHisto[bytes[6]]++;
      bucketHisto[bytes[7]]++;
    }
    word++;
  }
}
static double hllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double zPrime;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    zPrime = z;
    y *= 0.5;
    z -= pow(1 - x, 2)*y;
  } while(zPrime != z);
  return z / 3;
}

static double hllSigma(double x) {
  if (x == 1.0) return INFINITY;
  double zPrime;
  double y = 1;
  double z = x;
  do {
    x *= x;
    zPrime = z;
    z += x * y;
    y += y;
  } while(zPrime != z);
  return z;
}

// estimate the cardinality, the algorithm refer this paper: "New cardinality estimation algorithms for HyperLogLog sketches"
static uint64_t hllCountCnt(uint8_t *buckets) {
  double m = HLL_BUCKETS;
  int32_t buckethisto[64] = {0};
  hllBucketHisto(buckets,buckethisto);

  double z = m * hllTau((m-buckethisto[HLL_DATA_BITS+1])/(double)m);
  for (int j = HLL_DATA_BITS; j >= 1; --j) {
    z += buckethisto[j];
    z *= 0.5;
  }
  z += m * hllSigma(buckethisto[0]/(double)m);
  double E = (double)llroundl(HLL_ALPHA_INF*m*m/z);

  return (uint64_t) E;
}

static uint8_t hllCountNum(void *ele, int32_t elesize, int32_t *buk) {
  uint64_t hash = MurmurHash3_64(ele,elesize);
  int32_t index = hash & HLL_BUCKET_MASK;
  hash >>= HLL_BUCKET_BITS;
  hash |= ((uint64_t)1<<HLL_DATA_BITS);
  uint64_t bit = 1;
  uint8_t count = 1;
  while((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *buk = index;
  return count;
}

static void hll_function(SQLFunctionCtx *pCtx) {
  SHLLInfo *pHLLInfo = getOutputInfo(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *val = GET_INPUT_DATA(pCtx, i);
    if (isNull(val, pCtx->inputType)) {
      continue;
    }
    int32_t elesize = pCtx->inputBytes;
    if(IS_VAR_DATA_TYPE(pCtx->inputType)) {
      elesize = varDataLen(val);
      val = varDataVal(val);
    }
    int32_t index = 0;
    uint8_t count = hllCountNum(val,elesize,&index);
    uint8_t oldcount = pHLLInfo->buckets[index];
    if (count > oldcount) {
      pHLLInfo->buckets[index] = count;
    }
  }
  GET_RES_INFO(pCtx)->numOfRes = 1;
}

static void hll_func_merge(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SHLLInfo *pHLLInfo = (SHLLInfo *)GET_ROWCELL_INTERBUF(pResInfo);

  SHLLInfo *pData = (SHLLInfo *)GET_INPUT_DATA_LIST(pCtx);
  for (int i = 0; i < HLL_BUCKETS; i++) {
    if (pData->buckets[i] > pHLLInfo->buckets[i]) {
      pHLLInfo->buckets[i] = pData->buckets[i];
    }
  }
}

static void hll_func_finalizer(SQLFunctionCtx *pCtx) {
  SHLLInfo *pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  GET_RES_INFO(pCtx)->numOfRes = 1;
  *(uint64_t *)(pCtx->pOutput) = hllCountCnt(pInfo->buckets);
  doFinalizer(pCtx);
}
/* hyperloglog end */

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type,
                          int32_t *bytes, int32_t *interBytes, int16_t extLength, bool isSuperTable, SUdfInfo* pUdfInfo) {
  if (!isValidDataType(dataType)) {
    qError("Illegal data type %d or data type length %d", dataType, dataBytes);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  assert(!TSDB_FUNC_IS_SCALAR(functionId));
  assert(functionId != TSDB_FUNC_SCALAR_EXPR);

  if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG_DUMMY ||
      functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TAGPRJ ||
      functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_INTERP)
  {
    *type = (int16_t)dataType;
    *bytes = dataBytes;

    if (functionId == TSDB_FUNC_INTERP) {
      *interBytes = sizeof(SInterpInfoDetail);
    } else if (functionId == TSDB_FUNC_DIFF) {
      *interBytes = sizeof(SDiffFuncInfo);
    } else {
      *interBytes = 0;
    }


    return TSDB_CODE_SUCCESS;
  }

  // (uid, tid) + VGID + TAGSIZE + VARSTR_HEADER_SIZE
  if (functionId == TSDB_FUNC_TID_TAG) { // todo use struct
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = (dataBytes + sizeof(int16_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t) + VARSTR_HEADER_SIZE);
    *interBytes = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_BLKINFO) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = 16384;
    *interBytes = 0;
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_COUNT) {
    *type = TSDB_DATA_TYPE_BIGINT;
    *bytes = sizeof(int64_t);
    *interBytes = 0;
    return TSDB_CODE_SUCCESS;
  }


  if (functionId == TSDB_FUNC_TS_COMP) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = 1;  // this results is compressed ts data, only one byte
    *interBytes = POINTER_BYTES;
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_DERIVATIVE) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);  // this results is compressed ts data, only one byte
    *interBytes = sizeof(SDerivInfo);
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_STATE_COUNT || functionId == TSDB_FUNC_STATE_DURATION) {
    *type = TSDB_DATA_TYPE_BIGINT;
    *bytes = sizeof(int64_t);
    *interBytes = sizeof(SStateInfo);
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_CSUM) {
    if (IS_SIGNED_NUMERIC_TYPE(dataType)) {
      *type = TSDB_DATA_TYPE_BIGINT;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(dataType)) {
      *type = TSDB_DATA_TYPE_UBIGINT;
    } else {
      *type = TSDB_DATA_TYPE_DOUBLE;
    }

    *bytes = sizeof(int64_t);
    *interBytes = sizeof(SCumSumInfo);
    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_MAVG) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SMovingAvgInfo) + sizeof(double) * param;
    return TSDB_CODE_SUCCESS;
  }

  if (isSuperTable) {
    if (functionId < 0) {
      if (pUdfInfo->bufSize > 0) {
        *type = TSDB_DATA_TYPE_BINARY;
        *bytes = pUdfInfo->bufSize;
        *interBytes = *bytes;
      } else {
        *type = pUdfInfo->resType;
        *bytes = pUdfInfo->resBytes;
        *interBytes = *bytes;
      }

      return TSDB_CODE_SUCCESS;
    }

    if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = (dataBytes + DATA_SET_FLAG_SIZE);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_SUM) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SSumInfo);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_AVG) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SAvgInfo);
      *interBytes = *bytes;
      return TSDB_CODE_SUCCESS;

    } else if (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_IRATE) {
      *type = TSDB_DATA_TYPE_DOUBLE;
      *bytes = sizeof(SRateInfo);
      *interBytes = sizeof(SRateInfo);
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = (sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_UNIQUE) {
      *type = TSDB_DATA_TYPE_BINARY;
      int64_t size = sizeof(UniqueUnit) + dataBytes + extLength;
      size *= param;
      size += sizeof(SUniqueFuncInfo);
      if (size > MAX_UNIQUE_RESULT_SIZE) {
        size = MAX_UNIQUE_RESULT_SIZE;
      }
      *bytes = (int32_t)size;
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_MODE) {
      *type = TSDB_DATA_TYPE_BINARY;
      int64_t size = sizeof(ModeUnit) + dataBytes;
      size *= MAX_MODE_INNER_RESULT_ROWS;
      size += sizeof(SModeFuncInfo);
      if (size > MAX_MODE_INNER_RESULT_SIZE){
        size = MAX_MODE_INNER_RESULT_SIZE;
      }
      *bytes = (int32_t)size;
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_TAIL) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = (sizeof(STailInfo) + (sizeof(TailUnit) + dataBytes + POINTER_BYTES + extLength) * param);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_HYPERLOGLOG) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SHLLInfo);
      *interBytes = sizeof(SHLLInfo);
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_SAMPLE) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = (sizeof(SSampleFuncInfo) + dataBytes*param + sizeof(int64_t)*param + extLength*param);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_SPREAD) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SSpreadInfo);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_APERCT) {
      *type = TSDB_DATA_TYPE_BINARY;
      int16_t bytesHist = sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1) + sizeof(SHistogramInfo) + sizeof(SAPercentileInfo);
      int32_t bytesDigest = (int32_t) (sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
      *bytes = MAX(bytesHist, bytesDigest);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_LAST_ROW) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = (sizeof(SLastrowInfo) + dataBytes);
      *interBytes = *bytes;

      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_TWA) {
      *type = TSDB_DATA_TYPE_DOUBLE;
      *bytes = sizeof(STwaInfo);
      *interBytes = *bytes;
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_ELAPSED) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SElapsedInfo);
      *interBytes = *bytes;
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_HISTOGRAM) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = 512;
      *interBytes = (sizeof(SHistogramFuncInfo) + param * sizeof(SHistogramFuncBin));
      return TSDB_CODE_SUCCESS;
    }
  }

  if (functionId == TSDB_FUNC_SUM) {
    if (IS_SIGNED_NUMERIC_TYPE(dataType)) {
      *type = TSDB_DATA_TYPE_BIGINT;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(dataType)) {
      *type = TSDB_DATA_TYPE_UBIGINT;
    } else {
      *type = TSDB_DATA_TYPE_DOUBLE;
    }

    *bytes = sizeof(int64_t);
    *interBytes = sizeof(SSumInfo);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == TSDB_FUNC_APERCT) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    int16_t bytesHist = sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1);
    int32_t bytesDigest = (int32_t) (sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
    *interBytes = MAX(bytesHist, bytesDigest);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == TSDB_FUNC_TWA) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(STwaInfo);
    return TSDB_CODE_SUCCESS;
  }

  if (functionId < 0) {
    *type = pUdfInfo->resType;
    *bytes = pUdfInfo->resBytes;

    if (pUdfInfo->bufSize > 0) {
      *interBytes = pUdfInfo->bufSize;
    } else {
      *interBytes = *bytes;
    }

    return TSDB_CODE_SUCCESS;
  }

  if (functionId == TSDB_FUNC_AVG) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SAvgInfo);
  } else if (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_IRATE) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SRateInfo);
  } else if (functionId == TSDB_FUNC_STDDEV) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SStddevInfo);
  } else if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    *interBytes = dataBytes + DATA_SET_FLAG_SIZE;
  } else if (functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_LAST) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    *interBytes = (dataBytes + sizeof(SFirstLastInfo));
  } else if (functionId == TSDB_FUNC_SPREAD) {
    *type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SSpreadInfo);
  } else if (functionId == TSDB_FUNC_PERCT) {
    *type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SPercentileInfo);
  } else if (functionId == TSDB_FUNC_LEASTSQR) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = MAX(TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE, sizeof(SLeastsquaresInfo));  // string
    *interBytes = *bytes;
  } else if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = (dataBytes + sizeof(SFirstLastInfo));
    *interBytes = *bytes;
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;

    size_t size = sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param;

    // the output column may be larger than sizeof(STopBotInfo)
    *interBytes = (int32_t)size;
  } else if (functionId == TSDB_FUNC_UNIQUE) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    int64_t size = sizeof(UniqueUnit) + dataBytes + extLength;
    size *= param;
    size += sizeof(SUniqueFuncInfo);
    if (size > MAX_UNIQUE_RESULT_SIZE){
      size = MAX_UNIQUE_RESULT_SIZE;
    }
    *interBytes = (int32_t)size;
  } else if(functionId == TSDB_FUNC_MODE) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    int64_t size = sizeof(ModeUnit) + dataBytes;
    size *= MAX_MODE_INNER_RESULT_ROWS;
    size += sizeof(SModeFuncInfo);
    if (size > MAX_MODE_INNER_RESULT_SIZE){
      size = MAX_MODE_INNER_RESULT_SIZE;
    }
    *interBytes = (int32_t)size;
  } else if (functionId == TSDB_FUNC_TAIL) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;

    size_t size = (sizeof(STailInfo) + (sizeof(TailUnit) + dataBytes + POINTER_BYTES + extLength) * param);

    // the output column may be larger than sizeof(STopBotInfo)
    *interBytes = (int32_t)size;
  } else if (functionId == TSDB_FUNC_HYPERLOGLOG) {
    *type = TSDB_DATA_TYPE_UBIGINT;
    *bytes = sizeof(uint64_t);
    *interBytes = sizeof(SHLLInfo);
  } else if (functionId == TSDB_FUNC_SAMPLE) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    size_t size = sizeof(SSampleFuncInfo) + dataBytes*param + sizeof(int64_t)*param + extLength*param;
    *interBytes = (int32_t)size;
  } else if (functionId == TSDB_FUNC_LAST_ROW) {
    *type = (int16_t)dataType;
    *bytes = dataBytes;
    *interBytes = dataBytes;
  } else if (functionId == TSDB_FUNC_STDDEV_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SStddevdstInfo);
    *interBytes = (*bytes);

  } else if (functionId == TSDB_FUNC_ELAPSED) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = tDataTypes[*type].bytes;
    *interBytes = sizeof(SElapsedInfo);
  } else if (functionId == TSDB_FUNC_HISTOGRAM) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = 512;
    *interBytes = (sizeof(SHistogramFuncInfo) + param * sizeof(SHistogramFuncBin));
  } else {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  return TSDB_CODE_SUCCESS;
}

bool isTimeWindowFunction(int32_t functionId) {
  return ((functionId >= TSDB_FUNC_WSTART) && (functionId <= TSDB_FUNC_QDURATION));
}

// TODO use hash table
int32_t isValidFunction(const char* name, int32_t len) {

  for (int32_t i = 0; i < TSDB_FUNC_SCALAR_NUM_FUNCTIONS; ++i) {
    int32_t nameLen = (int32_t) strlen(aScalarFunctions[i].name);
    if (len != nameLen) {
      continue;
    }

    if (strncasecmp(aScalarFunctions[i].name, name, len) == 0) {
      return aScalarFunctions[i].functionId;
    }
  }

  for(int32_t i = 0; i < TSDB_FUNC_MAX_NUM; ++i) {
    int32_t nameLen = (int32_t) strlen(aAggs[i].name);
    if (len != nameLen) {
      continue;
    }

    if (strncasecmp(aAggs[i].name, name, len) == 0) {
      return i;
    }
  }
  return -1;
}

bool isValidStateOper(char *oper, int32_t len){
  return strncmp(oper, "lt", len) == 0 || strncmp(oper, "gt", len) == 0 || strncmp(oper, "le", len) == 0 ||
         strncmp(oper, "ge", len) == 0 || strncmp(oper, "ne", len) == 0 || strncmp(oper, "eq", len) == 0;
}

#define STATEOPER(OPER, COMP, TYPE) if (strncmp(oper->pz, OPER, oper->nLen) == 0) {\
if (pVar->nType == TSDB_DATA_TYPE_BIGINT && *(TYPE)data COMP pVar->i64) return true;\
else if(pVar->nType == TSDB_DATA_TYPE_DOUBLE && *(TYPE)data COMP pVar->dKey) return true;\
else return false;}

#define STATEJUDGE(TYPE) STATEOPER("lt", <, TYPE)\
STATEOPER("gt", >, TYPE)\
STATEOPER("le", <=, TYPE)\
STATEOPER("ge", >=, TYPE)\
STATEOPER("ne", !=, TYPE)\
STATEOPER("eq", ==, TYPE)

static bool isStateOperTrue(void *data, int16_t type, tVariant *oper, tVariant *pVar){
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      STATEJUDGE(int32_t *)
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      STATEJUDGE(uint32_t *)
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      STATEJUDGE(int64_t *)
      break;
    }case TSDB_DATA_TYPE_UBIGINT: {
      STATEJUDGE(uint64_t *)
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      STATEJUDGE(double *)
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      STATEJUDGE(float *)
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      STATEJUDGE(int16_t *)
      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      STATEJUDGE(uint16_t *)
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      STATEJUDGE(int8_t *)
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      STATEJUDGE(uint8_t *)
      break;
    }
    default:
      qError("error input type");
  }
  return false;
}

static bool function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return false;
  }

  memset(pCtx->pOutput, 0, (size_t)pCtx->outputBytes);
  initResultInfo(pResultInfo, pCtx->interBufBytes);
  return true;
}

/**
 * in handling the stable query, function_finalizer is called after the secondary
 * merge being completed, during the first merge procedure, which is executed at the
 * vnode side, the finalize will never be called.
 *
 * @param pCtx
 */
static void function_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
  }

  doFinalizer(pCtx);
}

/*
 * count function does need the finalize, if data is missing, the default value, which is 0, is used
 * count function does not use the pCtx->interResBuf to keep the intermediate buffer
 */
static void count_function(SQLFunctionCtx *pCtx) {
  int32_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pCtx->hasNull == true. pCtx->preAggVals.isSet == true;
   * 2. for general non-primary key columns, pCtx->hasNull may be true or false, pCtx->preAggVals.isSet == true;
   * 3. for primary key column, pCtx->hasNull always be false, pCtx->preAggVals.isSet == false;
   */
  if (pCtx->preAggVals.isSet) {
    numOfElem = pCtx->size - pCtx->preAggVals.statis.numOfNull;
  } else {
    if (pCtx->hasNull) {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        char *val = GET_INPUT_DATA(pCtx, i);
        if (isNull(val, pCtx->inputType)) {
          continue;
        }

        numOfElem += 1;
      }
    } else {
      //when counting on the primary time stamp column and no statistics data is presented, use the size value directly.
      numOfElem = pCtx->size;
    }
  }

  if (numOfElem > 0) {
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }

  *((int64_t *)pCtx->pOutput) += numOfElem;
  SET_VAL(pCtx, numOfElem, 1);
}

static void count_func_merge(SQLFunctionCtx *pCtx) {
  int64_t *pData = (int64_t *)GET_INPUT_DATA_LIST(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i) {
    *((int64_t *)pCtx->pOutput) += pData[i];
  }

  SET_VAL(pCtx, pCtx->size, 1);
}

/**
 * 1. If the column value for filter exists, we need to load the SFields, which serves
 *    as the pre-filter to decide if the actual data block is required or not.
 * 2. If it queries on the non-primary timestamp column, SFields is also required to get the not-null value.
 *
 * @param colId
 * @param filterCols
 * @return
 */
int32_t countRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return BLK_DATA_NO_NEEDED;
  } else {
    return BLK_DATA_STATIS_NEEDED;
  }
}

int32_t noDataRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_NO_NEEDED;
}
#define LIST_ADD_N_DOUBLE_FLOAT(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      SET_DOUBLE_VAL(&(x) , GET_DOUBLE_VAL(&(x)) + GET_FLOAT_VAL(&(d)[i]));                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)
#define LIST_ADD_N_DOUBLE(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      SET_DOUBLE_VAL(&(x) , (x) + (d)[i]);                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)

#define LIST_ADD_N(x, ctx, p, t, numOfElem, tsdbType)              \
  do {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      (x) += (d)[i];                                               \
      (numOfElem)++;                                               \
    }                                                              \
  } while(0)

#define UPDATE_DATA(ctx, left, right, num, sign, k) \
  do {                                              \
    if (((left) < (right)) ^ (sign)) {              \
      (left) = (right);                             \
      DO_UPDATE_TAG_COLUMNS(ctx, k);                \
      (num) += 1;                                   \
    }                                               \
  } while (0)

#define DUPATE_DATA_WITHOUT_TS(ctx, left, right, num, sign) \
  do {                                                      \
    if (((left) < (right)) ^ (sign)) {                      \
      (left) = (right);                                     \
      DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx);                \
      (num) += 1;                                           \
    }                                                       \
  } while (0)

#define LOOPCHECK_N(val, list, ctx, tsdbType, sign, num)          \
  for (int32_t i = 0; i < ((ctx)->size); ++i) {                   \
    if ((ctx)->hasNull && isNull((char *)&(list)[i], tsdbType)) { \
      continue;                                                   \
    }                                                             \
    TSKEY key = (ctx)->ptsList != NULL? GET_TS_DATA(ctx, i):0;    \
    UPDATE_DATA(ctx, val, (list)[i], num, sign, key);             \
  }

#define TYPED_LOOPCHECK_N(type, data, list, ctx, tsdbType, sign, notNullElems) \
  do {                                                                         \
    type *_data = (type *)data;                                                \
    type *_list = (type *)list;                                                \
    LOOPCHECK_N(*_data, _list, ctx, tsdbType, sign, notNullElems);             \
  } while (0)

static void do_sum(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  if (pCtx->preAggVals.isSet) {
    notNullElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    assert(pCtx->size >= pCtx->preAggVals.statis.numOfNull);

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t *retVal = (int64_t *)pCtx->pOutput;
      *retVal += pCtx->preAggVals.statis.sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t *retVal = (uint64_t *)pCtx->pOutput;
      *retVal += (uint64_t)pCtx->preAggVals.statis.sum;
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      double *retVal = (double*) pCtx->pOutput;
      SET_DOUBLE_VAL(retVal, *retVal + GET_DOUBLE_VAL((const char*)&(pCtx->preAggVals.statis.sum)));
    }
  } else {  // computing based on the true data block
    void *pData = GET_INPUT_DATA_LIST(pCtx);
    notNullElems = 0;

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t *retVal = (int64_t *)pCtx->pOutput;

      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(*retVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t *retVal = (uint64_t *)pCtx->pOutput;

      if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
        LIST_ADD_N(*retVal, pCtx, pData, uint64_t, notNullElems, pCtx->inputType);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *retVal = (double *)pCtx->pOutput;
      LIST_ADD_N_DOUBLE(*retVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = (double *)pCtx->pOutput;
      LIST_ADD_N_DOUBLE_FLOAT(*retVal, pCtx, pData, float, notNullElems, pCtx->inputType);
    }
  }

  // data in the check operation are all null, not output
  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
}

static void sum_function(SQLFunctionCtx *pCtx) {
  do_sum(pCtx);

  // keep the result data in output buffer, not in the intermediate buffer
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult == DATA_SET_FLAG && pCtx->stableQuery) {
    // set the flag for super table query
    SSumInfo *pSum = (SSumInfo *)pCtx->pOutput;
    pSum->hasResult = DATA_SET_FLAG;
  }
}

static void sum_func_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  GET_TRUE_DATA_TYPE();
  assert(pCtx->stableQuery);

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *    input = GET_INPUT_DATA(pCtx, i);
    SSumInfo *pInput = (SSumInfo *)input;
    if (pInput->hasResult != DATA_SET_FLAG) {
      continue;
    }

    notNullElems++;

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      *(int64_t *)pCtx->pOutput += pInput->isum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      *(uint64_t *) pCtx->pOutput += pInput->usum;
    } else {
      SET_DOUBLE_VAL((double *)pCtx->pOutput, *(double *)pCtx->pOutput + pInput->dsum);
    }
  }

  SET_VAL(pCtx, notNullElems, 1);
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static int32_t statisRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_STATIS_NEEDED;
}

static int32_t dataBlockRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  return BLK_DATA_ALL_NEEDED;
}

// todo: if column in current data block are null, opt for this case
static int32_t firstFuncRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }

  // no result for first query, data block is required
  if (GET_RES_INFO(pCtx) == NULL || GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t lastFuncRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order != pCtx->param[0].i64) {
    return BLK_DATA_NO_NEEDED;
  }

  if (GET_RES_INFO(pCtx) == NULL || GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t firstDistFuncRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }

  // not initialized yet, it is the first block, load it.
  if (pCtx->pOutput == NULL) {
    return BLK_DATA_ALL_NEEDED;
  }

  // the pCtx should be set to current Ctx and output buffer before call this function. Otherwise, pCtx->pOutput is
  // the previous windowRes output buffer, not current unloaded block. In this case, the following filter is invalid
  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->pOutput + pCtx->inputBytes);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    return BLK_DATA_ALL_NEEDED;
  } else {  // data in current block is not earlier than current result
    return (pInfo->ts <= w->skey) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

static int32_t lastDistFuncRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  if (pCtx->order != pCtx->param[0].i64) {
    return BLK_DATA_NO_NEEDED;
  }

  // not initialized yet, it is the first block, load it.
  if (pCtx->pOutput == NULL) {
    return BLK_DATA_ALL_NEEDED;
  }

  // the pCtx should be set to current Ctx and output buffer before call this function. Otherwise, pCtx->pOutput is
  // the previous windowRes output buffer, not current unloaded block. In this case, the following filter is invalid
  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->pOutput + pCtx->inputBytes);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return (pInfo->ts >= w->ekey) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

static int32_t tailFuncRequired(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId) {
  // not initialized yet, it is the first block, load it.
  if (pCtx->pOutput == NULL) {
    return BLK_DATA_ALL_NEEDED;
  }

  // the pCtx should be set to current Ctx and output buffer before call this function. Otherwise, pCtx->pOutput is
  // the previous windowRes output buffer, not current unloaded block. In this case, the following filter is invalid
  STailInfo *pInfo = (STailInfo*) (pCtx->pOutput);
  TailUnit **pList = pInfo->res;
  if (pInfo->num >= pCtx->param[0].i64 && pList[0]->timestamp > w->ekey){
    return BLK_DATA_NO_NEEDED;
  } else {
    return BLK_DATA_ALL_NEEDED;
  }
}

//////////////////////////////////////////////////////////////////////////////////////////////
/*
 * The intermediate result of average is kept in the interResultBuf.
 * For super table query, once the avg_function/avg_function_f is finished, copy the intermediate
 * result into output buffer.
 */
static void avg_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  // NOTE: keep the intermediate result into the interResultBuf
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  SAvgInfo *pAvgInfo = (SAvgInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  double   *pVal = &pAvgInfo->sum;

  if (pCtx->preAggVals.isSet) { // Pre-aggregation
    notNullElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    assert(notNullElems >= 0);

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      *pVal += pCtx->preAggVals.statis.sum;
    }  else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      *pVal += (uint64_t) pCtx->preAggVals.statis.sum;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      *pVal += GET_DOUBLE_VAL((const char *)&(pCtx->preAggVals.statis.sum));
    }
  } else {
    void *pData = GET_INPUT_DATA_LIST(pCtx);

    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_ADD_N(*pVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N_DOUBLE(*pVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N_DOUBLE_FLOAT(*pVal, pCtx, pData, float, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
      LIST_ADD_N(*pVal, pCtx, pData, uint64_t, notNullElems, pCtx->inputType);
    }
  }

  if (!pCtx->hasNull) {
    assert(notNullElems == pCtx->size);
  }

  SET_VAL(pCtx, notNullElems, 1);
  pAvgInfo->num += notNullElems;

  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }

  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SAvgInfo));
  }
}

static void avg_func_merge(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  double *sum = (double*) pCtx->pOutput;
  char   *input = GET_INPUT_DATA_LIST(pCtx);

  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgInfo *pInput = (SAvgInfo *)input;
    if (pInput->num == 0) {  // current input is null
      continue;
    }

    SET_DOUBLE_VAL(sum, *sum + pInput->sum);

    // keep the number of data into the temp buffer
    *(int64_t *)GET_ROWCELL_INTERBUF(pResInfo) += pInput->num;
  }
}

/*
 * the average value is calculated in finalize routine, since current routine does not know the exact number of points
 */
static void avg_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  if (pCtx->currentStage == MERGE_STAGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);

    if (GET_INT64_VAL(GET_ROWCELL_INTERBUF(pResInfo)) <= 0) {
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }

    SET_DOUBLE_VAL((double *)pCtx->pOutput,(*(double *)pCtx->pOutput) / *(int64_t *)GET_ROWCELL_INTERBUF(pResInfo));
  } else {  // this is the secondary merge, only in the secondary merge, the input type is TSDB_DATA_TYPE_BINARY
    assert(IS_NUMERIC_TYPE(pCtx->inputType));
    SAvgInfo *pAvgInfo = (SAvgInfo *)GET_ROWCELL_INTERBUF(pResInfo);

    if (pAvgInfo->num == 0) {  // all data are NULL or empty table
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }

    SET_DOUBLE_VAL((double *)pCtx->pOutput, pAvgInfo->sum / pAvgInfo->num);
  }

  // cannot set the numOfIteratedElems again since it is set during previous iteration
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////////////////

static void minMax_function(SQLFunctionCtx *pCtx, char *pOutput, int32_t isMin, int32_t *notNullElems) {
  // data in current data block are qualified to the query
  if (pCtx->preAggVals.isSet) {
    *notNullElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    assert(*notNullElems >= 0);

    if (*notNullElems == 0) {
      return;
    }

    void*   tval = NULL;
    int16_t index = 0;

    if (isMin) {
      tval = &pCtx->preAggVals.statis.min;
      index = pCtx->preAggVals.statis.minIndex;
    } else {
      tval = &pCtx->preAggVals.statis.max;
      index = pCtx->preAggVals.statis.maxIndex;
    }

    TSKEY key = TSKEY_INITIAL_VAL;
    if (pCtx->ptsList != NULL) {
      /**
       * NOTE: work around the bug caused by invalid pre-calculated function.
       * Here the selectivity + ts will not return correct value.
       *
       * The following codes of 3 lines will be removed later.
       */
//      if (index < 0 || index >= pCtx->size + pCtx->startOffset) {
//        index = 0;
//      }

      // the index is the original position, not the relative position
      key = pCtx->ptsList[index];
    }

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t val = GET_INT64_VAL(tval);
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        int8_t *data = (int8_t *)pOutput;

        UPDATE_DATA(pCtx, *data, (int8_t)val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        int16_t *data = (int16_t *)pOutput;

        UPDATE_DATA(pCtx, *data, (int16_t)val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        int32_t *data = (int32_t *)pOutput;
#if defined(_DEBUG_VIEW)
        qDebug("max value updated according to pre-cal:%d", *data);
#endif

        if ((*data < val) ^ isMin) {
          *data = (int32_t)val;
          for (int32_t i = 0; i < (pCtx)->tagInfo.numOfTagCols; ++i) {
            SQLFunctionCtx *__ctx = pCtx->tagInfo.pTagCtxList[i];
            if (__ctx->functionId == TSDB_FUNC_TS_DUMMY) {
              __ctx->tag.i64 = key;
              __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
            }

            aAggs[TSDB_FUNC_TAG].xFunction(__ctx);
          }
        }
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        int64_t *data = (int64_t *)pOutput;
        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t val = GET_UINT64_VAL(tval);
      if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
        uint8_t *data = (uint8_t *)pOutput;

        UPDATE_DATA(pCtx, *data, (uint8_t)val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
        uint16_t *data = (uint16_t *)pOutput;
        UPDATE_DATA(pCtx, *data, (uint16_t)val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
        uint32_t *data = (uint32_t *)pOutput;
        UPDATE_DATA(pCtx, *data, (uint32_t)val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
        uint64_t *data = (uint64_t *)pOutput;
        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
        double *data = (double *)pOutput;
        double  val = GET_DOUBLE_VAL(tval);

        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      float *data = (float *)pOutput;
      double val = GET_DOUBLE_VAL(tval);

      UPDATE_DATA(pCtx, *data, (float)val, notNullElems, isMin, key);
    }

    return;
  }

  void  *p = GET_INPUT_DATA_LIST(pCtx);
  TSKEY *tsList = GET_TS_LIST(pCtx);

  *notNullElems = 0;

  if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      TYPED_LOOPCHECK_N(int8_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      TYPED_LOOPCHECK_N(int16_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      int32_t *pData = p;
      int32_t *retVal = (int32_t*) pOutput;

      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (pCtx->hasNull && isNull((const char*)&pData[i], pCtx->inputType)) {
          continue;
        }

        if ((*retVal < pData[i]) ^ isMin) {
          *retVal = pData[i];
          if(tsList) {
            TSKEY k = tsList[i];
            DO_UPDATE_TAG_COLUMNS(pCtx, k);
          }
        }
        *notNullElems += 1;
      }
#if defined(_DEBUG_VIEW)
      qDebug("max value updated:%d", *retVal);
#endif
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      TYPED_LOOPCHECK_N(int64_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
    if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
      TYPED_LOOPCHECK_N(uint8_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
      TYPED_LOOPCHECK_N(uint16_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
      TYPED_LOOPCHECK_N(uint32_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
      TYPED_LOOPCHECK_N(uint64_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    TYPED_LOOPCHECK_N(double, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    TYPED_LOOPCHECK_N(float, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
  }
}

static bool min_func_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  GET_TRUE_DATA_TYPE();

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->pOutput) = INT8_MAX;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *(uint8_t *) pCtx->pOutput = UINT8_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->pOutput) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)pCtx->pOutput) = UINT16_MAX;
      break;
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->pOutput) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)pCtx->pOutput) = UINT32_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->pOutput) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)pCtx->pOutput) = UINT64_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->pOutput) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double *)pCtx->pOutput), DBL_MAX);
      break;
    default:
      qError("illegal data type:%d in min/max query", pCtx->inputType);
  }

  return true;
}

static bool max_func_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  GET_TRUE_DATA_TYPE();

  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->pOutput) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_UINT:
      *((uint32_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->pOutput) = -FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      SET_DOUBLE_VAL(((double *)pCtx->pOutput), -DBL_MAX);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->pOutput) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      *((uint64_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->pOutput) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      *((uint16_t *)pCtx->pOutput) = 0;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->pOutput) = INT8_MIN;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      *((uint8_t *)pCtx->pOutput) = 0;
      break;
    default:
      qError("illegal data type:%d in min/max query", pCtx->inputType);
  }

  return true;
}

/*
 * the output result of min/max function is the final output buffer, not the intermediate result buffer
 */
static void min_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->pOutput, 1, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;

    // set the flag for super table query
    if (pCtx->stableQuery) {
      *(pCtx->pOutput + pCtx->inputBytes) = DATA_SET_FLAG;
    }
  }
}

static void max_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->pOutput, 0, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;

    // set the flag for super table query
    if (pCtx->stableQuery) {
      *(pCtx->pOutput + pCtx->inputBytes) = DATA_SET_FLAG;
    }
  }
}

static int32_t minmax_merge_impl(SQLFunctionCtx *pCtx, int32_t bytes, char *output, bool isMin) {
  int32_t notNullElems = 0;

  GET_TRUE_DATA_TYPE();
  assert(pCtx->stableQuery);

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *input = GET_INPUT_DATA(pCtx, i);
    if (input[bytes] != DATA_SET_FLAG) {
      continue;
    }

    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t v = GET_INT8_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int8_t *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t v = GET_INT16_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int16_t *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t v = GET_INT32_VAL(input);
        if ((*(int32_t *)output < v) ^ isMin) {
          *(int32_t *)output = v;

          for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
            SQLFunctionCtx *__ctx = pCtx->tagInfo.pTagCtxList[j];
            aAggs[TSDB_FUNC_TAG].xFunction(__ctx);
          }

          notNullElems++;
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float v = GET_FLOAT_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(float *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double v = GET_DOUBLE_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(double *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t v = GET_INT64_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int64_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t v = GET_UINT8_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint8_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t v = GET_UINT16_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint16_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UINT: {
        uint32_t v = GET_UINT32_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint32_t *)output, v, notNullElems, isMin);
        break;
      }

      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t v = GET_UINT64_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(uint64_t *)output, v, notNullElems, isMin);
        break;
      }

      default:
        break;
    }
  }

  return notNullElems;
}

static void min_func_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_merge_impl(pCtx, pCtx->outputBytes, pCtx->pOutput, 1);

  SET_VAL(pCtx, notNullElems, 1);

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void max_func_merge(SQLFunctionCtx *pCtx) {
  int32_t numOfElem = minmax_merge_impl(pCtx, pCtx->outputBytes, pCtx->pOutput, 0);

  SET_VAL(pCtx, numOfElem, 1);

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (numOfElem > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

#define LOOP_STDDEV_IMPL(type, r, d, ctx, delta, _type, num)          \
  for (int32_t i = 0; i < (ctx)->size; ++i) {                         \
    if ((ctx)->hasNull && isNull((char *)&((type *)d)[i], (_type))) { \
      continue;                                                       \
    }                                                                 \
    (num) += 1;                                                       \
    (r) += POW2(((type *)d)[i] - (delta));                            \
  }

static void stddev_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevInfo *pStd = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == REPEAT_SCAN && pStd->stage == 0) {
    pStd->stage++;
    avg_finalizer(pCtx);

    pResInfo->initialized = true; // set it initialized to avoid re-initialization

    // save average value into tmpBuf, for second stage scan
    SAvgInfo *pAvg = GET_ROWCELL_INTERBUF(pResInfo);

    pStd->avg = GET_DOUBLE_VAL(pCtx->pOutput);
    assert((isnan(pAvg->sum) && pAvg->num == 0) || (pStd->num == pAvg->num && pStd->avg == pAvg->sum));
  }
  
  if (pStd->stage == 0) {
    // the first stage is to calculate average value
    avg_function(pCtx);
  } else if (pStd->num > 0) {
    // the second stage to calculate standard deviation
    // if pStd->num == 0, there are no numbers in the first round check. No need to do the second round
    double *retVal = &pStd->res;
    double  avg = pStd->avg;
    
    void *pData = GET_INPUT_DATA_LIST(pCtx);
    int32_t num = 0;

    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        for (int32_t i = 0; i < pCtx->size; ++i) {
          if (pCtx->hasNull && isNull((const char*) (&((int32_t *)pData)[i]), pCtx->inputType)) {
            continue;
          }
          num += 1;
          *retVal += POW2(((int32_t *)pData)[i] - avg);
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        LOOP_STDDEV_IMPL(uint64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        LOOP_STDDEV_IMPL(uint16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        LOOP_STDDEV_IMPL(uint8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        LOOP_STDDEV_IMPL(uint32_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
        break;
      }
      default:
        qError("stddev function not support data type:%d", pCtx->inputType);
    }
    
    SET_VAL(pCtx, 1, 1);
  }
}

static void stddev_finalizer(SQLFunctionCtx *pCtx) {
  SStddevInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  
  if (pStd->num <= 0) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
  } else {
    double *retValue = (double *)pCtx->pOutput;
    SET_DOUBLE_VAL(retValue, sqrt(pStd->res / pStd->num));
    SET_VAL(pCtx, 1, 1);
  }
  
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////
int32_t tsCompare(const void* p1, const void* p2) {
  TSKEY k = *(TSKEY*)p1;
  SResPair* pair = (SResPair*)p2;

  if (k == pair->key) {
    return 0;
  } else {
    return k < pair->key? -1:1;
  }
}

static void stddev_dst_function(SQLFunctionCtx *pCtx) {
  SStddevdstInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // the second stage to calculate standard deviation
  double *retVal = &pStd->res;

  // all data are null, no need to proceed
  SArray* resList = (SArray*) pCtx->param[0].pz;
  if (resList == NULL) {
    return;
  }

  // find the correct group average results according to the tag value
  int32_t len = (int32_t) taosArrayGetSize(resList);
  assert(len > 0);

  double avg = 0;
  if (len == 1) {
    SResPair* p = taosArrayGet(resList, 0);
    avg = p->avg;
  } else {  // todo opt performance by using iterator since the timestamp lsit is matched with the output result
    SResPair* p = bsearch(&pCtx->startTs, resList->pData, len, sizeof(SResPair), tsCompare);
    if (p == NULL) {
      return;
    }

    avg = p->avg;
  }

  void *pData = GET_INPUT_DATA_LIST(pCtx);
  int32_t num = 0;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (pCtx->hasNull && isNull((const char*) (&((int32_t *)pData)[i]), pCtx->inputType)) {
          continue;
        }
        num += 1;
        *retVal += POW2(((int32_t *)pData)[i] - avg);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      LOOP_STDDEV_IMPL(uint16_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      LOOP_STDDEV_IMPL(uint32_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      LOOP_STDDEV_IMPL(uint64_t, *retVal, pData, pCtx, avg, pCtx->inputType, num);
      break;
    }
    default:
      qError("stddev function not support data type:%d", pCtx->inputType);
  }

  pStd->num += num;
  SET_VAL(pCtx, num, 1);

  // copy to the final output buffer for super table
  memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx)), sizeof(SAvgInfo));
}

static void stddev_dst_merge(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevdstInfo* pRes = GET_ROWCELL_INTERBUF(pResInfo);

  char   *input = GET_INPUT_DATA_LIST(pCtx);

  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SStddevdstInfo *pInput = (SStddevdstInfo *)input;
    if (pInput->num == 0) {  // current input is null
      continue;
    }

    pRes->num += pInput->num;
    pRes->res += pInput->res;
  }
}

static void stddev_dst_finalizer(SQLFunctionCtx *pCtx) {
  SStddevdstInfo *pStd = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pStd->num <= 0) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
  } else {
    double *retValue = (double *)pCtx->pOutput;
    SET_DOUBLE_VAL(retValue, sqrt(pStd->res / pStd->num));
    SET_VAL(pCtx, 1, 1);
  }

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////
static bool first_last_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  // used to keep the timestamp for comparison
  pCtx->param[1].nType = 0;
  pCtx->param[1].i64 = 0;
  
  return true;
}

// todo opt for null block
static void first_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo* pResInfo = GET_RES_INFO(pCtx);
  int32_t notNullElems = 0;
  int32_t step = 1;
  int32_t i = 0;
  bool inputAsc = true;

  // input data come from sub query, input data order equal to sub query order
  if(pCtx->numOfParams == 3) {
    if(pCtx->param[2].nType == TSDB_DATA_TYPE_INT && pCtx->param[2].i64 == TSDB_ORDER_DESC) {
      step = -1;
      i = pCtx->size - 1;
      inputAsc = false;
    }
  } else if (pCtx->order == TSDB_ORDER_DESC) {
    return ;
  }

  if(pCtx->order == TSDB_ORDER_ASC && inputAsc) {
    for (int32_t m = 0; m < pCtx->size; ++m, i+=step) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
        continue;
      }
      
      memcpy(pCtx->pOutput, data, pCtx->inputBytes);
      if (pCtx->ptsList != NULL) {
        TSKEY k = GET_TS_DATA(pCtx, i);
        DO_UPDATE_TAG_COLUMNS(pCtx, k);
      }

      SResultRowCellInfo *pInfo = GET_RES_INFO(pCtx);
      pInfo->hasResult = DATA_SET_FLAG;
      pInfo->complete = true;
      
      notNullElems++;
      break;
    }
  } else {  // desc order
    for (int32_t m = 0; m < pCtx->size; ++m, i+=step) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType) && (!pCtx->requireNull)) {
        continue;
      }

      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;

      char* buf = GET_ROWCELL_INTERBUF(pResInfo);
      if (pResInfo->hasResult != DATA_SET_FLAG || (*(TSKEY*)buf) > ts) {
        pResInfo->hasResult = DATA_SET_FLAG;
        memcpy(pCtx->pOutput, data, pCtx->inputBytes);

        *(TSKEY*)buf = ts;
        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
      }

      notNullElems++;
      break;
    }
  }
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = GET_TS_LIST(pCtx);
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->pOutput + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || timestamp[index] < pInfo->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->inputBytes);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->ts = timestamp[index];
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo->ts);
  }
}

/*
 * format of intermediate result: "timestamp,value" need to compare the timestamp in the first part (before the comma)
 * to decide if the value is earlier than current intermediate result
 */
static void first_dist_function(SQLFunctionCtx *pCtx) {
  /*
   * do not to check data in the following cases:
   * 1. data block that are not loaded
   * 2. scan data files in desc order
   */
  if (pCtx->order == TSDB_ORDER_DESC/* || pCtx->preAggVals.dataBlockLoaded == false*/) {
    return;
  }
  
  int32_t notNullElems = 0;

  // find the first not null value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    first_data_assign_impl(pCtx, data, i);
    
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_dist_func_merge(SQLFunctionCtx *pCtx) {
  assert(pCtx->stableQuery);

  char *          pData = GET_INPUT_DATA_LIST(pCtx);
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->outputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  // The param[1] is used to keep the initial value of max ts value
  if (pCtx->param[1].nType != pCtx->outputType || pCtx->param[1].i64 > pInput->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->outputBytes);
    pCtx->param[1].i64 = pInput->ts;
    pCtx->param[1].nType = pCtx->outputType;
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
  
  SET_VAL(pCtx, 1, 1);
  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

//////////////////////////////////////////////////////////////////////////////////////////
/*
 * last function:
 * 1. since the last block may be all null value, so, we simply access the last block is not valid
 *    each block need to be checked.
 * 2. If numOfNull == pBlock->numOfBlocks, the whole block is empty. Otherwise, there is at
 *    least one data in this block that is not null.(TODO opt for this case)
 */
static void last_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo* pResInfo = GET_RES_INFO(pCtx);
  int32_t notNullElems = 0;
  int32_t step = -1;
  int32_t i = pCtx->size - 1;

  // input data come from sub query, input data order equal to sub query order
  if(pCtx->numOfParams == 3) {
    if(pCtx->param[2].nType == TSDB_DATA_TYPE_INT && pCtx->param[2].i64 == TSDB_ORDER_DESC) {
      step = 1;
      i = 0;
    }
  } else if (pCtx->order != pCtx->param[0].i64) {
    return;
  }

  if (pCtx->order == TSDB_ORDER_DESC) {
    for (int32_t m = pCtx->size - 1; m >= 0; --m, i += step) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType) && (!pCtx->requireNull)) {
        continue;
      }

      memcpy(pCtx->pOutput, data, pCtx->inputBytes);

      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;
      DO_UPDATE_TAG_COLUMNS(pCtx, ts);

      pResInfo->hasResult = DATA_SET_FLAG;
      pResInfo->complete = true;  // set query completed on this column
      notNullElems++;
      break;
    }
  } else {  // ascending order
    for (int32_t m = pCtx->size - 1; m >= 0; --m, i += step) {
      char *data = GET_INPUT_DATA(pCtx, i);
      if (pCtx->hasNull && isNull(data, pCtx->inputType) && (!pCtx->requireNull)) {
        continue;
      }

      TSKEY ts = pCtx->ptsList ? GET_TS_DATA(pCtx, i) : 0;

      char* buf = GET_ROWCELL_INTERBUF(pResInfo);
      if (pResInfo->hasResult != DATA_SET_FLAG || (*(TSKEY*)buf) < ts) {
        pResInfo->hasResult = DATA_SET_FLAG;
        memcpy(pCtx->pOutput, data, pCtx->inputBytes);

        *(TSKEY*)buf = ts;
        DO_UPDATE_TAG_COLUMNS(pCtx, ts);
      }

      notNullElems++;
      break;
    }
  }

  SET_VAL(pCtx, notNullElems, 1);
}

static void last_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = GET_TS_LIST(pCtx);
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->pOutput + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || pInfo->ts < timestamp[index]) {
#if defined(_DEBUG_VIEW)
    qDebug("assign index:%d, ts:%" PRId64 ", val:%d, ", index, timestamp[index], *(int32_t *)pData);
#endif
    
    memcpy(pCtx->pOutput, pData, pCtx->inputBytes);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->ts = timestamp[index];
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo->ts);
  }
}

static void last_dist_function(SQLFunctionCtx *pCtx) {
  /*
   * 1. for scan data is not the required order
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (pCtx->order != pCtx->param[0].i64) {
    return;
  }

  int32_t notNullElems = 0;
  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      if (!pCtx->requireNull) {
        continue; 
      }
    }
    
    last_data_assign_impl(pCtx, data, i);
    
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

/*
 * in the secondary merge(local reduce), the output is limited by the
 * final output size, so the main difference between last_dist_func_merge and second_merge
 * is: the output data format in computing
 */
static void last_dist_func_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_DATA_LIST(pCtx);
  
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->outputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  /*
   * param[1] used to keep the corresponding timestamp to decide if current result is
   * the true last result
   */
  if (pCtx->param[1].nType != pCtx->outputType || pCtx->param[1].i64 < pInput->ts) {
    memcpy(pCtx->pOutput, pData, pCtx->outputBytes);
    pCtx->param[1].i64 = pInput->ts;
    pCtx->param[1].nType = pCtx->outputType;
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
  
  SET_VAL(pCtx, 1, 1);
  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

//////////////////////////////////////////////////////////////////////////////////
/*
 * NOTE: last_row does not use the interResultBuf to keep the result
 */
static void last_row_function(SQLFunctionCtx *pCtx) {
  assert(pCtx->size >= 1);
  char *pData = GET_INPUT_DATA_LIST(pCtx);

  // assign the last element in current data block
  assignVal(pCtx->pOutput, pData + (pCtx->size - 1) * pCtx->inputBytes, pCtx->inputBytes, pCtx->inputType);
  
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  
  // set the result to final result buffer in case of super table query
  if (pCtx->stableQuery) {
    SLastrowInfo *pInfo1 = (SLastrowInfo *)(pCtx->pOutput + pCtx->inputBytes);
    pInfo1->ts = GET_TS_DATA(pCtx, pCtx->size - 1);
    pInfo1->hasResult = DATA_SET_FLAG;
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo1->ts);
  } else {
    TSKEY ts = GET_TS_DATA(pCtx, pCtx->size - 1);
    DO_UPDATE_TAG_COLUMNS(pCtx, ts);
  }

  SET_VAL(pCtx, pCtx->size, 1);
}

static void last_row_finalizer(SQLFunctionCtx *pCtx) {
  // do nothing at the first stage
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
    return;
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static void valuePairAssign(tValuePair *dst, int16_t type, const char *val, int64_t tsKey, char *pTags,
                            SExtTagsInfo *pTagInfo, int16_t stage) {
  dst->v.nType = type;
  dst->v.i64 = *(int64_t *)val;
  dst->timestamp = tsKey;
  
  int32_t size = 0;
  if (stage == MERGE_STAGE) {
    memcpy(dst->pTags, pTags, (size_t)pTagInfo->tagsLen);
  } else {  // the tags are dumped from the ctx tag fields
    for (int32_t i = 0; i < pTagInfo->numOfTagCols; ++i) {
      SQLFunctionCtx* ctx = pTagInfo->pTagCtxList[i];
      if (ctx->functionId == TSDB_FUNC_TS_DUMMY) {
        ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
        ctx->tag.i64 = tsKey;
      }
      
      tVariantDump(&ctx->tag, dst->pTags + size, ctx->tag.nType, true);
      size += pTagInfo->pTagCtxList[i]->outputBytes;
    }
  }
}

#define VALUEPAIRASSIGN(dst, src, __l)                 \
  do {                                                 \
    (dst)->timestamp = (src)->timestamp;               \
    (dst)->v = (src)->v;                               \
    memcpy((dst)->pTags, (src)->pTags, (size_t)(__l)); \
  } while (0)

static int32_t topBotComparFn(const void *p1, const void *p2, const void *param)
{
  uint16_t     type = *(uint16_t *) param;
  tValuePair  *val1 = *(tValuePair **) p1;
  tValuePair  *val2 = *(tValuePair **) p2;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.i64 == val2->v.i64) {
      return 0;
    }

    return (val1->v.i64 > val2->v.i64) ? 1 : -1;
   } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
     if (val1->v.u64 == val2->v.u64) {
       return 0;
     }

     return (val1->v.u64 > val2->v.u64) ? 1 : -1;
  }

  if (val1->v.dKey == val2->v.dKey) {
    return 0;
  }

  return (val1->v.dKey > val2->v.dKey) ? 1 : -1;
}

static void topBotSwapFn(void *dst, void *src, const void *param)
{
  char         tag[32768];
  tValuePair   temp;
  uint16_t     tagLen = *(uint16_t *) param;
  tValuePair  *vdst = *(tValuePair **) dst;
  tValuePair  *vsrc = *(tValuePair **) src;

  memset(tag, 0, sizeof(tag));
  temp.pTags = tag;

  VALUEPAIRASSIGN(&temp, vdst, tagLen);
  VALUEPAIRASSIGN(vdst, vsrc, tagLen);
  VALUEPAIRASSIGN(vsrc, &temp, tagLen);
}

static void do_top_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  tVariant val = {0};
  tVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);
  
  tValuePair **pList = pInfo->res;
  assert(pList != NULL);

  if (pInfo->num < maxLen) {
    valuePairAssign(pList[pInfo->num], type, (const char *)&val.i64, ts, pTags, pTagInfo, stage);

    taosheapsort((void *) pList, sizeof(tValuePair **), pInfo->num + 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 0);
 
    pInfo->num++;
  } else {
    if ((IS_SIGNED_NUMERIC_TYPE(type) && val.i64 > pList[0]->v.i64) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u64 > pList[0]->v.u64) ||
        (IS_FLOAT_TYPE(type) && val.dKey > pList[0]->v.dKey)) {
      valuePairAssign(pList[0], type, (const char *)&val.i64, ts, pTags, pTagInfo, stage);
      taosheapadjust((void *) pList, sizeof(tValuePair **), 0, maxLen - 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 0);
    }
  }
}

static void do_bottom_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                   SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  tVariant val = {0};
  tVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);

  tValuePair **pList = pInfo->res;
  assert(pList != NULL);

  if (pInfo->num < maxLen) {
    valuePairAssign(pList[pInfo->num], type, (const char *)&val.i64, ts, pTags, pTagInfo, stage);

    taosheapsort((void *) pList, sizeof(tValuePair **), pInfo->num + 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 1);

    pInfo->num++;
  } else {
    if ((IS_SIGNED_NUMERIC_TYPE(type) && val.i64 < pList[0]->v.i64) ||
        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u64 < pList[0]->v.u64) ||
        (IS_FLOAT_TYPE(type) && val.dKey < pList[0]->v.dKey)) {
      valuePairAssign(pList[0], type, (const char *)&val.i64, ts, pTags, pTagInfo, stage);
      taosheapadjust((void *) pList, sizeof(tValuePair **), 0, maxLen - 1, (const void *) &type, topBotComparFn, (const void *) &pTagInfo->tagsLen, topBotSwapFn, 1);
    }
  }
}

static int32_t resAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = *(tValuePair **)pLeft;
  tValuePair *pRightElem = *(tValuePair **)pRight;
  
  if (pLeftElem->timestamp == pRightElem->timestamp) {
    return 0;
  } else {
    return pLeftElem->timestamp > pRightElem->timestamp ? 1 : -1;
  }
}

static int32_t resDescComparFn(const void *pLeft, const void *pRight) { return -resAscComparFn(pLeft, pRight); }

static int32_t resDataAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = *(tValuePair **)pLeft;
  tValuePair *pRightElem = *(tValuePair **)pRight;
  
  if (IS_FLOAT_TYPE(pLeftElem->v.nType)) {
    if (pLeftElem->v.dKey == pRightElem->v.dKey) {
      return 0;
    } else {
      return pLeftElem->v.dKey > pRightElem->v.dKey ? 1 : -1;
    }
  } else if (IS_SIGNED_NUMERIC_TYPE(pLeftElem->v.nType)){
    if (pLeftElem->v.i64 == pRightElem->v.i64) {
      return 0;
    } else {
      return pLeftElem->v.i64 > pRightElem->v.i64 ? 1 : -1;
    }
  } else {
    if (pLeftElem->v.u64 == pRightElem->v.u64) {
      return 0;
    } else {
      return pLeftElem->v.u64 > pRightElem->v.u64 ? 1 : -1;
    }
  }
}

static int32_t resDataDescComparFn(const void *pLeft, const void *pRight) { return -resDataAscComparFn(pLeft, pRight); }

static void copyTopBotRes(SQLFunctionCtx *pCtx, int32_t type) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  STopBotInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);
  
  tValuePair **tvp = pRes->res;
  
  int32_t step = QUERY_ASC_FORWARD_STEP;
  int32_t len = (int32_t)(GET_RES_INFO(pCtx)->numOfRes);

  switch (type) {
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT: {
      int32_t *output = (int32_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int32_t)tvp[i]->v.i64;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *output = (int64_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i64;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *output = (double *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        SET_DOUBLE_VAL(output, tvp[i]->v.dKey);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *output = (float *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (float)tvp[i]->v.dKey;
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *output = (int16_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int16_t)tvp[i]->v.i64;
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *output = (int8_t *)pCtx->pOutput;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = (int8_t)tvp[i]->v.i64;
      }
      break;
    }
    default: {
      qError("top/bottom function not support data type:%d", pCtx->inputType);
      return;
    }
  }
  
  // set the output timestamp of each record.
  TSKEY *output = pCtx->ptsOutputBuf;
  for (int32_t i = 0; i < len; ++i, output += step) {
    *output = tvp[i]->timestamp;
  }
  
  // set the corresponding tag data for each record
  // todo check malloc failure
  if (pCtx->tagInfo.numOfTagCols == 0) {
    return ;
  }

  char **pData = calloc(pCtx->tagInfo.numOfTagCols, POINTER_BYTES);
  for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
    pData[i] = pCtx->tagInfo.pTagCtxList[i]->pOutput;
  }
  
  for (int32_t i = 0; i < len; ++i, output += step) {
    int16_t offset = 0;
    for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
      memcpy(pData[j], tvp[i]->pTags + offset, (size_t)pCtx->tagInfo.pTagCtxList[j]->outputBytes);
      offset += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
      pData[j] += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
    }
  }
  
  tfree(pData);
}

/*
 * keep the intermediate results during scan data blocks in the format of:
 * +-----------------------------------+-------------one value pair-----------+------------next value pair-----------+
 * |-------------pointer area----------|----ts---+-----+-----n tags-----------|----ts---+-----+-----n tags-----------|
 * +..[Value Pointer1][Value Pointer2].|timestamp|value|tags1|tags2|....|tagsn|timestamp|value|tags1|tags2|....|tagsn+
 */
static void buildTopBotStruct(STopBotInfo *pTopBotInfo, SQLFunctionCtx *pCtx) {
  char *tmp = (char *)pTopBotInfo + sizeof(STopBotInfo);
  pTopBotInfo->res = (tValuePair**) tmp;
  tmp += POINTER_BYTES * pCtx->param[0].i64;

  size_t size = sizeof(tValuePair) + pCtx->tagInfo.tagsLen;

  for (int32_t i = 0; i < pCtx->param[0].i64; ++i) {
    pTopBotInfo->res[i] = (tValuePair*) tmp;
    pTopBotInfo->res[i]->pTags = tmp + sizeof(tValuePair);
    tmp += size;
  }
}

bool topbot_datablock_filter(SQLFunctionCtx *pCtx, const char *minval, const char *maxval) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo == NULL) {
    return true;
  }

  STopBotInfo *pTopBotInfo = getOutputInfo(pCtx);
  
  // required number of results are not reached, continue load data block
  if (pTopBotInfo->num < pCtx->param[0].i64) {
    return true;
  }
  
  if ((void *)pTopBotInfo->res[0] != (void *)((char *)pTopBotInfo + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i64)) {
    buildTopBotStruct(pTopBotInfo, pCtx);
  }

  tValuePair **pRes = (tValuePair**) pTopBotInfo->res;
  
  if (pCtx->functionId == TSDB_FUNC_TOP) {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(maxval) > pRes[0]->v.i64;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(maxval) > pRes[0]->v.i64;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(maxval) > pRes[0]->v.i64;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(maxval) > pRes[0]->v.i64;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(maxval) > pRes[0]->v.dKey;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(maxval) > pRes[0]->v.dKey;
      default:
        return true;
    }
  } else {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(minval) < pRes[0]->v.i64;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(minval) < pRes[0]->v.i64;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(minval) < pRes[0]->v.i64;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(minval) < pRes[0]->v.i64;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(minval) < pRes[0]->v.dKey;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(minval) < pRes[0]->v.dKey;
      default:
        return true;
    }
  }
}

static bool top_bottom_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  STopBotInfo *pInfo = getOutputInfo(pCtx);
  buildTopBotStruct(pInfo, pCtx);
  return true;
}

static void top_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  STopBotInfo *pRes = getOutputInfo(pCtx);
  assert(pRes->num >= 0);

  if ((void *)pRes->res[0] != (void *)((char *)pRes + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i64)) {
    buildTopBotStruct(pRes, pCtx);
  }
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;

    // NOTE: Set the default timestamp if it is missing [todo refactor]
    TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
    do_top_function_add(pRes, (int32_t)pCtx->param[0].i64, data, ts, pCtx->inputType, &pCtx->tagInfo, NULL, 0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_func_merge(SQLFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    int16_t type = (pCtx->outputType == TSDB_DATA_TYPE_FLOAT)? TSDB_DATA_TYPE_DOUBLE:pCtx->outputType;
    do_top_function_add(pOutput, (int32_t)pCtx->param[0].i64, &pInput->res[i]->v.i64, pInput->res[i]->timestamp,
                        type, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
  
  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  STopBotInfo *pRes = getOutputInfo(pCtx);
  
  if ((void *)pRes->res[0] != (void *)((char *)pRes + sizeof(STopBotInfo) + POINTER_BYTES * pCtx->param[0].i64)) {
    buildTopBotStruct(pRes, pCtx);
  }

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }

    notNullElems++;
    // NOTE: Set the default timestamp if it is missing [todo refactor]
    TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
    do_bottom_function_add(pRes, (int32_t)pCtx->param[0].i64, data, ts, pCtx->inputType, &pCtx->tagInfo, NULL, 0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_func_merge(SQLFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    int16_t type = (pCtx->outputType == TSDB_DATA_TYPE_FLOAT) ? TSDB_DATA_TYPE_DOUBLE : pCtx->outputType;
    do_bottom_function_add(pOutput, (int32_t)pCtx->param[0].i64, &pInput->res[i]->v.i64, pInput->res[i]->timestamp, type,
                           &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }

  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_bottom_func_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  
  // data in temporary list is less than the required number of results, not enough qualified number of results
  STopBotInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);
  if (pRes->num == 0) {  // no result
    assert(pResInfo->hasResult != DATA_SET_FLAG);
    // TODO:
  }
  
  GET_RES_INFO(pCtx)->numOfRes = pRes->num;
  tValuePair **tvp = pRes->res;
  
  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[2].i64 == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    __compar_fn_t comparator = (pCtx->param[3].i64 == TSDB_ORDER_ASC) ? resAscComparFn : resDescComparFn;
    qsort(tvp, (size_t)pResInfo->numOfRes, POINTER_BYTES, comparator);
  } else /*if (pCtx->param[2].i64 > PRIMARYKEY_TIMESTAMP_COL_INDEX)*/ {
    __compar_fn_t comparator = (pCtx->param[3].i64 == TSDB_ORDER_ASC) ? resDataAscComparFn : resDataDescComparFn;
    qsort(tvp, (size_t)pResInfo->numOfRes, POINTER_BYTES, comparator);
  }
  
  GET_TRUE_DATA_TYPE();
  copyTopBotRes(pCtx, type);
  
  doFinalizer(pCtx);
}

///////////////////////////////////////////////////////////////////////////////////////////////
static bool percentile_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }

  // in the first round, get the min-max value of all involved data
  SPercentileInfo *pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->minval, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->maxval, -DBL_MAX);
  pInfo->numOfElems = 0;

  return true;
}

static void percentile_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == REPEAT_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      
      return;
    } else {
      pInfo->pMemBucket = tMemBucketCreate(pCtx->inputBytes, pCtx->inputType, pInfo->minval, pInfo->maxval);
    }
  }

  // the first stage, only acquire the min/max value
  if (pInfo->stage == 0) {
    if (pCtx->preAggVals.isSet) {
      double tmin = 0.0, tmax = 0.0;
      if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
        tmin = (double)GET_INT64_VAL(&pCtx->preAggVals.statis.min);
        tmax = (double)GET_INT64_VAL(&pCtx->preAggVals.statis.max);
      } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
        tmin = GET_DOUBLE_VAL(&pCtx->preAggVals.statis.min);
        tmax = GET_DOUBLE_VAL(&pCtx->preAggVals.statis.max);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
        tmin = (double)GET_UINT64_VAL(&pCtx->preAggVals.statis.min);
        tmax = (double)GET_UINT64_VAL(&pCtx->preAggVals.statis.max);
      } else {
        assert(true);
      }

      if (GET_DOUBLE_VAL(&pInfo->minval) > tmin) {
        SET_DOUBLE_VAL(&pInfo->minval, tmin);
      }

      if (GET_DOUBLE_VAL(&pInfo->maxval) < tmax) {
        SET_DOUBLE_VAL(&pInfo->maxval, tmax);
      }

      pInfo->numOfElems += (pCtx->size - pCtx->preAggVals.statis.numOfNull);
    } else {
      for (int32_t i = 0; i < pCtx->size; ++i) {
        char *data = GET_INPUT_DATA(pCtx, i);
        if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
          continue;
        }

        double v = 0;
        GET_TYPED_DATA(v, double, pCtx->inputType, data);

        if (v < GET_DOUBLE_VAL(&pInfo->minval)) {
          SET_DOUBLE_VAL(&pInfo->minval, v);
        }

        if (v > GET_DOUBLE_VAL(&pInfo->maxval)) {
          SET_DOUBLE_VAL(&pInfo->maxval, v);
        }

        pInfo->numOfElems += 1;
      }
    }

    return;
  }

  // the second stage, calculate the true percentile value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;
    tMemBucketPut(pInfo->pMemBucket, data, 1);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void percentile_finalizer(SQLFunctionCtx *pCtx) {
  double v = pCtx->param[0].nType == TSDB_DATA_TYPE_INT ? pCtx->param[0].i64 : pCtx->param[0].dKey;
  
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo* ppInfo = (SPercentileInfo *) GET_ROWCELL_INTERBUF(pResInfo);

  tMemBucket * pMemBucket = ppInfo->pMemBucket;
  if (pMemBucket == NULL || pMemBucket->total == 0) {  // check for null
    assert(ppInfo->numOfElems == 0);
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
  } else {
    SET_DOUBLE_VAL((double *)pCtx->pOutput, getPercentile(pMemBucket, v));
  }
  
  tMemBucketDestroy(pMemBucket);
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static void buildHistogramInfo(SAPercentileInfo* pInfo) {
  pInfo->pHisto = (SHistogramInfo*) ((char*) pInfo + sizeof(SAPercentileInfo));
  pInfo->pHisto->elems = (SHistBin*) ((char*)pInfo->pHisto + sizeof(SHistogramInfo));
}

//
//   ----------------- tdigest -------------------
//
//////////////////////////////////////////////////////////////////////////////////

static bool tdigest_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo *pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }
  
  // new TDigest
  SAPercentileInfo *pInfo = getOutputInfo(pCtx);
  char *tmp = (char *)pInfo + sizeof(SAPercentileInfo);
  pInfo->pTDigest = tdigestNewFrom(tmp, COMPRESSION);
  return true;
}

static void tdigest_do(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *  pAPerc = getOutputInfo(pCtx);

  assert(pAPerc->pTDigest != NULL);
  if(pAPerc->pTDigest == NULL) {
    qError("tdigest_do tdigest is null.");
    return ;
  }

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    notNullElems += 1;

    double v = 0; // value  
    long long w = 1; // weigth
    GET_TYPED_DATA(v, double, pCtx->inputType, data);
    tdigestAdd(pAPerc->pTDigest, v, w);
  }

  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }

  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void tdigest_merge(SQLFunctionCtx *pCtx) {
  SAPercentileInfo *pInput = (SAPercentileInfo *)GET_INPUT_DATA_LIST(pCtx);
  assert(pInput->pTDigest);
  pInput->pTDigest = (TDigest*)((char*)pInput + sizeof(SAPercentileInfo));
  tdigestAutoFill(pInput->pTDigest, COMPRESSION);

  // input merge no elements , no need merge
  if(pInput->pTDigest->num_centroids == 0 && pInput->pTDigest->num_buffered_pts == 0) {
    return ;
  }

  SAPercentileInfo *pOutput = getOutputInfo(pCtx);
  if(pOutput->pTDigest->num_centroids == 0) {
    memcpy(pOutput->pTDigest, pInput->pTDigest, (size_t)TDIGEST_SIZE(COMPRESSION));
    tdigestAutoFill(pOutput->pTDigest, COMPRESSION);
  } else {
    tdigestMerge(pOutput->pTDigest, pInput->pTDigest);
  }
  
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  SET_VAL(pCtx, 1, 1);
}

static void tdigest_finalizer(SQLFunctionCtx *pCtx) {
  double q = (pCtx->param[0].nType == TSDB_DATA_TYPE_INT) ? pCtx->param[0].i64 : pCtx->param[0].dKey;

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *  pAPerc = getOutputInfo(pCtx);

  if (pCtx->currentStage == MERGE_STAGE) {
    if (pResInfo->hasResult == DATA_SET_FLAG) {  // check for null
      double res = tdigestQuantile(pAPerc->pTDigest, q/100);
      memcpy(pCtx->pOutput, &res, sizeof(double));
    } else {
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  } else {
    if (pAPerc->pTDigest->size > 0) {
      double res = tdigestQuantile(pAPerc->pTDigest, q/100);
      memcpy(pCtx->pOutput, &res, sizeof(double));
    } else {  // no need to free
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  }

  pAPerc->pTDigest = NULL;
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
int32_t getAlgo(SQLFunctionCtx * pCtx) {
  if(pCtx->numOfParams != 2){
    return ALGO_DEFAULT;
  }
  if(pCtx->param[1].nType != TSDB_DATA_TYPE_INT) {
    return ALGO_DEFAULT;
  }
  return (int32_t)pCtx->param[1].i64;
}

static bool apercentile_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (getAlgo(pCtx) == ALGO_TDIGEST) {
    return tdigest_setup(pCtx, pResultInfo);
  }

  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }
  
  SAPercentileInfo *pInfo = getOutputInfo(pCtx);
  buildHistogramInfo(pInfo);
  
  char *tmp = (char *)pInfo + sizeof(SAPercentileInfo);
  pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
  return true;
}

static void apercentile_function(SQLFunctionCtx *pCtx) {
  if (getAlgo(pCtx) == ALGO_TDIGEST) {
    tdigest_do(pCtx);
    return;
  }

  int32_t notNullElems = 0;
  
  SResultRowCellInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pInfo = getOutputInfo(pCtx);
  buildHistogramInfo(pInfo);

  assert(pInfo->pHisto->elems != NULL);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;

    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, data);
    tHistogramAdd(&pInfo->pHisto, v);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void apercentile_func_merge(SQLFunctionCtx *pCtx) {
  if (getAlgo(pCtx) == ALGO_TDIGEST) {
    tdigest_merge(pCtx);
    return;
  }

  SAPercentileInfo *pInput = (SAPercentileInfo *)GET_INPUT_DATA_LIST(pCtx);
  
  pInput->pHisto = (SHistogramInfo*) ((char *)pInput + sizeof(SAPercentileInfo));
  pInput->pHisto->elems = (SHistBin*) ((char *)pInput->pHisto + sizeof(SHistogramInfo));

  if (pInput->pHisto->numOfElems <= 0) {
    return;
  }
  
  SAPercentileInfo *pOutput = getOutputInfo(pCtx);
  buildHistogramInfo(pOutput);
  SHistogramInfo  *pHisto = pOutput->pHisto;
  
  if (pHisto->numOfElems <= 0) {
    memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
  } else {
    //TODO(dengyihao): avoid memcpy   
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
    memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    tHistogramDestroy(&pRes);
  }

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  SET_VAL(pCtx, 1, 1);
}

static void apercentile_finalizer(SQLFunctionCtx *pCtx) {
  if (getAlgo(pCtx) == ALGO_TDIGEST) {
    tdigest_finalizer(pCtx);
    return;
  }

  double v = (pCtx->param[0].nType == TSDB_DATA_TYPE_INT) ? pCtx->param[0].i64 : pCtx->param[0].dKey;
  
  SResultRowCellInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pOutput = GET_ROWCELL_INTERBUF(pResInfo);

  if (pCtx->currentStage == MERGE_STAGE) {
    if (pResInfo->hasResult == DATA_SET_FLAG) {  // check for null
      assert(pOutput->pHisto->numOfElems > 0);
      
      double  ratio[] = {v};
      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
      
      memcpy(pCtx->pOutput, res, sizeof(double));
      free(res);
    } else {
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  } else {
    if (pOutput->pHisto->numOfElems > 0) {
      double ratio[] = {v};
      
      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
      memcpy(pCtx->pOutput, res, sizeof(double));
      free(res);
    } else {  // no need to free
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  }
  
  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////
static bool leastsquares_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // 2*3 matrix
  pInfo->startVal = pCtx->param[0].dKey;
  return true;
}

#define LEASTSQR_CAL(p, x, y, index, step) \
  do {                                     \
    (p)[0][0] += (double)(x) * (x);        \
    (p)[0][1] += (double)(x);              \
    (p)[0][2] += (double)(x) * (y)[index]; \
    (p)[1][2] += (y)[index];               \
    (x) += step;                           \
  } while (0)

#define LEASTSQR_CAL_LOOP(ctx, param, x, y, tsdbType, n, step) \
  for (int32_t i = 0; i < (ctx)->size; ++i) {                  \
    if ((ctx)->hasNull && isNull((char *)&(y)[i], tsdbType)) { \
      continue;                                                \
    }                                                          \
    (n)++;                                                     \
    LEASTSQR_CAL(param, x, y, i, step);                        \
  }

static void leastsquares_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  double(*param)[3] = pInfo->mat;
  double x = pInfo->startVal;
  
  void *pData = GET_INPUT_DATA_LIST(pCtx);
  
  int32_t numOfElem = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *p = pData;
      //            LEASTSQR_CAL_LOOP(pCtx, param, pParamData, p);
      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (pCtx->hasNull && isNull((const char*) p, pCtx->inputType)) {
          continue;
        }
        
        param[0][0] += x * x;
        param[0][1] += x;
        param[0][2] += x * p[i];
        param[1][2] += p[i];
        
        x += pCtx->param[1].dKey;
        numOfElem++;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    }
  }
  
  pInfo->startVal = x;
  pInfo->num += numOfElem;
  
  if (pInfo->num > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  SET_VAL(pCtx, numOfElem, 1);
}

static void leastsquares_finalizer(SQLFunctionCtx *pCtx) {
  // no data in query
  SResultRowCellInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquaresInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  if (pInfo->num == 0) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
    return;
  }
  
  double(*param)[3] = pInfo->mat;
  
  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];
  
  param[0][0] -= param[1][0] * (param[0][1] / param[1][1]);
  param[0][2] -= param[1][2] * (param[0][1] / param[1][1]);
  param[0][1] = 0;
  param[1][2] -= param[0][2] * (param[1][0] / param[0][0]);
  param[1][0] = 0;
  param[0][2] /= param[0][0];
  
  param[1][2] /= param[1][1];
  
  int32_t maxOutputSize = TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE - VARSTR_HEADER_SIZE;
  size_t n = snprintf(varDataVal(pCtx->pOutput), maxOutputSize, "{slop:%.6lf, intercept:%.6lf}",
      param[0][2], param[1][2]);
  
  varDataSetLen(pCtx->pOutput, n);
  doFinalizer(pCtx);
}

static void date_col_output_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  *(int64_t *)(pCtx->pOutput) = pCtx->startTs;
}

static void col_project_function(SQLFunctionCtx *pCtx) {
  if (pCtx->colId <= TSDB_UD_COLUMN_INDEX && pCtx->colId > TSDB_RES_COL_ID) {    // user-specified constant value
    return;
  }

  // only one row is required.
  if (pCtx->param[0].i64 == 1) {
    SET_VAL(pCtx, pCtx->size, 1);
  } else {
    INC_INIT_VAL(pCtx, pCtx->size);
  }

  char *pData = GET_INPUT_DATA_LIST(pCtx);
  if (pCtx->order == TSDB_ORDER_ASC) {
    // ASC
    int32_t numOfRows = (pCtx->param[0].i64 == 1)? 1:pCtx->size;
    memcpy(pCtx->pOutput, pData, (size_t) numOfRows * pCtx->inputBytes);
  } else {
    // DESC
    for(int32_t i = 0; i < pCtx->size; ++i) {
      char* dst = pCtx->pOutput + (pCtx->size - 1 - i) * pCtx->inputBytes;
      char* src = pData + i * pCtx->inputBytes;
      if (IS_VAR_DATA_TYPE(pCtx->inputType))
        varDataCopy(dst, src);
      else
        memcpy(dst, src, pCtx->inputBytes);
    }
  }
}

/**
 * only used for tag projection query in select clause
 * @param pCtx
 * @return
 */
static void tag_project_function(SQLFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size);
  
  assert(pCtx->inputBytes == pCtx->outputBytes);

  tVariantDump(&pCtx->tag, pCtx->pOutput, pCtx->outputType, true);

  char* data = pCtx->pOutput;
  pCtx->pOutput += pCtx->outputBytes;

  // directly copy from the first one
  for (int32_t i = 1; i < pCtx->size; ++i) {
    memmove(pCtx->pOutput, data, pCtx->outputBytes);
    pCtx->pOutput += pCtx->outputBytes;
  }
}

/**
 * used in group by clause. when applying group by tags, the tags value is
 * assign by using tag function.
 * NOTE: there is only ONE output for ONE query range
 * @param pCtx
 * @return
 */
static void copy_function(SQLFunctionCtx *pCtx);

static void tag_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, 1, 1);
  if (pCtx->currentStage == MERGE_STAGE) {
    copy_function(pCtx);
  } else {
    tVariantDump(&pCtx->tag, pCtx->pOutput, pCtx->outputType, true);
  }
}

static void copy_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  
  char *pData = GET_INPUT_DATA_LIST(pCtx);
  assignVal(pCtx->pOutput, pData, pCtx->inputBytes, pCtx->inputType);
}

static void full_copy_function(SQLFunctionCtx *pCtx) {
  copy_function(pCtx);
  
  for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
    SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
    if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
      aAggs[TSDB_FUNC_TAG].xFunction(tagCtx);
    }
  }
}


static bool diff_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  SDiffFuncInfo* pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pDiffInfo->valueAssigned = false;
  pDiffInfo->i64Prev = 0;
  pDiffInfo->ignoreNegative = (pCtx->param[0].i64 == 1) ? true : false;
  return true;
}

static bool deriv_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResultInfo) {
  if (!function_setup(pCtx, pResultInfo)) {
    return false;
  }

  // diff function require the value is set to -1
  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  pDerivInfo->ignoreNegative = pCtx->param[1].i64;
  pDerivInfo->prevTs   = -1;
  pDerivInfo->tsWindow = pCtx->param[0].i64;
  pDerivInfo->valueSet = false;
  return false;
}

static void deriv_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  void *data = GET_INPUT_DATA_LIST(pCtx);

  int32_t notNullElems = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size - 1;

  TSKEY *pTimestamp = pCtx->ptsOutputBuf;
  TSKEY *tsList = GET_TS_LIST(pCtx);

  double *pOutput = (double *)pCtx->pOutput;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pData = (int32_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          SET_DOUBLE_VAL(pOutput, ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs));
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }

      break;
    };

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pData = (int64_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = (double) pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *pData = (double *)data;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float *pData = (float *)data;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pData = (int16_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pData = (int8_t *)data;
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (!pDerivInfo->valueSet) {  // initial value is not set yet
          pDerivInfo->valueSet  = true;
        } else {
          *pOutput = ((pData[i] - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
          if (pDerivInfo->ignoreNegative && *pOutput < 0) {
          } else {
            *pTimestamp = tsList[i];

            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDerivInfo->prevValue = pData[i];
        pDerivInfo->prevTs    = tsList[i];
      }
      break;
    }
    default:
      qError("error input type");
  }
  if (notNullElems > 0) {
    for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
      SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
      if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
        aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
      }
    }
  }
  GET_RES_INFO(pCtx)->numOfRes += notNullElems;
}


// TODO difference in date column
static void diff_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SDiffFuncInfo *pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);

  void *data = GET_INPUT_DATA_LIST(pCtx);
  bool  isFirstBlock = (pDiffInfo->valueAssigned == false);

  int32_t notNullElems = 0;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size - 1;

  TSKEY* pTimestamp = pCtx->ptsOutputBuf;
  TSKEY* tsList = GET_TS_LIST(pCtx);

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pData = (int32_t *)data;
      int32_t *pOutput = (int32_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {
          int32_t diff = (int32_t)(pData[i] - pDiffInfo->i64Prev);
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            *pOutput = (int32_t)(pData[i] - pDiffInfo->i64Prev);  // direct previous may be null
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->i64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    };

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pData = (int64_t *)data;
      int64_t *pOutput = (int64_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {
          int64_t diff = pData[i] - pDiffInfo->i64Prev;
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            *pOutput = pData[i] - pDiffInfo->i64Prev;  // direct previous may be null
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->i64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double *pData = (double *)data;
      double *pOutput = (double *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {
          double diff = pData[i] - pDiffInfo->d64Prev;
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            SET_DOUBLE_VAL(pOutput, pData[i] - pDiffInfo->d64Prev);  // direct previous may be null
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->d64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float *pData = (float *)data;
      float *pOutput = (float *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {  
          float diff = (float)(pData[i] - pDiffInfo->d64Prev);
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            *pOutput = (float)(pData[i] - pDiffInfo->d64Prev);  
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->d64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pData = (int16_t *)data;
      int16_t *pOutput = (int16_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {
          int16_t diff = (int16_t)(pData[i] - pDiffInfo->i64Prev);
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            *pOutput = (int16_t)(pData[i] - pDiffInfo->i64Prev);
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->i64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pData = (int8_t *)data;
      int8_t *pOutput = (int8_t *)pCtx->pOutput;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((char *)&pData[i], pCtx->inputType)) {
          continue;
        }

        if (pDiffInfo->valueAssigned) {
          int8_t diff = (int8_t)(pData[i] - pDiffInfo->i64Prev);
          if (diff >= 0 || !pDiffInfo->ignoreNegative) {
            *pOutput = (int8_t)(pData[i] - pDiffInfo->i64Prev);
            *pTimestamp = (tsList != NULL)? tsList[i]:0;
            pOutput    += 1;
            pTimestamp += 1;
            notNullElems++;
          }
        }

        pDiffInfo->i64Prev = pData[i];
        pDiffInfo->valueAssigned = true;
      }
      break;
    }

    default:
      qError("error input type");
  }

  // initial value is not set yet
  if (!pDiffInfo->valueAssigned || notNullElems <= 0) {
    /*
     * 1. current block and blocks before are full of null
     * 2. current block may be null value
     */
    assert(pCtx->hasNull);
  } else {
    for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
      SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
      if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
        aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
      }
    }
    int32_t forwardStep = (isFirstBlock) ? notNullElems : notNullElems;

    GET_RES_INFO(pCtx)->numOfRes += forwardStep;
  }
}

char *getScalarExprColumnData(void *param, const char* name, int32_t colId) {
  SScalarExprSupport *pSupport = (SScalarExprSupport *)param;
  
  int32_t index = -1;
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    if (colId == pSupport->colList[i].colId) {
      index = i;
      break;
    }
  }
  
  assert(index >= 0);
  return pSupport->data[index] + pSupport->offset * pSupport->colList[index].bytes;
}

static void scalar_expr_function(SQLFunctionCtx *pCtx) {
  GET_RES_INFO(pCtx)->numOfRes += pCtx->size;
  SScalarExprSupport *sas = (SScalarExprSupport *)pCtx->param[1].pz;
  tExprOperandInfo output;
  output.data = pCtx->pOutput;
  exprTreeNodeTraverse(sas->pExprInfo->pExpr, pCtx->size, &output, sas, pCtx->order, getScalarExprColumnData);
}

#define LIST_MINMAX_N(ctx, minOutput, maxOutput, elemCnt, data, type, tsdbType, numOfNotNullElem) \
  {                                                                                               \
    type *inputData = (type *)data;                                                               \
    for (int32_t i = 0; i < elemCnt; ++i) {                                                       \
      if ((ctx)->hasNull && isNull((char *)&inputData[i], tsdbType)) {                            \
        continue;                                                                                 \
      }                                                                                           \
      if (inputData[i] < minOutput) {                                                             \
        minOutput = (double)inputData[i];                                                                 \
      }                                                                                           \
      if (inputData[i] > maxOutput) {                                                             \
        maxOutput = (double)inputData[i];                                                                 \
      }                                                                                           \
      numOfNotNullElem++;                                                                         \
    }                                                                                             \
  }

/////////////////////////////////////////////////////////////////////////////////
static bool spread_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // this is the server-side setup function in client-side, the secondary merge do not need this procedure
  if (pCtx->currentStage == MERGE_STAGE) {
    pCtx->param[0].dKey = DBL_MAX;
    pCtx->param[3].dKey = -DBL_MAX;
  } else {
    pInfo->min = DBL_MAX;
    pInfo->max = -DBL_MAX;
  }
  
  return true;
}

static void spread_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  int32_t numOfElems = 0;

  // todo : opt with pre-calculated result
  // column missing cause the hasNull to be true
  if (pCtx->preAggVals.isSet) {
    numOfElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    
    // all data are null in current data block, ignore current data block
    if (numOfElems == 0) {
      goto _spread_over;
    }

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType) || IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType) ||
        (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP)) {
      if (pInfo->min > pCtx->preAggVals.statis.min) {
        pInfo->min = (double)pCtx->preAggVals.statis.min;
      }

      if (pInfo->max < pCtx->preAggVals.statis.max) {
        pInfo->max = (double)pCtx->preAggVals.statis.max;
      }
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      if (pInfo->min > GET_DOUBLE_VAL((const char *)&(pCtx->preAggVals.statis.min))) {
        pInfo->min = GET_DOUBLE_VAL((const char *)&(pCtx->preAggVals.statis.min));
      }
      
      if (pInfo->max < GET_DOUBLE_VAL((const char *)&(pCtx->preAggVals.statis.max))) {
        pInfo->max = GET_DOUBLE_VAL((const char *)&(pCtx->preAggVals.statis.max));
      }
    }
    
    goto _spread_over;
  }
  
  void *pData = GET_INPUT_DATA_LIST(pCtx);
  numOfElems = 0;
  
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int8_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int16_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int32_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT || pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, int64_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, double, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, float, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UTINYINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint8_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_USMALLINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint16_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint32_t, pCtx->inputType, numOfElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_UBIGINT) {
    LIST_MINMAX_N(pCtx, pInfo->min, pInfo->max, pCtx->size, pData, uint64_t, pCtx->inputType, numOfElems);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == numOfElems);
  }
  
  _spread_over:
  SET_VAL(pCtx, numOfElems, 1);
  
  if (numOfElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
    pInfo->hasResult = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SSpreadInfo));
  }
}

/*
 * here we set the result value back to the intermediate buffer, to apply the finalize the function
 * the final result is generated in spread_function_finalizer
 */
void spread_func_merge(SQLFunctionCtx *pCtx) {
  SSpreadInfo *pData = (SSpreadInfo *)GET_INPUT_DATA_LIST(pCtx);
  if (pData->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  if (pCtx->param[0].dKey > pData->min) {
    pCtx->param[0].dKey = pData->min;
  }
  
  if (pCtx->param[3].dKey < pData->max) {
    pCtx->param[3].dKey = pData->max;
  }
  
  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

void spread_function_finalizer(SQLFunctionCtx *pCtx) {
  /*
   * here we do not check the input data types, because in case of metric query,
   * the type of intermediate data is binary
   */
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  if (pCtx->currentStage == MERGE_STAGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
    
    if (pResInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
    
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pCtx->param[3].dKey - pCtx->param[0].dKey);
  } else {
    assert(IS_NUMERIC_TYPE(pCtx->inputType) || (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP));
    
    SSpreadInfo *pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
    if (pInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return;
    }
    
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pInfo->max - pInfo->min);
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;  // todo add test case
  doFinalizer(pCtx);
}


/**
 * param[1]: start time
 * param[2]: end time
 * @param pCtx
 */
static bool twa_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->p.key    = INT64_MIN;
  pInfo->win      = TSWINDOW_INITIALIZER;
  return true;
}

static double twa_get_area(SPoint1 s, SPoint1 e) {
  if ((s.val >= 0 && e.val >= 0)|| (s.val <=0 && e.val <= 0)) {
    return (s.val + e.val) * (e.key - s.key) / 2;
  }

  double x = (s.key * e.val - e.key * s.val)/(e.val - s.val);
  double val = (s.val * (x - s.key) + e.val * (e.key - x)) / 2;
  return val;
}

static int32_t twa_function_impl(SQLFunctionCtx* pCtx, int32_t index, int32_t size) {
  int32_t notNullElems = 0;
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY    *tsList = GET_TS_LIST(pCtx);

  int32_t i = index;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  SPoint1* last = &pInfo->p;

  if (pCtx->start.key != INT64_MIN) {
    assert((pCtx->start.key < tsList[i] && pCtx->order == TSDB_ORDER_ASC) ||
               (pCtx->start.key > tsList[i] && pCtx->order == TSDB_ORDER_DESC));

    assert(last->key == INT64_MIN);

    last->key = tsList[i];
    GET_TYPED_DATA(last->val, double, pCtx->inputType, GET_INPUT_DATA(pCtx, index));

    pInfo->dOutput += twa_get_area(pCtx->start, *last);

    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->win.skey = pCtx->start.key;
    notNullElems++;
    i += step;
  } else if (pInfo->p.key == INT64_MIN) {
    last->key = tsList[i];
    GET_TYPED_DATA(last->val, double, pCtx->inputType, GET_INPUT_DATA(pCtx, index));

    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->win.skey = last->key;
    notNullElems++;
    i += step;
  }

  // calculate the value of
  switch(pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *val = (int8_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *val = (int16_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t *val = (int32_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *val = (int64_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = (double) val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double)val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *val = (float*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double)val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *val = (double*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *val = (uint8_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *val = (uint16_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *val = (uint32_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }

#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *val = (uint64_t*) GET_INPUT_DATA(pCtx, 0);
      for (; i < size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &val[i], pCtx->inputType)) {
          continue;
        }
        
#ifndef _TD_NINGSI_60
        SPoint1 st = {.key = tsList[i], .val = (double) val[i]};
#else
        SPoint1 st;
        st.key = tsList[i];
        st.val = (double) val[i];
#endif        
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    default: assert(0);
  }

  // the last interpolated time window value
  if (pCtx->end.key != INT64_MIN) {
    pInfo->dOutput  += twa_get_area(pInfo->p, pCtx->end);
    pInfo->p = pCtx->end;
  }

  pInfo->win.ekey  = pInfo->p.key;
  return notNullElems;
}

static void twa_function(SQLFunctionCtx *pCtx) {
  void *data = GET_INPUT_DATA_LIST(pCtx);

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  STwaInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  
  // skip null value
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC)? 0:(pCtx->size - 1);
  while (pCtx->hasNull && i < pCtx->size && i >= 0 && isNull((char *)data + pCtx->inputBytes * i, pCtx->inputType)) {
    i += step;
  }

  int32_t notNullElems = 0;
  if (i >= 0 && i < pCtx->size) {
    notNullElems = twa_function_impl(pCtx, i, pCtx->size);
  }

  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, pInfo, sizeof(STwaInfo));
  }
}

/*
 * To copy the input to interResBuf to avoid the input buffer space be over writen
 * by next input data. The TWA function only applies to each table, so no merge procedure
 * is required, we simply copy to the resut ot interResBuffer.
 */
void twa_function_copy(SQLFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  
  memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
  pResInfo->hasResult = ((STwaInfo *)pCtx->pInput)->hasResult;
}

void twa_function_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STwaInfo *pInfo = (STwaInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  if (pInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }

  assert(pInfo->win.ekey == pInfo->p.key && pInfo->hasResult == pResInfo->hasResult);
  if (pInfo->win.ekey == pInfo->win.skey) {
    SET_DOUBLE_VAL((double *)pCtx->pOutput, pInfo->p.val);
  } else {
    SET_DOUBLE_VAL((double *)pCtx->pOutput , pInfo->dOutput / (pInfo->win.ekey - pInfo->win.skey));
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

static void interp_function(SQLFunctionCtx *pCtx) {
  int32_t fillType = (int32_t) pCtx->param[2].i64;
  //bool ascQuery = (pCtx->order == TSDB_ORDER_ASC);

  if (pCtx->start.key == pCtx->startTs) {
    assert(pCtx->start.key != INT64_MIN);
    
    COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, &pCtx->start.val);
    
    goto interp_success_exit;    
  } else if (pCtx->end.key == pCtx->startTs && pCtx->end.key != INT64_MIN && fillType == TSDB_FILL_NEXT) {
    COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, &pCtx->end.val);
    
    goto interp_success_exit;    
  }

  switch (fillType) {
    case TSDB_FILL_NULL:
      setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      break;
      
    case TSDB_FILL_SET_VALUE:
      tVariantDump(&pCtx->param[1], pCtx->pOutput, pCtx->inputType, true);
      break;
      
    case TSDB_FILL_LINEAR:
      if (pCtx->start.key == INT64_MIN || pCtx->start.key > pCtx->startTs 
       || pCtx->end.key == INT64_MIN || pCtx->end.key < pCtx->startTs) {
        goto interp_exit;
      }

      double v1 = -1, v2 = -1;
      GET_TYPED_DATA(v1, double, pCtx->inputType, &pCtx->start.val);
      GET_TYPED_DATA(v2, double, pCtx->inputType, &pCtx->end.val);
      
      SPoint point1 = {.key = pCtx->start.key, .val = &v1};
      SPoint point2 = {.key = pCtx->end.key, .val = &v2};
      SPoint point  = {.key = pCtx->startTs, .val = pCtx->pOutput};

      int32_t srcType = pCtx->inputType;
      if (isNull((char *)&pCtx->start.val, srcType) || isNull((char *)&pCtx->end.val, srcType)) {
        setNull(pCtx->pOutput, srcType, pCtx->inputBytes);
      } else {
        bool exceedMax = false, exceedMin = false;
        taosGetLinearInterpolationVal(&point, pCtx->outputType, &point1, &point2, TSDB_DATA_TYPE_DOUBLE, &exceedMax, &exceedMin);
        if (exceedMax || exceedMin) {
          __compar_fn_t func = getComparFunc((int32_t)pCtx->inputType, 0);
          if (func(&pCtx->start.val, &pCtx->end.val) <= 0) {        
            COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, exceedMax ? &pCtx->start.val : &pCtx->end.val);
          } else {
            COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, exceedMax ? &pCtx->end.val : &pCtx->start.val);
          }
        }
      }
      break;

    case TSDB_FILL_PREV:
      if (pCtx->start.key == INT64_MIN || pCtx->start.key > pCtx->startTs) {
        goto interp_exit;
      }

      COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, &pCtx->start.val);
      break;

    case TSDB_FILL_NEXT:
      if (pCtx->end.key == INT64_MIN || pCtx->end.key < pCtx->startTs) {
        goto interp_exit;
      }
    
      COPY_TYPED_DATA(pCtx->pOutput, pCtx->inputType, &pCtx->end.val);
      break;

    case TSDB_FILL_NONE:
    default:
      goto interp_exit;
  }


interp_success_exit:  

  *(TSKEY*)pCtx->ptsOutputBuf = pCtx->startTs;

  INC_INIT_VAL(pCtx, 1);

interp_exit:

  pCtx->start.key = INT64_MIN;
  pCtx->end.key = INT64_MIN;
  pCtx->endTs = pCtx->startTs;

  return;
}

static bool ts_comp_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;  // not initialized since it has been initialized
  }

  STSCompInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->pTSBuf = tsBufCreate(false, pCtx->order);
  pInfo->pTSBuf->tsOrder = pCtx->order;
  return true;
}

static void ts_comp_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  STSBuf *     pTSbuf = ((STSCompInfo *)(GET_ROWCELL_INTERBUF(pResInfo)))->pTSBuf;
  
  const char *input = GET_INPUT_DATA_LIST(pCtx);
  
  // primary ts must be existed, so no need to check its existance
  if (pCtx->order == TSDB_ORDER_ASC) {
    tsBufAppend(pTSbuf, (int32_t)pCtx->param[0].i64, &pCtx->tag, input, pCtx->size * TSDB_KEYSIZE);
  } else {
    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *d = GET_INPUT_DATA(pCtx, i);
      tsBufAppend(pTSbuf, (int32_t)pCtx->param[0].i64, &pCtx->tag, d, (int32_t)TSDB_KEYSIZE);
    }
  }
  
  SET_VAL(pCtx, pCtx->size, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void ts_comp_finalize(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STSCompInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  STSBuf *     pTSbuf = pInfo->pTSBuf;
  
  tsBufFlush(pTSbuf);
  qDebug("total timestamp :%"PRId64, pTSbuf->numOfTotal);

  // TODO refactor transfer ownership of current file
  *(FILE **)pCtx->pOutput = pTSbuf->f;

  pResInfo->complete = true;

  // get the file size
  struct stat fStat;
  if ((fstat(fileno(pTSbuf->f), &fStat) == 0)) {
    pResInfo->numOfRes = fStat.st_size;
  }

  pTSbuf->remainOpen = true;
  tsBufDestroy(pTSbuf);

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////////////
// rate functions
static double do_calc_rate(const SRateInfo* pRateInfo, double tickPerSec) {
  if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->firstKey) ||
      (pRateInfo->firstKey >= pRateInfo->lastKey)) {
    return 0.0;
  }

  double diff = 0;
  if (pRateInfo->isIRate) {
    // If the previous value of the last is greater than the last value, only keep the last point instead of the delta
    // value between two values.
    diff = pRateInfo->lastValue;
    if (diff >= pRateInfo->firstValue) {
      diff -= pRateInfo->firstValue;
    }
  } else {
    diff = pRateInfo->correctionValue + pRateInfo->lastValue -  pRateInfo->firstValue;
    if (diff <= 0) {
      return 0;
    }
  }
  
  int64_t duration = pRateInfo->lastKey - pRateInfo->firstKey;
  if (duration == 0) {
    return 0;
  }

  return (duration > 0)? ((double)diff) / (duration/tickPerSec):0.0;
}

static bool rate_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  
  SRateInfo *pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->correctionValue = 0;
  pInfo->firstKey    = INT64_MIN;
  pInfo->lastKey     = INT64_MIN;
  pInfo->firstValue  = (double) INT64_MIN;
  pInfo->lastValue   = (double) INT64_MIN;

  pInfo->hasResult = 0;
  pInfo->isIRate = (pCtx->functionId == TSDB_FUNC_IRATE);
  return true;
}

static void rate_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  
  int32_t    notNullElems = 0;
  SRateInfo *pRateInfo = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY     *primaryKey = GET_TS_LIST(pCtx);
  
  qDebug("%p rate_function() size:%d, hasNull:%d", pCtx, pCtx->size, pCtx->hasNull);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      qDebug("%p rate_function() index of null data:%d", pCtx, i);
      continue;
    }
    
    notNullElems++;
    
    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, pData);
    
    if ((INT64_MIN == pRateInfo->firstValue) || (INT64_MIN == pRateInfo->firstKey)) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
    }
    
    if (INT64_MIN == pRateInfo->lastValue) {
      pRateInfo->lastValue = v;
    } else if (v < pRateInfo->lastValue) {
      pRateInfo->correctionValue += pRateInfo->lastValue;
    }
    
    pRateInfo->lastValue = v;
    pRateInfo->lastKey   = primaryKey[i];
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pRateInfo->hasResult = DATA_SET_FLAG;
    pResInfo->hasResult  = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SRateInfo));
  }
}

static void rate_func_copy(SQLFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
  pResInfo->hasResult = ((SRateInfo*)pCtx->pInput)->hasResult;
}

static void rate_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo  = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);

  if (pRateInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }
  
  SET_DOUBLE_VAL((double*) pCtx->pOutput, do_calc_rate(pRateInfo, (double) TSDB_TICK_PER_SECOND(pCtx->param[0].i64)));

  // cannot set the numOfIteratedElems again since it is set during previous iteration
  pResInfo->numOfRes  = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
  
  doFinalizer(pCtx);
}

static void irate_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo  *pResInfo = GET_RES_INFO(pCtx);

  int32_t    notNullElems = 0;
  SRateInfo *pRateInfo    = (SRateInfo *)GET_ROWCELL_INTERBUF(pResInfo);
  TSKEY     *primaryKey   = GET_TS_LIST(pCtx);

  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;
    
    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, pData);

    if (INT64_MIN == pRateInfo->lastKey) {
      pRateInfo->lastValue = v;
      pRateInfo->lastKey   = primaryKey[i];
      continue;
    }

    if (primaryKey[i] > pRateInfo->lastKey) {
      if ((INT64_MIN == pRateInfo->firstKey) || pRateInfo->lastKey > pRateInfo->firstKey) {
        pRateInfo->firstValue = pRateInfo->lastValue;
        pRateInfo->firstKey = pRateInfo->lastKey;
      }

      pRateInfo->lastValue = v;
      pRateInfo->lastKey   = primaryKey[i];
      
      continue;
    }
    
    if ((INT64_MIN == pRateInfo->firstKey) || primaryKey[i] > pRateInfo->firstKey) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
      break;
    }
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pRateInfo->hasResult = DATA_SET_FLAG;
    pResInfo->hasResult  = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pCtx->stableQuery) {
    memcpy(pCtx->pOutput, GET_ROWCELL_INTERBUF(pResInfo), sizeof(SRateInfo));
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

void blockInfo_func(SQLFunctionCtx* pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);

  int32_t len = *(int32_t*) pCtx->pInput;
  blockDistInfoFromBinary((char*)pCtx->pInput + sizeof(int32_t), len, pDist);
  pDist->rowSize = (uint16_t)pCtx->param[0].i64;

  memcpy(pCtx->pOutput, pCtx->pInput, sizeof(int32_t) + len);

  pResInfo->numOfRes  = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void mergeTableBlockDist(SResultRowCellInfo* pResInfo, const STableBlockDist* pSrc) {
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);
  assert(pDist != NULL && pSrc != NULL);

  pDist->numOfTables += pSrc->numOfTables;
  pDist->numOfRowsInMemTable += pSrc->numOfRowsInMemTable;
  pDist->numOfSmallBlocks += pSrc->numOfSmallBlocks;
  pDist->numOfFiles += pSrc->numOfFiles;
  pDist->totalSize += pSrc->totalSize;
  pDist->totalRows += pSrc->totalRows;

  if (pResInfo->hasResult == DATA_SET_FLAG) {
    pDist->maxRows = MAX(pDist->maxRows, pSrc->maxRows);
    pDist->minRows = MIN(pDist->minRows, pSrc->minRows);
  } else {
    pDist->maxRows = pSrc->maxRows;
    pDist->minRows = pSrc->minRows;

    int32_t maxSteps = TSDB_MAX_MAX_ROW_FBLOCK/TSDB_BLOCK_DIST_STEP_ROWS;
    if (TSDB_MAX_MAX_ROW_FBLOCK % TSDB_BLOCK_DIST_STEP_ROWS != 0) {
      ++maxSteps;
    }
    pDist->dataBlockInfos = taosArrayInit(maxSteps, sizeof(SFileBlockInfo));
    taosArraySetSize(pDist->dataBlockInfos, maxSteps);
  }

  size_t steps = taosArrayGetSize(pSrc->dataBlockInfos);
  for (int32_t i = 0; i < steps; ++i) {
    int32_t srcNumBlocks = ((SFileBlockInfo*)taosArrayGet(pSrc->dataBlockInfos, i))->numBlocksOfStep;
    SFileBlockInfo* blockInfo = (SFileBlockInfo*)taosArrayGet(pDist->dataBlockInfos, i);
    blockInfo->numBlocksOfStep += srcNumBlocks;
  }
}

void block_func_merge(SQLFunctionCtx* pCtx) {
  STableBlockDist info = {0};
  int32_t len = *(int32_t*) pCtx->pInput;
  blockDistInfoFromBinary(((char*)pCtx->pInput) + sizeof(int32_t), len, &info);
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  mergeTableBlockDist(pResInfo, &info);
  taosArrayDestroy(&info.dataBlockInfos);

  pResInfo->numOfRes = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
}

void getPercentiles(STableBlockDist *pTableBlockDist, int64_t totalBlocks, int32_t numOfPercents,
                    double* percents, int32_t* percentiles) {
  if (totalBlocks == 0) {
    for (int32_t i = 0; i < numOfPercents; ++i) {
      percentiles[i] = 0;
    }
    return;
  }

  SArray *blocksInfos = pTableBlockDist->dataBlockInfos;
  size_t  numSteps = taosArrayGetSize(blocksInfos);
  size_t  cumulativeBlocks = 0;

  int percentIndex = 0;
  for (int32_t indexStep = 0; indexStep < numSteps; ++indexStep) {
    int32_t numStepBlocks = ((SFileBlockInfo *)taosArrayGet(blocksInfos, indexStep))->numBlocksOfStep;
    if (numStepBlocks == 0) continue;
    cumulativeBlocks += numStepBlocks;

    while (percentIndex < numOfPercents) {
      double blockRank = totalBlocks * percents[percentIndex];
      if (blockRank <= cumulativeBlocks) {
        percentiles[percentIndex] = indexStep;
        ++percentIndex;
      } else {
        break;
      }
    }
  }

  for (int32_t i = 0; i < numOfPercents; ++i) {
    percentiles[i] = (percentiles[i]+1) * TSDB_BLOCK_DIST_STEP_ROWS - TSDB_BLOCK_DIST_STEP_ROWS/2;
  }
}

void generateBlockDistResult(STableBlockDist *pTableBlockDist, char* result) {
  if (pTableBlockDist == NULL) {
    return;
  }

  SArray* blockInfos = pTableBlockDist->dataBlockInfos;
  uint64_t totalRows = pTableBlockDist->totalRows;
  size_t   numSteps = taosArrayGetSize(blockInfos);
  int64_t totalBlocks = 0;
  int64_t min = -1, max = -1, avg = 0;

  for (int32_t i = 0; i < numSteps; i++) {
    SFileBlockInfo *blockInfo = taosArrayGet(blockInfos, i);
    int64_t blocks = blockInfo->numBlocksOfStep;
    totalBlocks += blocks;
  }

  avg = totalBlocks > 0 ? (int64_t)(totalRows/totalBlocks) : 0;
  min = totalBlocks > 0 ? pTableBlockDist->minRows : 0;
  max = totalBlocks > 0 ? pTableBlockDist->maxRows : 0;

  double stdDev = 0;
  if (totalBlocks > 0) {
    double variance = 0;
    for (int32_t i = 0; i < numSteps; i++) {
      SFileBlockInfo *blockInfo = taosArrayGet(blockInfos, i);
      int64_t         blocks = blockInfo->numBlocksOfStep;
      int32_t         rows = (i * TSDB_BLOCK_DIST_STEP_ROWS + TSDB_BLOCK_DIST_STEP_ROWS / 2);
      variance += blocks * (rows - avg) * (rows - avg);
    }
    variance = variance / totalBlocks;
    stdDev = sqrt(variance);
  }

  double percents[] = {0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 0.99};
  int32_t percentiles[] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
  assert(sizeof(percents)/sizeof(double) == sizeof(percentiles)/sizeof(int32_t));
  getPercentiles(pTableBlockDist, totalBlocks, sizeof(percents)/sizeof(double), percents, percentiles);

  uint64_t totalLen = pTableBlockDist->totalSize;
  int32_t rowSize = pTableBlockDist->rowSize;
  int32_t smallBlocks = pTableBlockDist->numOfSmallBlocks;
  double compRatio = (totalRows>0) ? ((double)(totalLen)/(rowSize*totalRows)) : 1;
  int sz = sprintf(result + VARSTR_HEADER_SIZE,
                   "summary: \n\t "
                   "5th=[%d], 10th=[%d], 20th=[%d], 30th=[%d], 40th=[%d], 50th=[%d]\n\t "
                   "60th=[%d], 70th=[%d], 80th=[%d], 90th=[%d], 95th=[%d], 99th=[%d]\n\t "
                   "Min=[%"PRId64"(Rows)] Max=[%"PRId64"(Rows)] Avg=[%"PRId64"(Rows)] Stddev=[%.2f] \n\t "
                   "Rows=[%"PRIu64"], Blocks=[%"PRId64"], SmallBlocks=[%d], Size=[%.3f(Kb)] Comp=[%.5g]\n\t "
                   "RowsInMem=[%d] \n\t",
                   percentiles[0], percentiles[1], percentiles[2], percentiles[3], percentiles[4], percentiles[5],
                   percentiles[6], percentiles[7], percentiles[8], percentiles[9], percentiles[10], percentiles[11],
                   min, max, avg, stdDev,
                   totalRows, totalBlocks, smallBlocks, totalLen/1024.0, compRatio,
                   pTableBlockDist->numOfRowsInMemTable);
  varDataSetLen(result, sz);
  UNUSED(sz);
}

void blockinfo_func_finalizer(SQLFunctionCtx* pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  STableBlockDist* pDist = (STableBlockDist*) GET_ROWCELL_INTERBUF(pResInfo);

  pDist->rowSize = (uint16_t)pCtx->param[0].i64;
  generateBlockDistResult(pDist, pCtx->pOutput);

  if (pDist->dataBlockInfos != NULL) {
    taosArrayDestroy(&pDist->dataBlockInfos);
    pDist->dataBlockInfos = NULL;
  }

  // cannot set the numOfIteratedElems again since it is set during previous iteration
  pResInfo->numOfRes  = 1;
  pResInfo->hasResult = DATA_SET_FLAG;

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
//cumulative_sum function

static bool csum_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SCumSumInfo* pCumSumInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pCumSumInfo->i64CumSum = 0;
  return true;
}

static void csum_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SCumSumInfo* pCumSumInfo = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t notNullElems = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size -1;

  TSKEY* pTimestamp = pCtx->ptsOutputBuf;
  TSKEY* tsList = GET_TS_LIST(pCtx);

  for (; i < pCtx->size && i >= 0; i += step) {
    char* pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      qDebug("%p csum_function() index of null data:%d", pCtx, i);
      continue;
    }

    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t v = 0;
      GET_TYPED_DATA(v, int64_t, pCtx->inputType, pData);
      pCumSumInfo->i64CumSum += v;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t v = 0;
      GET_TYPED_DATA(v, uint64_t, pCtx->inputType, pData);
      pCumSumInfo->u64CumSum += v;
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      double v = 0;
      GET_TYPED_DATA(v, double, pCtx->inputType, pData);
      pCumSumInfo->d64CumSum += v;
    }

    *pTimestamp = (tsList != NULL) ? tsList[i] : 0;
    if (IS_SIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      int64_t *retVal = (int64_t *)pCtx->pOutput;
      *retVal = (int64_t)(pCumSumInfo->i64CumSum);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(pCtx->inputType)) {
      uint64_t *retVal = (uint64_t *)pCtx->pOutput;
      *retVal = (uint64_t)(pCumSumInfo->u64CumSum);
    } else if (IS_FLOAT_TYPE(pCtx->inputType)) {
      double *retVal = (double*) pCtx->pOutput;
      SET_DOUBLE_VAL(retVal, pCumSumInfo->d64CumSum);
    }

    ++notNullElems;
    pCtx->pOutput += pCtx->outputBytes;
    pTimestamp++;
  }

  if (notNullElems == 0) {
    assert(pCtx->hasNull);
  } else {
    for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
      SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
      if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
        aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
      }
    }
    GET_RES_INFO(pCtx)->numOfRes += notNullElems;
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
}

//////////////////////////////////////////////////////////////////////////////////
// Simple Moving_average function

static bool mavg_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SMovingAvgInfo* mavgInfo = GET_ROWCELL_INTERBUF(pResInfo);
  mavgInfo->pos = 0;
  mavgInfo->kPointsMeet = false;
  mavgInfo->sum = 0;
  mavgInfo->numPointsK = (int32_t)pCtx->param[0].i64;
  mavgInfo->points = (double*)((char*)mavgInfo + sizeof(SMovingAvgInfo));
  return true;
}

static void mavg_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SMovingAvgInfo* mavgInfo = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t notNullElems = 0;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size -1;

  TSKEY* pTimestamp = pCtx->ptsOutputBuf;
  char* pOutput = pCtx->pOutput;
  TSKEY* tsList = GET_TS_LIST(pCtx);

  for (; i < pCtx->size && i >= 0; i += step) {
    char* pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      qDebug("%p mavg_function() index of null data:%d", pCtx, i);
      continue;
    }

    double v = 0;
    GET_TYPED_DATA(v, double, pCtx->inputType, pData);

    if (!mavgInfo->kPointsMeet && mavgInfo->pos < mavgInfo->numPointsK - 1) {
      mavgInfo->points[mavgInfo->pos] = v;
      mavgInfo->sum += v;
    } else {
      if (!mavgInfo->kPointsMeet && mavgInfo->pos == mavgInfo->numPointsK - 1){
        mavgInfo->sum += v;
        mavgInfo->kPointsMeet = true;
      } else {
        mavgInfo->sum = mavgInfo->sum + v - mavgInfo->points[mavgInfo->pos];
      }
      mavgInfo->points[mavgInfo->pos] = v;

      *pTimestamp = (tsList != NULL) ? tsList[i] : 0;
      SET_DOUBLE_VAL(pOutput, mavgInfo->sum / mavgInfo->numPointsK)

      ++notNullElems;
      pOutput += pCtx->outputBytes;
      pTimestamp++;
    }

    ++mavgInfo->pos;
    if (mavgInfo->pos == mavgInfo->numPointsK) {
      mavgInfo->pos = 0;
    }
  }

 {
    for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
      SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
      if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
        aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
      }
    }
    GET_RES_INFO(pCtx)->numOfRes += notNullElems;
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
}

//////////////////////////////////////////////////////////////////////////////////
// Sample function with reservoir sampling algorithm

static void assignResultSample(SQLFunctionCtx *pCtx, SSampleFuncInfo *pInfo, int32_t index, int64_t ts, void *pData, uint16_t type, int16_t bytes, char *inputTags) {
  assignVal(pInfo->values + index*bytes, pData, bytes, type);
  *(pInfo->timeStamps + index) = ts;

  SExtTagsInfo* pTagInfo = &pCtx->tagInfo;
  int32_t posTag = 0;
  char* tags = pInfo->taglists + index*pTagInfo->tagsLen;
  if (pCtx->currentStage == MERGE_STAGE) {
    assert(inputTags != NULL);
    memcpy(tags, inputTags, (size_t)pTagInfo->tagsLen);
  } else {
    assert(inputTags == NULL);
    for (int32_t i = 0; i < pTagInfo->numOfTagCols; ++i) {
      SQLFunctionCtx* ctx = pTagInfo->pTagCtxList[i];
      if (ctx->functionId == TSDB_FUNC_TS_DUMMY) {
        ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
        ctx->tag.i64 = ts;
      }

      tVariantDump(&ctx->tag, tags + posTag, ctx->tag.nType, true);
      posTag += pTagInfo->pTagCtxList[i]->outputBytes;
    }
  }
}

static void do_reservoir_sample(SQLFunctionCtx *pCtx, SSampleFuncInfo *pInfo, int32_t samplesK, int64_t ts, void *pData,  uint16_t type, int16_t bytes) {
  pInfo->totalPoints++;
  if (pInfo->numSampled < samplesK) {
    assignResultSample(pCtx, pInfo, pInfo->numSampled, ts, pData, type, bytes, NULL);
    pInfo->numSampled++;
  } else {
    int32_t j = rand() % (pInfo->totalPoints);
    if (j < samplesK) {
      assignResultSample(pCtx, pInfo, j, ts, pData, type, bytes, NULL);
    }
  }
}

static void copySampleFuncRes(SQLFunctionCtx *pCtx, int32_t type) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SSampleFuncInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);

  TSKEY* pTimestamp = pCtx->ptsOutputBuf;
  char* pOutput = pCtx->pOutput;
  for (int32_t i = 0; i < pRes->numSampled; ++i) {
    assignVal(pOutput, pRes->values + i*pRes->colBytes, pRes->colBytes, type);
    *pTimestamp = *(pRes->timeStamps + i);
    pOutput += pCtx->outputBytes;
    pTimestamp++;
  }

  if (pCtx->tagInfo.numOfTagCols == 0) {
    return ;
  }

  char **tagOutputs = calloc(pCtx->tagInfo.numOfTagCols, POINTER_BYTES);
  for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
    tagOutputs[i] = pCtx->tagInfo.pTagCtxList[i]->pOutput;
  }

  for (int32_t i = 0; i < pRes->numSampled; ++i) {
    int16_t tagOffset = 0;
    for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
      memcpy(tagOutputs[j], pRes->taglists + i*pCtx->tagInfo.tagsLen + tagOffset, (size_t)pCtx->tagInfo.pTagCtxList[j]->outputBytes);
      tagOffset += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
      tagOutputs[j] += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
    }
  }

  tfree(tagOutputs);
}

static bool sample_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  srand(taosSafeRand());

  SSampleFuncInfo *pRes = getOutputInfo(pCtx);
  pRes->totalPoints = 0;
  pRes->numSampled = 0;
  pRes->values = ((char*)pRes + sizeof(SSampleFuncInfo));
  pRes->colBytes = (pCtx->currentStage != MERGE_STAGE) ? pCtx->inputBytes : pCtx->outputBytes;
  pRes->timeStamps = (int64_t *)((char *)pRes->values + pRes->colBytes * pCtx->param[0].i64);
  pRes->taglists = (char*)pRes->timeStamps + sizeof(int64_t) * pCtx->param[0].i64;
  return true;
}

static void sample_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SSampleFuncInfo *pRes = getOutputInfo(pCtx);

  if (pRes->values !=  ((char*)pRes + sizeof(SSampleFuncInfo))) {
    pRes->values =  ((char*)pRes + sizeof(SSampleFuncInfo));
    pRes->timeStamps = (int64_t*)((char*)pRes->values + pRes->colBytes * pCtx->param[0].i64);
    pRes->taglists = (char*)pRes->timeStamps + sizeof(int64_t) * pCtx->param[0].i64;
  }

  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }

    notNullElems++;

    TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
    do_reservoir_sample(pCtx, pRes, (int32_t)pCtx->param[0].i64, ts, data, pCtx->inputType, pRes->colBytes);
  }

  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }

  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void sample_func_merge(SQLFunctionCtx *pCtx) {
  SSampleFuncInfo* pInput = (SSampleFuncInfo*)GET_INPUT_DATA_LIST(pCtx);
  pInput->values = ((char*)pInput + sizeof(SSampleFuncInfo));
  pInput->timeStamps = (int64_t*)((char*)pInput->values + pInput->colBytes * pCtx->param[0].i64);
  pInput->taglists = (char*)pInput->timeStamps + sizeof(int64_t)*pCtx->param[0].i64;

  SSampleFuncInfo *pOutput = getOutputInfo(pCtx);
  pOutput->totalPoints = pInput->totalPoints;
  pOutput->numSampled = pInput->numSampled;
  for (int32_t i = 0; i < pInput->numSampled; ++i) {
    assignResultSample(pCtx, pOutput, i, pInput->timeStamps[i],
                       pInput->values + i * pInput->colBytes, pCtx->outputType, pInput->colBytes,
                       pInput->taglists + i*pCtx->tagInfo.tagsLen);
  }

  SET_VAL(pCtx, pInput->numSampled, pOutput->numSampled);
  if (pOutput->numSampled > 0) {
    SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void sample_func_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SSampleFuncInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);

  if (pRes->numSampled == 0) {  // no result
    assert(pResInfo->hasResult != DATA_SET_FLAG);
  }

  pResInfo->numOfRes = pRes->numSampled;
  GET_TRUE_DATA_TYPE();
  copySampleFuncRes(pCtx, type);

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
// elapsed function

static bool elapsedSetup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SElapsedInfo *pInfo = getOutputInfo(pCtx);
  pInfo->min = MAX_TS_KEY;
  pInfo->max = 0;
  pInfo->hasResult = 0;

  return true;
}

static void elapsedFunction(SQLFunctionCtx *pCtx) {
  SElapsedInfo *pInfo = getOutputInfo(pCtx);
  if (pCtx->preAggVals.isSet) {
    if (pInfo->min == MAX_TS_KEY) {
      pInfo->min = pCtx->preAggVals.statis.min;
      pInfo->max = pCtx->preAggVals.statis.max;
    } else {
      if (pCtx->order == TSDB_ORDER_ASC) {
        pInfo->max = pCtx->preAggVals.statis.max;
      } else {
        pInfo->min = pCtx->preAggVals.statis.min;
      }
    }
  } else {
    // 0 == pCtx->size mean this is end interpolation.
    if (0 == pCtx->size) {
      if (pCtx->order == TSDB_ORDER_DESC) {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->min = pCtx->end.key;
        }
      } else {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->max = pCtx->end.key + 1;
        }
      }
      goto elapsedOver;
    }

    int64_t *ptsList = (int64_t *)GET_INPUT_DATA_LIST(pCtx);
    // pCtx->start.key == INT64_MIN mean this is first window or there is actual start point of current window.
    // pCtx->end.key == INT64_MIN mean current window does not end in current data block or there is actual end point of current window.
    if (pCtx->order == TSDB_ORDER_DESC) {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->max = (pInfo->max < ptsList[pCtx->size - 1]) ? ptsList[pCtx->size - 1] : pInfo->max;
      } else {
        pInfo->max = pCtx->start.key + 1;
      }

      if (pCtx->end.key != INT64_MIN) {
        pInfo->min = pCtx->end.key;
      } else {
        pInfo->min = ptsList[0];
      }
    } else {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->min = (pInfo->min > ptsList[0]) ? ptsList[0] : pInfo->min;
      } else {
        pInfo->min = pCtx->start.key;
      }

      if (pCtx->end.key != INT64_MIN) {
        pInfo->max = pCtx->end.key + 1;
      } else {
        pInfo->max = ptsList[pCtx->size - 1];
      }
    }
  }

elapsedOver:
  SET_VAL(pCtx, pCtx->size, 1);
  
  if (pCtx->size > 0) {
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
    pInfo->hasResult = DATA_SET_FLAG;
  }
}

static void elapsedMerge(SQLFunctionCtx *pCtx) {
  SElapsedInfo *pInfo = getOutputInfo(pCtx);
  memcpy(pInfo, pCtx->pInput, (size_t)pCtx->inputBytes);
  GET_RES_INFO(pCtx)->hasResult = pInfo->hasResult;
}

static void elapsedFinalizer(SQLFunctionCtx *pCtx) {
  if (GET_RES_INFO(pCtx)->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
    return;
  }

  SElapsedInfo *pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double result = (double)pInfo->max - (double)pInfo->min;
  *(double *)pCtx->pOutput = result >= 0 ? result : -result;
  if (pCtx->numOfParams > 0 && pCtx->param[0].i64 > 0) {
    *(double *)pCtx->pOutput = *(double *)pCtx->pOutput / pCtx->param[0].i64;
  }
  GET_RES_INFO(pCtx)->numOfRes = 1;

  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static bool histogram_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  SHistogramFuncInfo *pRes = getOutputInfo(pCtx);
  if (!pRes) {
    return false;
  }

  int32_t numOfBins = (int32_t)pCtx->param[0].i64;
  double* listBin = (double*) pCtx->param[1].pz;
  int32_t normalized = (int32_t)pCtx->param[2].i64;
  pRes->numOfBins = numOfBins;
  pRes->normalized = normalized;
  pRes->orderedBins = (SHistogramFuncBin*)((char*)pRes + sizeof(SHistogramFuncInfo));
  for (int32_t i = 0; i < numOfBins; ++i) {
    double lower = listBin[i] < listBin[i + 1] ? listBin[i] : listBin[i + 1];
    double upper = listBin[i + 1] > listBin[i] ? listBin[i + 1] : listBin[i];
    pRes->orderedBins[i].lower = lower;
    pRes->orderedBins[i].upper = upper;
    pRes->orderedBins[i].count = 0;
  }
  return true;
}

static void histogram_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo* pResInfo = GET_RES_INFO(pCtx);

  SHistogramFuncInfo* pRes = getOutputInfo(pCtx);

  if (pRes->orderedBins != (SHistogramFuncBin*)((char*)pRes + sizeof(SHistogramFuncInfo))) {
    pRes->orderedBins = (SHistogramFuncBin*)((char*)pRes + sizeof(SHistogramFuncInfo));
  }

  int32_t notNullElems = 0;
  int32_t totalElems = 0;
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }

    notNullElems++;
    double v;
    GET_TYPED_DATA(v, double, pCtx->inputType, data);

    for (int32_t b = 0; b < pRes->numOfBins; ++b) {
      if (v > pRes->orderedBins[b].lower && v <= pRes->orderedBins[b].upper) {
        pRes->orderedBins[b].count++;
        totalElems++;
        break;
      }
    }
  }

  if (pRes->normalized) {
    for (int32_t b = 0; b < pRes->numOfBins; ++b) {
      if (totalElems != 0) {
        pRes->orderedBins[b].count = pRes->orderedBins[b].count / (double)totalElems;
      } else {
        pRes->orderedBins[b].count = 0;
      }
    }
  }

  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void histogram_func_merge(SQLFunctionCtx *pCtx) {
  SHistogramFuncInfo* pInput = (SHistogramFuncInfo*) GET_INPUT_DATA_LIST(pCtx);
  pInput->orderedBins = (SHistogramFuncBin*)((char*)pInput + sizeof(SHistogramFuncInfo));

  SHistogramFuncInfo* pRes = getOutputInfo(pCtx);
  for (int32_t i = 0; i < pInput->numOfBins; ++i) {
    pRes->orderedBins[i].count += pInput->orderedBins[i].count;
  }
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->numOfRes = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void histogram_func_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SHistogramFuncInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);

  if (!pRes) {
    return;
  }

  for (int32_t i = 0; i < pRes->numOfBins; ++i) {
    int sz;
    if (!pRes->normalized) {
      int64_t count = (int64_t)pRes->orderedBins[i].count;
      sz = sprintf(pCtx->pOutput + VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%"PRId64"}",
                   pRes->orderedBins[i].lower, pRes->orderedBins[i].upper, count);
    } else {
      sz = sprintf(pCtx->pOutput + VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}",
                   pRes->orderedBins[i].lower, pRes->orderedBins[i].upper, pRes->orderedBins[i].count);
    }
    varDataSetLen(pCtx->pOutput, sz);
    pCtx->pOutput += pCtx->outputBytes;
  }

  pResInfo->numOfRes = pRes->numOfBins;
  pResInfo->hasResult = DATA_SET_FLAG;

  doFinalizer(pCtx);
}

// unique&tail copy
static void copyRes(SQLFunctionCtx *pCtx, void *data, int32_t bytes) {
  size_t size = sizeof(int64_t) + bytes + pCtx->tagInfo.tagsLen;
  int32_t len = (int32_t)(GET_RES_INFO(pCtx)->numOfRes);

  char *tsOutput = pCtx->ptsOutputBuf;
  char *output = pCtx->pOutput;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->param[3].i64);
  char *tvp = (char*)data + (size * ((pCtx->param[3].i64 == TSDB_ORDER_ASC) ? 0 : len -1));
  for (int32_t i = 0; i < len; ++i) {
    memcpy(tsOutput, tvp, sizeof(int64_t));
    memcpy(output, tvp + sizeof(int64_t), bytes);
    tvp += (step * size);
    tsOutput += sizeof(int64_t);
    output += bytes;
  }

  // set the corresponding tag data for each record
  // todo check malloc failure
  if (pCtx->tagInfo.numOfTagCols == 0) {
    return ;
  }

  char **pData = calloc(pCtx->tagInfo.numOfTagCols, POINTER_BYTES);
  for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
    pData[i] = pCtx->tagInfo.pTagCtxList[i]->pOutput;
  }

  tvp = (char*)data + (size * ((pCtx->param[3].i64 == TSDB_ORDER_ASC) ? 0 : len -1));
  for (int32_t i = 0; i < len; ++i) {
    int32_t offset = (int32_t)sizeof(int64_t) + bytes;
    for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
      memcpy(pData[j], tvp + offset, (size_t)pCtx->tagInfo.pTagCtxList[j]->outputBytes);
      offset += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
      pData[j] += pCtx->tagInfo.pTagCtxList[j]->outputBytes;
    }
    tvp += (step * size);
  }

  tfree(pData);
}

static bool unique_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  if(*pCtx->pUniqueSet != NULL){
    taosHashClear(*pCtx->pUniqueSet);
  }else{
    *pCtx->pUniqueSet = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }

  return true;
}

static void do_unique_function(SQLFunctionCtx *pCtx, SUniqueFuncInfo *pInfo, TSKEY timestamp, char *pData, char *tag, int32_t bytes, int16_t type){
  int32_t hashKeyBytes = bytes;
  if(IS_VAR_DATA_TYPE(type)){     // for var data, we can not use bytes, because there are dirty data in the back of var data
    hashKeyBytes = varDataTLen(pData);
  }
  UniqueUnit **unique = taosHashGet(*pCtx->pUniqueSet, pData, hashKeyBytes);
  if (unique == NULL) {
    size_t size = sizeof(UniqueUnit) + bytes + pCtx->tagInfo.tagsLen;
    char *tmp = pInfo->res + pInfo->num * size;
    ((UniqueUnit*)tmp)->timestamp = timestamp;
    char *data = tmp + sizeof(UniqueUnit);
    char *tags = tmp + sizeof(UniqueUnit) + bytes;
    memcpy(data, pData, bytes);

    if (pCtx->currentStage == MERGE_STAGE && tag != NULL) {
      memcpy(tags, tag, (size_t)pCtx->tagInfo.tagsLen);
    }else{
      int32_t offset = 0;
      for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
        SQLFunctionCtx *tagCtx = pCtx->tagInfo.pTagCtxList[j];
        if (tagCtx->functionId == TSDB_FUNC_TS_DUMMY) {
          tagCtx->tag.nType = TSDB_DATA_TYPE_BIGINT;
          tagCtx->tag.i64 = timestamp;
        }

        tVariantDump(&tagCtx->tag, tagCtx->pOutput, tagCtx->tag.nType, true);
        memcpy(tags + offset, tagCtx->pOutput, tagCtx->outputBytes);
        offset += tagCtx->outputBytes;
      }
    }

    taosHashPut(*pCtx->pUniqueSet, pData, hashKeyBytes, &tmp, sizeof(UniqueUnit*));
    pInfo->num++;
  }else if((*unique)->timestamp > timestamp){
    (*unique)->timestamp = timestamp;
  }
}

static void unique_function(SQLFunctionCtx *pCtx) {
  SUniqueFuncInfo *pInfo = getOutputInfo(pCtx);

  for (int32_t i = 0; i < pCtx->size; i++) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    TSKEY k = 0;
    if (pCtx->ptsList != NULL) {
      k = GET_TS_DATA(pCtx, i);
    }
    do_unique_function(pCtx, pInfo, k, pData, NULL, pCtx->inputBytes, pCtx->inputType);

    if (sizeof(SUniqueFuncInfo) + pInfo->num * (sizeof(UniqueUnit) + pCtx->inputBytes + pCtx->tagInfo.tagsLen) >= MAX_UNIQUE_RESULT_SIZE
        || (pInfo->num > pCtx->param[0].i64)){
      GET_RES_INFO(pCtx)->numOfRes = -1;    // mark out of memory
      return;
    }
  }

  GET_RES_INFO(pCtx)->numOfRes = 1;
}

static void unique_function_merge(SQLFunctionCtx *pCtx) {
  SUniqueFuncInfo *pInput = (SUniqueFuncInfo *)GET_INPUT_DATA_LIST(pCtx);
  SUniqueFuncInfo *pOutput = getOutputInfo(pCtx);
  size_t size = sizeof(UniqueUnit) + pCtx->outputBytes + pCtx->tagInfo.tagsLen;
  for (int32_t i = 0; i < pInput->num; ++i) {
    char *tmp = pInput->res + i* size;
    TSKEY timestamp = ((UniqueUnit*)tmp)->timestamp;
    char *data = tmp + sizeof(UniqueUnit);
    char *tags = tmp + sizeof(UniqueUnit) + pCtx->outputBytes;
    do_unique_function(pCtx, pOutput, timestamp, data, tags, pCtx->outputBytes, pCtx->outputType);

    if (sizeof(SUniqueFuncInfo) + pOutput->num * (sizeof(UniqueUnit) + pCtx->outputBytes + pCtx->tagInfo.tagsLen) >= MAX_UNIQUE_RESULT_SIZE
        || (pOutput->num > pCtx->param[0].i64)){
      GET_RES_INFO(pCtx)->numOfRes = -1;    // mark out of memory
      return;
    }
  }

//  GET_RES_INFO(pCtx)->numOfRes = pOutput->num;
}

typedef struct{
  int32_t dataOffset;
  __compar_fn_t comparFn;
} SortSupporter;

static int32_t sortCompareFn(const void *p1, const void *p2, const void *param) {
  SortSupporter *support = (SortSupporter *)param;
  return support->comparFn((const char*)p1 + support->dataOffset, (const char*)p2 + support->dataOffset);
}

static void unique_func_finalizer(SQLFunctionCtx *pCtx) {
  SUniqueFuncInfo *pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  GET_RES_INFO(pCtx)->numOfRes = pInfo->num;
  int32_t bytes = 0;
  int32_t type = 0;
  if (pCtx->currentStage == MERGE_STAGE) {
    bytes = pCtx->outputBytes;
    type = pCtx->outputType;
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  } else {
    bytes = pCtx->inputBytes;
    type = pCtx->inputType;
  }
  SortSupporter support = {0};
  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[2].i64 == PRIMARYKEY_TIMESTAMP_COL_INDEX || pCtx->param[2].i64 == TSDB_RES_COL_ID) {
    support.dataOffset = 0;
    support.comparFn = compareInt64Val;
  } else{
    support.dataOffset = sizeof(int64_t);
    support.comparFn = getComparFunc(type, 0);
  }

  size_t size = sizeof(int64_t) + bytes + pCtx->tagInfo.tagsLen;
  taosqsort(pInfo->res, (size_t)GET_RES_INFO(pCtx)->numOfRes, size, &support, sortCompareFn);
  copyRes(pCtx, pInfo->res, bytes);
  doFinalizer(pCtx);
}

static bool mode_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }
  if(*pCtx->pModeSet != NULL){
    taosHashClear(*pCtx->pModeSet);
  }else{
    *pCtx->pModeSet = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }

  return true;
}

static void do_mode_function(SQLFunctionCtx *pCtx, SModeFuncInfo *pInfo, char *pData, int64_t count, int32_t bytes, int16_t type){
  int32_t hashKeyBytes = bytes;
  if(IS_VAR_DATA_TYPE(type)){     // for var data, we can not use bytes, because there are dirty data in the back of var data
    hashKeyBytes = varDataTLen(pData);
  }
  ModeUnit **mode = taosHashGet(*pCtx->pModeSet, pData, hashKeyBytes);
  if (mode == NULL) {
    size_t size = sizeof(ModeUnit) + bytes;
    char *tmp = pInfo->res + pInfo->num * size;
    ((ModeUnit*)tmp)->count = count;
    char *data = tmp + sizeof(ModeUnit);
    memcpy(data, pData, bytes);

    taosHashPut(*pCtx->pModeSet, pData, hashKeyBytes, &tmp, sizeof(ModeUnit*));
    pInfo->num++;
  }else{
    (*mode)->count += count;
  }
}

static void mode_function(SQLFunctionCtx *pCtx) {
  SModeFuncInfo *pInfo = getOutputInfo(pCtx);

  for (int32_t i = 0; i < pCtx->size; i++) {
    char *pData = GET_INPUT_DATA(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      continue;
    }

    do_mode_function(pCtx, pInfo, pData, 1, pCtx->inputBytes, pCtx->inputType);

    if (sizeof(SModeFuncInfo) + pInfo->num * (sizeof(ModeUnit) + pCtx->inputBytes) >= MAX_MODE_INNER_RESULT_SIZE){
      GET_RES_INFO(pCtx)->numOfRes = -1;    // mark out of memory
      return;
    }
  }
  GET_RES_INFO(pCtx)->numOfRes = 1;
}

static void mode_function_merge(SQLFunctionCtx *pCtx) {
  SModeFuncInfo *pInput = (SModeFuncInfo *)GET_INPUT_DATA_LIST(pCtx);
  SModeFuncInfo *pOutput = getOutputInfo(pCtx);
  size_t size = sizeof(ModeUnit) + pCtx->outputBytes;
  for (int32_t i = 0; i < pInput->num; ++i) {
    char *tmp = pInput->res + i* size;
    char *data = tmp + sizeof(ModeUnit);
    do_mode_function(pCtx, pOutput, data, ((ModeUnit*)tmp)->count, pCtx->outputBytes, pCtx->outputType);

    if (sizeof(SModeFuncInfo) + pOutput->num * (sizeof(ModeUnit) + pCtx->outputBytes) >= MAX_MODE_INNER_RESULT_SIZE){
      GET_RES_INFO(pCtx)->numOfRes = -1;    // mark out of memory
      return;
    }
  }
}

static void mode_func_finalizer(SQLFunctionCtx *pCtx) {
  int32_t bytes = 0;
  int32_t type = 0;
  if (pCtx->currentStage == MERGE_STAGE) {
    bytes = pCtx->outputBytes;
    type = pCtx->outputType;
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  } else {
    bytes = pCtx->inputBytes;
    type = pCtx->inputType;
  }

  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SModeFuncInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);

  size_t size = sizeof(ModeUnit) + bytes;

  char *tvp = pRes->res;
  char *result = NULL;
  int64_t maxCount = 0;
  for (int32_t i = 0; i < pRes->num; ++i) {
    int64_t count = ((ModeUnit*)tvp)->count;
    if (count > maxCount){
      maxCount = count;
      result = tvp;
    }else if(count == maxCount){
      result = NULL;
    }
    tvp += size;
  }

  if (result){
    memcpy(pCtx->pOutput, result + sizeof(ModeUnit), bytes);
  }else{
    setNull(pCtx->pOutput, type, 0);
  }
  pResInfo->numOfRes = 1;
  doFinalizer(pCtx);
}

static void buildTailStruct(STailInfo *pTailInfo, SQLFunctionCtx *pCtx) {
  char *tmp = (char *)pTailInfo + sizeof(STailInfo);
  pTailInfo->res = (TailUnit**) tmp;
  tmp += POINTER_BYTES * pCtx->param[0].i64;

  int32_t bytes = 0;
  if (pCtx->currentStage == MERGE_STAGE) {
    bytes = pCtx->outputBytes;
  } else {
    bytes = pCtx->inputBytes;
  }
  size_t size = sizeof(TailUnit) + bytes + pCtx->tagInfo.tagsLen;

  for (int32_t i = 0; i < pCtx->param[0].i64; ++i) {
    pTailInfo->res[i] = (TailUnit*) tmp;
    tmp += size;
  }
}

static void valueTailAssign(TailUnit *dst, int32_t bytes, const char *val, int64_t tsKey,
                            SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  dst->timestamp = tsKey;
  memcpy(dst->data, val, bytes);

  if (stage == MERGE_STAGE) {
    memcpy(dst->data + bytes, pTags, (size_t)pTagInfo->tagsLen);
  } else {  // the tags are dumped from the ctx tag fields
    int32_t size = 0;
    for (int32_t i = 0; i < pTagInfo->numOfTagCols; ++i) {
      SQLFunctionCtx* ctx = pTagInfo->pTagCtxList[i];
      if (ctx->functionId == TSDB_FUNC_TS_DUMMY) {
        ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;
        ctx->tag.i64 = tsKey;
      }

      tVariantDump(&ctx->tag, ctx->pOutput, ctx->tag.nType, true);
      memcpy(dst->data + bytes + size, ctx->pOutput, ctx->outputBytes);
      size += ctx->outputBytes;
    }
  }
}

static int32_t tailComparFn(const void *p1, const void *p2, const void *param) {
  TailUnit  *d1 = *(TailUnit **) p1;
  TailUnit  *d2 = *(TailUnit **) p2;
  return compareInt64Val(d1, d2);
}

static void tailSwapFn(void *dst, void *src, const void *param)
{
  TailUnit  **vdst = (TailUnit **) dst;
  TailUnit  **vsrc = (TailUnit **) src;

  TailUnit  *tmp = *vdst;
  *vdst = *vsrc;
  *vsrc = tmp;
}

static void do_tail_function_add(STailInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, int32_t bytes,
                                SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  TailUnit **pList = pInfo->res;

  if (pInfo->num < maxLen) {
    valueTailAssign(pList[pInfo->num], bytes, pData, ts, pTagInfo, pTags, stage);

    taosheapsort((void *) pList, sizeof(TailUnit **), pInfo->num + 1, NULL, tailComparFn, NULL, tailSwapFn, 0);

    pInfo->num++;
  } else if(pList[0]->timestamp < ts) {
    valueTailAssign(pList[0], bytes, pData, ts, pTagInfo, pTags, stage);
    taosheapadjust((void *) pList, sizeof(TailUnit **), 0, maxLen - 1, NULL, tailComparFn, NULL, tailSwapFn, 0);
  }
}

static bool tail_function_setup(SQLFunctionCtx *pCtx, SResultRowCellInfo* pResInfo) {
  if (!function_setup(pCtx, pResInfo)) {
    return false;
  }

  STailInfo *pInfo = getOutputInfo(pCtx);
  buildTailStruct(pInfo, pCtx);
  return true;
}

static void tail_function(SQLFunctionCtx *pCtx) {
  STailInfo *pRes = getOutputInfo(pCtx);

//  if (pCtx->stableQuery){
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_DATA(pCtx, i);

      TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
      do_tail_function_add(pRes, (int32_t)pCtx->param[0].i64, data, ts,
                           pCtx->inputBytes, &pCtx->tagInfo, NULL, pCtx->currentStage);
    }
//  }else{
//    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
//      if (pRes->offset++ < (int32_t)pCtx->param[1].i64){
//        continue;
//      }
//      if (pRes->num >= (int32_t)(pCtx->param[0].i64 - pCtx->param[1].i64)){    // query complete
//        pCtx->resultInfo->complete = true;
//        for (int32_t j = 0; j < pCtx->tagInfo.numOfTagCols; ++j) {
//          SQLFunctionCtx *ctx = pCtx->tagInfo.pTagCtxList[j];
//          ctx->resultInfo->complete = true;
//        }
//        break;
//      }
//      char *data = GET_INPUT_DATA(pCtx, i);
//
//      TSKEY ts = (pCtx->ptsList != NULL)? GET_TS_DATA(pCtx, i):0;
//
//      valueTailAssign(pRes->res[pRes->num], pCtx->inputBytes, data, ts, &pCtx->tagInfo, NULL, pCtx->currentStage);
//
//      pRes->num++;
//    }
//  }

  // treat the result as only one result
  GET_RES_INFO(pCtx)->numOfRes = 1;
}

static void tail_func_merge(SQLFunctionCtx *pCtx) {
  STailInfo *pInput = (STailInfo *)GET_INPUT_DATA_LIST(pCtx);

  // construct the input data struct from binary data
  buildTailStruct(pInput, pCtx);

  STailInfo *pOutput = getOutputInfo(pCtx);

  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    do_tail_function_add(pOutput, (int32_t)pCtx->param[0].i64, pInput->res[i]->data, pInput->res[i]->timestamp,
                        pCtx->outputBytes, &pCtx->tagInfo, pInput->res[i]->data + pCtx->outputBytes, pCtx->currentStage);
  }

  GET_RES_INFO(pCtx)->numOfRes = pOutput->num;
}

static void tail_func_finalizer(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);

  // data in temporary list is less than the required number of results, not enough qualified number of results
  STailInfo *pRes = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t bytes = 0;
  int32_t type = 0;
  if (pCtx->currentStage == MERGE_STAGE) {
    bytes = pCtx->outputBytes;
    type = pCtx->outputType;
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  } else {
    bytes = pCtx->inputBytes;
    type = pCtx->inputType;
  }

//  if(pCtx->stableQuery){
    GET_RES_INFO(pCtx)->numOfRes = pRes->num - (int32_t)pCtx->param[1].i64;
//  }else{
//    GET_RES_INFO(pCtx)->numOfRes = pRes->num;
//  }
  if (GET_RES_INFO(pCtx)->numOfRes <= 0) {
    doFinalizer(pCtx);
    return;
  }

  taosqsort(pRes->res, pRes->num, POINTER_BYTES, NULL, tailComparFn);

  size_t size = sizeof(int64_t) + bytes + pCtx->tagInfo.tagsLen;
  void *data = calloc(size, GET_RES_INFO(pCtx)->numOfRes);
  if(!data){
    qError("calloc error in tail_func_finalizer: size:%d, num:%d", (int32_t)size, GET_RES_INFO(pCtx)->numOfRes);
    doFinalizer(pCtx);
    return;
  }
  for(int32_t i = 0; i < GET_RES_INFO(pCtx)->numOfRes; i++){
    memcpy((char*)data + i * size, pRes->res[i], size);
  }

  SortSupporter support = {0};
  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[2].i64 != PRIMARYKEY_TIMESTAMP_COL_INDEX && pCtx->param[2].i64 != TSDB_RES_COL_ID) {
    support.dataOffset = sizeof(int64_t);
    support.comparFn = getComparFunc(type, 0);
    taosqsort(data, (size_t)GET_RES_INFO(pCtx)->numOfRes, size, &support, sortCompareFn);
  }

  copyRes(pCtx, data, bytes);
  free(data);
  doFinalizer(pCtx);
}


static void state_count_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SStateInfo *pStateInfo = GET_ROWCELL_INTERBUF(pResInfo);

  char *data = GET_INPUT_DATA_LIST(pCtx);
  int64_t *pOutput = (int64_t *)pCtx->pOutput;

  for (int32_t i = 0; i < pCtx->size;  i++,pOutput++,data += pCtx->inputBytes) {
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      setNull(pOutput, TSDB_DATA_TYPE_BIGINT, 0);
      continue;
    }
    if (isStateOperTrue(data, pCtx->inputType, &pCtx->param[0], &pCtx->param[1])){
      *pOutput = ++pStateInfo->countPrev;
    }else{
      *pOutput = -1;
      pStateInfo->countPrev = 0;
    }
  }

  for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
    SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
    if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
      aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
    }
  }
  pResInfo->numOfRes += pCtx->size;
}

static void state_duration_function(SQLFunctionCtx *pCtx) {
  SResultRowCellInfo *pResInfo = GET_RES_INFO(pCtx);
  SStateInfo *pStateInfo = GET_ROWCELL_INTERBUF(pResInfo);

  char *data = GET_INPUT_DATA_LIST(pCtx);
  TSKEY* tsList = GET_TS_LIST(pCtx);
  int64_t *pOutput = (int64_t *)pCtx->pOutput;

  for (int32_t i = 0; i < pCtx->size; i++,pOutput++,data += pCtx->inputBytes) {
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      setNull(pOutput, TSDB_DATA_TYPE_BIGINT, 0);
      continue;
    }
    if (isStateOperTrue(data, pCtx->inputType, &pCtx->param[0], &pCtx->param[1])){
      if (pStateInfo->durationStart == 0) {
        *pOutput = 0;
        pStateInfo->durationStart = tsList[i];
      } else {
        *pOutput = (tsList[i] - pStateInfo->durationStart)/pCtx->param[2].i64;
      }
    } else{
      *pOutput = -1;
      pStateInfo->durationStart = 0;
    }
  }

  for (int t = 0; t < pCtx->tagInfo.numOfTagCols; ++t) {
    SQLFunctionCtx* tagCtx = pCtx->tagInfo.pTagCtxList[t];
    if (tagCtx->functionId == TSDB_FUNC_TAG_DUMMY) {
      aAggs[TSDB_FUNC_TAGPRJ].xFunction(tagCtx);
    }
  }
  pResInfo->numOfRes += pCtx->size;
}

int16_t getTimeWindowFunctionID(int16_t colIndex) {
  switch (colIndex) {
    case TSDB_TSWIN_START_COLUMN_INDEX: {
      return TSDB_FUNC_WSTART;
    }
    case TSDB_TSWIN_STOP_COLUMN_INDEX: {
      return TSDB_FUNC_WSTOP;
    }
    case TSDB_TSWIN_DURATION_COLUMN_INDEX: {
      return TSDB_FUNC_WDURATION;
    }
    case TSDB_QUERY_START_COLUMN_INDEX: {
      return TSDB_FUNC_QSTART;
    }
    case TSDB_QUERY_STOP_COLUMN_INDEX: {
      return TSDB_FUNC_QSTOP;
    }
    case TSDB_QUERY_DURATION_COLUMN_INDEX: {
      return TSDB_FUNC_QDURATION;
    }
    default:
      return TSDB_FUNC_INVALID_ID;
  }
}

static void window_start_function(SQLFunctionCtx *pCtx) {
  if (pCtx->functionId == TSDB_FUNC_WSTART) {
    SET_VAL(pCtx, pCtx->size, 1);
    *(int64_t *)(pCtx->pOutput) = pCtx->startTs;
  } else { //TSDB_FUNC_QSTART
    int32_t size = MIN(pCtx->size, pCtx->allocRows); //size cannot exceeds allocated rows
    SET_VAL(pCtx, pCtx->size, size);
    //INC_INIT_VAL(pCtx, size);
    char *output = pCtx->pOutput;
    for (int32_t i = 0; i < size; ++i) {
      if (pCtx->qWindow.skey == INT64_MIN) {
        *(TKEY *)output = TSDB_DATA_TIMESTAMP_NULL;
      } else {
        memcpy(output, &pCtx->qWindow.skey, pCtx->outputBytes);
      }
      output += pCtx->outputBytes;
    }
  }
}

static void window_stop_function(SQLFunctionCtx *pCtx) {
  if (pCtx->functionId == TSDB_FUNC_WSTOP) {
    SET_VAL(pCtx, pCtx->size, 1);
    *(int64_t *)(pCtx->pOutput) = pCtx->endTs;
  } else { //TSDB_FUNC_QSTOP
    int32_t size = MIN(pCtx->size, pCtx->allocRows); //size cannot exceeds allocated rows
    SET_VAL(pCtx, pCtx->size, size);
    //INC_INIT_VAL(pCtx, size);
    char *output = pCtx->pOutput;
    for (int32_t i = 0; i < size; ++i) {
      if (pCtx->qWindow.ekey == INT64_MAX) {
        *(TKEY *)output = TSDB_DATA_TIMESTAMP_NULL;
      } else {
        memcpy(output, &pCtx->qWindow.ekey, pCtx->outputBytes);
      }
      output += pCtx->outputBytes;
    }
  }
}

static void window_duration_function(SQLFunctionCtx *pCtx) {
  int64_t duration;
  if (pCtx->functionId == TSDB_FUNC_WDURATION) {
    SET_VAL(pCtx, pCtx->size, 1);
    duration = pCtx->endTs - pCtx->startTs;
    if (duration < 0) {
      duration = -duration;
    }
    *(int64_t *)(pCtx->pOutput) = duration;
  } else { //TSDB_FUNC_QDURATION
    int32_t size = MIN(pCtx->size, pCtx->allocRows); //size cannot exceeds allocated rows
    SET_VAL(pCtx, pCtx->size, size);
    //INC_INIT_VAL(pCtx, size);
    duration = pCtx->qWindow.ekey - pCtx->qWindow.skey;
    if (duration < 0) {
      duration = -duration;
    }
    char *output = pCtx->pOutput;
    for (int32_t i = 0; i < size; ++i) {
      if (pCtx->qWindow.skey == INT64_MIN || pCtx->qWindow.ekey == INT64_MAX) {
        *(int64_t *)output = TSDB_DATA_BIGINT_NULL;
      } else {
        memcpy(output, &duration, pCtx->outputBytes);
      }
      output += pCtx->outputBytes;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////
/*
 * function compatible list.
 * tag and ts are not involved in the compatibility check
 *
 * 1. functions that are not simultaneously present with any other functions. e.g., diff/ts_z/top/bottom
 * 2. functions that are only allowed to be present only with same functions. e.g., last_row, interp
 * 3. functions that are allowed to be present with other functions.
 *    e.g., count/sum/avg/min/max/stddev/percentile/apercentile/first/last...
 *
 */
int32_t functionCompatList[] = {
    // count,       sum,            avg,       min,        max,         stddev,    percentile,   apercentile, first,        last
    1,              1,              1,         1,          1,           1,          1,           1,           1,            1,
    // last_row,    top,            bottom,    spread,     twa,         leastsqr,   ts,          ts_dummy,    tag_dummy,    ts_comp
    4,              -1,             -1,        1,          1,           1,          1,           1,           1,            -1,
    //  tag,        colprj,         tagprj,    arithm,    diff,         first_dist, last_dist,   stddev_dst,  interp        rate,       irate
    1,              1,              1,         1,         -1,           1,          1,           1,           5,            1,          1,
    // tid_tag,     deriv,          csum,      mavg,      sample,       block_info, elapsed,     histogram,   unique,       mode,       tail
    6,              8,              -1,        -1,        -1,           7,          1,           -1,          -1,           1,          -1,
    // stateCount,  stateDuration,  wstart,    wstop,     wduration,    qstart,     qstop,       qduration,   hyperloglog
    1,              1,              1,         1,         1,            1,          1,           1,           1,
};

SAggFunctionInfo aAggs[TSDB_FUNC_MAX_NUM] = {{
                              // 0, count function does not invoke the finalize function
                              "count",
                              TSDB_FUNC_COUNT,
                              TSDB_FUNC_COUNT,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              count_function,
                              doFinalizer,
                              count_func_merge,
                              countRequired,
                          },
                          {
                              // 1
                              "sum",
                              TSDB_FUNC_SUM,
                              TSDB_FUNC_SUM,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              sum_function,
                              function_finalizer,
                              sum_func_merge,
                              statisRequired,
                          },
                          {
                              // 2
                              "avg",
                              TSDB_FUNC_AVG,
                              TSDB_FUNC_AVG,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              avg_function,
                              avg_finalizer,
                              avg_func_merge,
                              statisRequired,
                          },
                          {
                              // 3
                              "min",
                              TSDB_FUNC_MIN,
                              TSDB_FUNC_MIN,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              min_func_setup,
                              min_function,
                              function_finalizer,
                              min_func_merge,
                              statisRequired,
                          },
                          {
                              // 4
                              "max",
                              TSDB_FUNC_MAX,
                              TSDB_FUNC_MAX,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              max_func_setup,
                              max_function,
                              function_finalizer,
                              max_func_merge,
                              statisRequired,
                          },
                          {
                              // 5
                              "stddev",
                              TSDB_FUNC_STDDEV,
                              TSDB_FUNC_STDDEV_DST,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              function_setup,
                              stddev_function,
                              stddev_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 6
                              "percentile",
                              TSDB_FUNC_PERCT,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              percentile_function_setup,
                              percentile_function,
                              percentile_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 7
                              "apercentile",
                              TSDB_FUNC_APERCT,
                              TSDB_FUNC_APERCT,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_STABLE,
                              apercentile_function_setup,
                              apercentile_function,
                              apercentile_finalizer,
                              apercentile_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 8
                              "first",
                              TSDB_FUNC_FIRST,
                              TSDB_FUNC_FIRST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              first_function,
                              function_finalizer,
                              noop1,
                              firstFuncRequired,
                          },
                          {
                              // 9
                              "last",
                              TSDB_FUNC_LAST,
                              TSDB_FUNC_LAST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              last_function,
                              function_finalizer,
                              noop1,
                              lastFuncRequired,
                          },
                          {
                              // 10
                              "last_row",
                              TSDB_FUNC_LAST_ROW,
                              TSDB_FUNC_LAST_ROW,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS |
                                  TSDB_FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              last_row_function,
                              last_row_finalizer,
                              last_dist_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 11
                              "top",
                              TSDB_FUNC_TOP,
                              TSDB_FUNC_TOP,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_NEED_TS |
                                  TSDB_FUNCSTATE_SELECTIVITY,
                              top_bottom_function_setup,
                              top_function,
                              top_bottom_func_finalizer,
                              top_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 12
                              "bottom",
                              TSDB_FUNC_BOTTOM,
                              TSDB_FUNC_BOTTOM,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_NEED_TS |
                                  TSDB_FUNCSTATE_SELECTIVITY,
                              top_bottom_function_setup,
                              bottom_function,
                              top_bottom_func_finalizer,
                              bottom_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 13
                              "spread",
                              TSDB_FUNC_SPREAD,
                              TSDB_FUNC_SPREAD,
                              TSDB_BASE_FUNC_SO,
                              spread_function_setup,
                              spread_function,
                              spread_function_finalizer,
                              spread_func_merge,
                              countRequired,
                          },
                          {
                              // 14
                              "twa",
                              TSDB_FUNC_TWA,
                              TSDB_FUNC_TWA,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS,
                              twa_function_setup,
                              twa_function,
                              twa_function_finalizer,
                              twa_function_copy,
                              dataBlockRequired,
                          },
                          {
                              // 15
                              "leastsquares",
                              TSDB_FUNC_LEASTSQR,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              leastsquares_function_setup,
                              leastsquares_function,
                              leastsquares_finalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 16
                              "ts",
                              TSDB_FUNC_TS,
                              TSDB_FUNC_TS,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              date_col_output_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 17
                              "ts",
                              TSDB_FUNC_TS_DUMMY,
                              TSDB_FUNC_TS_DUMMY,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              noop1,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 18
                              "tag_dummy",
                              TSDB_FUNC_TAG_DUMMY,
                              TSDB_FUNC_TAG_DUMMY,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              tag_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 19
                              "ts",
                              TSDB_FUNC_TS_COMP,
                              TSDB_FUNC_TS_COMP,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_NEED_TS,
                              ts_comp_function_setup,
                              ts_comp_function,
                              ts_comp_finalize,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 20
                              "tag",
                              TSDB_FUNC_TAG,
                              TSDB_FUNC_TAG,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              tag_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 21, column project sql function
                              "colprj",
                              TSDB_FUNC_PRJ,
                              TSDB_FUNC_PRJ,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              col_project_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 22, multi-output, tag function has only one result
                              "tagprj",
                              TSDB_FUNC_TAGPRJ,
                              TSDB_FUNC_TAGPRJ,
                              TSDB_BASE_FUNC_MO,
                              function_setup,
                              tag_project_function,
                              doFinalizer,
                              copy_function,
                              noDataRequired,
                          },
                          {
                              // 23
                              "arithmetic",
                              TSDB_FUNC_SCALAR_EXPR,
                              TSDB_FUNC_SCALAR_EXPR,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              scalar_expr_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 24
                              "diff",
                              TSDB_FUNC_DIFF,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              diff_function_setup,
                              diff_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
    // distributed version used in two-stage aggregation processes
                          {
                              // 25
                              "first_dist",
                              TSDB_FUNC_FIRST_DST,
                              TSDB_FUNC_FIRST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              first_dist_function,
                              function_finalizer,
                              first_dist_func_merge,
                              firstDistFuncRequired,
                          },
                          {
                              // 26
                              "last_dist",
                              TSDB_FUNC_LAST_DST,
                              TSDB_FUNC_LAST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              last_dist_function,
                              function_finalizer,
                              last_dist_func_merge,
                              lastDistFuncRequired,
                          },
                          {
                              // 27
                              "stddev",   // return table id and the corresponding tags for join match and subscribe
                              TSDB_FUNC_STDDEV_DST,
                              TSDB_FUNC_AVG,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STABLE,
                              function_setup,
                              stddev_dst_function,
                              stddev_dst_finalizer,
                              stddev_dst_merge,
                              dataBlockRequired,
                          },
                          {
                              // 28
                              "interp",
                              TSDB_FUNC_INTERP,
                              TSDB_FUNC_INTERP,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              interp_function,
                              doFinalizer,
                              full_copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 29
                              "rate",
                              TSDB_FUNC_RATE,
                              TSDB_FUNC_RATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              rate_function,
                              rate_finalizer,
                              rate_func_copy,
                              dataBlockRequired,
                          },
                          {
                              // 30
                              "irate",
                              TSDB_FUNC_IRATE,
                              TSDB_FUNC_IRATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              irate_function,
                              rate_finalizer,
                              rate_func_copy,
                              dataBlockRequired,
                          },
                          {
                              // 31
                              "tbid",   // return table id and the corresponding tags for join match and subscribe
                              TSDB_FUNC_TID_TAG,
                              TSDB_FUNC_TID_TAG,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE,
                              function_setup,
                              noop1,
                              noop1,
                              noop1,
                              dataBlockRequired,
                          },
                          {   //32
                              "derivative",   // return table id and the corresponding tags for join match and subscribe
                              TSDB_FUNC_DERIVATIVE,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              deriv_function_setup,
                              deriv_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 33
                              "csum",
                              TSDB_FUNC_CSUM,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              csum_function_setup,
                              csum_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 34
                              "mavg",
                              TSDB_FUNC_MAVG,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              mavg_function_setup,
                              mavg_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 35
                              "sample",
                              TSDB_FUNC_SAMPLE,
                              TSDB_FUNC_SAMPLE,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              sample_function_setup,
                              sample_function,
                              sample_func_finalizer,
                              sample_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 36
                              "_block_dist",
                              TSDB_FUNC_BLKINFO,
                              TSDB_FUNC_BLKINFO,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STABLE,
                              function_setup,
                              blockInfo_func,
                              blockinfo_func_finalizer,
                              block_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 37
                              "elapsed",
                              TSDB_FUNC_ELAPSED,
                              TSDB_FUNC_ELAPSED,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STABLE,
                              elapsedSetup,
                              elapsedFunction,
                              elapsedFinalizer,
                              elapsedMerge,
                              dataBlockRequired,
                          },
                          {
                              //38
                              "histogram",
                              TSDB_FUNC_HISTOGRAM,
                              TSDB_FUNC_HISTOGRAM,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE,
                              histogram_function_setup,
                              histogram_function,
                              histogram_func_finalizer,
                              histogram_func_merge,
                              dataBlockRequired,
                          },
                          {
                              // 39
                              "unique",
                              TSDB_FUNC_UNIQUE,
                              TSDB_FUNC_UNIQUE,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_SELECTIVITY,
                              unique_function_setup,
                              unique_function,
                              unique_func_finalizer,
                              unique_function_merge,
                              dataBlockRequired,
                          },
                          {
                             // 40
                             "mode",
                             TSDB_FUNC_MODE,
                             TSDB_FUNC_MODE,
                             TSDB_BASE_FUNC_SO,
                             mode_function_setup,
                             mode_function,
                             mode_func_finalizer,
                             mode_function_merge,
                             dataBlockRequired,
                          },
                          {
                              // 41
                              "tail",
                              TSDB_FUNC_TAIL,
                              TSDB_FUNC_TAIL,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_SELECTIVITY,
                              tail_function_setup,
                              tail_function,
                              tail_func_finalizer,
                              tail_func_merge,
                              tailFuncRequired,
                          },
                          {
                              // 42
                              "stateCount",
                              TSDB_FUNC_STATE_COUNT,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              state_count_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 43
                              "stateDuration",
                              TSDB_FUNC_STATE_DURATION,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              state_duration_function,
                              doFinalizer,
                              noop1,
                              dataBlockRequired,
                          },
                          {
                              // 44
                              "_wstart",
                              TSDB_FUNC_WSTART,
                              TSDB_FUNC_WSTART,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_start_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 45
                              "_wstop",
                              TSDB_FUNC_WSTOP,
                              TSDB_FUNC_WSTOP,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_stop_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 46
                              "_wduration",
                              TSDB_FUNC_WDURATION,
                              TSDB_FUNC_WDURATION,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_duration_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 47
                              "_qstart",
                              TSDB_FUNC_QSTART,
                              TSDB_FUNC_QSTART,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_start_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 48
                              "_qstop",
                              TSDB_FUNC_QSTOP,
                              TSDB_FUNC_QSTOP,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_stop_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 49
                              "_qduration",
                              TSDB_FUNC_QDURATION,
                              TSDB_FUNC_QDURATION,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              window_duration_function,
                              doFinalizer,
                              copy_function,
                              dataBlockRequired,
                          },
                          {
                              // 50
                              "hyperloglog",
                              TSDB_FUNC_HYPERLOGLOG,
                              TSDB_FUNC_HYPERLOGLOG,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              hll_function,
                              hll_func_finalizer,
                              hll_func_merge,
                              dataBlockRequired,
                          }
};
