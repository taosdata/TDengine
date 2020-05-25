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
#include "qast.h"
#include "qextbuffer.h"
#include "qfill.h"
#include "qhistogram.h"
#include "qpercentile.h"
#include "qsyntaxtreefunction.h"
#include "qtsbuf.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tscLog.h"
#include "tscSubquery.h"
#include "tscompression.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "tutil.h"

#define GET_INPUT_CHAR(x) (((char *)((x)->aInputElemBuf)) + ((x)->startOffset) * ((x)->inputBytes))
#define GET_INPUT_CHAR_INDEX(x, y) (GET_INPUT_CHAR(x) + (y) * (x)->inputBytes)

#define GET_TRUE_DATA_TYPE()                          \
  int32_t type = 0;                                   \
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {  \
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
  } while (0);

#define INC_INIT_VAL(ctx, res) (GET_RES_INFO(ctx)->numOfRes += (res));

#define DO_UPDATE_TAG_COLUMNS(ctx, ts)                                           \
  do {                                                                           \
    for (int32_t i = 0; i < (ctx)->tagInfo.numOfTagCols; ++i) {                  \
      SQLFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[i];                     \
      if (__ctx->functionId == TSDB_FUNC_TS_DUMMY) {                             \
        __ctx->tag = (tVariant){.i64Key = (ts), .nType = TSDB_DATA_TYPE_BIGINT}; \
      }                                                                          \
      aAggs[TSDB_FUNC_TAG].xFunction(__ctx);                                     \
    }                                                                            \
  } while (0);

#define DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx) \
do {\
for (int32_t i = 0; i < (ctx)->tagInfo.numOfTagCols; ++i) {                  \
      SQLFunctionCtx *__ctx = (ctx)->tagInfo.pTagCtxList[i];                     \
      aAggs[TSDB_FUNC_TAG].xFunction(__ctx);                                     \
    }     \
} while(0);

void noop1(SQLFunctionCtx *UNUSED_PARAM(pCtx)) {}
void noop2(SQLFunctionCtx *UNUSED_PARAM(pCtx), int32_t UNUSED_PARAM(index)) {}

void doFinalizer(SQLFunctionCtx *pCtx) { resetResultInfo(GET_RES_INFO(pCtx)); }

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
    int64_t isum;
    double  dsum;
  };
  int8_t hasResult;
} SSumInfo;

// the attribute of hasResult is not needed since the num attribute would server as this purpose
typedef struct SAvgInfo {
  double  sum;
  int64_t num;  // num servers as the hasResult attribute in other struct
} SAvgInfo;

typedef struct SStddevInfo {
  double  avg;
  int64_t num;
  double  res;
  int8_t  stage;
} SStddevInfo;

typedef struct SFirstLastInfo {
  int8_t hasResult;
  TSKEY  ts;
} SFirstLastInfo;

typedef struct SFirstLastInfo SLastrowInfo;
typedef struct SPercentileInfo {
  tMemBucket *pMemBucket;
} SPercentileInfo;

typedef struct STopBotInfo {
  int32_t      num;
  tValuePair **res;
} STopBotInfo;

// leastsquares do not apply to super table
typedef struct SLeastsquareInfo {
  double  mat[2][3];
  double  startVal;
  int64_t num;
} SLeastsquareInfo;

typedef struct SAPercentileInfo {
  SHistogramInfo *pHisto;
} SAPercentileInfo;

typedef struct STSCompInfo {
  STSBuf *pTSBuf;
} STSCompInfo;

typedef struct SRateInfo {
  int64_t CorrectionValue;
  int64_t firstValue;
  TSKEY   firstKey;
  int64_t lastValue;
  TSKEY   lastKey;
  int8_t  hasResult;  // flag to denote has value
  bool    isIRate;    // true for IRate functions, false for Rate functions
  int64_t num;        // for sum/avg
  double  sum;        // for sum/avg
} SRateInfo;


int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type,
                          int16_t *bytes, int16_t *interBytes, int16_t extLength, bool isSuperTable) {
  if (!isValidDataType(dataType, dataBytes)) {
    tscError("Illegal data type %d or data type length %d", dataType, dataBytes);
    return TSDB_CODE_INVALID_SQL;
  }
  
  if (functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY || functionId == TSDB_FUNC_TAG_DUMMY ||
      functionId == TSDB_FUNC_DIFF || functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TAGPRJ ||
      functionId == TSDB_FUNC_TAG || functionId == TSDB_FUNC_INTERP) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    *interBytes = *bytes + sizeof(SResultInfo);
    return TSDB_CODE_SUCCESS;
  }
  
  // (uid, tid) + VGID + TAGSIZE + VARSTR_HEADER_SIZE
  if (functionId == TSDB_FUNC_TID_TAG) { // todo use struct
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = dataBytes + sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t) + VARSTR_HEADER_SIZE;
    *interBytes = *bytes;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == TSDB_FUNC_COUNT) {
    *type = TSDB_DATA_TYPE_BIGINT;
    *bytes = sizeof(int64_t);
    *interBytes = *bytes;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == TSDB_FUNC_ARITHM) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = *bytes;
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == TSDB_FUNC_TS_COMP) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(int32_t);  // this results is compressed ts data
    *interBytes = POINTER_BYTES;
    return TSDB_CODE_SUCCESS;
  }
  
  if (isSuperTable) {
    if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = dataBytes + DATA_SET_FLAG_SIZE;
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
      
    } else if (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE) {
      *type = TSDB_DATA_TYPE_DOUBLE;
      *bytes = sizeof(SRateInfo);
      *interBytes = sizeof(SRateInfo);
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param;
      *interBytes = *bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_SPREAD) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SSpreadInfo);
      *interBytes = *bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_APERCT) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1) + sizeof(SHistogramInfo) + sizeof(SAPercentileInfo);
      *interBytes = *bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_LAST_ROW) {
      *type = TSDB_DATA_TYPE_BINARY;
      *bytes = sizeof(SLastrowInfo) + dataBytes;
      *interBytes = *bytes;
      
      return TSDB_CODE_SUCCESS;
    } else if (functionId == TSDB_FUNC_TWA) {
      *type = TSDB_DATA_TYPE_DOUBLE;
      *bytes = sizeof(STwaInfo);
      *interBytes = *bytes;
      return TSDB_CODE_SUCCESS;
    }
  }
  
  if (functionId == TSDB_FUNC_SUM) {
    if (dataType >= TSDB_DATA_TYPE_TINYINT && dataType <= TSDB_DATA_TYPE_BIGINT) {
      *type = TSDB_DATA_TYPE_BIGINT;
    } else {
      *type = TSDB_DATA_TYPE_DOUBLE;
    }
    
    *bytes = sizeof(int64_t);
    *interBytes = sizeof(SSumInfo);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == TSDB_FUNC_APERCT) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes =
        sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1);
    return TSDB_CODE_SUCCESS;
  } else if (functionId == TSDB_FUNC_TWA) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(STwaInfo);
    return TSDB_CODE_SUCCESS;
  }
  
  if (functionId == TSDB_FUNC_AVG) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SAvgInfo);
  } else if (functionId >= TSDB_FUNC_RATE && functionId <= TSDB_FUNC_AVG_IRATE) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SRateInfo);
  } else if (functionId == TSDB_FUNC_STDDEV) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SStddevInfo);
  } else if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    *interBytes = dataBytes + DATA_SET_FLAG_SIZE;
  } else if (functionId == TSDB_FUNC_FIRST || functionId == TSDB_FUNC_LAST) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    *interBytes = dataBytes + sizeof(SResultInfo);
  } else if (functionId == TSDB_FUNC_SPREAD) {
    *type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    *interBytes = sizeof(SSpreadInfo);
  } else if (functionId == TSDB_FUNC_PERCT) {
    *type = (int16_t)TSDB_DATA_TYPE_DOUBLE;
    *bytes = (int16_t)sizeof(double);
    *interBytes = (int16_t)sizeof(double);
  } else if (functionId == TSDB_FUNC_LEASTSQR) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE;  // string
    *interBytes = *bytes + sizeof(SResultInfo);
  } else if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = dataBytes + sizeof(SFirstLastInfo);
    *interBytes = *bytes;
  } else if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    
    size_t size = sizeof(STopBotInfo) + (sizeof(tValuePair) + POINTER_BYTES + extLength) * param;
    
    // the output column may be larger than sizeof(STopBotInfo)
    *interBytes = size;
  } else if (functionId == TSDB_FUNC_LAST_ROW) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    *interBytes = dataBytes + sizeof(SLastrowInfo);
  } else {
    return TSDB_CODE_INVALID_SQL;
  }
  
  return TSDB_CODE_SUCCESS;
}

bool stableQueryFunctChanged(int32_t funcId) {
  return (aAggs[funcId].stableFuncId != funcId);
}

/**
 * the numOfRes should be kept, since it may be used later
 * and allow the ResultInfo to be re initialized
 */
void resetResultInfo(SResultInfo *pResInfo) { pResInfo->initialized = false; }

void initResultInfo(SResultInfo *pResInfo) {
  pResInfo->initialized = true;  // the this struct has been initialized flag
  
  pResInfo->complete = false;
  pResInfo->hasResult = false;
  pResInfo->numOfRes = 0;
  
  memset(pResInfo->interResultBuf, 0, (size_t)pResInfo->bufLen);
}

void setResultInfoBuf(SResultInfo *pResInfo, int32_t size, bool superTable) {
  assert(pResInfo->interResultBuf == NULL);
  
  pResInfo->bufLen = size;
  pResInfo->superTableQ = superTable;
  
  pResInfo->interResultBuf = calloc(1, (size_t)size);
}

// set the query flag to denote that query is completed
static void no_next_step(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->complete = true;
}

static bool function_setup(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->initialized) {
    return false;
  }
  
  memset(pCtx->aOutputBuf, 0, (size_t)pCtx->outputBytes);
  
  initResultInfo(pResInfo);
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
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pResInfo->hasResult != DATA_SET_FLAG) {
    tscTrace("no result generated, result is set to NULL");
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
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
        char *val = GET_INPUT_CHAR_INDEX(pCtx, i);
        if (isNull(val, pCtx->inputType)) {
          continue;
        }
        
        numOfElem += 1;
      }
    } else {
      /*
       * when counting on the primary time stamp column and no statistics data is provided,
       * simple use the size value
       */
      numOfElem = pCtx->size;
    }
  }
  
  if (numOfElem > 0) {
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
  
  *((int64_t *)pCtx->aOutputBuf) += numOfElem;
  SET_VAL(pCtx, numOfElem, 1);
}

static void count_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  
  *((int64_t *)pCtx->aOutputBuf) += 1;
  
  // do not need it actually
  SResultInfo *pInfo = GET_RES_INFO(pCtx);
  pInfo->hasResult = DATA_SET_FLAG;
}

static void count_func_merge(SQLFunctionCtx *pCtx) {
  int64_t *pData = (int64_t *)GET_INPUT_CHAR(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i) {
    *((int64_t *)pCtx->aOutputBuf) += pData[i];
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
int32_t count_load_data_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return BLK_DATA_NO_NEEDED;
  } else {
    return BLK_DATA_FILEDS_NEEDED;
  }
}

int32_t no_data_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  return BLK_DATA_NO_NEEDED;
}

#define LIST_ADD_N(x, ctx, p, t, numOfElem, tsdbType)              \
  {                                                                \
    t *d = (t *)(p);                                               \
    for (int32_t i = 0; i < (ctx)->size; ++i) {                    \
      if (((ctx)->hasNull) && isNull((char *)&(d)[i], tsdbType)) { \
        continue;                                                  \
      };                                                           \
      (x) += (d)[i];                                               \
      (numOfElem)++;                                               \
    }                                                              \
  };

#define UPDATE_DATA(ctx, left, right, num, sign, k) \
  do {                                              \
    if (((left) < (right)) ^ (sign)) {              \
      (left) = (right);                             \
      DO_UPDATE_TAG_COLUMNS(ctx, k);                \
      (num) += 1;                                   \
    }                                               \
  } while (0);

#define DUPATE_DATA_WITHOUT_TS(ctx, left, right, num, sign) \
do {                                              \
    if (((left) < (right)) ^ (sign)) {              \
      (left) = (right);                             \
      DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx);                \
      (num) += 1;                                   \
    }                                               \
  } while (0);


#define LOOPCHECK_N(val, list, ctx, tsdbType, sign, num)          \
  for (int32_t i = 0; i < ((ctx)->size); ++i) {                   \
    if ((ctx)->hasNull && isNull((char *)&(list)[i], tsdbType)) { \
      continue;                                                   \
    }                                                             \
    TSKEY key = (ctx)->ptsList[i];                                \
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
    
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t *retVal = (int64_t*) pCtx->aOutputBuf;
      *retVal += pCtx->preAggVals.statis.sum;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = (double*) pCtx->aOutputBuf;
      *retVal += GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.sum));
    }
  } else {  // computing based on the true data block
    void *pData = GET_INPUT_CHAR(pCtx);
    notNullElems = 0;
    
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t *retVal = (int64_t*) pCtx->aOutputBuf;
      
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(*retVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(*retVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *retVal = (double*) pCtx->aOutputBuf;
      LIST_ADD_N(*retVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = (double*) pCtx->aOutputBuf;
      LIST_ADD_N(*retVal, pCtx, pData, float, notNullElems, pCtx->inputType);
    }
  }
  
  // data in the check operation are all null, not output
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
  }
}

static void do_sum_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  int64_t *res = (int64_t*) pCtx->aOutputBuf;
  
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    *res += GET_INT8_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    *res += GET_INT16_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    *res += GET_INT32_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    *res += GET_INT64_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *retVal = (double*) pCtx->aOutputBuf;
    *retVal += GET_DOUBLE_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    double *retVal = (double*) pCtx->aOutputBuf;
    *retVal += GET_FLOAT_VAL(pData);
  }
  
  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

static void sum_function(SQLFunctionCtx *pCtx) {
  do_sum(pCtx);
  
  // keep the result data in output buffer, not in the intermediate buffer
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult == DATA_SET_FLAG && pResInfo->superTableQ) {
    // set the flag for super table query
    SSumInfo *pSum = (SSumInfo *)pCtx->aOutputBuf;
    pSum->hasResult = DATA_SET_FLAG;
  }
}

static void sum_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  do_sum_f(pCtx, index);
  
  // keep the result data in output buffer, not in the intermediate buffer
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult == DATA_SET_FLAG && pResInfo->superTableQ) {
    SSumInfo *pSum = (SSumInfo *)pCtx->aOutputBuf;
    pSum->hasResult = DATA_SET_FLAG;
  }
}

static int32_t sum_merge_impl(const SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  GET_TRUE_DATA_TYPE();
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *    input = GET_INPUT_CHAR_INDEX(pCtx, i);
    SSumInfo *pInput = (SSumInfo *)input;
    if (pInput->hasResult != DATA_SET_FLAG) {
      continue;
    }
    
    notNullElems++;
    
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_BIGINT: {
        *(int64_t *)pCtx->aOutputBuf += pInput->isum;
        break;
      };
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE: {
        *(double *)pCtx->aOutputBuf += pInput->dsum;
      }
    }
  }
  
  return notNullElems;
}

static void sum_func_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = sum_merge_impl(pCtx);
  
  SET_VAL(pCtx, notNullElems, 1);
  SSumInfo *pSumInfo = (SSumInfo *)pCtx->aOutputBuf;
  
  if (notNullElems > 0) {
    //    pCtx->numOfIteratedElems += notNullElems;
    pSumInfo->hasResult = DATA_SET_FLAG;
  }
}

static void sum_func_second_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = sum_merge_impl(pCtx);
  
  SET_VAL(pCtx, notNullElems, 1);
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static int32_t precal_req_load_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  return BLK_DATA_FILEDS_NEEDED;
}

static int32_t data_req_load_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  return BLK_DATA_ALL_NEEDED;
}

// todo: if  column in current data block are null, opt for this case
static int32_t first_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }
  
  // no result for first query, data block is required
  if (GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t last_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_ASC) {
    return BLK_DATA_NO_NEEDED;
  }
  
  if (GET_RES_INFO(pCtx)->numOfRes <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t first_dist_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return BLK_DATA_NO_NEEDED;
  }
  
  // result buffer has not been set yet.
  return BLK_DATA_ALL_NEEDED;
  //todo optimize the filter info
//  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->aOutputBuf + pCtx->inputBytes);
//  if (pInfo->hasResult != DATA_SET_FLAG) {
//    return BLK_DATA_ALL_NEEDED;
//  } else {  // data in current block is not earlier than current result
//    return (pInfo->ts <= start) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
//  }
}

static int32_t last_dist_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId) {
  if (pCtx->order == TSDB_ORDER_ASC) {
    return BLK_DATA_NO_NEEDED;
  }
  
  return BLK_DATA_ALL_NEEDED;
//  SFirstLastInfo *pInfo = (SFirstLastInfo*) (pCtx->aOutputBuf + pCtx->inputBytes);
//  if (pInfo->hasResult != DATA_SET_FLAG) {
//    return BLK_DATA_ALL_NEEDED;
//  } else {
//    return (pInfo->ts > end) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
//  }
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
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  SAvgInfo *pAvgInfo = (SAvgInfo *)pResInfo->interResultBuf;
  double *  pVal = &pAvgInfo->sum;
  
  if (pCtx->preAggVals.isSet) {
    // Pre-aggregation
    notNullElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    assert(notNullElems >= 0);
    
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      *pVal += pCtx->preAggVals.statis.sum;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      *pVal += GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.sum));
    }
  } else {
    void *pData = GET_INPUT_CHAR(pCtx);
    
    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_ADD_N(*pVal, pCtx, pData, int32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      LIST_ADD_N(*pVal, pCtx, pData, int64_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N(*pVal, pCtx, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N(*pVal, pCtx, pData, float, notNullElems, pCtx->inputType);
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
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SAvgInfo));
  }
}

static void avg_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  
  // NOTE: keep the intermediate result into the interResultBuf
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  SAvgInfo *pAvgInfo = (SAvgInfo *)pResInfo->interResultBuf;
  
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    pAvgInfo->sum += GET_INT8_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    pAvgInfo->sum += GET_INT16_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    pAvgInfo->sum += GET_INT32_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    pAvgInfo->sum += GET_INT64_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    pAvgInfo->sum += GET_DOUBLE_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    pAvgInfo->sum += GET_FLOAT_VAL(pData);
  }
  
  // restore sum and count of elements
  pAvgInfo->num += 1;
  
  // set has result flag
  pResInfo->hasResult = DATA_SET_FLAG;
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SAvgInfo));
  }
}

static void avg_func_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  SAvgInfo *pAvgInfo = (SAvgInfo *)pResInfo->interResultBuf;
  char *    input = GET_INPUT_CHAR(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgInfo *pInput = (SAvgInfo *)input;
    if (pInput->num == 0) {  // current buffer is null
      continue;
    }
    
    pAvgInfo->sum += pInput->sum;
    pAvgInfo->num += pInput->num;
  }
  
  // if the data set hasResult is not set, the result is null
  if (pAvgInfo->num > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SAvgInfo));
  }
}

static void avg_func_second_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  double *sum = (double*) pCtx->aOutputBuf;
  char *  input = GET_INPUT_CHAR(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgInfo *pInput = (SAvgInfo *)input;
    if (pInput->num == 0) {  // current input is null
      continue;
    }
    
    *sum += pInput->sum;
    
    // keep the number of data into the temp buffer
    *(int64_t *)pResInfo->interResultBuf += pInput->num;
  }
}

/*
 * the average value is calculated in finalize routine, since current routine does not know the exact number of points
 */
static void avg_finalizer(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
    
    if (GET_INT64_VAL(pResInfo->interResultBuf) <= 0) {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;  // empty table
    }
    
    *(double *)pCtx->aOutputBuf = (*(double *)pCtx->aOutputBuf) / *(int64_t *)pResInfo->interResultBuf;
  } else {  // this is the secondary merge, only in the secondary merge, the input type is TSDB_DATA_TYPE_BINARY
    assert(pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_DOUBLE);
    
    SAvgInfo *pAvgInfo = (SAvgInfo *)pResInfo->interResultBuf;
    
    if (pAvgInfo->num == 0) {  // all data are NULL or empty table
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
    
    *(double *)pCtx->aOutputBuf = pAvgInfo->sum / pAvgInfo->num;
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
    
    void *  tval = NULL;
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
      if (index < 0 || index >= pCtx->size + pCtx->startOffset) {
        index = 0;
      }
      
      key = pCtx->ptsList[index];
    }
    
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t val = GET_INT64_VAL(tval);
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        int8_t *data = (int8_t *)pOutput;
        
        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        int16_t *data = (int16_t *)pOutput;
        
        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        int32_t *data = (int32_t *)pOutput;
#if defined(_DEBUG_VIEW)
        tscTrace("max value updated according to pre-cal:%d", *data);
#endif
        
        if ((*data < val) ^ isMin) {
          *data = val;
          for (int32_t i = 0; i < (pCtx)->tagInfo.numOfTagCols; ++i) {
            SQLFunctionCtx *__ctx = pCtx->tagInfo.pTagCtxList[i];
            if (__ctx->functionId == TSDB_FUNC_TS_DUMMY) {
              __ctx->tag = (tVariant){.i64Key = key, .nType = TSDB_DATA_TYPE_BIGINT};
            }
            
            aAggs[TSDB_FUNC_TAG].xFunction(__ctx);
          }
        }
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        int64_t *data = (int64_t *)pOutput;
        UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *data = (double *)pOutput;
      double  val = GET_DOUBLE_VAL(tval);
      
      UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      float *data = (float *)pOutput;
      double val = GET_DOUBLE_VAL(tval);
      
      UPDATE_DATA(pCtx, *data, val, notNullElems, isMin, key);
    }
    
    return;
  }
  
  void *p = GET_INPUT_CHAR(pCtx);
  *notNullElems = 0;
  
  if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
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
          TSKEY k = pCtx->ptsList[i];
          
          DO_UPDATE_TAG_COLUMNS(pCtx, k);
        }
        
        *notNullElems += 1;
      }
#if defined(_DEBUG_VIEW)
      tscTrace("max value updated:%d", *retVal);
#endif
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      TYPED_LOOPCHECK_N(int64_t, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    TYPED_LOOPCHECK_N(double, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    TYPED_LOOPCHECK_N(float, pOutput, p, pCtx, pCtx->inputType, isMin, *notNullElems);
  }
}

static bool min_func_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;  // not initialized since it has been initialized
  }
  
  GET_TRUE_DATA_TYPE();
  
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->aOutputBuf) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->aOutputBuf) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *((double *)pCtx->aOutputBuf) = DBL_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->aOutputBuf) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->aOutputBuf) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->aOutputBuf) = INT8_MAX;
      break;
    default:
      tscError("illegal data type:%d in min/max query", pCtx->inputType);
  }
  
  return true;
}

static bool max_func_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;  // not initialized since it has been initialized
  }
  
  GET_TRUE_DATA_TYPE();
  
  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)pCtx->aOutputBuf) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)pCtx->aOutputBuf) = -FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *((double *)pCtx->aOutputBuf) = -DBL_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)pCtx->aOutputBuf) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)pCtx->aOutputBuf) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)pCtx->aOutputBuf) = INT8_MIN;
      break;
    default:
      tscError("illegal data type:%d in min/max query", pCtx->inputType);
  }
  
  return true;
}

/*
 * the output result of min/max function is the final output buffer, not the intermediate result buffer
 */
static void min_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->aOutputBuf, 1, &notNullElems);
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    // set the flag for super table query
    if (pResInfo->superTableQ) {
      *(pCtx->aOutputBuf + pCtx->inputBytes) = DATA_SET_FLAG;
    }
  }
}

static void max_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->aOutputBuf, 0, &notNullElems);
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    // set the flag for super table query
    if (pResInfo->superTableQ) {
      *(pCtx->aOutputBuf + pCtx->inputBytes) = DATA_SET_FLAG;
    }
  }
}

static int32_t minmax_merge_impl(SQLFunctionCtx *pCtx, int32_t bytes, char *output, bool isMin) {
  int32_t notNullElems = 0;
  
  GET_TRUE_DATA_TYPE();
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *input = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (input[bytes] != DATA_SET_FLAG) {
      continue;
    }
    
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t v = GET_INT8_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int8_t *)output, v, notNullElems, isMin);
        break;
      };
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t v = GET_INT16_VAL(input);
        DUPATE_DATA_WITHOUT_TS(pCtx, *(int16_t *)output, v, notNullElems, isMin);
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t v = GET_INT32_VAL(input);
        if ((*(int32_t *)output < v) ^ isMin) {
          *(int32_t *)output = v;
          
          for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
            SQLFunctionCtx *__ctx = pCtx->tagInfo.pTagCtxList[i];
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
      };
      default:
        break;
    }
  }
  
  return notNullElems;
}

static void min_func_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_merge_impl(pCtx, pCtx->inputBytes, pCtx->aOutputBuf, 1);
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {  // for super table query, SResultInfo is not used
    char *flag = pCtx->aOutputBuf + pCtx->inputBytes;
    *flag = DATA_SET_FLAG;
  }
}

static void min_func_second_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_merge_impl(pCtx, pCtx->outputBytes, pCtx->aOutputBuf, 1);
  
  SET_VAL(pCtx, notNullElems, 1);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void max_func_merge(SQLFunctionCtx *pCtx) {
  int32_t numOfElems = minmax_merge_impl(pCtx, pCtx->inputBytes, pCtx->aOutputBuf, 0);
  
  SET_VAL(pCtx, numOfElems, 1);
  if (numOfElems > 0) {
    char *flag = pCtx->aOutputBuf + pCtx->inputBytes;
    *flag = DATA_SET_FLAG;
  }
}

static void max_func_second_merge(SQLFunctionCtx *pCtx) {
  int32_t numOfElem = minmax_merge_impl(pCtx, pCtx->outputBytes, pCtx->aOutputBuf, 0);
  
  SET_VAL(pCtx, numOfElem, 1);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (numOfElem > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void minMax_function_f(SQLFunctionCtx *pCtx, int32_t index, int32_t isMin) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  TSKEY key = pCtx->ptsList[index];
  
  int32_t num = 0;
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    int8_t *output = (int8_t *)pCtx->aOutputBuf;
    int8_t  i = GET_INT8_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    int16_t *output = (int16_t*) pCtx->aOutputBuf;
    int16_t  i = GET_INT16_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    int32_t *output = (int32_t*) pCtx->aOutputBuf;
    int32_t  i = GET_INT32_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    int64_t *output = (int64_t*) pCtx->aOutputBuf;
    int64_t  i = GET_INT64_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    float *output = (float*) pCtx->aOutputBuf;
    float  i = GET_FLOAT_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *output = (double*) pCtx->aOutputBuf;
    double  i = GET_DOUBLE_VAL(pData);
    
    UPDATE_DATA(pCtx, *output, i, num, isMin, key);
  }
  
  GET_RES_INFO(pCtx)->hasResult = DATA_SET_FLAG;
}

static void max_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  minMax_function_f(pCtx, index, 0);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult == DATA_SET_FLAG) {
    char *flag = pCtx->aOutputBuf + pCtx->inputBytes;
    *flag = DATA_SET_FLAG;
  }
}

static void min_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  minMax_function_f(pCtx, index, 1);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pResInfo->hasResult == DATA_SET_FLAG) {
    char *flag = pCtx->aOutputBuf + pCtx->inputBytes;
    *flag = DATA_SET_FLAG;
  }
}

#define LOOP_STDDEV_IMPL(type, r, d, ctx, delta, tsdbType)             \
  for (int32_t i = 0; i < (ctx)->size; ++i) {                          \
    if ((ctx)->hasNull && isNull((char *)&((type *)d)[i], tsdbType)) { \
      continue;                                                        \
    }                                                                  \
    (r) += POW2(((type *)d)[i] - (delta));                             \
  }

static void stddev_function(SQLFunctionCtx *pCtx) {
  // the second stage to calculate standard deviation
  SStddevInfo *pStd = GET_RES_INFO(pCtx)->interResultBuf;
  
  if (pStd->stage == 0) {  // the first stage is to calculate average value
    avg_function(pCtx);
  } else {
    double *retVal = &pStd->res;
    double  avg = pStd->avg;
    
    void *pData = GET_INPUT_CHAR(pCtx);
    
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        for (int32_t i = 0; i < pCtx->size; ++i) {
          if (pCtx->hasNull && isNull((const char*) (&((int32_t *)pData)[i]), pCtx->inputType)) {
            continue;
          }
          *retVal += POW2(((int32_t *)pData)[i] - avg);
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx, avg, pCtx->inputType);
        break;
      }
      default:
        tscError("stddev function not support data type:%d", pCtx->inputType);
    }
    
    // TODO get the correct data
    SET_VAL(pCtx, 1, 1);
  }
}

static void stddev_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  // the second stage to calculate standard deviation
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevInfo *pStd = pResInfo->interResultBuf;
  
  /* the first stage is to calculate average value */
  if (pStd->stage == 0) {
    avg_function_f(pCtx, index);
  } else {
    double avg = pStd->avg;
    void * pData = GET_INPUT_CHAR_INDEX(pCtx, index);
    
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      return;
    }
    
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        pStd->res += POW2(GET_INT32_VAL(pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        pStd->res += POW2(GET_FLOAT_VAL(pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        pStd->res += POW2(GET_DOUBLE_VAL(pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        pStd->res += POW2(GET_INT64_VAL(pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        pStd->res += POW2(GET_INT16_VAL(pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        pStd->res += POW2(GET_INT8_VAL(pData) - avg);
        break;
      }
      default:
        tscError("stddev function not support data type:%d", pCtx->inputType);
    }
    
    SET_VAL(pCtx, 1, 1);
  }
}

static void stddev_next_step(SQLFunctionCtx *pCtx) {
  /*
   * the stddevInfo and the average info struct share the same buffer area
   * And the position of each element in their struct is exactly the same matched
   */
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SStddevInfo *pStd = pResInfo->interResultBuf;
  
  if (pStd->stage == 0) {
    /*
     * stddev is calculated in two stage:
     * 1. get the average value of all data;
     * 2. get final result, based on the average values;
     * so, if this routine is in second stage, no further step is required
     */
    pStd->stage++;
    avg_finalizer(pCtx);
    
    pResInfo->initialized = true; // set it initialized to avoid re-initialization
    
    // save average value into tmpBuf, for second stage scan
    SAvgInfo *pAvg = pResInfo->interResultBuf;
    
    pStd->avg = GET_DOUBLE_VAL(pCtx->aOutputBuf);
    assert((isnan(pAvg->sum) && pAvg->num == 0) || (pStd->num == pAvg->num && pStd->avg == pAvg->sum));
  } else {
    pResInfo->complete = true;
  }
}

static void stddev_finalizer(SQLFunctionCtx *pCtx) {
  SStddevInfo *pStd = (SStddevInfo *)GET_RES_INFO(pCtx)->interResultBuf;
  
  if (pStd->num <= 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
  } else {
    double *retValue = (double *)pCtx->aOutputBuf;
    *retValue = sqrt(pStd->res / pStd->num);
    SET_VAL(pCtx, 1, 1);
  }
  
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////
static bool first_last_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  // used to keep the timestamp for comparison
  pCtx->param[1].nType = 0;
  pCtx->param[1].i64Key = 0;
  
  return true;
}

// todo opt for null block
static void first_function(SQLFunctionCtx *pCtx) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  int32_t notNullElems = 0;
  
  // handle the null value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    memcpy(pCtx->aOutputBuf, data, pCtx->inputBytes);
    
    TSKEY k = pCtx->ptsList[i];
    DO_UPDATE_TAG_COLUMNS(pCtx, k);
    
    SResultInfo *pInfo = GET_RES_INFO(pCtx);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->complete = true;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);
  
  TSKEY ts = pCtx->ptsList[index];
  DO_UPDATE_TAG_COLUMNS(pCtx, ts);
  
  SResultInfo *pInfo = GET_RES_INFO(pCtx);
  pInfo->hasResult = DATA_SET_FLAG;
  pInfo->complete = true;  // get the first not-null data, completed
}

static void first_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = pCtx->ptsList;
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->aOutputBuf + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || timestamp[index] < pInfo->ts) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);
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
  if (pCtx->size == 0) {
    return;
  }
  
  /*
   * do not to check data in the following cases:
   * 1. data block that are not loaded
   * 2. scan data files in desc order
   */
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  int32_t notNullElems = 0;
  
  // find the first not null value
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    first_data_assign_impl(pCtx, data, i);
    
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void first_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->size == 0) {
    return;
  }
  
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  if (pCtx->order == TSDB_ORDER_DESC) {
    return;
  }
  
  first_data_assign_impl(pCtx, pData, index);
  
  SET_VAL(pCtx, 1, 1);
}

static void first_dist_func_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pCtx->size == 1 && pResInfo->superTableQ);
  
  SFirstLastInfo *pInput = (SFirstLastInfo *)(pData + pCtx->inputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  SFirstLastInfo *pOutput = (SFirstLastInfo *)(pCtx->aOutputBuf + pCtx->inputBytes);
  if (pOutput->hasResult != DATA_SET_FLAG || pInput->ts < pOutput->ts) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes + sizeof(SFirstLastInfo));
    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
}

static void first_dist_func_second_merge(SQLFunctionCtx *pCtx) {
  assert(pCtx->resultInfo->superTableQ);
  
  char *          pData = GET_INPUT_CHAR(pCtx);
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->outputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  // The param[1] is used to keep the initial value of max ts value
  if (pCtx->param[1].nType != pCtx->outputType || pCtx->param[1].i64Key > pInput->ts) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->outputBytes);
    pCtx->param[1].i64Key = pInput->ts;
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
  if (pCtx->order == TSDB_ORDER_ASC) {
    return;
  }
  
  int32_t notNullElems = 0;
  
  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    memcpy(pCtx->aOutputBuf, data, pCtx->inputBytes);
    
    TSKEY ts = pCtx->ptsList[i];
    DO_UPDATE_TAG_COLUMNS(pCtx, ts);
    
    SResultInfo *pInfo = GET_RES_INFO(pCtx);
    pInfo->hasResult = DATA_SET_FLAG;
    
    pInfo->complete = true;  // set query completed on this column
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void last_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  assert(pCtx->order != TSDB_ORDER_ASC);
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);
  
  TSKEY ts = pCtx->ptsList[index];
  DO_UPDATE_TAG_COLUMNS(pCtx, ts);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  pResInfo->complete = true;  // set query completed
}

static void last_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  int64_t *timestamp = pCtx->ptsList;
  
  SFirstLastInfo *pInfo = (SFirstLastInfo *)(pCtx->aOutputBuf + pCtx->inputBytes);
  
  if (pInfo->hasResult != DATA_SET_FLAG || pInfo->ts < timestamp[index]) {
#if defined(_DEBUG_VIEW)
    tscTrace("assign index:%d, ts:%" PRId64 ", val:%d, ", index, timestamp[index], *(int32_t *)pData);
#endif
    
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);
    pInfo->hasResult = DATA_SET_FLAG;
    pInfo->ts = timestamp[index];
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo->ts);
  }
}

static void last_dist_function(SQLFunctionCtx *pCtx) {
  if (pCtx->size == 0) {
    return;
  }
  
  /*
   * 1. for scan data in asc order, no need to check data
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (pCtx->order == TSDB_ORDER_ASC) {
    return;
  }
  
  int32_t notNullElems = 0;
  
  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    last_data_assign_impl(pCtx, data, i);
    
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
    
    notNullElems++;
    break;
  }
  
  SET_VAL(pCtx, notNullElems, 1);
}

static void last_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->size == 0) {
    return;
  }
  
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  /*
   * 1. for scan data in asc order, no need to check data
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (pCtx->order == TSDB_ORDER_ASC) {
    return;
  }
  
  last_data_assign_impl(pCtx, pData, index);
  
  SET_VAL(pCtx, 1, 1);
}

static void last_dist_func_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pCtx->size == 1 && pResInfo->superTableQ);
  
  // the input data is null
  SFirstLastInfo *pInput = (SFirstLastInfo *)(pData + pCtx->inputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  SFirstLastInfo *pOutput = (SFirstLastInfo *)(pCtx->aOutputBuf + pCtx->inputBytes);
  if (pOutput->hasResult != DATA_SET_FLAG || pOutput->ts < pInput->ts) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes + sizeof(SFirstLastInfo));
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInput->ts);
  }
}

/*
 * in the secondary merge(local reduce), the output is limited by the
 * final output size, so the main difference between last_dist_func_merge and second_merge
 * is: the output data format in computing
 */
static void last_dist_func_second_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);
  
  SFirstLastInfo *pInput = (SFirstLastInfo*) (pData + pCtx->outputBytes);
  if (pInput->hasResult != DATA_SET_FLAG) {
    return;
  }
  
  /*
   * param[1] used to keep the corresponding timestamp to decide if current result is
   * the true last result
   */
  if (pCtx->param[1].nType != pCtx->outputType || pCtx->param[1].i64Key < pInput->ts) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->outputBytes);
    pCtx->param[1].i64Key = pInput->ts;
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
  assert(pCtx->size == 1);
  
  char *pData = GET_INPUT_CHAR(pCtx);
  assignVal(pCtx->aOutputBuf, pData, pCtx->inputBytes, pCtx->inputType);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  
  SLastrowInfo *pInfo = (SLastrowInfo *)pResInfo->interResultBuf;
  pInfo->ts = pCtx->param[0].i64Key;
  pInfo->hasResult = DATA_SET_FLAG;
  
  // set the result to final result buffer
  if (pResInfo->superTableQ) {
    SLastrowInfo *pInfo1 = (SLastrowInfo *)(pCtx->aOutputBuf + pCtx->inputBytes);
    pInfo1->ts = pCtx->param[0].i64Key;
    pInfo1->hasResult = DATA_SET_FLAG;
    
    DO_UPDATE_TAG_COLUMNS(pCtx, pInfo1->ts);
  }
  
  SET_VAL(pCtx, pCtx->size, 1);
}

static void last_row_finalizer(SQLFunctionCtx *pCtx) {
  // do nothing at the first stage
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    if (pResInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  } else {
    if (pResInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static void valuePairAssign(tValuePair *dst, int16_t type, const char *val, int64_t tsKey, char *pTags,
                            SExtTagsInfo *pTagInfo, int16_t stage) {
  dst->v.nType = type;
  dst->v.i64Key = *(int64_t *)val;
  dst->timestamp = tsKey;
  
  int32_t size = 0;
  if (stage == SECONDARY_STAGE_MERGE || stage == FIRST_STAGE_MERGE) {
    memcpy(dst->pTags, pTags, (size_t)pTagInfo->tagsLen);
  } else {  // the tags are dumped from the ctx tag fields
    for (int32_t i = 0; i < pTagInfo->numOfTagCols; ++i) {
      SQLFunctionCtx* __ctx = pTagInfo->pTagCtxList[i];
      if (__ctx->functionId == TSDB_FUNC_TS_DUMMY) {
        __ctx->tag = (tVariant) {.nType = TSDB_DATA_TYPE_BIGINT, .i64Key = tsKey};
      }
      
      tVariantDump(&pTagInfo->pTagCtxList[i]->tag, dst->pTags + size, pTagInfo->pTagCtxList[i]->tag.nType);
      size += pTagInfo->pTagCtxList[i]->outputBytes;
    }
  }
}

#define VALUEPAIRASSIGN(dst, src, __l)                 \
  do {                                                 \
    (dst)->timestamp = (src)->timestamp;               \
    (dst)->v = (src)->v;                               \
    memcpy((dst)->pTags, (src)->pTags, (size_t)(__l)); \
  } while (0);

static void do_top_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  tVariant val = {0};
  tVariantCreateFromBinary(&val, pData, tDataTypeDesc[type].nSize, type);
  
  tValuePair **pList = pInfo->res;
  assert(pList != NULL);
  
  if (pInfo->num < maxLen) {
    if (pInfo->num == 0 ||
        ((type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) &&
            val.i64Key >= pList[pInfo->num - 1]->v.i64Key) ||
        ((type >= TSDB_DATA_TYPE_FLOAT && type <= TSDB_DATA_TYPE_DOUBLE) &&
            val.dKey >= pList[pInfo->num - 1]->v.dKey)) {
      valuePairAssign(pList[pInfo->num], type, (const char*)&val.i64Key, ts, pTags, pTagInfo, stage);
    } else {
      int32_t i = pInfo->num - 1;
      
      if (type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) {
        while (i >= 0 && pList[i]->v.i64Key > val.i64Key) {
          VALUEPAIRASSIGN(pList[i + 1], pList[i], pTagInfo->tagsLen);
          i -= 1;
        }
      } else {
        while (i >= 0 && pList[i]->v.dKey > val.dKey) {
          VALUEPAIRASSIGN(pList[i + 1], pList[i], pTagInfo->tagsLen);
          i -= 1;
        }
      }
      
      valuePairAssign(pList[i + 1], type, (const char*) &val.i64Key, ts, pTags, pTagInfo, stage);
    }
    
    pInfo->num++;
  } else {
    int32_t i = 0;
    
    if (((type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) && val.i64Key > pList[0]->v.i64Key) ||
        ((type >= TSDB_DATA_TYPE_FLOAT && type <= TSDB_DATA_TYPE_DOUBLE) && val.dKey > pList[0]->v.dKey)) {
      // find the appropriate the slot position
      if (type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) {
        while (i + 1 < maxLen && pList[i + 1]->v.i64Key < val.i64Key) {
          VALUEPAIRASSIGN(pList[i], pList[i + 1], pTagInfo->tagsLen);
          i += 1;
        }
      } else {
        while (i + 1 < maxLen && pList[i + 1]->v.dKey < val.dKey) {
          VALUEPAIRASSIGN(pList[i], pList[i + 1], pTagInfo->tagsLen);
          i += 1;
        }
      }
      
      valuePairAssign(pList[i], type, (const char*) &val.i64Key, ts, pTags, pTagInfo, stage);
    }
  }
}

static void do_bottom_function_add(STopBotInfo *pInfo, int32_t maxLen, void *pData, int64_t ts, uint16_t type,
                                   SExtTagsInfo *pTagInfo, char *pTags, int16_t stage) {
  tValuePair **pList = pInfo->res;
  
  tVariant val = {0};
  tVariantCreateFromBinary(&val, pData, tDataTypeDesc[type].nSize, type);
  
  if (pInfo->num < maxLen) {
    if (pInfo->num == 0) {
      valuePairAssign(pList[pInfo->num], type, (const char*) &val.i64Key, ts, pTags, pTagInfo, stage);
    } else {
      int32_t i = pInfo->num - 1;
      
      if (type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) {
        while (i >= 0 && pList[i]->v.i64Key < val.i64Key) {
          VALUEPAIRASSIGN(pList[i + 1], pList[i], pTagInfo->tagsLen);
          i -= 1;
        }
      } else {
        while (i >= 0 && pList[i]->v.dKey < val.dKey) {
          VALUEPAIRASSIGN(pList[i + 1], pList[i], pTagInfo->tagsLen);
          i -= 1;
        }
      }
      
      valuePairAssign(pList[i + 1], type, (const char*)&val.i64Key, ts, pTags, pTagInfo, stage);
    }
    
    pInfo->num++;
  } else {
    int32_t i = 0;
    
    if (((type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) && val.i64Key < pList[0]->v.i64Key) ||
        ((type >= TSDB_DATA_TYPE_FLOAT && type <= TSDB_DATA_TYPE_DOUBLE) && val.dKey < pList[0]->v.dKey)) {
      // find the appropriate the slot position
      if (type >= TSDB_DATA_TYPE_TINYINT && type <= TSDB_DATA_TYPE_BIGINT) {
        while (i + 1 < maxLen && pList[i + 1]->v.i64Key > val.i64Key) {
          VALUEPAIRASSIGN(pList[i], pList[i + 1], pTagInfo->tagsLen);
          i += 1;
        }
      } else {
        while (i + 1 < maxLen && pList[i + 1]->v.dKey > val.dKey) {
          VALUEPAIRASSIGN(pList[i], pList[i + 1], pTagInfo->tagsLen);
          i += 1;
        }
      }
      
      valuePairAssign(pList[i], type, (const char*)&val.i64Key, ts, pTags, pTagInfo, stage);
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
  
  int32_t type = pLeftElem->v.nType;
  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
    if (pLeftElem->v.dKey == pRightElem->v.dKey) {
      return 0;
    } else {
      return pLeftElem->v.dKey > pRightElem->v.dKey ? 1 : -1;
    }
  } else {
    if (pLeftElem->v.i64Key == pRightElem->v.i64Key) {
      return 0;
    } else {
      return pLeftElem->v.i64Key > pRightElem->v.i64Key ? 1 : -1;
    }
  }
}

static int32_t resDataDescComparFn(const void *pLeft, const void *pRight) { return -resDataAscComparFn(pLeft, pRight); }

static void copyTopBotRes(SQLFunctionCtx *pCtx, int32_t type) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STopBotInfo *pRes = pResInfo->interResultBuf;
  
  tValuePair **tvp = pRes->res;
  
  int32_t step = QUERY_ASC_FORWARD_STEP;
  int32_t len = GET_RES_INFO(pCtx)->numOfRes;
  
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *output = (int32_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *output = (int64_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *output = (double *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.dKey;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *output = (float *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.dKey;
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *output = (int16_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *output = (int8_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i]->v.i64Key;
      }
      break;
    }
    default: {
      tscError("top/bottom function not support data type:%d", pCtx->inputType);
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
  char **pData = calloc(pCtx->tagInfo.numOfTagCols, POINTER_BYTES);
  for (int32_t i = 0; i < pCtx->tagInfo.numOfTagCols; ++i) {
    pData[i] = pCtx->tagInfo.pTagCtxList[i]->aOutputBuf;
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

bool top_bot_datablock_filter(SQLFunctionCtx *pCtx, int32_t functionId, char *minval, char *maxval) {
  STopBotInfo *pTopBotInfo = (STopBotInfo *)GET_RES_INFO(pCtx)->interResultBuf;
  
  int32_t numOfExistsRes = pTopBotInfo->num;
  
  // required number of results are not reached, continue load data block
  if (numOfExistsRes < pCtx->param[0].i64Key) {
    return true;
  }
  
  tValuePair *pRes = (tValuePair*) pTopBotInfo->res;
  
  if (functionId == TSDB_FUNC_TOP) {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(maxval) > pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(maxval) > pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(maxval) > pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(maxval) > pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(maxval) > pRes[0].v.dKey;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(maxval) > pRes[0].v.dKey;
      default:
        return true;
    }
  } else {
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        return GET_INT8_VAL(minval) < pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_SMALLINT:
        return GET_INT16_VAL(minval) < pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_INT:
        return GET_INT32_VAL(minval) < pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_BIGINT:
        return GET_INT64_VAL(minval) < pRes[0].v.i64Key;
      case TSDB_DATA_TYPE_FLOAT:
        return GET_FLOAT_VAL(minval) < pRes[0].v.dKey;
      case TSDB_DATA_TYPE_DOUBLE:
        return GET_DOUBLE_VAL(minval) < pRes[0].v.dKey;
      default:
        return true;
    }
  }
}

/*
 * Parameters values:
 * 1. param[0]: maximum allowable results
 * 2. param[1]: order by type (time or value)
 * 3. param[2]: asc/desc order
 *
 * top/bottom use the intermediate result buffer to keep the intermediate result
 */
static STopBotInfo *getTopBotOutputInfo(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  // only the first_stage_merge is directly written data into final output buffer
  if (pResInfo->superTableQ && pCtx->currentStage != SECONDARY_STAGE_MERGE) {
    return (STopBotInfo*) pCtx->aOutputBuf;
  } else {  // during normal table query and super table at the secondary_stage, result is written to intermediate buffer
    return pResInfo->interResultBuf;
  }
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
  
  tmp += POINTER_BYTES * pCtx->param[0].i64Key;
  
  size_t size = sizeof(tValuePair) + pCtx->tagInfo.tagsLen;
  
  for (int32_t i = 0; i < pCtx->param[0].i64Key; ++i) {
    pTopBotInfo->res[i] = (tValuePair*) tmp;
    pTopBotInfo->res[i]->pTags = tmp + sizeof(tValuePair);
    tmp += size;
  }
}

static bool top_bottom_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  STopBotInfo *pInfo = getTopBotOutputInfo(pCtx);
  buildTopBotStruct(pInfo, pCtx);
  
  return true;
}

static void top_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  assert(pRes->num >= 0);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;
    do_top_function_add(pRes, pCtx->param[0].i64Key, data, pCtx->ptsList[i], pCtx->inputType, &pCtx->tagInfo, NULL, 0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  assert(pRes->num >= 0);
  
  SET_VAL(pCtx, 1, 1);
  do_top_function_add(pRes, pCtx->param[0].i64Key, pData, pCtx->ptsList[index], pCtx->inputType, &pCtx->tagInfo, NULL,
                      0);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void top_func_merge(SQLFunctionCtx *pCtx) {
  char *input = GET_INPUT_CHAR(pCtx);
  
  STopBotInfo *pInput = (STopBotInfo *)input;
  if (pInput->num <= 0) {
    return;
  }
  
  // remmap the input buffer may cause the struct pointer invalid, so rebuild the STopBotInfo is necessary
  buildTopBotStruct(pInput, pCtx);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ && pCtx->outputType == TSDB_DATA_TYPE_BINARY && pCtx->size == 1);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  for (int32_t i = 0; i < pInput->num; ++i) {
    do_top_function_add(pOutput, pCtx->param[0].i64Key, &pInput->res[i]->v.i64Key, pInput->res[i]->timestamp,
                        pCtx->inputType, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
}

static void top_func_second_merge(SQLFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_CHAR(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    do_top_function_add(pOutput, pCtx->param[0].i64Key, &pInput->res[i]->v.i64Key, pInput->res[i]->timestamp,
                        pCtx->outputType, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
  
  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;
    do_bottom_function_add(pRes, pCtx->param[0].i64Key, data, pCtx->ptsList[i], pCtx->inputType, &pCtx->tagInfo, NULL,
                           0);
  }
  
  if (!pCtx->hasNull) {
    assert(pCtx->size == notNullElems);
  }
  
  // treat the result as only one result
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void bottom_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  STopBotInfo *pRes = getTopBotOutputInfo(pCtx);
  SET_VAL(pCtx, 1, 1);
  do_bottom_function_add(pRes, pCtx->param[0].i64Key, pData, pCtx->ptsList[index], pCtx->inputType, &pCtx->tagInfo,
                         NULL, 0);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void bottom_func_merge(SQLFunctionCtx *pCtx) {
  char *input = GET_INPUT_CHAR(pCtx);
  
  STopBotInfo *pInput = (STopBotInfo *)input;
  if (pInput->num <= 0) {
    return;
  }
  
  // remmap the input buffer may cause the struct pointer invalid, so rebuild the STopBotInfo is necessary
  buildTopBotStruct(pInput, pCtx);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ && pCtx->outputType == TSDB_DATA_TYPE_BINARY && pCtx->size == 1);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  for (int32_t i = 0; i < pInput->num; ++i) {
    do_bottom_function_add(pOutput, pCtx->param[0].i64Key, &pInput->res[i]->v.i64Key, pInput->res[i]->timestamp,
                           pCtx->inputType, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
}

static void bottom_func_second_merge(SQLFunctionCtx *pCtx) {
  STopBotInfo *pInput = (STopBotInfo *)GET_INPUT_CHAR(pCtx);
  
  // construct the input data struct from binary data
  buildTopBotStruct(pInput, pCtx);
  
  STopBotInfo *pOutput = getTopBotOutputInfo(pCtx);
  
  // the intermediate result is binary, we only use the output data type
  for (int32_t i = 0; i < pInput->num; ++i) {
    do_bottom_function_add(pOutput, pCtx->param[0].i64Key, &pInput->res[i]->v.i64Key, pInput->res[i]->timestamp,
                           pCtx->outputType, &pCtx->tagInfo, pInput->res[i]->pTags, pCtx->currentStage);
  }
  
  SET_VAL(pCtx, pInput->num, pOutput->num);
  
  if (pOutput->num > 0) {
    SResultInfo *pResInfo = GET_RES_INFO(pCtx);
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void top_bottom_func_finalizer(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  // data in temporary list is less than the required number of results, not enough qualified number of results
  STopBotInfo *pRes = pResInfo->interResultBuf;
  if (pRes->num == 0) {  // no result
    assert(pResInfo->hasResult != DATA_SET_FLAG);
    // TODO:
  }
  
  GET_RES_INFO(pCtx)->numOfRes = pRes->num;
  tValuePair **tvp = pRes->res;
  
  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[1].i64Key == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    __compar_fn_t comparator = (pCtx->param[2].i64Key == TSDB_ORDER_ASC) ? resAscComparFn : resDescComparFn;
    qsort(tvp, pResInfo->numOfRes, POINTER_BYTES, comparator);
  } else if (pCtx->param[1].i64Key > PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    __compar_fn_t comparator = (pCtx->param[2].i64Key == TSDB_ORDER_ASC) ? resDataAscComparFn : resDataDescComparFn;
    qsort(tvp, pResInfo->numOfRes, POINTER_BYTES, comparator);
  }
  
  GET_TRUE_DATA_TYPE();
  copyTopBotRes(pCtx, type);
  
  doFinalizer(pCtx);
}

///////////////////////////////////////////////////////////////////////////////////////////////
static bool percentile_function_setup(SQLFunctionCtx *pCtx) {
  const int32_t MAX_AVAILABLE_BUFFER_SIZE = 1 << 20;  // 1MB
  const int32_t NUMOFCOLS = 1;
  
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SSchema      field[1] = {{pCtx->inputType, "dummyCol", 0, pCtx->inputBytes}};
  
  SColumnModel *pModel = createColumnModel(field, 1, 1000);
  int32_t    orderIdx = 0;
  
  // tOrderDesc object
  tOrderDescriptor *pDesc = tOrderDesCreate(&orderIdx, NUMOFCOLS, pModel, TSDB_ORDER_DESC);
  
  ((SPercentileInfo *)(pResInfo->interResultBuf))->pMemBucket =
      tMemBucketCreate(1024, MAX_AVAILABLE_BUFFER_SIZE, pCtx->inputBytes, pCtx->inputType, pDesc);
  
  return true;
}

static void percentile_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  SResultInfo *    pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo *pInfo = pResInfo->interResultBuf;
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;
    tMemBucketPut(pInfo->pMemBucket, data, 1);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void percentile_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  SPercentileInfo *pInfo = (SPercentileInfo *)pResInfo->interResultBuf;
  tMemBucketPut(pInfo->pMemBucket, pData, 1);
  
  SET_VAL(pCtx, 1, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void percentile_finalizer(SQLFunctionCtx *pCtx) {
  double v = pCtx->param[0].nType == TSDB_DATA_TYPE_INT ? pCtx->param[0].i64Key : pCtx->param[0].dKey;
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  tMemBucket * pMemBucket = ((SPercentileInfo *)pResInfo->interResultBuf)->pMemBucket;
  
  if (pMemBucket->numOfElems > 0) {  // check for null
    *(double *)pCtx->aOutputBuf = getPercentile(pMemBucket, v);
  } else {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
  }
  
  tOrderDescDestroy(pMemBucket->pOrderDesc);
  tMemBucketDestroy(pMemBucket);
  
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////
static SAPercentileInfo *getAPerctInfo(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pResInfo->superTableQ && pCtx->currentStage != SECONDARY_STAGE_MERGE) {
    return (SAPercentileInfo*) pCtx->aOutputBuf;
  } else {
    return pResInfo->interResultBuf;
  }
}

static bool apercentile_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SAPercentileInfo *pInfo = getAPerctInfo(pCtx);
  
  char *tmp = (char *)pInfo + sizeof(SAPercentileInfo);
  pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
  return true;
}

static void apercentile_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pInfo = getAPerctInfo(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(data, pCtx->inputType)) {
      continue;
    }
    
    notNullElems += 1;
    double v = 0;
    
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        v = GET_INT8_VAL(data);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        v = GET_INT16_VAL(data);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        v = GET_INT64_VAL(data);
        break;
      case TSDB_DATA_TYPE_FLOAT:
        v = GET_FLOAT_VAL(data);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        v = GET_DOUBLE_VAL(data);
        break;
      default:
        v = GET_INT32_VAL(data);
        break;
    }
    
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

static void apercentile_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pInfo = getAPerctInfo(pCtx);  // pResInfo->interResultBuf;
  
  double v = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT:
      v = GET_INT8_VAL(pData);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      v = GET_INT16_VAL(pData);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      v = GET_INT64_VAL(pData);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      v = GET_FLOAT_VAL(pData);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      v = GET_DOUBLE_VAL(pData);
      break;
    default:
      v = GET_INT32_VAL(pData);
      break;
  }
  
  tHistogramAdd(&pInfo->pHisto, v);
  
  SET_VAL(pCtx, 1, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void apercentile_func_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  SAPercentileInfo *pInput = (SAPercentileInfo *)GET_INPUT_CHAR(pCtx);
  
  pInput->pHisto = (SHistogramInfo*) ((char *)pInput + sizeof(SAPercentileInfo));
  pInput->pHisto->elems = (SHistBin*) ((char *)pInput->pHisto + sizeof(SHistogramInfo));
  
  if (pInput->pHisto->numOfElems <= 0) {
    return;
  }
  
  size_t size = sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1);
  
  SAPercentileInfo *pOutput = getAPerctInfo(pCtx);  //(SAPercentileInfo *)pCtx->aOutputBuf;
  SHistogramInfo *  pHisto = pOutput->pHisto;
  
  if (pHisto->numOfElems <= 0) {
    memcpy(pHisto, pInput->pHisto, size);
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
  } else {
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    
    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
    memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    
    tHistogramDestroy(&pRes);
  }
  
  SET_VAL(pCtx, 1, 1);
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void apercentile_func_second_merge(SQLFunctionCtx *pCtx) {
  SAPercentileInfo *pInput = (SAPercentileInfo *)GET_INPUT_CHAR(pCtx);
  
  pInput->pHisto = (SHistogramInfo*) ((char *)pInput + sizeof(SAPercentileInfo));
  pInput->pHisto->elems = (SHistBin*) ((char *)pInput->pHisto + sizeof(SHistogramInfo));
  
  if (pInput->pHisto->numOfElems <= 0) {
    return;
  }
  
  SAPercentileInfo *pOutput = getAPerctInfo(pCtx);
  SHistogramInfo *  pHisto = pOutput->pHisto;
  
  if (pHisto->numOfElems <= 0) {
    memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
  } else {
    pHisto->elems = (SHistBin*) ((char *)pHisto + sizeof(SHistogramInfo));
    
    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
    tHistogramDestroy(&pOutput->pHisto);
    pOutput->pHisto = pRes;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  pResInfo->hasResult = DATA_SET_FLAG;
  SET_VAL(pCtx, 1, 1);
}

static void apercentile_finalizer(SQLFunctionCtx *pCtx) {
  double v = (pCtx->param[0].nType == TSDB_DATA_TYPE_INT) ? pCtx->param[0].i64Key : pCtx->param[0].dKey;
  
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo *pOutput = pResInfo->interResultBuf;
  
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    if (pResInfo->hasResult == DATA_SET_FLAG) {  // check for null
      assert(pOutput->pHisto->numOfElems > 0);
      
      double  ratio[] = {v};
      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
      
      memcpy(pCtx->aOutputBuf, res, sizeof(double));
      free(res);
    } else {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  } else {
    if (pOutput->pHisto->numOfElems > 0) {
      double ratio[] = {v};
      
      double *res = tHistogramUniform(pOutput->pHisto, ratio, 1);
      memcpy(pCtx->aOutputBuf, res, sizeof(double));
      free(res);
    } else {  // no need to free
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
  }
  
  doFinalizer(pCtx);
}

/////////////////////////////////////////////////////////////////////////////////
static bool leastsquares_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquareInfo *pInfo = pResInfo->interResultBuf;
  
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
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquareInfo *pInfo = pResInfo->interResultBuf;
  
  double(*param)[3] = pInfo->mat;
  double x = pInfo->startVal;
  
  void *pData = GET_INPUT_CHAR(pCtx);
  
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
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      float *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *p = pData;
      LEASTSQR_CAL_LOOP(pCtx, param, x, p, pCtx->inputType, numOfElem, pCtx->param[1].dKey);
      break;
    };
  }
  
  pInfo->startVal = x;
  pInfo->num += numOfElem;
  
  if (pInfo->num > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  SET_VAL(pCtx, numOfElem, 1);
}

static void leastsquares_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquareInfo *pInfo = pResInfo->interResultBuf;
  
  double(*param)[3] = pInfo->mat;
  
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *p = pData;
      LEASTSQR_CAL(param, pInfo->startVal, p, index, pCtx->param[1].dKey);
      break;
    }
    default:
      tscError("error data type in leastsquare function:%d", pCtx->inputType);
  };
  
  SET_VAL(pCtx, 1, 1);
  pInfo->num += 1;
  
  if (pInfo->num > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

static void leastsquares_finalizer(SQLFunctionCtx *pCtx) {
  // no data in query
  SResultInfo *     pResInfo = GET_RES_INFO(pCtx);
  SLeastsquareInfo *pInfo = pResInfo->interResultBuf;
  
  if (pInfo->num == 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
    return;
  }
  
  double(*param)[3] = pInfo->mat;
  
  param[1][1] = pInfo->num;
  param[1][0] = param[0][1];
  
  param[0][0] -= param[1][0] * (param[0][1] / param[1][1]);
  param[0][2] -= param[1][2] * (param[0][1] / param[1][1]);
  param[0][1] = 0;
  param[1][2] -= param[0][2] * (param[1][0] / param[0][0]);
  param[1][0] = 0;
  param[0][2] /= param[0][0];
  
  param[1][2] /= param[1][1];
  
  int32_t maxOutputSize = TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE - VARSTR_HEADER_SIZE;
  size_t n = snprintf(varDataVal(pCtx->aOutputBuf), maxOutputSize, "{slop:%.6lf, intercept:%.6lf}",
      param[0][2], param[1][2]);
  
  varDataSetLen(pCtx->aOutputBuf, n);
  doFinalizer(pCtx);
}

static void date_col_output_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  *(int64_t *)(pCtx->aOutputBuf) = pCtx->nStartQueryTimestamp;
}

static FORCE_INLINE void date_col_output_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  date_col_output_function(pCtx);
}

static void col_project_function(SQLFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size);
  
  char *pData = GET_INPUT_CHAR(pCtx);
  if (pCtx->order == TSDB_ORDER_ASC) {
    memcpy(pCtx->aOutputBuf, pData, (size_t)pCtx->size * pCtx->inputBytes);
  } else {
    for(int32_t i = 0; i < pCtx->size; ++i) {
      memcpy(pCtx->aOutputBuf + (pCtx->size - 1 - i) * pCtx->inputBytes, pData + i * pCtx->inputBytes,
             pCtx->inputBytes);
    }
  }
  
  pCtx->aOutputBuf += pCtx->size * pCtx->outputBytes;
}

static void col_project_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  // only one output
  if (pCtx->param[0].i64Key == 1 && pResInfo->numOfRes >= 1) {
    return;
  }
  
  INC_INIT_VAL(pCtx, 1);
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);
  
  pCtx->aOutputBuf += pCtx->inputBytes;
}

/**
 * only used for tag projection query in select clause
 * @param pCtx
 * @return
 */
static void tag_project_function(SQLFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size);
  
  assert(pCtx->inputBytes == pCtx->outputBytes);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char* output = pCtx->aOutputBuf;
  
    if (pCtx->tag.nType == TSDB_DATA_TYPE_BINARY || pCtx->tag.nType == TSDB_DATA_TYPE_NCHAR) {
      varDataSetLen(output, pCtx->tag.nLen);
      tVariantDump(&pCtx->tag, varDataVal(output), pCtx->outputType);
    } else {
      tVariantDump(&pCtx->tag, output, pCtx->outputType);
    }
    
    pCtx->aOutputBuf += pCtx->outputBytes;
  }
}

static void tag_project_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  INC_INIT_VAL(pCtx, 1);
  
  char* output = pCtx->aOutputBuf;
  if (pCtx->tag.nType == TSDB_DATA_TYPE_BINARY || pCtx->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    *(int16_t*) output = pCtx->tag.nLen;
    output += VARSTR_HEADER_SIZE;
  }
  
  tVariantDump(&pCtx->tag, output, pCtx->tag.nType);
  pCtx->aOutputBuf += pCtx->outputBytes;
}

/**
 * used in group by clause. when applying group by tags, the tags value is
 * assign by using tag function.
 * NOTE: there is only ONE output for ONE query range
 * @param pCtx
 * @return
 */
static void tag_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, 1, 1);
  
  char* output = pCtx->aOutputBuf;
  
  // todo refactor to dump length presented string(var string)
  if (pCtx->tag.nType == TSDB_DATA_TYPE_BINARY || pCtx->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    *(int16_t*) output = pCtx->tag.nLen;
    output += VARSTR_HEADER_SIZE;
  }
  
  tVariantDump(&pCtx->tag, output, pCtx->tag.nType);
}

static void tag_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  SET_VAL(pCtx, 1, 1);
  
  char* output = pCtx->aOutputBuf;
  
  // todo refactor to dump length presented string(var string)
  if (pCtx->tag.nType == TSDB_DATA_TYPE_BINARY || pCtx->tag.nType == TSDB_DATA_TYPE_NCHAR) {
    *(int16_t*) output = pCtx->tag.nLen;
    output += VARSTR_HEADER_SIZE;
  }
  
  tVariantDump(&pCtx->tag, output, pCtx->tag.nType);
}

static void copy_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);
  
  char *pData = GET_INPUT_CHAR(pCtx);
  assignVal(pCtx->aOutputBuf, pData, pCtx->inputBytes, pCtx->inputType);
}

enum {
  INITIAL_VALUE_NOT_ASSIGNED = 0,
};

static bool diff_function_setup(SQLFunctionCtx *pCtx) {
  if (function_setup(pCtx)) {
    return false;
  }
  
  // diff function require the value is set to -1
  pCtx->param[1].nType = INITIAL_VALUE_NOT_ASSIGNED;
  return false;
}

// TODO difference in date column
static void diff_function(SQLFunctionCtx *pCtx) {
  void *data = GET_INPUT_CHAR(pCtx);
  bool  isFirstBlock = (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED);
  
  int32_t notNullElems = 0;
  
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  int32_t i = (pCtx->order == TSDB_ORDER_ASC) ? 0 : pCtx->size - 1;
  
  TSKEY * pTimestamp = pCtx->ptsOutputBuf;
  
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pData = (int32_t *)data;
      int32_t *pOutput = (int32_t *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].i64Key = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;  // direct previous may be null
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        }
        
        pCtx->param[1].i64Key = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pData = (int64_t *)data;
      int64_t *pOutput = (int64_t *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].i64Key = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        }
        
        pCtx->param[1].i64Key = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double *pData = (double *)data;
      double *pOutput = (double *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].dKey = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].dKey;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += 1;
          pTimestamp += 1;
        }
        
        pCtx->param[1].dKey = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      float *pData = (float *)data;
      float *pOutput = (float *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].dKey = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].dKey;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        }
        
        // keep the last value, the remain may be all null
        pCtx->param[1].dKey = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pData = (int16_t *)data;
      int16_t *pOutput = (int16_t *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((const char*) &pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].i64Key = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        }
        
        pCtx->param[1].i64Key = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pData = (int8_t *)data;
      int8_t *pOutput = (int8_t *)pCtx->aOutputBuf;
      
      for (; i < pCtx->size && i >= 0; i += step) {
        if (pCtx->hasNull && isNull((char *)&pData[i], pCtx->inputType)) {
          continue;
        }
        
        if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->param[1].i64Key = pData[i];
          pCtx->param[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSDB_ORDER_ASC) || (i == pCtx->size - 1 && pCtx->order == TSDB_ORDER_DESC)) {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        } else {
          *pOutput = pData[i] - pCtx->param[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          
          pOutput += 1;
          pTimestamp += 1;
        }
        
        pCtx->param[1].i64Key = pData[i];
        pCtx->param[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    default:
      tscError("error input type");
  }
  
  // initial value is not set yet
  if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED || notNullElems <= 0) {
    /*
     * 1. current block and blocks before are full of null
     * 2. current block may be null value
     */
    assert(pCtx->hasNull);
  } else {
    int32_t forwardStep = (isFirstBlock) ? notNullElems - 1 : notNullElems;
    
    GET_RES_INFO(pCtx)->numOfRes += forwardStep;
    
    pCtx->aOutputBuf += forwardStep * pCtx->outputBytes;
    pCtx->ptsOutputBuf = (char*)pCtx->ptsOutputBuf + forwardStep * TSDB_KEYSIZE;
  }
}

#define DIFF_IMPL(ctx, d, type)                                                              \
  do {                                                                                       \
    if ((ctx)->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {                               \
      (ctx)->param[1].nType = (ctx)->inputType;                                              \
      *(type *)&(ctx)->param[1].i64Key = *(type *)(d);                                       \
    } else {                                                                                 \
      *(type *)(ctx)->aOutputBuf = *(type *)(d) - (*(type *)(&(ctx)->param[1].i64Key));      \
      *(type *)(&(ctx)->param[1].i64Key) = *(type *)(d);                                     \
      *(int64_t *)(ctx)->ptsOutputBuf = (ctx)->ptsList[index];                               \
    }                                                                                        \
  } while (0);

static void diff_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  // the output start from the second source element
  if (pCtx->param[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is set
    GET_RES_INFO(pCtx)->numOfRes += 1;
  }
  
  int32_t step = 1/*GET_FORWARD_DIRECTION_FACTOR(pCtx->order)*/;
  
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      if (pCtx->param[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
        pCtx->param[1].nType = pCtx->inputType;
        pCtx->param[1].i64Key = *(int32_t *)pData;
      } else {
        *(int32_t *)pCtx->aOutputBuf = *(int32_t *)pData - pCtx->param[1].i64Key;
        pCtx->param[1].i64Key = *(int32_t *)pData;
        *(int64_t *)pCtx->ptsOutputBuf = pCtx->ptsList[index];
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      DIFF_IMPL(pCtx, pData, int64_t);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      DIFF_IMPL(pCtx, pData, double);
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      DIFF_IMPL(pCtx, pData, float);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      DIFF_IMPL(pCtx, pData, int16_t);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      DIFF_IMPL(pCtx, pData, int8_t);
      break;
    };
    default:
      tscError("error input type");
  }
  
  if (GET_RES_INFO(pCtx)->numOfRes > 0) {
    pCtx->aOutputBuf += pCtx->outputBytes * step;
    pCtx->ptsOutputBuf = (char *)pCtx->ptsOutputBuf + TSDB_KEYSIZE * step;
  }
}

char *getArithColumnData(void *param, const char* name, int32_t colId) {
  SArithmeticSupport *pSupport = (SArithmeticSupport *)param;
  
  int32_t index = -1;
  for (int32_t i = 0; i < pSupport->numOfCols; ++i) {
    if (colId == pSupport->colList[i].colId) {
      index = i;
      break;
    }
  }
  
  assert(index >= 0 && colId >= 0);
  return pSupport->data[index] + pSupport->offset * pSupport->colList[index].bytes;
}

static void arithmetic_function(SQLFunctionCtx *pCtx) {
  GET_RES_INFO(pCtx)->numOfRes += pCtx->size;
  SArithmeticSupport *sas = (SArithmeticSupport *)pCtx->param[1].pz;
  
  tExprTreeCalcTraverse(sas->pArithExpr->pExpr, pCtx->size, pCtx->aOutputBuf, sas, pCtx->order, getArithColumnData);
  
  pCtx->aOutputBuf += pCtx->outputBytes * pCtx->size;
  pCtx->param[1].pz = NULL;
}

static void arithmetic_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  INC_INIT_VAL(pCtx, 1);
  SArithmeticSupport *sas = (SArithmeticSupport *)pCtx->param[1].pz;
  
  sas->offset = index;
  tExprTreeCalcTraverse(sas->pArithExpr->pExpr, 1, pCtx->aOutputBuf, sas, pCtx->order, getArithColumnData);
  
  pCtx->aOutputBuf += pCtx->outputBytes;
}

#define LIST_MINMAX_N(ctx, minOutput, maxOutput, elemCnt, data, type, tsdbType, numOfNotNullElem) \
  {                                                                                               \
    type *inputData = (type *)data;                                                               \
    for (int32_t i = 0; i < elemCnt; ++i) {                                                       \
      if ((ctx)->hasNull && isNull((char *)&inputData[i], tsdbType)) {                            \
        continue;                                                                                 \
      }                                                                                           \
      if (inputData[i] < minOutput) {                                                             \
        minOutput = inputData[i];                                                                 \
      }                                                                                           \
      if (inputData[i] > maxOutput) {                                                             \
        maxOutput = inputData[i];                                                                 \
      }                                                                                           \
      numOfNotNullElem++;                                                                         \
    }                                                                                             \
  }

/////////////////////////////////////////////////////////////////////////////////
static bool spread_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SSpreadInfo *pInfo = GET_RES_INFO(pCtx)->interResultBuf;
  
  // this is the server-side setup function in client-side, the secondary merge do not need this procedure
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    pCtx->param[0].dKey = DBL_MAX;
    pCtx->param[3].dKey = -DBL_MAX;
  } else {
    pInfo->min = DBL_MAX;
    pInfo->max = -DBL_MAX;
  }
  
  return true;
}

static void spread_function(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo *pInfo = pResInfo->interResultBuf;
  
  int32_t numOfElems = pCtx->size;
  
  // todo : opt with pre-calculated result
  // column missing cause the hasNull to be true
  if (pCtx->preAggVals.isSet) {
    numOfElems = pCtx->size - pCtx->preAggVals.statis.numOfNull;
    
    // all data are null in current data block, ignore current data block
    if (numOfElems == 0) {
      goto _spread_over;
    }
    
    if ((pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) ||
        (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP)) {
      if (pInfo->min > pCtx->preAggVals.statis.min) {
        pInfo->min = pCtx->preAggVals.statis.min;
      }
      
      if (pInfo->max < pCtx->preAggVals.statis.max) {
        pInfo->max = pCtx->preAggVals.statis.max;
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      if (pInfo->min > GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.min))) {
        pInfo->min = GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.min));
      }
      
      if (pInfo->max < GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.max))) {
        pInfo->max = GET_DOUBLE_VAL(&(pCtx->preAggVals.statis.max));
      }
    }
    
    goto _spread_over;
  }
  
  void *pData = GET_INPUT_CHAR(pCtx);
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
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SSpreadInfo));
  }
}

static void spread_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo *pInfo = pResInfo->interResultBuf;
  
  double val = 0.0;
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    val = GET_INT8_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    val = GET_INT16_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    val = GET_INT32_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT || pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
    val = GET_INT64_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    val = GET_DOUBLE_VAL(pData);
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    val = GET_FLOAT_VAL(pData);
  }
  
  // keep the result data in output buffer, not in the intermediate buffer
  if (val > pInfo->max) {
    pInfo->max = val;
  }
  
  if (val < pInfo->min) {
    pInfo->min = val;
  }
  
  pResInfo->hasResult = DATA_SET_FLAG;
  pInfo->hasResult = DATA_SET_FLAG;
  
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SSpreadInfo));
  }
}

void spread_func_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  SSpreadInfo *pResData = pResInfo->interResultBuf;
  
  int32_t notNullElems = 0;
  for (int32_t i = 0; i < pCtx->size; ++i) {
    SSpreadInfo *input = (SSpreadInfo *)GET_INPUT_CHAR_INDEX(pCtx, i);
    
    /* no assign tag, the value is null */
    if (input->hasResult != DATA_SET_FLAG) {
      continue;
    }
    
    if (pResData->min > input->min) {
      pResData->min = input->min;
    }
    
    if (pResData->max < input->max) {
      pResData->max = input->max;
    }
    
    pResData->hasResult = DATA_SET_FLAG;
    notNullElems++;
  }
  
  if (notNullElems > 0) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SSpreadInfo));
    pResInfo->hasResult = DATA_SET_FLAG;
  }
}

/*
 * here we set the result value back to the intermediate buffer, to apply the finalize the function
 * the final result is generated in spread_function_finalizer
 */
void spread_func_sec_merge(SQLFunctionCtx *pCtx) {
  SSpreadInfo *pData = (SSpreadInfo *)GET_INPUT_CHAR(pCtx);
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
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
    
    if (pResInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
    
    *(double *)pCtx->aOutputBuf = pCtx->param[3].dKey - pCtx->param[0].dKey;
  } else {
    assert((pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_DOUBLE) ||
        (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP));
    
    SSpreadInfo *pInfo = GET_RES_INFO(pCtx)->interResultBuf;
    if (pInfo->hasResult != DATA_SET_FLAG) {
      setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      return;
    }
    
    *(double *)pCtx->aOutputBuf = pInfo->max - pInfo->min;
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;  // todo add test case
  doFinalizer(pCtx);
}


/**
 * param[1]: start time
 * param[2]: end time
 * @param pCtx
 */
static bool twa_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);  //->aOutputBuf + pCtx->outputBytes;
  STwaInfo *   pInfo = pResInfo->interResultBuf;
  
  pInfo->lastKey = INT64_MIN;
  pInfo->type = pCtx->inputType;
  
  return true;
}

static FORCE_INLINE void setTWALastVal(SQLFunctionCtx *pCtx, const char *data, int32_t i, STwaInfo *pInfo) {
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT:
      pInfo->iLastValue = GET_INT32_VAL(data + pCtx->inputBytes * i);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pInfo->iLastValue = GET_INT8_VAL(data + pCtx->inputBytes * i);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pInfo->iLastValue = GET_INT16_VAL(data + pCtx->inputBytes * i);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pInfo->iLastValue = GET_INT64_VAL(data + pCtx->inputBytes * i);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pInfo->dLastValue = GET_FLOAT_VAL(data + pCtx->inputBytes * i);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pInfo->dLastValue = GET_DOUBLE_VAL(data + pCtx->inputBytes * i);
      break;
    default:
      assert(0);
  }
}

static void twa_function(SQLFunctionCtx *pCtx) {
  void * data = GET_INPUT_CHAR(pCtx);
  TSKEY *primaryKey = pCtx->ptsList;
  
  int32_t notNullElems = 0;
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STwaInfo *   pInfo = pResInfo->interResultBuf;
  
  int32_t i = 0;
  
  // skip null value
  while (pCtx->hasNull && i < pCtx->size && isNull((char *)data + pCtx->inputBytes * i, pCtx->inputType)) {
    i++;
  }
  
  if (i >= pCtx->size) {
    return;
  }
  
  if (pInfo->lastKey == INT64_MIN) {
    pInfo->lastKey = pCtx->nStartQueryTimestamp;
    setTWALastVal(pCtx, data, i, pInfo);
    
    pInfo->hasResult = DATA_SET_FLAG;
  }
  
  notNullElems++;
  
  if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT || pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    pInfo->dOutput += pInfo->dLastValue * (primaryKey[i] - pInfo->lastKey);
  } else {
    pInfo->iOutput += pInfo->iLastValue * (primaryKey[i] - pInfo->lastKey);
  }
  
  pInfo->lastKey = primaryKey[i];
  setTWALastVal(pCtx, data, i, pInfo);
  
  for (++i; i < pCtx->size; i++) {
    if (pCtx->hasNull && isNull((char *)data + pCtx->inputBytes * i, pCtx->inputType)) {
      continue;
    }
    
    notNullElems++;
    if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT || pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      pInfo->dOutput += pInfo->dLastValue * (primaryKey[i] - pInfo->lastKey);
    } else {
      pInfo->iOutput += pInfo->iLastValue * (primaryKey[i] - pInfo->lastKey);
    }
    
    pInfo->lastKey = primaryKey[i];
    setTWALastVal(pCtx, data, i, pInfo);
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pResInfo->hasResult = DATA_SET_FLAG;
  }
  
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pInfo, sizeof(STwaInfo));
  }
  
  //  pCtx->numOfIteratedElems += notNullElems;
}

static void twa_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SET_VAL(pCtx, 1, 1);
  
  TSKEY *primaryKey = pCtx->ptsList;
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STwaInfo *pInfo = pResInfo->interResultBuf;
  
  if (pInfo->lastKey == INT64_MIN) {
    pInfo->lastKey = pCtx->nStartQueryTimestamp;
    setTWALastVal(pCtx, pData, 0, pInfo);
    
    pInfo->hasResult = DATA_SET_FLAG;
  }
  
  if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT || pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    pInfo->dOutput += pInfo->dLastValue * (primaryKey[index] - pInfo->lastKey);
  } else {
    pInfo->iOutput += pInfo->iLastValue * (primaryKey[index] - pInfo->lastKey);
  }
  
  // record the last key/value
  pInfo->lastKey = primaryKey[index];
  setTWALastVal(pCtx, pData, 0, pInfo);
  
  //  pCtx->numOfIteratedElems += 1;
  pResInfo->hasResult = DATA_SET_FLAG;
  
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(STwaInfo));
  }
}

static void twa_func_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  STwaInfo *pBuf = (STwaInfo *)pCtx->aOutputBuf;
  char *    indicator = pCtx->aInputElemBuf;
  
  int32_t numOfNotNull = 0;
  for (int32_t i = 0; i < pCtx->size; ++i, indicator += sizeof(STwaInfo)) {
    STwaInfo *pInput = (STwaInfo*) indicator;
    
    if (pInput->hasResult != DATA_SET_FLAG) {
      continue;
    }
    
    numOfNotNull++;
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      pBuf->iOutput += pInput->iOutput;
    } else {
      pBuf->dOutput += pInput->dOutput;
    }
    
    pBuf->SKey = pInput->SKey;
    pBuf->EKey = pInput->EKey;
    pBuf->lastKey = pInput->lastKey;
    pBuf->iLastValue = pInput->iLastValue;
  }
  
  SET_VAL(pCtx, numOfNotNull, 1);
  
  if (numOfNotNull > 0) {
    pBuf->hasResult = DATA_SET_FLAG;
  }
}

/*
 * To copy the input to interResBuf to avoid the input buffer space be over writen
 * by next input data. The TWA function only applies to each table, so no merge procedure
 * is required, we simply copy to the resut ot interResBuffer.
 */
void twa_function_copy(SQLFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  memcpy(pResInfo->interResultBuf, pCtx->aInputElemBuf, (size_t)pCtx->inputBytes);
  pResInfo->hasResult = ((STwaInfo *)pCtx->aInputElemBuf)->hasResult;
}

void twa_function_finalizer(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STwaInfo *pInfo = (STwaInfo *)pResInfo->interResultBuf;
  assert(pInfo->EKey >= pInfo->lastKey && pInfo->hasResult == pResInfo->hasResult);
  
  if (pInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->aOutputBuf, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }
  
  if (pInfo->SKey == pInfo->EKey) {
    *(double *)pCtx->aOutputBuf = 0;
  } else if (pInfo->type >= TSDB_DATA_TYPE_TINYINT && pInfo->type <= TSDB_DATA_TYPE_BIGINT) {
    pInfo->iOutput += pInfo->iLastValue * (pInfo->EKey - pInfo->lastKey);
    *(double *)pCtx->aOutputBuf = pInfo->iOutput / (double)(pInfo->EKey - pInfo->SKey);
  } else {
    pInfo->dOutput += pInfo->dLastValue * (pInfo->EKey - pInfo->lastKey);
    *(double *)pCtx->aOutputBuf = pInfo->dOutput / (pInfo->EKey - pInfo->SKey);
  }
  
  GET_RES_INFO(pCtx)->numOfRes = 1;
  doFinalizer(pCtx);
}

/**
 * param[1]: default value/previous value of specified timestamp
 * param[2]: next value of specified timestamp
 * param[3]: denotes if the result is a precious result or interpolation results
 *
 * @param pCtx
 */
static void interp_function(SQLFunctionCtx *pCtx) {
  // at this point, the value is existed, return directly
  if (pCtx->param[3].i64Key == 1) {
    char *pData = GET_INPUT_CHAR(pCtx);
    assignVal(pCtx->aOutputBuf, pData, pCtx->inputBytes, pCtx->inputType);
  } else {
    /*
     * use interpolation to generate the result.
     * Note: the result of primary timestamp column uses the timestamp specified by user in the query sql
     */
    assert(pCtx->param[3].i64Key == 2);
    
    SInterpInfo        interpInfo = *(SInterpInfo *)pCtx->aOutputBuf;
    SInterpInfoDetail *pInfoDetail = interpInfo.pInterpDetail;
    
    /* set no output result */
    if (pInfoDetail->type == TSDB_FILL_NONE) {
      pCtx->param[3].i64Key = 0;
    } else if (pInfoDetail->primaryCol == 1) {
      *(TSKEY *)pCtx->aOutputBuf = pInfoDetail->ts;
    } else {
      if (pInfoDetail->type == TSDB_FILL_NULL) {
        setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      } else if (pInfoDetail->type == TSDB_FILL_SET_VALUE) {
        tVariantDump(&pCtx->param[1], pCtx->aOutputBuf, pCtx->inputType);
      } else if (pInfoDetail->type == TSDB_FILL_PREV) {
        char *data = pCtx->param[1].pz;
        char *pVal = data + TSDB_KEYSIZE;
        
        if (pCtx->outputType == TSDB_DATA_TYPE_FLOAT) {
          float v = GET_DOUBLE_VAL(pVal);
          assignVal(pCtx->aOutputBuf, (const char*) &v, pCtx->outputBytes, pCtx->outputType);
        } else {
          assignVal(pCtx->aOutputBuf, pVal, pCtx->outputBytes, pCtx->outputType);
        }
        
      } else if (pInfoDetail->type == TSDB_FILL_LINEAR) {
        char *data1 = pCtx->param[1].pz;
        char *data2 = pCtx->param[2].pz;
        
        char *pVal1 = data1 + TSDB_KEYSIZE;
        char *pVal2 = data2 + TSDB_KEYSIZE;
        
        SPoint point1 = {.key = *(TSKEY *)data1, .val = &pCtx->param[1].i64Key};
        SPoint point2 = {.key = *(TSKEY *)data2, .val = &pCtx->param[2].i64Key};
        
        SPoint point = {.key = pInfoDetail->ts, .val = pCtx->aOutputBuf};
        
        int32_t srcType = pCtx->inputType;
        if ((srcType >= TSDB_DATA_TYPE_TINYINT && srcType <= TSDB_DATA_TYPE_BIGINT) ||
            srcType == TSDB_DATA_TYPE_TIMESTAMP || srcType == TSDB_DATA_TYPE_DOUBLE) {
          point1.val = pVal1;
          
          point2.val = pVal2;
          
          if (isNull(pVal1, srcType) || isNull(pVal2, srcType)) {
            setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
          } else {
            taosDoLinearInterpolation(pCtx->outputType, &point1, &point2, &point);
          }
        } else if (srcType == TSDB_DATA_TYPE_FLOAT) {
          float v1 = GET_DOUBLE_VAL(pVal1);
          float v2 = GET_DOUBLE_VAL(pVal2);
          
          point1.val = &v1;
          point2.val = &v2;
          
          if (isNull(pVal1, srcType) || isNull(pVal2, srcType)) {
            setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
          } else {
            taosDoLinearInterpolation(pCtx->outputType, &point1, &point2, &point);
          }
          
        } else {
          setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
        }
      }
    }
    
    free(interpInfo.pInterpDetail);
  }
  
  pCtx->size = pCtx->param[3].i64Key;
  
  tVariantDestroy(&pCtx->param[1]);
  tVariantDestroy(&pCtx->param[2]);
  
  // data in the check operation are all null, not output
  SET_VAL(pCtx, pCtx->size, 1);
}

static bool ts_comp_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;  // not initialized since it has been initialized
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STSCompInfo *pInfo = pResInfo->interResultBuf;
  
  pInfo->pTSBuf = tsBufCreate(false);
  pInfo->pTSBuf->tsOrder = pCtx->order;
  return true;
}

static void ts_comp_function(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STSBuf *     pTSbuf = ((STSCompInfo *)(pResInfo->interResultBuf))->pTSBuf;
  
  const char *input = GET_INPUT_CHAR(pCtx);
  
  // primary ts must be existed, so no need to check its existance
  if (pCtx->order == TSDB_ORDER_ASC) {
    tsBufAppend(pTSbuf, 0, pCtx->tag.i64Key, input, pCtx->size * TSDB_KEYSIZE);
  } else {
    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *d = GET_INPUT_CHAR_INDEX(pCtx, i);
      tsBufAppend(pTSbuf, 0, pCtx->tag.i64Key, d, TSDB_KEYSIZE);
    }
  }
  
  SET_VAL(pCtx, pCtx->size, 1);
  
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void ts_comp_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  STSCompInfo *pInfo = pResInfo->interResultBuf;
  
  STSBuf *pTSbuf = pInfo->pTSBuf;
  
  tsBufAppend(pTSbuf, 0, pCtx->tag.i64Key, pData, TSDB_KEYSIZE);
  SET_VAL(pCtx, pCtx->size, 1);
  
  pResInfo->hasResult = DATA_SET_FLAG;
}

static void ts_comp_finalize(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  
  STSCompInfo *pInfo = pResInfo->interResultBuf;
  STSBuf *     pTSbuf = pInfo->pTSBuf;
  
  tsBufFlush(pTSbuf);
  strcpy(pCtx->aOutputBuf, pTSbuf->path);
  
  tsBufDestory(pTSbuf);
  doFinalizer(pCtx);
}

//////////////////////////////////////////////////////////////////////////////////////////////
// RATE functions

static double do_calc_rate(const SRateInfo* pRateInfo) {
  if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->firstKey) || (pRateInfo->firstKey >= pRateInfo->lastKey)) {
    return 0;
  }
  
  int64_t diff = 0;
  
  if (pRateInfo->isIRate) {
    diff = pRateInfo->lastValue;
    if (diff >= pRateInfo->firstValue) {
      diff -= pRateInfo->firstValue;
    }
  } else {
    diff = pRateInfo->CorrectionValue + pRateInfo->lastValue -  pRateInfo->firstValue;
    if (diff <= 0) {
      return 0;
    }
  }
  
  int64_t duration = pRateInfo->lastKey - pRateInfo->firstKey;
  duration = (duration + 500) / 1000;
  
  double resultVal = ((double)diff) / duration;
  
  tscTrace("do_calc_rate() isIRate:%d firstKey:%" PRId64 " lastKey:%" PRId64 " firstValue:%" PRId64 " lastValue:%" PRId64 " CorrectionValue:%" PRId64 " resultVal:%f",
         pRateInfo->isIRate, pRateInfo->firstKey, pRateInfo->lastKey, pRateInfo->firstValue, pRateInfo->lastValue, pRateInfo->CorrectionValue, resultVal);
  
  return resultVal;
}


static bool rate_function_setup(SQLFunctionCtx *pCtx) {
  if (!function_setup(pCtx)) {
    return false;
  }
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);  //->aOutputBuf + pCtx->outputBytes;
  SRateInfo *   pInfo = pResInfo->interResultBuf;
  
  pInfo->CorrectionValue = 0;
  pInfo->firstKey    = INT64_MIN;
  pInfo->lastKey     = INT64_MIN;
  pInfo->firstValue  = INT64_MIN;
  pInfo->lastValue   = INT64_MIN;
  pInfo->num = 0;
  pInfo->sum = 0;
  
  pInfo->hasResult = 0;
  pInfo->isIRate = ((pCtx->functionId == TSDB_FUNC_IRATE) || (pCtx->functionId == TSDB_FUNC_SUM_IRATE) || (pCtx->functionId == TSDB_FUNC_AVG_IRATE));
  return true;
}


static void rate_function(SQLFunctionCtx *pCtx) {
  
  int32_t      notNullElems = 0;
  SResultInfo *pResInfo     = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo    = (SRateInfo *)pResInfo->interResultBuf;
  TSKEY       *primaryKey   = pCtx->ptsList;
  
  tscTrace("%p rate_function() size:%d, hasNull:%d", pCtx, pCtx->size, pCtx->hasNull);
  
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *pData = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      tscTrace("%p rate_function() index of null data:%d", pCtx, i);
      continue;
    }
    
    notNullElems++;
    
    int64_t v = 0;
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        v = (int64_t)GET_INT8_VAL(pData);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        v = (int64_t)GET_INT16_VAL(pData);
        break;
      case TSDB_DATA_TYPE_INT:
        v = (int64_t)GET_INT32_VAL(pData);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        v = (int64_t)GET_INT64_VAL(pData);
        break;
      default:
        assert(0);
    }
    
    if ((INT64_MIN == pRateInfo->firstValue) || (INT64_MIN == pRateInfo->firstKey)) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
      
      tscTrace("firstValue:%" PRId64 " firstKey:%" PRId64, pRateInfo->firstValue, pRateInfo->firstKey);
    }
    
    if (INT64_MIN == pRateInfo->lastValue) {
      pRateInfo->lastValue = v;
    } else if (v < pRateInfo->lastValue) {
      pRateInfo->CorrectionValue += pRateInfo->lastValue;
      tscTrace("CorrectionValue:%" PRId64, pRateInfo->CorrectionValue);
    }
    
    pRateInfo->lastValue = v;
    pRateInfo->lastKey   = primaryKey[i];
    tscTrace("lastValue:%" PRId64 " lastKey:%" PRId64, pRateInfo->lastValue, pRateInfo->lastKey);
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
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SRateInfo));
  }
}

static void rate_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  // NOTE: keep the intermediate result into the interResultBuf
  SResultInfo *pResInfo   = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo  = (SRateInfo *)pResInfo->interResultBuf;
  TSKEY       *primaryKey = pCtx->ptsList;
  
  int64_t v = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT:
      v = (int64_t)GET_INT8_VAL(pData);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      v = (int64_t)GET_INT16_VAL(pData);
      break;
    case TSDB_DATA_TYPE_INT:
      v = (int64_t)GET_INT32_VAL(pData);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      v = (int64_t)GET_INT64_VAL(pData);
      break;
    default:
      assert(0);
  }
  
  if ((INT64_MIN == pRateInfo->firstValue) || (INT64_MIN == pRateInfo->firstKey)) {
    pRateInfo->firstValue = v;
    pRateInfo->firstKey = primaryKey[index];
  }
  
  if (INT64_MIN == pRateInfo->lastValue) {
    pRateInfo->lastValue = v;
  } else if (v < pRateInfo->lastValue) {
    pRateInfo->CorrectionValue += pRateInfo->lastValue;
  }
  
  pRateInfo->lastValue = v;
  pRateInfo->lastKey   = primaryKey[index];
  
  tscTrace("====%p rate_function_f() index:%d lastValue:%" PRId64 " lastKey:%" PRId64 " CorrectionValue:%" PRId64, pCtx, index, pRateInfo->lastValue, pRateInfo->lastKey, pRateInfo->CorrectionValue);
  
  SET_VAL(pCtx, 1, 1);
  
  // set has result flag
  pRateInfo->hasResult = DATA_SET_FLAG;
  pResInfo->hasResult  = DATA_SET_FLAG;
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SRateInfo));
  }
}



static void rate_func_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  tscTrace("rate_func_merge() size:%d", pCtx->size);
  
  //SRateInfo *pRateInfo = (SRateInfo *)pResInfo->interResultBuf;
  SRateInfo *pBuf      = (SRateInfo *)pCtx->aOutputBuf;
  char      *indicator = pCtx->aInputElemBuf;
  
  assert(1 == pCtx->size);
  
  int32_t numOfNotNull = 0;
  for (int32_t i = 0; i < pCtx->size; ++i, indicator += sizeof(SRateInfo)) {
    SRateInfo *pInput = (SRateInfo *)indicator;
    if (DATA_SET_FLAG != pInput->hasResult) {
      continue;
    }
    
    numOfNotNull++;
    memcpy(pBuf, pInput, sizeof(SRateInfo));
    tscTrace("%p rate_func_merge() isIRate:%d firstKey:%" PRId64 " lastKey:%" PRId64 " firstValue:%" PRId64 " lastValue:%" PRId64 " CorrectionValue:%" PRId64,
           pCtx, pInput->isIRate, pInput->firstKey, pInput->lastKey, pInput->firstValue, pInput->lastValue, pInput->CorrectionValue);
  }
  
  SET_VAL(pCtx, numOfNotNull, 1);
  
  if (numOfNotNull > 0) {
    pBuf->hasResult = DATA_SET_FLAG;
  }
  
  return;
}



static void rate_func_copy(SQLFunctionCtx *pCtx) {
  assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
  
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  memcpy(pResInfo->interResultBuf, pCtx->aInputElemBuf, (size_t)pCtx->inputBytes);
  pResInfo->hasResult = ((SRateInfo*)pCtx->aInputElemBuf)->hasResult;
  
  SRateInfo* pRateInfo = (SRateInfo*)pCtx->aInputElemBuf;
  tscTrace("%p rate_func_second_merge() firstKey:%" PRId64 " lastKey:%" PRId64 " firstValue:%" PRId64 " lastValue:%" PRId64 " CorrectionValue:%" PRId64 " hasResult:%d",
         pCtx, pRateInfo->firstKey, pRateInfo->lastKey, pRateInfo->firstValue, pRateInfo->lastValue, pRateInfo->CorrectionValue, pRateInfo->hasResult);
}



static void rate_finalizer(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo  = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo = (SRateInfo *)pResInfo->interResultBuf;
  
  tscTrace("%p isIRate:%d firstKey:%" PRId64 " lastKey:%" PRId64 " firstValue:%" PRId64 " lastValue:%" PRId64 " CorrectionValue:%" PRId64 " hasResult:%d",
         pCtx, pRateInfo->isIRate, pRateInfo->firstKey, pRateInfo->lastKey, pRateInfo->firstValue, pRateInfo->lastValue, pRateInfo->CorrectionValue, pRateInfo->hasResult);
  
  if (pRateInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->aOutputBuf, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }
  
  *(double*)pCtx->aOutputBuf = do_calc_rate(pRateInfo);
  
  tscTrace("rate_finalizer() output result:%f", *(double *)pCtx->aOutputBuf);
  
  // cannot set the numOfIteratedElems again since it is set during previous iteration
  pResInfo->numOfRes  = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
  
  doFinalizer(pCtx);
}


static void irate_function(SQLFunctionCtx *pCtx) {
  
  int32_t       notNullElems = 0;
  SResultInfo  *pResInfo     = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo    = (SRateInfo *)pResInfo->interResultBuf;
  TSKEY        *primaryKey   = pCtx->ptsList;
  
  tscTrace("%p irate_function() size:%d, hasNull:%d", pCtx, pCtx->size, pCtx->hasNull);
  
  if (pCtx->size < 1) {
    return;
  }
  
  for (int32_t i = pCtx->size - 1; i >= 0; --i) {
    char *pData = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
      tscTrace("%p irate_function() index of null data:%d", pCtx, i);
      continue;
    }
    
    notNullElems++;
    
    int64_t v = 0;
    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_TINYINT:
        v = (int64_t)GET_INT8_VAL(pData);
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        v = (int64_t)GET_INT16_VAL(pData);
        break;
      case TSDB_DATA_TYPE_INT:
        v = (int64_t)GET_INT32_VAL(pData);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        v = (int64_t)GET_INT64_VAL(pData);
        break;
      default:
        assert(0);
    }
    
    // TODO: calc once if only call this function once ????
    if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->lastValue)) {
      pRateInfo->lastValue = v;
      pRateInfo->lastKey   = primaryKey[i];
      
      tscTrace("%p irate_function() lastValue:%" PRId64 " lastKey:%" PRId64, pCtx, pRateInfo->lastValue, pRateInfo->lastKey);
      continue;
    }
    
    if ((INT64_MIN == pRateInfo->firstKey) || (INT64_MIN == pRateInfo->firstValue)){
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = primaryKey[i];
      
      tscTrace("%p irate_function() firstValue:%" PRId64 " firstKey:%" PRId64, pCtx, pRateInfo->firstValue, pRateInfo->firstKey);
      break;
    }
  }
  
  SET_VAL(pCtx, notNullElems, 1);
  
  if (notNullElems > 0) {
    pRateInfo->hasResult = DATA_SET_FLAG;
    pResInfo->hasResult  = DATA_SET_FLAG;
  }
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SRateInfo));
  }
}

static void irate_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNull && isNull(pData, pCtx->inputType)) {
    return;
  }
  
  // NOTE: keep the intermediate result into the interResultBuf
  SResultInfo  *pResInfo   = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo  = (SRateInfo *)pResInfo->interResultBuf;
  TSKEY        *primaryKey = pCtx->ptsList;
  
  int64_t v = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT:
      v = (int64_t)GET_INT8_VAL(pData);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      v = (int64_t)GET_INT16_VAL(pData);
      break;
    case TSDB_DATA_TYPE_INT:
      v = (int64_t)GET_INT32_VAL(pData);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      v = (int64_t)GET_INT64_VAL(pData);
      break;
    default:
      assert(0);
  }
  
  pRateInfo->firstKey   = pRateInfo->lastKey;
  pRateInfo->firstValue = pRateInfo->lastValue;
  
  pRateInfo->lastValue = v;
  pRateInfo->lastKey   = primaryKey[index];
  
  tscTrace("====%p irate_function_f() index:%d lastValue:%" PRId64 " lastKey:%" PRId64 " firstValue:%" PRId64 " firstKey:%" PRId64, pCtx, index, pRateInfo->lastValue, pRateInfo->lastKey, pRateInfo->firstValue , pRateInfo->firstKey);
  
  SET_VAL(pCtx, 1, 1);
  
  // set has result flag
  pRateInfo->hasResult = DATA_SET_FLAG;
  pResInfo->hasResult  = DATA_SET_FLAG;
  
  // keep the data into the final output buffer for super table query since this execution may be the last one
  if (pResInfo->superTableQ) {
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SRateInfo));
  }
}

static void do_sumrate_merge(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo = GET_RES_INFO(pCtx);
  assert(pResInfo->superTableQ);
  
  SRateInfo *pRateInfo = (SRateInfo *)pResInfo->interResultBuf;
  char *    input = GET_INPUT_CHAR(pCtx);
  
  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SRateInfo *pInput = (SRateInfo *)input;
    
    tscTrace("%p do_sumrate_merge() hasResult:%d input num:%" PRId64 " input sum:%f total num:%" PRId64 " total sum:%f", pCtx, pInput->hasResult, pInput->num, pInput->sum, pRateInfo->num, pRateInfo->sum);
    
    if (pInput->hasResult != DATA_SET_FLAG) {
      continue;
    } else if (pInput->num == 0) {
      pRateInfo->sum += do_calc_rate(pInput);
      pRateInfo->num++;
    } else {
      pRateInfo->sum += pInput->sum;
      pRateInfo->num += pInput->num;
    }
    pRateInfo->hasResult = DATA_SET_FLAG;
  }
  
  // if the data set hasResult is not set, the result is null
  if (DATA_SET_FLAG == pRateInfo->hasResult) {
    pResInfo->hasResult = DATA_SET_FLAG;
    SET_VAL(pCtx, pRateInfo->num, 1);
    memcpy(pCtx->aOutputBuf, pResInfo->interResultBuf, sizeof(SRateInfo));
  }
}

static void sumrate_func_merge(SQLFunctionCtx *pCtx) {
  tscTrace("%p sumrate_func_merge() process ...", pCtx);
  do_sumrate_merge(pCtx);
}

static void sumrate_func_second_merge(SQLFunctionCtx *pCtx) {
  tscTrace("%p sumrate_func_second_merge() process ...", pCtx);
  do_sumrate_merge(pCtx);
}

static void sumrate_finalizer(SQLFunctionCtx *pCtx) {
  SResultInfo *pResInfo  = GET_RES_INFO(pCtx);
  SRateInfo   *pRateInfo = (SRateInfo *)pResInfo->interResultBuf;
  
  tscTrace("%p sumrate_finalizer() superTableQ:%d num:%" PRId64 " sum:%f hasResult:%d", pCtx, pResInfo->superTableQ, pRateInfo->num, pRateInfo->sum, pRateInfo->hasResult);
  
  if (pRateInfo->hasResult != DATA_SET_FLAG) {
    setNull(pCtx->aOutputBuf, TSDB_DATA_TYPE_DOUBLE, sizeof(double));
    return;
  }
  
  if (pRateInfo->num == 0) {
    // from meter
    *(double*)pCtx->aOutputBuf = do_calc_rate(pRateInfo);
  } else if (pCtx->functionId == TSDB_FUNC_SUM_RATE || pCtx->functionId == TSDB_FUNC_SUM_IRATE) {
    *(double*)pCtx->aOutputBuf = pRateInfo->sum;
  } else {
    *(double*)pCtx->aOutputBuf = pRateInfo->sum / pRateInfo->num;
  }
  
  pResInfo->numOfRes  = 1;
  pResInfo->hasResult = DATA_SET_FLAG;
  doFinalizer(pCtx);
}


/////////////////////////////////////////////////////////////////////////////////////////////


/*
 * function compatible list.
 * tag and ts are not involved in the compatibility check
 *
 * 1. functions that are not simultaneously present with any other functions. e.g.,
 * diff/ts_z/top/bottom
 * 2. functions that are only allowed to be present only with same functions. e.g., last_row, interp
 * 3. functions that are allowed to be present with other functions.
 *    e.g., count/sum/avg/min/max/stddev/percentile/apercentile/first/last...
 *
 */
int32_t funcCompatDefList[] = {
    // count,       sum,      avg,       min,      max,  stddev,    percentile, apercentile, first,   last
    1,          1,        1,         1,        1,      1,          1,           1,        1,      1,
    // last_row,    top,    bottom,     spread,    twa,  leastsqr,     ts,       ts_dummy, tag_dummy, ts_z
    4,         -1,       -1,         1,        1,      1,          1,           1,        1,     -1,
    //  tag,       colprj,  tagprj,   arithmetic, diff, first_dist, last_dist,    interp      rate   irate
    1,          1,        1,         1,       -1,      1,          1,           5,        1,      1,
    // sum_rate, sum_irate, avg_rate, avg_irate
    1,          1,        1,         1,
};

SQLAggFuncElem aAggs[] = {{
                              // 0, count function does not invoke the finalize function
                              "count",
                              TSDB_FUNC_COUNT,
                              TSDB_FUNC_COUNT,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              count_function,
                              count_function_f,
                              no_next_step,
                              doFinalizer,
                              count_func_merge,
                              count_func_merge,
                              count_load_data_info,
                          },
                          {
                              // 1
                              "sum",
                              TSDB_FUNC_SUM,
                              TSDB_FUNC_SUM,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              sum_function,
                              sum_function_f,
                              no_next_step,
                              function_finalizer,
                              sum_func_merge,
                              sum_func_second_merge,
                              precal_req_load_info,
                          },
                          {
                              // 2
                              "avg",
                              TSDB_FUNC_AVG,
                              TSDB_FUNC_AVG,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              avg_function,
                              avg_function_f,
                              no_next_step,
                              avg_finalizer,
                              avg_func_merge,
                              avg_func_second_merge,
                              precal_req_load_info,
                          },
                          {
                              // 3
                              "min",
                              TSDB_FUNC_MIN,
                              TSDB_FUNC_MIN,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              min_func_setup,
                              min_function,
                              min_function_f,
                              no_next_step,
                              function_finalizer,
                              min_func_merge,
                              min_func_second_merge,
                              precal_req_load_info,
                          },
                          {
                              // 4
                              "max",
                              TSDB_FUNC_MAX,
                              TSDB_FUNC_MAX,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              max_func_setup,
                              max_function,
                              max_function_f,
                              no_next_step,
                              function_finalizer,
                              max_func_merge,
                              max_func_second_merge,
                              precal_req_load_info,
                          },
                          {
                              // 5
                              "stddev",
                              TSDB_FUNC_STDDEV,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              function_setup,
                              stddev_function,
                              stddev_function_f,
                              stddev_next_step,
                              stddev_finalizer,
                              noop1,
                              noop1,
                              data_req_load_info,
                          },
                          {
                              // 6
                              "percentile",
                              TSDB_FUNC_PERCT,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              percentile_function_setup,
                              percentile_function,
                              percentile_function_f,
                              no_next_step,
                              percentile_finalizer,
                              noop1,
                              noop1,
                              data_req_load_info,
                          },
                          {
                              // 7
                              "apercentile",
                              TSDB_FUNC_APERCT,
                              TSDB_FUNC_APERCT,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_STABLE,
                              apercentile_function_setup,
                              apercentile_function,
                              apercentile_function_f,
                              no_next_step,
                              apercentile_finalizer,
                              apercentile_func_merge,
                              apercentile_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 8
                              "first",
                              TSDB_FUNC_FIRST,
                              TSDB_FUNC_FIRST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              first_function,
                              first_function_f,
                              no_next_step,
                              function_finalizer,
                              noop1,
                              noop1,
                              first_data_req_info,
                          },
                          {
                              // 9
                              "last",
                              TSDB_FUNC_LAST,
                              TSDB_FUNC_LAST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_SELECTIVITY,
                              function_setup,
                              last_function,
                              last_function_f,
                              no_next_step,
                              function_finalizer,
                              noop1,
                              noop1,
                              last_data_req_info,
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
                              noop2,
                              no_next_step,
                              last_row_finalizer,
                              noop1,
                              last_dist_func_second_merge,
                              data_req_load_info,
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
                              top_function_f,
                              no_next_step,
                              top_bottom_func_finalizer,
                              top_func_merge,
                              top_func_second_merge,
                              data_req_load_info,
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
                              bottom_function_f,
                              no_next_step,
                              top_bottom_func_finalizer,
                              bottom_func_merge,
                              bottom_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 13
                              "spread",
                              TSDB_FUNC_SPREAD,
                              TSDB_FUNC_SPREAD,
                              TSDB_BASE_FUNC_SO,
                              spread_function_setup,
                              spread_function,
                              spread_function_f,
                              no_next_step,
                              spread_function_finalizer,
                              spread_func_merge,
                              spread_func_sec_merge,
                              count_load_data_info,
                          },
                          {
                              // 14
                              "twa",
                              TSDB_FUNC_TWA,
                              TSDB_FUNC_TWA,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              twa_function_setup,
                              twa_function,
                              twa_function_f,
                              no_next_step,
                              twa_function_finalizer,
                              twa_func_merge,
                              twa_function_copy,
                              data_req_load_info,
                          },
                          {
                              // 15
                              "leastsquares",
                              TSDB_FUNC_LEASTSQR,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
                              leastsquares_function_setup,
                              leastsquares_function,
                              leastsquares_function_f,
                              no_next_step,
                              leastsquares_finalizer,
                              noop1,
                              noop1,
                              data_req_load_info,
                          },
                          {
                              // 16
                              "ts",
                              TSDB_FUNC_TS,
                              TSDB_FUNC_TS,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              date_col_output_function,
                              date_col_output_function_f,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              no_data_info,
                          },
                          {
                              // 17
                              "ts",
                              TSDB_FUNC_TS_DUMMY,
                              TSDB_FUNC_TS_DUMMY,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              noop1,
                              noop2,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              data_req_load_info,
                          },
                          {
                              // 18
                              "tag",
                              TSDB_FUNC_TAG_DUMMY,
                              TSDB_FUNC_TAG_DUMMY,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              tag_function,
                              noop2,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              no_data_info,
                          },
                          {
                              // 19
                              "ts",
                              TSDB_FUNC_TS_COMP,
                              TSDB_FUNC_TS_COMP,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_NEED_TS,
                              ts_comp_function_setup,
                              ts_comp_function,
                              ts_comp_function_f,
                              no_next_step,
                              ts_comp_finalize,
                              copy_function,
                              copy_function,
                              data_req_load_info,
                          },
                          {
                              // 20
                              "tag",
                              TSDB_FUNC_TAG,
                              TSDB_FUNC_TAG,
                              TSDB_BASE_FUNC_SO,
                              function_setup,
                              tag_function,
                              tag_function_f,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              no_data_info,
                          },
                          {
                              // 21, column project sql function
                              "colprj",
                              TSDB_FUNC_PRJ,
                              TSDB_FUNC_PRJ,
                              TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              col_project_function,
                              col_project_function_f,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              data_req_load_info,
                          },
                          {
                              // 22, multi-output, tag function has only one result
                              "tagprj",
                              TSDB_FUNC_TAGPRJ,
                              TSDB_FUNC_TAGPRJ,
                              TSDB_BASE_FUNC_MO,
                              function_setup,
                              tag_project_function,
                              tag_project_function_f,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              no_data_info,
                          },
                          {
                              // 23
                              "arithmetic",
                              TSDB_FUNC_ARITHM,
                              TSDB_FUNC_ARITHM,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              arithmetic_function,
                              arithmetic_function_f,
                              no_next_step,
                              doFinalizer,
                              copy_function,
                              copy_function,
                              data_req_load_info,
                          },
                          {
                              // 24
                              "diff",
                              TSDB_FUNC_DIFF,
                              TSDB_FUNC_INVALID_ID,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_NEED_TS,
                              diff_function_setup,
                              diff_function,
                              diff_function_f,
                              no_next_step,
                              doFinalizer,
                              noop1,
                              noop1,
                              data_req_load_info,
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
                              first_dist_function_f,
                              no_next_step,
                              function_finalizer,
                              first_dist_func_merge,
                              first_dist_func_second_merge,
                              first_dist_data_req_info,
                          },
                          {
                              // 26
                              "last_dist",
                              TSDB_FUNC_LAST_DST,
                              TSDB_FUNC_LAST_DST,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS | TSDB_FUNCSTATE_SELECTIVITY,
                              first_last_function_setup,
                              last_dist_function,
                              last_dist_function_f,
                              no_next_step,
                              function_finalizer,
                              last_dist_func_merge,
                              last_dist_func_second_merge,
                              last_dist_data_req_info,
                          },
                          {
                              // 27
                              "interp",
                              TSDB_FUNC_INTERP,
                              TSDB_FUNC_INTERP,
                              TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_STABLE | TSDB_FUNCSTATE_NEED_TS,
                              function_setup,
                              interp_function,
                              do_sum_f,  // todo filter handle
                              no_next_step,
                              doFinalizer,
                              noop1,
                              copy_function,
                              no_data_info,
                          },
                          {
                              // 28
                              "rate",
                              TSDB_FUNC_RATE,
                              TSDB_FUNC_RATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              rate_function,
                              rate_function_f,
                              no_next_step,
                              rate_finalizer,
                              rate_func_merge,
                              rate_func_copy,
                              data_req_load_info,
                          },
                          {
                              // 29
                              "irate",
                              TSDB_FUNC_IRATE,
                              TSDB_FUNC_IRATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              irate_function,
                              irate_function_f,
                              no_next_step,
                              rate_finalizer,
                              rate_func_merge,
                              rate_func_copy,
                              data_req_load_info,
                          },
                          {
                              // 30
                              "sum_rate",
                              TSDB_FUNC_SUM_RATE,
                              TSDB_FUNC_SUM_RATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              rate_function,
                              rate_function_f,
                              no_next_step,
                              sumrate_finalizer,
                              sumrate_func_merge,
                              sumrate_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 31
                              "sum_irate",
                              TSDB_FUNC_SUM_IRATE,
                              TSDB_FUNC_SUM_IRATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              irate_function,
                              irate_function_f,
                              no_next_step,
                              sumrate_finalizer,
                              sumrate_func_merge,
                              sumrate_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 32
                              "avg_rate",
                              TSDB_FUNC_AVG_RATE,
                              TSDB_FUNC_AVG_RATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              rate_function,
                              rate_function_f,
                              no_next_step,
                              sumrate_finalizer,
                              sumrate_func_merge,
                              sumrate_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 33
                              "avg_irate",
                              TSDB_FUNC_AVG_IRATE,
                              TSDB_FUNC_AVG_IRATE,
                              TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
                              rate_function_setup,
                              irate_function,
                              irate_function_f,
                              no_next_step,
                              sumrate_finalizer,
                              sumrate_func_merge,
                              sumrate_func_second_merge,
                              data_req_load_info,
                          },
                          {
                              // 34
                              "tid_tag",   // return table id and the corresponding tags for join match and subscribe
                              TSDB_FUNC_TID_TAG,
                              TSDB_FUNC_TID_TAG,
                              TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_STABLE,
                              function_setup,
                              noop1,
                              noop2,
                              no_next_step,
                              noop1,
                              noop1,
                              noop1,
                              data_req_load_info,
                          }};
