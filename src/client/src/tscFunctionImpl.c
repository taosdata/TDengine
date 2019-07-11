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

#pragma GCC diagnostic ignored "-Wincompatible-pointer-types"

#include <assert.h>
#include <ctype.h>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wctype.h>

#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "thistogram.h"
#include "tinterpolation.h"
#include "tlog.h"
#include "tscSyntaxtreefunction.h"
#include "tsqlfunction.h"
#include "ttypes.h"
#include "tutil.h"

typedef struct tValuePair {
  tVariant v;
  int64_t  timestamp;
} tValuePair;

typedef struct SSpreadRuntime {
  double start;
  double end;
  char   valid;
} SSpreadRuntime;

void getResultInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, int16_t *type,
                   int16_t *bytes) {
  if (!isValidDataType(dataType, dataBytes)) {
    pError("Illegal data type %d or data type length %d", dataType, dataBytes);
    return;
  }

  if (functionId == TSDB_FUNC_MIN || functionId == TSDB_FUNC_MAX || functionId == TSDB_FUNC_FIRST ||
      functionId == TSDB_FUNC_LAST || functionId == TSDB_FUNC_TS || functionId == TSDB_FUNC_TS_DUMMY ||
      functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM || functionId == TSDB_FUNC_DIFF ||
      functionId == TSDB_FUNC_PRJ || functionId == TSDB_FUNC_TAGPRJ || functionId == TSDB_FUNC_TAG ||
      functionId == TSDB_FUNC_INTERP || functionId == TSDB_FUNC_LAST_ROW) {
    *type = (int16_t)dataType;
    *bytes = (int16_t)dataBytes;
    return;
  }

  if (functionId == TSDB_FUNC_COUNT) {
    *type = TSDB_DATA_TYPE_BIGINT;
    *bytes = sizeof(int64_t);
    return;
  }

  if (functionId == TSDB_FUNC_AVG || functionId == TSDB_FUNC_PERCT || functionId == TSDB_FUNC_APERCT ||
      functionId == TSDB_FUNC_STDDEV || functionId == TSDB_FUNC_ARITHM || functionId == TSDB_FUNC_SPREAD ||
      functionId == TSDB_FUNC_WAVG) {
    *type = TSDB_DATA_TYPE_DOUBLE;
    *bytes = sizeof(double);
    return;
  }

  if (functionId == TSDB_FUNC_SUM) {
    if (dataType >= TSDB_DATA_TYPE_TINYINT && dataType <= TSDB_DATA_TYPE_BIGINT) {
      *type = TSDB_DATA_TYPE_BIGINT;
    } else {
      *type = TSDB_DATA_TYPE_DOUBLE;
    }

    *bytes = sizeof(int64_t);
    return;
  }

  if (functionId == TSDB_FUNC_LEASTSQR) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = TSDB_AVG_FUNCTION_INTER_BUFFER_SIZE;  // string
  } else if (functionId == TSDB_FUNC_FIRST_DST || functionId == TSDB_FUNC_LAST_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = dataBytes + DATA_SET_FLAG_SIZE + TSDB_KEYSIZE;
  } else if (functionId == TSDB_FUNC_SPREAD_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SSpreadRuntime);
  } else if (functionId == TSDB_FUNC_WAVG_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SWavgRuntime);
  } else if (functionId == TSDB_FUNC_MIN_DST || functionId == TSDB_FUNC_MAX_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = dataBytes + DATA_SET_FLAG_SIZE;
  } else if (functionId == TSDB_FUNC_SUM_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SSumRuntime);
  } else if (functionId == TSDB_FUNC_AVG_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SAvgRuntime);
  } else if (functionId == TSDB_FUNC_TOP_DST || functionId == TSDB_FUNC_BOTTOM_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(int64_t) + sizeof(tValuePair) * param;
  } else if (functionId == TSDB_FUNC_APERCT_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1) + sizeof(SHistogramInfo);
  } else if (functionId == TSDB_FUNC_LAST_ROW_DST) {
    *type = TSDB_DATA_TYPE_BINARY;
    *bytes = dataBytes + DATA_SET_FLAG_SIZE + TSDB_KEYSIZE;
  }
}

/*
 * whether has put the first result into the output buffer
 * decided by if there are timestamp of value and the seperator ','
 */

#define IS_DATA_NOT_ASSIGNED(ctx) (*(char *)((ctx)->aOutputBuf + TSDB_KEYSIZE) != DATA_SET_FLAG)
#define SET_DATA_ASSIGNED(ctx) (*(char *)((ctx)->aOutputBuf + TSDB_KEYSIZE) = DATA_SET_FLAG)

#define SET_VAL(ctx, numOfElem, numOfRes)     \
  do {                                        \
    if ((numOfElem) <= 0) {                   \
      break;                                  \
    }                                         \
    (ctx)->numOfIteratedElems += (numOfElem); \
    (ctx)->numOfOutputElems = (numOfRes);     \
  } while (0);

#define INC_INIT_VAL(ctx, numOfElem, numOfRes) \
  do {                                         \
    (ctx)->numOfIteratedElems += (numOfElem);  \
    (ctx)->numOfOutputElems += (numOfRes);     \
  } while (0);

void noop(SQLFunctionCtx *pCtx) { UNUSED(pCtx); /* suppress warning*/ }
bool no_next_step(SQLFunctionCtx *pCtx) {
  UNUSED(pCtx);
  return false;
}

#define INIT_VAL(ctx)              \
  do {                             \
    (ctx)->currentStage = 0;       \
    (ctx)->numOfOutputElems = 0;   \
    (ctx)->numOfIteratedElems = 0; \
  } while (0);

#define GET_INPUT_CHAR(x) (((char *)((x)->aInputElemBuf)) + ((x)->startOffset) * ((x)->inputBytes))
#define GET_INPUT_CHAR_INDEX(x, y) (GET_INPUT_CHAR(x) + (y) * (x)->inputBytes)

#define SET_HAS_DATA_FLAG(x) ((x) = DATA_SET_FLAG)
#define HAS_DATA_FLAG(x) ((x) == DATA_SET_FLAG)

#define tPow(x) ((x) * (x))

void function_setup(SQLFunctionCtx *pCtx) {
  memset(pCtx->aOutputBuf, 0, pCtx->outputBytes);
  pCtx->intermediateBuf[0].i64Key = 0;
  INIT_VAL(pCtx);
}

/**
 * in handling the stable query, function_finalize is called after the secondary
 * merge being completed, during the first merge procedure, which is executed at the
 * vnode side, the finalize will never be called.
 * todo: add more information for debug in SQLFunctionCtx
 * @param pCtx
 */
void function_finalize(SQLFunctionCtx *pCtx) {
  if (pCtx->numOfIteratedElems == 0) {
    pTrace("no result generated, result is set to NULL");
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
  }
}

static bool count_function(SQLFunctionCtx *pCtx) {
  int32_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pCtx->hasNullValue == true. pCtx->preAggVals.isSet == true;
   * 2. for general non-primary key columns, pCtx->hasNullValue may be true or false, pCtx->preAggVals.isSet == true;
   * 3. for primary key column, pCtx->hasNullValue always be false, pCtx->preAggVals.isSet == false;
   */
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus)) {  // Pre-aggregation
    if (pCtx->preAggVals.isSet) {
      numOfElem = pCtx->size - pCtx->preAggVals.numOfNullPoints;
    } else {
      assert(pCtx->hasNullValue == false);
      numOfElem = pCtx->size;
    }

    goto _count_over;
  }

  /*
   * In following cases, the data block is loaded:
   * 1. it is a first/last file block for a query
   * 2. actual block data is loaded in case of handling other queries, such as apercentile/wavg/stddev etc.
   * 3. it is a cache block
   */
  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *val = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(val, pCtx->inputType)) {
        continue;
      }

      numOfElem += 1;
    }
  } else {
    numOfElem = pCtx->size;
  }

_count_over:
  *((int64_t *)pCtx->aOutputBuf) += numOfElem;
  SET_VAL(pCtx, numOfElem, 1);
  return true;
}

static bool count_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  *((int64_t *)pCtx->aOutputBuf) += 1;
  return true;
}

static void count_dist_merge(SQLFunctionCtx *pCtx) {
  int64_t *pData = (int64_t *)GET_INPUT_CHAR(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i) {
    *((int64_t *)pCtx->aOutputBuf) += pData[i];
  }

  SET_VAL(pCtx, pCtx->size, 1);
}

/**
 * 1. If the column value for filter exists, we need to load the SFields, which serves
 * as the pre-filter to decide if the actual data block is required or not.
 * 2. If it queries on the non-primary timestamp column, SFields is also required to get the not-null value.
 *
 * @param colId
 * @param filterCols
 * @return
 */
int32_t count_load_data_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    return BLK_DATA_NO_NEEDED;
  } else {
    return BLK_DATA_FILEDS_NEEDED;
  }
}

int32_t no_data_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  return BLK_DATA_NO_NEEDED;
}

#define LIST_ADD(x, n, p)             \
  for (int32_t i = 0; i < (n); ++i) { \
    (x) += (p)[i];                    \
  };

#define TYPED_LIST_ADD(x, n, s, t) \
  do {                             \
    t *p = (t *)s;                 \
    LIST_ADD(x, n, p);             \
  } while (0);

#define LIST_ADD_N(x, n, p, t, numOfElem, tsdbType) \
  {                                                 \
    t *d = (t *)(p);                                \
    for (int32_t i = 0; i < (n); ++i) {             \
      if (isNull((char *)&(d)[i], tsdbType)) {      \
        continue;                                   \
      };                                            \
      (x) += (d)[i];                                \
      numOfElem++;                                  \
    }                                               \
  };

#define LOOPCHECK(v, d, n, sign)                    \
  for (int32_t i = 0; i < (n); ++i) {               \
    (v) = (((v) < (d)[i]) ^ (sign)) ? (d)[i] : (v); \
  }

#define LOOPCHECK_N(val, list, num, tsdbType, sign, notNullElem) \
  for (int32_t i = 0; i < (num); ++i) {                          \
    if (isNull((char *)&(list)[i], tsdbType)) {                  \
      continue;                                                  \
    }                                                            \
    (val) = (((val) < (list)[i]) ^ (sign)) ? (list)[i] : (val);  \
    notNullElem += 1;                                            \
  }

#define TYPED_LOOPCHECK(t, v, d, n, sign) \
  do {                                    \
    t *_d = (t *)d;                       \
    t *v1 = (t *)v;                       \
    LOOPCHECK(*v1, _d, n, sign);          \
  } while (0)

#define TYPED_LOOPCHECK_N(type, data, list, num, tsdbType, sign, notNullElems) \
  do {                                                                         \
    type *_data = (type *)data;                                                \
    type *_list = (type *)list;                                                \
    LOOPCHECK_N(*_data, _list, num, tsdbType, sign, notNullElems);             \
  } while (0)

static bool sum_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) && pCtx->preAggVals.isSet) {
    // it's the whole block to be calculated, so the assert must be correct
    assert(pCtx->size >= pCtx->preAggVals.numOfNullPoints);
    notNullElems = (pCtx->size - pCtx->preAggVals.numOfNullPoints);

    if (notNullElems > 0) {
      if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
        int64_t *retVal = pCtx->aOutputBuf;
        *retVal += pCtx->preAggVals.sum;
      } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
        double *retVal = pCtx->aOutputBuf;
        *retVal += *(double *)&(pCtx->preAggVals.sum);
      }
    }
    goto _sum_over;
  }

  void *pData = GET_INPUT_CHAR(pCtx);

  if (pCtx->hasNullValue) {
    notNullElems = 0;

    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t *retVal = pCtx->aOutputBuf;

      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        LIST_ADD_N(*retVal, pCtx->size, pData, int8_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(*retVal, pCtx->size, pData, int16_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(*retVal, pCtx->size, pData, int32_t, notNullElems, pCtx->inputType);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(*retVal, pCtx->size, pData, int64_t, notNullElems, pCtx->inputType);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *retVal = pCtx->aOutputBuf;
      LIST_ADD_N(*retVal, pCtx->size, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = pCtx->aOutputBuf;
      LIST_ADD_N(*retVal, pCtx->size, pData, float, notNullElems, pCtx->inputType);
    }
  } else {
    notNullElems = pCtx->size;

    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t *retVal = pCtx->aOutputBuf;

      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        TYPED_LIST_ADD(*retVal, pCtx->size, pData, int8_t);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        TYPED_LIST_ADD(*retVal, pCtx->size, pData, int16_t);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        TYPED_LIST_ADD(*retVal, pCtx->size, pData, int32_t);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        TYPED_LIST_ADD(*retVal, pCtx->size, pData, int64_t);
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *retVal = pCtx->aOutputBuf;
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, double);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      double *retVal = pCtx->aOutputBuf;
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, float);
    }
  }

_sum_over:
  // data in the check operation are all null, not output
  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool sum_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  int64_t *res = pCtx->aOutputBuf;

  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    *res += *(int8_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    *res += *(int16_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    *res += *(int32_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    *res += *(int64_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *retVal = pCtx->aOutputBuf;
    *retVal += *(double *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    double *retVal = pCtx->aOutputBuf;
    *retVal += *(float *)pData;
  }

  return true;
}

static bool sum_dist_intern_function(SQLFunctionCtx *pCtx) {
  sum_function(pCtx);

  // keep the result data in output buffer, not in the intermediate buffer
  if (pCtx->numOfIteratedElems > 0) {
    char *pOutputBuf = pCtx->aOutputBuf;
    *(pOutputBuf + sizeof(double)) = DATA_SET_FLAG;
  }

  return true;
}

static bool sum_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  sum_function_f(pCtx, index);

  /* keep the result data in output buffer, not in the intermediate buffer */
  if (pCtx->numOfIteratedElems) {
    char *pOutputBuf = pCtx->aOutputBuf;
    *(pOutputBuf + sizeof(double)) = DATA_SET_FLAG;
  }

  return true;
}

static int32_t do_sum_merge_impl(const SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  int32_t type = (pCtx->outputType != TSDB_DATA_TYPE_BINARY) ? pCtx->outputType : pCtx->inputType;
  char *  input = GET_INPUT_CHAR(pCtx);

  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SSumRuntime *pInput = (SSumRuntime *)input;
    if (pInput->valFlag != DATA_SET_FLAG) {
      continue;
    }

    notNullElems++;

    switch (type) {
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_BIGINT: {
        *(int64_t *)pCtx->aOutputBuf += pInput->iOutput;
        break;
      };
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE: {
        *(double *)pCtx->aOutputBuf += pInput->dOutput;
      }
    }
  }
  return notNullElems;
}

static void sum_dist_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = do_sum_merge_impl(pCtx);

  SET_VAL(pCtx, notNullElems, 1);
  SSumRuntime *pSumRuntime = (SSumRuntime *)pCtx->aOutputBuf;

  if (notNullElems > 0) {
    pCtx->numOfIteratedElems += notNullElems;
    pSumRuntime->valFlag = DATA_SET_FLAG;
  }
}

static void sum_dist_second_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = do_sum_merge_impl(pCtx);

  SET_VAL(pCtx, notNullElems, 1);
  /* NOTE: no flag value exists for secondary merge */
  if (notNullElems > 0) {
    pCtx->numOfIteratedElems += notNullElems;
  }
}

static int32_t precal_req_load_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  return BLK_DATA_FILEDS_NEEDED;
}

static int32_t data_req_load_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  return BLK_DATA_ALL_NEEDED;
}

// todo: if  column in current data block are null, opt for this case
static int32_t first_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  if (pCtx->order == TSQL_SO_DESC) {
    return BLK_DATA_NO_NEEDED;
  }

  /* no result for first query, data block is required */
  if (pCtx->numOfOutputElems <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t last_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId, int32_t blockStatus) {
  if (pCtx->order == TSQL_SO_ASC) {
    return BLK_DATA_NO_NEEDED;
  }

  if (pCtx->numOfOutputElems <= 0) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    return BLK_DATA_NO_NEEDED;
  }
}

static int32_t first_dist_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId,
                                        int32_t blockStatus) {
  if (pCtx->order == TSQL_SO_DESC) {
    return BLK_DATA_NO_NEEDED;
  }

  if (IS_DATA_NOT_ASSIGNED(pCtx)) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    // data in current block is not earlier than current result
    TSKEY ts = *(TSKEY *)pCtx->aOutputBuf;
    return (ts <= start) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

static int32_t last_dist_data_req_info(SQLFunctionCtx *pCtx, TSKEY start, TSKEY end, int32_t colId,
                                       int32_t blockStatus) {
  if (pCtx->order == TSQL_SO_ASC) {
    return BLK_DATA_NO_NEEDED;
  }

  if (IS_DATA_NOT_ASSIGNED(pCtx)) {
    return BLK_DATA_ALL_NEEDED;
  } else {
    TSKEY ts = *(TSKEY *)pCtx->aOutputBuf;
    return (ts > end) ? BLK_DATA_NO_NEEDED : BLK_DATA_ALL_NEEDED;
  }
}

/*
 * the average value is calculated in finalize routine, since current routine does not know the exact number of points
 */
static void avg_finalizer(SQLFunctionCtx *pCtx) {
  // pCtx->numOfIteratedElems is the number of not null elements in current
  // query range
  if (pCtx->numOfIteratedElems == 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
    return;  // empty table
  }

  if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
    int64_t *retVal = pCtx->aOutputBuf;
    *(double *)pCtx->aOutputBuf = (*retVal) / (double)pCtx->numOfIteratedElems;
  } else {
    double *retVal = pCtx->aOutputBuf;
    *retVal = *retVal / (double)pCtx->numOfIteratedElems;
  }

  /* cannot set the numOfIteratedElems again since it is set during previous iteration */
  pCtx->numOfOutputElems = 1;
}

static void avg_dist_function_setup(SQLFunctionCtx *pCtx) {
  pCtx->intermediateBuf[0].nType = TSDB_DATA_TYPE_DOUBLE;
  memset(pCtx->aOutputBuf, 0, pCtx->outputBytes);
  INIT_VAL(pCtx);
}

static void avg_dist_merge(SQLFunctionCtx *pCtx) {
  SAvgRuntime *pDest = (SAvgRuntime *)pCtx->aOutputBuf;

  char *input = GET_INPUT_CHAR(pCtx);
  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgRuntime *pInput = (SAvgRuntime *)input;
    if (pInput->valFlag != DATA_SET_FLAG) {  // current buffer is null
      continue;
    }

    pDest->sum += pInput->sum;
    pDest->num += pInput->num;
    pDest->valFlag = DATA_SET_FLAG;
  }

  /* if the data set flag is not set, the result is null */
  pCtx->numOfIteratedElems = pDest->num;
}

static void avg_dist_second_merge(SQLFunctionCtx *pCtx) {
  double *sum = pCtx->aOutputBuf;
  char *  input = GET_INPUT_CHAR(pCtx);

  for (int32_t i = 0; i < pCtx->size; ++i, input += pCtx->inputBytes) {
    SAvgRuntime *pInput = (SAvgRuntime *)input;
    if (pInput->valFlag != DATA_SET_FLAG) {  // current input is null
      continue;
    }

    *sum += pInput->sum;
    pCtx->numOfIteratedElems += pInput->num;
  }
}

/*
 * there is only tiny difference between avg_dist_intern_function and sum_function,
 * the output type
 */
static bool avg_dist_intern_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  double *retVal = pCtx->aOutputBuf;

  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) && pCtx->preAggVals.isSet) {
    // Pre-aggregation
    notNullElems = pCtx->size - pCtx->preAggVals.numOfNullPoints;
    assert(notNullElems >= 0);

    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      *retVal += pCtx->preAggVals.sum;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      *retVal += *(double *)&(pCtx->preAggVals.sum);
    } else {
      return false;
    }

    goto _sum_over;
  }

  void *pData = GET_INPUT_CHAR(pCtx);

  if (pCtx->hasNullValue) {
    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_ADD_N(*retVal, pCtx->size, pData, int8_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_ADD_N(*retVal, pCtx->size, pData, int16_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_ADD_N(*retVal, pCtx->size, pData, int32_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      LIST_ADD_N(*retVal, pCtx->size, pData, int64_t, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N(*retVal, pCtx->size, pData, double, notNullElems, pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N(*retVal, pCtx->size, pData, float, notNullElems, pCtx->inputType);
    }
  } else {
    notNullElems = pCtx->size;

    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, int8_t);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, int16_t);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, int32_t);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, int64_t);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, double);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      TYPED_LIST_ADD(*retVal, pCtx->size, pData, float);
    }
  }

_sum_over:
  if (notNullElems > 0) {
    SET_VAL(pCtx, notNullElems, 1);
    SAvgRuntime *pAvgRuntime = (SAvgRuntime *)pCtx->aOutputBuf;
    pAvgRuntime->num += notNullElems;

    // the delimiter of ',' is used to denote current buffer has output or not
    pAvgRuntime->valFlag = DATA_SET_FLAG;
  }

  return true;
}

static bool avg_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  SAvgRuntime *pDest = (SAvgRuntime *)pCtx->aOutputBuf;

  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    pDest->sum += *(int8_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    pDest->sum += *(int16_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    pDest->sum += *(int32_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    pDest->sum += *(int64_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    pDest->sum += *(double *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    pDest->sum += *(float *)pData;
  }

  // restore sum and count of elements
  pDest->num += 1;
  pDest->valFlag = DATA_SET_FLAG;
  return true;
}

/////////////////////////////////////////////////////////////////////////////////////////////

static bool minMax_function(SQLFunctionCtx *pCtx, char *pOutput, int32_t isMin, int32_t *notNullElems) {
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) && pCtx->preAggVals.isSet) {  // pre-agg
    /* data in current data block are qualified to the query */
    *notNullElems = pCtx->size - pCtx->preAggVals.numOfNullPoints;
    assert(*notNullElems >= 0);

    void *tval = (void *)(isMin ? &pCtx->preAggVals.min : &pCtx->preAggVals.max);
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      int64_t val = *(int64_t *)tval;
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        int8_t *data = (int8_t *)pOutput;
        *data = (*data < val) ^ isMin ? val : *data;
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        int16_t *data = (int16_t *)pOutput;
        *data = (*data < val) ^ isMin ? val : *data;
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        int32_t *data = (int32_t *)pOutput;
        *data = (*data < val) ^ isMin ? val : *data;
#if defined(_DEBUG_VIEW)
        pTrace("max value updated according to pre-cal:%d", *data);
#endif

      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        int64_t *data = (int64_t *)pOutput;
        *data = (*data < val) ^ isMin ? val : *data;
      }
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      double *data = (double *)pOutput;
      double  val = *(double *)tval;

      *data = (*data < val) ^ isMin ? val : *data;
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      float *data = (float *)pOutput;
      double val = *(double *)tval;

      *data = (*data < val) ^ isMin ? val : *data;
    } else {
      return false;
    }

    return true;
  }

  void *p = GET_INPUT_CHAR(pCtx);
  if (pCtx->hasNullValue) {
    *notNullElems = 0;
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        TYPED_LOOPCHECK_N(int8_t, pOutput, p, pCtx->size, pCtx->inputType, isMin, *notNullElems);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        TYPED_LOOPCHECK_N(int16_t, pOutput, p, pCtx->size, pCtx->inputType, isMin, *notNullElems);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        int32_t *pData = p;
        int32_t *retVal = pOutput;

        for (int32_t i = 0; i < pCtx->size; ++i) {
          if (isNull(&pData[i], pCtx->inputType)) {
            continue;
          }

          *retVal = ((*retVal < pData[i]) ^ isMin) ? pData[i] : *retVal;
          *notNullElems += 1;
        }
#if defined(_DEBUG_VIEW)
        pTrace("max value updated:%d", *retVal);
#endif
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        TYPED_LOOPCHECK_N(int64_t, pOutput, p, pCtx->size, pCtx->inputType, isMin, *notNullElems);
      }

    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      TYPED_LOOPCHECK_N(double, pOutput, p, pCtx->size, pCtx->inputType, isMin, *notNullElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      TYPED_LOOPCHECK_N(float, pOutput, p, pCtx->size, pCtx->inputType, isMin, *notNullElems);
    }
  } else {
    *notNullElems = pCtx->size;
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
        TYPED_LOOPCHECK(int8_t, pOutput, p, pCtx->size, isMin);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
        TYPED_LOOPCHECK(int16_t, pOutput, p, pCtx->size, isMin);
      } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
        int32_t *pData = p;
        int32_t *retVal = pCtx->aOutputBuf;

        for (int32_t i = 0; i < pCtx->size; ++i) {
          *retVal = ((*retVal < pData[i]) ^ isMin) ? pData[i] : *retVal;
        }
      } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
        TYPED_LOOPCHECK(int64_t, pOutput, p, pCtx->size, isMin);
      }

    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      TYPED_LOOPCHECK(double, pOutput, p, pCtx->size, isMin);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      TYPED_LOOPCHECK(float, pOutput, p, pCtx->size, isMin);
    }
  }

  return true;
}

static void min_function_setup(SQLFunctionCtx *pCtx) {
  void *retVal = pCtx->aOutputBuf;
  memset(retVal, 0, pCtx->outputBytes);

  int32_t type = 0;
  if (pCtx->inputType == TSDB_DATA_TYPE_BINARY) {
    type = pCtx->outputType;
  } else {
    type = pCtx->inputType;
  }

  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)retVal) = INT32_MAX;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)retVal) = FLT_MAX;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *((double *)retVal) = DBL_MAX;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)retVal) = INT64_MAX;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)retVal) = INT16_MAX;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)retVal) = INT8_MAX;
      break;
    default:
      pError("illegal data type:%d in min/max query", pCtx->inputType);
  }

  INIT_VAL(pCtx);
}

static void max_function_setup(SQLFunctionCtx *pCtx) {
  void *retVal = pCtx->aOutputBuf;
  memset(retVal, 0, pCtx->outputBytes);

  int32_t type = (pCtx->inputType == TSDB_DATA_TYPE_BINARY) ? pCtx->outputType : pCtx->inputType;

  switch (type) {
    case TSDB_DATA_TYPE_INT:
      *((int32_t *)retVal) = INT32_MIN;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      *((float *)retVal) = -FLT_MIN;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      *((double *)retVal) = -DBL_MIN;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      *((int64_t *)retVal) = INT64_MIN;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      *((int16_t *)retVal) = INT16_MIN;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      *((int8_t *)retVal) = INT8_MIN;
      break;
    default:
      pError("illegal data type:%d in min/max query", pCtx->inputType);
  }

  INIT_VAL(pCtx);
}

static bool min_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  bool    ret = minMax_function(pCtx, pCtx->aOutputBuf, 1, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);
  return ret;
}

static bool max_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  bool    ret = minMax_function(pCtx, pCtx->aOutputBuf, 0, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);
  return ret;
}

static bool min_dist_intern_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->aOutputBuf, 1, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    pCtx->aOutputBuf[pCtx->inputBytes] = DATA_SET_FLAG;
  }

  return true;
}

static bool max_dist_intern_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  minMax_function(pCtx, pCtx->aOutputBuf, 0, &notNullElems);

  SET_VAL(pCtx, notNullElems, 1);

  if (notNullElems > 0) {
    pCtx->aOutputBuf[pCtx->inputBytes] = DATA_SET_FLAG;
  }

  return true;
}

static int32_t minmax_dist_merge_impl(SQLFunctionCtx *pCtx, int32_t bytes, char *output, bool isMin) {
  int32_t notNullElems = 0;

  int32_t type = (pCtx->inputType != TSDB_DATA_TYPE_BINARY) ? pCtx->inputType : pCtx->outputType;
  for (int32_t i = 0; i < pCtx->size; ++i) {
    char *input = GET_INPUT_CHAR_INDEX(pCtx, i);
    if (input[bytes] != DATA_SET_FLAG) {
      continue;
    }

    notNullElems++;
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t v = *(int8_t *)input;
        if ((*(int8_t *)output < v) ^ isMin) {
          *(int8_t *)output = v;
        }
        break;
      };
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t v = *(int16_t *)input;
        if ((*(int16_t *)output < v) ^ isMin) {
          *(int16_t *)output = v;
        }
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t v = *(int32_t *)input;
        if ((*(int32_t *)output < v) ^ isMin) {
          *(int32_t *)output = v;
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float v = *(float *)input;
        if ((*(float *)output < v) ^ isMin) {
          *(float *)output = v;
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double v = *(double *)input;
        if ((*(double *)output < v) ^ isMin) {
          *(double *)output = v;
        }
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t v = *(int64_t *)input;
        if ((*(int64_t *)output < v) ^ isMin) {
          *(int64_t *)output = v;
        }
        break;
      };
      default:
        break;
    }
  }

  return notNullElems;
}

static void min_dist_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_dist_merge_impl(pCtx, pCtx->inputBytes, pCtx->aOutputBuf, 1);

  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    (pCtx->aOutputBuf)[pCtx->inputBytes] = DATA_SET_FLAG;
    pCtx->numOfIteratedElems += notNullElems;
  }
}

static void min_dist_second_merge(SQLFunctionCtx *pCtx) {
  char *  output = (char *)pCtx->aOutputBuf;
  int32_t notNullElems = minmax_dist_merge_impl(pCtx, pCtx->outputBytes, output, 1);

  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    pCtx->numOfIteratedElems += notNullElems;
  }
}

static void max_dist_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_dist_merge_impl(pCtx, pCtx->inputBytes, pCtx->aOutputBuf, 0);

  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    (pCtx->aOutputBuf)[pCtx->inputBytes] = DATA_SET_FLAG;
    pCtx->numOfIteratedElems += notNullElems;
  }
}

static void max_dist_second_merge(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = minmax_dist_merge_impl(pCtx, pCtx->outputBytes, pCtx->aOutputBuf, 0);

  SET_VAL(pCtx, notNullElems, 1);
  if (notNullElems > 0) {
    pCtx->numOfIteratedElems += notNullElems;
  }
}

static bool minMax_function_f(SQLFunctionCtx *pCtx, int32_t index, int32_t isMin) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);

  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    int8_t *output = (int8_t *)pCtx->aOutputBuf;
    int8_t  i = *(int8_t *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    int16_t *output = pCtx->aOutputBuf;
    int16_t  i = *(int16_t *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    int32_t *output = pCtx->aOutputBuf;
    int32_t  i = *(int32_t *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    int64_t *output = pCtx->aOutputBuf;
    int64_t  i = *(int64_t *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    float *output = pCtx->aOutputBuf;
    float  i = *(float *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *output = pCtx->aOutputBuf;
    double  i = *(double *)pData;
    *output = ((*output < i) ^ isMin) ? i : *output;
  }

  return true;
}

static bool min_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  return minMax_function_f(pCtx, index, 1);
}

static bool max_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  return minMax_function_f(pCtx, index, 0);
}

static bool min_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  min_function_f(pCtx, index);
  if (pCtx->numOfIteratedElems) {
    (pCtx->aOutputBuf)[pCtx->inputBytes] = DATA_SET_FLAG;
  }

  return true;
}

static bool max_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  max_function_f(pCtx, index);
  if (pCtx->numOfIteratedElems) {
    ((char *)pCtx->aOutputBuf)[pCtx->inputBytes] = DATA_SET_FLAG;
  }

  return true;
}

#define LOOP_STDDEV_IMPL(type, r, d, n, delta, tsdbType) \
  for (int32_t i = 0; i < (n); ++i) {                    \
    if (isNull((char *)&((type *)d)[i], tsdbType)) {     \
      continue;                                          \
    }                                                    \
    (r) += tPow(((type *)d)[i] - (delta));               \
  }

static bool stddev_function(SQLFunctionCtx *pCtx) {
  if (pCtx->currentStage == 0) {
    /* the first stage to calculate average value */
    return sum_function(pCtx);
  } else {
    /* the second stage to calculate standard deviation */
    double *retVal = pCtx->aOutputBuf;
    double  avg = pCtx->intermediateBuf[1].dKey;
    void *  pData = GET_INPUT_CHAR(pCtx);

    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        for (int32_t i = 0; i < pCtx->size; ++i) {
          if (isNull(&((int32_t *)pData)[i], pCtx->inputType)) {
            continue;
          }
          *retVal += tPow(((int32_t *)pData)[i] - avg);
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        LOOP_STDDEV_IMPL(float, *retVal, pData, pCtx->size, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        LOOP_STDDEV_IMPL(double, *retVal, pData, pCtx->size, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        LOOP_STDDEV_IMPL(int64_t, *retVal, pData, pCtx->size, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        LOOP_STDDEV_IMPL(int16_t, *retVal, pData, pCtx->size, avg, pCtx->inputType);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        LOOP_STDDEV_IMPL(int8_t, *retVal, pData, pCtx->size, avg, pCtx->inputType);
        break;
      }
      default:
        pError("stddev function not support data type:%d", pCtx->inputType);
    }

    return true;
  }
}

static bool stddev_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->currentStage == 0) {
    /* the first stage is to calculate average value */
    return sum_function_f(pCtx, index);
  } else {
    /* the second stage to calculate standard deviation */
    double *retVal = pCtx->aOutputBuf;
    double  avg = pCtx->intermediateBuf[1].dKey;

    void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
    if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
      return true;
    }

    switch (pCtx->inputType) {
      case TSDB_DATA_TYPE_INT: {
        *retVal += tPow((*(int32_t *)pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        *retVal += tPow((*(float *)pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        *retVal += tPow((*(double *)pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        *retVal += tPow((*(int64_t *)pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        *retVal += tPow((*(int16_t *)pData) - avg);
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        *retVal += tPow((*(int8_t *)pData) - avg);
        break;
      }
      default:
        pError("stddev function not support data type:%d", pCtx->inputType);
    }

    return true;
  }
}

static bool stddev_next_step(SQLFunctionCtx *pCtx) {
  if (pCtx->currentStage == 0) {
    /*
     * stddev is calculated in two stage:
     * 1. get the average value of all points;
     * 2. get final result, based on the average values;
     * so, if this routine is in second stage, no further step is required
     */

    ++pCtx->currentStage;
    avg_finalizer(pCtx);

    // save average value into tmpBuf, for second stage scan
    pCtx->intermediateBuf[1].dKey = ((double *)pCtx->aOutputBuf)[0];
    *((double *)pCtx->aOutputBuf) = 0;
    return true;
  }
  return false;
}

static void stddev_finalizer(SQLFunctionCtx *pCtx) {
  if (pCtx->numOfIteratedElems <= 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
    return;
  }

  double *retValue = (double *)pCtx->aOutputBuf;
  *retValue = sqrt(*retValue / pCtx->numOfIteratedElems);
}

static bool first_function(SQLFunctionCtx *pCtx) {
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) || pCtx->order == TSQL_SO_DESC) {
    return true;
  }

  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    // handle the null value
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(data, pCtx->inputType)) {
        continue;
      }

      memcpy(pCtx->aOutputBuf, data, pCtx->inputBytes);
      notNullElems++;
      break;
    }
  } else {
    char *pData = GET_INPUT_CHAR_INDEX(pCtx, 0);
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);

    notNullElems = pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return notNullElems <= 0;
}

static bool first_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->order == TSQL_SO_DESC) {
    return true;
  }

  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);

  // no need to re-enter current data block
  return false;
}

static void first_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  char *   retVal = pCtx->aOutputBuf;
  int64_t *timestamp = pCtx->ptsList;

  if (IS_DATA_NOT_ASSIGNED(pCtx) || timestamp[index] < *(int64_t *)retVal) {
    *((int64_t *)retVal) = timestamp[index];
    retVal[TSDB_KEYSIZE] = DATA_SET_FLAG;
    memcpy(&retVal[TSDB_KEYSIZE + DATA_SET_FLAG_SIZE], pData, pCtx->inputBytes);
    SET_DATA_ASSIGNED(pCtx);
  }
}

/*
 * format of intermediate result: "timestamp,value" need to compare the timestamp in the first part (before the comma) to
 * decide if the value is earlier than current intermediate result
 */
static bool first_dist_function(SQLFunctionCtx *pCtx) {
  if (pCtx->size == 0) {
    return true;
  }

  /*
   * do not to check data in the following cases:
   * 1. data block that are not loaded
   * 2. scan data files in desc order
   */
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) || pCtx->order == TSQL_SO_DESC) {
    return true;
  }

  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    int32_t i = 0;
    // find the first not null value
    while (i < pCtx->size) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (!isNull(data, pCtx->inputType)) {
        break;
      }
      i++;
    }

    if (i < pCtx->size) {
      char *pData = GET_INPUT_CHAR_INDEX(pCtx, i);
      first_data_assign_impl(pCtx, pData, i);
      notNullElems++;
    } else {
      // no data, all data are null
      // do nothing
    }
  } else {
    char *pData = GET_INPUT_CHAR(pCtx);
    first_data_assign_impl(pCtx, pData, 0);
    notNullElems = pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool first_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->size == 0) {
    return true;
  }

  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  if (pCtx->order == TSQL_SO_DESC) {
    return true;
  }

  first_data_assign_impl(pCtx, pData, 0);

  SET_VAL(pCtx, 1, 1);
  return true;
}

static void first_dist_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);

  if (pData[TSDB_KEYSIZE] != DATA_SET_FLAG) {
    return;
  }

  if (IS_DATA_NOT_ASSIGNED(pCtx) || *(int64_t *)pData < *(int64_t *)pCtx->aOutputBuf) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes + TSDB_KEYSIZE + DATA_SET_FLAG_SIZE);
    SET_DATA_ASSIGNED(pCtx);
  }

  pCtx->numOfIteratedElems += 1;
}

static void first_dist_second_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);

  if (pData[TSDB_KEYSIZE] != DATA_SET_FLAG) {
    return;
  }

  /*
   * NOTE: if secondary merge is not continue executed, the detection of if data assigned or not may be failed.
   * Execution on other tables may change the value, since the SQLFunctionCtx is shared by all tables belonged
   * to different groups
   */
  if (pCtx->intermediateBuf[0].i64Key == 0 || *(int64_t *)pData < pCtx->intermediateBuf[0].i64Key) {
    pCtx->intermediateBuf[0].i64Key = *(int64_t *)pData;
    memcpy(pCtx->aOutputBuf, pData + TSDB_KEYSIZE + DATA_SET_FLAG_SIZE, pCtx->outputBytes);
  }

  SET_VAL(pCtx, 1, 1);
}

//////////////////////////////////////////////////////////////////////////////////////////
/*
 *
 * last function problem:
 * 1. since the last block may be all null value, so, we simply access the last block is not valid
 *    each block need to be checked.
 * 2. If numOfNullPoints == pBlock->numOfBlocks, the whole block is empty. Otherwise, there is at
 *    least one data in this block that is not null.
 * 3. we access the data block in ascending order, so comparison is not needed. The later accessed
 *    block must have greater value of timestamp.
 */
static bool last_function(SQLFunctionCtx *pCtx) {
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) || pCtx->order == TSQL_SO_ASC) {
    return true;
  }

  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    /* get the last not NULL records */
    for (int32_t i = pCtx->size - 1; i >= 0; --i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(data, pCtx->inputType)) {
        continue;
      }

      memcpy(pCtx->aOutputBuf, data, pCtx->inputBytes);
      notNullElems++;
      break;
    }
  } else {
    char *pData = GET_INPUT_CHAR_INDEX(pCtx, pCtx->size - 1);
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);

    notNullElems = pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return notNullElems <= 0;
}

static bool last_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->order == TSQL_SO_ASC) {
    return true;
  }

  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);

  // no need to re-enter current data block
  return false;
}

static void last_data_assign_impl(SQLFunctionCtx *pCtx, char *pData, int32_t index) {
  char *   retVal = pCtx->aOutputBuf;
  int64_t *timestamp = pCtx->ptsList;

  if (IS_DATA_NOT_ASSIGNED(pCtx) || *(int64_t *)retVal < timestamp[index]) {
#if defined(_DEBUG_VIEW)
    pTrace("assign index:%d, ts:%lld, val:%d, ", index, timestamp[index], *(int32_t *)pData);
#endif
    *((int64_t *)retVal) = timestamp[index];
    retVal[TSDB_KEYSIZE] = DATA_SET_FLAG;
    memcpy(&retVal[TSDB_KEYSIZE + DATA_SET_FLAG_SIZE], pData, pCtx->inputBytes);
    SET_DATA_ASSIGNED(pCtx);
  }
}

static bool last_dist_function(SQLFunctionCtx *pCtx) {
  if (pCtx->size == 0) {
    return true;
  }

  /*
   * 1. for scan data in asc order, no need to check data
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus) || pCtx->order == TSQL_SO_ASC) {
    return true;
  }

  int32_t notNullElems = 0;
  if (pCtx->hasNullValue) {
    int32_t i = pCtx->size - 1;
    while (i >= 0) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (!isNull(data, pCtx->inputType)) {
        break;
      }

      i--;
    }

    if (i < 0) {
      /* all data in current block are NULL, do nothing */
    } else {
      char *pData = GET_INPUT_CHAR_INDEX(pCtx, i);
      last_data_assign_impl(pCtx, pData, i);
      notNullElems++;
    }
  } else {
    char *pData = GET_INPUT_CHAR_INDEX(pCtx, pCtx->size - 1);
    last_data_assign_impl(pCtx, pData, pCtx->size - 1);
    notNullElems = pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool last_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  if (pCtx->size == 0) {
    return true;
  }

  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  /*
   * 1. for scan data in asc order, no need to check data
   * 2. for data blocks that are not loaded, no need to check data
   */
  if (pCtx->order == TSQL_SO_ASC) {
    return true;
  }

  last_data_assign_impl(pCtx, pData, index);

  SET_VAL(pCtx, 1, 1);
  return true;
}

static void last_dist_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);
  char *retVal = pCtx->aOutputBuf;

  /* the input data is null */
  if (pData[TSDB_KEYSIZE] != DATA_SET_FLAG) {
    return;
  }

  if (IS_DATA_NOT_ASSIGNED(pCtx) || *(int64_t *)pData > *(int64_t *)retVal) {
    memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes + DATA_SET_FLAG_SIZE + TSDB_KEYSIZE);
    SET_DATA_ASSIGNED(pCtx);
  }

  pCtx->numOfIteratedElems += 1;
}

/*
 * in the secondary merge(local reduce), the output is limited by the
 * final output size, so the main difference between last_dist_merge and second_merge
 * is: the output data format in computing
 */
static void last_dist_second_merge(SQLFunctionCtx *pCtx) {
  char *pData = GET_INPUT_CHAR(pCtx);
  char *retVal = pCtx->aOutputBuf;

  /* the input data is null */
  if (pData[TSDB_KEYSIZE] != DATA_SET_FLAG) {
    return;
  }

  // todo refactor, pls refer to first_dist_second_merge for reasons
  if (pCtx->intermediateBuf[0].i64Key == 0 || *(int64_t *)pData > pCtx->intermediateBuf[0].i64Key) {
    pCtx->intermediateBuf[0].i64Key = *(int64_t *)pData;
    memcpy(retVal, pData + TSDB_KEYSIZE + DATA_SET_FLAG_SIZE, pCtx->outputBytes);
  }

  SET_VAL(pCtx, 1, 1);
}

/*
 * there must be data in last_row_dist function
 */
static int32_t last_row_dist_function(SQLFunctionCtx *pCtx) {
  assert(pCtx->size == 1);

  char *pData = GET_INPUT_CHAR(pCtx);
  *(TSKEY *)pCtx->aOutputBuf = pCtx->intermediateBuf[1].i64Key;

  SET_DATA_ASSIGNED(pCtx);
  assignVal(pCtx->aOutputBuf + TSDB_KEYSIZE + DATA_SET_FLAG_SIZE, pData, pCtx->inputBytes, pCtx->inputType);

  SET_VAL(pCtx, pCtx->size, 1);

  return true;
}

//////////////////////////////////////////////////////////////////////////////////

/*
 * intermediate parameters usage:
 * 1. param[0]: maximum allowable results
 * 2. param[1]: order by type (time or value)
 * 3. param[2]: asc/desc order
 * 4. param[3]: no use
 *
 * 1. intermediateBuf[0]: number of existed results
 * 2. intermediateBuf[1]: results linklist
 * 3. intermediateBuf[2]: no use
 * 4. intermediateBuf[3]: reserved for tags
 *
 */
static void top_bottom_function_setup(SQLFunctionCtx *pCtx) {
  /* top-K value */
  pCtx->intermediateBuf[0].nType = TSDB_DATA_TYPE_BIGINT;
  pCtx->intermediateBuf[0].i64Key = 0;

  /*
   * keep the intermediate results during scan all data blocks
   * in the format of: timestamp|value
   */
  pCtx->intermediateBuf[1].pz = (tValuePair *)calloc(1, sizeof(tValuePair) * pCtx->param[0].i64Key);
  pCtx->intermediateBuf[1].nType = TSDB_DATA_TYPE_BINARY;

  INIT_VAL(pCtx);
}

#define top_add_elem_impl(list, len, val, ts, seg, mx, type)   \
  do {                                                         \
    if (len < mx) {                                            \
      if ((len) == 0 || (val) >= list[(len)-1].v.seg) {        \
        (list)[len].v.nType = (type);                          \
        (list)[len].v.seg = (val);                             \
        (list)[len].timestamp = (ts);                          \
      } else {                                                 \
        int32_t i = (len)-1;                                   \
        while (i >= 0 && (list)[i].v.seg > (val)) {            \
          (list)[i + 1] = (list)[i];                           \
          i -= 1;                                              \
        }                                                      \
        (list)[i + 1].v.nType = (type);                        \
        (list)[i + 1].v.seg = (val);                           \
        (list)[i + 1].timestamp = (ts);                        \
      }                                                        \
      len += 1;                                                \
    } else {                                                   \
      if ((val) > (list)[0].v.seg) {                           \
        int32_t i = 0;                                         \
        while (i + 1 < (len) && (list)[i + 1].v.seg < (val)) { \
          (list)[i] = (list)[i + 1];                           \
          i += 1;                                              \
        }                                                      \
        (list)[i].v.nType = (type);                            \
        (list)[i].v.seg = (val);                               \
        (list)[i].timestamp = (ts);                            \
      }                                                        \
    }                                                          \
  } while (0);

#define bottom_add_elem_impl(list, len, val, ts, seg, mx, type) \
  do {                                                          \
    if (len < mx) {                                             \
      if ((len) == 0) {                                         \
        (list)[len].v.nType = (type);                           \
        (list)[len].v.seg = (val);                              \
        (list)[len].timestamp = (ts);                           \
      } else {                                                  \
        int32_t i = (len)-1;                                    \
        while (i >= 0 && (list)[i].v.seg < (val)) {             \
          (list)[i + 1] = (list)[i];                            \
          i -= 1;                                               \
        }                                                       \
        (list)[i + 1].v.nType = (type);                         \
        (list)[i + 1].v.seg = (val);                            \
        (list)[i + 1].timestamp = (ts);                         \
      }                                                         \
      len += 1;                                                 \
    } else {                                                    \
      if ((val) < (list)[0].v.seg) {                            \
        int32_t i = 0;                                          \
        while (i + 1 < (len) && (list)[i + 1].v.seg > (val)) {  \
          (list)[i] = (list)[i + 1];                            \
          i += 1;                                               \
        }                                                       \
        (list)[i].v.nType = (type);                             \
        (list)[i].v.seg = (val);                                \
        (list)[i].timestamp = (ts);                             \
      }                                                         \
    }                                                           \
  } while (0);

static void top_function_do_add(int32_t *len, int32_t maxLen, tValuePair *pList, void *pData, int64_t *timestamp,
                                uint16_t dataType) {
  switch (dataType) {
    case TSDB_DATA_TYPE_INT: {
      tValuePair *intList = pList;
      int32_t     value = *(int32_t *)pData;
      if (*len < maxLen) {
        if (*len == 0 || value >= intList[*len - 1].v.i64Key) {
          intList[*len].v.nType = dataType;
          intList[*len].v.i64Key = value;
          intList[*len].timestamp = *timestamp;
        } else {
          int32_t i = (*len) - 1;
          while (i >= 0 && intList[i].v.i64Key > value) {
            intList[i + 1] = intList[i];
            i -= 1;
          }
          intList[i + 1].v.nType = dataType;
          intList[i + 1].v.i64Key = value;
          intList[i + 1].timestamp = *timestamp;
        }
        (*len)++;
      } else {
        if (value > pList[0].v.i64Key) {
          int32_t i = 0;
          while (i + 1 < maxLen && pList[i + 1].v.i64Key < value) {
            pList[i] = pList[i + 1];
            i += 1;
          }

          pList[i].v.nType = dataType;
          pList[i].v.i64Key = value;
          pList[i].timestamp = *timestamp;
        }
      }
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      top_add_elem_impl(pList, *len, *(double *)pData, *timestamp, dKey, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      top_add_elem_impl(pList, *len, *(int64_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      top_add_elem_impl(pList, *len, *(float *)pData, *timestamp, dKey, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      top_add_elem_impl(pList, *len, *(int16_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      top_add_elem_impl(pList, *len, *(int8_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
    default:
      pError("top/bottom function not support data type:%d", dataType);
  };
}

static void bottom_function_do_add(int32_t *len, int32_t maxLen, tValuePair *pList, void *pData, int64_t *timestamp,
                                   uint16_t dataType) {
  switch (dataType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t value = *(int32_t *)pData;
      if ((*len) < maxLen) {
        if (*len == 0) {
          pList[*len].v.i64Key = value;
          pList[*len].timestamp = *timestamp;
        } else {
          int32_t i = (*len) - 1;
          while (i >= 0 && pList[i].v.i64Key < value) {
            pList[i + 1] = pList[i];
            i -= 1;
          }
          pList[i + 1].v.i64Key = value;
          pList[i + 1].timestamp = *timestamp;
        }
        (*len)++;
      } else {
        if (value < pList[0].v.i64Key) {
          int32_t i = 0;
          while (i + 1 < maxLen && pList[i + 1].v.i64Key > value) {
            pList[i] = pList[i + 1];
            i += 1;
          }
          pList[i].v.i64Key = value;
          pList[i].timestamp = *timestamp;
        }
      }
    } break;
    case TSDB_DATA_TYPE_DOUBLE: {
      bottom_add_elem_impl(pList, *len, *(double *)pData, *timestamp, dKey, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      bottom_add_elem_impl(pList, *len, *(int64_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      bottom_add_elem_impl(pList, *len, *(float *)pData, *timestamp, dKey, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      bottom_add_elem_impl(pList, *len, *(int16_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      bottom_add_elem_impl(pList, *len, *(int8_t *)pData, *timestamp, i64Key, maxLen, dataType);
      break;
    }
  };
}

static bool top_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      if (isNull(GET_INPUT_CHAR_INDEX(pCtx, i), pCtx->inputType)) {
        continue;
      }
      notNullElems++;
      top_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                          GET_INPUT_CHAR_INDEX(pCtx, i), &pCtx->ptsList[i], pCtx->inputType);
    }
  } else {
    notNullElems = pCtx->size;
    for (int32_t i = 0; i < pCtx->size; ++i) {
      top_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                          GET_INPUT_CHAR_INDEX(pCtx, i), &pCtx->ptsList[i], pCtx->inputType);
    }
  }

  SET_VAL(pCtx, notNullElems, pCtx->intermediateBuf[0].i64Key);
  return true;
}

static bool top_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, pCtx->param[0].i64Key);
  top_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, pData,
                      &pCtx->ptsList[index], pCtx->inputType);

  return true;
}

static bool bottom_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;
  void *  pData = GET_INPUT_CHAR(pCtx);

  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      if (isNull(GET_INPUT_CHAR_INDEX(pCtx, i), pCtx->inputType)) {
        continue;
      }

      bottom_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                             (char *)pData + pCtx->inputBytes * i, &pCtx->ptsList[i], pCtx->inputType);
      notNullElems++;
    }
  } else {
    notNullElems = pCtx->size;
    for (int32_t i = 0; i < pCtx->size; ++i) {
      bottom_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                             (char *)pData + pCtx->inputBytes * i, pCtx->ptsList + i, pCtx->inputType);
    }
  }

  SET_VAL(pCtx, notNullElems, pCtx->intermediateBuf[0].i64Key);
  return true;
}

static bool bottom_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, pCtx->param[0].i64Key);
  bottom_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, pData,
                         pCtx->ptsList + index, pCtx->inputType);

  return true;
}

static int32_t resAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = (tValuePair *)pLeft;
  tValuePair *pRightElem = (tValuePair *)pRight;

  if (pLeftElem->timestamp == pRightElem->timestamp) {
    return 0;
  } else {
    return pLeftElem->timestamp > pRightElem->timestamp ? 1 : -1;
  }
}

static int32_t resDescComparFn(const void *pLeft, const void *pRight) { return -resAscComparFn(pLeft, pRight); }

static int32_t resDataAscComparFn(const void *pLeft, const void *pRight) {
  tValuePair *pLeftElem = (tValuePair *)pLeft;
  tValuePair *pRightElem = (tValuePair *)pRight;

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
  tValuePair *tvp = (tValuePair *)pCtx->intermediateBuf[1].pz;

  // copy to result set buffer
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);

  // in case of second stage merge, always use incremental output.
  if (pCtx->currentStage == SECONDARY_STAGE_MERGE) {
    step = QUERY_ASC_FORWARD_STEP;
  }

  int32_t len = pCtx->numOfOutputElems;

  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *output = (int32_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *output = (int64_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *output = (double *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.dKey;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *output = (float *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.dKey;
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *output = (int16_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.i64Key;
      }
      break;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *output = (int8_t *)pCtx->aOutputBuf;
      for (int32_t i = 0; i < len; ++i, output += step) {
        *output = tvp[i].v.i64Key;
      }
      break;
    }
    default: {
      pError("top/bottom function not support data type:%d", pCtx->inputType);
      return;
    }
  }

  // set the output timestamp of each record.
  TSKEY *output = pCtx->ptsOutputBuf;
  for (int32_t i = 0; i < len; ++i, output += step) {
    *output = tvp[i].timestamp;
  }
}

static void top_bottom_function_finalizer(SQLFunctionCtx *pCtx) {
  /*
   * data in temporary list is less than the required count not enough qualified number of results
   */
  if (pCtx->intermediateBuf[0].i64Key < pCtx->param[0].i64Key) {
    pCtx->numOfOutputElems = pCtx->intermediateBuf[0].i64Key;
  }

  tValuePair *tvp = (tValuePair *)pCtx->intermediateBuf[1].pz;

  // user specify the order of output by sort the result according to timestamp
  if (pCtx->param[1].i64Key == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    __compar_fn_t comparator = (pCtx->param[2].i64Key == TSQL_SO_ASC) ? resAscComparFn : resDescComparFn;
    qsort(tvp, pCtx->numOfOutputElems, sizeof(tValuePair), comparator);
  } else if (pCtx->param[1].i64Key > PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    __compar_fn_t comparator = (pCtx->param[2].i64Key == TSQL_SO_ASC) ? resDataAscComparFn : resDataDescComparFn;
    qsort(tvp, pCtx->numOfOutputElems, sizeof(tValuePair), comparator);
  }

  int32_t type = pCtx->outputType == TSDB_DATA_TYPE_BINARY ? pCtx->inputBytes : pCtx->outputType;
  copyTopBotRes(pCtx, type);

  tfree(pCtx->intermediateBuf[1].pz);
}

typedef struct STopBotRuntime {
  int32_t    num;
  tValuePair res[];
} STopBotRuntime;

bool top_bot_datablock_filter(SQLFunctionCtx *pCtx, int32_t functionId, char *minval, char *maxval) {
  int32_t numOfExistsRes = 0;
  if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_BOTTOM) {
    numOfExistsRes = (int32_t)pCtx->intermediateBuf[0].i64Key;
  } else {
    STopBotRuntime *pTpBtRuntime = (STopBotRuntime *)pCtx->aOutputBuf;
    numOfExistsRes = pTpBtRuntime->num;
  }

  if (numOfExistsRes < pCtx->param[0].i64Key) {
    /* required number of results are not reached, continue load data block */
    return true;
  } else {  //
    tValuePair *pRes = pCtx->intermediateBuf[1].pz;
    if (functionId == TSDB_FUNC_TOP || functionId == TSDB_FUNC_TOP_DST) {
      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          return *(int8_t *)maxval > pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_SMALLINT:
          return *(int16_t *)maxval > pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_INT:
          return *(int32_t *)maxval > pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_BIGINT:
          return *(int64_t *)maxval > pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_FLOAT:
          return *(float *)maxval > pRes[0].v.dKey;
        case TSDB_DATA_TYPE_DOUBLE:
          return *(double *)maxval > pRes[0].v.dKey;
        default:
          return true;
      }
    } else {
      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          return *(int8_t *)minval < pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_SMALLINT:
          return *(int16_t *)minval < pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_INT:
          return *(int32_t *)minval < pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_BIGINT:
          return *(int64_t *)minval < pRes[0].v.i64Key;
        case TSDB_DATA_TYPE_FLOAT:
          return *(float *)minval < pRes[0].v.dKey;
        case TSDB_DATA_TYPE_DOUBLE:
          return *(double *)minval < pRes[0].v.dKey;
        default:
          return true;
      }
    }
  }
}

/*
 * intermediate parameters usage:
 * 1. param[0]: maximum allowable results
 * 2. param[1]: order by type (time or value)
 * 3. param[2]: asc/desc order
 * 4. param[3]: no use
 *
 * 1. intermediateBuf[0]: number of existed results
 * 2. intermediateBuf[1]: results linklist
 * 3. intermediateBuf[2]: no use
 * 4. intermediateBuf[3]: reserved for tags
 *
 */
static void top_bottom_dist_function_setup(SQLFunctionCtx *pCtx) {
  /* top-K value */
  pCtx->intermediateBuf[0].nType = TSDB_DATA_TYPE_BIGINT;
  pCtx->intermediateBuf[0].i64Key = 0;

  if (pCtx->outputType == TSDB_DATA_TYPE_BINARY) {
    STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;
    pCtx->intermediateBuf[1].pz = pRes->res;
    pRes->num = 0;

    memset(pRes, 0, sizeof(STopBotRuntime) + pCtx->param[0].i64Key * sizeof(tValuePair));
  } else {
    /*
     * keep the intermediate results during scan all data blocks in the format of: timestamp|value
     */
    int32_t size = sizeof(tValuePair) * pCtx->param[0].i64Key;
    pCtx->intermediateBuf[1].pz = (tValuePair *)calloc(1, size);
    pCtx->intermediateBuf[1].nType = TSDB_DATA_TYPE_BINARY;
    pCtx->intermediateBuf[1].nLen = size;
  }

  INIT_VAL(pCtx);
}

static bool top_dist_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;
  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      if (isNull(GET_INPUT_CHAR_INDEX(pCtx, i), pCtx->inputType)) {
        continue;
      }
      notNullElems++;
      top_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, GET_INPUT_CHAR_INDEX(pCtx, i),
                          &pCtx->ptsList[i], pCtx->inputType);
    }
  } else {
    notNullElems = pCtx->size;
    for (int32_t i = 0; i < pCtx->size; ++i) {
      top_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, GET_INPUT_CHAR_INDEX(pCtx, i),
                          &pCtx->ptsList[i], pCtx->inputType);
    }
  }

  /* treat the result as only one result */
  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool top_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;

  SET_VAL(pCtx, 1, 1);
  top_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, pData, &pCtx->ptsList[index],
                      pCtx->inputType);

  return true;
}

static void top_dist_merge(SQLFunctionCtx *pCtx) {
  char *input = GET_INPUT_CHAR(pCtx);

  STopBotRuntime *pInput = (STopBotRuntime *)input;
  if (pInput->num <= 0) {
    return;
  }

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;
  for (int32_t i = 0; i < pInput->num; ++i) {
    top_function_do_add(&pRes->num, pCtx->param[0].i64Key, pRes->res, &pInput->res[i].v.i64Key,
                        &pInput->res[i].timestamp, pCtx->inputType);
  }

  pCtx->numOfIteratedElems = pRes->num;
}

static void top_dist_second_merge(SQLFunctionCtx *pCtx) {
  STopBotRuntime *pInput = (STopBotRuntime *)GET_INPUT_CHAR(pCtx);

  /* the intermediate result is binary, we only use the output data type */
  for (int32_t i = 0; i < pInput->num; ++i) {
    top_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                        &pInput->res[i].v.i64Key, &pInput->res[i].timestamp, pCtx->outputType);
  }

  SET_VAL(pCtx, pInput->num, pCtx->intermediateBuf[0].i64Key);
}

static bool bottom_dist_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;
  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      if (isNull(GET_INPUT_CHAR_INDEX(pCtx, i), pCtx->inputType)) {
        continue;
      }
      notNullElems++;
      bottom_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                             GET_INPUT_CHAR_INDEX(pCtx, i), &pCtx->ptsList[i], pCtx->inputType);
    }
  } else {
    notNullElems = pCtx->size;
    for (int32_t i = 0; i < pCtx->size; ++i) {
      bottom_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                             GET_INPUT_CHAR_INDEX(pCtx, i), &pCtx->ptsList[i], pCtx->inputType);
    }
  }

  /* treat the result as only one result */
  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool bottom_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;

  SET_VAL(pCtx, 1, 1);
  bottom_function_do_add(&pRes->num, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz, pData, &pCtx->ptsList[index],
                         pCtx->inputType);

  return true;
}

static void bottom_dist_merge(SQLFunctionCtx *pCtx) {
  STopBotRuntime *pInput = (STopBotRuntime *)GET_INPUT_CHAR(pCtx);
  if (pInput->num <= 0) {
    return;
  }

  STopBotRuntime *pRes = (STopBotRuntime *)pCtx->aOutputBuf;
  for (int32_t i = 0; i < pInput->num; ++i) {
    bottom_function_do_add(&pRes->num, pCtx->param[0].i64Key, pRes->res, &pInput->res[i].v.i64Key,
                           &pInput->res[i].timestamp, pCtx->inputType);
  }

  pCtx->numOfIteratedElems = pRes->num;
}

static void bottom_dist_second_merge(SQLFunctionCtx *pCtx) {
  STopBotRuntime *pInput = (STopBotRuntime *)GET_INPUT_CHAR(pCtx);

  /* the intermediate result is binary, we only use the output data type */
  for (int32_t i = 0; i < pInput->num; ++i) {
    bottom_function_do_add(&pCtx->intermediateBuf[0].i64Key, pCtx->param[0].i64Key, pCtx->intermediateBuf[1].pz,
                           &pInput->res[i].v.i64Key, &pInput->res[i].timestamp, pCtx->outputType);
  }

  SET_VAL(pCtx, pInput->num, pCtx->intermediateBuf[0].i64Key);
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void percentile_function_setup(SQLFunctionCtx *pCtx) {
  pCtx->intermediateBuf[0].nType = TSDB_DATA_TYPE_DOUBLE;

  const int32_t MAX_AVAILABLE_BUFFER_SIZE = 1 << 20;
  const int32_t NUMOFCOLS = 1;

  if (pCtx->intermediateBuf[2].pz != NULL) {
    assert(pCtx->intermediateBuf[1].pz != NULL);
    return;
  }

  SSchema field[1] = {
      {pCtx->inputType, "dummyCol", 0, pCtx->inputBytes},
  };
  tColModel *pModel = tColModelCreate(field, 1, 1000);
  int32_t    orderIdx = 0;

  // tOrderDesc object
  pCtx->intermediateBuf[2].pz = tOrderDesCreate(&orderIdx, NUMOFCOLS, pModel, TSQL_SO_DESC);

  tMemBucketCreate((tMemBucket **)&(pCtx->intermediateBuf[1].pz), 1024, MAX_AVAILABLE_BUFFER_SIZE, pCtx->inputBytes,
                   pCtx->inputType, (tOrderDescriptor *)pCtx->intermediateBuf[2].pz);

  INIT_VAL(pCtx);
}

static bool percentile_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(data, pCtx->inputType)) {
        continue;
      }

      notNullElems += 1;
      tMemBucketPut((tMemBucket *)(pCtx->intermediateBuf[1].pz), data, 1);
    }
  } else {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      tMemBucketPut((tMemBucket *)(pCtx->intermediateBuf[1].pz), data, 1);
    }

    notNullElems += pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool percentile_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  tMemBucketPut((tMemBucket *)(pCtx->intermediateBuf[1].pz), pData, 1);
  return true;
}

static void percentile_finalizer(SQLFunctionCtx *pCtx) {
  double v = pCtx->param[0].nType == TSDB_DATA_TYPE_INT ? pCtx->param[0].i64Key : pCtx->param[0].dKey;

  if (pCtx->numOfIteratedElems > 0) {  // check for null
    double val = getPercentile(pCtx->intermediateBuf[1].pz, v);
    *((double *)pCtx->aOutputBuf) = val;
  } else {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
  }

  tMemBucketDestroy((tMemBucket **)&(pCtx->intermediateBuf[1].pz));
  tOrderDescDestroy(pCtx->intermediateBuf[2].pz);

  pCtx->intermediateBuf[1].pz = NULL;
  pCtx->intermediateBuf[2].pz = NULL;
}

static bool apercentile_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(data, pCtx->inputType)) {
        continue;
      }

      notNullElems += 1;
      double v = 0;

      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          v = *(int8_t *)data;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          v = *(int16_t *)data;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          v = *(int64_t *)data;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          v = *(float *)data;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          v = *(double *)data;
          break;
        default:
          v = *(int32_t *)data;
          break;
      }

      tHistogramAdd(&pCtx->param[1].pz, v);
    }
  } else {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char * data = GET_INPUT_CHAR_INDEX(pCtx, i);
      double v = 0;

      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          v = *(int8_t *)data;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          v = *(int16_t *)data;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          v = *(int64_t *)data;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          v = *(float *)data;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          v = *(double *)data;
          break;
        default:
          v = *(int32_t *)data;
          break;
      }

      tHistogramAdd(&pCtx->param[1].pz, v);
    }

    notNullElems += pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool apercentile_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  double v = 0;
  SET_VAL(pCtx, 1, 1);

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT:
      v = *(int8_t *)pData;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      v = *(int16_t *)pData;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      v = *(int64_t *)pData;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      v = *(float *)pData;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      v = *(double *)pData;
      break;
    default:
      v = *(int32_t *)pData;
      break;
  }

  tHistogramAdd(&pCtx->param[1].pz, v);
  return true;
}

static void apercentile_finalizer(SQLFunctionCtx *pCtx) {
  double v = pCtx->param[0].nType == TSDB_DATA_TYPE_INT ? pCtx->param[0].i64Key : pCtx->param[0].dKey;

  if (pCtx->numOfIteratedElems > 0) {  // check for null
    double  ratio[] = {v};
    double *res = tHistogramUniform(pCtx->param[1].pz, ratio, 1);
    memcpy(pCtx->aOutputBuf, res, sizeof(double));
    free(res);
  } else {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
  }

  SET_VAL(pCtx, pCtx->numOfIteratedElems, 1);
  tHistogramDestroy(&(pCtx->param[1].pz));
}

static void apercentile_dist_function_setup(SQLFunctionCtx *pCtx) {
  function_setup(pCtx);
  if (pCtx->outputType == TSDB_DATA_TYPE_BINARY) {
    tHistogramCreateFrom(pCtx->aOutputBuf, MAX_HISTOGRAM_BIN);
  } else { /* for secondary merge at client-side */
    pCtx->param[1].pz = tHistogramCreate(MAX_HISTOGRAM_BIN);
  }
}

static bool apercentile_dist_intern_function(SQLFunctionCtx *pCtx) {
  int32_t notNullElems = 0;

  SHistogramInfo *pHisto = (SHistogramInfo *)pCtx->aOutputBuf;

  if (pCtx->hasNullValue) {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char *data = GET_INPUT_CHAR_INDEX(pCtx, i);
      if (isNull(data, pCtx->inputType)) {
        continue;
      }

      notNullElems += 1;
      double v = 0;

      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          v = *(int8_t *)data;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          v = *(int16_t *)data;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          v = *(int64_t *)data;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          v = *(float *)data;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          v = *(double *)data;
          break;
        default:
          v = *(int32_t *)data;
          break;
      }

      tHistogramAdd(&pHisto, v);
    }
  } else {
    for (int32_t i = 0; i < pCtx->size; ++i) {
      char * data = GET_INPUT_CHAR_INDEX(pCtx, i);
      double v = 0;

      switch (pCtx->inputType) {
        case TSDB_DATA_TYPE_TINYINT:
          v = *(int8_t *)data;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          v = *(int16_t *)data;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          v = *(int64_t *)data;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          v = *(float *)data;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          v = *(double *)data;
          break;
        default:
          v = *(int32_t *)data;
          break;
      }

      tHistogramAdd(&pHisto, v);
    }

    notNullElems += pCtx->size;
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool apercentile_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SHistogramInfo *pHisto = (SHistogramInfo *)pCtx->aOutputBuf;
  SET_VAL(pCtx, 1, 1);

  double v = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_TINYINT:
      v = *(int8_t *)pData;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      v = *(int16_t *)pData;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      v = *(int64_t *)pData;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      v = *(float *)pData;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      v = *(double *)pData;
      break;
    default:
      v = *(int32_t *)pData;
      break;
  }

  tHistogramAdd(&pHisto, v);
  return true;
}

static void apercentile_dist_merge(SQLFunctionCtx *pCtx) {
  SHistogramInfo *pInput = (SHistogramInfo *)GET_INPUT_CHAR(pCtx);
  if (pInput->numOfElems <= 0) {
    return;
  }

  size_t size = sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1);

  SHistogramInfo *pHisto = (SHistogramInfo *)pCtx->aOutputBuf;
  if (pHisto->numOfElems <= 0) {
    char *ptr = pHisto->elems;
    memcpy(pHisto, pInput, size);
    pHisto->elems = ptr;
  } else {
    pInput->elems = (char *)pInput + sizeof(SHistogramInfo);
    pHisto->elems = (char *)pHisto + sizeof(SHistogramInfo);

    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput, MAX_HISTOGRAM_BIN);
    memcpy(pHisto, pRes, size);

    pHisto->elems = (char *)pHisto + sizeof(SHistogramInfo);
    tHistogramDestroy(&pRes);
  }

  pCtx->numOfIteratedElems += 1;
}

static void apercentile_dist_second_merge(SQLFunctionCtx *pCtx) {
  SHistogramInfo *pInput = (SHistogramInfo *)GET_INPUT_CHAR(pCtx);
  if (pInput->numOfElems <= 0) {
    return;
  }

  SHistogramInfo *pHisto = (SHistogramInfo *)pCtx->param[1].pz;
  if (pHisto->numOfElems <= 0) {
    memcpy(pHisto, pInput, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
    pHisto->elems = (char *)pHisto + sizeof(SHistogramInfo);
  } else {
    pInput->elems = (char *)pInput + sizeof(SHistogramInfo);
    pHisto->elems = (char *)pHisto + sizeof(SHistogramInfo);

    SHistogramInfo *pRes = tHistogramMerge(pHisto, pInput, MAX_HISTOGRAM_BIN);
    tHistogramDestroy(&pCtx->param[1].pz);
    pCtx->param[1].pz = pRes;
  }

  pCtx->numOfIteratedElems += 1;
}

static void leastsquares_function_setup(SQLFunctionCtx *pCtx) {
  if (pCtx->intermediateBuf[1].pz != NULL) {
    return;
  }
  // 2*3 matrix
  INIT_VAL(pCtx);
  pCtx->intermediateBuf[1].pz = (double(*)[3])calloc(1, sizeof(double) * 2 * 3);

  // set the start x-axle value
  pCtx->intermediateBuf[0].dKey = pCtx->param[0].dKey;
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
  for (int32_t i = 0; i < ctx->size; ++i) {                    \
    if (isNull((char *)&(y)[i], tsdbType)) {                   \
      continue;                                                \
    }                                                          \
    n++;                                                       \
    LEASTSQR_CAL(param, x, y, i, step);                        \
  }

static bool leastsquares_function(SQLFunctionCtx *pCtx) {
  double(*param)[3] = (double(*)[3])pCtx->intermediateBuf[1].pz;

  double x = pCtx->intermediateBuf[0].dKey;
  void * pData = GET_INPUT_CHAR(pCtx);

  int32_t numOfElem = 0;
  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *p = pData;
      //            LEASTSQR_CAL_LOOP(pCtx, param, pParamData, p);
      for (int32_t i = 0; i < pCtx->size; ++i) {
        if (isNull(p, pCtx->inputType)) {
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

  pCtx->intermediateBuf[0].dKey = x;

  SET_VAL(pCtx, numOfElem, 1);
  return true;
}

static bool leastsquares_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);
  double(*param)[3] = (double(*)[3])pCtx->intermediateBuf[1].pz;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *p = pData;
      LEASTSQR_CAL(param, pCtx->intermediateBuf[0].dKey, p, index, pCtx->param[1].dKey);
      break;
    }
    default:
      pError("error data type in leastsquares function:%d", pCtx->inputType);
  };

  return true;
}

static void leastsquare_finalizer(SQLFunctionCtx *pCtx) {
  /* no data in query */
  if (pCtx->numOfIteratedElems <= 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
    tfree(pCtx->intermediateBuf[1].pz);

    return;
  }

  double(*param)[3] = (double(*)[3])pCtx->intermediateBuf[1].pz;
  param[1][1] = pCtx->numOfIteratedElems;
  param[1][0] = param[0][1];

  param[0][0] -= param[1][0] * (param[0][1] / param[1][1]);
  param[0][2] -= param[1][2] * (param[0][1] / param[1][1]);
  param[0][1] = 0;
  param[1][2] -= param[0][2] * (param[1][0] / param[0][0]);
  param[1][0] = 0;
  param[0][2] /= param[0][0];

  param[1][2] /= param[1][1];

  sprintf(pCtx->aOutputBuf, "(%lf, %lf)", param[0][2], param[1][2]);
  tfree(pCtx->intermediateBuf[1].pz);
}

static bool date_col_output_function(SQLFunctionCtx *pCtx) {
  if (pCtx->scanFlag == SUPPLEMENTARY_SCAN) {
    return true;
  }

  SET_VAL(pCtx, pCtx->size, 1);
  *(int64_t *)(pCtx->aOutputBuf) = pCtx->nStartQueryTimestamp;
  return true;
}

static bool col_project_function(SQLFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size, pCtx->size);

  char *pDest = 0;
  if (pCtx->order == TSQL_SO_ASC) {
    pDest = pCtx->aOutputBuf;
  } else {
    pDest = pCtx->aOutputBuf - (pCtx->size - 1) * pCtx->inputBytes;
  }

  char *pData = GET_INPUT_CHAR(pCtx);
  memcpy(pDest, pData, (size_t)pCtx->size * pCtx->inputBytes);

  pCtx->aOutputBuf += pCtx->size * pCtx->outputBytes * GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  return true;
}

static bool col_project_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  INC_INIT_VAL(pCtx, 1, 1);

  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  memcpy(pCtx->aOutputBuf, pData, pCtx->inputBytes);

  pCtx->aOutputBuf += pCtx->inputBytes * GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  return true;
}

/**
 * only used for tag projection query in select clause
 * @param pCtx
 * @return
 */
static bool tag_project_function(SQLFunctionCtx *pCtx) {
  INC_INIT_VAL(pCtx, pCtx->size, pCtx->size);

  assert(pCtx->inputBytes == pCtx->outputBytes);
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);

  for (int32_t i = 0; i < pCtx->size; ++i) {
    tVariantDump(&pCtx->intermediateBuf[3], pCtx->aOutputBuf, pCtx->intermediateBuf[3].nType);
    pCtx->aOutputBuf += pCtx->outputBytes * factor;
  }
  return true;
}

static bool tag_project_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  INC_INIT_VAL(pCtx, 1, 1);
  tVariantDump(&pCtx->intermediateBuf[3], pCtx->aOutputBuf, pCtx->intermediateBuf[3].nType);
  pCtx->aOutputBuf += pCtx->outputBytes * GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  return true;
}

/**
 * used in group by clause. when applying group by tags, the tags value is
 * assign
 * by using tag function.
 * NOTE: there is only ONE output for ONE query range
 * @param pCtx
 * @return
 */
static bool tag_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, 1, 1);
  tVariantDump(&pCtx->intermediateBuf[3], pCtx->aOutputBuf, pCtx->intermediateBuf[3].nType);
  return true;
}

static bool tag_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  SET_VAL(pCtx, 1, 1);
  tVariantDump(&pCtx->intermediateBuf[3], pCtx->aOutputBuf, pCtx->intermediateBuf[3].nType);
  return true;
}

static bool copy_function(SQLFunctionCtx *pCtx) {
  SET_VAL(pCtx, pCtx->size, 1);

  char *pData = GET_INPUT_CHAR(pCtx);
  assignVal(pCtx->aOutputBuf, pData, pCtx->inputBytes, pCtx->inputType);
  return false;
}

enum {
  INITIAL_VALUE_NOT_ASSIGNED = 0,
};

static void diff_function_setup(SQLFunctionCtx *pCtx) {
  function_setup(pCtx);
  pCtx->intermediateBuf[1].nType = INITIAL_VALUE_NOT_ASSIGNED;
  // diff function require the value is set to -1
}

// TODO difference in date column
static bool diff_function(SQLFunctionCtx *pCtx) {
  pCtx->numOfIteratedElems += pCtx->size;

  void *pData = GET_INPUT_CHAR(pCtx);
  bool  isFirstBlock = (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED);

  int32_t notNullElems = 0;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);

  int32_t i = (pCtx->order == TSQL_SO_ASC) ? 0 : pCtx->size - 1;
  TSKEY * pTimestamp = pCtx->ptsOutputBuf;

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      int32_t *pDd = (int32_t *)pData;
      int32_t *pOutput = (int32_t *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull(&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].i64Key = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if ((i == 0 && pCtx->order == TSQL_SO_ASC) || (i == pCtx->size - 1 && pCtx->order == TSQL_SO_DESC)) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - step];
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        }

        pCtx->intermediateBuf[1].i64Key = pDd[i];
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *pDd = (int64_t *)pData;
      int64_t *pOutput = (int64_t *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull(&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].i64Key = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if (i == 0) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];

          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - 1];
          *pTimestamp = pCtx->ptsList[i];

          pOutput += step;
          pTimestamp += step;
        }

        pCtx->intermediateBuf[1].i64Key = pDd[i];
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      double *pDd = (double *)pData;
      double *pOutput = (double *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull(&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].dKey = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if (i == 0) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].dKey;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - 1];
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        }

        memcpy(&pCtx->intermediateBuf[1].i64Key, &pDd[i], pCtx->inputBytes);
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      float *pDd = (float *)pData;
      float *pOutput = (float *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull(&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].dKey = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if (i == 0) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].dKey;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - 1];
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        }

        // keep the last value, the remain may be all null
        pCtx->intermediateBuf[1].dKey = pDd[i];
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *pDd = (int16_t *)pData;
      int16_t *pOutput = (int16_t *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull(&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].i64Key = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if (i == 0) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - 1];
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        }

        pCtx->intermediateBuf[1].i64Key = pDd[i];
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *pDd = (int8_t *)pData;
      int8_t *pOutput = (int8_t *)pCtx->aOutputBuf;

      for (; i < pCtx->size && i >= 0; i += step) {
        if (isNull((char *)&pDd[i], pCtx->inputType)) {
          continue;
        }

        if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
          pCtx->intermediateBuf[1].i64Key = pDd[i];
          pCtx->intermediateBuf[1].nType = pCtx->inputType;
        } else if (i == 0) {
          *pOutput = pDd[i] - pCtx->intermediateBuf[1].i64Key;
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        } else {
          *pOutput = pDd[i] - pDd[i - 1];
          *pTimestamp = pCtx->ptsList[i];
          pOutput += step;
          pTimestamp += step;
        }

        pCtx->intermediateBuf[1].i64Key = pDd[i];
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        notNullElems++;
      }
      break;
    };
    default:
      pError("error input type");
  }

  if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED ||
      notNullElems <= 0) {  // initial value is not set yet
                            /*
                             * 1. current block and blocks before are full of null
                             * 2. current block may be null value
                             */
    assert(pCtx->hasNullValue);
  } else {
    if (isFirstBlock) {
      pCtx->numOfOutputElems = notNullElems - 1;
    } else {
      pCtx->numOfOutputElems += notNullElems;
    }

    int32_t forwardStep = (isFirstBlock) ? notNullElems - 1 : notNullElems;

    pCtx->aOutputBuf = pCtx->aOutputBuf + forwardStep * pCtx->outputBytes * step;
    pCtx->ptsOutputBuf = (char *)pCtx->ptsOutputBuf + forwardStep * TSDB_KEYSIZE * step;
  }

  return true;
}

#define TYPE_DIFF_IMPL(ctx, d, type)                                                        \
  do {                                                                                      \
    if (ctx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {                      \
      ctx->intermediateBuf[1].nType = ctx->inputType;                                       \
      *(type *)&ctx->intermediateBuf[1].i64Key = *(type *)d;                                \
    } else {                                                                                \
      *(type *)ctx->aOutputBuf = *(type *)d - (*(type *)(&ctx->intermediateBuf[1].i64Key)); \
      *(type *)(&ctx->intermediateBuf[1].i64Key) = *(type *)d;                              \
      *(int64_t *)ctx->ptsOutputBuf = *(int64_t *)(ctx->ptsList + (TSDB_KEYSIZE)*index);    \
    }                                                                                       \
  } while (0);

static bool diff_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  char *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  pCtx->numOfIteratedElems += 1;
  if (pCtx->intermediateBuf[1].nType != INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is set
    pCtx->numOfOutputElems += 1;
  }

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(pCtx->order);

  switch (pCtx->inputType) {
    case TSDB_DATA_TYPE_INT: {
      if (pCtx->intermediateBuf[1].nType == INITIAL_VALUE_NOT_ASSIGNED) {  // initial value is not set yet
        pCtx->intermediateBuf[1].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = *(int32_t *)pData;
      } else {
        *(int32_t *)pCtx->aOutputBuf = *(int32_t *)pData - pCtx->intermediateBuf[1].i64Key;
        pCtx->intermediateBuf[1].i64Key = *(int32_t *)pData;
        *(int64_t *)pCtx->ptsOutputBuf = pCtx->ptsList[index];
      }
      break;
    };
    case TSDB_DATA_TYPE_BIGINT: {
      TYPE_DIFF_IMPL(pCtx, pData, int64_t);
      break;
    };
    case TSDB_DATA_TYPE_DOUBLE: {
      TYPE_DIFF_IMPL(pCtx, pData, double);
      break;
    };
    case TSDB_DATA_TYPE_FLOAT: {
      TYPE_DIFF_IMPL(pCtx, pData, float);
      break;
    };
    case TSDB_DATA_TYPE_SMALLINT: {
      TYPE_DIFF_IMPL(pCtx, pData, int16_t);
      break;
    };
    case TSDB_DATA_TYPE_TINYINT: {
      TYPE_DIFF_IMPL(pCtx, pData, int8_t);
      break;
    };
    default:
      pError("error input type");
  }

  if (pCtx->numOfOutputElems > 0) {
    pCtx->aOutputBuf = pCtx->aOutputBuf + pCtx->outputBytes * step;
    pCtx->ptsOutputBuf = (char *)pCtx->ptsOutputBuf + TSDB_KEYSIZE * step;
  }
  return true;
}

char *arithmetic_callback_function(void *param, char *name, int32_t colId) {
  SArithmeticSupport *pSupport = (SArithmeticSupport *)param;

  SSqlFunctionExpr *pExpr = pSupport->pExpr;
  int32_t           colIndexInBuf = -1;

  for (int32_t i = 0; i < pExpr->pBinExprInfo.numOfCols; ++i) {
    if (colId == pExpr->pBinExprInfo.pReqColumns[i].colId) {
      colIndexInBuf = pExpr->pBinExprInfo.pReqColumns[i].colIdxInBuf;
      break;
    }
  }

  assert(colIndexInBuf >= 0 && colId >= 0);
  return pSupport->data[colIndexInBuf] + pSupport->offset * pSupport->elemSize[colIndexInBuf];
}

bool arithmetic_function(SQLFunctionCtx *pCtx) {
  pCtx->numOfOutputElems += pCtx->size;
  SArithmeticSupport *sas = (SArithmeticSupport *)pCtx->param[0].pz;

  tSQLBinaryExprCalcTraverse(sas->pExpr->pBinExprInfo.pBinExpr, pCtx->size, pCtx->aOutputBuf, sas, pCtx->order,
                             arithmetic_callback_function);

  pCtx->aOutputBuf = pCtx->aOutputBuf + pCtx->outputBytes * pCtx->size * GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  return true;
}

bool arithmetic_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  INC_INIT_VAL(pCtx, 1, 1);
  SArithmeticSupport *sas = (SArithmeticSupport *)pCtx->param[0].pz;

  sas->offset = index;
  tSQLBinaryExprCalcTraverse(sas->pExpr->pBinExprInfo.pBinExpr, 1, pCtx->aOutputBuf, sas, pCtx->order,
                             arithmetic_callback_function);

  pCtx->aOutputBuf = pCtx->aOutputBuf + pCtx->outputBytes * GET_FORWARD_DIRECTION_FACTOR(pCtx->order);
  return true;
}

#define LIST_MINMAX_N(ctx, minOutput, maxOutput, elemCnt, data, type, tsdbType, numOfNotNullElem) \
  {                                                                                               \
    type *inputData = (type *)data;                                                               \
    for (int32_t i = 0; i < elemCnt; ++i) {                                                       \
      if (isNull((char *)&inputData[i], tsdbType)) {                                              \
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

#define LIST_MINMAX(ctx, minOutput, maxOutput, elemCnt, data, type, tsdbType) \
  {                                                                           \
    type *inputData = (type *)data;                                           \
    for (int32_t i = 0; i < elemCnt; ++i) {                                   \
      if (inputData[i] < minOutput) {                                         \
        minOutput = inputData[i];                                             \
      }                                                                       \
      if (inputData[i] > maxOutput) {                                         \
        maxOutput = inputData[i];                                             \
      }                                                                       \
    }                                                                         \
  }

void spread_function_setup(SQLFunctionCtx *pCtx) {
  if ((pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_DOUBLE) ||
      (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) || pCtx->inputType == TSDB_DATA_TYPE_BINARY) {
    pCtx->intermediateBuf[0].dKey = DBL_MAX;
    pCtx->intermediateBuf[3].dKey = -DBL_MAX;
  } else {
    pError("illegal data type:%d in spread function query", pCtx->inputType);
  }

  memset(pCtx->aOutputBuf, 0, pCtx->outputBytes);
  INIT_VAL(pCtx);
}

bool spread_function(SQLFunctionCtx *pCtx) {
  int32_t numOfElems = pCtx->size;

  /* column missing cause the hasNullValue to be true */
  if (!IS_DATA_BLOCK_LOADED(pCtx->blockStatus)) {  // Pre-aggregation
    if (pCtx->preAggVals.isSet) {
      numOfElems = pCtx->size - pCtx->preAggVals.numOfNullPoints;
      if (numOfElems == 0) {
        /* all data are null in current data block, ignore current data block */
        goto _spread_over;
      }

      if ((pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) ||
          (pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP)) {
        if (pCtx->intermediateBuf[0].dKey > pCtx->preAggVals.min) {
          pCtx->intermediateBuf[0].dKey = pCtx->preAggVals.min;
        }

        if (pCtx->intermediateBuf[3].dKey < pCtx->preAggVals.max) {
          pCtx->intermediateBuf[3].dKey = pCtx->preAggVals.max;
        }
      } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE || pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
        if (pCtx->intermediateBuf[0].dKey > *(double *)&(pCtx->preAggVals.min)) {
          pCtx->intermediateBuf[0].dKey = *(double *)&(pCtx->preAggVals.min);
        }

        if (pCtx->intermediateBuf[3].dKey < *(double *)&(pCtx->preAggVals.max)) {
          pCtx->intermediateBuf[3].dKey = *(double *)&(pCtx->preAggVals.max);
        }
      }
    } else {
      // do nothing
    }

    goto _spread_over;
  }

  void *pData = GET_INPUT_CHAR(pCtx);

  if (pCtx->hasNullValue) {
    numOfElems = 0;

    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int8_t,
                    pCtx->inputType, numOfElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int16_t,
                    pCtx->inputType, numOfElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int32_t,
                    pCtx->inputType, numOfElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT || pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int64_t,
                    pCtx->inputType, numOfElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, double,
                    pCtx->inputType, numOfElems);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_MINMAX_N(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, float,
                    pCtx->inputType, numOfElems);
    }
  } else {
    if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int8_t,
                  pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int16_t,
                  pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int32_t,
                  pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT || pCtx->inputType == TSDB_DATA_TYPE_TIMESTAMP) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, int64_t,
                  pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, double,
                  pCtx->inputType);
    } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
      LIST_MINMAX(pCtx, pCtx->intermediateBuf[0].dKey, pCtx->intermediateBuf[3].dKey, pCtx->size, pData, float,
                  pCtx->inputType);
    }
  }

_spread_over:

  SET_VAL(pCtx, numOfElems, 1);
  return true;
}

bool spread_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  void *pData = GET_INPUT_CHAR_INDEX(pCtx, index);
  if (pCtx->hasNullValue && isNull(pData, pCtx->inputType)) {
    return true;
  }

  SET_VAL(pCtx, 1, 1);

  double val = 0.0;
  if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    val = *(int8_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    val = *(int16_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    val = *(int32_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    val = *(int64_t *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    val = *(double *)pData;
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    val = *(float *)pData;
  }

  if (pCtx->intermediateBuf[0].dKey > val) {
    pCtx->intermediateBuf[0].dKey = val;
  }

  if (pCtx->intermediateBuf[3].dKey < val) {
    pCtx->intermediateBuf[3].dKey = val;
  }

  return true;
}

void spread_function_finalize(SQLFunctionCtx *pCtx) {
  /*
   * here we do not check the input data types, because in case of metric query,
   * the type of intermediate data is binary
   */
  if (pCtx->numOfIteratedElems <= 0) {
    setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
    return;
  }

  *(double *)pCtx->aOutputBuf = pCtx->intermediateBuf[3].dKey - pCtx->intermediateBuf[0].dKey;
  SET_VAL(pCtx, pCtx->numOfIteratedElems, 1);
}

void spread_dist_function_setup(SQLFunctionCtx *pCtx) {
  spread_function_setup(pCtx);

  /*
   * this is the server-side setup function in client-side, the secondary merge do not need this procedure
   */
  if (pCtx->outputType == TSDB_DATA_TYPE_BINARY) {
    double *pResData = pCtx->aOutputBuf;
    pResData[0] = DBL_MAX;   // init min value
    pResData[1] = -DBL_MAX;  // max value
  }
}

bool spread_dist_intern_function(SQLFunctionCtx *pCtx) {
  // restore value for calculation, since the intermediate result kept in output buffer
  SSpreadRuntime *pOutput = (SSpreadRuntime *)pCtx->aOutputBuf;
  pCtx->intermediateBuf[0].dKey = pOutput->start;
  pCtx->intermediateBuf[3].dKey = pOutput->end;

  spread_function(pCtx);

  if (pCtx->numOfIteratedElems) {
    pOutput->start = pCtx->intermediateBuf[0].dKey;
    pOutput->end = pCtx->intermediateBuf[3].dKey;
    pOutput->valid = DATA_SET_FLAG;
  }

  return true;
}

bool spread_dist_intern_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  // restore value for calculation, since the intermediate result kept
  // in output buffer
  SSpreadRuntime *pOutput = (SSpreadRuntime *)pCtx->aOutputBuf;
  pCtx->intermediateBuf[0].dKey = pOutput->start;
  pCtx->intermediateBuf[3].dKey = pOutput->end;

  spread_function_f(pCtx, index);

  /* keep the result data in output buffer, not in the intermediate buffer */
  if (pCtx->numOfIteratedElems) {
    pOutput->start = pCtx->intermediateBuf[0].dKey;
    pOutput->end = pCtx->intermediateBuf[3].dKey;
    pOutput->valid = DATA_SET_FLAG;
  }
  return true;
}

void spread_dist_merge(SQLFunctionCtx *pCtx) {
  /*
   * min,max in double format
   * pResData[0] = min
   * pResData[1] = max
   */
  SSpreadRuntime *pResData = (SSpreadRuntime *)pCtx->aOutputBuf;

  int32_t notNullElems = 0;
  for (int32_t i = 0; i < pCtx->size; ++i) {
    SSpreadRuntime *input = (SSpreadRuntime *)GET_INPUT_CHAR_INDEX(pCtx, i);

    /* no assign tag, the value is null */
    if (input->valid != DATA_SET_FLAG) {
      continue;
    }

    if (pResData->start > input->start) {
      pResData->start = input->start;
    }

    if (pResData->end < input->end) {
      pResData->end = input->end;
    }

    pResData->valid = DATA_SET_FLAG;
    notNullElems++;
  }

  pCtx->numOfIteratedElems += notNullElems;
}

/*
 * here we set the result value back to the intermediate buffer, to apply the finalize the function
 * the final result is generated in spread_function_finalize
 */
void spread_dist_second_merge(SQLFunctionCtx *pCtx) {
  SSpreadRuntime *pData = (SSpreadRuntime *)GET_INPUT_CHAR(pCtx);

  if (pData->valid != DATA_SET_FLAG) {
    return;
  }

  if (pCtx->intermediateBuf[0].dKey > pData->start) {
    pCtx->intermediateBuf[0].dKey = pData->start;
  }

  if (pCtx->intermediateBuf[3].dKey < pData->end) {
    pCtx->intermediateBuf[3].dKey = pData->end;
  }

  pCtx->numOfIteratedElems += 1;
}

/*
 * Compare two strings
 *    TSDB_MATCH:            Match
 *    TSDB_NOMATCH:          No match
 *    TSDB_NOWILDCARDMATCH:  No match in spite of having * or % wildcards.
 * Like matching rules:
 *      '%': Matches zero or more characters
 *      '_': Matches one character
 *
 */
int patternMatch(const char *patterStr, const char *str, size_t size, const SPatternCompareInfo *pInfo) {
  char c, c1;

  int32_t i = 0;
  int32_t j = 0;

  while ((c = patterStr[i++]) != 0) {
    if (c == pInfo->matchAll) { /* Match "*" */

      while ((c = patterStr[i++]) == pInfo->matchAll || c == pInfo->matchOne) {
        if (c == pInfo->matchOne && (j > size || str[j++] == 0)) {
          // empty string, return not match
          return TSDB_PATTERN_NOWILDCARDMATCH;
        }
      }

      if (c == 0) {
        return TSDB_PATTERN_MATCH; /* "*" at the end of the pattern matches */
      }

      char next[3] = {toupper(c), tolower(c), 0};
      while (1) {
        size_t n = strcspn(str, next);
        str += n;

        if (str[0] == 0 || (n >= size - 1)) {
          break;
        }

        int32_t ret = patternMatch(&patterStr[i], ++str, size - n - 1, pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }
      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    c1 = str[j++];

    if (j <= size) {
      if (c == c1) {
        continue;
      }

      if (tolower(c) == tolower(c1)) {
        continue;
      }

      if (c == pInfo->matchOne && c1 != 0) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return (str[j] == 0 || j >= size) ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

int WCSPatternMatch(const wchar_t *patterStr, const wchar_t *str, size_t size, const SPatternCompareInfo *pInfo) {
  wchar_t c, c1;
  wchar_t matchOne = L'_';  // "_"
  wchar_t matchAll = L'%';  // "%"

  int32_t i = 0;
  int32_t j = 0;

  while ((c = patterStr[i++]) != 0) {
    if (c == matchAll) { /* Match "%" */

      while ((c = patterStr[i++]) == matchAll || c == matchOne) {
        if (c == matchOne && (j > size || str[j++] == 0)) {
          return TSDB_PATTERN_NOWILDCARDMATCH;
        }
      }
      if (c == 0) {
        return TSDB_PATTERN_MATCH;
      }

      wchar_t accept[3] = {towupper(c), towlower(c), 0};
      while (1) {
        size_t n = wcsspn(str, accept);

        str += n;
        if (str[0] == 0 || (n >= size - 1)) {
          break;
        }

        str++;

        int32_t ret = WCSPatternMatch(&patterStr[i], str, wcslen(str), pInfo);
        if (ret != TSDB_PATTERN_NOMATCH) {
          return ret;
        }
      }

      return TSDB_PATTERN_NOWILDCARDMATCH;
    }

    c1 = str[j++];

    if (j <= size) {
      if (c == c1) {
        continue;
      }

      if (towlower(c) == towlower(c1)) {
        continue;
      }
      if (c == matchOne && c1 != 0) {
        continue;
      }
    }

    return TSDB_PATTERN_NOMATCH;
  }

  return str[j] == 0 ? TSDB_PATTERN_MATCH : TSDB_PATTERN_NOMATCH;
}

static void getStatics_i8(int64_t *primaryKey, int32_t type, int8_t *data, int32_t numOfRow, int64_t *min, int64_t *max,
                          int64_t *sum, int64_t *wsum, int32_t *numOfNull) {
  *min = INT64_MAX;
  *max = INT64_MIN;
  *wsum = 0;

  int64_t lastKey = 0;
  int8_t  lastVal = TSDB_DATA_TINYINT_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull((char *)&data[i], type)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (type != TSDB_DATA_TYPE_BOOL) {
      // ignore the bool data type pre-calculation
      if (isNull((char *)&lastVal, type)) {
        lastKey = primaryKey[i];
        lastVal = data[i];
      } else {
        *wsum = lastVal * (primaryKey[i] - lastKey);
        lastKey = primaryKey[i];
        lastVal = data[i];
      }
    }
  }
}

static void getStatics_i16(int64_t *primaryKey, int16_t *data, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int64_t *wsum, int32_t *numOfNull) {
  *min = INT64_MAX;
  *max = INT64_MIN;
  *wsum = 0;

  int64_t lastKey = 0;
  int16_t lastVal = TSDB_DATA_SMALLINT_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(&data[i], TSDB_DATA_TYPE_SMALLINT)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (isNull(&lastVal, TSDB_DATA_TYPE_SMALLINT)) {
      lastKey = primaryKey[i];
      lastVal = data[i];
    } else {
      *wsum = lastVal * (primaryKey[i] - lastKey);
      lastKey = primaryKey[i];
      lastVal = data[i];
    }
  }
}

static void getStatics_i32(int64_t *primaryKey, int32_t *data, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int64_t *wsum, int32_t *numOfNull) {
  *min = INT64_MAX;
  *max = INT64_MIN;
  *wsum = 0;

  int64_t lastKey = 0;
  int32_t lastVal = TSDB_DATA_INT_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(&data[i], TSDB_DATA_TYPE_INT)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (isNull(&lastVal, TSDB_DATA_TYPE_INT)) {
      lastKey = primaryKey[i];
      lastVal = data[i];
    } else {
      *wsum = lastVal * (primaryKey[i] - lastKey);
      lastKey = primaryKey[i];
      lastVal = data[i];
    }
  }
}

static void getStatics_i64(int64_t *primaryKey, int64_t *data, int32_t numOfRow, int64_t *min, int64_t *max,
                           int64_t *sum, int64_t *wsum, int32_t *numOfNull) {
  *min = INT64_MAX;
  *max = INT64_MIN;
  *wsum = 0;

  int64_t lastKey = 0;
  int64_t lastVal = TSDB_DATA_BIGINT_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(&data[i], TSDB_DATA_TYPE_BIGINT)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (isNull(&lastVal, TSDB_DATA_TYPE_BIGINT)) {
      lastKey = primaryKey[i];
      lastVal = data[i];
    } else {
      *wsum = lastVal * (primaryKey[i] - lastKey);
      lastKey = primaryKey[i];
      lastVal = data[i];
    }
  }
}

static void getStatics_f(int64_t *primaryKey, float *data, int32_t numOfRow, double *min, double *max, double *sum,
                         double *wsum, int32_t *numOfNull) {
  *min = DBL_MAX;
  *max = -DBL_MAX;
  *wsum = 0;

  int64_t lastKey = 0;
  float   lastVal = TSDB_DATA_FLOAT_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(&data[i], TSDB_DATA_TYPE_FLOAT)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (isNull(&lastVal, TSDB_DATA_TYPE_FLOAT)) {
      lastKey = primaryKey[i];
      lastVal = data[i];
    } else {
      *wsum = lastVal * (primaryKey[i] - lastKey);
      lastKey = primaryKey[i];
      lastVal = data[i];
    }
  }
}

static void getStatics_d(int64_t *primaryKey, double *data, int32_t numOfRow, double *min, double *max, double *sum,
                         double *wsum, int32_t *numOfNull) {
  *min = DBL_MAX;
  *max = -DBL_MAX;
  *wsum = 0;

  int64_t lastKey = 0;
  double  lastVal = TSDB_DATA_DOUBLE_NULL;

  for (int32_t i = 0; i < numOfRow; ++i) {
    if (isNull(&data[i], TSDB_DATA_TYPE_DOUBLE)) {
      (*numOfNull) += 1;
      continue;
    }

    *sum += data[i];
    if (*min > data[i]) {
      *min = data[i];
    }

    if (*max < data[i]) {
      *max = data[i];
    }

    if (isNull(&lastVal, TSDB_DATA_TYPE_DOUBLE)) {
      lastKey = primaryKey[i];
      lastVal = data[i];
    } else {
      *wsum = lastVal * (primaryKey[i] - lastKey);
      lastKey = primaryKey[i];
      lastVal = data[i];
    }
  }
}

void getStatistics(char *priData, char *data, int32_t size, int32_t numOfRow, int32_t type, int64_t *min, int64_t *max,
                   int64_t *sum, int64_t *wsum, int32_t *numOfNull) {
  int64_t *primaryKey = (int64_t *)priData;
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    for (int32_t i = 0; i < numOfRow; ++i) {
      if (isNull(data + i * size, type)) {
        (*numOfNull) += 1;
        continue;
      }
    }
  } else {
    if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
      getStatics_i8(primaryKey, type, (int8_t *)data, numOfRow, min, max, sum, wsum, numOfNull);
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      getStatics_i16(primaryKey, (int16_t *)data, numOfRow, min, max, sum, wsum, numOfNull);
    } else if (type == TSDB_DATA_TYPE_INT) {
      getStatics_i32(primaryKey, (int32_t *)data, numOfRow, min, max, sum, wsum, numOfNull);
    } else if (type == TSDB_DATA_TYPE_BIGINT || type == TSDB_DATA_TYPE_TIMESTAMP) {
      getStatics_i64(primaryKey, (int64_t *)data, numOfRow, min, max, sum, wsum, numOfNull);
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      getStatics_d(primaryKey, (double *)data, numOfRow, min, max, sum, wsum, numOfNull);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      getStatics_f(primaryKey, (float *)data, numOfRow, min, max, sum, wsum, numOfNull);
    }
  }
}

void wavg_function_setup(SQLFunctionCtx *pCtx) {
  memset(pCtx->aOutputBuf, 0, pCtx->outputBytes);

  pCtx->intermediateBuf[1].nType = TSDB_DATA_TYPE_TIMESTAMP;
  pCtx->intermediateBuf[2].nType = -1;

  INIT_VAL(pCtx);
}

bool wavg_function(SQLFunctionCtx *pCtx) {
  void *   pData = GET_INPUT_CHAR(pCtx);
  int64_t *primaryKey = pCtx->ptsList;
  assert(IS_DATA_BLOCK_LOADED(pCtx->blockStatus));
  /* assert(IS_INTER_BLOCK(pCtx->blockStatus)); */

  int32_t notNullElems = 0;

  if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    int64_t *retVal = pCtx->aOutputBuf;
    int32_t *pDb = (int32_t *)pData;
    int32_t  i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull(&pDb[i], TSDB_DATA_TYPE_INT)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].i64Key = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].i64Key = pDb[i];
      }

      break;
    }

    /* if (IS_INTER_BLOCK(pCtx->blockStatus)) { */
    /*     *retVal += pCtx->preAggVals.wsum; */
    /* } */

    for (++i; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_INT)) continue;

      notNullElems++;
      /* if (!IS_INTER_BLOCK(pCtx->blockStatus)) { */
      *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      /* } */
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].i64Key = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    int64_t *retVal = pCtx->aOutputBuf;
    int64_t *pDb = (int64_t *)pData;
    int32_t  i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull(&pDb[i], TSDB_DATA_TYPE_BIGINT)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].i64Key = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].i64Key = pDb[i];
      }

      break;
    }

    /* if (IS_INTER_BLOCK(pCtx->blockStatus)) { */
    /*     *retVal += pCtx->preAggVals.wsum; */
    /* } */

    for (++i; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_BIGINT)) continue;

      notNullElems++;
      /* if (!IS_INTER_BLOCK(pCtx->blockStatus)) { */
      *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      /* } */
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].i64Key = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *retVal = pCtx->aOutputBuf;
    double *pDb = (double *)pData;
    int32_t i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull(&pDb[i], TSDB_DATA_TYPE_DOUBLE)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].dKey = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].dKey = pDb[i];
      }

      break;
    }

    /* if (IS_INTER_BLOCK(pCtx->blockStatus)) { */
    /*     *retVal += *(double *)(&(pCtx->preAggVals.wsum)); */
    /* } */

    for (++i; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_DOUBLE)) continue;

      notNullElems++;
      *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].dKey = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    double *retVal = pCtx->aOutputBuf;
    float * pDb = (float *)pData;
    int32_t i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull(&pDb[i], TSDB_DATA_TYPE_FLOAT)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].dKey = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].dKey = pDb[i];
      }

      break;
    }

    /* if (IS_INTER_BLOCK(pCtx->blockStatus)) { */
    /*     *retVal += *(double *)(&(pCtx->preAggVals.wsum)); */
    /* } */

    for (++i; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_FLOAT)) continue;

      notNullElems++;
      *retVal += pCtx->intermediateBuf[2].dKey * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].dKey = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    int64_t *retVal = pCtx->aOutputBuf;
    int16_t *pDb = (int16_t *)pData;
    int32_t  i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull(&pDb[i], TSDB_DATA_TYPE_SMALLINT)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].i64Key = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].i64Key = pDb[i];
      }

      break;
    }

    for (++i; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_SMALLINT)) continue;

      notNullElems++;
      *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].i64Key = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    int64_t *retVal = pCtx->aOutputBuf;
    int8_t * pDb = (int8_t *)pData;
    int32_t  i = 0;

    // Start diff in the block
    for (; i < pCtx->size; ++i) {
      assert(primaryKey[i] >= pCtx->intermediateBuf[1].i64Key);

      if (isNull((char *)&pDb[i], TSDB_DATA_TYPE_TINYINT)) continue;

      if (pCtx->intermediateBuf[2].nType == -1) {
        pCtx->intermediateBuf[2].i64Key = pDb[i];
        pCtx->intermediateBuf[2].nType = pCtx->inputType;
        pCtx->intermediateBuf[1].i64Key = pCtx->nStartQueryTimestamp;
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      } else {
        *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
        pCtx->intermediateBuf[1].i64Key = primaryKey[i];
        pCtx->intermediateBuf[2].i64Key = pDb[i];
      }

      break;
    }

    for (++i; i < pCtx->size; i++) {
      if (isNull((char *)&pDb[i], TSDB_DATA_TYPE_TINYINT)) continue;

      notNullElems++;
      *retVal += pCtx->intermediateBuf[2].i64Key * (primaryKey[i] - pCtx->intermediateBuf[1].i64Key);
      pCtx->intermediateBuf[1].i64Key = primaryKey[i];
      pCtx->intermediateBuf[2].i64Key = pDb[i];
    }
  }

  pCtx->numOfIteratedElems += notNullElems;

  return true;
}

bool wavg_function_f(SQLFunctionCtx *pCtx, int32_t index) {
  // TODO :
  return false;
}
void wavg_function_finalize(SQLFunctionCtx *pCtx) {
  if (pCtx->intermediateBuf[2].nType == -1) {
    *((double *)(pCtx->aOutputBuf)) = TSDB_DATA_DOUBLE_NULL;
    SET_VAL(pCtx, 0, 0);
    return;
  }

  assert(pCtx->intermediateBuf[3].i64Key >= pCtx->intermediateBuf[1].i64Key);
  if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
    int64_t *retVal = pCtx->aOutputBuf;
    *retVal += pCtx->intermediateBuf[2].i64Key * (pCtx->intermediateBuf[3].i64Key - pCtx->intermediateBuf[1].i64Key);
    *(double *)pCtx->aOutputBuf = (*retVal) / (double)(pCtx->intermediateBuf[3].i64Key - pCtx->nStartQueryTimestamp);
  } else {
    double *retVal = pCtx->aOutputBuf;
    *retVal += pCtx->intermediateBuf[2].dKey * (pCtx->intermediateBuf[3].i64Key - pCtx->intermediateBuf[1].i64Key);
    *retVal = *retVal / (pCtx->intermediateBuf[3].i64Key - pCtx->nStartQueryTimestamp);
  }
  SET_VAL(pCtx, 1, 1);
}

static bool wavg_dist_function(SQLFunctionCtx *pCtx) {
  void *   pData = GET_INPUT_CHAR(pCtx);
  int64_t *primaryKey = pCtx->ptsList;
  assert(IS_DATA_BLOCK_LOADED(pCtx->blockStatus));

  SWavgRuntime *output = pCtx->aOutputBuf;

  output->type = pCtx->inputType;
  int32_t notNullElems = 0;

  if (pCtx->inputType == TSDB_DATA_TYPE_INT) {
    int32_t *pDb = (int32_t *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_INT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->iOutput += output->iLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->iLastValue = pDb[i];
    }

  } else if (pCtx->inputType == TSDB_DATA_TYPE_BIGINT) {
    int64_t *pDb = (int64_t *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_BIGINT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->iOutput += output->iLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->iLastValue = pDb[i];
    }

  } else if (pCtx->inputType == TSDB_DATA_TYPE_DOUBLE) {
    double *pDb = (double *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_BIGINT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->dOutput += output->dLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->dLastValue = pDb[i];
    }

  } else if (pCtx->inputType == TSDB_DATA_TYPE_FLOAT) {
    float *pDb = (float *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_FLOAT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->dOutput += output->dLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->dLastValue = pDb[i];
    }

  } else if (pCtx->inputType == TSDB_DATA_TYPE_SMALLINT) {
    int16_t *pDb = (int16_t *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull(&pDb[i], TSDB_DATA_TYPE_SMALLINT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->iOutput += output->iLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->iLastValue = pDb[i];
    }
  } else if (pCtx->inputType == TSDB_DATA_TYPE_TINYINT) {
    int8_t *pDb = (int8_t *)pData;

    for (int32_t i = 0; i < pCtx->size; i++) {
      if (isNull((char *)&pDb[i], TSDB_DATA_TYPE_TINYINT)) continue;

      notNullElems++;
      SET_HAS_DATA_FLAG(output->valFlag);

      output->iOutput += output->iLastValue * (primaryKey[i] - output->lastKey);
      output->lastKey = primaryKey[i];
      output->iLastValue = pDb[i];
    }
  }

  SET_VAL(pCtx, notNullElems, 1);
  return true;
}

static bool wavg_dist_function_f(SQLFunctionCtx *pCtx, int32_t index) { return false; }

static void wavg_dist_merge(SQLFunctionCtx *pCtx) {
  SWavgRuntime *pBuf = (SWavgRuntime *)pCtx->aOutputBuf;
  char *        indicator = pCtx->aInputElemBuf;

  int32_t numOfNotNull = 0;
  for (int32_t i = 0; i < pCtx->size; ++i) {
    SWavgRuntime *pInput = indicator;

    if (!HAS_DATA_FLAG(pInput->valFlag)) {
      indicator += sizeof(SWavgRuntime);
      continue;
    }

    numOfNotNull++;
    if (pCtx->inputType >= TSDB_DATA_TYPE_TINYINT && pCtx->inputType <= TSDB_DATA_TYPE_BIGINT) {
      pBuf->iOutput += pInput->iOutput;
    } else {
      pBuf->dOutput += pInput->dOutput;
    }

    pBuf->sKey = pInput->sKey;
    pBuf->eKey = pInput->eKey;
    pBuf->lastKey = pInput->lastKey;
    pBuf->iLastValue = pInput->iLastValue;
  }

  SET_VAL(pCtx, numOfNotNull, 1);
}

static void wavg_dist_second_merge(SQLFunctionCtx *pCtx) {
  SWavgRuntime *pWavg = (SWavgRuntime *)pCtx->aInputElemBuf;

  if (!HAS_DATA_FLAG(pWavg->valFlag)) {
    *((int64_t *)(pCtx->aOutputBuf)) = TSDB_DATA_DOUBLE_NULL;
    SET_VAL(pCtx, 0, 0);
    return;
  }

  if (pWavg->type >= TSDB_DATA_TYPE_TINYINT && pWavg->type <= TSDB_DATA_TYPE_BIGINT) {
    *(double *)pCtx->aOutputBuf =
        (pWavg->iOutput + pWavg->iLastValue * (pWavg->eKey - pWavg->lastKey)) / (double)(pWavg->eKey - pWavg->sKey);
  } else {
    *(double *)pCtx->aOutputBuf =
        (pWavg->dOutput + pWavg->dLastValue * (pWavg->eKey - pWavg->lastKey)) / (pWavg->eKey - pWavg->sKey);
  }
  SET_VAL(pCtx, 1, 1);
}

/**
 * param[1]: default value/previous value of specified timestamp
 * param[2]: next value of specified timestamp
 * param[3]: denotes if the result is a precious result or interpolation results
 *
 * intermediate[0]: interpolation type
 * intermediate[1]: precious specified timestamp, the pCtx->startTimetamp is changed during query to satisfy the query procedure
 * intermediate[2]: flag that denotes if it is a primary timestamp column or not
 * intermediate[3]: tags. reserved for tags, the function is available for stable query, so the intermediate[3] must be reserved.
 *
 * @param pCtx
 */
static bool interp_function(SQLFunctionCtx *pCtx) {
  /* at this point, the value is existed, return directly */
  if (pCtx->param[3].i64Key == 1) {
    char *pData = GET_INPUT_CHAR(pCtx);
    assignVal(pCtx->aOutputBuf, pData, pCtx->inputBytes, pCtx->inputType);
  } else {
    /*
     * use interpolation to generate the result.
     * Note: the result of primary timestamp column uses the timestamp specified by user in the query sql
     */
    assert(pCtx->param[3].i64Key == 2);
    int32_t interpoType = pCtx->intermediateBuf[0].i64Key;

    if (interpoType == TSDB_INTERPO_NONE) {
      /* set no output result */
      pCtx->param[3].i64Key = 0;
    } else if (pCtx->intermediateBuf[2].i64Key == 1) {
      *(TSKEY *)pCtx->aOutputBuf = pCtx->intermediateBuf[1].i64Key;
    } else {
      if (interpoType == TSDB_INTERPO_NULL) {
        setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
      } else if (interpoType == TSDB_INTERPO_SET_VALUE) {
        tVariantDump(&pCtx->param[1], pCtx->aOutputBuf, pCtx->inputType);
      } else if (interpoType == TSDB_INTERPO_PREV) {
        if (strcmp(pCtx->param[1].pz, TSDB_DATA_NULL_STR_L) == 0) {
          setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
          goto _end;
        }

        char *data = pCtx->param[1].pz;
        char *pVal = NULL;
        if (pCtx->param[1].nType == TSDB_DATA_TYPE_BINARY) {
          pVal = strsep(&data, ",");
          pVal = strsep(&data, ",");
        } else {
          wchar_t *token = NULL;
          pVal = wcstok(data, L",", &token);
          pVal = wcstok(NULL, L",", &token);
        }

        if ((pCtx->outputType >= TSDB_DATA_TYPE_BOOL && pCtx->outputType <= TSDB_DATA_TYPE_BIGINT) ||
            pCtx->outputType == TSDB_DATA_TYPE_TIMESTAMP) {
          int64_t v = strtoll(pVal, NULL, 10);
          assignVal(pCtx->aOutputBuf, &v, pCtx->outputBytes, pCtx->outputType);
        } else if (pCtx->outputType == TSDB_DATA_TYPE_FLOAT) {
          float v = (float)strtod(pVal, NULL);
          if (isNull(&v, pCtx->outputType)) {
            setNull(pCtx->aOutputBuf, pCtx->inputType, pCtx->inputBytes);
          } else {
            assignVal(pCtx->aOutputBuf, &v, pCtx->outputBytes, pCtx->outputType);
          }
        } else if (pCtx->outputType == TSDB_DATA_TYPE_DOUBLE) {
          double v = strtod(pVal, NULL);
          if (isNull(&v, pCtx->outputType)) {
            setNull(pCtx->aOutputBuf, pCtx->inputType, pCtx->inputBytes);
          } else {
            assignVal(pCtx->aOutputBuf, &v, pCtx->outputBytes, pCtx->outputType);
          }
        } else if (pCtx->outputType == TSDB_DATA_TYPE_BINARY) {
          assignVal(pCtx->aOutputBuf, pVal, pCtx->outputBytes, pCtx->outputType);
        } else if (pCtx->outputType == TSDB_DATA_TYPE_NCHAR) {
          assignVal(pCtx->aOutputBuf, pVal, pCtx->outputBytes, pCtx->outputType);
        }

      } else if (interpoType == TSDB_INTERPO_LINEAR) {
        if (strcmp(pCtx->param[1].pz, TSDB_DATA_NULL_STR_L) == 0) {
          setNull(pCtx->aOutputBuf, pCtx->outputType, pCtx->outputBytes);
          goto _end;
        }

        char *data1 = pCtx->param[1].pz;
        char *data2 = pCtx->param[2].pz;

        char *pTimestamp1 = strsep(&data1, ",");
        char *pTimestamp2 = strsep(&data2, ",");

        char *pVal1 = strsep(&data1, ",");
        char *pVal2 = strsep(&data2, ",");

        SPoint point1 = {.key = strtol(pTimestamp1, NULL, 10), .val = &pCtx->param[1].i64Key};
        SPoint point2 = {.key = strtol(pTimestamp2, NULL, 10), .val = &pCtx->param[2].i64Key};

        SPoint point = {.key = pCtx->intermediateBuf[1].i64Key, .val = pCtx->aOutputBuf};

        int32_t srcType = pCtx->inputType;
        if ((srcType >= TSDB_DATA_TYPE_TINYINT && srcType <= TSDB_DATA_TYPE_BIGINT) ||
            srcType == TSDB_DATA_TYPE_TIMESTAMP) {
          int64_t v1 = strtol(pVal1, NULL, 10);
          point1.val = &v1;

          int64_t v2 = strtol(pVal2, NULL, 10);
          point2.val = &v2;

          if (isNull(&v1, srcType) || isNull(&v2, srcType)) {
            setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
          } else {
            taosDoLinearInterpolation(pCtx->outputType, &point1, &point2, &point);
          }
        } else if (srcType == TSDB_DATA_TYPE_FLOAT) {
          float v1 = strtod(pVal1, NULL);
          point1.val = &v1;

          float v2 = strtod(pVal2, NULL);
          point2.val = &v2;

          if (isNull(&v1, srcType) || isNull(&v2, srcType)) {
            setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
          } else {
            taosDoLinearInterpolation(pCtx->outputType, &point1, &point2, &point);
          }
        } else if (srcType == TSDB_DATA_TYPE_DOUBLE) {
          double v1 = strtod(pVal1, NULL);
          point1.val = &v1;

          double v2 = strtod(pVal2, NULL);
          point2.val = &v2;

          if (isNull(&v1, srcType) || isNull(&v2, srcType)) {
            setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
          } else {
            taosDoLinearInterpolation(pCtx->outputType, &point1, &point2, &point);
          }
        } else if (srcType == TSDB_DATA_TYPE_BOOL || srcType == TSDB_DATA_TYPE_BINARY ||
                   srcType == TSDB_DATA_TYPE_NCHAR) {
          setNull(pCtx->aOutputBuf, srcType, pCtx->inputBytes);
        }
      }
    }
  }

_end:
  pCtx->size = pCtx->param[3].i64Key;

  tVariantDestroy(&pCtx->param[1]);
  tVariantDestroy(&pCtx->param[2]);

  // data in the check operation are all null, not output
  SET_VAL(pCtx, pCtx->size, 1);
  return false;
}

/*
 * function with the same value is compatible in selection clause
 * Note: tag function, ts function is not need to check the compatible with other functions
 *
 * top/bottom is the last one
 */
int32_t funcCompatList[36] = {
    /* count, sum, avg, min, max, stddev, percentile, apercentile, first,
       last, last_row, leastsqr, */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 7, 1,

    /* top, bottom, spread, wavg, ts, ts_dummy, tag, colprj, tagprj,
       arithmetic, diff, */
    2, 5, 1, 1, 1, 1, 1, 3, 3, 3, 4,

    /*sum_d, avg_d, min_d, max_d, first_d, last_d, last_row_d, spread_dst,
       wavg_dst, top_dst, bottom_dst, */
    1, 1, 1, 1, 1, 1, 7, 1, 1, 2, 5,

    /*apercentile_dst, interp*/
    1, 6,
};

SQLAggFuncElem aAggs[36] = {
    {
        // 0
        "count", TSDB_FUNC_COUNT, TSDB_FUNC_COUNT, TSDB_BASE_FUNC_SO, function_setup, count_function, count_function_f,
        no_next_step, noop, count_dist_merge, count_dist_merge, count_load_data_info,
    },
    {
        // 1
        "sum", TSDB_FUNC_SUM, TSDB_FUNC_SUM_DST, TSDB_BASE_FUNC_SO, function_setup, sum_function, sum_function_f,
        no_next_step, function_finalize, noop, noop, precal_req_load_info,
    },
    {
        // 2
        "avg", TSDB_FUNC_AVG, TSDB_FUNC_AVG_DST, TSDB_BASE_FUNC_SO, function_setup, sum_function, sum_function_f,
        no_next_step, avg_finalizer, noop, noop, precal_req_load_info,
    },
    {
        // 3
        "min", TSDB_FUNC_MIN, TSDB_FUNC_MIN_DST, TSDB_BASE_FUNC_SO, min_function_setup, min_function, min_function_f,
        no_next_step, function_finalize, noop, noop, precal_req_load_info,
    },
    {
        // 4
        "max", TSDB_FUNC_MAX, TSDB_FUNC_MAX_DST, TSDB_BASE_FUNC_SO, max_function_setup, max_function, max_function_f,
        no_next_step, function_finalize, noop, noop, precal_req_load_info,
    },
    {
        // 5
        "stddev", TSDB_FUNC_STDDEV, TSDB_FUNC_INVALID_ID, TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF,
        function_setup, stddev_function, stddev_function_f, stddev_next_step, stddev_finalizer, noop, noop,
        data_req_load_info,
    },
    {
        // 6
        "percentile", TSDB_FUNC_PERCT, TSDB_FUNC_INVALID_ID,
        TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF, percentile_function_setup, percentile_function,
        percentile_function_f, no_next_step, percentile_finalizer, noop, noop, data_req_load_info,
    },
    {
        // 7
        "apercentile", TSDB_FUNC_APERCT, TSDB_FUNC_APERCT_DST,
        TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF, function_setup, apercentile_function,
        apercentile_function_f, no_next_step, apercentile_finalizer, noop, noop, data_req_load_info,
    },
    {
        // 8
        "first", TSDB_FUNC_FIRST, TSDB_FUNC_FIRST_DST, TSDB_BASE_FUNC_SO, function_setup, first_function,
        first_function_f, no_next_step, function_finalize, noop, noop, first_data_req_info,
    },
    {
        // 9
        "last", TSDB_FUNC_LAST, TSDB_FUNC_LAST_DST, TSDB_BASE_FUNC_SO, function_setup, last_function, last_function_f,
        no_next_step, function_finalize, noop, noop, last_data_req_info,
    },
    {
        // 10
        "last_row", TSDB_FUNC_LAST_ROW, TSDB_FUNC_LAST_ROW_DST,
        TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_METRIC, function_setup, interp_function, noop,
        no_next_step, noop, noop, copy_function, no_data_info,
    },
    {
        // 11
        "leastsquares", TSDB_FUNC_LEASTSQR, TSDB_FUNC_INVALID_ID,
        TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_STREAM | TSDB_FUNCSTATE_OF, leastsquares_function_setup,
        leastsquares_function, leastsquares_function_f, no_next_step, leastsquare_finalizer, noop, noop,
        data_req_load_info,
    },
    {
        // 12
        "top", TSDB_FUNC_TOP, TSDB_FUNC_TOP_DST, TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_NEED_TS,
        top_bottom_function_setup, top_function, top_function_f, no_next_step, top_bottom_function_finalizer, noop,
        noop, data_req_load_info,
    },
    {
        // 13
        "bottom", TSDB_FUNC_BOTTOM, TSDB_FUNC_BOTTOM_DST,
        TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_NEED_TS, top_bottom_function_setup, bottom_function,
        bottom_function_f, no_next_step, top_bottom_function_finalizer, noop, noop, data_req_load_info,
    },
    {
        // 14
        "spread", TSDB_FUNC_SPREAD, TSDB_FUNC_SPREAD_DST, TSDB_BASE_FUNC_SO, spread_function_setup, spread_function,
        spread_function_f, no_next_step, spread_function_finalize, noop, noop, count_load_data_info,
    },
    {
        // 15
        "wavg", TSDB_FUNC_WAVG, TSDB_FUNC_WAVG_DST, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS, wavg_function_setup,
        wavg_function, wavg_function_f, no_next_step, wavg_function_finalize, noop, noop, data_req_load_info,
    },
    {
        // 16
        "ts", TSDB_FUNC_TS, TSDB_FUNC_TS, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS, function_setup,
        date_col_output_function, date_col_output_function, no_next_step, noop, copy_function, copy_function,
        no_data_info,
    },
    {
        // 17
        "ts", TSDB_FUNC_TS_DUMMY, TSDB_FUNC_TS_DUMMY, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS, function_setup, noop,
        noop, no_next_step, noop, copy_function, copy_function, no_data_info,
    },
    {
        // 18
        "tag", TSDB_FUNC_TAG, TSDB_FUNC_TAG, TSDB_BASE_FUNC_SO, function_setup, tag_function, tag_function_f,
        no_next_step, noop, copy_function, copy_function, no_data_info,
    },
    // column project sql function
    {
        // 19
        "colprj", TSDB_FUNC_PRJ, TSDB_FUNC_PRJ, TSDB_BASE_FUNC_MO | TSDB_FUNCSTATE_NEED_TS, function_setup,
        col_project_function, col_project_function_f, no_next_step, noop, copy_function, copy_function,
        data_req_load_info,
    },
    {
        // 20
        "tagprj", TSDB_FUNC_TAGPRJ, TSDB_FUNC_TAGPRJ,
        TSDB_BASE_FUNC_MO,  // multi-output, tag function has only one result
        function_setup, tag_project_function, tag_project_function_f, no_next_step, noop, copy_function, copy_function,
        no_data_info,
    },
    {
        // 21
        "arithmetic", TSDB_FUNC_ARITHM, TSDB_FUNC_ARITHM,
        TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_NEED_TS, function_setup, arithmetic_function,
        arithmetic_function_f, no_next_step, noop, copy_function, copy_function, data_req_load_info,
    },
    {
        // 22
        "diff", TSDB_FUNC_DIFF, TSDB_FUNC_INVALID_ID, TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_NEED_TS, diff_function_setup,
        diff_function, diff_function_f, no_next_step, noop, noop, noop, data_req_load_info,
    },
    // distriubted version used in two-stage aggregation processes
    {
        // 23
        "sum_dst", TSDB_FUNC_SUM_DST, TSDB_FUNC_SUM_DST, TSDB_BASE_FUNC_SO, function_setup, sum_dist_intern_function,
        sum_dist_intern_function_f, no_next_step, function_finalize, sum_dist_merge, sum_dist_second_merge,
        precal_req_load_info,
    },
    {
        // 24
        "avg_dst", TSDB_FUNC_AVG_DST, TSDB_FUNC_AVG_DST, TSDB_BASE_FUNC_SO, avg_dist_function_setup,
        avg_dist_intern_function, avg_dist_intern_function_f, no_next_step, avg_finalizer, avg_dist_merge,
        avg_dist_second_merge, precal_req_load_info,
    },
    {
        // 25
        "min_dst", TSDB_FUNC_MIN_DST, TSDB_FUNC_MIN_DST, TSDB_BASE_FUNC_SO, min_function_setup,
        min_dist_intern_function, min_dist_intern_function_f, no_next_step, function_finalize, min_dist_merge,
        min_dist_second_merge, precal_req_load_info,
    },
    {
        // 26
        "max_dst", TSDB_FUNC_MAX_DST, TSDB_FUNC_MAX_DST, TSDB_BASE_FUNC_SO, max_function_setup,
        max_dist_intern_function, max_dist_intern_function_f, no_next_step, function_finalize, max_dist_merge,
        max_dist_second_merge, precal_req_load_info,
    },
    {
        // 27
        "first_dist", TSDB_FUNC_FIRST_DST, TSDB_FUNC_FIRST_DST, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
        function_setup, first_dist_function, first_dist_function_f, no_next_step, function_finalize, first_dist_merge,
        first_dist_second_merge, first_dist_data_req_info,
    },
    {
        // 28
        "last_dist", TSDB_FUNC_LAST_DST, TSDB_FUNC_LAST_DST, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS, function_setup,
        last_dist_function, last_dist_function_f, no_next_step, function_finalize, last_dist_merge,
        last_dist_second_merge, last_dist_data_req_info,
    },
    {
        // 29
        "last_row_dist", TSDB_FUNC_LAST_ROW_DST, TSDB_FUNC_LAST_ROW_DST, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS,
        function_setup, last_row_dist_function, noop, no_next_step, function_finalize, noop, last_dist_second_merge,
        data_req_load_info,  // this function is not necessary
    },
    {
        // 30
        "spread_dst", TSDB_FUNC_SPREAD_DST, TSDB_FUNC_SPREAD_DST, TSDB_BASE_FUNC_SO, spread_dist_function_setup,
        spread_dist_intern_function, spread_dist_intern_function_f, no_next_step,
        spread_function_finalize,  // no finalize
        spread_dist_merge, spread_dist_second_merge, count_load_data_info,
    },
    {
        // 31
        "wavg_dst", TSDB_FUNC_WAVG_DST, TSDB_FUNC_WAVG_DST, TSDB_BASE_FUNC_SO | TSDB_FUNCSTATE_NEED_TS, function_setup,
        wavg_dist_function, wavg_dist_function_f, no_next_step, noop, wavg_dist_merge, wavg_dist_second_merge,
        data_req_load_info,
    },
    {
        // 32
        "top_dst", TSDB_FUNC_TOP_DST, TSDB_FUNC_TOP_DST,
        TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_NEED_TS, top_bottom_dist_function_setup,
        top_dist_function, top_dist_function_f, no_next_step, top_bottom_function_finalizer, top_dist_merge,
        top_dist_second_merge, data_req_load_info,
    },
    {
        // 33
        "bottom_dst", TSDB_FUNC_BOTTOM_DST, TSDB_FUNC_BOTTOM_DST,
        TSDB_FUNCSTATE_MO | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_NEED_TS, top_bottom_dist_function_setup,
        bottom_dist_function, bottom_dist_function_f, no_next_step, top_bottom_function_finalizer, bottom_dist_merge,
        bottom_dist_second_merge, data_req_load_info,
    },
    {
        // 34
        "apercentile_dst", TSDB_FUNC_APERCT_DST, TSDB_FUNC_APERCT_DST, TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_METRIC,
        apercentile_dist_function_setup, apercentile_dist_intern_function, apercentile_dist_intern_function_f,
        no_next_step, apercentile_finalizer, apercentile_dist_merge, apercentile_dist_second_merge, data_req_load_info,
    },
    {
        // 35
        "interp", TSDB_FUNC_INTERP, TSDB_FUNC_INTERP,
        TSDB_FUNCSTATE_SO | TSDB_FUNCSTATE_OF | TSDB_FUNCSTATE_METRIC | TSDB_FUNCSTATE_NEED_TS, function_setup,
        interp_function,
        sum_function_f,  // todo filter handle
        no_next_step, noop, noop, copy_function, no_data_info,
    }};
