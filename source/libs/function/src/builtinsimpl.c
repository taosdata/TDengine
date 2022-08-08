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

#include "builtinsimpl.h"
#include "cJSON.h"
#include "function.h"
#include "query.h"
#include "querynodes.h"
#include "taggfunction.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tdigest.h"
#include "tglobal.h"
#include "thistogram.h"
#include "tpercentile.h"

#define HISTOGRAM_MAX_BINS_NUM 1000
#define MAVG_MAX_POINTS_NUM    1000
#define TAIL_MAX_POINTS_NUM    100
#define TAIL_MAX_OFFSET        100

#define UNIQUE_MAX_RESULT_SIZE (1024 * 1024 * 10)
#define MODE_MAX_RESULT_SIZE   UNIQUE_MAX_RESULT_SIZE

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
} SSumRes;

typedef struct SAvgRes {
  double  result;
  SSumRes sum;
  int64_t count;
  int16_t type;  // store the original input type, used in merge function
} SAvgRes;

typedef struct STuplePos {
  int32_t pageId;
  int32_t offset;
} STuplePos;

typedef struct SMinmaxResInfo {
  bool      assign;  // assign the first value or not
  int64_t   v;
  STuplePos tuplePos;

  STuplePos nullTuplePos;
  bool      nullTupleSaved;
} SMinmaxResInfo;

typedef struct STopBotResItem {
  SVariant  v;
  uint64_t  uid;  // it is a table uid, used to extract tag data during building of the final result for the tag data
  STuplePos tuplePos;  // tuple data of this chosen row
} STopBotResItem;

typedef struct STopBotRes {
  int32_t         maxSize;
  int16_t         type;

  STuplePos       nullTuplePos;
  bool            nullTupleSaved;

  STopBotResItem* pItems;
} STopBotRes;

typedef struct SFirstLastRes {
  bool hasResult;
  // used for last_row function only, isNullRes in SResultRowEntry can not be passed to downstream.So,
  // this attribute is required
  bool      isNull;
  int32_t   bytes;
  int64_t   ts;
  STuplePos pos;
  char      buf[];
} SFirstLastRes;

typedef struct SStddevRes {
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
} SStddevRes;

typedef struct SLeastSQRInfo {
  double  matrix[2][3];
  double  startVal;
  double  stepVal;
  int64_t num;
} SLeastSQRInfo;

typedef struct SPercentileInfo {
  double      result;
  tMemBucket* pMemBucket;
  int32_t     stage;
  double      minval;
  double      maxval;
  int64_t     numOfElems;
} SPercentileInfo;

typedef struct SAPercentileInfo {
  double          result;
  double          percent;
  int8_t          algo;
  SHistogramInfo* pHisto;
  TDigest*        pTDigest;
} SAPercentileInfo;

typedef enum {
  APERCT_ALGO_UNKNOWN = 0,
  APERCT_ALGO_DEFAULT,
  APERCT_ALGO_TDIGEST,
} EAPerctAlgoType;

typedef struct SDiffInfo {
  bool hasPrev;
  bool includeNull;
  bool ignoreNegative;  // replace the ignore with case when
  bool firstOutput;
  union {
    int64_t i64;
    double  d64;
  } prev;

  int64_t prevTs;
} SDiffInfo;

typedef struct SSpreadInfo {
  double result;
  bool   hasResult;
  double min;
  double max;
} SSpreadInfo;

typedef struct SElapsedInfo {
  double  result;
  TSKEY   min;
  TSKEY   max;
  int64_t timeUnit;
} SElapsedInfo;

typedef struct STwaInfo {
  double      dOutput;
  bool        isNull;
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

typedef enum { UNKNOWN_BIN = 0, USER_INPUT_BIN, LINEAR_BIN, LOG_BIN } EHistoBinType;

typedef struct SHLLFuncInfo {
  uint64_t result;
  uint8_t  buckets[HLL_BUCKETS];
} SHLLInfo;

typedef struct SStateInfo {
  union {
    int64_t count;
    int64_t durationStart;
  };
} SStateInfo;

typedef enum {
  STATE_OPER_INVALID = 0,
  STATE_OPER_LT,
  STATE_OPER_GT,
  STATE_OPER_LE,
  STATE_OPER_GE,
  STATE_OPER_NE,
  STATE_OPER_EQ,
} EStateOperType;

typedef struct SMavgInfo {
  int32_t pos;
  double  sum;
  int32_t numOfPoints;
  bool    pointsMeet;
  double  points[];
} SMavgInfo;

typedef struct SSampleInfo {
  int32_t    samples;
  int32_t    totalPoints;
  int32_t    numSampled;
  uint8_t    colType;
  int16_t    colBytes;

  STuplePos  nullTuplePos;
  bool       nullTupleSaved;

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
  int16_t     colBytes;
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
  int16_t   colBytes;
  bool      hasNull;  // null is not hashable, handle separately
  SHashObj* pHash;
  char      pItems[];
} SUniqueInfo;

typedef struct SModeItem {
  int64_t count;
  char    data[];
} SModeItem;

typedef struct SModeInfo {
  int32_t   numOfPoints;
  uint8_t   colType;
  int16_t   colBytes;
  SHashObj* pHash;
  char      pItems[];
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
} SRateInfo;

typedef struct SGroupKeyInfo {
  bool hasResult;
  bool isNull;
  char data[];
} SGroupKeyInfo;

#define SET_VAL(_info, numOfElem, res) \
  do {                                 \
    if ((numOfElem) <= 0) {            \
      break;                           \
    }                                  \
    (_info)->numOfRes = (res);         \
  } while (0)

#define GET_TS_LIST(x)    ((TSKEY*)((x)->ptsList))
#define GET_TS_DATA(x, y) (GET_TS_LIST(x)[(y)])

#define DO_UPDATE_TAG_COLUMNS_WITHOUT_TS(ctx)                      \
  do {                                                             \
    for (int32_t _i = 0; _i < (ctx)->tagInfo.numOfTagCols; ++_i) { \
      SqlFunctionCtx* __ctx = (ctx)->tagInfo.pTagCtxList[_i];      \
      __ctx->fpSet.process(__ctx);                                 \
    }                                                              \
  } while (0);

#define DO_UPDATE_SUBSID_RES(ctx, ts)                          \
  do {                                                         \
    for (int32_t _i = 0; _i < (ctx)->subsidiaries.num; ++_i) { \
      SqlFunctionCtx* __ctx = (ctx)->subsidiaries.pCtx[_i];    \
      if (__ctx->functionId == FUNCTION_TS_DUMMY) {            \
        __ctx->tag.i = (ts);                                   \
        __ctx->tag.nType = TSDB_DATA_TYPE_BIGINT;              \
      }                                                        \
      __ctx->fpSet.process(__ctx);                             \
    }                                                          \
  } while (0)

#define UPDATE_DATA(ctx, left, right, num, sign, _ts) \
  do {                                                \
    if (((left) < (right)) ^ (sign)) {                \
      (left) = (right);                               \
      DO_UPDATE_SUBSID_RES(ctx, _ts);                 \
      (num) += 1;                                     \
    }                                                 \
  } while (0)

#define LOOPCHECK_N(val, _col, ctx, _t, _nrow, _start, sign, num)        \
  do {                                                                   \
    _t* d = (_t*)((_col)->pData);                                        \
    for (int32_t i = (_start); i < (_nrow) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      }                                                                  \
      TSKEY ts = (ctx)->ptsList != NULL ? GET_TS_DATA(ctx, i) : 0;       \
      UPDATE_DATA(ctx, val, d[i], num, sign, ts);                        \
    }                                                                    \
  } while (0)

#define LIST_ADD_N(_res, _col, _start, _rows, _t, numOfElem)             \
  do {                                                                   \
    _t* d = (_t*)(_col->pData);                                          \
    for (int32_t i = (_start); i < (_rows) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      };                                                                 \
      (_res) += (d)[i];                                                  \
      (numOfElem)++;                                                     \
    }                                                                    \
  } while (0)

#define LIST_SUB_N(_res, _col, _start, _rows, _t, numOfElem)             \
  do {                                                                   \
    _t* d = (_t*)(_col->pData);                                          \
    for (int32_t i = (_start); i < (_rows) + (_start); ++i) {            \
      if (((_col)->hasNull) && colDataIsNull_f((_col)->nullbitmap, i)) { \
        continue;                                                        \
      };                                                                 \
      (_res) -= (d)[i];                                                  \
      (numOfElem)++;                                                     \
    }                                                                    \
  } while (0)

#define LIST_AVG_N(sumT, T)                                               \
  do {                                                                    \
    T* plist = (T*)pCol->pData;                                           \
    for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) { \
      if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {        \
        continue;                                                         \
      }                                                                   \
                                                                          \
      numOfElem += 1;                                                     \
      pAvgRes->count -= 1;                                                \
      sumT -= plist[i];                                                   \
    }                                                                     \
  } while (0)

#define LIST_STDDEV_SUB_N(sumT, T)                                 \
  do {                                                             \
    T* plist = (T*)pCol->pData;                                    \
    for (int32_t i = start; i < numOfRows + start; ++i) {          \
      if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) { \
        continue;                                                  \
      }                                                            \
      numOfElem += 1;                                              \
      pStddevRes->count -= 1;                                      \
      sumT -= plist[i];                                            \
      pStddevRes->quadraticISum -= plist[i] * plist[i];            \
    }                                                              \
  } while (0)

#define LEASTSQR_CAL(p, x, y, index, step) \
  do {                                     \
    (p)[0][0] += (double)(x) * (x);        \
    (p)[0][1] += (double)(x);              \
    (p)[0][2] += (double)(x) * (y)[index]; \
    (p)[1][2] += (y)[index];               \
    (x) += step;                           \
  } while (0)

#define STATE_COMP(_op, _lval, _param) STATE_COMP_IMPL(_op, _lval, GET_STATE_VAL(_param))

#define GET_STATE_VAL(param) ((param.nType == TSDB_DATA_TYPE_BIGINT) ? (param.i) : (param.d))

#define STATE_COMP_IMPL(_op, _lval, _rval) \
  do {                                     \
    switch (_op) {                         \
      case STATE_OPER_LT:                  \
        return ((_lval) < (_rval));        \
        break;                             \
      case STATE_OPER_GT:                  \
        return ((_lval) > (_rval));        \
        break;                             \
      case STATE_OPER_LE:                  \
        return ((_lval) <= (_rval));       \
        break;                             \
      case STATE_OPER_GE:                  \
        return ((_lval) >= (_rval));       \
        break;                             \
      case STATE_OPER_NE:                  \
        return ((_lval) != (_rval));       \
        break;                             \
      case STATE_OPER_EQ:                  \
        return ((_lval) == (_rval));       \
        break;                             \
      default:                             \
        break;                             \
    }                                      \
  } while (0)

#define INIT_INTP_POINT(_p, _k, _v) \
  do {                              \
    (_p).key = (_k);                \
    (_p).val = (_v);                \
  } while (0)

bool dummyGetEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* UNUSED_PARAM(pEnv)) { return true; }

bool dummyInit(SqlFunctionCtx* UNUSED_PARAM(pCtx), SResultRowEntryInfo* UNUSED_PARAM(pResultInfo)) { return true; }

int32_t dummyProcess(SqlFunctionCtx* UNUSED_PARAM(pCtx)) { return 0; }

int32_t dummyFinalize(SqlFunctionCtx* UNUSED_PARAM(pCtx), SSDataBlock* UNUSED_PARAM(pBlock)) { return 0; }

bool functionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (pResultInfo->initialized) {
    return false;
  }

  if (pCtx->pOutput != NULL) {
    memset(pCtx->pOutput, 0, (size_t)pCtx->resDataInfo.bytes);
  }

  initResultRowEntry(pResultInfo, pCtx->resDataInfo.interBufSize);
  return true;
}

int32_t functionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->isNullRes == 1) ? 1 : (pResInfo->numOfRes == 0);

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  colDataAppend(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  int32_t              bytes = pDestCtx->input.pData[0]->info.bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (pSResInfo->numOfRes != 0 && (pDResInfo->numOfRes == 0 || pDBuf->ts > pSBuf->ts)) {
    memcpy(pDBuf->buf, pSBuf->buf, bytes);
    pDBuf->ts = pSBuf->ts;
    pDResInfo->numOfRes = 1;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;
  cleanupResultRowEntry(pResInfo);

  char* in = finalResult;
  colDataAppend(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_COLUMN == nodeType(pParam) && PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pParam)->colId) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  }
  return FUNC_DATA_REQUIRED_STATIS_LOAD;
}

bool getCountFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

static FORCE_INLINE int32_t getNumOfElems(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pInputCol->hasNull == true. pInput->colDataAggIsSet == true;
   * 2. for general non-primary key columns, pInputCol->hasNull may be true or false, pInput->colDataAggIsSet == true;
   * 3. for primary key column, pInputCol->hasNull always be false, pInput->colDataAggIsSet == false;
   */
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  if (pInput->colDataAggIsSet && pInput->totalRows == pInput->numOfRows) {
    numOfElem = pInput->numOfRows - pInput->pColumnDataAgg[0]->numOfNull;
    ASSERT(numOfElem >= 0);
  } else {
    if (pInputCol->hasNull) {
      for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
        if (colDataIsNull(pInputCol, pInput->totalRows, i, NULL)) {
          continue;
        }
        numOfElem += 1;
      }
    } else {
      // when counting on the primary time stamp column and no statistics data is presented, use the size value
      // directly.
      numOfElem = pInput->numOfRows;
    }
  }
  return numOfElem;
}

/*
 * count function does need the finalize, if data is missing, the default value, which is 0, is used
 * count function does not use the pCtx->interResBuf to keep the intermediate buffer
 */
int32_t countFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = getNumOfElems(pCtx);

  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  SInputColumnInfoData* pInput = &pCtx->input;

  int32_t type = pInput->pData[0]->info.type;

  char* buf = GET_ROWCELL_INTERBUF(pResInfo);
  if (IS_NULL_TYPE(type)) {
    // select count(NULL) returns 0
    numOfElem = 1;
    *((int64_t*)buf) = 0;
  } else {
    *((int64_t*)buf) += numOfElem;
  }

  if (tsCountAlwaysReturnValue) {
    pResInfo->numOfRes = 1;
  } else {
    SET_VAL(pResInfo, 1, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t countInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = getNumOfElems(pCtx);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t*)buf) -= numOfElem;

  SET_VAL(pResInfo, *((int64_t*)buf), 1);
  return TSDB_CODE_SUCCESS;
}

int32_t combineFunction(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  char*                pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  char*                pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  *((int64_t*)pDBuf) += *((int64_t*)pSBuf);

  SET_VAL(pDResInfo, *((int64_t*)pDBuf), 1);
  return TSDB_CODE_SUCCESS;
}

int32_t sumFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (IS_NULL_TYPE(type)) {
    numOfElem = 0;
    goto _sum_over;
  }

  if (pInput->colDataAggIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;
    ASSERT(numOfElem >= 0);

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pSumRes->isum += pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pSumRes->usum += pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pSumRes->dsum += GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
      if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_SMALLINT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_INT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_BIGINT) {
        LIST_ADD_N(pSumRes->isum, pCol, start, numOfRows, int64_t, numOfElem);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      if (type == TSDB_DATA_TYPE_UTINYINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_USMALLINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UBIGINT) {
        LIST_ADD_N(pSumRes->usum, pCol, start, numOfRows, uint64_t, numOfElem);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      LIST_ADD_N(pSumRes->dsum, pCol, start, numOfRows, double, numOfElem);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      LIST_ADD_N(pSumRes->dsum, pCol, start, numOfRows, float, numOfElem);
    }
  }

  // check for overflow
  if (IS_FLOAT_TYPE(type) && (isinf(pSumRes->dsum) || isnan(pSumRes->dsum))) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElem = 1;
  }

_sum_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t sumInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pInput->colDataAggIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;
    ASSERT(numOfElem >= 0);

    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pSumRes->isum -= pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pSumRes->usum -= pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pSumRes->dsum -= GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
      if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_SMALLINT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_INT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_BIGINT) {
        LIST_SUB_N(pSumRes->isum, pCol, start, numOfRows, int64_t, numOfElem);
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      if (type == TSDB_DATA_TYPE_UTINYINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint8_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_USMALLINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint16_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint32_t, numOfElem);
      } else if (type == TSDB_DATA_TYPE_UBIGINT) {
        LIST_SUB_N(pSumRes->usum, pCol, start, numOfRows, uint64_t, numOfElem);
      }
    } else if (type == TSDB_DATA_TYPE_DOUBLE) {
      LIST_SUB_N(pSumRes->dsum, pCol, start, numOfRows, double, numOfElem);
    } else if (type == TSDB_DATA_TYPE_FLOAT) {
      LIST_SUB_N(pSumRes->dsum, pCol, start, numOfRows, float, numOfElem);
    }
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t sumCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SSumRes*             pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSumRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    pDBuf->isum += pSBuf->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->usum += pSBuf->usum;
  } else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
    pDBuf->dsum += pSBuf->dsum;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

int32_t getAvgInfoSize() { return (int32_t)sizeof(SAvgRes); }

bool getAvgFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SAvgRes);
  return true;
}

bool avgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SAvgRes* pRes = GET_ROWCELL_INTERBUF(pResultInfo);
  memset(pRes, 0, sizeof(SAvgRes));
  return true;
}

int32_t avgFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SAvgRes* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pAvgRes->type = type;

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(type)) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElem = 1;
    goto _avg_over;
  }

  if (pInput->colDataAggIsSet) {
    numOfElem = numOfRows - pAgg->numOfNull;
    ASSERT(numOfElem >= 0);

    pAvgRes->count += numOfElem;
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      pAvgRes->sum.isum += pAgg->sum;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      pAvgRes->sum.usum += pAgg->sum;
    } else if (IS_FLOAT_TYPE(type)) {
      pAvgRes->sum.dsum += GET_DOUBLE_VAL((const char*)&(pAgg->sum));
    }
  } else {  // computing based on the true data block
    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t* plist = (int8_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.isum += plist[i];
        }

        break;
      }

      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t* plist = (int16_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.isum += plist[i];
        }
        break;
      }

      case TSDB_DATA_TYPE_INT: {
        int32_t* plist = (int32_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.isum += plist[i];
        }

        break;
      }

      case TSDB_DATA_TYPE_BIGINT: {
        int64_t* plist = (int64_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.isum += plist[i];
        }
        break;
      }

      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t* plist = (uint8_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.usum += plist[i];
        }

        break;
      }

      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t* plist = (uint16_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.usum += plist[i];
        }
        break;
      }

      case TSDB_DATA_TYPE_UINT: {
        uint32_t* plist = (uint32_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.usum += plist[i];
        }

        break;
      }

      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t* plist = (uint64_t*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.usum += plist[i];
        }
        break;
      }

      case TSDB_DATA_TYPE_FLOAT: {
        float* plist = (float*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.dsum += plist[i];
        }
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        double* plist = (double*)pCol->pData;
        for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
          if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
            continue;
          }

          numOfElem += 1;
          pAvgRes->count += 1;
          pAvgRes->sum.dsum += plist[i];
        }
        break;
      }

      default:
        break;
    }
  }

_avg_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

static void avgTransferInfo(SAvgRes* pInput, SAvgRes* pOutput) {
  pOutput->type = pInput->type;
  if (IS_SIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->sum.isum += pInput->sum.isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->sum.usum += pInput->sum.usum;
  } else {
    pOutput->sum.dsum += pInput->sum.dsum;
  }

  pOutput->count += pInput->count;

  return;
}

int32_t avgFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SAvgRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*    data = colDataGetData(pCol, i);
    SAvgRes* pInputInfo = (SAvgRes*)varDataVal(data);
    avgTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t avgInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SAvgRes* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      LIST_AVG_N(pAvgRes->sum.isum, int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      LIST_AVG_N(pAvgRes->sum.isum, int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      LIST_AVG_N(pAvgRes->sum.isum, int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      LIST_AVG_N(pAvgRes->sum.isum, int64_t);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      LIST_AVG_N(pAvgRes->sum.usum, uint8_t);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      LIST_AVG_N(pAvgRes->sum.usum, uint16_t);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      LIST_AVG_N(pAvgRes->sum.usum, uint32_t);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      LIST_AVG_N(pAvgRes->sum.usum, uint64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      LIST_AVG_N(pAvgRes->sum.dsum, float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      LIST_AVG_N(pAvgRes->sum.dsum, double);
      break;
    }
    default:
      break;
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t avgCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SAvgRes*             pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAvgRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    pDBuf->sum.isum += pSBuf->sum.isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->sum.usum += pSBuf->sum.usum;
  } else {
    pDBuf->sum.dsum += pSBuf->sum.dsum;
  }
  pDBuf->count += pSBuf->count;

  return TSDB_CODE_SUCCESS;
}

int32_t avgFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;

  SAvgRes* pAvgRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t  type = pAvgRes->type;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    pAvgRes->result = pAvgRes->sum.isum / ((double)pAvgRes->count);
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pAvgRes->result = pAvgRes->sum.usum / ((double)pAvgRes->count);
  } else {
    pAvgRes->result = pAvgRes->sum.dsum / ((double)pAvgRes->count);
  }

  // check for overflow
  if (isinf(pAvgRes->result) || isnan(pAvgRes->result)) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t avgPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAvgRes*             pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getAvgInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  return FUNC_DATA_REQUIRED_STATIS_LOAD;
}

bool minmaxFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;  // not initialized since it has been initialized
  }

  SMinmaxResInfo* buf = GET_ROWCELL_INTERBUF(pResultInfo);
  buf->assign = false;
  buf->tuplePos.pageId = -1;

  buf->nullTupleSaved = false;
  buf->nullTuplePos.pageId = -1;
  return true;
}

bool getMinmaxFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMinmaxResInfo);
  return true;
}

static void doSaveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);
static void doCopyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos);

static int32_t findRowIndex(int32_t start, int32_t num, SColumnInfoData* pCol, const char* tval) {
  // the data is loaded, not only the block SMA value
  for (int32_t i = start; i < num + start; ++i) {
    char* p = colDataGetData(pCol, i);
    if (memcpy((void*)tval, p, pCol->info.bytes) == 0) {
      return i;
    }
  }

  ASSERT(0);
}

int32_t doMinMaxHelper(SqlFunctionCtx* pCtx, int32_t isMinFunc) {
  int32_t numOfElems = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pBuf = GET_ROWCELL_INTERBUF(pResInfo);

  if (IS_NULL_TYPE(type)) {
    numOfElems = 0;
    goto _min_max_over;
  }

  // data in current data block are qualified to the query
  if (pInput->colDataAggIsSet) {
    numOfElems = pInput->numOfRows - pAgg->numOfNull;
    ASSERT(pInput->numOfRows == pInput->totalRows && numOfElems >= 0);
    if (numOfElems == 0) {
      return numOfElems;
    }

    void*   tval = NULL;
    int16_t index = 0;

    if (isMinFunc) {
      tval = &pInput->pColumnDataAgg[0]->min;
    } else {
      tval = &pInput->pColumnDataAgg[0]->max;
    }

    if (!pBuf->assign) {
      pBuf->v = *(int64_t*)tval;
      if (pCtx->subsidiaries.num > 0) {
        index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
        doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
      }
    } else {
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        int64_t prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        int64_t val = GET_INT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }

      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        uint64_t prev = 0;
        GET_TYPED_DATA(prev, uint64_t, type, &pBuf->v);

        uint64_t val = GET_UINT64_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      } else if (type == TSDB_DATA_TYPE_DOUBLE) {
        double prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        double val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
          if (pCtx->subsidiaries.num > 0) {
            index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
            doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      } else if (type == TSDB_DATA_TYPE_FLOAT) {
        double prev = 0;
        GET_TYPED_DATA(prev, int64_t, type, &pBuf->v);

        double val = GET_DOUBLE_VAL(tval);
        if ((prev < val) ^ isMinFunc) {
          pBuf->v = val;
        }

        if (pCtx->subsidiaries.num > 0) {
          index = findRowIndex(pInput->startRowIndex, pInput->numOfRows, pCol, tval);
          doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pBuf->tuplePos);
        }
      }
    }

    pBuf->assign = true;
    return numOfElems;
  }

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
      int8_t* pData = (int8_t*)pCol->pData;
      int8_t* val = (int8_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_SMALLINT) {
      int16_t* pData = (int16_t*)pCol->pData;
      int16_t* val = (int16_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_INT) {
      int32_t* pData = (int32_t*)pCol->pData;
      int32_t* val = (int32_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_BIGINT) {
      int64_t* pData = (int64_t*)pCol->pData;
      int64_t* val = (int64_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    }
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    if (type == TSDB_DATA_TYPE_UTINYINT) {
      uint8_t* pData = (uint8_t*)pCol->pData;
      uint8_t* val = (uint8_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_USMALLINT) {
      uint16_t* pData = (uint16_t*)pCol->pData;
      uint16_t* val = (uint16_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_UINT) {
      uint32_t* pData = (uint32_t*)pCol->pData;
      uint32_t* val = (uint32_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    } else if (type == TSDB_DATA_TYPE_UBIGINT) {
      uint64_t* pData = (uint64_t*)pCol->pData;
      uint64_t* val = (uint64_t*)&pBuf->v;

      for (int32_t i = start; i < start + numOfRows; ++i) {
        if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        if (!pBuf->assign) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
          pBuf->assign = true;
        } else {
          // ignore the equivalent data value
          if ((*val) == pData[i]) {
            continue;
          }

          if ((*val < pData[i]) ^ isMinFunc) {
            *val = pData[i];
            if (pCtx->subsidiaries.num > 0) {
              doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
            }
          }
        }

        numOfElems += 1;
      }
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    double* pData = (double*)pCol->pData;
    double* val = (double*)&pBuf->v;

    for (int32_t i = start; i < start + numOfRows; ++i) {
      if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      if (!pBuf->assign) {
        *val = pData[i];
        if (pCtx->subsidiaries.num > 0) {
          doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
        }
        pBuf->assign = true;
      } else {
        // ignore the equivalent data value
        if ((*val) == pData[i]) {
          continue;
        }

        if ((*val < pData[i]) ^ isMinFunc) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      }

      numOfElems += 1;
    }
  } else if (type == TSDB_DATA_TYPE_FLOAT) {
    float*  pData = (float*)pCol->pData;
    double* val = (double*)&pBuf->v;

    for (int32_t i = start; i < start + numOfRows; ++i) {
      if ((pCol->hasNull) && colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      if (!pBuf->assign) {
        *val = pData[i];
        if (pCtx->subsidiaries.num > 0) {
          doSaveTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
        }
        pBuf->assign = true;
      } else {
        // ignore the equivalent data value
        if ((*val) == pData[i]) {
          continue;
        }

        if ((*val < pData[i]) ^ isMinFunc) {
          *val = pData[i];
          if (pCtx->subsidiaries.num > 0) {
            doCopyTupleData(pCtx, i, pCtx->pSrcBlock, &pBuf->tuplePos);
          }
        }
      }

      numOfElems += 1;
    }
  }

_min_max_over:
  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pBuf->nullTupleSaved ) {
    doSaveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pBuf->nullTuplePos);
    pBuf->nullTupleSaved = true;
  }
  return numOfElems;
}

int32_t minFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = doMinMaxHelper(pCtx, 1);
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t maxFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = doMinMaxHelper(pCtx, 0);
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static void setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex);

static void setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos, int32_t rIndex);

int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  SMinmaxResInfo* pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t currentRow = pBlock->info.rows;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  pEntryInfo->isNullRes = (pEntryInfo->isNullRes == 1) ? 1 : (pEntryInfo->numOfRes == 0);

  if (pCol->info.type == TSDB_DATA_TYPE_FLOAT) {
    float v = *(double*)&pRes->v;
    colDataAppend(pCol, currentRow, (const char*)&v, pEntryInfo->isNullRes);
  } else {
    colDataAppend(pCol, currentRow, (const char*)&pRes->v, pEntryInfo->isNullRes);
  }

  if (pEntryInfo->numOfRes > 0) {
    setSelectivityValue(pCtx, pBlock, &pRes->tuplePos, currentRow);
  } else {
    setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
  }

  return pEntryInfo->numOfRes;
}

void setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return;
  }

  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
    int32_t         dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colDataAppendNULL(pDstCol, rowIndex);
  }
}

void setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return;
  }

  int32_t pageId = pTuplePos->pageId;
  int32_t offset = pTuplePos->offset;

  if (pTuplePos->pageId != -1) {
    int32_t    numOfCols = pCtx->subsidiaries.num;
    SFilePage* pPage = getBufPage(pCtx->pBuf, pageId);

    bool* nullList = (bool*)((char*)pPage + offset);
    char* pStart = (char*)(nullList + numOfCols * sizeof(bool));

    // todo set the offset value to optimize the performance.
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

      SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
      int32_t      dstSlotId = pc->pExpr->base.resSchema.slotId;

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      ASSERT(pc->pExpr->base.resSchema.bytes == pDstCol->info.bytes);
      if (nullList[j]) {
        colDataAppendNULL(pDstCol, rowIndex);
      } else {
        colDataAppend(pDstCol, rowIndex, pStart, false);
      }
      pStart += pDstCol->info.bytes;
    }

    releaseBufPage(pCtx->pBuf, pPage);
  }
}

void releaseSource(STuplePos* pPos) {
  if (pPos->pageId == -1) {
    return;
  }
  // Todo(liuyao) relase row
}

// This function append the selectivity to subsidiaries function context directly, without fetching data
// from intermediate disk based buf page
void appendSelectivityValue(SqlFunctionCtx* pCtx, int32_t rowIndex, int32_t pos) {
  if (pCtx->subsidiaries.num <= 0) {
    return;
  }

  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];

    // get data from source col
    SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t      srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pSrcCol = taosArrayGet(pCtx->pSrcBlock->pDataBlock, srcSlotId);

    char* pData = colDataGetData(pSrcCol, rowIndex);

    // append to dest col
    int32_t  dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pCtx->pDstBlock->pDataBlock, dstSlotId);
    ASSERT(pc->pExpr->base.resSchema.bytes == pDstCol->info.bytes);

    if (colDataIsNull_s(pSrcCol, rowIndex) == true) {
      colDataAppendNULL(pDstCol, pos);
    } else {
      colDataAppend(pDstCol, pos, pData, false);
    }
  }

}

void replaceTupleData(STuplePos* pDestPos, STuplePos* pSourcePos) {
  releaseSource(pDestPos);
  *pDestPos = *pSourcePos;
}

int32_t minMaxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t isMinFunc) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SMinmaxResInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SMinmaxResInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  if (IS_FLOAT_TYPE(type)) {
    if (pSBuf->assign && ((((*(double*)&pDBuf->v) < (*(double*)&pSBuf->v)) ^ isMinFunc) || !pDBuf->assign)) {
      *(double*)&pDBuf->v = *(double*)&pSBuf->v;
      replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
      pDBuf->assign = true;
    }
  } else {
    if (pSBuf->assign && (((pDBuf->v < pSBuf->v) ^ isMinFunc) || !pDBuf->assign)) {
      pDBuf->v = pSBuf->v;
      replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
      pDBuf->assign = true;
    }
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

int32_t minCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 1);
}
int32_t maxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  return minMaxCombine(pDestCtx, pSourceCtx, 0);
}

int32_t getStddevInfoSize() { return (int32_t)sizeof(SStddevRes); }

bool getStddevFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStddevRes);
  return true;
}

bool stddevFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SStddevRes* pRes = GET_ROWCELL_INTERBUF(pResultInfo);
  memset(pRes, 0, sizeof(SStddevRes));
  return true;
}

int32_t stddevFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SStddevRes* pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pStddevRes->type = type;

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(type)) {
    GET_RES_INFO(pCtx)->isNullRes = 1;
    numOfElem = 1;
    goto _stddev_over;
  }

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* plist = (int8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->isum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* plist = (int16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->isum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* plist = (int32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->isum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* plist = (int64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->isum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* plist = (uint8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + start; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->usum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* plist = (uint16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->usum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t* plist = (uint32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->usum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }

      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* plist = (uint64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->usum += plist[i];
        pStddevRes->quadraticISum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float* plist = (float*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->dsum += plist[i];
        pStddevRes->quadraticDSum += plist[i] * plist[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* plist = (double*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem += 1;
        pStddevRes->count += 1;
        pStddevRes->dsum += plist[i];
        pStddevRes->quadraticDSum += plist[i] * plist[i];
      }
      break;
    }

    default:
      break;
  }

_stddev_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

static void stddevTransferInfo(SStddevRes* pInput, SStddevRes* pOutput) {
  pOutput->type = pInput->type;
  if (IS_SIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->quadraticISum += pInput->quadraticISum;
    pOutput->isum += pInput->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(pOutput->type)) {
    pOutput->quadraticUSum += pInput->quadraticUSum;
    pOutput->usum += pInput->usum;
  } else {
    pOutput->quadraticDSum += pInput->quadraticDSum;
    pOutput->dsum += pInput->dsum;
  }

  pOutput->count += pInput->count;
}

int32_t stddevFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SStddevRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
    char*       data = colDataGetData(pCol, i);
    SStddevRes* pInputInfo = (SStddevRes*)varDataVal(data);
    stddevTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t stddevInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SStddevRes* pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  // computing based on the true data block
  SColumnInfoData* pCol = pInput->pData[0];

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, int8_t);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, int16_t);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, int32_t);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, int64_t);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, uint8_t);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, uint16_t);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, uint32_t);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      LIST_STDDEV_SUB_N(pStddevRes->isum, uint64_t);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      LIST_STDDEV_SUB_N(pStddevRes->dsum, float);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      LIST_STDDEV_SUB_N(pStddevRes->dsum, double);
      break;
    }
    default:
      break;
  }

  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SStddevRes*           pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t               type = pStddevRes->type;
  double                avg;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    avg = pStddevRes->isum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticISum / ((double)pStddevRes->count) - avg * avg));
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    avg = pStddevRes->usum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticUSum / ((double)pStddevRes->count) - avg * avg));
  } else {
    avg = pStddevRes->dsum / ((double)pStddevRes->count);
    pStddevRes->result = sqrt(fabs(pStddevRes->quadraticDSum / ((double)pStddevRes->count) - avg * avg));
  }

  // check for overflow
  if (isinf(pStddevRes->result) || isnan(pStddevRes->result)) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t stddevPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStddevRes*          pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getStddevInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t stddevCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SStddevRes*          pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SStddevRes*          pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    pDBuf->isum += pSBuf->isum;
    pDBuf->quadraticISum += pSBuf->quadraticISum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->usum += pSBuf->usum;
    pDBuf->quadraticUSum += pSBuf->quadraticUSum;
  } else {
    pDBuf->dsum += pSBuf->dsum;
    pDBuf->quadraticDSum += pSBuf->quadraticDSum;
  }
  pDBuf->count += pSBuf->count;
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

bool getLeastSQRFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SLeastSQRInfo);
  return true;
}

bool leastSQRFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SLeastSQRInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  pInfo->startVal = IS_FLOAT_TYPE(pCtx->param[1].param.nType) ? pCtx->param[1].param.d : (double)pCtx->param[1].param.i;
  pInfo->stepVal = IS_FLOAT_TYPE(pCtx->param[2].param.nType) ? pCtx->param[2].param.d : (double)pCtx->param[2].param.i;
  return true;
}

int32_t leastSQRFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  SInputColumnInfoData* pInput = &pCtx->input;
  int32_t               type = pInput->pData[0]->info.type;

  SLeastSQRInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SColumnInfoData* pCol = pInput->pData[0];

  double(*param)[3] = pInfo->matrix;
  double x = pInfo->startVal;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* plist = (int8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* plist = (int16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* plist = (int32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* plist = (int64_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_FLOAT: {
      float* plist = (float*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* plist = (double*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_NULL: {
      GET_RES_INFO(pCtx)->isNullRes = 1;
      numOfElem = 1;
      break;
    }

    default:
      break;
  }

  pInfo->startVal = x;
  pInfo->num += numOfElem;

  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t leastSQRFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SLeastSQRInfo*       pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;

  if (0 == pInfo->num) {
    colDataAppendNULL(pCol, currentRow);
    return 0;
  }

  double(*param)[3] = pInfo->matrix;

  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];

  double param00 = param[0][0] - param[1][0] * (param[0][1] / param[1][1]);
  double param02 = param[0][2] - param[1][2] * (param[0][1] / param[1][1]);
  // param[0][1] = 0;
  double param12 = param[1][2] - param02 * (param[1][0] / param00);
  // param[1][0] = 0;
  param02 /= param00;

  param12 /= param[1][1];

  char   buf[64] = {0};
  size_t len =
      snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%.6lf, intercept:%.6lf}", param02, param12);
  varDataSetLen(buf, len);

  colDataAppend(pCol, currentRow, buf, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t leastSQRInvertFunction(SqlFunctionCtx* pCtx) {
  // TODO
  return TSDB_CODE_SUCCESS;
}

int32_t leastSQRCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SLeastSQRInfo*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  double(*pDparam)[3] = pDBuf->matrix;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SLeastSQRInfo*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  double(*pSparam)[3] = pSBuf->matrix;
  for (int32_t i = 0; i < pSBuf->num; i++) {
    pDparam[0][0] += pDBuf->startVal * pDBuf->startVal;
    pDparam[0][1] += pDBuf->startVal;
    pDBuf->startVal += pDBuf->stepVal;
  }
  pDparam[0][2] += pSparam[0][2] + pDBuf->num * pDBuf->stepVal * pSparam[1][2];
  pDparam[1][2] += pSparam[1][2];
  pDBuf->num += pSBuf->num;
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

bool getPercentileFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SPercentileInfo);
  return true;
}

bool percentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  // in the first round, get the min-max value of all involved data
  SPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->minval, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->maxval, -DBL_MAX);
  pInfo->numOfElems = 0;

  return true;
}

int32_t percentileFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  if (pCtx->scanFlag == REPEAT_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      return 0;
    } else {
      pInfo->pMemBucket = tMemBucketCreate(pCol->info.bytes, type, pInfo->minval, pInfo->maxval);
    }
  }

  // the first stage, only acquire the min/max value
  if (pInfo->stage == 0) {
    if (pCtx->input.colDataAggIsSet) {
      double tmin = 0.0, tmax = 0.0;
      if (IS_SIGNED_NUMERIC_TYPE(type)) {
        tmin = (double)GET_INT64_VAL(&pAgg->min);
        tmax = (double)GET_INT64_VAL(&pAgg->max);
      } else if (IS_FLOAT_TYPE(type)) {
        tmin = GET_DOUBLE_VAL(&pAgg->min);
        tmax = GET_DOUBLE_VAL(&pAgg->max);
      } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
        tmin = (double)GET_UINT64_VAL(&pAgg->min);
        tmax = (double)GET_UINT64_VAL(&pAgg->max);
      }

      if (GET_DOUBLE_VAL(&pInfo->minval) > tmin) {
        SET_DOUBLE_VAL(&pInfo->minval, tmin);
      }

      if (GET_DOUBLE_VAL(&pInfo->maxval) < tmax) {
        SET_DOUBLE_VAL(&pInfo->maxval, tmax);
      }

      pInfo->numOfElems += (pInput->numOfRows - pAgg->numOfNull);
    } else {
      // check the valid data one by one
      int32_t start = pInput->startRowIndex;
      for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
        if (colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        char* data = colDataGetData(pCol, i);

        double v = 0;
        GET_TYPED_DATA(v, double, type, data);
        if (v < GET_DOUBLE_VAL(&pInfo->minval)) {
          SET_DOUBLE_VAL(&pInfo->minval, v);
        }

        if (v > GET_DOUBLE_VAL(&pInfo->maxval)) {
          SET_DOUBLE_VAL(&pInfo->maxval, v);
        }

        pInfo->numOfElems += 1;
      }
    }
  } else {
    // the second stage, calculate the true percentile value
    int32_t start = pInput->startRowIndex;
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      char* data = colDataGetData(pCol, i);
      numOfElems += 1;
      tMemBucketPut(pInfo->pMemBucket, data, 1);
    }

    SET_VAL(pResInfo, numOfElems, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SVariant* pVal = &pCtx->param[1].param;
  double    v =
      (IS_SIGNED_NUMERIC_TYPE(pVal->nType) ? pVal->i : (IS_UNSIGNED_NUMERIC_TYPE(pVal->nType) ? pVal->u : pVal->d));

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo*     ppInfo = (SPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  tMemBucket* pMemBucket = ppInfo->pMemBucket;
  if (pMemBucket != NULL && pMemBucket->total > 0) {  // check for null
    SET_DOUBLE_VAL(&ppInfo->result, getPercentile(pMemBucket, v));
  }

  tMemBucketDestroy(pMemBucket);
  return functionFinalize(pCtx, pBlock);
}

bool getApercentileFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  int32_t bytesHist =
      (int32_t)(sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
  int32_t bytesDigest = (int32_t)(sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
  pEnv->calcMemSize = TMAX(bytesHist, bytesDigest);
  return true;
}

int32_t getApercentileMaxSize() {
  int32_t bytesHist =
      (int32_t)(sizeof(SAPercentileInfo) + sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
  int32_t bytesDigest = (int32_t)(sizeof(SAPercentileInfo) + TDIGEST_SIZE(COMPRESSION));
  return TMAX(bytesHist, bytesDigest);
}

static int8_t getApercentileAlgo(char* algoStr) {
  int8_t algoType;
  if (strcasecmp(algoStr, "default") == 0) {
    algoType = APERCT_ALGO_DEFAULT;
  } else if (strcasecmp(algoStr, "t-digest") == 0) {
    algoType = APERCT_ALGO_TDIGEST;
  } else {
    algoType = APERCT_ALGO_UNKNOWN;
  }

  return algoType;
}

static void buildHistogramInfo(SAPercentileInfo* pInfo) {
  pInfo->pHisto = (SHistogramInfo*)((char*)pInfo + sizeof(SAPercentileInfo));
  pInfo->pHisto->elems = (SHistBin*)((char*)pInfo->pHisto + sizeof(SHistogramInfo));
}

static void buildTDigestInfo(SAPercentileInfo* pInfo) {
  pInfo->pTDigest = (TDigest*)((char*)pInfo + sizeof(SAPercentileInfo));
}

bool apercentileFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);

  SVariant* pVal = &pCtx->param[1].param;
  pInfo->percent =
      (IS_SIGNED_NUMERIC_TYPE(pVal->nType) ? pVal->i : (IS_UNSIGNED_NUMERIC_TYPE(pVal->nType) ? pVal->u : pVal->d));

  if (pCtx->numOfParams == 2) {
    pInfo->algo = APERCT_ALGO_DEFAULT;
  } else if (pCtx->numOfParams == 3) {
    pInfo->algo = getApercentileAlgo(varDataVal(pCtx->param[2].param.pz));
    if (pInfo->algo == APERCT_ALGO_UNKNOWN) {
      return false;
    }
  }

  char* tmp = (char*)pInfo + sizeof(SAPercentileInfo);
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    pInfo->pTDigest = tdigestNewFrom(tmp, COMPRESSION);
  } else {
    buildHistogramInfo(pInfo);
    pInfo->pHisto = tHistogramCreateFrom(tmp, MAX_HISTOGRAM_BIN);
    qDebug("%s set up histogram, numOfElems:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems:%p", __FUNCTION__,
           pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);
  }

  return true;
}

int32_t apercentileFunction(SqlFunctionCtx* pCtx) {
  int32_t               numOfElems = 0;
  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pCol = pInput->pData[0];
  int32_t          type = pCol->info.type;

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  int32_t start = pInput->startRowIndex;
  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      numOfElems += 1;
      char* data = colDataGetData(pCol, i);

      double  v = 0;  // value
      int64_t w = 1;  // weigth
      GET_TYPED_DATA(v, double, type, data);
      tdigestAdd(pInfo->pTDigest, v, w);
    }
  } else {
    qDebug("%s before add %d elements into histogram, total:%d, numOfEntry:%d, pHisto:%p, elems: %p", __FUNCTION__,
           numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);

    // might be a race condition here that pHisto can be overwritten or setup function
    // has not been called, need to relink the buffer pHisto points to.
    buildHistogramInfo(pInfo);
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      numOfElems += 1;
      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      tHistogramAdd(&pInfo->pHisto, v);
    }

    qDebug("%s after add %d elements into histogram, total:%d, numOfEntry:%d, pHisto:%p, elems: %p", __FUNCTION__,
           numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static void apercentileTransferInfo(SAPercentileInfo* pInput, SAPercentileInfo* pOutput) {
  pOutput->percent = pInput->percent;
  pOutput->algo = pInput->algo;
  if (pOutput->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInput);
    tdigestAutoFill(pInput->pTDigest, COMPRESSION);

    if (pInput->pTDigest->num_centroids == 0 && pInput->pTDigest->num_buffered_pts == 0) {
      return;
    }

    buildTDigestInfo(pOutput);
    TDigest* pTDigest = pOutput->pTDigest;

    if (pTDigest->num_centroids <= 0) {
      memcpy(pTDigest, pInput->pTDigest, (size_t)TDIGEST_SIZE(COMPRESSION));
      tdigestAutoFill(pTDigest, COMPRESSION);
    } else {
      tdigestMerge(pTDigest, pInput->pTDigest);
    }
  } else {
    buildHistogramInfo(pInput);
    if (pInput->pHisto->numOfElems <= 0) {
      return;
    }

    buildHistogramInfo(pOutput);
    SHistogramInfo* pHisto = pOutput->pHisto;

    if (pHisto->numOfElems <= 0) {
      memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems,
              pHisto->numOfEntries, pHisto);
    } else {
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));
      qDebug("%s input histogram, elem:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems,
             pHisto->numOfEntries, pInput->pHisto);

      SHistogramInfo* pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
      memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems,
             pHisto->numOfEntries, pHisto);
      tHistogramDestroy(&pRes);
    }
  }
}

int32_t apercentileFunctionMerge(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  qDebug("%s total %d rows will merge, %p", __FUNCTION__, pInput->numOfRows, pInfo->pHisto);

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char* data = colDataGetData(pCol, i);

    SAPercentileInfo* pInputInfo = (SAPercentileInfo*)varDataVal(data);
    apercentileTransferInfo(pInputInfo, pInfo);
  }

  if (pInfo->algo != APERCT_ALGO_TDIGEST) {
    qDebug("%s after merge, total:%d, numOfEntry:%d, %p", __FUNCTION__, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries,
           pInfo->pHisto);
  }

  SET_VAL(pResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*    pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInfo);
    if (pInfo->pTDigest->size > 0) {
      pInfo->result = tdigestQuantile(pInfo->pTDigest, pInfo->percent / 100);
    } else {  // no need to free
      // setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return TSDB_CODE_SUCCESS;
    }
  } else {
    buildHistogramInfo(pInfo);
    if (pInfo->pHisto->numOfElems > 0) {
      qDebug("%s get the final res, elements:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems:%p", __FUNCTION__,
             pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto, pInfo->pHisto->elems);

      double  ratio[] = {pInfo->percent};
      double* res = tHistogramUniform(pInfo->pHisto, ratio, 1);
      pInfo->result = *res;
      // memcpy(pCtx->pOutput, res, sizeof(double));
      taosMemoryFree(res);
    } else {  // no need to free
      // setNull(pCtx->pOutput, pCtx->outputType, pCtx->outputBytes);
      return TSDB_CODE_SUCCESS;
    }
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t apercentilePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*    pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  int32_t resultBytes = getApercentileMaxSize();
  char*   res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    memcpy(varDataVal(res), pInfo, resultBytes);
    varDataSetLen(res, resultBytes);
  } else {
    memcpy(varDataVal(res), pInfo, resultBytes);
    varDataSetLen(res, resultBytes);
  }

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t apercentileCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SAPercentileInfo*    pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAPercentileInfo*    pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  qDebug("%s start to combine apercentile, %p", __FUNCTION__, pDBuf->pHisto);

  apercentileTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

EFuncDataRequired lastDynDataReq(void* pRes, STimeWindow* pTimeWindow) {
  SResultRowEntryInfo* pEntry = (SResultRowEntryInfo*) pRes;

  // not initialized yet, data is required
  if (pEntry == NULL) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SFirstLastRes* pResult = GET_ROWCELL_INTERBUF(pEntry);
  if (pResult->hasResult && pResult->ts >= pTimeWindow->ekey) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  } else {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
}

int32_t getFirstLastInfoSize(int32_t resBytes) { return sizeof(SFirstLastRes) + resBytes; }

bool getFirstLastFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = getFirstLastInfoSize(pNode->node.resType.bytes);
  return true;
}

bool getSelectivityFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = pNode->node.resType.bytes;
  return true;
}

bool getGroupKeyFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pNode = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  pEnv->calcMemSize = sizeof(SGroupKeyInfo) + pNode->node.resType.bytes;
  return true;
}

static FORCE_INLINE TSKEY getRowPTs(SColumnInfoData* pTsColInfo, int32_t rowIndex) {
  if (pTsColInfo == NULL) {
    return 0;
  }

  return *(TSKEY*)colDataGetData(pTsColInfo, rowIndex);
}

static void saveTupleData(const SSDataBlock* pSrcBlock, int32_t rowIndex, SqlFunctionCtx* pCtx, SFirstLastRes* pInfo) {
  if (pCtx->subsidiaries.num <= 0) {
    return;
  }

  if (!pInfo->hasResult) {
    doSaveTupleData(pCtx, rowIndex, pSrcBlock, &pInfo->pos);
  } else {
    doCopyTupleData(pCtx, rowIndex, pSrcBlock, &pInfo->pos);
  }
}

static void doSaveCurrentVal(SqlFunctionCtx* pCtx, int32_t rowIndex, int64_t currentTs, int32_t type, char* pData) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (IS_VAR_DATA_TYPE(type)) {
    pInfo->bytes = varDataTLen(pData);
  }

  memcpy(pInfo->buf, pData, pInfo->bytes);
  pInfo->ts = currentTs;
  saveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo);

  pInfo->hasResult = true;
}

// This ordinary first function does not care if current scan is ascending order or descending order scan
// the OPTIMIZED version of first function will only handle the ascending order scan
int32_t firstFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  pInfo->bytes = pInputCol->info.bytes;

  // All null data column, return directly.
  if (pInput->colDataAggIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows)) {
    ASSERT(pInputCol->hasNull == true);
    return 0;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataAggIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first function.
  //  we will use this opt implementation in an new version that is only available in scan subplan
#if 0
  if (blockDataOrder == TSDB_ORDER_ASC) {
    // filter according to current result firstly
    if (pResInfo->numOfRes > 0) {
      if (pInfo->ts < startKey) {
        return TSDB_CODE_SUCCESS;
      }
    }

    for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
        doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
        break;
      }
    }
  } else {
    // in case of descending order time stamp serial, which usually happens as the results of the nest query,
    // all data needs to be check.
    if (pResInfo->numOfRes > 0) {
      if (pInfo->ts < endKey) {
        return TSDB_CODE_SUCCESS;
      }
    }

    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);

      if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
        doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
        break;
      }
    }
  }
#else
  for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
    if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
      continue;
    }

    numOfElems++;

    char* data = colDataGetData(pInputCol, i);
    TSKEY cts = getRowPTs(pInput->pPTS, i);
    if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
      doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
      pResInfo->numOfRes = 1;
    }
  }
#endif

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t lastFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t type = pInputCol->info.type;
  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  // All null data column, return directly.
  if (pInput->colDataAggIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows)) {
    ASSERT(pInputCol->hasNull == true);
    return 0;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataAggIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first function.
#if 0
  if (blockDataOrder == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveCurrentVal(pCtx, i, cts, type, data);
      }

      break;
    }
  } else {  // descending order
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveCurrentVal(pCtx, i, cts, type, data);
      }
      break;
    }
  }
#else
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
      continue;
    }

    numOfElems++;

    char* data = colDataGetData(pInputCol, i);
    TSKEY cts = getRowPTs(pInput->pPTS, i);
    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      doSaveCurrentVal(pCtx, i, cts, type, data);
      pResInfo->numOfRes = 1;
    }
  }
#endif

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static void firstLastTransferInfo(SqlFunctionCtx* pCtx, SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst) {
  SInputColumnInfoData* pColInfo = &pCtx->input;
  int32_t               start = pColInfo->startRowIndex;

  pOutput->bytes = pInput->bytes;
  TSKEY* tsIn = &pInput->ts;
  TSKEY* tsOut = &pOutput->ts;

  if (pOutput->hasResult) {
    if (isFirst) {
      if (*tsIn > *tsOut) {
        return;
      }
    } else {
      if (*tsIn < *tsOut) {
        return;
      }
    }
  }

  *tsOut = *tsIn;
  memcpy(pOutput->buf, pInput->buf, pOutput->bytes);
  saveTupleData(pCtx->pSrcBlock, start, pCtx, pOutput);

  pOutput->hasResult = true;
}

static int32_t firstLastFunctionMergeImpl(SqlFunctionCtx* pCtx, bool isFirstQuery) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SFirstLastRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  int32_t numOfElems = 0;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*          data = colDataGetData(pCol, i);
    SFirstLastRes* pInputInfo = (SFirstLastRes*)varDataVal(data);
    firstLastTransferInfo(pCtx, pInputInfo, pInfo, isFirstQuery);
    if (!numOfElems) {
      numOfElems = pInputInfo->hasResult ? 1 : 0;
    }
  }

  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t firstFunctionMerge(SqlFunctionCtx* pCtx) { return firstLastFunctionMergeImpl(pCtx, true); }

int32_t lastFunctionMerge(SqlFunctionCtx* pCtx) { return firstLastFunctionMergeImpl(pCtx, false); }

int32_t firstLastFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SFirstLastRes* pRes = GET_ROWCELL_INTERBUF(pResInfo);
  colDataAppend(pCol, pBlock->info.rows, pRes->buf, pRes->isNull || pResInfo->isNullRes);

  // handle selectivity
  setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);

  return pResInfo->numOfRes;
}

int32_t firstLastPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t resultBytes = getFirstLastInfoSize(pRes->bytes);

  // todo check for failure
  char* res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));
  memcpy(varDataVal(res), pRes, resultBytes);

  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);
  setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);

  taosMemoryFree(res);
  return 1;
}

// todo rewrite:
int32_t lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  int32_t              bytes = pDestCtx->input.pData[0]->info.bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (pSResInfo->numOfRes != 0 && (pDResInfo->numOfRes == 0 || pDBuf->ts < pSBuf->ts)) {
    memcpy(pDBuf->buf, pSBuf->buf, bytes);
    pDBuf->ts = pSBuf->ts;
    pDResInfo->numOfRes = 1;
  }
  return TSDB_CODE_SUCCESS;
}

static void doSaveLastrow(SqlFunctionCtx* pCtx, char* pData, int32_t rowIndex, int64_t cts, SFirstLastRes* pInfo) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  if (colDataIsNull_s(pInputCol, rowIndex)) {
    pInfo->isNull = true;
  } else {
    pInfo->isNull = false;

    if (IS_VAR_DATA_TYPE(pInputCol->info.type)) {
      pInfo->bytes = varDataTLen(pData);
    }

    memcpy(pInfo->buf, pData, pInfo->bytes);
  }

  pInfo->ts = cts;
  saveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo);

  pInfo->hasResult = true;
}

int32_t lastRowFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

#if 0
  // the optimized version only function if all tuples in one block are monotonious increasing or descreasing.
  // this is NOT always works if project operator exists in downstream.
  if (blockDataOrder == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      numOfElems++;

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveLastrow(pCtx, data, i, cts, pInfo);
      }

      break;
    }
  } else {  // descending order
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      char* data = colDataGetData(pInputCol, i);
      TSKEY cts = getRowPTs(pInput->pPTS, i);
      numOfElems++;

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        doSaveLastrow(pCtx, data, i, cts, pInfo);
      }
      break;
    }
  }
#else
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    char* data = colDataGetData(pInputCol, i);
    TSKEY cts = getRowPTs(pInput->pPTS, i);
    numOfElems++;

    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      doSaveLastrow(pCtx, data, i, cts, pInfo);
      pResInfo->numOfRes = 1;
    }
  }

#endif
  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

bool getDiffFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SDiffInfo);
  return true;
}

bool diffFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  SDiffInfo* pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pDiffInfo->hasPrev = false;
  pDiffInfo->prev.i64 = 0;
  if (pCtx->numOfParams > 1) {
    pDiffInfo->ignoreNegative = pCtx->param[1].param.i;  // TODO set correct param
  } else {
    pDiffInfo->ignoreNegative = false;
  }
  pDiffInfo->includeNull = false;
  pDiffInfo->firstOutput = false;
  return true;
}

static void doSetPrevVal(SDiffInfo* pDiffInfo, int32_t type, const char* pv) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      pDiffInfo->prev.i64 = *(int8_t*)pv;
      break;
    case TSDB_DATA_TYPE_INT:
      pDiffInfo->prev.i64 = *(int32_t*)pv;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pDiffInfo->prev.i64 = *(int16_t*)pv;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pDiffInfo->prev.i64 = *(int64_t*)pv;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pDiffInfo->prev.d64 = *(float*)pv;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pDiffInfo->prev.d64 = *(double*)pv;
      break;
    default:
      ASSERT(0);
  }
}

static void doHandleDiff(SDiffInfo* pDiffInfo, int32_t type, const char* pv, SColumnInfoData* pOutput, int32_t pos,
                         int32_t order) {
  int32_t factor = (order == TSDB_ORDER_ASC) ? 1 : -1;
  switch (type) {
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)pv;
      int64_t delta = factor * (v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;

      break;
    }
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t  v = *(int8_t*)pv;
      int64_t delta = factor * (v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)pv;
      int64_t delta = factor * (v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)pv;
      int64_t delta = factor * (v - pDiffInfo->prev.i64);  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float  v = *(float*)pv;
      double delta = factor * (v - pDiffInfo->prev.d64);                               // direct previous may be null
      if ((delta < 0 && pDiffInfo->ignoreNegative) || isinf(delta) || isnan(delta)) {  // check for overflow
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendDouble(pOutput, pos, &delta);
      }
      pDiffInfo->prev.d64 = v;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)pv;
      double delta = factor * (v - pDiffInfo->prev.d64);                               // direct previous may be null
      if ((delta < 0 && pDiffInfo->ignoreNegative) || isinf(delta) || isnan(delta)) {  // check for overflow
        colDataSetNull_f(pOutput->nullbitmap, pos);
      } else {
        colDataAppendDouble(pOutput, pos, &delta);
      }
      pDiffInfo->prev.d64 = v;
      break;
    }
    default:
      ASSERT(0);
  }
}

int32_t diffFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t numOfElems = 0;
  int32_t startOffset = pCtx->offset;

  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  if (pCtx->order == TSDB_ORDER_ASC) {
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
      int32_t pos = startOffset + numOfElems;

      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        if (pDiffInfo->includeNull) {
          colDataSetNull_f(pOutput->nullbitmap, pos);

          numOfElems += 1;
        }
        continue;
      }

      char* pv = colDataGetData(pInputCol, i);

      if (pDiffInfo->hasPrev) {
        doHandleDiff(pDiffInfo, pInputCol->info.type, pv, pOutput, pos, pCtx->order);
        // handle selectivity
        if (pCtx->subsidiaries.num > 0) {
          appendSelectivityValue(pCtx, i, pos);
        }

        numOfElems++;
      } else {
        doSetPrevVal(pDiffInfo, pInputCol->info.type, pv);
      }

      pDiffInfo->hasPrev = true;
    }
  } else {
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
      int32_t pos = startOffset + numOfElems;

      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        if (pDiffInfo->includeNull) {
          colDataSetNull_f(pOutput->nullbitmap, pos);

          numOfElems += 1;
        }
        continue;
      }

      char* pv = colDataGetData(pInputCol, i);

      // there is a row of previous data block to be handled in the first place.
      if (pDiffInfo->hasPrev) {
        doHandleDiff(pDiffInfo, pInputCol->info.type, pv, pOutput, pos, pCtx->order);
        // handle selectivity
        if (pCtx->subsidiaries.num > 0) {
          appendSelectivityValue(pCtx, i, pos);
        }

        numOfElems++;
      } else {
        doSetPrevVal(pDiffInfo, pInputCol->info.type, pv);
      }

      pDiffInfo->hasPrev = true;
    }
  }

  // initial value is not set yet
  return numOfElems;
}

int32_t getTopBotInfoSize(int64_t numOfItems) { return sizeof(STopBotRes) + numOfItems * sizeof(STopBotResItem); }

bool getTopBotFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SValueNode* pkNode = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  pEnv->calcMemSize = sizeof(STopBotRes) + pkNode->datum.i * sizeof(STopBotResItem);
  return true;
}

bool topBotFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  STopBotRes*           pRes = GET_ROWCELL_INTERBUF(pResInfo);
  SInputColumnInfoData* pInput = &pCtx->input;

  pRes->maxSize = pCtx->param[1].param.i;

  pRes->nullTupleSaved = false;
  pRes->nullTuplePos.pageId = -1;
  return true;
}

static STopBotRes* getTopBotOutputInfo(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = GET_ROWCELL_INTERBUF(pResInfo);
  pRes->pItems = (STopBotResItem*)((char*)pRes + sizeof(STopBotRes));

  return pRes;
}

static void doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock, uint16_t type,
                            uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery);

static void addResult(SqlFunctionCtx* pCtx, STopBotResItem* pSourceItem, int16_t type, bool isTopQuery);

int32_t topFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  STopBotRes* pRes = getTopBotOutputInfo(pCtx);
  pRes->type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char* data = colDataGetData(pCol, i);
    doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, true);
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    doSaveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    pRes->nullTupleSaved = true;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t bottomFunction(SqlFunctionCtx* pCtx) {
  int32_t              numOfElems = 0;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  STopBotRes* pRes = getTopBotOutputInfo(pCtx);
  pRes->type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;
    char* data = colDataGetData(pCol, i);
    doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, false);
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    doSaveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    pRes->nullTupleSaved = true;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t topBotResComparFn(const void* p1, const void* p2, const void* param) {
  uint16_t type = *(uint16_t*)param;

  STopBotResItem* val1 = (STopBotResItem*)p1;
  STopBotResItem* val2 = (STopBotResItem*)p2;

  if (IS_SIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.i == val2->v.i) {
      return 0;
    }

    return (val1->v.i > val2->v.i) ? 1 : -1;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    if (val1->v.u == val2->v.u) {
      return 0;
    }

    return (val1->v.u > val2->v.u) ? 1 : -1;
  }

  if (val1->v.d == val2->v.d) {
    return 0;
  }

  return (val1->v.d > val2->v.d) ? 1 : -1;
}

void doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock, uint16_t type,
                     uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery) {
  STopBotRes* pRes = getTopBotOutputInfo(pCtx);

  SVariant val = {0};
  taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);

  STopBotResItem* pItems = pRes->pItems;
  assert(pItems != NULL);

  // not full yet
  if (pEntryInfo->numOfRes < pRes->maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = val;
    pItem->uid = uid;

    // save the data of this tuple
    if (pCtx->subsidiaries.num > 0) {
      doSaveTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
    }
#ifdef BUF_PAGE_DEBUG
    qDebug("page_saveTuple i:%d, item:%p,pageId:%d, offset:%d\n", pEntryInfo->numOfRes, pItem, pItem->tuplePos.pageId,
           pItem->tuplePos.offset);
#endif
    // allocate the buffer and keep the data of this row into the new allocated buffer
    pEntryInfo->numOfRes++;
    taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type, topBotResComparFn,
                 !isTopQuery);
  } else {  // replace the minimum value in the result
    if ((isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && val.i > pItems[0].v.i) ||
                        (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u > pItems[0].v.u) ||
                        (IS_FLOAT_TYPE(type) && val.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && val.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u < pItems[0].v.u) ||
                         (IS_FLOAT_TYPE(type) && val.d < pItems[0].v.d)))) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = val;
      pItem->uid = uid;

      // save the data of this tuple by over writing the old data
      if (pCtx->subsidiaries.num > 0) {
        doCopyTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
      }
#ifdef BUF_PAGE_DEBUG
      qDebug("page_copyTuple pageId:%d, offset:%d", pItem->tuplePos.pageId, pItem->tuplePos.offset);
#endif
      taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, !isTopQuery);
    }
  }
}

/*
 * +------------------------------------+--------------+--------------+
 * |            null bitmap             |              |              |
 * |(n columns, one bit for each column)| src column #1| src column #2|
 * +------------------------------------+--------------+--------------+
 */
void doSaveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  SFilePage* pPage = NULL;

  // todo refactor: move away
  int32_t completeRowSize = pCtx->subsidiaries.num * sizeof(bool);
  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
    completeRowSize += pc->pExpr->base.resSchema.bytes;
  }

  if (pCtx->curBufPage == -1) {
    pPage = getNewBufPage(pCtx->pBuf, 0, &pCtx->curBufPage);
    pPage->num = sizeof(SFilePage);
  } else {
    pPage = getBufPage(pCtx->pBuf, pCtx->curBufPage);
    if (pPage->num + completeRowSize > getBufPageSize(pCtx->pBuf)) {
      // current page is all used, let's prepare a new buffer page
      releaseBufPage(pCtx->pBuf, pPage);
      pPage = getNewBufPage(pCtx->pBuf, 0, &pCtx->curBufPage);
      pPage->num = sizeof(SFilePage);
    }
  }

  pPos->pageId = pCtx->curBufPage;
  pPos->offset = pPage->num;

  // keep the current row data, extract method
  int32_t offset = 0;
  bool*   nullList = (bool*)((char*)pPage + pPage->num);
  char*   pStart = (char*)(nullList + sizeof(bool) * pCtx->subsidiaries.num);
  for (int32_t i = 0; i < pCtx->subsidiaries.num; ++i) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[i];

    SFunctParam* pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t      srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
    if ((nullList[i] = colDataIsNull_s(pCol, rowIndex)) == true) {
      offset += pCol->info.bytes;
      continue;
    }

    char* p = colDataGetData(pCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart + offset, p, (pCol->info.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(p) : varDataTLen(p));
    } else {
      memcpy(pStart + offset, p, pCol->info.bytes);
    }

    offset += pCol->info.bytes;
  }

  pPage->num += completeRowSize;

  setBufPageDirty(pPage, true);
  releaseBufPage(pCtx->pBuf, pPage);
#ifdef BUF_PAGE_DEBUG
  qDebug("page_saveTuple pos:%p,pageId:%d, offset:%d\n", pPos, pPos->pageId, pPos->offset);
#endif
}

void doCopyTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  SFilePage* pPage = getBufPage(pCtx->pBuf, pPos->pageId);

  int32_t numOfCols = pCtx->subsidiaries.num;

  bool* nullList = (bool*)((char*)pPage + pPos->offset);
  char* pStart = (char*)(nullList + numOfCols * sizeof(bool));

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[i];
    SFunctParam*    pFuncParam = &pc->pExpr->base.pParam[0];
    int32_t         srcSlotId = pFuncParam->pCol->slotId;

    SColumnInfoData* pCol = taosArrayGet(pSrcBlock->pDataBlock, srcSlotId);
    if ((nullList[i] = colDataIsNull_s(pCol, rowIndex)) == true) {
      offset += pCol->info.bytes;
      continue;
    }

    char* p = colDataGetData(pCol, rowIndex);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart + offset, p, (pCol->info.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(p) : varDataTLen(p));
    } else {
      memcpy(pStart + offset, p, pCol->info.bytes);
    }

    offset += pCol->info.bytes;
  }

  setBufPageDirty(pPage, true);
  releaseBufPage(pCtx->pBuf, pPage);
#ifdef BUF_PAGE_DEBUG
  qDebug("page_copyTuple pos:%p, pageId:%d, offset:%d", pPos, pPos->pageId, pPos->offset);
#endif
}

int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);

  int16_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  if (pEntryInfo->numOfRes <= 0) {
    colDataAppendNULL(pCol, currentRow);
    setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
    return pEntryInfo->numOfRes;
  }
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STopBotResItem* pItem = &pRes->pItems[i];
    if (type == TSDB_DATA_TYPE_FLOAT) {
      float v = pItem->v.d;
      colDataAppend(pCol, currentRow, (const char*)&v, false);
    } else {
      colDataAppend(pCol, currentRow, (const char*)&pItem->v.i, false);
    }
#ifdef BUF_PAGE_DEBUG
    qDebug("page_finalize i:%d,item:%p,pageId:%d, offset:%d\n", i, pItem, pItem->tuplePos.pageId,
           pItem->tuplePos.offset);
#endif
    setSelectivityValue(pCtx, pBlock, &pRes->pItems[i].tuplePos, currentRow);
    currentRow += 1;
  }

  return pEntryInfo->numOfRes;
}

void addResult(SqlFunctionCtx* pCtx, STopBotResItem* pSourceItem, int16_t type, bool isTopQuery) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);
  STopBotResItem*      pItems = pRes->pItems;
  assert(pItems != NULL);

  // not full yet
  if (pEntryInfo->numOfRes < pRes->maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = pSourceItem->v;
    pItem->uid = pSourceItem->uid;
    pItem->tuplePos.pageId = -1;
    replaceTupleData(&pItem->tuplePos, &pSourceItem->tuplePos);
    pEntryInfo->numOfRes++;
    taosheapsort((void*)pItems, sizeof(STopBotResItem), pEntryInfo->numOfRes, (const void*)&type, topBotResComparFn,
                 !isTopQuery);
  } else {  // replace the minimum value in the result
    if ((isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && pSourceItem->v.i > pItems[0].v.i) ||
                        (IS_UNSIGNED_NUMERIC_TYPE(type) && pSourceItem->v.u > pItems[0].v.u) ||
                        (IS_FLOAT_TYPE(type) && pSourceItem->v.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && pSourceItem->v.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && pSourceItem->v.u < pItems[0].v.u) ||
                         (IS_FLOAT_TYPE(type) && pSourceItem->v.d < pItems[0].v.d)))) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = pSourceItem->v;
      pItem->uid = pSourceItem->uid;

      // save the data of this tuple by over writing the old data
      replaceTupleData(&pItem->tuplePos, &pSourceItem->tuplePos);
      taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, !isTopQuery);
    }
  }
}

int32_t topCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  for (int32_t i = 0; i < pSResInfo->numOfRes; i++) {
    addResult(pDestCtx, pSBuf->pItems + i, type, true);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t bottomCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  int32_t              type = pDestCtx->input.pData[0]->info.type;
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  for (int32_t i = 0; i < pSResInfo->numOfRes; i++) {
    addResult(pDestCtx, pSBuf->pItems + i, type, false);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t getSpreadInfoSize() { return (int32_t)sizeof(SSpreadInfo); }

bool getSpreadFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSpreadInfo);
  return true;
}

bool spreadFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  SET_DOUBLE_VAL(&pInfo->min, DBL_MAX);
  SET_DOUBLE_VAL(&pInfo->max, -DBL_MAX);
  pInfo->hasResult = false;
  return true;
}

int32_t spreadFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pInput->colDataAggIsSet) {
    numOfElems = pInput->numOfRows - pAgg->numOfNull;
    if (numOfElems == 0) {
      goto _spread_over;
    }
    double tmin = 0.0, tmax = 0.0;
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      tmin = (double)GET_INT64_VAL(&pAgg->min);
      tmax = (double)GET_INT64_VAL(&pAgg->max);
    } else if (IS_FLOAT_TYPE(type)) {
      tmin = GET_DOUBLE_VAL(&pAgg->min);
      tmax = GET_DOUBLE_VAL(&pAgg->max);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      tmin = (double)GET_UINT64_VAL(&pAgg->min);
      tmax = (double)GET_UINT64_VAL(&pAgg->max);
    }

    if (GET_DOUBLE_VAL(&pInfo->min) > tmin) {
      SET_DOUBLE_VAL(&pInfo->min, tmin);
    }

    if (GET_DOUBLE_VAL(&pInfo->max) < tmax) {
      SET_DOUBLE_VAL(&pInfo->max, tmax);
    }

  } else {  // computing based on the true data block
    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    int32_t numOfRows = pInput->numOfRows;

    // check the valid data one by one
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }

      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      if (v < GET_DOUBLE_VAL(&pInfo->min)) {
        SET_DOUBLE_VAL(&pInfo->min, v);
      }

      if (v > GET_DOUBLE_VAL(&pInfo->max)) {
        SET_DOUBLE_VAL(&pInfo->max, v);
      }

      numOfElems += 1;
    }
  }

_spread_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  if (numOfElems > 0) {
    pInfo->hasResult = true;
  }

  return TSDB_CODE_SUCCESS;
}

static void spreadTransferInfo(SSpreadInfo* pInput, SSpreadInfo* pOutput) {
  pOutput->hasResult = pInput->hasResult;
  if (pInput->max > pOutput->max) {
    pOutput->max = pInput->max;
  }

  if (pInput->min < pOutput->min) {
    pOutput->min = pInput->min;
  }
}

int32_t spreadFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*        data = colDataGetData(pCol, i);
    SSpreadInfo* pInputInfo = (SSpreadInfo*)varDataVal(data);
    if (pInputInfo->hasResult) {
      spreadTransferInfo(pInputInfo, pInfo);
    }
  }

  if (pInfo->hasResult) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t spreadFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SSpreadInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  if (pInfo->hasResult == true) {
    SET_DOUBLE_VAL(&pInfo->result, pInfo->max - pInfo->min);
  } else {
    GET_RES_INFO(pCtx)->isNullRes = 1;
  }
  return functionFinalize(pCtx, pBlock);
}

int32_t spreadPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSpreadInfo*         pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getSpreadInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t spreadCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SSpreadInfo*         pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSpreadInfo*         pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  spreadTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

int32_t getElapsedInfoSize() { return (int32_t)sizeof(SElapsedInfo); }

bool getElapsedFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SElapsedInfo);
  return true;
}

bool elapsedFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->result = 0;
  pInfo->min = TSKEY_MAX;
  pInfo->max = 0;

  if (pCtx->numOfParams > 1) {
    pInfo->timeUnit = pCtx->param[1].param.i;
  } else {
    pInfo->timeUnit = 1;
  }

  return true;
}

int32_t elapsedFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  numOfElems = pInput->numOfRows;  // since this is the primary timestamp, no need to exclude NULL values
  if (numOfElems == 0) {
    goto _elapsed_over;
  }

  if (pInput->colDataAggIsSet) {
    if (pInfo->min == TSKEY_MAX) {
      pInfo->min = GET_INT64_VAL(&pAgg->min);
      pInfo->max = GET_INT64_VAL(&pAgg->max);
    } else {
      if (pCtx->order == TSDB_ORDER_ASC) {
        pInfo->max = GET_INT64_VAL(&pAgg->max);
      } else {
        pInfo->min = GET_INT64_VAL(&pAgg->min);
      }
    }
  } else {  // computing based on the true data block
    if (0 == pInput->numOfRows) {
      if (pCtx->order == TSDB_ORDER_DESC) {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->min = pCtx->end.key;
        }
      } else {
        if (pCtx->end.key != INT64_MIN) {
          pInfo->max = pCtx->end.key + 1;
        }
      }
      goto _elapsed_over;
    }

    SColumnInfoData* pCol = pInput->pData[0];

    int32_t start = pInput->startRowIndex;
    TSKEY*  ptsList = (int64_t*)colDataGetData(pCol, 0);
    if (pCtx->order == TSDB_ORDER_DESC) {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->max =
            (pInfo->max < ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->max;
      } else {
        pInfo->max = pCtx->start.key + 1;
      }

      if (pCtx->end.key != INT64_MIN) {
        pInfo->min = pCtx->end.key;
      } else {
        pInfo->min = ptsList[start];
      }
    } else {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->min = (pInfo->min > ptsList[start]) ? ptsList[start] : pInfo->min;
      } else {
        pInfo->min = pCtx->start.key;
      }

      if (pCtx->end.key != INT64_MIN) {
        pInfo->max = pCtx->end.key + 1;
      } else {
        pInfo->max = ptsList[start + pInput->numOfRows - 1];
      }
    }
  }

_elapsed_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

static void elapsedTransferInfo(SElapsedInfo* pInput, SElapsedInfo* pOutput) {
  pOutput->timeUnit = pInput->timeUnit;
  if (pOutput->min > pInput->min) {
    pOutput->min = pInput->min;
  }

  if (pOutput->max < pInput->max) {
    pOutput->max = pInput->max;
  }
}

int32_t elapsedFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*         data = colDataGetData(pCol, i);
    SElapsedInfo* pInputInfo = (SElapsedInfo*)varDataVal(data);
    elapsedTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t elapsedFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SElapsedInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  double        result = (double)pInfo->max - (double)pInfo->min;
  result = (result >= 0) ? result : -result;
  pInfo->result = result / pInfo->timeUnit;
  return functionFinalize(pCtx, pBlock);
}

int32_t elapsedPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SElapsedInfo*        pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getElapsedInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t elapsedCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SElapsedInfo*        pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SElapsedInfo*        pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  elapsedTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

int32_t getHistogramInfoSize() {
  return (int32_t)sizeof(SHistoFuncInfo) + HISTOGRAM_MAX_BINS_NUM * sizeof(SHistoFuncBin);
}

bool getHistogramFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHistoFuncInfo) + HISTOGRAM_MAX_BINS_NUM * sizeof(SHistoFuncBin);
  return true;
}

static int8_t getHistogramBinType(char* binTypeStr) {
  int8_t binType;
  if (strcasecmp(binTypeStr, "user_input") == 0) {
    binType = USER_INPUT_BIN;
  } else if (strcasecmp(binTypeStr, "linear_bin") == 0) {
    binType = LINEAR_BIN;
  } else if (strcasecmp(binTypeStr, "log_bin") == 0) {
    binType = LOG_BIN;
  } else {
    binType = UNKNOWN_BIN;
  }

  return binType;
}

static bool getHistogramBinDesc(SHistoFuncInfo* pInfo, char* binDescStr, int8_t binType, bool normalized) {
  cJSON*  binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double* intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      return false;
    }

    cJSON* start = cJSON_GetObjectItem(binDesc, "start");
    cJSON* factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON* width = cJSON_GetObjectItem(binDesc, "width");
    cJSON* count = cJSON_GetObjectItem(binDesc, "count");
    cJSON* infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      return false;
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      return false;
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      return false;
    }

    int32_t counter = (int32_t)count->valueint;
    if (infinity->valueint == false) {
      startIndex = 0;
      numOfBins = counter + 1;
    } else {
      startIndex = 1;
      numOfBins = counter + 3;
    }

    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (cJSON_IsNumber(width) && factor == NULL && binType == LINEAR_BIN) {
      // linear bin process
      if (width->valuedouble == 0) {
        taosMemoryFree(intervals);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          return false;
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        taosMemoryFree(intervals);
        return false;
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        taosMemoryFree(intervals);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          return false;
        }
        startIndex++;
      }
    } else {
      taosMemoryFree(intervals);
      return false;
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      ASSERT(numOfBins >= 4);
      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      return false;
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    cJSON* bin = binDesc->child;
    if (bin == NULL) {
      taosMemoryFree(intervals);
      return false;
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        taosMemoryFree(intervals);
        return false;
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        taosMemoryFree(intervals);
        return false;
      }
      bin = bin->next;
      i++;
    }
  } else {
    return false;
  }

  pInfo->numOfBins = numOfBins - 1;
  pInfo->normalized = normalized;
  for (int32_t i = 0; i < pInfo->numOfBins; ++i) {
    pInfo->bins[i].lower = intervals[i] < intervals[i + 1] ? intervals[i] : intervals[i + 1];
    pInfo->bins[i].upper = intervals[i + 1] > intervals[i] ? intervals[i + 1] : intervals[i];
    pInfo->bins[i].count = 0;
  }

  taosMemoryFree(intervals);
  return true;
}

bool histogramFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->numOfBins = 0;
  pInfo->totalCount = 0;
  pInfo->normalized = 0;

  int8_t binType = getHistogramBinType(varDataVal(pCtx->param[1].param.pz));
  if (binType == UNKNOWN_BIN) {
    return false;
  }
  char*   binDesc = varDataVal(pCtx->param[2].param.pz);
  int64_t normalized = pCtx->param[3].param.i;
  if (normalized != 0 && normalized != 1) {
    return false;
  }
  if (!getHistogramBinDesc(pInfo, binDesc, binType, (bool)normalized)) {
    return false;
  }

  return true;
}

static int32_t histogramFunctionImpl(SqlFunctionCtx* pCtx, bool isPartial) {
  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pInput->pData[0]->info.type;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  int32_t numOfElems = 0;
  for (int32_t i = start; i < numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;

    char*  data = colDataGetData(pCol, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    for (int32_t k = 0; k < pInfo->numOfBins; ++k) {
      if (v > pInfo->bins[k].lower && v <= pInfo->bins[k].upper) {
        pInfo->bins[k].count++;
        pInfo->totalCount++;
        break;
      }
    }
  }

  if (!isPartial) {
    SET_VAL(GET_RES_INFO(pCtx), numOfElems, pInfo->numOfBins);
  } else {
    SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t histogramFunction(SqlFunctionCtx* pCtx) { return histogramFunctionImpl(pCtx, false); }

int32_t histogramFunctionPartial(SqlFunctionCtx* pCtx) { return histogramFunctionImpl(pCtx, true); }

static void histogramTransferInfo(SHistoFuncInfo* pInput, SHistoFuncInfo* pOutput) {
  pOutput->normalized = pInput->normalized;
  pOutput->numOfBins = pInput->numOfBins;
  pOutput->totalCount += pInput->totalCount;
  for (int32_t k = 0; k < pOutput->numOfBins; ++k) {
    pOutput->bins[k].lower = pInput->bins[k].lower;
    pOutput->bins[k].upper = pInput->bins[k].upper;
    pOutput->bins[k].count += pInput->bins[k].count;
  }
}

int32_t histogramFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SHistoFuncInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*           data = colDataGetData(pCol, i);
    SHistoFuncInfo* pInputInfo = (SHistoFuncInfo*)varDataVal(data);
    histogramTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), pInfo->numOfBins, pInfo->numOfBins);
  return TSDB_CODE_SUCCESS;
}

int32_t histogramFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHistoFuncInfo*      pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;

  if (pInfo->normalized) {
    for (int32_t k = 0; k < pResInfo->numOfRes; ++k) {
      if (pInfo->totalCount != 0) {
        pInfo->bins[k].percentage = pInfo->bins[k].count / (double)pInfo->totalCount;
      } else {
        pInfo->bins[k].percentage = 0;
      }
    }
  }

  for (int32_t i = 0; i < pResInfo->numOfRes; ++i) {
    int32_t len;
    char    buf[512] = {0};
    if (!pInfo->normalized) {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%" PRId64 "}",
                    pInfo->bins[i].lower, pInfo->bins[i].upper, pInfo->bins[i].count);
    } else {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}", pInfo->bins[i].lower,
                    pInfo->bins[i].upper, pInfo->bins[i].percentage);
    }
    varDataSetLen(buf, len);
    colDataAppend(pCol, currentRow, buf, false);
    currentRow++;
  }

  return pResInfo->numOfRes;
}

int32_t histogramPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHistoFuncInfo*      pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getHistogramInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t histogramCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SHistoFuncInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SHistoFuncInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  histogramTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

int32_t getHLLInfoSize() { return (int32_t)sizeof(SHLLInfo); }

bool getHLLFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SHLLInfo);
  return true;
}

static uint8_t hllCountNum(void* data, int32_t bytes, int32_t* buk) {
  uint64_t hash = MurmurHash3_64(data, bytes);
  int32_t  index = hash & HLL_BUCKET_MASK;
  hash >>= HLL_BUCKET_BITS;
  hash |= ((uint64_t)1 << HLL_DATA_BITS);
  uint64_t bit = 1;
  uint8_t  count = 1;
  while ((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *buk = index;
  return count;
}

static void hllBucketHisto(uint8_t* buckets, int32_t* bucketHisto) {
  uint64_t* word = (uint64_t*)buckets;
  uint8_t*  bytes;

  for (int32_t j = 0; j < HLL_BUCKETS >> 3; j++) {
    if (*word == 0) {
      bucketHisto[0] += 8;
    } else {
      bytes = (uint8_t*)word;
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
    z -= pow(1 - x, 2) * y;
  } while (zPrime != z);
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
  } while (zPrime != z);
  return z;
}

// estimate the cardinality, the algorithm refer this paper: "New cardinality estimation algorithms for HyperLogLog
// sketches"
static uint64_t hllCountCnt(uint8_t* buckets) {
  double  m = HLL_BUCKETS;
  int32_t buckethisto[64] = {0};
  hllBucketHisto(buckets, buckethisto);

  double z = m * hllTau((m - buckethisto[HLL_DATA_BITS + 1]) / (double)m);
  for (int j = HLL_DATA_BITS; j >= 1; --j) {
    z += buckethisto[j];
    z *= 0.5;
  }

  z += m * hllSigma(buckethisto[0] / (double)m);
  double E = (double)llroundl(HLL_ALPHA_INF * m * m / z);

  return (uint64_t)E;
}

int32_t hllFunction(SqlFunctionCtx* pCtx) {
  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  int32_t type = pCol->info.type;
  int32_t bytes = pCol->info.bytes;

  int32_t start = pInput->startRowIndex;
  int32_t numOfRows = pInput->numOfRows;

  int32_t numOfElems = 0;
  for (int32_t i = start; i < numOfRows + start; ++i) {
    if (pCol->hasNull && colDataIsNull_s(pCol, i)) {
      continue;
    }

    numOfElems++;

    char* data = colDataGetData(pCol, i);
    if (IS_VAR_DATA_TYPE(type)) {
      bytes = varDataLen(data);
      data = varDataVal(data);
    }

    int32_t index = 0;
    uint8_t count = hllCountNum(data, bytes, &index);
    uint8_t oldcount = pInfo->buckets[index];
    if (count > oldcount) {
      pInfo->buckets[index] = count;
    }
  }

  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static void hllTransferInfo(SHLLInfo* pInput, SHLLInfo* pOutput) {
  for (int32_t k = 0; k < HLL_BUCKETS; ++k) {
    if (pOutput->buckets[k] < pInput->buckets[k]) {
      pOutput->buckets[k] = pInput->buckets[k];
    }
  }
}

int32_t hllFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_BINARY);

  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*     data = colDataGetData(pCol, i);
    SHLLInfo* pInputInfo = (SHLLInfo*)varDataVal(data);
    hllTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t hllFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pInfo = GET_RES_INFO(pCtx);

  SHLLInfo* pHllInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pHllInfo->result = hllCountCnt(pHllInfo->buckets);
  if (tsCountAlwaysReturnValue && pHllInfo->result == 0) {
    pInfo->numOfRes = 1;
  }

  return functionFinalize(pCtx, pBlock);
}

int32_t hllPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SHLLInfo*            pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getHLLInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataAppend(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t hllCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SHLLInfo*            pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SHLLInfo*            pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  hllTransferInfo(pSBuf, pDBuf);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  return TSDB_CODE_SUCCESS;
}

bool getStateFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SStateInfo);
  return true;
}

static int8_t getStateOpType(char* opStr) {
  int8_t opType;
  if (strncasecmp(opStr, "LT", 2) == 0) {
    opType = STATE_OPER_LT;
  } else if (strncasecmp(opStr, "GT", 2) == 0) {
    opType = STATE_OPER_GT;
  } else if (strncasecmp(opStr, "LE", 2) == 0) {
    opType = STATE_OPER_LE;
  } else if (strncasecmp(opStr, "GE", 2) == 0) {
    opType = STATE_OPER_GE;
  } else if (strncasecmp(opStr, "NE", 2) == 0) {
    opType = STATE_OPER_NE;
  } else if (strncasecmp(opStr, "EQ", 2) == 0) {
    opType = STATE_OPER_EQ;
  } else {
    opType = STATE_OPER_INVALID;
  }

  return opType;
}

static bool checkStateOp(int8_t op, SColumnInfoData* pCol, int32_t index, SVariant param) {
  char* data = colDataGetData(pCol, index);
  switch (pCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t v = *(uint8_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t v = *(uint16_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t v = *(uint32_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t v = *(uint64_t*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float*)data;
      STATE_COMP(op, v, param);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)data;
      STATE_COMP(op, v, param);
      break;
    }
    default: {
      ASSERT(0);
    }
  }
  return false;
}

int32_t stateCountFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return 0;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    numOfElems++;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataAppendNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, i);
      }
      continue;
    }

    bool ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);

    int64_t output = -1;
    if (ret) {
      output = ++pInfo->count;
    } else {
      pInfo->count = 0;
    }
    colDataAppend(pOutput, i, (char*)&output, false);

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, i);
    }
  }

  return numOfElems;
}

int32_t stateDurationFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  // TODO: process timeUnit for different db precisions
  int32_t timeUnit = 1;
  if (pCtx->numOfParams == 5) {  // TODO: param number incorrect
    timeUnit = pCtx->param[3].param.i;
  }

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return 0;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    numOfElems++;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataAppendNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, i);
      }
      continue;
    }

    bool    ret = checkStateOp(op, pInputCol, i, pCtx->param[2].param);
    int64_t output = -1;
    if (ret) {
      if (pInfo->durationStart == 0) {
        output = 0;
        pInfo->durationStart = tsList[i];
      } else {
        output = (tsList[i] - pInfo->durationStart) / timeUnit;
      }
    } else {
      pInfo->durationStart = 0;
    }
    colDataAppend(pOutput, i, (char*)&output, false);

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, i);
    }
  }

  return numOfElems;
}

bool getCsumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

int32_t csumFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSumRes*             pSumRes = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataAppendNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t v;
      GET_TYPED_DATA(v, int64_t, type, data);
      pSumRes->isum += v;
      colDataAppend(pOutput, pos, (char*)&pSumRes->isum, false);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t v;
      GET_TYPED_DATA(v, uint64_t, type, data);
      pSumRes->usum += v;
      colDataAppend(pOutput, pos, (char*)&pSumRes->usum, false);
    } else if (IS_FLOAT_TYPE(type)) {
      double v;
      GET_TYPED_DATA(v, double, type, data);
      pSumRes->dsum += v;
      // check for overflow
      if (isinf(pSumRes->dsum) || isnan(pSumRes->dsum)) {
        colDataAppendNULL(pOutput, pos);
      } else {
        colDataAppend(pOutput, pos, (char*)&pSumRes->dsum, false);
      }
    }

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, pos);
    }

    numOfElems++;
  }

  return numOfElems;
}

bool getMavgFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SMavgInfo) + MAVG_MAX_POINTS_NUM * sizeof(double);
  return true;
}

bool mavgFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  SMavgInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->pos = 0;
  pInfo->sum = 0;
  pInfo->numOfPoints = pCtx->param[1].param.i;
  if (pInfo->numOfPoints < 1 || pInfo->numOfPoints > MAVG_MAX_POINTS_NUM) {
    return false;
  }
  pInfo->pointsMeet = false;

  return true;
}

int32_t mavgFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SMavgInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataAppendNULL(pOutput, i);
      continue;
    }

    char*  data = colDataGetData(pInputCol, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    if (!pInfo->pointsMeet && (pInfo->pos < pInfo->numOfPoints - 1)) {
      pInfo->points[pInfo->pos] = v;
      pInfo->sum += v;
    } else {
      if (!pInfo->pointsMeet && (pInfo->pos == pInfo->numOfPoints - 1)) {
        pInfo->sum += v;
        pInfo->pointsMeet = true;
      } else {
        pInfo->sum = pInfo->sum + v - pInfo->points[pInfo->pos];
      }

      pInfo->points[pInfo->pos] = v;
      double result = pInfo->sum / pInfo->numOfPoints;
      // check for overflow
      if (isinf(result) || isnan(result)) {
        colDataAppendNULL(pOutput, pos);
      } else {
        colDataAppend(pOutput, pos, (char*)&result, false);
      }

      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, pos);
      }

      numOfElems++;
    }

    pInfo->pos++;
    if (pInfo->pos == pInfo->numOfPoints) {
      pInfo->pos = 0;
    }
  }

  return numOfElems;
}

bool getSampleFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t      numOfSamples = pVal->datum.i;
  pEnv->calcMemSize = sizeof(SSampleInfo) + numOfSamples * (pCol->node.resType.bytes + sizeof(STuplePos));
  return true;
}

bool sampleFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  taosSeedRand(taosSafeRand());

  SSampleInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->samples = pCtx->param[1].param.i;
  pInfo->totalPoints = 0;
  pInfo->numSampled = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  pInfo->nullTuplePos.pageId = -1;
  pInfo->nullTupleSaved = false;
  pInfo->data = (char*)pInfo + sizeof(SSampleInfo);
  pInfo->tuplePos = (STuplePos*)((char*)pInfo + sizeof(SSampleInfo) + pInfo->samples * pInfo->colBytes);

  return true;
}

static void sampleAssignResult(SSampleInfo* pInfo, char* data, int32_t index) {
  assignVal(pInfo->data + index * pInfo->colBytes, data, pInfo->colBytes, pInfo->colType);
}

static void doReservoirSample(SqlFunctionCtx* pCtx, SSampleInfo* pInfo, char* data, int32_t index) {
  pInfo->totalPoints++;
  if (pInfo->numSampled < pInfo->samples) {
    sampleAssignResult(pInfo, data, pInfo->numSampled);
    if (pCtx->subsidiaries.num > 0) {
      doSaveTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[pInfo->numSampled]);
    }
    pInfo->numSampled++;
  } else {
    int32_t j = taosRand() % (pInfo->totalPoints);
    if (j < pInfo->samples) {
      sampleAssignResult(pInfo, data, j);
      if (pCtx->subsidiaries.num > 0) {
        doCopyTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[j]);
      }
    }
  }
}

int32_t sampleFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    doReservoirSample(pCtx, pInfo, data, i);
  }

  if (pInfo->numSampled == 0 && pCtx->subsidiaries.num > 0 && !pInfo->nullTupleSaved) {
    doSaveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pInfo->nullTuplePos);
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(pResInfo, pInfo->numSampled, pInfo->numSampled);
  return TSDB_CODE_SUCCESS;
}

int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  SSampleInfo* pInfo = GET_ROWCELL_INTERBUF(pEntryInfo);
  pEntryInfo->complete = true;

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;
  if (pInfo->numSampled == 0) {
    colDataAppendNULL(pCol, currentRow);
    setSelectivityValue(pCtx, pBlock, &pInfo->nullTuplePos, currentRow);
    return pInfo->numSampled;
  }
  for (int32_t i = 0; i < pInfo->numSampled; ++i) {
    colDataAppend(pCol, currentRow + i, pInfo->data + i * pInfo->colBytes, false);
    setSelectivityValue(pCtx, pBlock, &pInfo->tuplePos[i], currentRow + i);
  }

  return pInfo->numSampled;
}

bool getTailFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t      numOfPoints = pVal->datum.i;
  pEnv->calcMemSize = sizeof(STailInfo) + numOfPoints * (POINTER_BYTES + sizeof(STailItem) + pCol->node.resType.bytes);
  return true;
}

bool tailFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  STailInfo* pInfo = GET_ROWCELL_INTERBUF(pResultInfo);
  pInfo->numAdded = 0;
  pInfo->numOfPoints = pCtx->param[1].param.i;
  if (pCtx->numOfParams == 4) {
    pInfo->offset = pCtx->param[2].param.i;
  } else {
    pInfo->offset = 0;
  }
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if ((pInfo->numOfPoints < 1 || pInfo->numOfPoints > TAIL_MAX_POINTS_NUM) ||
      (pInfo->numOfPoints < 0 || pInfo->numOfPoints > TAIL_MAX_OFFSET)) {
    return false;
  }

  pInfo->pItems = (STailItem**)((char*)pInfo + sizeof(STailInfo));
  char* pItem = (char*)pInfo->pItems + pInfo->numOfPoints * POINTER_BYTES;

  size_t unitSize = sizeof(STailItem) + pInfo->colBytes;
  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    pInfo->pItems[i] = (STailItem*)(pItem + i * unitSize);
    pInfo->pItems[i]->isNull = false;
  }

  return true;
}

static void tailAssignResult(STailItem* pItem, char* data, int32_t colBytes, TSKEY ts, bool isNull) {
  pItem->timestamp = ts;
  if (isNull) {
    pItem->isNull = true;
  } else {
    pItem->isNull = false;
    memcpy(pItem->data, data, colBytes);
  }
}

static int32_t tailCompFn(const void* p1, const void* p2, const void* param) {
  STailItem* d1 = *(STailItem**)p1;
  STailItem* d2 = *(STailItem**)p2;
  return compareInt64Val(&d1->timestamp, &d2->timestamp);
}

static void doTailAdd(STailInfo* pInfo, char* data, TSKEY ts, bool isNull) {
  STailItem** pList = pInfo->pItems;
  if (pInfo->numAdded < pInfo->numOfPoints) {
    tailAssignResult(pList[pInfo->numAdded], data, pInfo->colBytes, ts, isNull);
    taosheapsort((void*)pList, sizeof(STailItem**), pInfo->numAdded + 1, NULL, tailCompFn, 0);
    pInfo->numAdded++;
  } else if (pList[0]->timestamp < ts) {
    tailAssignResult(pList[0], data, pInfo->colBytes, ts, isNull);
    taosheapadjust((void*)pList, sizeof(STailItem**), 0, pInfo->numOfPoints - 1, NULL, tailCompFn, NULL, 0);
  }
}

int32_t tailFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STailInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t startOffset = pCtx->offset;
  if (pInfo->offset >= pInput->numOfRows) {
    return 0;
  } else {
    pInfo->numOfPoints = TMIN(pInfo->numOfPoints, pInput->numOfRows - pInfo->offset);
  }
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex - pInfo->offset; i += 1) {
    char* data = colDataGetData(pInputCol, i);
    doTailAdd(pInfo, data, tsList[i], colDataIsNull_s(pInputCol, i));
  }

  taosqsort(pInfo->pItems, pInfo->numOfPoints, POINTER_BYTES, NULL, tailCompFn);

  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    int32_t    pos = startOffset + i;
    STailItem* pItem = pInfo->pItems[i];
    if (pItem->isNull) {
      colDataAppendNULL(pOutput, pos);
    } else {
      colDataAppend(pOutput, pos, pItem->data, false);
    }
  }

  return pInfo->numOfPoints;
}

int32_t tailFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STailInfo*           pInfo = GET_ROWCELL_INTERBUF(pEntryInfo);
  pEntryInfo->complete = true;

  int32_t type = pCtx->input.pData[0]->info.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STailItem* pItem = pInfo->pItems[i];
    colDataAppend(pCol, currentRow, pItem->data, false);
    currentRow += 1;
  }

  return pEntryInfo->numOfRes;
}

bool getUniqueFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SUniqueInfo) + UNIQUE_MAX_RESULT_SIZE;
  return true;
}

bool uniqueFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  SUniqueInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->numOfPoints = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->pHash != NULL) {
    taosHashClear(pInfo->pHash);
  } else {
    pInfo->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  return true;
}

static void doUniqueAdd(SUniqueInfo* pInfo, char* data, TSKEY ts, bool isNull) {
  // handle null elements
  if (isNull == true) {
    int32_t      size = sizeof(SUniqueItem) + pInfo->colBytes;
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + pInfo->numOfPoints * size);
    if (pInfo->hasNull == false && pItem->isNull == false) {
      pItem->timestamp = ts;
      pItem->isNull = true;
      pInfo->numOfPoints++;
      pInfo->hasNull = true;
    } else if (pItem->timestamp > ts && pItem->isNull == true) {
      pItem->timestamp = ts;
    }
    return;
  }

  int32_t      hashKeyBytes = IS_VAR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;
  SUniqueItem* pHashItem = taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t      size = sizeof(SUniqueItem) + pInfo->colBytes;
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + pInfo->numOfPoints * size);
    pItem->timestamp = ts;
    memcpy(pItem->data, data, pInfo->colBytes);

    taosHashPut(pInfo->pHash, data, hashKeyBytes, (char*)pItem, sizeof(SUniqueItem*));
    pInfo->numOfPoints++;
  } else if (pHashItem->timestamp > ts) {
    pHashItem->timestamp = ts;
  }
}

int32_t uniqueFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SUniqueInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    char* data = colDataGetData(pInputCol, i);
    doUniqueAdd(pInfo, data, tsList[i], colDataIsNull_s(pInputCol, i));

    if (sizeof(SUniqueInfo) + pInfo->numOfPoints * (sizeof(SUniqueItem) + pInfo->colBytes) >= UNIQUE_MAX_RESULT_SIZE) {
      taosHashCleanup(pInfo->pHash);
      return 0;
    }
  }

  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    SUniqueItem* pItem = (SUniqueItem*)(pInfo->pItems + i * (sizeof(SUniqueItem) + pInfo->colBytes));
    if (pItem->isNull == true) {
      colDataAppendNULL(pOutput, i);
    } else {
      colDataAppend(pOutput, i, pItem->data, false);
    }
    if (pTsOutput != NULL) {
      colDataAppendInt64(pTsOutput, i, &pItem->timestamp);
    }
  }

  return pInfo->numOfPoints;
}

bool getModeFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SModeInfo) + MODE_MAX_RESULT_SIZE;
  return true;
}

bool modeFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  SModeInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->numOfPoints = 0;
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->pHash != NULL) {
    taosHashClear(pInfo->pHash);
  } else {
    pInfo->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  return true;
}

static void doModeAdd(SModeInfo* pInfo, char* data) {
  int32_t     hashKeyBytes = IS_VAR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;
  SModeItem** pHashItem = taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t    size = sizeof(SModeItem) + pInfo->colBytes;
    SModeItem* pItem = (SModeItem*)(pInfo->pItems + pInfo->numOfPoints * size);
    memcpy(pItem->data, data, pInfo->colBytes);
    pItem->count += 1;

    taosHashPut(pInfo->pHash, data, hashKeyBytes, &pItem, sizeof(SModeItem*));
    pInfo->numOfPoints++;
  } else {
    (*pHashItem)->count += 1;
  }
}

int32_t modeFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SModeInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    char* data = colDataGetData(pInputCol, i);
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }

    numOfElems++;
    doModeAdd(pInfo, data);

    if (sizeof(SModeInfo) + pInfo->numOfPoints * (sizeof(SModeItem) + pInfo->colBytes) >= MODE_MAX_RESULT_SIZE) {
      taosHashCleanup(pInfo->pHash);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t modeFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SModeInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  int32_t              currentRow = pBlock->info.rows;

  int32_t resIndex;
  int32_t maxCount = 0;
  for (int32_t i = 0; i < pInfo->numOfPoints; ++i) {
    SModeItem* pItem = (SModeItem*)(pInfo->pItems + i * (sizeof(SModeItem) + pInfo->colBytes));
    if (pItem->count > maxCount) {
      maxCount = pItem->count;
      resIndex = i;
    } else if (pItem->count == maxCount) {
      resIndex = -1;
    }
  }

  SModeItem* pResItem = (SModeItem*)(pInfo->pItems + resIndex * (sizeof(SModeItem) + pInfo->colBytes));
  colDataAppend(pCol, currentRow, pResItem->data, (resIndex == -1) ? true : false);

  return pResInfo->numOfRes;
}

bool getTwaFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(STwaInfo);
  return true;
}

bool twaFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  STwaInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pInfo->isNull = false;
  pInfo->p.key = INT64_MIN;
  pInfo->win = TSWINDOW_INITIALIZER;
  return true;
}

static double twa_get_area(SPoint1 s, SPoint1 e) {
  if ((s.val >= 0 && e.val >= 0) || (s.val <= 0 && e.val <= 0)) {
    return (s.val + e.val) * (e.key - s.key) / 2;
  }

  double x = (s.key * e.val - e.key * s.val) / (e.val - s.val);
  double val = (s.val * (x - s.key) + e.val * (e.key - x)) / 2;
  return val;
}

int32_t twaFunction(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  STwaInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  SPoint1*  last = &pInfo->p;
  int32_t   numOfElems = 0;

  if (IS_NULL_TYPE(pInputCol->info.type)) {
    pInfo->isNull = true;
    goto _twa_over;
  }

  int32_t i = pInput->startRowIndex;
  if (pCtx->start.key != INT64_MIN) {
    ASSERT((pCtx->start.key < tsList[i] && pCtx->order == TSDB_ORDER_ASC) ||
           (pCtx->start.key > tsList[i] && pCtx->order == TSDB_ORDER_DESC));

    ASSERT(last->key == INT64_MIN);
    for (; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        continue;
      }

      last->key = tsList[i];

      GET_TYPED_DATA(last->val, double, pInputCol->info.type, colDataGetData(pInputCol, i));

      pInfo->dOutput += twa_get_area(pCtx->start, *last);
      pInfo->win.skey = pCtx->start.key;
      numOfElems++;
      i += 1;
      break;
    }
  } else if (pInfo->p.key == INT64_MIN) {
    for (; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        continue;
      }

      last->key = tsList[i];

      GET_TYPED_DATA(last->val, double, pInputCol->info.type, colDataGetData(pInputCol, i));

      pInfo->win.skey = last->key;
      numOfElems++;
      i += 1;
      break;
    }
  }

  SPoint1 st = {0};

  // calculate the value of
  switch (pInputCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* val = (int8_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* val = (int16_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t* val = (int32_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* val = (int64_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float* val = (float*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double* val = (double*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* val = (uint8_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* val = (uint16_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t* val = (uint32_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* val = (uint64_t*)colDataGetData(pInputCol, 0);
      for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
        if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
          continue;
        }
        numOfElems++;

        INIT_INTP_POINT(st, tsList[i], val[i]);
        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }

    default:
      ASSERT(0);
  }

  // the last interpolated time window value
  if (pCtx->end.key != INT64_MIN) {
    pInfo->dOutput += twa_get_area(pInfo->p, pCtx->end);
    pInfo->p = pCtx->end;
  }

  pInfo->win.ekey = pInfo->p.key;

_twa_over:
  if (numOfElems == 0) {
    pInfo->isNull = true;
  }

  SET_VAL(pResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

/*
 * To copy the input to interResBuf to avoid the input buffer space be over writen
 * by next input data. The TWA function only applies to each table, so no merge procedure
 * is required, we simply copy to the resut ot interResBuffer.
 */
// void twa_function_copy(SQLFunctionCtx *pCtx) {
//   assert(pCtx->inputType == TSDB_DATA_TYPE_BINARY);
//   SResultRowEntryInfo *pResInfo = GET_RES_INFO(pCtx);
//
//   memcpy(GET_ROWCELL_INTERBUF(pResInfo), pCtx->pInput, (size_t)pCtx->inputBytes);
//   pResInfo->hasResult = ((STwaInfo *)pCtx->pInput)->hasResult;
// }

int32_t twaFinalize(struct SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  STwaInfo* pInfo = (STwaInfo*)GET_ROWCELL_INTERBUF(pResInfo);
  if (pInfo->isNull == true) {
    pResInfo->numOfRes = 0;
  } else {
    if (pInfo->win.ekey == pInfo->win.skey) {
      pInfo->dOutput = pInfo->p.val;
    } else {
      pInfo->dOutput = pInfo->dOutput / (pInfo->win.ekey - pInfo->win.skey);
    }

    pResInfo->numOfRes = 1;
  }

  return functionFinalize(pCtx, pBlock);
}

bool blockDistSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
  if (!functionSetup(pCtx, pResultInfo)) {
    return false;
  }

  STableBlockDistInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  pInfo->minRows = INT32_MAX;
  return true;
}

int32_t blockDistFunction(SqlFunctionCtx* pCtx) {
  const int32_t BLOCK_DIST_RESULT_ROWS = 24;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  STableBlockDistInfo* pDistInfo = GET_ROWCELL_INTERBUF(pResInfo);

  STableBlockDistInfo p1 = {0};
  tDeserializeBlockDistInfo(varDataVal(pInputCol->pData), varDataLen(pInputCol->pData), &p1);

  pDistInfo->numOfBlocks += p1.numOfBlocks;
  pDistInfo->numOfTables += p1.numOfTables;
  pDistInfo->numOfInmemRows += p1.numOfInmemRows;
  pDistInfo->totalSize += p1.totalSize;
  pDistInfo->totalRows += p1.totalRows;
  pDistInfo->numOfFiles += p1.numOfFiles;

  pDistInfo->defMinRows = p1.defMinRows;
  pDistInfo->defMaxRows = p1.defMaxRows;
  pDistInfo->rowSize = p1.rowSize;
  pDistInfo->numOfSmallBlocks = p1.numOfSmallBlocks;

  if (pDistInfo->minRows > p1.minRows) {
    pDistInfo->minRows = p1.minRows;
  }
  if (pDistInfo->maxRows < p1.maxRows) {
    pDistInfo->maxRows = p1.maxRows;
  }

  for (int32_t i = 0; i < tListLen(pDistInfo->blockRowsHisto); ++i) {
    pDistInfo->blockRowsHisto[i] += p1.blockRowsHisto[i];
  }

  pResInfo->numOfRes = BLOCK_DIST_RESULT_ROWS;  // default output rows
  return TSDB_CODE_SUCCESS;
}

int32_t tSerializeBlockDistInfo(void* buf, int32_t bufLen, const STableBlockDistInfo* pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->rowSize) < 0) return -1;

  if (tEncodeU16(&encoder, pInfo->numOfFiles) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->numOfBlocks) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->numOfTables) < 0) return -1;

  if (tEncodeU64(&encoder, pInfo->totalSize) < 0) return -1;
  if (tEncodeU64(&encoder, pInfo->totalRows) < 0) return -1;
  if (tEncodeI32(&encoder, pInfo->maxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pInfo->minRows) < 0) return -1;
  if (tEncodeI32(&encoder, pInfo->defMaxRows) < 0) return -1;
  if (tEncodeI32(&encoder, pInfo->defMinRows) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->numOfInmemRows) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->numOfSmallBlocks) < 0) return -1;

  for (int32_t i = 0; i < tListLen(pInfo->blockRowsHisto); ++i) {
    if (tEncodeI32(&encoder, pInfo->blockRowsHisto[i]) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeBlockDistInfo(void* buf, int32_t bufLen, STableBlockDistInfo* pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->rowSize) < 0) return -1;

  if (tDecodeU16(&decoder, &pInfo->numOfFiles) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->numOfBlocks) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->numOfTables) < 0) return -1;

  if (tDecodeU64(&decoder, &pInfo->totalSize) < 0) return -1;
  if (tDecodeU64(&decoder, &pInfo->totalRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pInfo->maxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pInfo->minRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pInfo->defMaxRows) < 0) return -1;
  if (tDecodeI32(&decoder, &pInfo->defMinRows) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->numOfInmemRows) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->numOfSmallBlocks) < 0) return -1;

  for (int32_t i = 0; i < tListLen(pInfo->blockRowsHisto); ++i) {
    if (tDecodeI32(&decoder, &pInfo->blockRowsHisto[i]) < 0) return -1;
  }

  tDecoderClear(&decoder);
  return 0;
}

int32_t blockDistFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  STableBlockDistInfo* pData = GET_ROWCELL_INTERBUF(pResInfo);

  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);

  if (pData->totalRows == 0) {
    pData->minRows = 0;
  }

  int32_t row = 0;
  char    st[256] = {0};
  double  totalRawSize = pData->totalRows * pData->rowSize;
  int32_t len = sprintf(st + VARSTR_HEADER_SIZE,
                        "Total_Blocks=[%d] Total_Size=[%.2f Kb] Average_size=[%.2f Kb] Compression_Ratio=[%.2f %c]",
                        pData->numOfBlocks, pData->totalSize / 1024.0, ((double)pData->totalSize) / pData->numOfBlocks,
                        pData->totalSize * 100 / totalRawSize, '%');

  varDataSetLen(st, len);
  colDataAppend(pColInfo, row++, st, false);

  int64_t avgRows = 0;
  if (pData->numOfBlocks > 0) {
    avgRows = pData->totalRows / pData->numOfBlocks;
  }

  len = sprintf(st + VARSTR_HEADER_SIZE,
                "Total_Rows=[%" PRId64 "] Inmem_Rows=[%d] MinRows=[%d] MaxRows=[%d] Average_Rows=[%" PRId64 "]",
                pData->totalRows, pData->numOfInmemRows, pData->minRows, pData->maxRows, avgRows);

  varDataSetLen(st, len);
  colDataAppend(pColInfo, row++, st, false);

  len = sprintf(st + VARSTR_HEADER_SIZE, "Total_Tables=[%d] Total_Files=[%d] Total_Vgroups=[%d]", pData->numOfTables,
                pData->numOfFiles, 0);

  varDataSetLen(st, len);
  colDataAppend(pColInfo, row++, st, false);

  len = sprintf(st + VARSTR_HEADER_SIZE,
                "--------------------------------------------------------------------------------");
  varDataSetLen(st, len);
  colDataAppend(pColInfo, row++, st, false);

  int32_t maxVal = 0;
  int32_t minVal = INT32_MAX;
  for (int32_t i = 0; i < tListLen(pData->blockRowsHisto); ++i) {
    if (maxVal < pData->blockRowsHisto[i]) {
      maxVal = pData->blockRowsHisto[i];
    }

    if (minVal > pData->blockRowsHisto[i]) {
      minVal = pData->blockRowsHisto[i];
    }
  }

  // maximum number of step is 80
  double factor = pData->numOfBlocks / 80.0;

  int32_t numOfBuckets = sizeof(pData->blockRowsHisto) / sizeof(pData->blockRowsHisto[0]);
  int32_t bucketRange = (pData->defMaxRows - pData->defMinRows) / numOfBuckets;

  for (int32_t i = 0; i < tListLen(pData->blockRowsHisto); ++i) {
    len = sprintf(st + VARSTR_HEADER_SIZE, "%04d |", pData->defMinRows + bucketRange * i);

    int32_t num = 0;
    if (pData->blockRowsHisto[i] > 0) {
      num = (pData->blockRowsHisto[i]) / factor;
    }

    for (int32_t j = 0; j < num; ++j) {
      int32_t x = sprintf(st + VARSTR_HEADER_SIZE + len, "%c", '|');
      len += x;
    }

    if (num > 0) {
      double v = pData->blockRowsHisto[i] * 100.0 / pData->numOfBlocks;
      len += sprintf(st + VARSTR_HEADER_SIZE + len, "  %d (%.2f%c)", pData->blockRowsHisto[i], v, '%');
    }

    varDataSetLen(st, len);
    colDataAppend(pColInfo, row++, st, false);
  }

  return TSDB_CODE_SUCCESS;
}

bool getDerivativeFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SDerivInfo);
  return true;
}

bool derivativeFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;  // not initialized since it has been initialized
  }

  SDerivInfo* pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pDerivInfo->ignoreNegative = pCtx->param[2].param.i;
  pDerivInfo->prevTs = -1;
  pDerivInfo->tsWindow = pCtx->param[1].param.i;
  pDerivInfo->valueSet = false;
  return true;
}

int32_t derivativeFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDerivInfo*          pDerivInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;

  int32_t i = pInput->startRowIndex;
  TSKEY*  tsList = (int64_t*)pInput->pPTS->pData;

  double v = 0;

  if (pCtx->order == TSDB_ORDER_ASC) {
    for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        continue;
      }

      char* d = (char*)pInputCol->pData + pInputCol->info.bytes * i;
      GET_TYPED_DATA(v, double, pInputCol->info.type, d);

      int32_t pos = pCtx->offset + numOfElems;
      if (!pDerivInfo->valueSet) {  // initial value is not set yet
        pDerivInfo->valueSet = true;
      } else {
        double r = ((v - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataAppendNULL(pOutput, pos);
          } else {
            colDataAppend(pOutput, pos, (const char*)&r, false);
          }

          if (pTsOutput != NULL) {
            colDataAppendInt64(pTsOutput, pos, &tsList[i]);
          }

          // handle selectivity
          if (pCtx->subsidiaries.num > 0) {
            appendSelectivityValue(pCtx, i, pos);
          }

          numOfElems++;
        }
      }

      pDerivInfo->prevValue = v;
      pDerivInfo->prevTs = tsList[i];
    }
  } else {
    for (; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
      if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
        continue;
      }

      char* d = (char*)pInputCol->pData + pInputCol->info.bytes * i;
      GET_TYPED_DATA(v, double, pInputCol->info.type, d);

      int32_t pos = pCtx->offset + numOfElems;
      if (!pDerivInfo->valueSet) {  // initial value is not set yet
        pDerivInfo->valueSet = true;
      } else {
        double r = ((pDerivInfo->prevValue - v) * pDerivInfo->tsWindow) / (pDerivInfo->prevTs - tsList[i]);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataAppendNULL(pOutput, pos);
          } else {
            colDataAppend(pOutput, pos, (const char*)&r, false);
          }

          if (pTsOutput != NULL) {
            colDataAppendInt64(pTsOutput, pos, &pDerivInfo->prevTs);
          }

          // handle selectivity
          if (pCtx->subsidiaries.num > 0) {
            appendSelectivityValue(pCtx, i, pos);
          }

          numOfElems++;
        }
      }

      pDerivInfo->prevValue = v;
      pDerivInfo->prevTs = tsList[i];
    }
  }

  return numOfElems;
}

bool getIrateFuncEnv(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SRateInfo);
  return true;
}

bool irateFuncSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;  // not initialized since it has been initialized
  }

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pInfo->firstKey = INT64_MIN;
  pInfo->lastKey = INT64_MIN;
  pInfo->firstValue = (double)INT64_MIN;
  pInfo->lastValue = (double)INT64_MIN;

  pInfo->hasResult = 0;
  return true;
}

int32_t irateFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SRateInfo*           pRateInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      continue;
    }

    numOfElems++;

    char*  data = colDataGetData(pInputCol, i);
    double v = 0;
    GET_TYPED_DATA(v, double, type, data);

    if (INT64_MIN == pRateInfo->lastKey) {
      pRateInfo->lastValue = v;
      pRateInfo->lastKey = tsList[i];
      continue;
    }

    if (tsList[i] > pRateInfo->lastKey) {
      if ((INT64_MIN == pRateInfo->firstKey) || pRateInfo->lastKey > pRateInfo->firstKey) {
        pRateInfo->firstValue = pRateInfo->lastValue;
        pRateInfo->firstKey = pRateInfo->lastKey;
      }

      pRateInfo->lastValue = v;
      pRateInfo->lastKey = tsList[i];

      continue;
    }

    if ((INT64_MIN == pRateInfo->firstKey) || tsList[i] > pRateInfo->firstKey) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = tsList[i];
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static double doCalcRate(const SRateInfo* pRateInfo, double tickPerSec) {
  if ((INT64_MIN == pRateInfo->lastKey) || (INT64_MIN == pRateInfo->firstKey) ||
      (pRateInfo->firstKey >= pRateInfo->lastKey)) {
    return 0.0;
  }

  double diff = 0;
  // If the previous value of the last is greater than the last value, only keep the last point instead of the delta
  // value between two values.
  diff = pRateInfo->lastValue;
  if (diff >= pRateInfo->firstValue) {
    diff -= pRateInfo->firstValue;
  }

  int64_t duration = pRateInfo->lastKey - pRateInfo->firstKey;
  if (duration == 0) {
    return 0;
  }

  return (duration > 0) ? ((double)diff) / (duration / tickPerSec) : 0.0;
}

int32_t irateFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  double     result = doCalcRate(pInfo, (double)TSDB_TICK_PER_SECOND(pCtx->param[1].param.i));
  colDataAppend(pCol, pBlock->info.rows, (const char*)&result, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t groupKeyFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SGroupKeyInfo*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t startIndex = pInput->startRowIndex;

  // escape rest of data blocks to avoid first entry to be overwritten.
  if (pInfo->hasResult) {
    goto _group_key_over;
  }

  if (colDataIsNull_s(pInputCol, startIndex)) {
    pInfo->isNull = true;
    pInfo->hasResult = true;
    goto _group_key_over;
  }

  char* data = colDataGetData(pInputCol, startIndex);
  if (IS_VAR_DATA_TYPE(pInputCol->info.type)) {
    memcpy(pInfo->data, data,
           (pInputCol->info.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(data) : varDataTLen(data));
  } else {
    memcpy(pInfo->data, data, pInputCol->info.bytes);
  }
  pInfo->hasResult = true;

_group_key_over:

  SET_VAL(pResInfo, 1, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t groupKeyFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SGroupKeyInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->hasResult) {
    colDataAppend(pCol, pBlock->info.rows, pInfo->data, pInfo->isNull ? true : false);
  } else {
    pResInfo->numOfRes = 0;
  }

  return pResInfo->numOfRes;
}

int32_t interpFunction(SqlFunctionCtx* pCtx) {
#if 0
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
      // do nothing
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
#endif

  return TSDB_CODE_SUCCESS;
}

int32_t cachedLastRowFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  // last_row function does not ignore the null value
  for (int32_t i = pInput->numOfRows + pInput->startRowIndex - 1; i >= pInput->startRowIndex; --i) {
    numOfElems++;

    char* data = colDataGetData(pInputCol, i);
    TSKEY cts = getRowPTs(pInput->pPTS, i);
    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      doSaveLastrow(pCtx, data, i, cts, pInfo);
      pResInfo->numOfRes = 1;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}
