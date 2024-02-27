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
#include "tcompare.h"
#include "tdatablock.h"
#include "tdigest.h"
#include "tfunctionInt.h"
#include "tglobal.h"
#include "thistogram.h"
#include "tpercentile.h"

#define HISTOGRAM_MAX_BINS_NUM 1000
#define MAVG_MAX_POINTS_NUM    1000
#define TAIL_MAX_POINTS_NUM    100
#define TAIL_MAX_OFFSET        100

#define HLL_BUCKET_BITS 14  // The bits of the bucket
#define HLL_DATA_BITS   (64 - HLL_BUCKET_BITS)
#define HLL_BUCKETS     (1 << HLL_BUCKET_BITS)
#define HLL_BUCKET_MASK (HLL_BUCKETS - 1)
#define HLL_ALPHA_INF   0.721347520444481703680  // constant for 0.5/ln(2)

// typedef struct SMinmaxResInfo {
//   bool      assign;  // assign the first value or not
//   int64_t   v;
//   STuplePos tuplePos;
//
//   STuplePos nullTuplePos;
//   bool      nullTupleSaved;
//   int16_t   type;
// } SMinmaxResInfo;

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
  uint64_t totalCount;
  uint8_t  buckets[HLL_BUCKETS];
} SHLLInfo;

typedef struct SStateInfo {
  union {
    int64_t count;
    int64_t durationStart;
  };
  int64_t prevTs;
  bool    isPrevTsSet;
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

//#define LIST_AVG_N(sumT, T)                                               \
//  do {                                                                    \
//    T* plist = (T*)pCol->pData;                                           \
//    for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) { \
//      if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {        \
//        continue;                                                         \
//      }                                                                   \
//                                                                          \
//      numOfElem += 1;                                                     \
//      pAvgRes->count -= 1;                                                \
//      sumT -= plist[i];                                                   \
//    }                                                                     \
//  } while (0)

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
      pStddevRes->quadraticISum -= (int64_t)(plist[i] * plist[i]); \
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

static int32_t firstLastTransferInfoImpl(SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst);

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
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = GET_ROWCELL_INTERBUF(pResInfo);
  colDataSetVal(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

int32_t firstCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              bytes = pDBuf->bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (TSDB_CODE_SUCCESS == firstLastTransferInfoImpl(pSBuf, pDBuf, true)) {
    pDBuf->hasResult = true;
  }

  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

int32_t functionFinalizeWithResultBuf(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, char* finalResult) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  char* in = finalResult;
  colDataSetVal(pCol, pBlock->info.rows, in, pResInfo->isNullRes);

  return pResInfo->numOfRes;
}

EFuncDataRequired countDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_COLUMN == nodeType(pParam) && PRIMARYKEY_TIMESTAMP_COL_ID == ((SColumnNode*)pParam)->colId) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  }
  return FUNC_DATA_REQUIRED_SMA_LOAD;
}

bool getCountFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

static int64_t getNumOfElems(SqlFunctionCtx* pCtx) {
  int64_t numOfElem = 0;

  /*
   * 1. column data missing (schema modified) causes pInputCol->hasNull == true. pInput->colDataSMAIsSet == true;
   * 2. for general non-primary key columns, pInputCol->hasNull may be true or false, pInput->colDataSMAIsSet == true;
   * 3. for primary key column, pInputCol->hasNull always be false, pInput->colDataSMAIsSet == false;
   */
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  if(1 == pInput->numOfRows && pInput->blankFill) {
    return 0;
  }
  if (pInput->colDataSMAIsSet && pInput->totalRows == pInput->numOfRows) {
    numOfElem = pInput->numOfRows - pInput->pColumnDataAgg[0]->numOfNull;
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
  int64_t numOfElem = 0;

  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  SInputColumnInfoData* pInput = &pCtx->input;

  int32_t type = pInput->pData[0]->info.type;

  char* buf = GET_ROWCELL_INTERBUF(pResInfo);
  if (IS_NULL_TYPE(type)) {
    // select count(NULL) returns 0
    numOfElem = 1;
    *((int64_t*)buf) += 0;
  } else {
    numOfElem = getNumOfElems(pCtx);
    *((int64_t*)buf) += numOfElem;
  }

  if (tsCountAlwaysReturnValue) {
    pResInfo->numOfRes = 1;
  } else {
    SET_VAL(pResInfo, *((int64_t*)buf), 1);
  }

  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t countInvertFunction(SqlFunctionCtx* pCtx) {
  int64_t numOfElem = getNumOfElems(pCtx);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  char*                buf = GET_ROWCELL_INTERBUF(pResInfo);
  *((int64_t*)buf) -= numOfElem;

  SET_VAL(pResInfo, *((int64_t*)buf), 1);
  return TSDB_CODE_SUCCESS;
}
#endif

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
  pSumRes->type = type;

  if (IS_NULL_TYPE(type)) {
    numOfElem = 0;
    goto _sum_over;
  }

  if (pInput->colDataSMAIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;

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
    numOfElem = 0;
  }

_sum_over:
  // data in the check operation are all null, not output
  SET_VAL(GET_RES_INFO(pCtx), numOfElem, 1);
  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
int32_t sumInvertFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElem = 0;

  // Only the pre-computing information loaded and actual data does not loaded
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnDataAgg*       pAgg = pInput->pColumnDataAgg[0];
  int32_t               type = pInput->pData[0]->info.type;

  SSumRes* pSumRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  if (pInput->colDataSMAIsSet) {
    numOfElem = pInput->numOfRows - pAgg->numOfNull;

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
#endif

int32_t sumCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SSumRes*             pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SSumRes*             pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
    pDBuf->isum += pSBuf->isum;
  } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
    pDBuf->usum += pSBuf->usum;
  } else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
    pDBuf->dsum += pSBuf->dsum;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

bool getSumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

EFuncDataRequired statisDataRequired(SFunctionNode* pFunc, STimeWindow* pTimeWindow) {
  return FUNC_DATA_REQUIRED_SMA_LOAD;
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

int32_t minFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;
  int32_t code = doMinMaxHelper(pCtx, 1, &numOfElems);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t maxFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;
  int32_t code = doMinMaxHelper(pCtx, 0, &numOfElems);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SET_VAL(GET_RES_INFO(pCtx), numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex);
static int32_t setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos,
                                   int32_t rowIndex);

int32_t minmaxFunctionFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  SMinmaxResInfo*      pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;
  int32_t currentRow = pBlock->info.rows;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  pEntryInfo->isNullRes = (pEntryInfo->numOfRes == 0) ? 1 : 0;

  // NOTE: do nothing change it, for performance issue
  if (!pEntryInfo->isNullRes) {
    switch (pCol->info.type) {
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT:
        ((int64_t*)pCol->pData)[currentRow] = pRes->v;
        break;
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT:
        colDataSetInt32(pCol, currentRow, (int32_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT:
        colDataSetInt16(pCol, currentRow, (int16_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT:
        colDataSetInt8(pCol, currentRow, (int8_t*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        colDataSetDouble(pCol, currentRow, (double*)&pRes->v);
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float v = GET_FLOAT_VAL(&pRes->v);
        colDataSetFloat(pCol, currentRow, &v);
        break;
      }
    }
  } else {
    colDataSetNULL(pCol, currentRow);
  }

  if (pCtx->subsidiaries.num > 0) {
    if (pEntryInfo->numOfRes > 0) {
      code = setSelectivityValue(pCtx, pBlock, &pRes->tuplePos, currentRow);
    } else {
      code = setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
    }
  }

  return code;
}

#ifdef BUILD_NO_CALL
int32_t setNullSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
    SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
    int32_t         dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colDataSetNULL(pDstCol, rowIndex);
  }

  return TSDB_CODE_SUCCESS;
}
#endif

int32_t setSelectivityValue(SqlFunctionCtx* pCtx, SSDataBlock* pBlock, const STuplePos* pTuplePos, int32_t rowIndex) {
  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if ((pCtx->saveHandle.pBuf != NULL && pTuplePos->pageId != -1) ||
      (pCtx->saveHandle.pState && pTuplePos->streamTupleKey.ts > 0)) {
    int32_t     numOfCols = pCtx->subsidiaries.num;
    const char* p = loadTupleData(pCtx, pTuplePos);
    if (p == NULL) {
      terrno = TSDB_CODE_NOT_FOUND;
      qError("Load tuple data failed since %s, groupId:%" PRIu64 ", ts:%" PRId64, terrstr(),
             pTuplePos->streamTupleKey.groupId, pTuplePos->streamTupleKey.ts);
      return terrno;
    }

    bool* nullList = (bool*)p;
    char* pStart = (char*)(nullList + numOfCols * sizeof(bool));

    // todo set the offset value to optimize the performance.
    for (int32_t j = 0; j < numOfCols; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      int32_t         dstSlotId = pc->pExpr->base.resSchema.slotId;

      // group_key function has its own process function
      // do not process there
      if (fmIsGroupKeyFunc(pc->functionId)) {
        continue;
      }

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      if (nullList[j]) {
        colDataSetNULL(pDstCol, rowIndex);
      } else {
        colDataSetVal(pDstCol, rowIndex, pStart, false);
      }
      pStart += pDstCol->info.bytes;
    }
  }

  return TSDB_CODE_SUCCESS;
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
    int32_t dstSlotId = pc->pExpr->base.resSchema.slotId;

    SColumnInfoData* pDstCol = taosArrayGet(pCtx->pDstBlock->pDataBlock, dstSlotId);

    if (colDataIsNull_s(pSrcCol, rowIndex) == true) {
      colDataSetNULL(pDstCol, pos);
    } else {
      colDataSetVal(pDstCol, pos, pData, false);
    }
  }
}

void replaceTupleData(STuplePos* pDestPos, STuplePos* pSourcePos) { *pDestPos = *pSourcePos; }

#define COMPARE_MINMAX_DATA(type) (( (*(type*)&pDBuf->v) < (*(type*)&pSBuf->v) ) ^ isMinFunc)
int32_t minMaxCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx, int32_t isMinFunc) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SMinmaxResInfo*      pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SMinmaxResInfo*      pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  switch (type) {
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int64_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int32_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int16_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT:
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(int8_t) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    case TSDB_DATA_TYPE_FLOAT: {
      if (pSBuf->assign && (COMPARE_MINMAX_DATA(double) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
    }
    default:
      if (pSBuf->assign && (strcmp((char*)&pDBuf->v, (char*)&pSBuf->v) || !pDBuf->assign)) {
        pDBuf->v = pSBuf->v;
        replaceTupleData(&pDBuf->tuplePos, &pSBuf->tuplePos);
        pDBuf->assign = true;
      }
      break;
  }
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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
    numOfElem = 0;
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
        pStddevRes->quadraticUSum += plist[i] * plist[i];
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
        pStddevRes->quadraticUSum += plist[i] * plist[i];
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
        pStddevRes->quadraticUSum += plist[i] * plist[i];
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
        pStddevRes->quadraticUSum += plist[i] * plist[i];
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
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SStddevRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
    char*       data = colDataGetData(pCol, i);
    SStddevRes* pInputInfo = (SStddevRes*)varDataVal(data);
    stddevTransferInfo(pInputInfo, pInfo);
  }

  SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  return TSDB_CODE_SUCCESS;
}

#ifdef BUILD_NO_CALL
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
#endif

int32_t stddevFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SStddevRes*           pStddevRes = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t               type = pStddevRes->type;
  double                avg;

  if (pStddevRes->count == 0) {
    GET_RES_INFO(pCtx)->numOfRes = 0;
    return functionFinalize(pCtx, pBlock);
  }

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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t stddevCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SStddevRes*          pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SStddevRes*          pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);
  int16_t              type = pDBuf->type == TSDB_DATA_TYPE_NULL ? pSBuf->type : pDBuf->type;

  stddevTransferInfo(pSBuf, pDBuf);

  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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

  GET_TYPED_DATA(pInfo->startVal, double, pCtx->param[1].param.nType, &pCtx->param[1].param.i);
  GET_TYPED_DATA(pInfo->stepVal, double, pCtx->param[2].param.nType, &pCtx->param[2].param.i);
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

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t* plist = (uint8_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }
        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t* plist = (uint16_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_UINT: {
      uint32_t* plist = (uint32_t*)pCol->pData;
      for (int32_t i = start; i < numOfRows + pInput->startRowIndex; ++i) {
        if (pCol->hasNull && colDataIsNull_f(pCol->nullbitmap, i)) {
          continue;
        }

        numOfElem++;
        LEASTSQR_CAL(param, x, plist, i, pInfo->stepVal);
      }
      break;
    }

    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t* plist = (uint64_t*)pCol->pData;
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
    colDataSetNULL(pCol, currentRow);
    return 0;
  }

  double(*param)[3] = pInfo->matrix;

  param[1][1] = (double)pInfo->num;
  param[1][0] = param[0][1];

  double param00 = param[0][0] - param[1][0] * (param[0][1] / param[1][1]);
  double param02 = param[0][2] - param[1][2] * (param[0][1] / param[1][1]);

  if (0 == param00) {
    colDataSetNULL(pCol, currentRow);
    return 0;
  }

  // param[0][1] = 0;
  double param12 = param[1][2] - param02 * (param[1][0] / param00);
  // param[1][0] = 0;
  param02 /= param00;

  param12 /= param[1][1];

  char buf[LEASTSQUARES_BUFF_LENGTH] = {0};
  char slopBuf[64] = {0};
  char interceptBuf[64] = {0};
  int  n = snprintf(slopBuf, 64, "%.6lf", param02);
  if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
    snprintf(slopBuf, 64, "%." DOUBLE_PRECISION_DIGITS, param02);
  }
  n = snprintf(interceptBuf, 64, "%.6lf", param12);
  if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
    snprintf(interceptBuf, 64, "%." DOUBLE_PRECISION_DIGITS, param12);
  }
  size_t len =
      snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%s, intercept:%s}", slopBuf, interceptBuf);
  varDataSetLen(buf, len);

  colDataSetVal(pCol, currentRow, buf, pResInfo->isNullRes);

  return pResInfo->numOfRes;
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
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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
  if (pCtx->scanFlag == MAIN_SCAN && pInfo->stage == 0) {
    pInfo->stage += 1;

    // all data are null, set it completed
    if (pInfo->numOfElems == 0) {
      pResInfo->complete = true;
      return TSDB_CODE_SUCCESS;
    } else {
      pInfo->pMemBucket = tMemBucketCreate(pCol->info.bytes, type, pInfo->minval, pInfo->maxval);
    }
  }

  // the first stage, only acquire the min/max value
  if (pInfo->stage == 0) {
    if (pCtx->input.colDataSMAIsSet) {
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
      int32_t code = tMemBucketPut(pInfo->pMemBucket, data, 1);
      if (code != TSDB_CODE_SUCCESS) {
        tMemBucketDestroy(pInfo->pMemBucket);
        return code;
      }
    }

    SET_VAL(pResInfo, numOfElems, 1);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t percentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SPercentileInfo*     ppInfo = (SPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  int32_t code = 0;
  double  v = 0;

  tMemBucket* pMemBucket = ppInfo->pMemBucket;
  if (pMemBucket != NULL && pMemBucket->total > 0) {  // check for null
    if (pCtx->numOfParams > 2) {
      char   buf[512] = {0};
      size_t len = 1;

      varDataVal(buf)[0] = '[';
      for (int32_t i = 1; i < pCtx->numOfParams; ++i) {
        SVariant* pVal = &pCtx->param[i].param;

        GET_TYPED_DATA(v, double, pVal->nType, &pVal->i);

        code = getPercentile(pMemBucket, v, &ppInfo->result);
        if (code != TSDB_CODE_SUCCESS) {
          goto _fin_error;
        }

        if (i == pCtx->numOfParams - 1) {
          len += snprintf(varDataVal(buf) + len, sizeof(buf) - VARSTR_HEADER_SIZE - len, "%.6lf]", ppInfo->result);
        } else {
          len += snprintf(varDataVal(buf) + len, sizeof(buf) - VARSTR_HEADER_SIZE - len, "%.6lf, ", ppInfo->result);
        }
      }

      int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

      varDataSetLen(buf, len);
      colDataSetVal(pCol, pBlock->info.rows, buf, false);

      tMemBucketDestroy(pMemBucket);
      return pResInfo->numOfRes;
    } else {
      SVariant* pVal = &pCtx->param[1].param;

      GET_TYPED_DATA(v, double, pVal->nType, &pVal->i);

      code = getPercentile(pMemBucket, v, &ppInfo->result);
      if (code != TSDB_CODE_SUCCESS) {
        goto _fin_error;
      }

      tMemBucketDestroy(pMemBucket);
      return functionFinalize(pCtx, pBlock);
    }
  }

_fin_error:

  tMemBucketDestroy(pMemBucket);
  return code;
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
  pInfo->percent = 0;
  GET_TYPED_DATA(pInfo->percent, double, pVal->nType, &pVal->i);

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
    buildTDigestInfo(pInfo);
    tdigestAutoFill(pInfo->pTDigest, COMPRESSION);
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
    // might be a race condition here that pHisto can be overwritten or setup function
    // has not been called, need to relink the buffer pHisto points to.
    buildHistogramInfo(pInfo);
    qDebug("%s before add %d elements into histogram, total:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems: %p",
           __FUNCTION__, numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto,
           pInfo->pHisto->elems);
    for (int32_t i = start; i < pInput->numOfRows + start; ++i) {
      if (colDataIsNull_f(pCol->nullbitmap, i)) {
        continue;
      }
      numOfElems += 1;
      char* data = colDataGetData(pCol, i);

      double v = 0;
      GET_TYPED_DATA(v, double, type, data);
      int32_t code = tHistogramAdd(&pInfo->pHisto, v);
      if (code != 0) {
        return TSDB_CODE_FAILED;
      }
    }

    qDebug("%s after add %d elements into histogram, total:%" PRId64 ", numOfEntry:%d, pHisto:%p, elems: %p",
           __FUNCTION__, numOfElems, pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries, pInfo->pHisto,
           pInfo->pHisto->elems);
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static void apercentileTransferInfo(SAPercentileInfo* pInput, SAPercentileInfo* pOutput, bool* hasRes) {
  pOutput->percent = pInput->percent;
  pOutput->algo = pInput->algo;
  if (pOutput->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInput);
    tdigestAutoFill(pInput->pTDigest, COMPRESSION);

    if (pInput->pTDigest->num_centroids == 0 && pInput->pTDigest->num_buffered_pts == 0) {
      return;
    }

    if (hasRes) {
      *hasRes = true;
    }

    buildTDigestInfo(pOutput);
    TDigest* pTDigest = pOutput->pTDigest;
    tdigestAutoFill(pTDigest, COMPRESSION);

    if (pTDigest->num_centroids <= 0 && pTDigest->num_buffered_pts == 0) {
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

    if (hasRes) {
      *hasRes = true;
    }

    buildHistogramInfo(pOutput);
    SHistogramInfo* pHisto = pOutput->pHisto;

    if (pHisto->numOfElems <= 0) {
      memcpy(pHisto, pInput->pHisto, sizeof(SHistogramInfo) + sizeof(SHistBin) * (MAX_HISTOGRAM_BIN + 1));
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems, pHisto->numOfEntries,
             pHisto);
    } else {
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));
      qDebug("%s input histogram, elem:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems,
             pHisto->numOfEntries, pInput->pHisto);

      SHistogramInfo* pRes = tHistogramMerge(pHisto, pInput->pHisto, MAX_HISTOGRAM_BIN);
      memcpy(pHisto, pRes, sizeof(SHistogramInfo) + sizeof(SHistBin) * MAX_HISTOGRAM_BIN);
      pHisto->elems = (SHistBin*)((char*)pHisto + sizeof(SHistogramInfo));

      qDebug("%s merge histo, total:%" PRId64 ", entry:%d, %p", __FUNCTION__, pHisto->numOfElems, pHisto->numOfEntries,
             pHisto);
      tHistogramDestroy(&pRes);
    }
  }
}

int32_t apercentileFunctionMerge(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SAPercentileInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  qDebug("%s total %" PRId64 " rows will merge, %p", __FUNCTION__, pInput->numOfRows, pInfo->pHisto);

  bool hasRes = false;
  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char* data = colDataGetData(pCol, i);

    SAPercentileInfo* pInputInfo = (SAPercentileInfo*)varDataVal(data);
    apercentileTransferInfo(pInputInfo, pInfo, &hasRes);
  }

  if (pInfo->algo != APERCT_ALGO_TDIGEST) {
    buildHistogramInfo(pInfo);
    qDebug("%s after merge, total:%" PRId64 ", numOfEntry:%d, %p", __FUNCTION__, pInfo->pHisto->numOfElems,
           pInfo->pHisto->numOfEntries, pInfo->pHisto);
  }

  SET_VAL(pResInfo, hasRes ? 1 : 0, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SAPercentileInfo*    pInfo = (SAPercentileInfo*)GET_ROWCELL_INTERBUF(pResInfo);

  if (pInfo->algo == APERCT_ALGO_TDIGEST) {
    buildTDigestInfo(pInfo);
    tdigestAutoFill(pInfo->pTDigest, COMPRESSION);
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
      // return TSDB_CODE_SUCCESS;
      qDebug("%s get the final res, elements:%" PRId64 ", numOfEntry:%d. result is null", __FUNCTION__,
             pInfo->pHisto->numOfElems, pInfo->pHisto->numOfEntries);
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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t apercentileCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SAPercentileInfo*    pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SAPercentileInfo*    pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  qDebug("%s start to combine apercentile, %p", __FUNCTION__, pDBuf->pHisto);

  apercentileTransferInfo(pSBuf, pDBuf, NULL);
  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

EFuncDataRequired firstDynDataReq(void* pRes, STimeWindow* pTimeWindow) {
  SResultRowEntryInfo* pEntry = (SResultRowEntryInfo*)pRes;

  // not initialized yet, data is required
  if (pEntry == NULL) {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SFirstLastRes* pResult = GET_ROWCELL_INTERBUF(pEntry);
  if (pResult->hasResult && pResult->ts <= pTimeWindow->skey) {
    return FUNC_DATA_REQUIRED_NOT_LOAD;
  } else {
    return FUNC_DATA_REQUIRED_DATA_LOAD;
  }
}

EFuncDataRequired lastDynDataReq(void* pRes, STimeWindow* pTimeWindow) {
  SResultRowEntryInfo* pEntry = (SResultRowEntryInfo*)pRes;

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
  if (pTsColInfo == NULL || pTsColInfo->pData == NULL) {
    return 0;
  }

  return *(TSKEY*)colDataGetData(pTsColInfo, rowIndex);
}

static void prepareBuf(SqlFunctionCtx* pCtx) {
  if (pCtx->subsidiaries.rowLen == 0) {
    int32_t rowLen = 0;
    for (int32_t j = 0; j < pCtx->subsidiaries.num; ++j) {
      SqlFunctionCtx* pc = pCtx->subsidiaries.pCtx[j];
      rowLen += pc->pExpr->base.resSchema.bytes;
    }

    pCtx->subsidiaries.rowLen = rowLen + pCtx->subsidiaries.num * sizeof(bool);
    pCtx->subsidiaries.buf = taosMemoryMalloc(pCtx->subsidiaries.rowLen);
  }
}

static int32_t firstlastSaveTupleData(const SSDataBlock* pSrcBlock, int32_t rowIndex, SqlFunctionCtx* pCtx,
                                      SFirstLastRes* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pCtx->subsidiaries.num <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (!pInfo->hasResult) {
    code = saveTupleData(pCtx, rowIndex, pSrcBlock, &pInfo->pos);
  } else {
    code = updateTupleData(pCtx, rowIndex, pSrcBlock, &pInfo->pos);
  }

  return code;
}

static int32_t doSaveCurrentVal(SqlFunctionCtx* pCtx, int32_t rowIndex, int64_t currentTs, int32_t type, char* pData) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  if (IS_VAR_DATA_TYPE(type)) {
    pInfo->bytes = varDataTLen(pData);
  }

  memcpy(pInfo->buf, pData, pInfo->bytes);
  pInfo->ts = currentTs;
  int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->hasResult = true;
  return TSDB_CODE_SUCCESS;
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

  if (IS_NULL_TYPE(pInputCol->info.type)) {
    return TSDB_CODE_SUCCESS;
  }

  // All null data column, return directly.
  if (pInput->colDataSMAIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) &&
      pInputCol->hasNull == true) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    return TSDB_CODE_SUCCESS;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataSMAIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first
  //  function. we will use this opt implementation in an new version that is only available in scan subplan
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
  int64_t* pts = (int64_t*)pInput->pPTS->pData;
  for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
    if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
      continue;
    }

    numOfElems++;

    char* data = colDataGetData(pInputCol, i);
    TSKEY cts = pts[i];
    if (pResInfo->numOfRes == 0 || pInfo->ts > cts) {
      int32_t code = doSaveCurrentVal(pCtx, i, cts, pInputCol->info.type, data);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pResInfo->numOfRes = 1;
    }
  }
#endif

  if (numOfElems == 0) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
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

  if (IS_NULL_TYPE(type)) {
    return TSDB_CODE_SUCCESS;
  }

  // All null data column, return directly.
  if (pInput->colDataSMAIsSet && (pInput->pColumnDataAgg[0]->numOfNull == pInput->totalRows) &&
      pInputCol->hasNull == true) {
    // save selectivity value for column consisted of all null values
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    return TSDB_CODE_SUCCESS;
  }

  SColumnDataAgg* pColAgg = (pInput->colDataSMAIsSet) ? pInput->pColumnDataAgg[0] : NULL;

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  //  please ref. to the comment in lastRowFunction for the reason why disabling the opt version of last/first
  //  function.
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
  int64_t* pts = (int64_t*)pInput->pPTS->pData;

#if 0
    for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
      if (pInputCol->hasNull && colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;
      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i]) {
        char* data = colDataGetData(pInputCol, i);
        doSaveCurrentVal(pCtx, i, pts[i], type, data);
        pResInfo->numOfRes = 1;
      }
    }
#else
  if (!pInputCol->hasNull) {
    numOfElems = 1;

    int32_t round = pInput->numOfRows >> 2;
    int32_t reminder = pInput->numOfRows & 0x03;

    for (int32_t i = pInput->startRowIndex, tick = 0; tick < round; i += 4, tick += 1) {
      int64_t cts = pts[i];
      int32_t chosen = i;

      if (cts < pts[i + 1]) {
        cts = pts[i + 1];
        chosen = i + 1;
      }

      if (cts < pts[i + 2]) {
        cts = pts[i + 2];
        chosen = i + 2;
      }

      if (cts < pts[i + 3]) {
        cts = pts[i + 3];
        chosen = i + 3;
      }

      if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
        char*   data = colDataGetData(pInputCol, chosen);
        int32_t code = doSaveCurrentVal(pCtx, i, cts, type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }

    for (int32_t i = pInput->startRowIndex + round * 4; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i]) {
        char*   data = colDataGetData(pInputCol, i);
        int32_t code = doSaveCurrentVal(pCtx, i, pts[i], type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }
  } else {
    for (int32_t i = pInput->startRowIndex; i < pInput->startRowIndex + pInput->numOfRows; ++i) {
      if (colDataIsNull(pInputCol, pInput->totalRows, i, pColAgg)) {
        continue;
      }

      numOfElems++;

      if (pResInfo->numOfRes == 0 || pInfo->ts < pts[i]) {
        char*   data = colDataGetData(pInputCol, i);
        int32_t code = doSaveCurrentVal(pCtx, i, pts[i], type, data);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        pResInfo->numOfRes = 1;
      }
    }
  }
#endif

#endif

  // save selectivity value for column consisted of all null values
  if (numOfElems == 0) {
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, pInput->startRowIndex, pCtx, pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  //  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}

static int32_t firstLastTransferInfoImpl(SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst) {
  if (!pInput->hasResult) {
    return TSDB_CODE_FAILED;
  }

  if (pOutput->hasResult) {
    if (isFirst) {
      if (pInput->ts > pOutput->ts) {
        return TSDB_CODE_FAILED;
      }
    } else {
      if (pInput->ts < pOutput->ts) {
        return TSDB_CODE_FAILED;
      }
    }
  }

  pOutput->isNull = pInput->isNull;
  pOutput->ts = pInput->ts;
  pOutput->bytes = pInput->bytes;

  memcpy(pOutput->buf, pInput->buf, pOutput->bytes);
  return TSDB_CODE_SUCCESS;
}

static int32_t firstLastTransferInfo(SqlFunctionCtx* pCtx, SFirstLastRes* pInput, SFirstLastRes* pOutput, bool isFirst,
                                     int32_t rowIndex) {
  if (TSDB_CODE_SUCCESS == firstLastTransferInfoImpl(pInput, pOutput, isFirst)) {
    int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pOutput);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pOutput->hasResult = true;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t firstLastFunctionMergeImpl(SqlFunctionCtx* pCtx, bool isFirstQuery) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SFirstLastRes* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  int32_t numOfElems = 0;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pCol, i)) {
      continue;
    }
    char*          data = colDataGetData(pCol, i);
    SFirstLastRes* pInputInfo = (SFirstLastRes*)varDataVal(data);
    int32_t        code = firstLastTransferInfo(pCtx, pInputInfo, pInfo, isFirstQuery, i);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
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
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SFirstLastRes* pRes = GET_ROWCELL_INTERBUF(pResInfo);
  colDataSetVal(pCol, pBlock->info.rows, pRes->buf, pRes->isNull || pResInfo->isNullRes);

  // handle selectivity
  code = setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);

  return code;
}

int32_t firstLastPartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pRes = GET_ROWCELL_INTERBUF(pEntryInfo);

  int32_t resultBytes = getFirstLastInfoSize(pRes->bytes);

  // todo check for failure
  char* res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));
  memcpy(varDataVal(res), pRes, resultBytes);

  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataSetVal(pCol, pBlock->info.rows, res, false);
  code = setSelectivityValue(pCtx, pBlock, &pRes->pos, pBlock->info.rows);

  taosMemoryFree(res);
  return code;
}

int32_t lastCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SFirstLastRes*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);
  int32_t              bytes = pDBuf->bytes;

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SFirstLastRes*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  if (TSDB_CODE_SUCCESS == firstLastTransferInfoImpl(pSBuf, pDBuf, false)) {
    pDBuf->hasResult = true;
  }

  pDResInfo->numOfRes = TMAX(pDResInfo->numOfRes, pSResInfo->numOfRes);
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
  return TSDB_CODE_SUCCESS;
}

static int32_t doSaveLastrow(SqlFunctionCtx* pCtx, char* pData, int32_t rowIndex, int64_t cts, SFirstLastRes* pInfo) {
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
  int32_t code = firstlastSaveTupleData(pCtx->pSrcBlock, rowIndex, pCtx, pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pInfo->hasResult = true;

  return TSDB_CODE_SUCCESS;
}

int32_t lastRowFunction(SqlFunctionCtx* pCtx) {
  int32_t numOfElems = 0;

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SFirstLastRes*       pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];

  int32_t type = pInputCol->info.type;
  int32_t bytes = pInputCol->info.bytes;
  pInfo->bytes = bytes;

  if (IS_NULL_TYPE(type)) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY startKey = getRowPTs(pInput->pPTS, 0);
  TSKEY endKey = getRowPTs(pInput->pPTS, pInput->totalRows - 1);

#if 0
  int32_t blockDataOrder = (startKey <= endKey) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;

  // the optimized version only valid if all tuples in one block are monotonious increasing or descreasing.
  // this assumption is NOT always works if project operator exists in downstream.
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

  int64_t* pts = (int64_t*)pInput->pPTS->pData;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; ++i) {
    bool  isNull = colDataIsNull(pInputCol, pInput->numOfRows, i, NULL);
    char* data = isNull ? NULL : colDataGetData(pInputCol, i);
    TSKEY cts = pts[i];

    numOfElems++;
    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
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
  pDiffInfo->prevTs = -1;
  if (pCtx->numOfParams > 1) {
    pDiffInfo->ignoreNegative = pCtx->param[1].param.i;  // TODO set correct param
  } else {
    pDiffInfo->ignoreNegative = false;
  }
  pDiffInfo->includeNull = true;
  pDiffInfo->firstOutput = false;
  return true;
}

static int32_t doSetPrevVal(SDiffInfo* pDiffInfo, int32_t type, const char* pv, int64_t ts) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      pDiffInfo->prev.i64 = *(bool*)pv ? 1 : 0;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT:
      pDiffInfo->prev.i64 = *(int8_t*)pv;
      break;
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT:
      pDiffInfo->prev.i64 = *(int32_t*)pv;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT:
      pDiffInfo->prev.i64 = *(int16_t*)pv;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
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
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }
  pDiffInfo->prevTs = ts;

  return TSDB_CODE_SUCCESS;
}

static int32_t doHandleDiff(SDiffInfo* pDiffInfo, int32_t type, const char* pv, SColumnInfoData* pOutput, int32_t pos,
                            int64_t ts) {
  pDiffInfo->prevTs = ts;
  switch (type) {
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t*)pv;
      int64_t delta = v - pDiffInfo->prev.i64;  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;

      break;
    }
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t  v = *(int8_t*)pv;
      int64_t delta = v - pDiffInfo->prev.i64;  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t*)pv;
      int64_t delta = v - pDiffInfo->prev.i64;  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t*)pv;
      int64_t delta = v - pDiffInfo->prev.i64;  // direct previous may be null
      if (delta < 0 && pDiffInfo->ignoreNegative) {
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetInt64(pOutput, pos, &delta);
      }
      pDiffInfo->prev.i64 = v;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float  v = *(float*)pv;
      double delta = v - pDiffInfo->prev.d64;  // direct previous may be null
      if ((delta < 0 && pDiffInfo->ignoreNegative) || isinf(delta) || isnan(delta)) {  // check for overflow
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetDouble(pOutput, pos, &delta);
      }
      pDiffInfo->prev.d64 = v;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double*)pv;
      double delta = v - pDiffInfo->prev.d64;  // direct previous may be null
      if ((delta < 0 && pDiffInfo->ignoreNegative) || isinf(delta) || isnan(delta)) {  // check for overflow
        colDataSetNull_f_s(pOutput, pos);
      } else {
        colDataSetDouble(pOutput, pos, &delta);
      }
      pDiffInfo->prev.d64 = v;
      break;
    }
    default:
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t diffFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SDiffInfo*           pDiffInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];

  TSKEY* tsList = (int64_t*)pInput->pPTS->pData;

  int32_t numOfElems = 0;
  int32_t startOffset = pCtx->offset;

  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    int32_t pos = startOffset + numOfElems;

    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      if (pDiffInfo->includeNull) {
        colDataSetNull_f_s(pOutput, pos);

        numOfElems += 1;
      }
      continue;
    }

    char* pv = colDataGetData(pInputCol, i);

    if (pDiffInfo->hasPrev) {
      if (tsList[i] == pDiffInfo->prevTs) {
        return TSDB_CODE_FUNC_DUP_TIMESTAMP;
      }
      int32_t code = doHandleDiff(pDiffInfo, pInputCol->info.type, pv, pOutput, pos, tsList[i]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, pos);
      }

      numOfElems++;
    } else {
      int32_t code = doSetPrevVal(pDiffInfo, pInputCol->info.type, pv, tsList[i]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    pDiffInfo->hasPrev = true;
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
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

static int32_t doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock,
                               uint16_t type, uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery);

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
    char*   data = colDataGetData(pCol, i);
    int32_t code = doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
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
    char*   data = colDataGetData(pCol, i);
    int32_t code = doAddIntoResult(pCtx, data, i, pCtx->pSrcBlock, pRes->type, pInput->uid, pResInfo, false);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pRes->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pRes->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
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
  } else if (TSDB_DATA_TYPE_FLOAT == type) {
    if (val1->v.f == val2->v.f) {
      return 0;
    }

    return (val1->v.f > val2->v.f) ? 1 : -1;
  }

  if (val1->v.d == val2->v.d) {
    return 0;
  }

  return (val1->v.d > val2->v.d) ? 1 : -1;
}

int32_t doAddIntoResult(SqlFunctionCtx* pCtx, void* pData, int32_t rowIndex, SSDataBlock* pSrcBlock, uint16_t type,
                        uint64_t uid, SResultRowEntryInfo* pEntryInfo, bool isTopQuery) {
  STopBotRes* pRes = getTopBotOutputInfo(pCtx);

  SVariant val = {0};
  taosVariantCreateFromBinary(&val, pData, tDataTypes[type].bytes, type);

  STopBotResItem* pItems = pRes->pItems;

  // not full yet
  if (pEntryInfo->numOfRes < pRes->maxSize) {
    STopBotResItem* pItem = &pItems[pEntryInfo->numOfRes];
    pItem->v = val;
    pItem->uid = uid;

    // save the data of this tuple
    if (pCtx->subsidiaries.num > 0) {
      int32_t code = saveTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
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
                        (TSDB_DATA_TYPE_FLOAT == type && val.f > pItems[0].v.f) ||
                        (TSDB_DATA_TYPE_DOUBLE == type && val.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && val.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && val.u < pItems[0].v.u) ||
                         (TSDB_DATA_TYPE_FLOAT == type && val.f < pItems[0].v.f) ||
                         (TSDB_DATA_TYPE_DOUBLE == type && val.d < pItems[0].v.d)))) {
      // replace the old data and the coresponding tuple data
      STopBotResItem* pItem = &pItems[0];
      pItem->v = val;
      pItem->uid = uid;

      // save the data of this tuple by over writing the old data
      if (pCtx->subsidiaries.num > 0) {
        int32_t code = updateTupleData(pCtx, rowIndex, pSrcBlock, &pItem->tuplePos);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
#ifdef BUF_PAGE_DEBUG
      qDebug("page_copyTuple pageId:%d, offset:%d", pItem->tuplePos.pageId, pItem->tuplePos.offset);
#endif
      taosheapadjust((void*)pItems, sizeof(STopBotResItem), 0, pEntryInfo->numOfRes - 1, (const void*)&type,
                     topBotResComparFn, NULL, !isTopQuery);
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * +------------------------------------+--------------+--------------+
 * |            null bitmap             |              |              |
 * |(n columns, one bit for each column)| src column #1| src column #2|
 * +------------------------------------+--------------+--------------+
 */
void* serializeTupleData(const SSDataBlock* pSrcBlock, int32_t rowIndex, SSubsidiaryResInfo* pSubsidiaryies,
                         char* buf) {
  char* nullList = buf;
  char* pStart = (char*)(nullList + sizeof(bool) * pSubsidiaryies->num);

  int32_t offset = 0;
  for (int32_t i = 0; i < pSubsidiaryies->num; ++i) {
    SqlFunctionCtx* pc = pSubsidiaryies->pCtx[i];

    // group_key function has its own process function
    // do not process there
    if (fmIsGroupKeyFunc(pc->functionId)) {
      continue;
    }

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

  return buf;
}

static int32_t doSaveTupleData(SSerializeDataHandle* pHandle, const void* pBuf, size_t length, SWinKey* key,
                               STuplePos* pPos, SFunctionStateStore* pStore) {
  STuplePos p = {0};
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = NULL;

    if (pHandle->currentPage == -1) {
      pPage = getNewBufPage(pHandle->pBuf, &pHandle->currentPage);
      if (pPage == NULL) {
        return terrno;
      }
      pPage->num = sizeof(SFilePage);
    } else {
      pPage = getBufPage(pHandle->pBuf, pHandle->currentPage);
      if (pPage == NULL) {
        return terrno;
      }
      if (pPage->num + length > getBufPageSize(pHandle->pBuf)) {
        // current page is all used, let's prepare a new buffer page
        releaseBufPage(pHandle->pBuf, pPage);
        pPage = getNewBufPage(pHandle->pBuf, &pHandle->currentPage);
        if (pPage == NULL) {
          return terrno;
        }
        pPage->num = sizeof(SFilePage);
      }
    }

    p = (STuplePos){.pageId = pHandle->currentPage, .offset = pPage->num};
    memcpy(pPage->data + pPage->num, pBuf, length);

    pPage->num += length;
    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);
  } else { // other tuple save policy
    if (pStore->streamStateFuncPut(pHandle->pState, key, pBuf, length) >= 0) {
      p.streamTupleKey = *key;
    }
  }

  *pPos = p;
  return TSDB_CODE_SUCCESS;
}

int32_t saveTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  prepareBuf(pCtx);

  SWinKey key;
  if (pCtx->saveHandle.pBuf == NULL) {
    SColumnInfoData* pColInfo = taosArrayGet(pSrcBlock->pDataBlock, 0);
    if (pColInfo->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
      int64_t skey = *(int64_t*)colDataGetData(pColInfo, rowIndex);

      key.groupId = pSrcBlock->info.id.groupId;
      key.ts = skey;
    }
  }

  char* buf = serializeTupleData(pSrcBlock, rowIndex, &pCtx->subsidiaries, pCtx->subsidiaries.buf);
  return doSaveTupleData(&pCtx->saveHandle, buf, pCtx->subsidiaries.rowLen, &key, pPos, pCtx->pStore);
}

static int32_t doUpdateTupleData(SSerializeDataHandle* pHandle, const void* pBuf, size_t length, STuplePos* pPos, SFunctionStateStore* pStore) {
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = getBufPage(pHandle->pBuf, pPos->pageId);
    if (pPage == NULL) {
      return terrno;
    }
    memcpy(pPage->data + pPos->offset, pBuf, length);
    setBufPageDirty(pPage, true);
    releaseBufPage(pHandle->pBuf, pPage);
  } else {
    pStore->streamStateFuncPut(pHandle->pState, &pPos->streamTupleKey, pBuf, length);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t updateTupleData(SqlFunctionCtx* pCtx, int32_t rowIndex, const SSDataBlock* pSrcBlock, STuplePos* pPos) {
  prepareBuf(pCtx);

  char* buf = serializeTupleData(pSrcBlock, rowIndex, &pCtx->subsidiaries, pCtx->subsidiaries.buf);
  return doUpdateTupleData(&pCtx->saveHandle, buf, pCtx->subsidiaries.rowLen, pPos, pCtx->pStore);
}

static char* doLoadTupleData(SSerializeDataHandle* pHandle, const STuplePos* pPos, SFunctionStateStore* pStore) {
  if (pHandle->pBuf != NULL) {
    SFilePage* pPage = getBufPage(pHandle->pBuf, pPos->pageId);
    if (pPage == NULL) {
      return NULL;
    }
    char* p = pPage->data + pPos->offset;
    releaseBufPage(pHandle->pBuf, pPage);
    return p;
  } else {
    void*   value = NULL;
    int32_t vLen;
    pStore->streamStateFuncGet(pHandle->pState, &pPos->streamTupleKey, &value, &vLen);
    return (char*)value;
  }
}

const char* loadTupleData(SqlFunctionCtx* pCtx, const STuplePos* pPos) {
  return doLoadTupleData(&pCtx->saveHandle, pPos, pCtx->pStore);
}

int32_t topBotFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);

  int16_t type = pCtx->pExpr->base.resSchema.type;
  int32_t slotId = pCtx->pExpr->base.resSchema.slotId;

  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  // todo assign the tag value and the corresponding row data
  int32_t currentRow = pBlock->info.rows;
  if (pEntryInfo->numOfRes <= 0) {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pRes->nullTuplePos, currentRow);
    return code;
  }
  for (int32_t i = 0; i < pEntryInfo->numOfRes; ++i) {
    STopBotResItem* pItem = &pRes->pItems[i];
    colDataSetVal(pCol, currentRow, (const char*)&pItem->v.i, false);
#ifdef BUF_PAGE_DEBUG
    qDebug("page_finalize i:%d,item:%p,pageId:%d, offset:%d\n", i, pItem, pItem->tuplePos.pageId,
           pItem->tuplePos.offset);
#endif
    code = setSelectivityValue(pCtx, pBlock, &pRes->pItems[i].tuplePos, currentRow);
    currentRow += 1;
  }

  return code;
}

void addResult(SqlFunctionCtx* pCtx, STopBotResItem* pSourceItem, int16_t type, bool isTopQuery) {
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);
  STopBotRes*          pRes = getTopBotOutputInfo(pCtx);
  STopBotResItem*      pItems = pRes->pItems;

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
                        (TSDB_DATA_TYPE_FLOAT == type && pSourceItem->v.f > pItems[0].v.f) ||
                        (TSDB_DATA_TYPE_DOUBLE == type && pSourceItem->v.d > pItems[0].v.d))) ||
        (!isTopQuery && ((IS_SIGNED_NUMERIC_TYPE(type) && pSourceItem->v.i < pItems[0].v.i) ||
                         (IS_UNSIGNED_NUMERIC_TYPE(type) && pSourceItem->v.u < pItems[0].v.u) ||
                         (TSDB_DATA_TYPE_FLOAT == type && pSourceItem->v.f < pItems[0].v.f) ||
                         (TSDB_DATA_TYPE_DOUBLE == type && pSourceItem->v.d < pItems[0].v.d)))) {
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
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  int16_t              type = pSBuf->type;
  for (int32_t i = 0; i < pSResInfo->numOfRes; i++) {
    addResult(pDestCtx, pSBuf->pItems + i, type, true);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t bottomCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  STopBotRes*          pSBuf = getTopBotOutputInfo(pSourceCtx);
  int16_t              type = pSBuf->type;
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

  if (pInput->colDataSMAIsSet) {
    numOfElems = pInput->numOfRows - pAgg->numOfNull;
    if (numOfElems == 0) {
      goto _spread_over;
    }
    double tmin = 0.0, tmax = 0.0;
    if (IS_SIGNED_NUMERIC_TYPE(type) || IS_TIMESTAMP_TYPE(type)) {
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
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

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
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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

  if (pInput->colDataSMAIsSet) {
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
        pInfo->max = (pInfo->max < ptsList[start]) ? ptsList[start] : pInfo->max;
      } else {
        pInfo->max = pCtx->start.key + 1;
      }

      if (pCtx->end.key == INT64_MIN) {
        pInfo->min =
            (pInfo->min > ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->min;
      } else {
        pInfo->min = pCtx->end.key;
      }
    } else {
      if (pCtx->start.key == INT64_MIN) {
        pInfo->min = (pInfo->min > ptsList[start]) ? ptsList[start] : pInfo->min;
      } else {
        pInfo->min = pCtx->start.key;
      }

      if (pCtx->end.key == INT64_MIN) {
        pInfo->max =
            (pInfo->max < ptsList[start + pInput->numOfRows - 1]) ? ptsList[start + pInput->numOfRows - 1] : pInfo->max;
      } else {
        pInfo->max = pCtx->end.key + 1;
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
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

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
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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
      cJSON_Delete(binDesc);
      return false;
    }

    cJSON* start = cJSON_GetObjectItem(binDesc, "start");
    cJSON* factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON* width = cJSON_GetObjectItem(binDesc, "width");
    cJSON* count = cJSON_GetObjectItem(binDesc, "count");
    cJSON* infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      cJSON_Delete(binDesc);
      return false;
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      cJSON_Delete(binDesc);
      return false;
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      cJSON_Delete(binDesc);
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
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      if (numOfBins < 4) {
        return false;
      }
      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      cJSON_Delete(binDesc);
      return false;
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    cJSON* bin = binDesc->child;
    if (bin == NULL) {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      bin = bin->next;
      i++;
    }
  } else {
    cJSON_Delete(binDesc);
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
  cJSON_Delete(binDesc);

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

  char*  binTypeStr = strndup(varDataVal(pCtx->param[1].param.pz), varDataLen(pCtx->param[1].param.pz));
  int8_t binType = getHistogramBinType(binTypeStr);
  taosMemoryFree(binTypeStr);

  if (binType == UNKNOWN_BIN) {
    return false;
  }
  char*   binDesc = strndup(varDataVal(pCtx->param[2].param.pz), varDataLen(pCtx->param[2].param.pz));
  int64_t normalized = pCtx->param[3].param.i;
  if (normalized != 0 && normalized != 1) {
    taosMemoryFree(binDesc);
    return false;
  }
  if (!getHistogramBinDesc(pInfo, binDesc, binType, (bool)normalized)) {
    taosMemoryFree(binDesc);
    return false;
  }
  taosMemoryFree(binDesc);

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
    GET_RES_INFO(pCtx)->numOfRes = pInfo->numOfBins;
  } else {
    GET_RES_INFO(pCtx)->numOfRes = 1;
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
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

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
    colDataSetVal(pCol, currentRow, buf, false);
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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

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
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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
  if (IS_NULL_TYPE(type)) {
    goto _hll_over;
  }

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

_hll_over:
  pInfo->totalCount += numOfElems;

  if (pInfo->totalCount == 0 && !tsCountAlwaysReturnValue) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
  } else {
    SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  }

  return TSDB_CODE_SUCCESS;
}

static void hllTransferInfo(SHLLInfo* pInput, SHLLInfo* pOutput) {
  for (int32_t k = 0; k < HLL_BUCKETS; ++k) {
    if (pOutput->buckets[k] < pInput->buckets[k]) {
      pOutput->buckets[k] = pInput->buckets[k];
    }
  }
  pOutput->totalCount += pInput->totalCount;
}

int32_t hllFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];

  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_SUCCESS;
  }

  SHLLInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;

  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*     data = colDataGetData(pCol, i);
    SHLLInfo* pInputInfo = (SHLLInfo*)varDataVal(data);
    hllTransferInfo(pInputInfo, pInfo);
  }

  if (pInfo->totalCount == 0 && !tsCountAlwaysReturnValue) {
    SET_VAL(GET_RES_INFO(pCtx), 0, 1);
  } else {
    SET_VAL(GET_RES_INFO(pCtx), 1, 1);
  }

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

  colDataSetVal(pCol, pBlock->info.rows, res, false);

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
  pDResInfo->isNullRes &= pSResInfo->isNullRes;
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
      return false;
    }
  }
  return false;
}

int32_t stateCountFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SStateInfo*          pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];

  int32_t          numOfElems = 0;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int8_t op = getStateOpType(varDataVal(pCtx->param[1].param.pz));
  if (STATE_OPER_INVALID == op) {
    return 0;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }

    pInfo->isPrevTsSet = true;
    numOfElems++;

    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataSetNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
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
    colDataSetVal(pOutput, pCtx->offset + numOfElems - 1, (char*)&output, false);

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
    }
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
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
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }

    pInfo->isPrevTsSet = true;
    numOfElems++;

    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      colDataSetNULL(pOutput, i);
      // handle selectivity
      if (pCtx->subsidiaries.num > 0) {
        appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
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
    colDataSetVal(pOutput, pCtx->offset + numOfElems - 1, (char*)&output, false);

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, pCtx->offset + numOfElems - 1);
    }
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

bool getCsumFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SSumRes);
  return true;
}

int32_t csumFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSumRes*             pSumRes = GET_ROWCELL_INTERBUF(pResInfo);

  SInputColumnInfoData* pInput = &pCtx->input;
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pSumRes->isPrevTsSet == true && tsList[i] == pSumRes->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pSumRes->prevTs = tsList[i];
    }
    pSumRes->isPrevTsSet = true;

    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataSetNULL(pOutput, i);
      continue;
    }

    char* data = colDataGetData(pInputCol, i);
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t v;
      GET_TYPED_DATA(v, int64_t, type, data);
      pSumRes->isum += v;
      colDataSetVal(pOutput, pos, (char*)&pSumRes->isum, false);
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t v;
      GET_TYPED_DATA(v, uint64_t, type, data);
      pSumRes->usum += v;
      colDataSetVal(pOutput, pos, (char*)&pSumRes->usum, false);
    } else if (IS_FLOAT_TYPE(type)) {
      double v;
      GET_TYPED_DATA(v, double, type, data);
      pSumRes->dsum += v;
      // check for overflow
      if (isinf(pSumRes->dsum) || isnan(pSumRes->dsum)) {
        colDataSetNULL(pOutput, pos);
      } else {
        colDataSetVal(pOutput, pos, (char*)&pSumRes->dsum, false);
      }
    }

    // handle selectivity
    if (pCtx->subsidiaries.num > 0) {
      appendSelectivityValue(pCtx, i, pos);
    }

    numOfElems++;
  }

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
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
  pInfo->prevTs = -1;
  pInfo->isPrevTsSet = false;
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
  TSKEY*                tsList = (int64_t*)pInput->pPTS->pData;

  SColumnInfoData* pInputCol = pInput->pData[0];
  SColumnInfoData* pTsOutput = pCtx->pTsOutput;
  SColumnInfoData* pOutput = (SColumnInfoData*)pCtx->pOutput;

  int32_t numOfElems = 0;
  int32_t type = pInputCol->info.type;
  int32_t startOffset = pCtx->offset;
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (pInfo->isPrevTsSet == true && tsList[i] == pInfo->prevTs) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    } else {
      pInfo->prevTs = tsList[i];
    }
    pInfo->isPrevTsSet = true;

    int32_t pos = startOffset + numOfElems;
    if (colDataIsNull_f(pInputCol->nullbitmap, i)) {
      // colDataSetNULL(pOutput, i);
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
        colDataSetNULL(pOutput, pos);
      } else {
        colDataSetVal(pOutput, pos, (char*)&result, false);
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

  pResInfo->numOfRes = numOfElems;
  return TSDB_CODE_SUCCESS;
}

static SSampleInfo* getSampleOutputInfo(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = GET_ROWCELL_INTERBUF(pResInfo);

  pInfo->data = (char*)pInfo + sizeof(SSampleInfo);
  pInfo->tuplePos = (STuplePos*)((char*)pInfo + sizeof(SSampleInfo) + pInfo->samples * pInfo->colBytes);

  return pInfo;
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

static int32_t doReservoirSample(SqlFunctionCtx* pCtx, SSampleInfo* pInfo, char* data, int32_t index) {
  pInfo->totalPoints++;
  if (pInfo->numSampled < pInfo->samples) {
    sampleAssignResult(pInfo, data, pInfo->numSampled);
    if (pCtx->subsidiaries.num > 0) {
      int32_t code = saveTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[pInfo->numSampled]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    pInfo->numSampled++;
  } else {
    int32_t j = taosRand() % (pInfo->totalPoints);
    if (j < pInfo->samples) {
      sampleAssignResult(pInfo, data, j);
      if (pCtx->subsidiaries.num > 0) {
        int32_t code = updateTupleData(pCtx, index, pCtx->pSrcBlock, &pInfo->tuplePos[j]);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t sampleFunction(SqlFunctionCtx* pCtx) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SSampleInfo*         pInfo = getSampleOutputInfo(pCtx);

  SInputColumnInfoData* pInput = &pCtx->input;

  SColumnInfoData* pInputCol = pInput->pData[0];
  for (int32_t i = pInput->startRowIndex; i < pInput->numOfRows + pInput->startRowIndex; i += 1) {
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }

    char*   data = colDataGetData(pInputCol, i);
    int32_t code = doReservoirSample(pCtx, pInfo, data, i);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if (pInfo->numSampled == 0 && pCtx->subsidiaries.num > 0 && !pInfo->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pInfo->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(pResInfo, pInfo->numSampled, pInfo->numSampled);
  return TSDB_CODE_SUCCESS;
}

int32_t sampleFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(pCtx);

  SSampleInfo* pInfo = getSampleOutputInfo(pCtx);
  pEntryInfo->complete = true;

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t currentRow = pBlock->info.rows;
  if (pInfo->numSampled == 0) {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pInfo->nullTuplePos, currentRow);
    return code;
  }
  for (int32_t i = 0; i < pInfo->numSampled; ++i) {
    colDataSetVal(pCol, currentRow + i, pInfo->data + i * pInfo->colBytes, false);
    code = setSelectivityValue(pCtx, pBlock, &pInfo->tuplePos[i], currentRow + i);
  }

  return code;
}

bool getTailFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
#if 0
  SColumnNode* pCol = (SColumnNode*)nodesListGetNode(pFunc->pParameterList, 0);
  SValueNode*  pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  int32_t      numOfPoints = pVal->datum.i;
  pEnv->calcMemSize = sizeof(STailInfo) + numOfPoints * (POINTER_BYTES + sizeof(STailItem) + pCol->node.resType.bytes);
#endif
  return true;
}

bool tailFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResultInfo) {
#if 0
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
#endif

  return true;
}

static void tailAssignResult(STailItem* pItem, char* data, int32_t colBytes, TSKEY ts, bool isNull) {
#if 0
  pItem->timestamp = ts;
  if (isNull) {
    pItem->isNull = true;
  } else {
    pItem->isNull = false;
    memcpy(pItem->data, data, colBytes);
  }
#endif
}

#if 0
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
#endif

int32_t tailFunction(SqlFunctionCtx* pCtx) {
#if 0
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
      colDataSetNULL(pOutput, pos);
    } else {
      colDataSetVal(pOutput, pos, pItem->data, false);
    }
  }

  return pInfo->numOfPoints;
#endif
  return 0;
}

int32_t tailFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
#if 0
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
    colDataSetVal(pCol, currentRow, pItem->data, false);
    currentRow += 1;
  }

  return pEntryInfo->numOfRes;
#endif
  return 0;
}

bool getUniqueFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
#if 0
  pEnv->calcMemSize = sizeof(SUniqueInfo) + UNIQUE_MAX_RESULT_SIZE;
#endif
  return true;
}

bool uniqueFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
#if 0
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
#endif
  return true;
}

#if 0
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
#endif

int32_t uniqueFunction(SqlFunctionCtx* pCtx) {
#if 0
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
      colDataSetNULL(pOutput, i);
    } else {
      colDataSetVal(pOutput, i, pItem->data, false);
    }
    if (pTsOutput != NULL) {
      colDataSetInt64(pTsOutput, i, &pItem->timestamp);
    }
  }

  return pInfo->numOfPoints;
#endif
  return 0;
}

bool getModeFuncEnv(SFunctionNode* pFunc, SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(SModeInfo);
  return true;
}

bool modeFunctionSetup(SqlFunctionCtx* pCtx, SResultRowEntryInfo* pResInfo) {
  if (!functionSetup(pCtx, pResInfo)) {
    return false;
  }

  SModeInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  pInfo->colType = pCtx->resDataInfo.type;
  pInfo->colBytes = pCtx->resDataInfo.bytes;
  if (pInfo->pHash != NULL) {
    taosHashClear(pInfo->pHash);
  } else {
    pInfo->pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  pInfo->nullTupleSaved = false;
  pInfo->nullTuplePos.pageId = -1;

  pInfo->buf = taosMemoryMalloc(pInfo->colBytes);

  return true;
}

static void modeFunctionCleanup(SModeInfo * pInfo) {
  taosHashCleanup(pInfo->pHash);
  taosMemoryFreeClear(pInfo->buf);
}

static int32_t saveModeTupleData(SqlFunctionCtx* pCtx, char* data, SModeInfo *pInfo, STuplePos* pPos) {
  if (IS_VAR_DATA_TYPE(pInfo->colType)) {
    memcpy(pInfo->buf, data, varDataTLen(data));
  } else {
    memcpy(pInfo->buf, data, pInfo->colBytes);
  }

  return doSaveTupleData(&pCtx->saveHandle, pInfo->buf, pInfo->colBytes, NULL, pPos, pCtx->pStore);
}

static int32_t doModeAdd(SModeInfo* pInfo, int32_t rowIndex, SqlFunctionCtx* pCtx, char* data) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t hashKeyBytes = IS_STR_DATA_TYPE(pInfo->colType) ? varDataTLen(data) : pInfo->colBytes;

  SModeItem* pHashItem = (SModeItem *)taosHashGet(pInfo->pHash, data, hashKeyBytes);
  if (pHashItem == NULL) {
    int32_t    size = sizeof(SModeItem);
    SModeItem  item = {0};

    item.count += 1;
    code = saveModeTupleData(pCtx, data, pInfo, &item.dataPos);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pCtx->subsidiaries.num > 0) {
      code = saveTupleData(pCtx, rowIndex, pCtx->pSrcBlock, &item.tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    taosHashPut(pInfo->pHash, data, hashKeyBytes, &item, sizeof(SModeItem));
  } else {
    pHashItem->count += 1;
    if (pCtx->subsidiaries.num > 0) {
      code = updateTupleData(pCtx, rowIndex, pCtx->pSrcBlock, &pHashItem->tuplePos);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return code;
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
    if (colDataIsNull_s(pInputCol, i)) {
      continue;
    }
    numOfElems++;

    char*   data = colDataGetData(pInputCol, i);
    int32_t code = doModeAdd(pInfo, i, pCtx, data);
    if (code != TSDB_CODE_SUCCESS) {
      modeFunctionCleanup(pInfo);
      return code;
    }
  }

  if (numOfElems == 0 && pCtx->subsidiaries.num > 0 && !pInfo->nullTupleSaved) {
    int32_t code = saveTupleData(pCtx, pInput->startRowIndex, pCtx->pSrcBlock, &pInfo->nullTuplePos);
    if (code != TSDB_CODE_SUCCESS) {
      modeFunctionCleanup(pInfo);
      return code;
    }
    pInfo->nullTupleSaved = true;
  }

  SET_VAL(pResInfo, numOfElems, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t modeFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t              code = TSDB_CODE_SUCCESS;
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SModeInfo*           pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  int32_t              slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData*     pCol = taosArrayGet(pBlock->pDataBlock, slotId);
  int32_t              currentRow = pBlock->info.rows;

  STuplePos resDataPos, resTuplePos;
  int32_t maxCount = 0;

  void *pIter = taosHashIterate(pInfo->pHash, NULL);
  while (pIter != NULL) {
    SModeItem *pItem = (SModeItem *)pIter;
    if (pItem->count >= maxCount) {
      maxCount = pItem->count;
      resDataPos = pItem->dataPos;
      resTuplePos = pItem->tuplePos;
    }

    pIter = taosHashIterate(pInfo->pHash, pIter);
  }

  if (maxCount != 0) {
    const char* pData = loadTupleData(pCtx, &resDataPos);
    if (pData == NULL) {
      code = terrno = TSDB_CODE_NOT_FOUND;
      qError("Load tuple data failed since %s, groupId:%" PRIu64 ", ts:%" PRId64, terrstr(),
             resDataPos.streamTupleKey.groupId, resDataPos.streamTupleKey.ts);
      modeFunctionCleanup(pInfo);
      return code;
    }

    colDataSetVal(pCol, currentRow, pData, false);
    code = setSelectivityValue(pCtx, pBlock, &resTuplePos, currentRow);
  } else {
    colDataSetNULL(pCol, currentRow);
    code = setSelectivityValue(pCtx, pBlock, &pInfo->nullTuplePos, currentRow);
  }

  modeFunctionCleanup(pInfo);

  return code;
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
  if (e.key == INT64_MAX || s.key == INT64_MIN) {
    return 0;
  }

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
  if (pCtx->start.key != INT64_MIN && last->key == INT64_MIN) {
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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

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
        if (pInfo->p.key == st.key) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }

        pInfo->dOutput += twa_get_area(pInfo->p, st);
        pInfo->p = st;
      }
      break;
    }

    default:
      return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  // the last interpolated time window value
  if (pCtx->end.key != INT64_MIN) {
    pInfo->dOutput += twa_get_area(pInfo->p, pCtx->end);
    pInfo->p = pCtx->end;
    numOfElems += 1;
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
    } else if (pInfo->win.ekey == INT64_MAX || pInfo->win.skey == INT64_MIN) {  // no data in timewindow
      pInfo->dOutput = 0;
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
  const int32_t BLOCK_DIST_RESULT_ROWS = 25;

  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pInputCol = pInput->pData[0];
  SResultRowEntryInfo*  pResInfo = GET_RES_INFO(pCtx);
  STableBlockDistInfo*  pDistInfo = GET_ROWCELL_INTERBUF(pResInfo);

  STableBlockDistInfo p1 = {0};
  tDeserializeBlockDistInfo(varDataVal(pInputCol->pData), varDataLen(pInputCol->pData), &p1);

  pDistInfo->numOfBlocks += p1.numOfBlocks;
  pDistInfo->numOfTables += p1.numOfTables;
  pDistInfo->numOfInmemRows += p1.numOfInmemRows;
  pDistInfo->numOfSttRows += p1.numOfSttRows;
  pDistInfo->totalSize += p1.totalSize;
  pDistInfo->totalRows += p1.totalRows;
  pDistInfo->numOfFiles += p1.numOfFiles;

  pDistInfo->defMinRows = p1.defMinRows;
  pDistInfo->defMaxRows = p1.defMaxRows;
  pDistInfo->rowSize = p1.rowSize;

  if (pDistInfo->minRows > p1.minRows) {
    pDistInfo->minRows = p1.minRows;
  }
  if (pDistInfo->maxRows < p1.maxRows) {
    pDistInfo->maxRows = p1.maxRows;
  }
  pDistInfo->numOfVgroups += (p1.numOfTables != 0 ? 1 : 0);
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
  if (tEncodeU32(&encoder, pInfo->numOfSttRows) < 0) return -1;
  if (tEncodeU32(&encoder, pInfo->numOfVgroups) < 0) return -1;

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
  if (tDecodeU32(&decoder, &pInfo->numOfSttRows) < 0) return -1;
  if (tDecodeU32(&decoder, &pInfo->numOfVgroups) < 0) return -1;

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
  double  averageSize = 0;
  if (pData->numOfBlocks != 0) {
    averageSize = ((double)pData->totalSize) / pData->numOfBlocks;
  }
  uint64_t totalRawSize = pData->totalRows * pData->rowSize;
  double   compRatio = 0;
  if (totalRawSize != 0) {
    compRatio = pData->totalSize * 100 / (double)totalRawSize;
  }

  int32_t len = sprintf(st + VARSTR_HEADER_SIZE,
                        "Total_Blocks=[%d] Total_Size=[%.2f KiB] Average_size=[%.2f KiB] Compression_Ratio=[%.2f %c]",
                        pData->numOfBlocks, pData->totalSize / 1024.0, averageSize / 1024.0, compRatio, '%');

  varDataSetLen(st, len);
  colDataSetVal(pColInfo, row++, st, false);

  int64_t avgRows = 0;
  if (pData->numOfBlocks > 0) {
    avgRows = pData->totalRows / pData->numOfBlocks;
  }

  len = sprintf(st + VARSTR_HEADER_SIZE, "Block_Rows=[%" PRId64 "] MinRows=[%d] MaxRows=[%d] AvgRows=[%" PRId64 "]",
                pData->totalRows, pData->minRows, pData->maxRows, avgRows);
  varDataSetLen(st, len);
  colDataSetVal(pColInfo, row++, st, false);

  len = sprintf(st + VARSTR_HEADER_SIZE, "Inmem_Rows=[%d] Stt_Rows=[%d] ", pData->numOfInmemRows, pData->numOfSttRows);
  varDataSetLen(st, len);
  colDataSetVal(pColInfo, row++, st, false);

  len = sprintf(st + VARSTR_HEADER_SIZE, "Total_Tables=[%d] Total_Filesets=[%d] Total_Vgroups=[%d]", pData->numOfTables,
                pData->numOfFiles, pData->numOfVgroups);

  varDataSetLen(st, len);
  colDataSetVal(pColInfo, row++, st, false);

  len = sprintf(st + VARSTR_HEADER_SIZE,
                "--------------------------------------------------------------------------------");
  varDataSetLen(st, len);
  colDataSetVal(pColInfo, row++, st, false);

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
  int32_t bucketRange = ceil(((double) (pData->defMaxRows - pData->defMinRows)) / numOfBuckets);

  for (int32_t i = 0; i < tListLen(pData->blockRowsHisto); ++i) {
    len = sprintf(st + VARSTR_HEADER_SIZE, "%04d |", pData->defMinRows + bucketRange * (i + 1));

    int32_t num = 0;
    if (pData->blockRowsHisto[i] > 0) {
      num = (pData->blockRowsHisto[i]) / factor;
    }

    for (int32_t j = 0; j < num; ++j) {
      int32_t x = sprintf(st + VARSTR_HEADER_SIZE + len, "%c", '|');
      len += x;
    }

    if (pData->blockRowsHisto[i] > 0) {
      double v = pData->blockRowsHisto[i] * 100.0 / pData->numOfBlocks;
      len += sprintf(st + VARSTR_HEADER_SIZE + len, "  %d (%.2f%c)", pData->blockRowsHisto[i], v, '%');
    }

    varDataSetLen(st, len);
    colDataSetVal(pColInfo, row++, st, false);
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
        if (tsList[i] == pDerivInfo->prevTs) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }
        double r = ((v - pDerivInfo->prevValue) * pDerivInfo->tsWindow) / (tsList[i] - pDerivInfo->prevTs);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataSetNULL(pOutput, pos);
          } else {
            colDataSetVal(pOutput, pos, (const char*)&r, false);
          }

          if (pTsOutput != NULL) {
            colDataSetInt64(pTsOutput, pos, &tsList[i]);
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
        if (tsList[i] == pDerivInfo->prevTs) {
          return TSDB_CODE_FUNC_DUP_TIMESTAMP;
        }
        double r = ((pDerivInfo->prevValue - v) * pDerivInfo->tsWindow) / (pDerivInfo->prevTs - tsList[i]);
        if (pDerivInfo->ignoreNegative && r < 0) {
        } else {
          if (isinf(r) || isnan(r)) {
            colDataSetNULL(pOutput, pos);
          } else {
            colDataSetVal(pOutput, pos, (const char*)&r, false);
          }

          if (pTsOutput != NULL) {
            colDataSetInt64(pTsOutput, pos, &pDerivInfo->prevTs);
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

  pResInfo->numOfRes = numOfElems;

  return TSDB_CODE_SUCCESS;
}

int32_t getIrateInfoSize() { return (int32_t)sizeof(SRateInfo); }

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
      pRateInfo->hasResult = 1;
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
    } else if (tsList[i] == pRateInfo->lastKey) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
    }

    if ((INT64_MIN == pRateInfo->firstKey) || tsList[i] > pRateInfo->firstKey) {
      pRateInfo->firstValue = v;
      pRateInfo->firstKey = tsList[i];
    } else if (tsList[i] == pRateInfo->firstKey) {
      return TSDB_CODE_FUNC_DUP_TIMESTAMP;
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

static void irateTransferInfoImpl(TSKEY inputKey, SRateInfo* pInput, SRateInfo* pOutput, bool isFirstKey) {
  if (inputKey > pOutput->lastKey) {
    pOutput->firstKey   = pOutput->lastKey;
    pOutput->firstValue = pOutput->lastValue;

    pOutput->lastKey    = isFirstKey ? pInput->firstKey : pInput->lastKey;
    pOutput->lastValue  = isFirstKey ? pInput->firstValue : pInput->lastValue;
  } else if ((inputKey < pOutput->lastKey) && (inputKey > pOutput->firstKey)) {
    pOutput->firstKey   = isFirstKey ? pInput->firstKey : pInput->lastKey;
    pOutput->firstValue = isFirstKey ? pInput->firstValue : pInput->lastValue;
  } else {
    // inputKey < pOutput->firstKey
  }
}

static void irateCopyInfo(SRateInfo* pInput, SRateInfo* pOutput) {
  pOutput->firstKey = pInput->firstKey;
  pOutput->lastKey  = pInput->lastKey;

  pOutput->firstValue = pInput->firstValue;
  pOutput->lastValue  = pInput->lastValue;
}

static int32_t irateTransferInfo(SRateInfo* pInput, SRateInfo* pOutput) {
  if ((pInput->firstKey != INT64_MIN && (pInput->firstKey == pOutput->firstKey || pInput->firstKey == pOutput->lastKey)) ||
      (pInput->lastKey != INT64_MIN && (pInput->lastKey  == pOutput->firstKey || pInput->lastKey  == pOutput->lastKey))) {
    return TSDB_CODE_FUNC_DUP_TIMESTAMP;
  }

  if (pOutput->hasResult == 0) {
    irateCopyInfo(pInput, pOutput);
    pOutput->hasResult = pInput->hasResult;
    return TSDB_CODE_SUCCESS;
  }

  if (pInput->firstKey != INT64_MIN) {
    irateTransferInfoImpl(pInput->firstKey, pInput, pOutput, true);
  }

  if (pInput->lastKey != INT64_MIN) {
    irateTransferInfoImpl(pInput->lastKey, pInput, pOutput, false);
  }

  pOutput->hasResult = pInput->hasResult;
  return TSDB_CODE_SUCCESS;
}

int32_t irateFunctionMerge(SqlFunctionCtx* pCtx) {
  SInputColumnInfoData* pInput = &pCtx->input;
  SColumnInfoData*      pCol = pInput->pData[0];
  if (pCol->info.type != TSDB_DATA_TYPE_BINARY) {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));

  int32_t start = pInput->startRowIndex;
  for (int32_t i = start; i < start + pInput->numOfRows; ++i) {
    char*        data = colDataGetData(pCol, i);
    SRateInfo*   pInputInfo = (SRateInfo*)varDataVal(data);
    if (pInputInfo->hasResult) {
      int32_t code = irateTransferInfo(pInputInfo, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  if (pInfo->hasResult) {
    GET_RES_INFO(pCtx)->numOfRes = 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t iratePartialFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  SRateInfo*           pInfo = GET_ROWCELL_INTERBUF(GET_RES_INFO(pCtx));
  int32_t              resultBytes = getIrateInfoSize();
  char*                res = taosMemoryCalloc(resultBytes + VARSTR_HEADER_SIZE, sizeof(char));

  memcpy(varDataVal(res), pInfo, resultBytes);
  varDataSetLen(res, resultBytes);

  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  colDataSetVal(pCol, pBlock->info.rows, res, false);

  taosMemoryFree(res);
  return pResInfo->numOfRes;
}

int32_t irateFinalize(SqlFunctionCtx* pCtx, SSDataBlock* pBlock) {
  int32_t          slotId = pCtx->pExpr->base.resSchema.slotId;
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);

  SResultRowEntryInfo* pResInfo = GET_RES_INFO(pCtx);
  pResInfo->isNullRes = (pResInfo->numOfRes == 0) ? 1 : 0;

  SRateInfo* pInfo = GET_ROWCELL_INTERBUF(pResInfo);
  double     result = doCalcRate(pInfo, (double)TSDB_TICK_PER_SECOND(pCtx->param[1].param.i));
  colDataSetVal(pCol, pBlock->info.rows, (const char*)&result, pResInfo->isNullRes);

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

  if (pInputCol->pData == NULL || colDataIsNull_s(pInputCol, startIndex)) {
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
    int32_t currentRow = pBlock->info.rows;
    for (; currentRow < pBlock->info.rows + pResInfo->numOfRes; ++currentRow) {
      colDataSetVal(pCol, currentRow, pInfo->data, pInfo->isNull ? true : false);
    }
  } else {
    pResInfo->numOfRes = 0;
  }

  return pResInfo->numOfRes;
}

int32_t groupKeyCombine(SqlFunctionCtx* pDestCtx, SqlFunctionCtx* pSourceCtx) {
  SResultRowEntryInfo* pDResInfo = GET_RES_INFO(pDestCtx);
  SGroupKeyInfo*       pDBuf = GET_ROWCELL_INTERBUF(pDResInfo);

  SResultRowEntryInfo* pSResInfo = GET_RES_INFO(pSourceCtx);
  SGroupKeyInfo*       pSBuf = GET_ROWCELL_INTERBUF(pSResInfo);

  // escape rest of data blocks to avoid first entry to be overwritten.
  if (pDBuf->hasResult) {
    goto _group_key_over;
  }

  if (pSBuf->isNull) {
    pDBuf->isNull = true;
    pDBuf->hasResult = true;
    goto _group_key_over;
  }

  if (IS_VAR_DATA_TYPE(pSourceCtx->resDataInfo.type)) {
    memcpy(pDBuf->data, pSBuf->data,
           (pSourceCtx->resDataInfo.type == TSDB_DATA_TYPE_JSON) ? getJsonValueLen(pSBuf->data) : varDataTLen(pSBuf->data));
  } else {
    memcpy(pDBuf->data, pSBuf->data, pSourceCtx->resDataInfo.bytes);
  }

  pDBuf->hasResult = true;

_group_key_over:

  SET_VAL(pDResInfo, 1, 1);
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

    bool  isNull = colDataIsNull(pInputCol, pInput->numOfRows, i, NULL);
    char* data = isNull ? NULL : colDataGetData(pInputCol, i);

    TSKEY cts = getRowPTs(pInput->pPTS, i);
    if (pResInfo->numOfRes == 0 || pInfo->ts < cts) {
      int32_t code = doSaveLastrow(pCtx, data, i, cts, pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      pResInfo->numOfRes = 1;
    }
  }

  SET_VAL(pResInfo, numOfElems, 1);
  return TSDB_CODE_SUCCESS;
}
